/*
 * Copyright (C) 2009 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.providers.contacts;

import static android.provider.CallLog.Calls.MISSED_REASON_NOT_MISSED;

import static com.android.providers.contacts.ContactsProvider2.BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import android.content.ContentResolver;
import android.telecom.CallerInfo;
import com.android.providers.contacts.testutil.CommonDatabaseUtils;
import com.android.providers.contacts.util.ContactsPermissions;
import com.android.providers.contacts.util.FileUtilities;
import com.android.providers.contacts.util.PhoneAccountHandleMigrationUtils;

import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Intent;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.VoicemailContract.Voicemails;
import android.telecom.PhoneAccountHandle;
import android.telecom.TelecomManager;
import android.telephony.SubscriptionInfo;
import android.test.suitebuilder.annotation.MediumTest;

import org.junit.Assert;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link CallLogProvider}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.CallLogProviderTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class CallLogProviderTest extends BaseContactsProvider2Test {
    /** Fields specific to voicemail provider that should not be exposed by call_log*/
    private static final String[] VOICEMAIL_PROVIDER_SPECIFIC_COLUMNS = new String[] {
            Voicemails._DATA,
            Voicemails.HAS_CONTENT,
            Voicemails.MIME_TYPE,
            Voicemails.SOURCE_PACKAGE,
            Voicemails.SOURCE_DATA,
            Voicemails.STATE,
            Voicemails.DIRTY,
            Voicemails.DELETED};
    /** Total number of columns exposed by call_log provider. */
    private static final int NUM_CALLLOG_FIELDS = 41;

    private static final int MIN_MATCH = 7;

    private static final long TEST_TIMEOUT = 5000;

    private static final String TELEPHONY_PACKAGE = "com.android.phone";
    private static final String TELEPHONY_CLASS
            = "com.android.services.telephony.TelephonyConnectionService";
    private static final String TEST_PHONE_ACCOUNT_HANDLE_SUB_ID = "666";
    private static final int TEST_PHONE_ACCOUNT_HANDLE_SUB_ID_INT = 666;
    private static final String TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1 = "891004234814455936F";
    private static final String TEST_PHONE_ACCOUNT_HANDLE_ICC_ID2 = "891004234814455937";
    private static final String TEST_COMPONENT_NAME = "foo/bar";

    private static final Uri INVALID_CALL_LOG_URI = Uri.parse(
            "content://call_log/call_composer/%2fdata%2fdata%2fcom.android.providers"
                    + ".contacts%2fshared_prefs%2fContactsUpgradeReceiver.xml");

    private static final String TEST_FAIL_DID_NOT_TRHOW_SE =
            "fail test because Security Exception was not throw";


    private int mOldMinMatch;

    private CallLogProviderTestable mCallLogProvider;
    private BroadcastReceiver mBroadcastReceiver;

    @Override
    protected Class<? extends ContentProvider> getProviderClass() {
       return SynchronousContactsProvider2.class;
    }

    @Override
    protected String getAuthority() {
        return ContactsContract.AUTHORITY;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mCallLogProvider = addProvider(CallLogProviderTestable.class, CallLog.AUTHORITY);
        mBroadcastReceiver = mCallLogProvider.getBroadcastReceiverForTest();
        mOldMinMatch = mCallLogProvider.getMinMatchForTest();
        mCallLogProvider.setMinMatchForTest(MIN_MATCH);
    }

    @Override
    protected void tearDown() throws Exception {
        setUpWithVoicemailPermissions();
        mResolver.delete(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null);
        setTimeForTest(null);
        mCallLogProvider.setMinMatchForTest(mOldMinMatch);
        super.tearDown();
    }

    private CallLogDatabaseHelper getMockCallLogDatabaseHelper(String databaseNameForTesting) {
        CallLogDatabaseHelper callLogDatabaseHelper = new CallLogDatabaseHelper(
                mTestContext, databaseNameForTesting);
        SQLiteDatabase db = callLogDatabaseHelper.getWritableDatabase();
        // callLogDatabaseHelper.getOpenHelper().onCreate(db);
        db.execSQL("DELETE FROM " + CallLogDatabaseHelper.Tables.CALLS);
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                    PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1);
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                    PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1);
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                    PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1);
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, TEST_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1);
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                    PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID2);
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        {
            final ContentValues values = new ContentValues();
            values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                    PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME);
            values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 1);
            values.put(Calls.PHONE_ACCOUNT_ID, "FAKE_ICCID");
            db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values);
        }
        return callLogDatabaseHelper;
    }

    public void testPhoneAccountHandleMigrationSimEvent() throws IOException {
        CallLogDatabaseHelper originalCallLogDatabaseHelper
                = mCallLogProvider.getCallLogDatabaseHelperForTest();

        // Mock SubscriptionManager
        SubscriptionInfo subscriptionInfo = new SubscriptionInfo(
                TEST_PHONE_ACCOUNT_HANDLE_SUB_ID_INT, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1,
                        1, "a", "b", 1, 1, "test", 1, null, null, null, null, false, null, null);
        when(mSubscriptionManager.getActiveSubscriptionInfo(
                eq(TEST_PHONE_ACCOUNT_HANDLE_SUB_ID_INT))).thenReturn(subscriptionInfo);

        // Mock CallLogDatabaseHelper
        CallLogDatabaseHelper callLogDatabaseHelper = getMockCallLogDatabaseHelper(
                "testCallLogPhoneAccountHandleMigrationSimEvent.db");
        PhoneAccountHandleMigrationUtils phoneAccountHandleMigrationUtils = callLogDatabaseHelper
                .getPhoneAccountHandleMigrationUtils();

        // Test setPhoneAccountMigrationStatusPending as false
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(false);
        assertFalse(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        // Test CallLogDatabaseHelper.isPhoneAccountMigrationPending as true
        // and set for testing migration logic
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(true);
        assertTrue(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        mCallLogProvider.setCallLogDatabaseHelperForTest(callLogDatabaseHelper);
        final SQLiteDatabase sqLiteDatabase = callLogDatabaseHelper.getReadableDatabase();

        // Check each entry in the Calls table has a new coloumn of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING of 1
        assertEquals(6, DatabaseUtils.longForQuery(sqLiteDatabase, "select count(*) from " +
                CallLogDatabaseHelper.Tables.CALLS + " where " +
                        Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " = 1", null));

        // Prepare PhoneAccountHandle for the new sim event
        PhoneAccountHandle phoneAccountHandle = new PhoneAccountHandle(
                new ComponentName(TELEPHONY_PACKAGE, TELEPHONY_CLASS),
                        TEST_PHONE_ACCOUNT_HANDLE_SUB_ID);
        Intent intent = new Intent(TelecomManager.ACTION_PHONE_ACCOUNT_REGISTERED);
        intent.putExtra(TelecomManager.EXTRA_PHONE_ACCOUNT_HANDLE, phoneAccountHandle);

        mBroadcastReceiver.onReceive(mTestContext, intent);

        // Wait for a while until the migration happens
        long countMigrated = 0;

        while (countMigrated != 4) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // do nothing
            }
            countMigrated = DatabaseUtils.longForQuery(sqLiteDatabase, "select count(*) from " +
                    CallLogDatabaseHelper.Tables.CALLS + " where " +
                            Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " = 0", null);
        }

        // Check each entry in the CALLS that three coloumns of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING that has migrated
        assertEquals(4, countMigrated);
        // Check each entry in the CALLS that one coloumns of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING that is not expected to be migrated
        assertEquals(2, DatabaseUtils.longForQuery(sqLiteDatabase, "select count(*) from " +
                CallLogDatabaseHelper.Tables.CALLS + " where " +
                        Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " = 1", null));

        // Verify the pending status of phone account migration.
        assertTrue(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        mCallLogProvider.setCallLogDatabaseHelperForTest(originalCallLogDatabaseHelper);
    }


    public void testPhoneAccountHandleMigrationInitiation() throws Exception {
        CallLogDatabaseHelper originalCallLogDatabaseHelper
                = mCallLogProvider.getCallLogDatabaseHelperForTest();

        // Mock SubscriptionManager
        SubscriptionInfo subscriptionInfo = new SubscriptionInfo(
                TEST_PHONE_ACCOUNT_HANDLE_SUB_ID_INT, TEST_PHONE_ACCOUNT_HANDLE_ICC_ID1,
                        1, "a", "b", 1, 1, "test", 1, null, null, null, null, false, null, null);
        List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
        subscriptionInfoList.add(subscriptionInfo);
        when(mSubscriptionManager.getAllSubscriptionInfoList()).thenReturn(subscriptionInfoList);

        // Mock CallLogDatabaseHelper
        CallLogDatabaseHelper callLogDatabaseHelper = getMockCallLogDatabaseHelper(
                "testCallLogPhoneAccountHandleMigrationInitiation.db");
        PhoneAccountHandleMigrationUtils phoneAccountHandleMigrationUtils = callLogDatabaseHelper
                .getPhoneAccountHandleMigrationUtils();

        // Test setPhoneAccountMigrationStatusPending as false
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(false);
        assertFalse(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        // Test CallLogDatabaseHelper.isPhoneAccountMigrationPending as true
        // and set for testing migration logic
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(true);

        mCallLogProvider.setCallLogDatabaseHelperForTest(callLogDatabaseHelper);
        final SQLiteDatabase sqLiteDatabase = callLogDatabaseHelper.getReadableDatabase();

        // Check each entry in the Calls table has a new coloumn of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING as true
        assertEquals(6, DatabaseUtils.longForQuery(sqLiteDatabase, "select count(*) from " +
                CallLogDatabaseHelper.Tables.CALLS + " where " +
                        Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " == 1", null));

        // Prepare Task for BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES
        mCallLogProvider.mReadAccessLatch = new CountDownLatch(1);
        mCallLogProvider.performBackgroundTask(mCallLogProvider.BACKGROUND_TASK_INITIALIZE, null);
        assertTrue(mCallLogProvider.mReadAccessLatch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS));

        // Check each entry in the CALLS with a coloumn of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING that has migrated
        Cursor cursor = sqLiteDatabase.query(CallLogDatabaseHelper.Tables.CALLS, null,
                Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " = 0", null, null, null, null);
        assertEquals(4, cursor.getCount());
        while (cursor.moveToNext()) {
            assertEquals(TEST_PHONE_ACCOUNT_HANDLE_SUB_ID_INT, cursor.getInt(cursor.getColumnIndex(Calls.PHONE_ACCOUNT_ID)));
        }
        assertEquals(2, DatabaseUtils.longForQuery(sqLiteDatabase, "select count(*) from " +
                CallLogDatabaseHelper.Tables.CALLS + " where " +
                        Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " = 1", null));

        // Verify the pending status of phone account migration.
        assertTrue(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        mCallLogProvider.setCallLogDatabaseHelperForTest(originalCallLogDatabaseHelper);
    }

    public void testPhoneAccountHandleMigrationPendingStatus() {
        // Mock CallLogDatabaseHelper
        CallLogDatabaseHelper callLogDatabaseHelper = getMockCallLogDatabaseHelper(
                "testPhoneAccountHandleMigrationPendingStatus.db");
        PhoneAccountHandleMigrationUtils phoneAccountHandleMigrationUtils = callLogDatabaseHelper
                .getPhoneAccountHandleMigrationUtils();

        // Test setPhoneAccountMigrationStatusPending as false
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(false);
        assertFalse(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());

        // Test CallLogDatabaseHelper.isPhoneAccountMigrationPending as true
        // and set for testing migration logic
        phoneAccountHandleMigrationUtils.setPhoneAccountMigrationStatusPending(true);
        assertTrue(phoneAccountHandleMigrationUtils.isPhoneAccountMigrationPending());
    }

    public void testInsert_RegularCallRecord() {
        setTimeForTest(1000L);
        ContentValues values = getDefaultCallValues();
        Uri uri = mResolver.insert(Calls.CONTENT_URI, values);
        values.put(Calls.COUNTRY_ISO, "us");
        assertStoredValues(uri, values);
        assertSelection(uri, values, Calls._ID, ContentUris.parseId(uri));
        assertLastModified(uri, 1000);
    }

    private void setUpWithVoicemailPermissions() {
        mActor.addPermissions(ADD_VOICEMAIL_PERMISSION);
        mActor.addPermissions(READ_VOICEMAIL_PERMISSION);
        mActor.addPermissions(WRITE_VOICEMAIL_PERMISSION);
    }

    public void testInsert_VoicemailCallRecord() {
        setUpWithVoicemailPermissions();
        setTimeForTest(1000L);
        final ContentValues values = getDefaultCallValues();
        values.put(Calls.TYPE, Calls.VOICEMAIL_TYPE);
        values.put(Calls.VOICEMAIL_URI, "content://foo/voicemail/2");

        // Should fail with the base content uri without the voicemail param.
        EvenMoreAsserts.assertThrows(IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
                mResolver.insert(Calls.CONTENT_URI, values);
            }
        });

        // Now grant voicemail permission - should succeed.
        Uri uri  = mResolver.insert(Calls.CONTENT_URI_WITH_VOICEMAIL, values);
        assertStoredValues(uri, values);
        assertSelection(uri, values, Calls._ID, ContentUris.parseId(uri));
        assertLastModified(uri, 1000);
    }

    public void testUpdate() {
        setTimeForTest(1000L);
        Uri uri = insertCallRecord();
        ContentValues values = new ContentValues();
        values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
        values.put(Calls.NUMBER, "1-800-263-7643");
        values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
        values.put(Calls.DATE, 2000);
        values.put(Calls.DURATION, 40);
        values.put(Calls.CACHED_NAME, "1-800-GOOG-411");
        values.put(Calls.CACHED_NUMBER_TYPE, Phone.TYPE_CUSTOM);
        values.put(Calls.CACHED_NUMBER_LABEL, "Directory");

        int count = mResolver.update(uri, values, null, null);
        assertEquals(1, count);
        assertStoredValues(uri, values);
        assertLastModified(uri, 1000);
    }

    public void testDelete() {
        Uri uri = insertCallRecord();
        try {
            mResolver.delete(uri, null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
            // Expected
        }

        int count = mResolver.delete(Calls.CONTENT_URI, Calls._ID + "="
                + ContentUris.parseId(uri), null);
        assertEquals(1, count);
        assertEquals(0, getCount(uri, null, null));
    }

    public void testCallLogFilter() {
        ContentValues values = getDefaultCallValues();
        mResolver.insert(Calls.CONTENT_URI, values);

        Uri filterUri = Uri.withAppendedPath(Calls.CONTENT_FILTER_URI, "1-800-4664-411");
        Cursor c = mResolver.query(filterUri, null, null, null, null);
        assertEquals(1, c.getCount());
        c.moveToFirst();
        assertCursorValues(c, values);
        c.close();

        filterUri = Uri.withAppendedPath(Calls.CONTENT_FILTER_URI, "1-888-4664-411");
        c = mResolver.query(filterUri, null, null, null, null);
        assertEquals(0, c.getCount());
        c.close();
    }

    public void testAddCall() {
        CallerInfo ci = new CallerInfo();
        ci.setName("1-800-GOOG-411");
        ci.numberType = Phone.TYPE_CUSTOM;
        ci.numberLabel = "Directory";
        final ComponentName sComponentName = new ComponentName(
                "com.android.server.telecom",
                "TelecomServiceImpl");
        PhoneAccountHandle subscription = new PhoneAccountHandle(
                sComponentName, "sub0");

        // Allow self-calls in order to add the call
        ContactsPermissions.ALLOW_SELF_CALL = true;
        Uri uri = Calls.addCall(ci, getMockContext(), "1-800-263-7643",
                Calls.PRESENTATION_ALLOWED, Calls.OUTGOING_TYPE, 0, subscription, 2000,
                40, null, MISSED_REASON_NOT_MISSED, 0);
        ContactsPermissions.ALLOW_SELF_CALL = false;
        assertNotNull(uri);
        assertEquals("0@" + CallLog.AUTHORITY, uri.getAuthority());

        ContentValues values = new ContentValues();
        values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
        values.put(Calls.FEATURES, 0);
        values.put(Calls.NUMBER, "1-800-263-7643");
        values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
        values.put(Calls.DATE, 2000);
        values.put(Calls.DURATION, 40);
        values.put(Calls.CACHED_NAME, ci.getName());
        values.put(Calls.CACHED_NUMBER_TYPE, (String) null);
        values.put(Calls.CACHED_NUMBER_LABEL, (String) null);
        values.put(Calls.COUNTRY_ISO, "us");
        values.put(Calls.GEOCODED_LOCATION, "usa");
        values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME,
                "com.android.server.telecom/TelecomServiceImpl");
        values.put(Calls.PHONE_ACCOUNT_ID, "sub0");
        // Casting null to Long as there are many forms of "put" which have nullable second
        // parameters and the compiler needs a hint as to which form is correct.
        values.put(Calls.DATA_USAGE, (Long) null);
        values.put(Calls.MISSED_REASON, 0);
        values.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING, 0);
        assertStoredValues(uri, values);
    }

    // Test to check that the calls and voicemail uris returns expected results.
    public void testDifferentContentUris() {
        setUpWithVoicemailPermissions();
        // Insert one voicemaail and two regular call record.
        insertVoicemailRecord();
        insertCallRecord();
        insertCallRecord();

        // With the default uri, only 2 call entries should be returned.
        // With the voicemail uri all 3 should be returned.
        assertEquals(2, getCount(Calls.CONTENT_URI, null, null));
        assertEquals(3, getCount(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null));
    }

    public void testLimitParamReturnsCorrectLimit() {
        for (int i=0; i<10; i++) {
            insertCallRecord();
        }
        Uri uri = Calls.CONTENT_URI.buildUpon()
                .appendQueryParameter(Calls.LIMIT_PARAM_KEY, "4")
                .build();
        assertEquals(4, getCount(uri, null, null));
    }

    public void testLimitAndOffsetParamReturnsCorrectEntries() {
        for (int i=0; i<10; i++) {
            mResolver.insert(Calls.CONTENT_URI, getDefaultValues(Calls.INCOMING_TYPE));
        }
        for (int i=0; i<10; i++) {
            mResolver.insert(Calls.CONTENT_URI, getDefaultValues(Calls.MISSED_TYPE));
        }
        // Limit 4 records.  Discard first 8.
        Uri uri = Calls.CONTENT_URI.buildUpon()
                .appendQueryParameter(Calls.LIMIT_PARAM_KEY, "4")
                .appendQueryParameter(Calls.OFFSET_PARAM_KEY, "8")
                .build();
        String[] projection = new String[] {Calls._ID, Calls.TYPE};
        Cursor c = mResolver.query(uri, projection, null, null, null);
        try {
            // First two should be incoming, next two should be missed.
            for (int i = 0; i < 2; i++) {
                c.moveToNext();
                assertEquals(Calls.INCOMING_TYPE, c.getInt(1));
            }
            for (int i = 0; i < 2; i++) {
                c.moveToNext();
                assertEquals(Calls.MISSED_TYPE, c.getInt(1));
            }
        } finally {
            c.close();
        }
    }

    /**
     * Tests scenario where an app gives {@link ContentResolver} a file to open that is not in the
     * Call Log Provider directory.
     */
    public void testOpenFileOutsideOfScopeThrowsException() throws FileNotFoundException {
        try {
            mResolver.openFile(INVALID_CALL_LOG_URI, "w", null);
            // previous line should throw exception
            fail(TEST_FAIL_DID_NOT_TRHOW_SE);
        } catch (SecurityException e) {
            Assert.assertTrue(
                    e.toString().contains(FileUtilities.INVALID_CALL_LOG_PATH_EXCEPTION_MESSAGE));
        }
    }

    /**
     * Tests scenario where an app gives {@link ContentResolver} a file to delete that is not in the
     * Call Log Provider directory.
     */
    public void testDeleteFileOutsideOfScopeThrowsException() {
        try {
            mResolver.delete(INVALID_CALL_LOG_URI, "w", null);
            // previous line should throw exception
            fail(TEST_FAIL_DID_NOT_TRHOW_SE);
        } catch (SecurityException e) {
            Assert.assertTrue(
                    e.toString().contains(FileUtilities.INVALID_CALL_LOG_PATH_EXCEPTION_MESSAGE));
        }
    }

    /**
     * Tests scenario where an app gives {@link ContentResolver} a file to insert outside the
     * Call Log Provider directory.
     */
    public void testInsertFileOutsideOfScopeThrowsException() {
        try {
            mResolver.insert(INVALID_CALL_LOG_URI, new ContentValues());
            // previous line should throw exception
            fail(TEST_FAIL_DID_NOT_TRHOW_SE);
        } catch (SecurityException e) {
            Assert.assertTrue(
                    e.toString().contains(FileUtilities.INVALID_CALL_LOG_PATH_EXCEPTION_MESSAGE));
        }
    }

    public void testUriWithBadLimitParamThrowsException() {
        assertParamThrowsIllegalArgumentException(Calls.LIMIT_PARAM_KEY, "notvalid");
    }

    public void testUriWithBadOffsetParamThrowsException() {
        assertParamThrowsIllegalArgumentException(Calls.OFFSET_PARAM_KEY, "notvalid");
    }

    private void assertParamThrowsIllegalArgumentException(String key, String value) {
        Uri uri = Calls.CONTENT_URI.buildUpon()
                .appendQueryParameter(key, value)
                .build();
        try {
            mResolver.query(uri, null, null, null, null);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue("Error does not contain value in question.",
                    e.toString().contains(value));
        }
    }

    // Test to check that none of the voicemail provider specific fields are
    // insertable through call_log provider.
    public void testCannotAccessVoicemailSpecificFields_Insert() {
        for (String voicemailColumn : VOICEMAIL_PROVIDER_SPECIFIC_COLUMNS) {
            final ContentValues values = getDefaultCallValues();
            values.put(voicemailColumn, "foo");
            EvenMoreAsserts.assertThrows("Column: " + voicemailColumn,
                    IllegalArgumentException.class, new Runnable() {
                    @Override
                    public void run() {
                        mResolver.insert(Calls.CONTENT_URI, values);
                    }
                });
        }
    }

    // Test to check that none of the voicemail provider specific fields are
    // exposed through call_log provider query.
    public void testCannotAccessVoicemailSpecificFields_Query() {
        // Query.
        Cursor cursor = mResolver.query(Calls.CONTENT_URI, null, null, null, null);
        List<String> columnNames = Arrays.asList(cursor.getColumnNames());
        assertEquals(NUM_CALLLOG_FIELDS, columnNames.size());
        // None of the voicemail provider specific columns should be present.
        for (String voicemailColumn : VOICEMAIL_PROVIDER_SPECIFIC_COLUMNS) {
            assertFalse("Unexpected column: '" + voicemailColumn + "' returned.",
                    columnNames.contains(voicemailColumn));
        }
    }

    // Test to check that none of the voicemail provider specific fields are
    // updatable through call_log provider.
    public void testCannotAccessVoicemailSpecificFields_Update() {
        for (String voicemailColumn : VOICEMAIL_PROVIDER_SPECIFIC_COLUMNS) {
            final Uri insertedUri = insertCallRecord();
            final ContentValues values = new ContentValues();
            values.put(voicemailColumn, "foo");
            EvenMoreAsserts.assertThrows("Column: " + voicemailColumn,
                    IllegalArgumentException.class, new Runnable() {
                    @Override
                    public void run() {
                        mResolver.update(insertedUri, values, null, null);
                    }
                });
        }
    }

    public void testVoicemailPermissions_Insert() {
        EvenMoreAsserts.assertThrows(SecurityException.class, new Runnable() {
            @Override
            public void run() {
                mResolver.insert(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultVoicemailValues());
            }
        });
        // Should now succeed with permissions granted.
        setUpWithVoicemailPermissions();
        mResolver.insert(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultVoicemailValues());
    }

    public void testVoicemailPermissions_Update() {
        EvenMoreAsserts.assertThrows(SecurityException.class, new Runnable() {
            @Override
            public void run() {
                mResolver.update(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultVoicemailValues(),
                        null, null);
            }
        });

        // Should succeed with manage permission granted
        mActor.addPermissions(WRITE_VOICEMAIL_PERMISSION);
        mResolver.update(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultCallValues(), null, null);
        mActor.removePermissions(WRITE_VOICEMAIL_PERMISSION);

        // Should also succeed with full permissions granted.
        setUpWithVoicemailPermissions();
        mResolver.update(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultCallValues(), null, null);
    }

    public void testVoicemailPermissions_Query() {
        EvenMoreAsserts.assertThrows(SecurityException.class, new Runnable() {
            @Override
            public void run() {
                mResolver.query(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null, null, null);
            }
        });

        // Should succeed with read_all permission granted
        mActor.addPermissions(READ_VOICEMAIL_PERMISSION);
        mResolver.query(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null, null, null);
        mActor.removePermissions(READ_VOICEMAIL_PERMISSION);

        // Should also succeed with full permissions granted.
        setUpWithVoicemailPermissions();
        mResolver.query(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null, null, null);
    }

    public void testVoicemailPermissions_Delete() {
        EvenMoreAsserts.assertThrows(SecurityException.class, new Runnable() {
            @Override
            public void run() {
                mResolver.delete(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null);
            }
        });

        // Should succeed with manage permission granted
        mActor.addPermissions(WRITE_VOICEMAIL_PERMISSION);
        mResolver.delete(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null);
        mActor.removePermissions(WRITE_VOICEMAIL_PERMISSION);

        // Should now succeed with permissions granted.
        setUpWithVoicemailPermissions();
        mResolver.delete(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null);
    }

    public void testCopyEntriesFromCursor_AllEntriesSyncedWithoutDuplicatesPresent() {
        assertStoredValues(Calls.CONTENT_URI);

        assertEquals(10, mCallLogProvider.copyEntriesFromCursor(
                getTestCallLogCursor(), 5, /* forShadow =*/ true));

        assertStoredValues(Calls.CONTENT_URI,
                getTestCallLogValues(2),
                getTestCallLogValues(1),
                getTestCallLogValues(0));
        assertEquals(10, mCallLogProvider.getLastSyncTime(/* forShadow =*/ true));
        assertEquals(0, mCallLogProvider.getLastSyncTime(/* forShadow =*/ false));
    }

    public void testCopyEntriesFromCursor_DuplicatesIgnoredCorrectly() {
        mResolver.insert(Calls.CONTENT_URI, getTestCallLogValues(1));
        assertStoredValues(Calls.CONTENT_URI, getTestCallLogValues(1));

        assertEquals(10, mCallLogProvider.copyEntriesFromCursor(
                getTestCallLogCursor(), 5, /* forShadow =*/ false));

        assertStoredValues(Calls.CONTENT_URI,
                getTestCallLogValues(2),
                getTestCallLogValues(1),
                getTestCallLogValues(0));
        assertEquals(0, mCallLogProvider.getLastSyncTime(/* forShadow =*/ true));
        assertEquals(10, mCallLogProvider.getLastSyncTime(/* forShadow =*/ false));
    }

    public void testNullSubscriptionInfo() {
        PhoneAccountHandle handle = new PhoneAccountHandle(new ComponentName(
                TELEPHONY_PACKAGE, TELEPHONY_CLASS), TEST_PHONE_ACCOUNT_HANDLE_SUB_ID);
        when(mSubscriptionManager.getActiveSubscriptionInfo(eq(
                Integer.parseInt(TEST_PHONE_ACCOUNT_HANDLE_SUB_ID)))).thenReturn(null);

        mCallLogProvider.performBackgroundTask(
                BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES, handle);
    }

    private ContentValues getDefaultValues(int callType) {
        ContentValues values = new ContentValues();
        values.put(Calls.TYPE, callType);
        values.put(Calls.NUMBER, "1-800-4664-411");
        values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
        values.put(Calls.DATE, 1000);
        values.put(Calls.DURATION, 30);
        values.put(Calls.NEW, 1);
        return values;
    }

    private ContentValues getDefaultCallValues() {
        return getDefaultValues(Calls.INCOMING_TYPE);
    }

    private ContentValues getDefaultVoicemailValues() {
        return getDefaultValues(Calls.VOICEMAIL_TYPE);
    }

    private Uri insertCallRecord() {
        return mResolver.insert(Calls.CONTENT_URI, getDefaultCallValues());
    }

    private Uri insertVoicemailRecord() {
        return mResolver.insert(Calls.CONTENT_URI_WITH_VOICEMAIL, getDefaultVoicemailValues());
    }

    private Cursor getTestCallLogCursor() {
        final MatrixCursor cursor = new MatrixCursor(CallLogProvider.CALL_LOG_SYNC_PROJECTION);
        for (int i = 2; i >= 0; i--) {
            cursor.addRow(CommonDatabaseUtils.getArrayFromContentValues(getTestCallLogValues(i),
                    CallLogProvider.CALL_LOG_SYNC_PROJECTION));
        }
        return cursor;
    }

    /**
     * Returns a predefined {@link ContentValues} object based on the provided index.
     */
    private ContentValues getTestCallLogValues(int i) {
        ContentValues values = new ContentValues();
        values.put(Calls.ADD_FOR_ALL_USERS,1);
        switch (i) {
            case 0:
                values.put(Calls.NUMBER, "123456");
                values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
                values.put(Calls.TYPE, Calls.MISSED_TYPE);
                values.put(Calls.FEATURES, 0);
                values.put(Calls.DATE, 10);
                values.put(Calls.DURATION, 100);
                values.put(Calls.DATA_USAGE, 1000);
                values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, (String) null);
                values.put(Calls.PHONE_ACCOUNT_ID, (Long) null);
                values.put(Calls.PRIORITY, Calls.PRIORITY_NORMAL);
                break;
            case 1:
                values.put(Calls.NUMBER, "654321");
                values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
                values.put(Calls.TYPE, Calls.INCOMING_TYPE);
                values.put(Calls.FEATURES, 0);
                values.put(Calls.DATE, 5);
                values.put(Calls.DURATION, 200);
                values.put(Calls.DATA_USAGE, 0);
                values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, (String) null);
                values.put(Calls.PHONE_ACCOUNT_ID, (Long) null);
                values.put(Calls.PRIORITY, Calls.PRIORITY_NORMAL);
                break;
            case 2:
                values.put(Calls.NUMBER, "123456");
                values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
                values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
                values.put(Calls.FEATURES, Calls.FEATURES_VIDEO);
                values.put(Calls.DATE, 1);
                values.put(Calls.DURATION, 50);
                values.put(Calls.DATA_USAGE, 2000);
                values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, (String) null);
                values.put(Calls.PHONE_ACCOUNT_ID, (Long) null);
                values.put(Calls.PRIORITY, Calls.PRIORITY_URGENT);
                break;
        }
        return values;
    }
}
