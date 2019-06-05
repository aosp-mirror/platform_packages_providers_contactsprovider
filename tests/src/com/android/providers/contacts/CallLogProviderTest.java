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

import com.android.internal.telephony.CallerInfo;
import com.android.internal.telephony.PhoneConstants;
import com.android.providers.contacts.CallLogDatabaseHelper.DbProperties;
import com.android.providers.contacts.testutil.CommonDatabaseUtils;

import android.content.ComponentName;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.VoicemailContract.Voicemails;
import android.telecom.PhoneAccountHandle;
import android.test.suitebuilder.annotation.MediumTest;

import java.util.Arrays;
import java.util.List;

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
    private static final int NUM_CALLLOG_FIELDS = 34;

    private CallLogProviderTestable mCallLogProvider;

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
    }

    @Override
    protected void tearDown() throws Exception {
        setUpWithVoicemailPermissions();
        mResolver.delete(Calls.CONTENT_URI_WITH_VOICEMAIL, null, null);
        setTimeForTest(null);
        super.tearDown();
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
        ci.name = "1-800-GOOG-411";
        ci.numberType = Phone.TYPE_CUSTOM;
        ci.numberLabel = "Directory";
        final ComponentName sComponentName = new ComponentName(
                "com.android.server.telecom",
                "TelecomServiceImpl");
        PhoneAccountHandle subscription = new PhoneAccountHandle(
                sComponentName, "sub0");

        Uri uri = Calls.addCall(ci, getMockContext(), "1-800-263-7643",
                PhoneConstants.PRESENTATION_ALLOWED, Calls.OUTGOING_TYPE, 0, subscription, 2000,
                40, null);
        assertNotNull(uri);
        assertEquals("0@" + CallLog.AUTHORITY, uri.getAuthority());

        ContentValues values = new ContentValues();
        values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
        values.put(Calls.FEATURES, 0);
        values.put(Calls.NUMBER, "1-800-263-7643");
        values.put(Calls.NUMBER_PRESENTATION, Calls.PRESENTATION_ALLOWED);
        values.put(Calls.DATE, 2000);
        values.put(Calls.DURATION, 40);
        // Cached values should not be updated immediately by the framework when inserting the call.
        values.put(Calls.CACHED_NAME, (String) null);
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
                break;
        }
        return values;
    }
}
