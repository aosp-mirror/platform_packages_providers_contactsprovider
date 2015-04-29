/*
 * Copyright (C) 2015 The Android Open Source Project
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

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.MetadataSync;
import android.provider.ContactsContract.RawContacts;
import android.test.suitebuilder.annotation.MediumTest;
import com.android.providers.contacts.ContactsDatabaseHelper.MetadataSyncColumns;
import com.android.providers.contacts.testutil.RawContactUtil;
import com.android.providers.contacts.testutil.TestUtil;

/**
 * Unit tests for {@link com.android.providers.contacts.ContactMetadataProvider}.
 * <p/>
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.ContactMetadataProviderTest -w \
 * com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class ContactMetadataProviderTest extends BaseContactsProvider2Test {
    private static String TEST_ACCOUNT_TYPE = "test_account_type";
    private static String TEST_ACCOUNT_NAME = "test_account_name";
    private static String TEST_DATA_SET = "test_data_set";
    private static String TEST_BACKUP_ID = "1001";
    private static String TEST_DATA = "test_data";
    private static String SELECTION_BY_TEST_ACCOUNT = MetadataSync.ACCOUNT_NAME + "='" +
            TEST_ACCOUNT_NAME + "' AND " + MetadataSync.ACCOUNT_TYPE + "='" + TEST_ACCOUNT_TYPE +
            "' AND " + MetadataSync.DATA_SET + "='" + TEST_DATA_SET + "'";

    private ContactMetadataProvider mContactMetadataProvider;
    private AccountWithDataSet mTestAccount;
    private ContentValues defaultValues;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mContactMetadataProvider = (ContactMetadataProvider) addProvider(
               ContactMetadataProvider.class, MetadataSync.METADATA_AUTHORITY);
        // Reset the dbHelper to be the one ContactsProvider2 is using. Before this, two providers
        // are using different dbHelpers.
        mContactMetadataProvider.setDatabaseHelper(((SynchronousContactsProvider2)
                mActor.provider).getDatabaseHelper(getContext()));
        setupData();
    }

    public void testInsertWithInvalidUri() {
        try {
            mResolver.insert(Uri.withAppendedPath(MetadataSync.METADATA_AUTHORITY_URI,
                    "metadata"), getDefaultValues());
            fail("the insert was expected to fail, but it succeeded");
        } catch (IllegalArgumentException e) {
            // this was expected
        }
    }

    public void testGetMetadataByAccount() {
        Cursor c = mResolver.query(MetadataSync.CONTENT_URI, null, SELECTION_BY_TEST_ACCOUNT,
                null, null);
        assertEquals(1, c.getCount());

        ContentValues expectedValues = defaultValues;
        expectedValues.remove(MetadataSyncColumns.ACCOUNT_ID);
        c.moveToFirst();
        assertCursorValues(c, expectedValues);
        c.close();
    }

    public void testReplaceMetadataForSameAccountIdAndBackupId() {
        //Insert a new metadata with same account and backupId as defaultValues, but different data
        //field.
        ContentValues  newValues =  new ContentValues();
        newValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        newValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        newValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        newValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, TEST_BACKUP_ID);
        newValues.put(MetadataSync.DATA, "new data");
        newValues.put(MetadataSync.DELETED, 0);
        mResolver.insert(MetadataSync.CONTENT_URI, newValues);

        // Total two metadata entries
        Cursor c = mResolver.query(MetadataSync.CONTENT_URI, null, null,
                null, null);
        assertEquals(2, c.getCount());

        // Only one metadata entry for TEST_ACCOUNT
        c = mResolver.query(MetadataSync.CONTENT_URI, null, SELECTION_BY_TEST_ACCOUNT, null, null);
        assertEquals(1, c.getCount());
        newValues.remove(MetadataSyncColumns.ACCOUNT_ID);
        c.moveToFirst();
        assertCursorValues(c, newValues);
        c.close();
    }

    private void setupData() {
        mTestAccount = new AccountWithDataSet(TEST_ACCOUNT_NAME, TEST_ACCOUNT_TYPE, TEST_DATA_SET);
        long rawContactId1 = RawContactUtil.createRawContactWithAccountDataSet(
                mResolver, mTestAccount);
        createAccount(TEST_ACCOUNT_NAME, TEST_ACCOUNT_TYPE, TEST_DATA_SET);
        mResolver.insert(MetadataSync.CONTENT_URI, getDefaultValues());

        // Insert another entry for another account
        createAccount("John", "account2", null);
        ContentValues values = new ContentValues();
        values.put(MetadataSync.ACCOUNT_NAME, "John");
        values.put(MetadataSync.ACCOUNT_TYPE, "account2");
        values.put(MetadataSync.RAW_CONTACT_BACKUP_ID, "1");
        values.put(MetadataSync.DATA, TEST_DATA);
        values.put(MetadataSync.DELETED, 0);
        mResolver.insert(MetadataSync.CONTENT_URI, values);
    }

    private ContentValues getDefaultValues() {
        defaultValues = new ContentValues();
        defaultValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        defaultValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        defaultValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        defaultValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, TEST_BACKUP_ID);
        defaultValues.put(MetadataSync.DATA, TEST_DATA);
        defaultValues.put(MetadataSync.DELETED, 0);
        return defaultValues;
    }
}
