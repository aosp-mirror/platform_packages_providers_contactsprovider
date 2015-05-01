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

import android.content.ContentUris;
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
    private static String TEST_DATA_SET = "plus";
    private static String TEST_BACKUP_ID = "1001";
    private static String TEST_DATA = "{\n" +
            "  \"unique_contact_id\": {\n" +
            "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
            "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
            "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
            "    \"contact_id\": " + TEST_BACKUP_ID + ",\n" +
            "    \"data_set\": \"GOOGLE_PLUS\"\n" +
            "  },\n" +
            "  \"contact_prefs\": {\n" +
            "    \"send_to_voicemail\": true,\n" +
            "    \"starred\": true,\n" +
            "    \"pinned\": 2\n" +
            "  }\n" +
            "  }";

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

    public void testUpdateWithInvalidUri() {
        try {
            mResolver.update(Uri.withAppendedPath(MetadataSync.METADATA_AUTHORITY_URI,
                    "metadata"), getDefaultValues(), null, null);
            fail("the update was expected to fail, but it succeeded");
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

    public void testFailOnInsertMetadataForSameAccountIdAndBackupId() {
        // Insert a new metadata with same account and backupId as defaultValues should fail.
        String newData = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + TEST_BACKUP_ID + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": false,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";

        ContentValues  newValues =  new ContentValues();
        newValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        newValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        newValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        newValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, TEST_BACKUP_ID);
        newValues.put(MetadataSync.DATA, newData);
        newValues.put(MetadataSync.DELETED, 0);
        try {
            mResolver.insert(MetadataSync.CONTENT_URI, newValues);
        } catch (Exception e) {
            // Expected.
        }
    }

    public void testInsertAndUpdateMetadataSync() {
        // Create a raw contact with backupId.
        String backupId = "backupId10001";
        long rawContactId = RawContactUtil.createRawContactWithAccountDataSet(
                mResolver, mTestAccount);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        ContentValues values = new ContentValues();
        values.put(RawContacts.BACKUP_ID, backupId);
        assertEquals(1, mResolver.update(rawContactUri, values, null, null));

        assertStoredValue(rawContactUri, RawContacts._ID, rawContactId);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        assertStoredValue(rawContactUri, RawContacts.BACKUP_ID, backupId);
        assertStoredValue(rawContactUri, RawContacts.DATA_SET, TEST_DATA_SET);

        String deleted = "0";
        String insertJson = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 2\n" +
                "  }\n" +
                "  }";

        // Insert to MetadataSync table.
        ContentValues insertedValues = new ContentValues();
        insertedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        insertedValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        insertedValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        insertedValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        insertedValues.put(MetadataSync.DATA, insertJson);
        insertedValues.put(MetadataSync.DELETED, deleted);
        Uri metadataUri = mResolver.insert(MetadataSync.CONTENT_URI, insertedValues);

        long metadataId = ContentUris.parseId(metadataUri);
        assertEquals(true, metadataId > 0);

        // Check if RawContact table is updated  after inserting metadata.
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        assertStoredValue(rawContactUri, RawContacts.BACKUP_ID, backupId);
        assertStoredValue(rawContactUri, RawContacts.DATA_SET, TEST_DATA_SET);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "1");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "1");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "2");

        // Update the MetadataSync table.
        String updatedJson = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": false,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";
        ContentValues updatedValues = new ContentValues();
        updatedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        updatedValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        updatedValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        updatedValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        updatedValues.put(MetadataSync.DATA, updatedJson);
        updatedValues.put(MetadataSync.DELETED, deleted);
        assertEquals(1, mResolver.update(MetadataSync.CONTENT_URI, updatedValues, null, null));

        // Check if the update is correct.
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        assertStoredValue(rawContactUri, RawContacts.DATA_SET, TEST_DATA_SET);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "0");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "1");
    }

    public void testInsertMetadataWithoutUpdateTables() {
        // If raw contact doesn't exist, don't update raw contact tables.
        String backupId = "newBackupId";
        String deleted = "0";
        String insertJson = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 2\n" +
                "  }\n" +
                "  }";

        // Insert to MetadataSync table.
        ContentValues insertedValues = new ContentValues();
        insertedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        insertedValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        insertedValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        insertedValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        insertedValues.put(MetadataSync.DATA, insertJson);
        insertedValues.put(MetadataSync.DELETED, deleted);
        Uri metadataUri = mResolver.insert(MetadataSync.CONTENT_URI, insertedValues);

        long metadataId = ContentUris.parseId(metadataUri);
        assertEquals(true, metadataId > 0);
    }

    public void testFailUpdateDeletedMetadata() {
        String backupId = "backupId001";
        String newData = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": false,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";

        ContentValues  newValues =  new ContentValues();
        newValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        newValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        newValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        newValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        newValues.put(MetadataSync.DATA, newData);
        newValues.put(MetadataSync.DELETED, 1);

        try {
            mResolver.insert(MetadataSync.CONTENT_URI, newValues);
            fail("the update was expected to fail, but it succeeded");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void testInsertWithNullData() {
        ContentValues  newValues =  new ContentValues();
        String data = null;
        String backupId = "backupId002";
        newValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        newValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        newValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        newValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        newValues.put(MetadataSync.DATA, data);
        newValues.put(MetadataSync.DELETED, 0);

        try {
            mResolver.insert(MetadataSync.CONTENT_URI, newValues);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testUpdateWithNullData() {
        ContentValues  newValues =  new ContentValues();
        String data = null;
        newValues.put(MetadataSync.ACCOUNT_NAME, TEST_ACCOUNT_NAME);
        newValues.put(MetadataSync.ACCOUNT_TYPE, TEST_ACCOUNT_TYPE);
        newValues.put(MetadataSync.DATA_SET, TEST_DATA_SET);
        newValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, TEST_BACKUP_ID);
        newValues.put(MetadataSync.DATA, data);
        newValues.put(MetadataSync.DELETED, 0);

        try {
            mResolver.update(MetadataSync.CONTENT_URI, newValues, null, null);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testUpdateForMetadataSyncId() {
        Cursor c = mResolver.query(MetadataSync.CONTENT_URI, new String[] {MetadataSync._ID},
                SELECTION_BY_TEST_ACCOUNT, null, null);
        assertEquals(1, c.getCount());
        c.moveToNext();
        long metadataSyncId = c.getLong(0);
        c.close();

        Uri metadataUri = ContentUris.withAppendedId(MetadataSync.CONTENT_URI, metadataSyncId);
        ContentValues  newValues =  new ContentValues();
        String newData = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + TEST_ACCOUNT_TYPE + ",\n" +
                "    \"account_name\": " + TEST_ACCOUNT_NAME + ",\n" +
                "    \"contact_id\": " + TEST_BACKUP_ID + ",\n" +
                "    \"data_set\": \"GOOGLE_PLUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": false,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";
        newValues.put(MetadataSync.DATA, newData);
        newValues.put(MetadataSync.DELETED, 0);
        assertEquals(1, mResolver.update(metadataUri, newValues, null, null));
        assertStoredValue(metadataUri, MetadataSync.DATA, newData);
    }

    public void testDeleteMetadata() {
        //insert another metadata for TEST_ACCOUNT
        insertMetadata(TEST_ACCOUNT_NAME, TEST_ACCOUNT_TYPE, TEST_DATA_SET, "2", TEST_DATA, 0);
        Cursor c = mResolver.query(MetadataSync.CONTENT_URI, null, SELECTION_BY_TEST_ACCOUNT,
                null, null);
        assertEquals(2, c.getCount());
        int numOfDeletion = mResolver.delete(MetadataSync.CONTENT_URI, SELECTION_BY_TEST_ACCOUNT,
                null);
        assertEquals(2, numOfDeletion);
        c = mResolver.query(MetadataSync.CONTENT_URI, null, SELECTION_BY_TEST_ACCOUNT,
                null, null);
        assertEquals(0, c.getCount());
    }

    private void setupData() {
        mTestAccount = new AccountWithDataSet(TEST_ACCOUNT_NAME, TEST_ACCOUNT_TYPE, TEST_DATA_SET);
        long rawContactId1 = RawContactUtil.createRawContactWithAccountDataSet(
                mResolver, mTestAccount);
        createAccount(TEST_ACCOUNT_NAME, TEST_ACCOUNT_TYPE, TEST_DATA_SET);
        insertMetadata(getDefaultValues());

        // Insert another entry for another account
        createAccount("John", "account2", null);
        insertMetadata("John", "account2", null, "1", TEST_DATA, 0);
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

    private long insertMetadata(String accountName, String accountType, String dataSet, String
            backupId, String data, int deleted) {
        ContentValues values = new ContentValues();
        values.put(MetadataSync.ACCOUNT_NAME, accountName);
        values.put(MetadataSync.ACCOUNT_TYPE, accountType);
        values.put(MetadataSync.DATA_SET, dataSet);
        values.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        values.put(MetadataSync.DATA, data);
        values.put(MetadataSync.DELETED, deleted);
        return insertMetadata(values);
    }

    private long insertMetadata(ContentValues values) {
        return ContentUris.parseId(mResolver.insert(MetadataSync.CONTENT_URI, values));
    }
}
