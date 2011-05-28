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

import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.provider.BaseColumns;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.RawContacts;
import android.test.mock.MockContext;
import android.test.suitebuilder.annotation.MediumTest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link LegacyContactImporter}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.LegacyContactImporterTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 *
 * Note that this SHOULD be a large test, but had to be bumped down to medium due to a bug in the
 * SQLite cleanup code.
 */
@MediumTest
public class LegacyContactImporterTest extends BaseContactsProvider2Test {

    private static class LegacyMockContext extends MockContext {

        private String mFileName;

        public LegacyMockContext(String fileName) {
            mFileName = fileName;
        }

        @Override
        public SQLiteDatabase openOrCreateDatabase(String file, int mode,
                SQLiteDatabase.CursorFactory factory) {
            return SQLiteDatabase.openDatabase(mFileName, factory, SQLiteDatabase.OPEN_READONLY);
        }

        @Override
        public File getDatabasePath(String name) {
            return new File(mFileName);
        }
    }

    private LegacyMockContext createLegacyMockContext(String folder) throws IOException {
        Context context = getTestContext();
        File tempDb = new File(context.getFilesDir(), "legacy_contacts.db");
        if (tempDb.exists()) {
            tempDb.delete();
        }
        createSQLiteDatabaseFromDumpFile(tempDb.getPath(),
                new File(folder, "legacy_contacts.sql").getPath());
        return new LegacyMockContext(tempDb.getPath());
    }

    private void createSQLiteDatabaseFromDumpFile(String tempDbPath, String dumpFileAssetPath)
        throws IOException {

        final String[] ignoredTables = new String[] {"android_metadata", "sqlite_sequence"};

        Context context = getTestContext();
        SQLiteDatabase database = SQLiteDatabase.openOrCreateDatabase(tempDbPath, null);
        try {
            String data = readAssetAsString(dumpFileAssetPath);
            String[] commands = data.split(";\r|;\n|;\r\n");
            for (String command : commands) {
                boolean ignore = false;
                for (String ignoredTable : ignoredTables) {
                    if (command.contains(ignoredTable)) {
                        ignore = true;
                        break;
                    }
                }
                if (!ignore) {
                    database.execSQL(command);
                }
            }

            assertTrue(
                    "Database Version not set. Be sure to add " +
                    "'PRAGMA user_version = <number>;' to the SQL Script",
                    database.getVersion() != 0);
        } finally {
            database.close();
        }
    }

    @Override
    protected void setUp() throws Exception {
        SynchronousContactsProvider2.resetOpenHelper();
        super.setUp();
        addProvider(TestCallLogProvider.class, CallLog.AUTHORITY);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        SynchronousContactsProvider2.resetOpenHelper();
    }

    public void testContactUpgrade1() throws Exception {
        testAssetSet("test1");
    }

    public void testSyncedContactsUpgrade() throws Exception {
        testAssetSet("testSynced");
    }

    public void testUnsyncedContactsUpgrade() throws Exception {
        testAssetSet("testUnsynced");
    }

    private void testAssetSet(String folder) throws Exception {
        ContactsProvider2 provider = (ContactsProvider2)getProvider();
        LegacyContactImporter importer =
                new LegacyContactImporter(createLegacyMockContext(folder), provider);
        provider.importLegacyContacts(importer);

        assertQueryResults(folder + "/expected_groups.txt", Groups.CONTENT_URI, new String[]{
                Groups._ID,
                Groups.ACCOUNT_NAME,
                Groups.ACCOUNT_TYPE,
                Groups.DIRTY,
                Groups.GROUP_VISIBLE,
                Groups.NOTES,
                Groups.RES_PACKAGE,
                Groups.SOURCE_ID,
                Groups.SYSTEM_ID,
                Groups.TITLE,
                Groups.VERSION,
                Groups.SYNC1,
                Groups.SYNC2,
                Groups.SYNC3,
                Groups.SYNC4,
        });

        assertQueryResults(folder + "/expected_contacts.txt", Contacts.CONTENT_URI, new String[]{
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.PHOTO_ID,
                Contacts.TIMES_CONTACTED,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.CUSTOM_RINGTONE,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.STARRED,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
        });

        assertQueryResults(folder + "/expected_raw_contacts.txt", RawContacts.CONTENT_URI,
                new String[]{
                    RawContacts._ID,
                    RawContacts.ACCOUNT_NAME,
                    RawContacts.ACCOUNT_TYPE,
                    RawContacts.DELETED,
                    RawContacts.DIRTY,
                    RawContacts.SOURCE_ID,
                    RawContacts.VERSION,
                    RawContacts.SYNC1,
                    RawContacts.SYNC2,
                    RawContacts.SYNC3,
                    RawContacts.SYNC4,
                    RawContacts.DISPLAY_NAME_SOURCE,
                    RawContacts.DISPLAY_NAME_PRIMARY,
                    RawContacts.DISPLAY_NAME_ALTERNATIVE,
                    RawContacts.SORT_KEY_PRIMARY,
                    RawContacts.SORT_KEY_ALTERNATIVE,
        });

        assertQueryResults(folder + "/expected_data.txt", Data.CONTENT_URI, new String[]{
                Data._ID,
                Data.RAW_CONTACT_ID,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.DATA_VERSION,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
        });

        assertQueryResults(folder + "/expected_calls.txt", Calls.CONTENT_URI, new String[]{
                Calls._ID,
                Calls.NUMBER,
                Calls.DATE,
                Calls.DURATION,
                Calls.NEW,
                Calls.TYPE,
                Calls.CACHED_NAME,
                Calls.CACHED_NUMBER_LABEL,
                Calls.CACHED_NUMBER_TYPE,
        });

        provider.getDatabaseHelper().close();
    }

    private void assertQueryResults(String fileName, Uri uri, String[] projection)
            throws Exception {
        String expected = readAssetAsString(fileName).trim();
        String actual = dumpCursorToString(uri, projection).trim();
        assertEquals("Checking golden file " + fileName, expected, actual);
    }

    private String readAssetAsString(String fileName) throws IOException {
        Context context = getTestContext();
        InputStream input = context.getAssets().open(fileName);
        ByteArrayOutputStream contents = new ByteArrayOutputStream();
        int len;
        byte[] data = new byte[1024];
        do {
            len = input.read(data);
            if (len > 0) contents.write(data, 0, len);
        } while (len == data.length);
        return contents.toString();
    }

    private String dumpCursorToString(Uri uri, String[] projection) {
        Cursor c = mResolver.query(uri, projection, null, null, BaseColumns._ID);
        if (c == null) {
            return "Null cursor";
        }

        String cursorDump = DatabaseUtils.dumpCursorToString(c);
        c.close();
        return insertLineNumbers(cursorDump);
    }

    private String insertLineNumbers(String multiline) {
        String[] lines = multiline.split("\n");
        StringBuilder sb = new StringBuilder();

        // Ignore the first line that is a volatile header and the last line which is "<<<<<"
        for (int i = 1; i < lines.length - 1; i++) {
            sb.append(i).append(" ").append(lines[i]).append('\n');
        }
        return sb.toString();
    }


    public static class TestCallLogProvider extends CallLogProvider {
        private static ContactsDatabaseHelper mDbHelper;

        @Override
        protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
            if (mDbHelper == null) {
                mDbHelper = new ContactsDatabaseHelper(context);
            }
            return mDbHelper;
        }
    }
}
