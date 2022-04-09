/*
 * Copyright (C) 2022 The Android Open Source Project
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
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.Data;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@LargeTest
public class ContactsDatabaseMigrationTest extends FixedAndroidTestCase {

    public static final int NUM_ENTRIES_CONTACTS_DB_OLD_VERSION = 11;

    private void writeAssetFileToDisk(String assetName, File diskPath) throws IOException {
        final Context context = getTestContext();
        final byte[] BUF = new byte[1024 * 32];

        try (final InputStream input = context.getAssets().open(assetName)) {
            try (final OutputStream output = new FileOutputStream(diskPath)) {
                for (;;) {
                    final int len = input.read(BUF);
                    if (len == -1) {
                        break;
                    }
                    output.write(BUF, 0, len);
                }
            }
        }
    }

    /*
     * Test onUpgrade() step, check the IS_PHONE_ACCOUNT_MIGRATION_PENDING column is upgraded.
     */
    public void testPhoneAccountHandleMigrationMarkingOnUpgrade() throws IOException {
        final File sourceDbFile = getTestContext().getDatabasePath("contacts2.db");
        writeAssetFileToDisk(
                "phoneAccountHandleMigration/contacts2_oldversion.db", sourceDbFile);

        try (final SQLiteDatabase sourceDb = SQLiteDatabase.openDatabase(
                sourceDbFile.getAbsolutePath(), /* cursorFactory=*/ null,
                SQLiteDatabase.OPEN_READWRITE)) {

            final ContactsDatabaseHelper contactsDatabaseHelper = new ContactsDatabaseHelper(
                    getTestContext(), "contacts2.db", true, /* isTestInstance=*/ false);

            final SQLiteDatabase sqLiteDatabase = contactsDatabaseHelper.getReadableDatabase();

            // Check each entry in the Data has a new coloumn of
            // Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING that has a value of either 0 or 1
            assertEquals(NUM_ENTRIES_CONTACTS_DB_OLD_VERSION /** preconfigured entries */,
                    DatabaseUtils.longForQuery(sqLiteDatabase,
                            "select count(*) from " + ContactsDatabaseHelper.Tables.DATA
                                    + " where " + Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING
                                    + " >= 0", null));

            assertEquals(3 /** preconfigured entries for telephony component*/,
                    DatabaseUtils.longForQuery(sqLiteDatabase,
                            "select count(*) from " + ContactsDatabaseHelper.Tables.DATA
                                    + " where " + Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING
                                    + " == 1", null));

            assertEquals(8 /** preconfigured entries for no telephony component*/,
                    DatabaseUtils.longForQuery(sqLiteDatabase,
                            "select count(*) from " + ContactsDatabaseHelper.Tables.DATA
                                    + " where " + Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING
                                    + " == 0", null));
        }
    }

    /*
     * Test onCreate() step, check the IS_PHONE_ACCOUNT_MIGRATION_PENDING column is created
     * in the schema.
     */
    public void testPhoneAccountHandleMigrationOnCreate() throws IOException {
        final ContactsDatabaseHelper contactsDatabaseHelper = new ContactsDatabaseHelper(
                getTestContext(), null, true, /* isTestInstance=*/ false);

        final SQLiteDatabase sqLiteDatabase = contactsDatabaseHelper.getReadableDatabase();

        // Check there is a a new coloumn of Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING created
        // in the schema.
        assertEquals(0 /** 0 means no entries but the corresponding schema is created */,
                DatabaseUtils.longForQuery(sqLiteDatabase,
                        "select count(*) from " + ContactsDatabaseHelper.Tables.DATA
                                + " where " + Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING
                                + " >= 0", null));
    }
}