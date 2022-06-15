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

import android.content.Context;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@LargeTest
public class CallLogMigrationTest extends FixedAndroidTestCase {

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

    public void testMigration() throws IOException {
        final File sourceDbFile = new File(getTestContext().getCacheDir(), "contacts2src.db");
        writeAssetFileToDisk("calllogmigration/contacts2.db", sourceDbFile);

        try (final SQLiteDatabase sourceDb = SQLiteDatabase.openDatabase(
                sourceDbFile.getAbsolutePath(), /* cursorFactory=*/ null,
                SQLiteDatabase.OPEN_READWRITE)) {

            // Make sure the source tables exist initially.
            assertTrue(CallLogDatabaseHelper.tableExists(sourceDb, "calls"));
            assertTrue(CallLogDatabaseHelper.tableExists(sourceDb, "voicemail_status"));

            // Create the calllog DB to perform the migration.
            final CallLogDatabaseHelperTestable dbh =
                    new CallLogDatabaseHelperTestable(getTestContext(), sourceDb);

            final SQLiteDatabase db = dbh.getReadableDatabase();

            // Check the content:
            // Note what we worry here is basically insertion error due to additional constraints,
            // renames, etc.  So here, we just check the number of rows and don't check the content.
            assertEquals(3, DatabaseUtils.longForQuery(db, "select count(*) from " +
                    CallLogDatabaseHelper.Tables.CALLS, null));

            assertEquals(2, DatabaseUtils.longForQuery(db, "select count(*) from " +
                    CallLogDatabaseHelper.Tables.VOICEMAIL_STATUS, null));

            assertEquals("123456",
                    dbh.getProperty(CallLogDatabaseHelper.DbProperties.CALL_LOG_LAST_SYNCED, ""));

            // Also, the source table should have been removed.
            assertFalse(CallLogDatabaseHelper.tableExists(sourceDb, "calls"));
            assertFalse(CallLogDatabaseHelper.tableExists(sourceDb, "voicemail_status"));

            assertEquals("1",
                    dbh.getProperty(CallLogDatabaseHelper.DbProperties.DATA_MIGRATED, ""));
        }
    }
}
