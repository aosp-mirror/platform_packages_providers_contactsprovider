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

import static com.android.providers.contacts.CallLogDatabaseHelper.DATABASE_VERSION;

import android.content.ContentValues;
import android.content.Context;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.CallLog.Calls;
import android.provider.VoicemailContract;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

import com.android.providers.contacts.util.PhoneAccountHandleMigrationUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@LargeTest
public class CallLogMigrationTest extends FixedAndroidTestCase {
    private final static String TAG = CallLogMigrationTest.class.getSimpleName();

    // Maximum number for database version that need migration
    public static final int DATABASE_VERSION_NEED_MIGRATION = 10;
    // Component name for call log entries that don't need migration
    public static final String NO_MIGRATION_COMPONENT_NAME = "foo/bar";

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

    /** Insert a call log to db with specified phone account component name */
    private boolean insertCallLog(SQLiteDatabase db, String componentName) {
        final ContentValues values = new ContentValues();
        values.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, componentName);
        return db.insert(CallLogDatabaseHelper.Tables.CALLS, null, values) != -1;
    }

    /*
     * Test onUpgrade() step, check the IS_PHONE_ACCOUNT_MIGRATION_PENDING column is upgraded.
     */
    public void testPhoneAccountMigrationMarkingOnUpgrade() throws IOException {
        SQLiteDatabase db = new InMemoryCallLogProviderDbHelperV1(mContext,
                DATABASE_VERSION).getWritableDatabase();
        CallLogDatabaseHelperTestable testable = new CallLogDatabaseHelperTestable(
                getTestContext(), null);
        CallLogDatabaseHelper.OpenHelper openHelper = testable.getOpenHelper();
        // Insert 3 entries that 2 of its is_call_log_phone_account_migration_pending should be set
        // to 1
        assertTrue(insertCallLog(db, PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME));
        assertTrue(insertCallLog(db, PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME));
        assertTrue(insertCallLog(db, NO_MIGRATION_COMPONENT_NAME));

        openHelper.onUpgrade(db, DATABASE_VERSION_NEED_MIGRATION, DATABASE_VERSION);

        // Check each entry in the CALLS has a new coloumn of
        // Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING that has a value of either 0 or 1
        assertEquals(2, DatabaseUtils.longForQuery(
                db, "select count(*) from " + CallLogDatabaseHelper.Tables.CALLS
                        + " where " + Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING
                        + " = 1", null));
    }

    public static final class InMemoryCallLogProviderDbHelperV1 extends SQLiteOpenHelper {
        public InMemoryCallLogProviderDbHelperV1(Context context, int databaseVersion) {
            super(context,
                    null /* "null" DB name to make it an in-memory DB */,
                    null /* CursorFactory is null by default */,
                    databaseVersion);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            Log.d(TAG, "IN MEMORY DB CREATED");

            db.execSQL("CREATE TABLE " + CallLogDatabaseHelper.Tables.CALLS + " (" +
                    Calls._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Calls.NUMBER + " TEXT," +
                    Calls.NUMBER_PRESENTATION + " INTEGER NOT NULL DEFAULT ''," +
                    Calls.POST_DIAL_DIGITS + " TEXT NOT NULL DEFAULT ''," +
                    Calls.VIA_NUMBER + " TEXT NOT NULL DEFAULT ''," +
                    Calls.DATE + " INTEGER," +
                    Calls.DURATION + " INTEGER," +
                    Calls.DATA_USAGE + " INTEGER," +
                    Calls.TYPE + " INTEGER," +
                    Calls.FEATURES + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.PHONE_ACCOUNT_COMPONENT_NAME + " TEXT," +
                    Calls.PHONE_ACCOUNT_ID + " TEXT," +
                    Calls.PHONE_ACCOUNT_ADDRESS + " TEXT," +
                    Calls.PHONE_ACCOUNT_HIDDEN + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.SUB_ID + " INTEGER DEFAULT -1," +
                    Calls.NEW + " INTEGER," +
                    Calls.CACHED_NAME + " TEXT," +
                    Calls.CACHED_NUMBER_TYPE + " INTEGER," +
                    Calls.CACHED_NUMBER_LABEL + " TEXT," +
                    Calls.COUNTRY_ISO + " TEXT," +
                    Calls.VOICEMAIL_URI + " TEXT," +
                    Calls.IS_READ + " INTEGER," +
                    Calls.GEOCODED_LOCATION + " TEXT," +
                    Calls.CACHED_LOOKUP_URI + " TEXT," +
                    Calls.CACHED_MATCHED_NUMBER + " TEXT," +
                    Calls.CACHED_NORMALIZED_NUMBER + " TEXT," +
                    Calls.CACHED_PHOTO_ID + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.CACHED_PHOTO_URI + " TEXT," +
                    Calls.CACHED_FORMATTED_NUMBER + " TEXT," +
                    Calls.ADD_FOR_ALL_USERS + " INTEGER NOT NULL DEFAULT 1," +
                    Calls.LAST_MODIFIED + " INTEGER DEFAULT 0," +
                    Calls.CALL_SCREENING_COMPONENT_NAME + " TEXT," +
                    Calls.CALL_SCREENING_APP_NAME + " TEXT," +
                    Calls.BLOCK_REASON + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails._DATA + " TEXT," +
                    VoicemailContract.Voicemails.HAS_CONTENT + " INTEGER," +
                    VoicemailContract.Voicemails.MIME_TYPE + " TEXT," +
                    VoicemailContract.Voicemails.SOURCE_DATA + " TEXT," +
                    VoicemailContract.Voicemails.SOURCE_PACKAGE + " TEXT," +
                    VoicemailContract.Voicemails.TRANSCRIPTION + " TEXT," +
                    VoicemailContract.Voicemails.TRANSCRIPTION_STATE + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.STATE + " INTEGER," +
                    VoicemailContract.Voicemails.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.BACKED_UP + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.RESTORED + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.ARCHIVED + " INTEGER NOT NULL DEFAULT 0," +
                    VoicemailContract.Voicemails.IS_OMTP_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.MISSED_REASON + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.PRIORITY + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.SUBJECT + " TEXT," +
                    Calls.LOCATION + " TEXT," +
                    Calls.COMPOSER_PHOTO_URI + " TEXT" +
                    ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
    }
}
