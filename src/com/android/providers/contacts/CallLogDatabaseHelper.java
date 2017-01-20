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
 * limitations under the License
 */
package com.android.providers.contacts;

import android.annotation.Nullable;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.CallLog.Calls;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.util.PropertyUtils;

/**
 * SQLite database (helper) for {@link CallLogProvider} and {@link VoicemailContentProvider}.
 */
public class CallLogDatabaseHelper {
    private static final String TAG = "CallLogDatabaseHelper";

    private static final int DATABASE_VERSION = 4;

    private static final boolean DEBUG = false; // DON'T SUBMIT WITH TRUE

    private static final String DATABASE_NAME = "calllog.db";

    private static final String SHADOW_DATABASE_NAME = "calllog_shadow.db";

    private static CallLogDatabaseHelper sInstance;

    /** Instance for the "shadow" provider. */
    private static CallLogDatabaseHelper sInstanceForShadow;

    private final Context mContext;

    private final OpenHelper mOpenHelper;

    public interface Tables {
        String CALLS = "calls";
        String VOICEMAIL_STATUS = "voicemail_status";
    }

    public interface DbProperties {
        String CALL_LOG_LAST_SYNCED = "call_log_last_synced";
        String CALL_LOG_LAST_SYNCED_FOR_SHADOW = "call_log_last_synced_for_shadow";
        String DATA_MIGRATED = "migrated";
    }

    /**
     * Constants used in the contacts DB helper, which are needed for migration.
     *
     * DO NOT CHANCE ANY OF THE CONSTANTS.
     */
    private interface LegacyConstants {
        /** Table name used in the contacts DB.*/
        String CALLS_LEGACY = "calls";

        /** Table name used in the contacts DB.*/
        String VOICEMAIL_STATUS_LEGACY = "voicemail_status";

        /** Prop name used in the contacts DB.*/
        String CALL_LOG_LAST_SYNCED_LEGACY = "call_log_last_synced";
    }

    private final class OpenHelper extends SQLiteOpenHelper {
        public OpenHelper(Context context, String name, SQLiteDatabase.CursorFactory factory,
                int version) {
            super(context, name, factory, version);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            if (DEBUG) {
                Log.d(TAG, "onCreate");
            }

            PropertyUtils.createPropertiesTable(db);

            // *** NOTE ABOUT CHANGING THE DB SCHEMA ***
            //
            // The CALLS and VOICEMAIL_STATUS table used to be in the contacts2.db.  So we need to
            // migrate from these legacy tables, if exist, after creating the calllog DB, which is
            // done in migrateFromLegacyTables().
            //
            // This migration is slightly different from a regular upgrade step, because it's always
            // performed from the legacy schema (of the latest version -- because the migration
            // source is always the latest DB after all the upgrade steps) to the *latest* schema
            // at once.
            //
            // This means certain kind of changes are not doable without changing the
            // migration logic.  For example, if you rename a column in the DB, the migration step
            // will need to be updated to handle the column name change.

            db.execSQL("CREATE TABLE " + Tables.CALLS + " (" +
                    Calls._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Calls.NUMBER + " TEXT," +
                    Calls.NUMBER_PRESENTATION + " INTEGER NOT NULL DEFAULT " +
                    Calls.PRESENTATION_ALLOWED + "," +
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
                    Voicemails._DATA + " TEXT," +
                    Voicemails.HAS_CONTENT + " INTEGER," +
                    Voicemails.MIME_TYPE + " TEXT," +
                    Voicemails.SOURCE_DATA + " TEXT," +
                    Voicemails.SOURCE_PACKAGE + " TEXT," +
                    Voicemails.TRANSCRIPTION + " TEXT," +
                    Voicemails.STATE + " INTEGER," +
                    Voicemails.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.BACKED_UP + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.RESTORED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.ARCHIVED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.IS_OMTP_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0" +
                    ");");

            db.execSQL("CREATE TABLE " + Tables.VOICEMAIL_STATUS + " (" +
                    VoicemailContract.Status._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    VoicemailContract.Status.SOURCE_PACKAGE + " TEXT NOT NULL," +
                    VoicemailContract.Status.PHONE_ACCOUNT_COMPONENT_NAME + " TEXT," +
                    VoicemailContract.Status.PHONE_ACCOUNT_ID + " TEXT," +
                    VoicemailContract.Status.SETTINGS_URI + " TEXT," +
                    VoicemailContract.Status.VOICEMAIL_ACCESS_URI + " TEXT," +
                    VoicemailContract.Status.CONFIGURATION_STATE + " INTEGER," +
                    VoicemailContract.Status.DATA_CHANNEL_STATE + " INTEGER," +
                    VoicemailContract.Status.NOTIFICATION_CHANNEL_STATE + " INTEGER," +
                    VoicemailContract.Status.QUOTA_OCCUPIED + " INTEGER DEFAULT -1," +
                    VoicemailContract.Status.QUOTA_TOTAL + " INTEGER DEFAULT -1," +
                    VoicemailContract.Status.SOURCE_TYPE + " TEXT" +
                    ");");

            migrateFromLegacyTables(db);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            if (DEBUG) {
                Log.d(TAG, "onUpgrade");
            }

            if (oldVersion < 2) {
                upgradeToVersion2(db);
            }

            if (oldVersion < 3) {
                upgradeToVersion3(db);
            }

            if (oldVersion < 4) {
                upgradeToVersion4(db);
            }
        }
    }

    @VisibleForTesting
    CallLogDatabaseHelper(Context context, String databaseName) {
        mContext = context;
        mOpenHelper = new OpenHelper(mContext, databaseName, /* factory=*/ null, DATABASE_VERSION);
    }

    public static synchronized CallLogDatabaseHelper getInstance(Context context) {
        if (sInstance == null) {
            sInstance = new CallLogDatabaseHelper(context, DATABASE_NAME);
        }
        return sInstance;
    }

    public static synchronized CallLogDatabaseHelper getInstanceForShadow(Context context) {
        if (sInstanceForShadow == null) {
            // Shadow provider is always encryption-aware.
            sInstanceForShadow = new CallLogDatabaseHelper(
                    context.createDeviceProtectedStorageContext(), SHADOW_DATABASE_NAME);
        }
        return sInstanceForShadow;
    }

    public SQLiteDatabase getReadableDatabase() {
        return mOpenHelper.getReadableDatabase();
    }

    public SQLiteDatabase getWritableDatabase() {
        return mOpenHelper.getWritableDatabase();
    }

    public String getProperty(String key, String defaultValue) {
        return PropertyUtils.getProperty(getReadableDatabase(), key, defaultValue);
    }

    public void setProperty(String key, String value) {
        PropertyUtils.setProperty(getWritableDatabase(), key, value);
    }

    /**
     * Add the {@link Calls.VIA_NUMBER} Column to the CallLog Database.
     */
    private void upgradeToVersion2(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.CALLS + " ADD " + Calls.VIA_NUMBER +
                " TEXT NOT NULL DEFAULT ''");
    }

    /**
     * Add the {@link Status.SOURCE_TYPE} Column to the VoicemailStatus Database.
     */
    private void upgradeToVersion3(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.VOICEMAIL_STATUS + " ADD " + Status.SOURCE_TYPE +
                " TEXT");
    }

    /**
     * Add {@link Voicemails.BACKED_UP} {@link Voicemails.ARCHIVE} {@link
     * Voicemails.IS_OMTP_VOICEMAIL} column to the CallLog database.
     */
    private void upgradeToVersion4(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD backed_up INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE calls ADD restored INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE calls ADD archived INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE calls ADD is_omtp_voicemail INTEGER NOT NULL DEFAULT 0");
    }

    /**
     * Perform the migration from the contacts2.db (of the latest version) to the current calllog/
     * voicemail status tables.
     */
    private void migrateFromLegacyTables(SQLiteDatabase calllog) {
        final SQLiteDatabase contacts = getContactsWritableDatabaseForMigration();

        if (contacts == null) {
            Log.w(TAG, "Contacts DB == null, skipping migration. (running tests?)");
            return;
        }
        if (DEBUG) {
            Log.d(TAG, "migrateFromLegacyTables");
        }

        if ("1".equals(PropertyUtils.getProperty(calllog, DbProperties.DATA_MIGRATED, ""))) {
            return;
        }

        Log.i(TAG, "Migrating from old tables...");

        contacts.beginTransaction();
        try {
            if (!tableExists(contacts, LegacyConstants.CALLS_LEGACY)
                    || !tableExists(contacts, LegacyConstants.VOICEMAIL_STATUS_LEGACY)) {
                // This is fine on new devices. (or after a "clear data".)
                Log.i(TAG, "Source tables don't exist.");
                return;
            }
            calllog.beginTransaction();
            try {

                final ContentValues cv = new ContentValues();

                try (Cursor source = contacts.rawQuery(
                        "SELECT * FROM " + LegacyConstants.CALLS_LEGACY, null)) {
                    while (source.moveToNext()) {
                        cv.clear();

                        DatabaseUtils.cursorRowToContentValues(source, cv);

                        calllog.insertOrThrow(Tables.CALLS, null, cv);
                    }
                }

                try (Cursor source = contacts.rawQuery("SELECT * FROM " +
                        LegacyConstants.VOICEMAIL_STATUS_LEGACY, null)) {
                    while (source.moveToNext()) {
                        cv.clear();

                        DatabaseUtils.cursorRowToContentValues(source, cv);

                        calllog.insertOrThrow(Tables.VOICEMAIL_STATUS, null, cv);
                    }
                }

                contacts.execSQL("DROP TABLE " + LegacyConstants.CALLS_LEGACY + ";");
                contacts.execSQL("DROP TABLE " + LegacyConstants.VOICEMAIL_STATUS_LEGACY + ";");

                // Also copy the last sync time.
                PropertyUtils.setProperty(calllog, DbProperties.CALL_LOG_LAST_SYNCED,
                        PropertyUtils.getProperty(contacts,
                                LegacyConstants.CALL_LOG_LAST_SYNCED_LEGACY, null));

                Log.i(TAG, "Migration completed.");

                calllog.setTransactionSuccessful();
            } finally {
                calllog.endTransaction();
            }

            contacts.setTransactionSuccessful();
        } catch (RuntimeException e) {
            // We don't want to be stuck here, so we just swallow exceptions...
            Log.w(TAG, "Exception caught during migration", e);
        } finally {
            contacts.endTransaction();
        }
        PropertyUtils.setProperty(calllog, DbProperties.DATA_MIGRATED, "1");
    }

    @VisibleForTesting
    static boolean tableExists(SQLiteDatabase db, String table) {
        return DatabaseUtils.longForQuery(db,
                "select count(*) from sqlite_master where type='table' and name=?",
                new String[] {table}) > 0;
    }

    @VisibleForTesting
    @Nullable // We return null during tests when migration is not needed.
    SQLiteDatabase getContactsWritableDatabaseForMigration() {
        return ContactsDatabaseHelper.getInstance(mContext).getWritableDatabase();
    }

    @VisibleForTesting
    void closeForTest() {
        mOpenHelper.close();
    }

    public void wipeForTest() {
        getWritableDatabase().execSQL("DELETE FROM " + Tables.CALLS);
    }
}
