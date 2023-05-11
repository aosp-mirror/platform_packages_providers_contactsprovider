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
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteCantOpenDatabaseException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.preference.PreferenceManager;
import android.provider.CallLog.Calls;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.text.TextUtils;
import android.util.ArraySet;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.util.PhoneAccountHandleMigrationUtils;
import com.android.providers.contacts.util.PropertyUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQLite database (helper) for {@link CallLogProvider} and {@link VoicemailContentProvider}.
 */
public class CallLogDatabaseHelper {
    private static final String TAG = "CallLogDatabaseHelper";

    @VisibleForTesting
    static final int DATABASE_VERSION = 11;

    private static final boolean DEBUG = false; // DON'T SUBMIT WITH TRUE

    private static final String DATABASE_NAME = "calllog.db";

    private static final String SHADOW_DATABASE_NAME = "calllog_shadow.db";

    private static final int IDLE_CONNECTION_TIMEOUT_MS = 30000;

    private static CallLogDatabaseHelper sInstance;

    /** Instance for the "shadow" provider. */
    private static CallLogDatabaseHelper sInstanceForShadow;

    private final Context mContext;

    private final OpenHelper mOpenHelper;

    @VisibleForTesting
    final PhoneAccountHandleMigrationUtils mPhoneAccountHandleMigrationUtils;

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
    public interface LegacyConstants {
        /** Table name used in the contacts DB.*/
        String CALLS_LEGACY = "calls";

        /** Table name used in the contacts DB.*/
        String VOICEMAIL_STATUS_LEGACY = "voicemail_status";

        /** Prop name used in the contacts DB.*/
        String CALL_LOG_LAST_SYNCED_LEGACY = "call_log_last_synced";
    }

    @VisibleForTesting
    public class OpenHelper extends SQLiteOpenHelper {
        public OpenHelper(Context context, String name, SQLiteDatabase.CursorFactory factory,
                int version) {
            super(context, name, factory, version);
            // Memory optimization - close idle connections after 30s of inactivity
            setIdleConnectionTimeout(IDLE_CONNECTION_TIMEOUT_MS);
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
                    Calls.CALL_SCREENING_COMPONENT_NAME + " TEXT," +
                    Calls.CALL_SCREENING_APP_NAME + " TEXT," +
                    Calls.BLOCK_REASON + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.MISSED_REASON + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.PRIORITY + " INTEGER NOT NULL DEFAULT 0," +
                    Calls.SUBJECT + " TEXT," +
                    Calls.LOCATION + " TEXT," +
                    Calls.COMPOSER_PHOTO_URI + " TEXT," +
                    Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails._DATA + " TEXT," +
                    Voicemails.HAS_CONTENT + " INTEGER," +
                    Voicemails.MIME_TYPE + " TEXT," +
                    Voicemails.SOURCE_DATA + " TEXT," +
                    Voicemails.SOURCE_PACKAGE + " TEXT," +
                    Voicemails.TRANSCRIPTION + " TEXT," +
                    Voicemails.TRANSCRIPTION_STATE + " INTEGER NOT NULL DEFAULT 0," +
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

            if (oldVersion < 5) {
                upgradeToVersion5(db);
            }

            if (oldVersion < 6) {
                upgradeToVersion6(db);
            }

            if (oldVersion < 7) {
                upgradeToVersion7(db);
            }

            if (oldVersion < 8) {
                upgradetoVersion8(db);
            }

            if (oldVersion < 9) {
                upgradeToVersion9(db);
            }

            if (oldVersion < 10) {
                upgradeToVersion10(db);
            }

            if (oldVersion < 11) {
                upgradeToVersion11(db);
            }
        }

        @Override
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // Ignore
        }
    }

    @VisibleForTesting
    CallLogDatabaseHelper(Context context, String databaseName) {
        mContext = context;
        mPhoneAccountHandleMigrationUtils = new PhoneAccountHandleMigrationUtils(
                context, PhoneAccountHandleMigrationUtils.TYPE_CALL_LOG);
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
     * Updates phone account migration pending status, indicating if there is any phone account
     * handle that need to migrate. Called in CallLogProvider.
     */
    void updatePhoneAccountHandleMigrationPendingStatus() {
        mPhoneAccountHandleMigrationUtils.updatePhoneAccountHandleMigrationPendingStatus(
                getWritableDatabase());
    }

    /**
     * Migrate all the pending phone account handles based on the given iccId and subId. Used
     * by CallLogProvider.
     */
    void migratePendingPhoneAccountHandles(String iccId, String subId) {
        mPhoneAccountHandleMigrationUtils.migratePendingPhoneAccountHandles(
                iccId, subId, getWritableDatabase());
    }

    /**
     * Try to migrate any PhoneAccountId to SubId from IccId. Used by CallLogProvider.
     */
    void migrateIccIdToSubId() {
        mPhoneAccountHandleMigrationUtils.migrateIccIdToSubId(getWritableDatabase());
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
     * Add {@link Voicemails.TRANSCRIPTION_STATE} column to the CallLog database.
     */
    private void upgradeToVersion5(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD transcription_state INTEGER NOT NULL DEFAULT 0");
    }

    /**
     * Add {@link CallLog.Calls#CALL_SCREENING_COMPONENT_NAME}
     * {@link CallLog.Calls#CALL_SCREENING_APP_NAME}
     * {@link CallLog.Calls#BLOCK_REASON} column to the CallLog database.
     */
    private void upgradeToVersion6(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD call_screening_component_name TEXT");
        db.execSQL("ALTER TABLE calls ADD call_screening_app_name TEXT");
        db.execSQL("ALTER TABLE calls ADD block_reason INTEGER NOT NULL DEFAULT 0");
    }

    /**
     * Add {@code android.telecom.CallIdentification} columns; these are destined to be removed
     * in {@link #upgradetoVersion8(SQLiteDatabase)}.
     * @param db DB to upgrade
     */
    private void upgradeToVersion7(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD call_id_package_name TEXT NULL");
        db.execSQL("ALTER TABLE calls ADD call_id_app_name TEXT NULL");
        db.execSQL("ALTER TABLE calls ADD call_id_name TEXT NULL");
        db.execSQL("ALTER TABLE calls ADD call_id_description TEXT NULL");
        db.execSQL("ALTER TABLE calls ADD call_id_details TEXT NULL");
        db.execSQL("ALTER TABLE calls ADD call_id_nuisance_confidence INTEGER NULL");
    }

    /**
     * Remove the {@code android.telecom.CallIdentification} column.
     * @param db DB to upgrade
     */
    private void upgradetoVersion8(SQLiteDatabase db) {
        db.beginTransaction();
        try {
            String oldTable = Tables.CALLS + "_old";
            // SQLite3 doesn't support altering a column name, so we'll rename the old calls table..
            db.execSQL("ALTER TABLE calls RENAME TO " + oldTable);

            // ... create a new one (yes, this seems similar to what is in onCreate, but we can't
            // assume that one won't change in the future) ...
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
                    Calls.CALL_SCREENING_COMPONENT_NAME + " TEXT," +
                    Calls.CALL_SCREENING_APP_NAME + " TEXT," +
                    Calls.BLOCK_REASON + " INTEGER NOT NULL DEFAULT 0," +

                    Voicemails._DATA + " TEXT," +
                    Voicemails.HAS_CONTENT + " INTEGER," +
                    Voicemails.MIME_TYPE + " TEXT," +
                    Voicemails.SOURCE_DATA + " TEXT," +
                    Voicemails.SOURCE_PACKAGE + " TEXT," +
                    Voicemails.TRANSCRIPTION + " TEXT," +
                    Voicemails.TRANSCRIPTION_STATE + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.STATE + " INTEGER," +
                    Voicemails.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.BACKED_UP + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.RESTORED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.ARCHIVED + " INTEGER NOT NULL DEFAULT 0," +
                    Voicemails.IS_OMTP_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0" +
                    ");");

            String allTheColumns = Calls._ID + ", " +
                    Calls.NUMBER + ", " +
                    Calls.NUMBER_PRESENTATION + ", " +
                    Calls.POST_DIAL_DIGITS + ", " +
                    Calls.VIA_NUMBER + ", " +
                    Calls.DATE + ", " +
                    Calls.DURATION + ", " +
                    Calls.DATA_USAGE + ", " +
                    Calls.TYPE + ", " +
                    Calls.FEATURES + ", " +
                    Calls.PHONE_ACCOUNT_COMPONENT_NAME + ", " +
                    Calls.PHONE_ACCOUNT_ID + ", " +
                    Calls.PHONE_ACCOUNT_ADDRESS + ", " +
                    Calls.PHONE_ACCOUNT_HIDDEN + ", " +
                    Calls.SUB_ID + ", " +
                    Calls.NEW + ", " +
                    Calls.CACHED_NAME + ", " +
                    Calls.CACHED_NUMBER_TYPE + ", " +
                    Calls.CACHED_NUMBER_LABEL + ", " +
                    Calls.COUNTRY_ISO + ", " +
                    Calls.VOICEMAIL_URI + ", " +
                    Calls.IS_READ + ", " +
                    Calls.GEOCODED_LOCATION + ", " +
                    Calls.CACHED_LOOKUP_URI + ", " +
                    Calls.CACHED_MATCHED_NUMBER + ", " +
                    Calls.CACHED_NORMALIZED_NUMBER + ", " +
                    Calls.CACHED_PHOTO_ID + ", " +
                    Calls.CACHED_PHOTO_URI + ", " +
                    Calls.CACHED_FORMATTED_NUMBER + ", " +
                    Calls.ADD_FOR_ALL_USERS + ", " +
                    Calls.LAST_MODIFIED + ", " +
                    Calls.CALL_SCREENING_COMPONENT_NAME + ", " +
                    Calls.CALL_SCREENING_APP_NAME + ", " +
                    Calls.BLOCK_REASON + ", " +

                    Voicemails._DATA + ", " +
                    Voicemails.HAS_CONTENT + ", " +
                    Voicemails.MIME_TYPE + ", " +
                    Voicemails.SOURCE_DATA + ", " +
                    Voicemails.SOURCE_PACKAGE + ", " +
                    Voicemails.TRANSCRIPTION + ", " +
                    Voicemails.TRANSCRIPTION_STATE + ", " +
                    Voicemails.STATE + ", " +
                    Voicemails.DIRTY + ", " +
                    Voicemails.DELETED + ", " +
                    Voicemails.BACKED_UP + ", " +
                    Voicemails.RESTORED + ", " +
                    Voicemails.ARCHIVED + ", " +
                    Voicemails.IS_OMTP_VOICEMAIL;

            // .. so we insert into the new table all the values from the old table ...
            db.execSQL("INSERT INTO " + Tables.CALLS + " (" +
                    allTheColumns + ") SELECT " +
                    allTheColumns + " FROM " + oldTable);

            // .. and drop the old table we renamed.
            db.execSQL("DROP TABLE " + oldTable);

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    private void upgradeToVersion9(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD missed_reason INTEGER NOT NULL DEFAULT 0");
    }

    private void upgradeToVersion10(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD priority INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE calls ADD subject TEXT");
        db.execSQL("ALTER TABLE calls ADD location TEXT");
        db.execSQL("ALTER TABLE calls ADD composer_photo_uri TEXT");
    }

    private void upgradeToVersion11(SQLiteDatabase db) {
        // Create colums for IS_PHONE_ACCOUNT_MIGRATION_PENDING
        db.execSQL("ALTER TABLE calls ADD is_call_log_phone_account_migration_pending"
                + " INTEGER NOT NULL DEFAULT 0");
        mPhoneAccountHandleMigrationUtils.markAllTelephonyPhoneAccountsPendingMigration(db);
        mPhoneAccountHandleMigrationUtils.migrateIccIdToSubId(db);
    }

    @VisibleForTesting
    static boolean tableExists(SQLiteDatabase db, String table) {
        return DatabaseUtils.longForQuery(db,
                "select count(*) from sqlite_master where type='table' and name=?",
                new String[] {table}) > 0;
    }

    @VisibleForTesting
    @Nullable // We return null during tests when migration is not needed or database
              // is unavailable.
    SQLiteDatabase getContactsWritableDatabaseForMigration() {
        try {
            return ContactsDatabaseHelper.getInstance(mContext).getWritableDatabase();
        } catch (SQLiteCantOpenDatabaseException e) {
            Log.i(TAG, "Exception caught during opening database for migration: " + e);
            return null;
        }
    }

    public PhoneAccountHandleMigrationUtils getPhoneAccountHandleMigrationUtils() {
        return mPhoneAccountHandleMigrationUtils;
    }

    public ArraySet<String> selectDistinctColumn(String table, String column) {
        final ArraySet<String> ret = new ArraySet<>();
        final SQLiteDatabase db = getReadableDatabase();
        final Cursor c = db.rawQuery("SELECT DISTINCT "
                + column
                + " FROM " + table, null);
        try {
            c.moveToPosition(-1);
            while (c.moveToNext()) {
                if (c.isNull(0)) {
                    continue;
                }
                final String s = c.getString(0);

                if (!TextUtils.isEmpty(s)) {
                    ret.add(s);
                }
            }
            return ret;
        } finally {
            c.close();
        }
    }

    @VisibleForTesting
    void closeForTest() {
        mOpenHelper.close();
    }

    public void wipeForTest() {
        getWritableDatabase().execSQL("DELETE FROM " + Tables.CALLS);
    }

    @VisibleForTesting
    OpenHelper getOpenHelper() {
        return mOpenHelper;
    }
}
