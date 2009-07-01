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
 * limitations under the License
 */

package com.android.providers.contacts2;

import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.os.Binder;
import android.provider.BaseColumns;
import android.provider.SocialContract.Activities;
import android.provider.ContactsContract.Accounts;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RestrictionExceptions;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Phone;

import android.telephony.PhoneNumberUtils;
import android.util.Log;

import java.util.HashMap;
import java.util.LinkedList;

import com.android.providers.contacts2.R;

/**
 * Database open helper for contacts and social activity data. Designed as a
 * singleton to make sure that all {@link android.content.ContentProvider} users get the same
 * reference. Provides handy methods for maintaining package and mime-type
 * lookup tables.
 */
/* package */ class OpenHelper extends SQLiteOpenHelper {
    private static final String TAG = "OpenHelper";

    private static final int DATABASE_VERSION = 34;
    private static final String DATABASE_NAME = "contacts2.db";
    private static final String DATABASE_PRESENCE = "presence_db";

    public interface Tables {
        public static final String ACCOUNTS = "accounts";
        public static final String AGGREGATES = "aggregates";
        public static final String CONTACTS = "contacts";
        public static final String PACKAGE = "package";
        public static final String MIMETYPE = "mimetype";
        public static final String PHONE_LOOKUP = "phone_lookup";
        public static final String NAME_LOOKUP = "name_lookup";
        public static final String AGGREGATION_EXCEPTIONS = "agg_exceptions";
        public static final String RESTRICTION_EXCEPTIONS = "rest_exceptions";
        public static final String CONTACT_OPTIONS = "contact_options";
        public static final String DATA = "data";
        public static final String PRESENCE = "presence";
        public static final String NICKNAME_LOOKUP = "nickname_lookup";

        public static final String AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE = "aggregates "
                + "LEFT OUTER JOIN presence ON (aggregates._id = presence.aggregate_id) "
                + "LEFT OUTER JOIN data ON (primary_phone_id = data._id)";

        public static final String DATA_JOIN_MIMETYPE = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

        public static final String DATA_JOIN_MIMETYPE_CONTACTS = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id) "
                + "LEFT OUTER JOIN contacts ON (data.contact_id = contacts._id)";

        public static final String DATA_JOIN_MIMETYPE_CONTACTS_PACKAGE = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id) "
                + "LEFT OUTER JOIN contacts ON (data.contact_id = contacts._id) "
                + "LEFT OUTER JOIN package ON (contacts.package_id = package._id)";

        public static final String DATA_JOIN_MIMETYPE_CONTACTS_PACKAGE_AGGREGATES = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id) "
                + "LEFT OUTER JOIN contacts ON (data.contact_id = contacts._id) "
                + "LEFT OUTER JOIN package ON (contacts.package_id = package._id) "
                + "LEFT OUTER JOIN aggregates ON (contacts.aggregate_id = aggregates._id)";

        public static final String ACTIVITIES = "activities";

        public static final String ACTIVITIES_JOIN_MIMETYPE = "activities "
                + "LEFT OUTER JOIN mimetype ON (activities.mimetype_id = mimetype._id)";

        public static final String ACTIVITIES_JOIN_MIMETYPE_CONTACTS_PACKAGE_AGGREGATES = "activities "
                + "LEFT OUTER JOIN mimetype ON (activities.mimetype_id = mimetype._id) "
                + "LEFT OUTER JOIN contacts ON (activities.author_contact_id = contacts._id) "
                + "LEFT OUTER JOIN package ON (contacts.package_id = package._id) "
                + "LEFT OUTER JOIN aggregates ON (contacts.aggregate_id = aggregates._id)";

        public static final String CONTACTS_JOIN_PACKAGE_ACCOUNTS = "contacts "
                + "LEFT OUTER JOIN package ON (contacts.package_id = package._id) "
                + "LEFT OUTER JOIN accounts ON (contacts.accounts_id = accounts._id)";

        public static final String NAME_LOOKUP_JOIN_CONTACTS = "name_lookup "
                + "LEFT OUTER JOIN contacts ON (name_lookup.contact_id = contacts._id)";

        public static final String AGGREGATION_EXCEPTIONS_JOIN_CONTACTS = "agg_exceptions "
                + "INNER JOIN contacts contacts1 "
                + "ON (agg_exceptions.contact_id1 = contacts1._id) ";

        public static final String AGGREGATION_EXCEPTIONS_JOIN_CONTACTS_TWICE = "agg_exceptions "
                + "INNER JOIN contacts contacts1 "
                + "ON (agg_exceptions.contact_id1 = contacts1._id) "
                + "INNER JOIN contacts contacts2 "
                + "ON (agg_exceptions.contact_id2 = contacts2._id) ";

        public static final String CONTACTS_JOIN_CONTACT_OPTIONS = "contacts "
                + "LEFT OUTER JOIN contact_options ON (contacts._id = contact_options._id)";
    }

    public interface Clauses {
        public static final String WHERE_IM_MATCHES = MimetypeColumns.MIMETYPE + "=" + Im.MIMETYPE
                + " AND " + Im.PROTOCOL + "=? AND " + Im.DATA + "=?";

        public static final String WHERE_EMAIL_MATCHES = MimetypeColumns.MIMETYPE + "="
                + Email.MIMETYPE + " AND " + Email.DATA + "=?";
    }

    public interface AggregatesColumns {
        public static final String OPTIMAL_PRIMARY_PHONE_ID = "optimal_phone_id";
        public static final String OPTIMAL_PRIMARY_PHONE_PACKAGE_ID = "optimal_phone_package_id";
        public static final String FALLBACK_PRIMARY_PHONE_ID = "fallback_phone_id";

        public static final String OPTIMAL_PRIMARY_EMAIL_ID = "optimal_email_id";
        public static final String OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID = "optimal_email_package_id";
        public static final String FALLBACK_PRIMARY_EMAIL_ID = "fallback_email_id";

        public static final String SINGLE_RESTRICTED_PACKAGE_ID = "single_restricted_package_id";
    }

    public interface ContactsColumns {
        public static final String PACKAGE_ID = "package_id";
    }

    public interface DataColumns {
        public static final String MIMETYPE_ID = "mimetype_id";
    }

    public interface ActivitiesColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String MIMETYPE_ID = "mimetype_id";
    }

    public interface PhoneLookupColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String DATA_ID = "data_id";
        public static final String CONTACT_ID = "contact_id";
        public static final String NORMALIZED_NUMBER = "normalized_number";
    }

    public interface NameLookupColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String CONTACT_ID = "contact_id";
        public static final String NORMALIZED_NAME = "normalized_name";
        public static final String NAME_TYPE = "name_type";
    }

    public interface NameLookupType {
        public static final int FULL_NAME = 0;
        public static final int FULL_NAME_CONCATENATED = 1;
        public static final int FULL_NAME_REVERSE = 2;
        public static final int FULL_NAME_REVERSE_CONCATENATED = 3;
        public static final int FULL_NAME_WITH_NICKNAME = 4;
        public static final int FULL_NAME_WITH_NICKNAME_REVERSE = 5;
        public static final int GIVEN_NAME_ONLY = 6;
        public static final int GIVEN_NAME_ONLY_AS_NICKNAME = 7;
        public static final int FAMILY_NAME_ONLY = 8;
        public static final int FAMILY_NAME_ONLY_AS_NICKNAME = 9;
        public static final int NICKNAME = 10;

        // This is the highest name lookup type code
        public static final int TYPE_COUNT = 10;
    }

    public interface PackageColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String PACKAGE = "package";
    }

    /* package */ interface MimetypeColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String MIMETYPE = "mimetype";
    }

    public interface AggregationExceptionColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String CONTACT_ID1 = "contact_id1";
        public static final String CONTACT_ID2 = "contact_id2";
    }

    public interface RestrictionExceptionsColumns {
        public static final String PACKAGE_PROVIDER_ID = "package_provider_id";
        public static final String PACKAGE_CLIENT_ID = "package_client_id";
    }

    public interface ContactOptionsColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String CUSTOM_RINGTONE = "custom_ringtone";
        public static final String SEND_TO_VOICEMAIL = "send_to_voicemail";
    }

    public interface NicknameLookupColumns {
        public static final String NAME = "name";
        public static final String CLUSTER = "cluster";
    }

    private static final String[] NICKNAME_LOOKUP_COLUMNS = new String[] {
        NicknameLookupColumns.CLUSTER
    };

    private static final int COL_NICKNAME_LOOKUP_CLUSTER = 0;

    /** In-memory cache of previously found mimetype mappings */
    private final HashMap<String, Long> mMimetypeCache = new HashMap<String, Long>();
    /** In-memory cache of previously found package name mappings */
    private final HashMap<String, Long> mPackageCache = new HashMap<String, Long>();


    /** Compiled statements for querying and inserting mappings */
    private SQLiteStatement mMimetypeQuery;
    private SQLiteStatement mPackageQuery;
    private SQLiteStatement mAggregateIdQuery;
    private SQLiteStatement mAggregateIdUpdate;
    private SQLiteStatement mMimetypeInsert;
    private SQLiteStatement mPackageInsert;
    private SQLiteStatement mNameLookupInsert;

    private SQLiteStatement mDataMimetypeQuery;
    private SQLiteStatement mActivitiesMimetypeQuery;

    private final Context mContext;
    private final RestrictionExceptionsCache mCache;
    private HashMap<String, String[]> mNicknameClusterCache;


    private static OpenHelper sSingleton = null;

    public static synchronized OpenHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new OpenHelper(context);
        }
        return sSingleton;
    }

    /**
     * Private constructor, callers except unit tests should obtain an instance through
     * {@link #getInstance(Context)} instead.
     */
    /* package */ OpenHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        Log.i(TAG, "Creating OpenHelper");

        mContext = context;
        mCache = new RestrictionExceptionsCache();
        mCache.loadFromDatabase(context, getReadableDatabase());
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        // Create compiled statements for package and mimetype lookups
        mMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns._ID + " FROM "
                + Tables.MIMETYPE + " WHERE " + MimetypeColumns.MIMETYPE + "=?");
        mPackageQuery = db.compileStatement("SELECT " + PackageColumns._ID + " FROM "
                + Tables.PACKAGE + " WHERE " + PackageColumns.PACKAGE + "=?");
        mAggregateIdQuery = db.compileStatement("SELECT " + Contacts.AGGREGATE_ID + " FROM "
                + Tables.CONTACTS + " WHERE " + Contacts._ID + "=?");
        mAggregateIdUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.AGGREGATE_ID + "=?" + " WHERE " + Contacts._ID + "=?");
        mMimetypeInsert = db.compileStatement("INSERT INTO " + Tables.MIMETYPE + "("
                + MimetypeColumns.MIMETYPE + ") VALUES (?)");
        mPackageInsert = db.compileStatement("INSERT INTO " + Tables.PACKAGE + "("
                + PackageColumns.PACKAGE + ") VALUES (?)");

        mDataMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns.MIMETYPE + " FROM "
                + Tables.DATA_JOIN_MIMETYPE + " WHERE " + Tables.DATA + "." + Data._ID + "=?");
        mActivitiesMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns.MIMETYPE
                + " FROM " + Tables.ACTIVITIES_JOIN_MIMETYPE + " WHERE " + Tables.ACTIVITIES + "."
                + Activities._ID + "=?");
        mNameLookupInsert = db.compileStatement("INSERT INTO " + Tables.NAME_LOOKUP + "("
                + NameLookupColumns.CONTACT_ID + "," + NameLookupColumns.NAME_TYPE + ","
                + NameLookupColumns.NORMALIZED_NAME + ") VALUES (?,?,?)");

        // Make sure we have an in-memory presence table
        final String tableName = DATABASE_PRESENCE + "." + Tables.PRESENCE;
        final String indexName = DATABASE_PRESENCE + ".presenceIndex";

        db.execSQL("ATTACH DATABASE ':memory:' AS " + DATABASE_PRESENCE + ";");
        db.execSQL("CREATE TABLE IF NOT EXISTS " + tableName + " ("+
                BaseColumns._ID + " INTEGER PRIMARY KEY," +
                Presence.AGGREGATE_ID + " INTEGER REFERENCES aggregates(_id)," +
                Presence.DATA_ID + " INTEGER REFERENCES data(_id)," +
                Presence.IM_PROTOCOL + " TEXT," +
                Presence.IM_HANDLE + " TEXT," +
                Presence.IM_ACCOUNT + " TEXT," +
                Presence.PRESENCE_STATUS + " INTEGER," +
                Presence.PRESENCE_CUSTOM_STATUS + " TEXT," +
                "UNIQUE(" + Presence.IM_PROTOCOL + ", " + Presence.IM_HANDLE + ", " + Presence.IM_ACCOUNT + ")" +
        ");");

        db.execSQL("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + Tables.PRESENCE + " ("
                + Presence.AGGREGATE_ID + ");");
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        Log.i(TAG, "Bootstrapping database");

        db.execSQL("CREATE TABLE " + Tables.ACCOUNTS + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Accounts.NAME + " TEXT," +
                Accounts.TYPE + " TEXT," +
                Accounts.DATA1 + " TEXT," +
                Accounts.DATA2 + " TEXT, " +
                Accounts.DATA3 + " TEXT, " +
                Accounts.DATA4 + " TEXT, " +
                Accounts.DATA5 + " TEXT, " +
                " UNIQUE(" + Accounts.NAME + ", " + Accounts.TYPE + ") " +
        ");");

        // One row per group of contacts corresponding to the same person
        db.execSQL("CREATE TABLE " + Tables.AGGREGATES + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Aggregates.DISPLAY_NAME + " TEXT," +
                Aggregates.TIMES_CONTACTED + " INTEGER," +
                Aggregates.LAST_TIME_CONTACTED + " INTEGER," +
                Aggregates.STARRED + " INTEGER," +
                AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID + " INTEGER REFERENCES data(_id)," +
                AggregatesColumns.OPTIMAL_PRIMARY_PHONE_PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID + " INTEGER REFERENCES data(_id)," +
                AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID + " INTEGER REFERENCES data(_id)," +
                AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID + " INTEGER REFERENCES data(_id)," +
                AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                Aggregates.PHOTO_ID + " INTEGER REFERENCES data(_id)," +
                Aggregates.CUSTOM_RINGTONE + " TEXT," +
                Aggregates.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0" +
        ");");

        // Contacts table
        db.execSQL("CREATE TABLE " + Tables.CONTACTS + " (" +
                Contacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                ContactsColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id) NOT NULL," +
                Contacts.IS_RESTRICTED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.ACCOUNTS_ID + " INTEGER REFERENCES accounts(_id)," +
                Contacts.SOURCE_ID + " TEXT," +
                Contacts.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                Contacts.DIRTY + " INTEGER NOT NULL DEFAULT 1," +
                Contacts.AGGREGATE_ID + " INTEGER " +
        ");");

        // Contact options table. It has the same primary key as the corresponding contact.
        db.execSQL("CREATE TABLE " + Tables.CONTACT_OPTIONS + " (" +
                ContactOptionsColumns._ID + " INTEGER PRIMARY KEY," +
                ContactOptionsColumns.CUSTOM_RINGTONE + " TEXT," +
                ContactOptionsColumns.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0" +
       ");");

        // Package name mapping table
        db.execSQL("CREATE TABLE " + Tables.PACKAGE + " (" +
                PackageColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                PackageColumns.PACKAGE + " TEXT NOT NULL" +
        ");");

        // Mime-type mapping table
        db.execSQL("CREATE TABLE " + Tables.MIMETYPE + " (" +
                MimetypeColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                MimetypeColumns.MIMETYPE + " TEXT NOT NULL" +
        ");");

        // Public generic data table
        db.execSQL("CREATE TABLE " + Tables.DATA + " (" +
                Data._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                DataColumns.MIMETYPE_ID + " INTEGER REFERENCES mimetype(_id) NOT NULL," +
                Data.CONTACT_ID + " INTEGER NOT NULL," +
                Data.IS_PRIMARY + " INTEGER NOT NULL DEFAULT 0," +
                Data.IS_SUPER_PRIMARY + " INTEGER NOT NULL DEFAULT 0," +
                Data.DATA_VERSION + " INTEGER NOT NULL DEFAULT 0," +
                Data.DATA1 + " TEXT," +
                Data.DATA2 + " TEXT," +
                Data.DATA3 + " TEXT," +
                Data.DATA4 + " TEXT," +
                Data.DATA5 + " TEXT," +
                Data.DATA6 + " TEXT," +
                Data.DATA7 + " TEXT," +
                Data.DATA8 + " TEXT," +
                Data.DATA9 + " TEXT," +
                Data.DATA10 + " TEXT" +
        ");");

        /**
         * set contact.dirty whenever the contact is updated and the new version does not explicity
         * clear the dirty flag
         *
         * Want to have a data row that has the server version of the contact. Then when I save
         * an entry from the server into the provider I will set the server version of the data
         * while also clearing the dirty flag of the contact.
         *
         * increment the contact.version whenever the contact is updated
         */
        db.execSQL("CREATE TRIGGER " + Tables.CONTACTS + "_updated1 "
                + "   BEFORE UPDATE ON " + Tables.CONTACTS
                + " BEGIN "
                + "   UPDATE " + Tables.CONTACTS
                + "     SET "
                +         Contacts.VERSION + "=OLD." + Contacts.VERSION + "+1, "
                +         Contacts.DIRTY + "=1"
                + "     WHERE " + Contacts._ID + "=OLD." + Contacts._ID + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.CONTACTS + "_deleted "
                + "   BEFORE DELETE ON " + Tables.CONTACTS
                + " BEGIN "
                + "   DELETE FROM " + Tables.DATA
                + "     WHERE " + Data.CONTACT_ID + "=OLD." + Contacts._ID + ";"
                + "   DELETE FROM " + Tables.PHONE_LOOKUP
                + "     WHERE " + PhoneLookupColumns.CONTACT_ID + "=OLD." + Contacts._ID + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_updated AFTER UPDATE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.DATA
                + "     SET " + Data.DATA_VERSION + "=OLD." + Data.DATA_VERSION + "+1 "
                + "     WHERE " + Data._ID + "=OLD." + Data._ID + ";"
                + "   UPDATE " + Tables.CONTACTS
                + "     SET " + Contacts.DIRTY + "=1"
                + "     WHERE " + Contacts._ID + "=OLD." + Contacts._ID + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_deleted BEFORE DELETE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.CONTACTS
                + "     SET " + Contacts.DIRTY + "=1"
                + "     WHERE " + Contacts._ID + "=OLD." + Contacts._ID + ";"
                + "   DELETE FROM " + Tables.PHONE_LOOKUP
                + "     WHERE " + PhoneLookupColumns.DATA_ID + "=OLD." + Data._ID + ";"
                + " END");

        // Private phone numbers table used for lookup
        db.execSQL("CREATE TABLE " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns._ID + " INTEGER PRIMARY KEY," +
                PhoneLookupColumns.DATA_ID + " INTEGER REFERENCES data(_id) NOT NULL," +
                PhoneLookupColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id) NOT NULL," +
                PhoneLookupColumns.NORMALIZED_NUMBER + " TEXT NOT NULL" +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.NORMALIZED_NUMBER + " ASC, " +
                PhoneLookupColumns.CONTACT_ID + ", " +
                PhoneLookupColumns.DATA_ID +
        ");");

        // Private name/nickname table used for lookup
        db.execSQL("CREATE TABLE " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                NameLookupColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id) NOT NULL," +
                NameLookupColumns.NORMALIZED_NAME + " TEXT," +
                NameLookupColumns.NAME_TYPE + " INTEGER" +
        ");");

        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + " ASC, " +
                NameLookupColumns.NAME_TYPE + " ASC, " +
                NameLookupColumns.CONTACT_ID +
        ");");

        db.execSQL("CREATE TABLE " + Tables.NICKNAME_LOOKUP + " (" +
                NicknameLookupColumns.NAME + " TEXT," +
                NicknameLookupColumns.CLUSTER + " TEXT" +
        ");");

        db.execSQL("CREATE UNIQUE INDEX nickname_lookup_index ON " + Tables.NICKNAME_LOOKUP + " (" +
                NicknameLookupColumns.NAME + ", " +
                NicknameLookupColumns.CLUSTER +
        ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                AggregationExceptions.TYPE + " INTEGER NOT NULL, " +
                AggregationExceptionColumns.CONTACT_ID1 + " INTEGER REFERENCES contacts(_id), " +
                AggregationExceptionColumns.CONTACT_ID2 + " INTEGER REFERENCES contacts(_id)" +
		");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index1 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns.CONTACT_ID1 + ", " +
                AggregationExceptionColumns.CONTACT_ID2 +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index2 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns.CONTACT_ID2 + ", " +
                AggregationExceptionColumns.CONTACT_ID1 +
        ");");

        // Restriction exceptions table
        db.execSQL("CREATE TABLE " + Tables.RESTRICTION_EXCEPTIONS + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                RestrictionExceptions.PACKAGE_PROVIDER + " TEXT NOT NULL, " +
                RestrictionExceptions.PACKAGE_CLIENT + " TEXT NOT NULL, " +
                RestrictionExceptionsColumns.PACKAGE_PROVIDER_ID + " INTEGER NOT NULL, " +
                RestrictionExceptionsColumns.PACKAGE_CLIENT_ID + " INTEGER NOT NULL" +
        ");");

        // Activities table
        db.execSQL("CREATE TABLE " + Tables.ACTIVITIES + " (" +
                Activities._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                ActivitiesColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id) NOT NULL," +
                ActivitiesColumns.MIMETYPE_ID + " INTEGER REFERENCES mimetype(_id) NOT NULL," +
                Activities.RAW_ID + " TEXT," +
                Activities.IN_REPLY_TO + " TEXT," +
                Activities.AUTHOR_CONTACT_ID +  " INTEGER REFERENCES contacts(_id)," +
                Activities.TARGET_CONTACT_ID + " INTEGER REFERENCES contacts(_id)," +
                Activities.PUBLISHED + " INTEGER NOT NULL," +
                Activities.THREAD_PUBLISHED + " INTEGER NOT NULL," +
                Activities.TITLE + " TEXT NOT NULL," +
                Activities.SUMMARY + " TEXT," +
                Activities.LINK + " TEXT, " +
                Activities.THUMBNAIL + " BLOB" +
        ");");

        loadNicknameLookupTable(db);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG, "Upgrading from version " + oldVersion + " to " + newVersion
                + ", data will be lost!");

        db.execSQL("DROP TABLE IF EXISTS " + Tables.ACCOUNTS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.AGGREGATES + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CONTACTS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.PACKAGE + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.MIMETYPE + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.DATA + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.NAME_LOOKUP + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.NICKNAME_LOOKUP + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.RESTRICTION_EXCEPTIONS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.ACTIVITIES + ";");

        // TODO: we should not be dropping agg_exceptions and contact_options. In case that table's
        // schema changes, we should try to preserve the data, because it was entered by the user
        // and has never been synched to the server.
        db.execSQL("DROP TABLE IF EXISTS " + Tables.AGGREGATION_EXCEPTIONS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CONTACT_OPTIONS + ";");

        onCreate(db);
    }

    /**
     * Wipes all data except mime type and package lookup tables.
     */
    public void wipeData() {
        SQLiteDatabase db = getWritableDatabase();
        db.execSQL("DELETE FROM " + Tables.AGGREGATES + ";");
        db.execSQL("DELETE FROM " + Tables.CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.CONTACT_OPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.DATA + ";");
        db.execSQL("DELETE FROM " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATION_EXCEPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.RESTRICTION_EXCEPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.ACTIVITIES + ";");

        // Note: we are not removing reference data from Tables.NICKNAME_LOOKUP

        db.execSQL("VACUUM;");
    }

    /**
     * Return the {@link ApplicationInfo#uid} for the given package name.
     */
    public static int getUidForPackageName(PackageManager pm, String packageName) {
        try {
            ApplicationInfo clientInfo = pm.getApplicationInfo(packageName, 0 /* no flags */);
            return clientInfo.uid;
        } catch (NameNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Perform an internal string-to-integer lookup using the compiled
     * {@link SQLiteStatement} provided, using the in-memory cache to speed up
     * lookups. If a mapping isn't found in cache or database, it will be
     * created. All new, uncached answers are added to the cache automatically.
     *
     * @param query Compiled statement used to query for the mapping.
     * @param insert Compiled statement used to insert a new mapping when no
     *            existing one is found in cache or from query.
     * @param value Value to find mapping for.
     * @param cache In-memory cache of previous answers.
     * @return An unique integer mapping for the given value.
     */
    private synchronized long getCachedId(SQLiteStatement query, SQLiteStatement insert,
            String value, HashMap<String, Long> cache) {
        // Try an in-memory cache lookup
        if (cache.containsKey(value)) {
            return cache.get(value);
        }

        long id = -1;
        try {
            // Try searching database for mapping
            DatabaseUtils.bindObjectToProgram(query, 1, value);
            id = query.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            // Nothing found, so try inserting new mapping
            DatabaseUtils.bindObjectToProgram(insert, 1, value);
            id = insert.executeInsert();
        }

        if (id != -1) {
            // Cache and return the new answer
            cache.put(value, id);
            return id;
        } else {
            // Otherwise throw if no mapping found or created
            throw new IllegalStateException("Couldn't find or create internal "
                    + "lookup table entry for value " + value);
        }
    }

    /**
     * Convert a package name into an integer, using {@link Tables#PACKAGE} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getPackageId(String packageName) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        return getCachedId(mPackageQuery, mPackageInsert, packageName, mPackageCache);
    }

    /**
     * Convert a mime-type into an integer, using {@link Tables#MIMETYPE} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getMimeTypeId(String mimetype) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        return getCachedId(mMimetypeQuery, mMimetypeInsert, mimetype, mMimetypeCache);
    }

    /**
     * Find the mime-type for the given {@link Data#_ID}.
     */
    public String getDataMimeType(long dataId) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        try {
            // Try database query to find mimetype
            DatabaseUtils.bindObjectToProgram(mDataMimetypeQuery, 1, dataId);
            String mimetype = mDataMimetypeQuery.simpleQueryForString();
            return mimetype;
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    /**
     * Find the mime-type for the given {@link Activities#_ID}.
     */
    public String getActivityMimeType(long activityId) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        try {
            // Try database query to find mimetype
            DatabaseUtils.bindObjectToProgram(mActivitiesMimetypeQuery, 1, activityId);
            String mimetype = mActivitiesMimetypeQuery.simpleQueryForString();
            return mimetype;
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    /**
     * Updates the aggregate ID for the specified contact.
     */
    public void setAggregateId(long contactId, long aggregateId) {
        getWritableDatabase();
        DatabaseUtils.bindObjectToProgram(mAggregateIdUpdate, 1, aggregateId);
        DatabaseUtils.bindObjectToProgram(mAggregateIdUpdate, 2, contactId);
        mAggregateIdUpdate.execute();
    }

    /**
     * Returns aggregate ID for the given contact or zero if it is NULL.
     */
    public long getAggregateId(long contactId) {
        getReadableDatabase();
        try {
            DatabaseUtils.bindObjectToProgram(mAggregateIdQuery, 1, contactId);
            return mAggregateIdQuery.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return -1
            return 0;
        }
    }

    /**
     * Inserts a record in the {@link Tables#NAME_LOOKUP} table.
     */
    public void insertNameLookup(long contactId, int lookupType, String name) {
        getWritableDatabase();
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 1, contactId);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 2, lookupType);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 3, name);
        mNameLookupInsert.executeInsert();
    }

    public static void buildPhoneLookupQuery(SQLiteQueryBuilder qb, final String number) {
        final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
        final StringBuilder tables = new StringBuilder();
        tables.append("contacts, (SELECT data_id FROM phone_lookup "
                + "WHERE (phone_lookup.normalized_number GLOB '");
        tables.append(normalizedNumber);
        tables.append("*')) AS lookup, " + Tables.DATA_JOIN_MIMETYPE);
        qb.setTables(tables.toString());
        qb.appendWhere("lookup.data_id=data._id AND data.contact_id=contacts._id AND ");
        qb.appendWhere("PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        qb.appendWhereEscapeString(number);
        qb.appendWhere(")");
    }


    /**
     * Loads common nickname mappings into the database.
     */
    private void loadNicknameLookupTable(SQLiteDatabase db) {
        String[] strings = mContext.getResources().getStringArray(R.array.common_nicknames);
        if (strings == null || strings.length == 0) {
            return;
        }

        SQLiteStatement nicknameLookupInsert = db.compileStatement("INSERT INTO "
                + Tables.NICKNAME_LOOKUP + "(" + NicknameLookupColumns.NAME + ","
                + NicknameLookupColumns.CLUSTER + ") VALUES (?,?)");

        for (int clusterId = 0; clusterId < strings.length; clusterId++) {
            String[] names = strings[clusterId].split(",");
            for (int j = 0; j < names.length; j++) {
                String name = NameNormalizer.normalize(names[j]);
                try {
                    DatabaseUtils.bindObjectToProgram(nicknameLookupInsert, 1, name);
                    DatabaseUtils.bindObjectToProgram(nicknameLookupInsert, 2,
                            String.valueOf(clusterId));
                    nicknameLookupInsert.executeInsert();
                } catch (SQLiteException e) {

                    // Print the exception and keep going - this is not a fatal error
                    Log.e(TAG, "Cannot insert nickname: " + names[j], e);
                }
            }
        }
    }

    /**
     * Returns common nickname cluster IDs for a given name. For example, it
     * will return the same value for "Robert", "Bob" and "Rob". Some names belong to multiple
     * clusters, e.g. Leo could be Leonard or Leopold.
     *
     * May return null.
     *
     * @param normalizedName A normalized first name, see {@link NameNormalizer#normalize}.
     */
    public String[] getCommonNicknameClusters(String normalizedName) {
        if (mNicknameClusterCache == null) {
            mNicknameClusterCache = new HashMap<String, String[]>();
        }

        synchronized (mNicknameClusterCache) {
            if (mNicknameClusterCache.containsKey(normalizedName)) {
                return mNicknameClusterCache.get(normalizedName);
            }
        }

        String[] clusters = null;
        SQLiteDatabase db = getReadableDatabase();

        Cursor cursor = db.query(Tables.NICKNAME_LOOKUP, NICKNAME_LOOKUP_COLUMNS,
                NicknameLookupColumns.NAME + "=?", new String[] { normalizedName },
                null, null, null);
        try {
            int count = cursor.getCount();
            if (count > 0) {
                clusters = new String[count];
                for (int i = 0; i < count; i++) {
                    cursor.moveToNext();
                    clusters[i] = cursor.getString(COL_NICKNAME_LOOKUP_CLUSTER);
                }
            }
        } finally {
            cursor.close();
        }

        synchronized (mNicknameClusterCache) {
            mNicknameClusterCache.put(normalizedName, clusters);
        }

        return clusters;
    }

    /**
     * Add a {@link RestrictionExceptions} record. This will update the
     * in-memory lookup table, and write to the database when needed. Any
     * callers should enforce that the {@link Binder#getCallingUid()} has the
     * authority to grant exceptions.
     */
    public void addRestrictionException(Context context, ContentValues values) {
        final PackageManager pm = context.getPackageManager();
        final SQLiteDatabase db = this.getWritableDatabase();

        // Read incoming package values and find lookup values
        final String packageProvider = values.getAsString(RestrictionExceptions.PACKAGE_PROVIDER);
        final String packageClient = values.getAsString(RestrictionExceptions.PACKAGE_CLIENT);
        final long packageProviderId = getPackageId(packageProvider);
        final long packageClientId = getPackageId(packageClient);

        // Find the client UID to update our internal lookup table and write the
        // exception to our database if we changed the in-memory cache.
        final int clientUid = getUidForPackageName(pm, packageClient);
        boolean cacheChanged = mCache.addException(packageProviderId, clientUid);
        if (cacheChanged) {
            values.put(RestrictionExceptionsColumns.PACKAGE_PROVIDER_ID, packageProviderId);
            values.put(RestrictionExceptionsColumns.PACKAGE_CLIENT_ID, packageClientId);
            values.remove(RestrictionExceptions.ALLOW_ACCESS);
            db.insert(Tables.RESTRICTION_EXCEPTIONS, null, values);
        }

    }

    /**
     * Remove a {@link RestrictionExceptions} record. This will update the
     * in-memory lookup table, and write to the database when needed. Any
     * callers should enforce that the {@link Binder#getCallingUid()} has the
     * authority to revoke exceptions.
     */
    public void removeRestrictionException(Context context, ContentValues values) {
        final PackageManager pm = context.getPackageManager();
        final SQLiteDatabase db = this.getWritableDatabase();

        // Read incoming package values and find lookup values
        final String packageProvider = values.getAsString(RestrictionExceptions.PACKAGE_PROVIDER);
        final String packageClient = values.getAsString(RestrictionExceptions.PACKAGE_CLIENT);
        final long packageProviderId = getPackageId(packageProvider);
        final long packageClientId = getPackageId(packageClient);

        // Find the client UID to update our internal lookup table and remove
        // the exception from our database if we changed the in-memory cache.
        final int clientUid = getUidForPackageName(pm, packageClient);
        final boolean cacheChanged = mCache.removeException(packageProviderId, clientUid);
        if (cacheChanged) {
            db.delete(Tables.RESTRICTION_EXCEPTIONS,
                    RestrictionExceptionsColumns.PACKAGE_PROVIDER_ID + "=" + packageProviderId
                            + " AND " + RestrictionExceptionsColumns.PACKAGE_CLIENT_ID + "="
                            + packageClientId, null);
        }

    }

    /**
     * Return the exception clause that should be used when running {@link Data}
     * queries that may be impacted by {@link Contacts#IS_RESTRICTED}. Will
     * return a clause of all of the provider packages that have granted
     * exceptions to the requested client UID.
     */
    public String getRestrictionExceptionClause(int clientUid, String column) {
        return mCache.getExceptionQueryClause(clientUid, column);
    }

    /**
     * Utility class to build a selection query clause that matches a specific
     * column against any one of the contained values. You must provide any
     * escaping of the field values yourself.
     */
    private static class MatchesClause<T> extends LinkedList<T> {
        private final HashMap<String, String> mCache = new HashMap<String, String>();

        private static final String JOIN_OR = " OR ";

        public synchronized boolean addMatch(T object) {
            mCache.clear();
            return super.add(object);
        }

        public synchronized void removeMatch(T object) {
            mCache.clear();
            super.remove(object);
        }

        /**
         * Return the query clause that would match the given column string to
         * any values added through {@link #addMatch(Object)}.
         */
        public synchronized String getQueryClause(String column, StringBuilder recycle) {
            // We maintain an internal cache for each requested column, and only
            // build the actual value when needed.
            String queryClause = mCache.get(column);
            final int size = this.size();
            if (queryClause == null && size > 0) {
                recycle.setLength(0);
                for (int i = 0; i < size; i++) {
                    recycle.append(column);
                    recycle.append("=");
                    recycle.append(this.get(i));
                    recycle.append(JOIN_OR);
                }

                // Trim off the last "OR" clause and store cached value.
                final int length = recycle.length();
                recycle.delete(length - JOIN_OR.length(), length);
                queryClause = recycle.toString();
                mCache.put(column, queryClause);
            }
            return queryClause;
        }
    }

    /**
     * Optimized in-memory cache for storing {@link RestrictionExceptions} that
     * have been read up from database. Helper methods indicate when an
     * exception change require writing to disk, and build query clauses for a
     * specific {@link RestrictionExceptions#PACKAGE_CLIENT}.
     */
    private static class RestrictionExceptionsCache extends HashMap<Integer, MatchesClause<Long>> {
        private final StringBuilder mBuilder = new StringBuilder();

        private static final String[] PROJ_RESTRICTION_EXCEPTIONS = new String[] {
                RestrictionExceptionsColumns.PACKAGE_PROVIDER_ID,
                RestrictionExceptions.PACKAGE_CLIENT,
        };

        private static final int COL_PACKAGE_PROVIDER_ID = 0;
        private static final int COL_PACKAGE_CLIENT = 1;

        public void loadFromDatabase(Context context, SQLiteDatabase db) {
            final PackageManager pm = context.getPackageManager();

            // Load all existing exceptions from our database.
            Cursor cursor = null;
            try {
                cursor = db.query(Tables.RESTRICTION_EXCEPTIONS, PROJ_RESTRICTION_EXCEPTIONS, null,
                        null, null, null, null);
                while (cursor.moveToNext()) {
                    // Read provider and client package details from database
                    final long packageProviderId = cursor.getLong(COL_PACKAGE_PROVIDER_ID);
                    final String clientPackage = cursor.getString(COL_PACKAGE_CLIENT);

                    try {
                        // Create exception entry for this client
                        final int clientUid = getUidForPackageName(pm, clientPackage);
                        addException(packageProviderId, clientUid);
                    } catch (RuntimeException e) {
                        Log.w(TAG, "Failed to grant restriction exception to " + clientPackage);
                        continue;
                    }

                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }

        /**
         * Lazily fetch a {@link MatchesClause} instance, creating a new one if
         * both needed and requested.
         */
        private MatchesClause<Long> getLazy(int clientUid, boolean create) {
            MatchesClause<Long> matchesClause = get(clientUid);
            if (matchesClause == null && create) {
                matchesClause = new MatchesClause<Long>();
                put(clientUid, matchesClause);
            }
            return matchesClause;
        }

        /**
         * Build a query clause that will allow the restriction exceptions
         * granted to a specific {@link Binder#getCallingUid()}.
         */
        public String getExceptionQueryClause(int clientUid, String column) {
            MatchesClause<Long> matchesClause = getLazy(clientUid, false);
            if (matchesClause != null) {
                return matchesClause.getQueryClause(column, mBuilder);
            } else {
                return null;
            }
        }

        /**
         * Add a {@link RestrictionExceptions} into the cache. Returns true if
         * this action resulted in the cache being changed.
         */
        public boolean addException(long packageProviderId, int clientUid) {
            MatchesClause<Long> matchesClause = getLazy(clientUid, true);
            if (matchesClause.contains(packageProviderId)) {
                return false;
            } else {
                matchesClause.addMatch(packageProviderId);
                return true;
            }
        }

        /**
         * Remove a {@link RestrictionExceptions} from the cache. Returns true if
         * this action resulted in the cache being changed.
         */
        public boolean removeException(long packageProviderId, int clientUid) {
            MatchesClause<Long> matchesClause = getLazy(clientUid, false);
            if (matchesClause == null || !matchesClause.contains(packageProviderId)) {
                return false;
            } else {
                matchesClause.removeMatch(packageProviderId);
                return true;
            }
        }
    }

}
