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

import android.content.Context;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.SocialContract.Activities;
import android.provider.ContactsContract.Accounts;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.Im.PresenceColumns;

import android.util.Log;

import java.util.HashMap;

/**
 * Database open helper for contacts and social activity data. Designed as a
 * singleton to make sure that all {@link android.content.ContentProvider} users get the same
 * reference. Provides handy methods for maintaining package and mime-type
 * lookup tables.
 */
/* package */ class OpenHelper extends SQLiteOpenHelper {
    private static final String TAG = "OpenHelper";

    private static final int DATABASE_VERSION = 23;
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

        public static final String DATA = "data";
        public static final String PRESENCE = "presence";

        public static final String AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE = "aggregates "
                + "LEFT OUTER JOIN presence ON (aggregate._id = presence.aggregate_id) "
                + "LEFT OUTER JOIN data ON (aggregates.primary_phone_id = data._id)";

        public static final String DATA_JOIN_MIMETYPE = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

        public static final String DATA_JOIN_PACKAGE_MIMETYPE = "data "
                + "LEFT OUTER JOIN package ON (data.package_id = package._id) "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

        public static final String DATA_JOIN_PACKAGE_MIMETYPE_CONTACTS = "data "
                + "LEFT OUTER JOIN package ON (data.package_id = package._id) "
                + "LEFT JOIN contacts ON (data.contact_id = contacts._id) "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

        public static final String DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE = "data "
                + "LEFT OUTER JOIN package ON (data.package_id = package._id) "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id) "
                + "LEFT JOIN contacts ON (data.contact_id = contacts._id) "
                + "LEFT JOIN aggregates ON (contacts.aggregate_id = aggregates._id)";

        public static final String ACTIVITIES = "activities";

        public static final String ACTIVITIES_JOIN_MIMETYPE = "activities "
                + "LEFT OUTER JOIN mimetype ON (activities.mimetype_id = mimetype._id)";

        public static final String ACTIVITIES_JOIN_PACKAGE_MIMETYPE = "activities "
                + "LEFT OUTER JOIN package ON (activities.package_id = package._id) "
                + "LEFT OUTER JOIN mimetype ON (activities.mimetype_id = mimetype._id)";

        public static final String ACTIVITIES_JOIN_AGGREGATES_PACKAGE_MIMETYPE = "activities "
                + "LEFT OUTER JOIN package ON (activities.package_id = package._id) "
                + "LEFT OUTER JOIN mimetype ON (activities.mimetype_id = mimetype._id) "
                + "LEFT JOIN contacts ON (activities.author_contact_id = contacts._id) "
                + "LEFT JOIN aggregates ON (contacts.aggregate_id = aggregates._id)";


        public static final String CONTACTS_JOIN_ACCOUNTS = "contacts "
                + "LEFT OUTER JOIN accounts ON (accounts._id = contacts.accounts_id)";

        public static final String NAME_LOOKUP_JOIN_CONTACTS = "name_lookup "
                + "LEFT JOIN contacts ON (name_lookup.contact_id = contacts._id)";

        public static final String AGGREGATION_EXCEPTIONS_JOIN_CONTACTS_TWICE = "agg_exceptions "
                + "LEFT JOIN contacts contacts1 ON (agg_exceptions.contact_id1 = contacts1._id) "
                + "LEFT JOIN contacts contacts2 ON (agg_exceptions.contact_id2 = contacts2._id) ";
    }

    public interface Clauses {
        public static final String WHERE_IM_MATCHES = MimetypeColumns.MIMETYPE + "=" + Im.MIMETYPE
                + " AND " + Im.PROTOCOL + "=? AND " + Im.DATA + "=?";

        public static final String WHERE_EMAIL_MATCHES = MimetypeColumns.MIMETYPE + "="
                + Email.MIMETYPE + " AND " + Email.DATA + "=?";
    }

    public interface DataColumns {
        public static final String PACKAGE_ID = "package_id";
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
        public static final String DATA_ID = "data_id";
        public static final String CONTACT_ID = "contact_id";
        public static final String NORMALIZED_NAME = "normalized_name";
        public static final String NAME_TYPE = "name_type";
    }

    public interface NameLookupType {
        public static final int FULL_NAME = 0;
        public static final int FULL_NAME_CONCATENATED = 1;
        public static final int FULL_NAME_REVERSE = 2;
        public static final int FULL_NAME_REVERSE_CONCATENATED = 3;
        public static final int GIVEN_NAME_ONLY = 4;
        public static final int FAMILY_NAME_ONLY = 5;
        public static final int NICKNAME = 6;
        public static final int INITIALS = 7;

        // This is the highest name lookup type code
        public static final int TYPE_COUNT = 7;
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
    }

    /** In-memory cache of previously found mimetype mappings */
    private HashMap<String, Long> mMimetypeCache = new HashMap<String, Long>();
    /** In-memory cache of previously found package name mappings */
    private HashMap<String, Long> mPackageCache = new HashMap<String, Long>();


    /** Compiled statements for querying and inserting mappings */
    private SQLiteStatement mMimetypeQuery;
    private SQLiteStatement mPackageQuery;
    private SQLiteStatement mMimetypeInsert;
    private SQLiteStatement mPackageInsert;

    private SQLiteStatement mDataMimetypeQuery;
    private SQLiteStatement mActivitiesMimetypeQuery;

    private static OpenHelper sSingleton = null;

    /**
     * Obtain a singleton instance of {@link OpenHelper}, using the provided
     * {@link Context} to construct one when needed.
     */
    public static synchronized OpenHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new OpenHelper(context);
        }
        return sSingleton;
    }

    /**
     * Private constructor, callers should obtain an instance through
     * {@link #getInstance(Context)} instead.
     */
    private OpenHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        Log.i(TAG, "Creating OpenHelper");
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        // Create compiled statements for package and mimetype lookups
        mMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns._ID + " FROM "
                + Tables.MIMETYPE + " WHERE " + MimetypeColumns.MIMETYPE + "=?");
        mPackageQuery = db.compileStatement("SELECT " + PackageColumns._ID + " FROM "
                + Tables.PACKAGE + " WHERE " + PackageColumns.PACKAGE + "=?");
        mMimetypeInsert = db.compileStatement("INSERT INTO " + Tables.MIMETYPE + "("
                + MimetypeColumns.MIMETYPE + ") VALUES (?)");
        mPackageInsert = db.compileStatement("INSERT INTO " + Tables.PACKAGE + "("
                + PackageColumns.PACKAGE + ") VALUES (?)");

        mDataMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns.MIMETYPE + " FROM "
                + Tables.DATA_JOIN_MIMETYPE + " WHERE " + Tables.DATA + "." + Data._ID + "=?");
        mActivitiesMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns.MIMETYPE
                + " FROM " + Tables.ACTIVITIES_JOIN_MIMETYPE + " WHERE " + Tables.ACTIVITIES + "."
                + Activities._ID + "=?");

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

        db.execSQL("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " ("
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
                Aggregates.PRIMARY_PHONE_ID + " INTEGER REFERENCES data(_id)," +
                Aggregates.PRIMARY_EMAIL_ID + " INTEGER REFERENCES data(_id)," +
                Aggregates.PHOTO_ID + " INTEGER REFERENCES data(_id)," +
                Aggregates.CUSTOM_RINGTONE_ID + " INTEGER REFERENCES data(_id)" +
        ");");

        // Contacts table
        db.execSQL("CREATE TABLE " + Tables.CONTACTS + " (" +
                Contacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Contacts.ACCOUNTS_ID + " INTEGER REFERENCES accounts(_id)," +
                Contacts.SOURCE_ID + " TEXT," +
                Contacts.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                Contacts.DIRTY + " INTEGER NOT NULL DEFAULT 1," +
                Contacts.AGGREGATE_ID + " INTEGER " +
        ");");

        // Package name mapping table
        db.execSQL("CREATE TABLE " + Tables.PACKAGE + " (" +
                PackageColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                PackageColumns.PACKAGE + " TEXT" +
        ");");

        // Mime-type mapping table
        db.execSQL("CREATE TABLE " + Tables.MIMETYPE + " (" +
                MimetypeColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                MimetypeColumns.MIMETYPE + " TEXT" +
        ");");

        // Public generic data table
        db.execSQL("CREATE TABLE " + Tables.DATA + " (" +
                Data._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                DataColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id) NOT NULL," +
                DataColumns.MIMETYPE_ID + " INTEGER REFERENCES mimetype(_id) NOT NULL," +
                Data.CONTACT_ID + " INTEGER NOT NULL," +
                Data.IS_PRIMARY + " INTEGER," +
                Data.IS_SUPER_PRIMARY + " INTEGER," +
                Data.DATA1 + " NUMERIC," +
                Data.DATA2 + " NUMERIC," +
                Data.DATA3 + " NUMERIC," +
                Data.DATA4 + " NUMERIC," +
                Data.DATA5 + " NUMERIC," +
                Data.DATA6 + " NUMERIC," +
                Data.DATA7 + " NUMERIC," +
                Data.DATA8 + " NUMERIC," +
                Data.DATA9 + " NUMERIC," +
                Data.DATA10 + " NUMERIC" +
        ");");

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
                NameLookupColumns.DATA_ID + " INTEGER REFERENCES data(_id) NOT NULL," +
                NameLookupColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id) NOT NULL," +
                NameLookupColumns.NORMALIZED_NAME + " TEXT NOT NULL," +
                NameLookupColumns.NAME_TYPE + " INTEGER" +
        ");");

        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + " ASC, " +
                NameLookupColumns.NAME_TYPE + " ASC, " +
                NameLookupColumns.CONTACT_ID + ", " +
                NameLookupColumns.DATA_ID +
        ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                AggregationExceptions.TYPE + " INTEGER NOT NULL, " +
                AggregationExceptions.CONTACT_ID1 + " INTEGER REFERENCES contacts(_id), " +
                AggregationExceptions.CONTACT_ID2 + " INTEGER REFERENCES contacts(_id)" +
		");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index1 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptions.CONTACT_ID1 + ", " +
                AggregationExceptions.CONTACT_ID2 +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index2 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptions.CONTACT_ID2 + ", " +
                AggregationExceptions.CONTACT_ID1 +
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
        db.execSQL("DROP TABLE IF EXISTS " + Tables.ACTIVITIES + ";");

        // Note: we are not dropping agg_exceptions. In case that table's schema changes,
        // we want to try and preserve the data, because it was entered by the user.

        onCreate(db);
    }

    /**
     * Wipes all data except mime type and package lookup tables.
     */
    public void wipeData() {
        SQLiteDatabase db = getWritableDatabase();
        db.execSQL("DELETE FROM " + Tables.AGGREGATES + ";");
        db.execSQL("DELETE FROM " + Tables.CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.DATA + ";");
        db.execSQL("DELETE FROM " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATION_EXCEPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.ACTIVITIES + ";");
        db.execSQL("VACUUM;");
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
}
