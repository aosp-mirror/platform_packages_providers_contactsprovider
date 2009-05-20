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

import com.android.providers.contacts2.SocialContract.Activities;
import com.android.providers.contacts2.ContactsContract.Aggregates;
import com.android.providers.contacts2.ContactsContract.Contacts;
import com.android.providers.contacts2.ContactsContract.Data;

import android.content.Context;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteStatement;
import android.provider.BaseColumns;
import android.util.Log;

import java.util.HashMap;

/**
 * Database open helper for contacts and social activity data. Designed as a
 * singleton to make sure that all {@link ContentProvider} users get the same
 * reference. Provides handy methods for maintaining package and mime-type
 * lookup tables.
 */
/* package */ class OpenHelper extends SQLiteOpenHelper {
    private static final String TAG = "OpenHelper";

    private static final int DATABASE_VERSION = 15;
    private static final String DATABASE_NAME = "contacts2.db";

    public interface Tables {
        public static final String AGGREGATES = "aggregates";
        public static final String CONTACTS = "contacts";
        public static final String PACKAGE = "package";
        public static final String MIMETYPE = "mimetype";
        public static final String PHONE_LOOKUP = "phone_lookup";

        public static final String DATA = "data";

        public static final String DATA_JOIN_MIMETYPE = "data "
                + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

        public static final String DATA_JOIN_PACKAGE_MIMETYPE = "data "
                + "LEFT OUTER JOIN package ON (data.package_id = package._id) "
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
    }


    public interface ContactsColumns {
        public static final String AGGREGATION_NEEDED = "aggregation_needed";
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

    private interface PackageColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String PACKAGE = "package";
    }

    private interface MimetypeColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String MIMETYPE = "mimetype";
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

    public static synchronized OpenHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new OpenHelper(context);
        }
        return sSingleton;
    }

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
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        Log.i(TAG, "Bootstrapping database");

        // One row per group of contacts corresponding to the same person
        db.execSQL("CREATE TABLE " + Tables.AGGREGATES + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Aggregates.DISPLAY_NAME + " TEXT," +
                Aggregates.TIMES_CONTACTED + " INTEGER," +
                Aggregates.LAST_TIME_CONTACTED + " INTEGER," +
                Aggregates.STARRED + " INTEGER" +
        ");");

        // Contacts table
        db.execSQL("CREATE TABLE " + Tables.CONTACTS + " (" +
                Contacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Contacts.AGGREGATE_ID + " INTEGER, " +
                ContactsColumns.AGGREGATION_NEEDED + " INTEGER," +
                Contacts.CUSTOM_RINGTONE + " TEXT," +
                Contacts.SEND_TO_VOICEMAIL + " INTEGER" +
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
                Activities.THUMBNAIL + " BLOB" +
        ");");

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG, "Upgraing from version " + oldVersion + " to " + newVersion
                + ", data will be lost!");

        db.execSQL("DROP TABLE IF EXISTS " + Tables.AGGREGATES + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CONTACTS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.PACKAGE + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.MIMETYPE + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.DATA + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.ACTIVITIES + ";");

        onCreate(db);
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
