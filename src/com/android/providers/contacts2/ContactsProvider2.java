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

import com.android.providers.contacts2.ContactsContract.CommonDataKinds;
import com.android.providers.contacts2.ContactsContract.Contacts;
import com.android.providers.contacts2.ContactsContract.Data;
import com.android.providers.contacts2.ContactsContract.CommonDataKinds.Phone;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.BaseColumns;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Log;

import java.util.HashMap;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends ContentProvider {
    private static final String TAG = "~~~~~~~~~~~~~"; // TODO: set to class name

    private static final int DATABASE_VERSION = 11;
    private static final String DATABASE_NAME = "contacts2.db";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int CONTACTS = 1000;
    private static final int CONTACTS_ID = 1001;
    private static final int CONTACTS_DATA = 1002;

    private static final int DATA = 2000;
    private static final int DATA_ID = 2001;

    private static final int PHONE_LOOKUP = 3000;

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the data columns */
    private static final HashMap<String, String> sDataProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsProjectionMap;

    /** In-memory cache of previously found mimetype mappings */
    private static HashMap<String, Long> sMimetypeCache;
    /** In-memory cache of previously found package name mappings */
    private static HashMap<String, Long> sPackageCache;

    private static final String TABLE_CONTACTS = "contacts";
    private static final String TABLE_PACKAGE = "package";
    private static final String TABLE_MIMETYPE = "mimetype";
    private static final String TABLE_DATA = "data";
    private static final String TABLE_PHONE_LOOKUP = "phone_lookup";

    private static final String TABLE_DATA_JOIN_MIMETYPE = "data "
            + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

    private static final String TABLE_DATA_JOIN_PACKAGE_MIMETYPE = "data "
            + "LEFT OUTER JOIN package ON (data.package_id = package._id)"
            + "LEFT OUTER JOIN mimetype ON (data.mimetype_id = mimetype._id)";

    private interface PackageColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String PACKAGE = "package";
    }

    private interface MimetypeColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String MIMETYPE = "mimetype";
    }

    private interface DataColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String MIMETYPE_ID = "mimetype_id";
    }

    private interface PhoneLookupColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String DATA_ID = "data_id";
        public static final String CONTACT_ID = "contact_id";
        public static final String NORMALIZED_NUMBER = "normalized_number";
    }

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "phone_lookup/*", PHONE_LOOKUP);

        HashMap<String, String> columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.GIVEN_NAME, Contacts.GIVEN_NAME);
        columns.put(Contacts.PHONETIC_GIVEN_NAME, Contacts.PHONETIC_GIVEN_NAME);
        columns.put(Contacts.FAMILY_NAME, Contacts.FAMILY_NAME);
        columns.put(Contacts.PHONETIC_FAMILY_NAME, Contacts.PHONETIC_FAMILY_NAME);
        columns.put(Contacts.DISPLAY_NAME,
                Contacts.GIVEN_NAME + " || ' ' || " + Contacts.FAMILY_NAME +
                " AS " + Contacts.DISPLAY_NAME);
        columns.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sContactsProjectionMap = columns;

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, "data._id AS _id");
        columns.put(Data.CONTACT_ID, Data.CONTACT_ID);
        columns.put(Data.PACKAGE, Data.PACKAGE);
        columns.put(Data.MIMETYPE, Data.MIMETYPE);
        columns.put(Data.DATA1, Data.DATA1);
        columns.put(Data.DATA2, Data.DATA2);
        columns.put(Data.DATA3, Data.DATA3);
        columns.put(Data.DATA4, Data.DATA4);
        columns.put(Data.DATA5, Data.DATA5);
        columns.put(Data.DATA6, Data.DATA6);
        columns.put(Data.DATA7, Data.DATA7);
        columns.put(Data.DATA8, Data.DATA8);
        columns.put(Data.DATA9, Data.DATA9);
        columns.put(Data.DATA10, Data.DATA10);
        sDataProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sContactsProjectionMap);
        columns.putAll(sDataProjectionMap); // _id will be replaced with the one from data
        sDataContactsProjectionMap = columns;

        // Prepare package and mimetype caches
        sPackageCache = new HashMap<String, Long>();
        sMimetypeCache = new HashMap<String, Long>();
    }

    private OpenHelper mOpenHelper;

    private class OpenHelper extends SQLiteOpenHelper {
        /** Compiled statements for querying and inserting mappings */
        SQLiteStatement mimetypeQuery;
        SQLiteStatement packageQuery;
        SQLiteStatement mimetypeInsert;
        SQLiteStatement packageInsert;
        SQLiteStatement dataMimetypeQuery;

        public OpenHelper() {
            super(getContext(), DATABASE_NAME, null, DATABASE_VERSION);
            Log.i(TAG, "Creating OpenHelper");
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
            // Create compiled statements for package and mimetype lookups
            mimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns._ID + " FROM "
                    + TABLE_MIMETYPE + " WHERE " + MimetypeColumns.MIMETYPE + "=?");
            packageQuery = db.compileStatement("SELECT " + PackageColumns._ID + " FROM "
                    + TABLE_PACKAGE + " WHERE " + PackageColumns.PACKAGE + "=?");
            mimetypeInsert = db.compileStatement("INSERT INTO " + TABLE_MIMETYPE + "("
                    + MimetypeColumns.MIMETYPE + ") VALUES (?)");
            packageInsert = db.compileStatement("INSERT INTO " + TABLE_PACKAGE + "("
                    + PackageColumns.PACKAGE + ") VALUES (?)");
            dataMimetypeQuery = db.compileStatement("SELECT " + MimetypeColumns.MIMETYPE + " FROM "
                    + TABLE_DATA_JOIN_MIMETYPE + " WHERE " + TABLE_DATA + "." + Data._ID + "=?");
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            Log.i(TAG, "Bootstrapping database");

            // Public contacts table
            db.execSQL("CREATE TABLE " + TABLE_CONTACTS + " (" +
                    Contacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Contacts.GIVEN_NAME + " TEXT," +
                    Contacts.PHONETIC_GIVEN_NAME + " TEXT," +
                    Contacts.FAMILY_NAME + " TEXT," +
                    Contacts.PHONETIC_FAMILY_NAME + " TEXT," +
                    Contacts.TIMES_CONTACTED + " INTEGER," +
                    Contacts.LAST_TIME_CONTACTED + " INTEGER," +
                    Contacts.CUSTOM_RINGTONE + " TEXT," +
                    Contacts.SEND_TO_VOICEMAIL + " INTEGER," +
                    Contacts.STARRED + " INTEGER" +
            ");");

            // Package name mapping table
            db.execSQL("CREATE TABLE " + TABLE_PACKAGE + " (" +
                    PackageColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    PackageColumns.PACKAGE + " TEXT" +
            ");");

            // Mime-type mapping table
            db.execSQL("CREATE TABLE " + TABLE_MIMETYPE + " (" +
                    MimetypeColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    MimetypeColumns.MIMETYPE + " TEXT" +
            ");");

            // Public generic data table
            db.execSQL("CREATE TABLE " + TABLE_DATA + " (" +
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
            db.execSQL("CREATE TABLE " + TABLE_PHONE_LOOKUP + " (" +
                    PhoneLookupColumns._ID + " INTEGER PRIMARY KEY," +
                    PhoneLookupColumns.DATA_ID + " INTEGER REFERENCES data(_id) NOT NULL," +
                    PhoneLookupColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id) NOT NULL," +
                    PhoneLookupColumns.NORMALIZED_NUMBER + " TEXT NOT NULL" +
            ");");

            db.execSQL("CREATE INDEX phone_lookup_index ON " + TABLE_PHONE_LOOKUP + " (" +
                    PhoneLookupColumns.NORMALIZED_NUMBER + " ASC, " +
                    PhoneLookupColumns.CONTACT_ID + ", " +
                    PhoneLookupColumns.DATA_ID +
            ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.i(TAG, "Upgraing from version " + oldVersion + " to " + newVersion
                    + ", data will be lost!");

            db.execSQL("DROP TABLE IF EXISTS " + TABLE_CONTACTS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_PACKAGE + ";");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_MIMETYPE + ";");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_DATA + ";");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_PHONE_LOOKUP + ";");

            onCreate(db);
        }
    }

    @Override
    public boolean onCreate() {
        mOpenHelper = new OpenHelper();

        // TODO remove this, it's here to force opening the database on boot for testing
        mOpenHelper.getReadableDatabase();

        return true;
    }

    /**
     * Called when a change has been made.
     *
     * @param uri the uri that the change was made to
     */
    private void onChange(Uri uri) {
        getContext().getContentResolver().notifyChange(ContactsContract.AUTHORITY_URI, null);
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int match = sUriMatcher.match(uri);
        long id = 0;
        switch (match) {
            case CONTACTS: {
                id = insertContact(values);
                break;
            }

            case CONTACTS_DATA: {
                values.put(Data.CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values);
                break;
            }

            case DATA: {
                id = insertData(values);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        final Uri result = ContentUris.withAppendedId(Contacts.CONTENT_URI, id);
        onChange(result);
        return result;
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertContact(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        return db.insert(TABLE_CONTACTS, Contacts.GIVEN_NAME, values);
    }

    /**
     * Inserts an item in the data table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertData(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            // TODO: Consider enforcing Binder.getCallingUid() for package name
            // requested by this insert.

            // Replace package name and mime-type with internal mappings
            final String packageName = values.getAsString(Data.PACKAGE);
            values.put(DataColumns.PACKAGE_ID, getPackageId(packageName));
            values.remove(Data.PACKAGE);

            final String mimeType = values.getAsString(Data.MIMETYPE);
            values.put(DataColumns.MIMETYPE_ID, getMimeTypeId(mimeType));
            values.remove(Data.MIMETYPE);

            // Insert the data row itself
            id = db.insert(TABLE_DATA, Data.DATA1, values);

            // If it's a phone number add the normalized version to the lookup table
            if (CommonDataKinds.Phone.CONTENT_ITEM_TYPE.equals(mimeType)) {
                final ContentValues phoneValues = new ContentValues();
                final String number = values.getAsString(Phone.NUMBER);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER,
                        PhoneNumberUtils.getStrippedReversed(number));
                phoneValues.put(PhoneLookupColumns.DATA_ID, id);
                phoneValues.put(PhoneLookupColumns.CONTACT_ID, values.getAsLong(Phone.CONTACT_ID));
                db.insert(TABLE_PHONE_LOOKUP, null, phoneValues);
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return id;
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
     * Convert a package name into an integer, using {@link #TABLE_PACKAGE} for
     * lookups and possible allocation of new IDs as needed.
     */
    private long getPackageId(String packageName) {
        // Make sure compiled statements are ready by opening database
        mOpenHelper.getReadableDatabase();
        return getCachedId(mOpenHelper.packageQuery, mOpenHelper.packageInsert,
                packageName, sPackageCache);
    }

    /**
     * Convert a mime-type into an integer, using {@link #TABLE_MIMETYPE} for
     * lookups and possible allocation of new IDs as needed.
     */
    private long getMimeTypeId(String mimetype) {
        // Make sure compiled statements are ready by opening database
        mOpenHelper.getReadableDatabase();
        return getCachedId(mOpenHelper.mimetypeQuery, mOpenHelper.mimetypeInsert,
                mimetype, sMimetypeCache);
    }

    /**
     * Find the mime-type for the given data ID.
     */
    private String getDataMimeType(long dataId) {
        // Make sure compiled statements are ready by opening database
        mOpenHelper.getReadableDatabase();
        try {
            // Try database query to find mimetype
            DatabaseUtils.bindObjectToProgram(mOpenHelper.dataMimetypeQuery, 1, dataId);
            String mimetype = mOpenHelper.dataMimetypeQuery.simpleQueryForString();
            return mimetype;
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                int contactsDeleted = db.delete(TABLE_CONTACTS, Contacts._ID + "=" + contactId, null);
                int dataDeleted = db.delete(TABLE_DATA, Data.CONTACT_ID + "=" + contactId, null);
                return contactsDeleted + dataDeleted;
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                return db.delete(TABLE_DATA, Data._ID + "=" + dataId, null);
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS: {
                qb.setTables(TABLE_CONTACTS);
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                qb.setTables(TABLE_CONTACTS);
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case CONTACTS_DATA: {
                qb.setTables(TABLE_DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("contact_id = " + uri.getPathSegments().get(1));
                break;
            }

            case DATA_ID: {
                // TODO: enforce that caller has read access to this data
                qb.setTables(TABLE_DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case PHONE_LOOKUP: {
                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = Contacts._ID;
                }

                final String number = uri.getLastPathSegment();
                final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
                final StringBuilder tables = new StringBuilder();
                tables.append("contacts, (SELECT data_id FROM phone_lookup WHERE (phone_lookup.normalized_number GLOB '");
                tables.append(normalizedNumber);
                tables.append("*')) AS lookup, data");
                qb.setTables(tables.toString());
                qb.appendWhere("lookup.data_id=data._id AND data.contact_id=contacts._id AND ");
                qb.appendWhere("PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
                qb.appendWhereEscapeString(number);
                qb.appendWhere(")");
                qb.setProjectionMap(sDataContactsProjectionMap);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs, null, null, sortOrder);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    @Override
    public String getType(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
            case CONTACTS_DATA: return Data.CONTENT_TYPE;
            case DATA: return Data.CONTENT_TYPE;
            case DATA_ID:
                long dataId = ContentUris.parseId(uri);
                return getDataMimeType(dataId);
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }
}
