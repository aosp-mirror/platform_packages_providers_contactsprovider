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
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
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

    private static final int DATABASE_VERSION = 8;
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
        sContactsProjectionMap = columns;

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, "data._id AS _id");
        columns.put(Data.CONTACT_ID, Data.CONTACT_ID);
        columns.put(Data.PACKAGE, Data.PACKAGE);
        columns.put(Data.KIND, Data.KIND);
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
    }

    private OpenHelper mOpenHelper;

    private class OpenHelper extends SQLiteOpenHelper {
        public OpenHelper() {
            super(getContext(), DATABASE_NAME, null, DATABASE_VERSION);
            Log.i(TAG, "Creating OpenHelper");
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            Log.i(TAG, "Bootstrapping database");

            // Public contacts table
            db.execSQL("CREATE TABLE contacts (" +
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

            // Public generic data table
            db.execSQL("CREATE TABLE data (" +
                    Data._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Data.CONTACT_ID + " INTEGER NOT NULL," +
                    Data.PACKAGE + " TEXT NOT NULL," +
                    Data.KIND + " INTEGER NOT NULL," +
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
            db.execSQL("CREATE TABLE phone_lookup (" +
                    "_id INTEGER PRIMARY KEY," +
                    "data_id INTEGER REFERENCES data(_id) NOT NULL," +
                    "contact_id INTEGER REFERENCES contacts(_id) NOT NULL," +
                    "normalized_number TEXT NOT NULL" +
            ");");

            db.execSQL("CREATE INDEX phone_lookup_index ON phone_lookup (" +
                    "normalized_number ASC, contact_id, data_id" +
            ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.i(TAG, "Upgraing from version " + oldVersion + " to " + newVersion
                    + ", data will be lost!");

            db.execSQL("DROP TABLE IF EXISTS contacts;");
            db.execSQL("DROP TABLE IF EXISTS data;");
            db.execSQL("DROP TABLE IF EXISTS phone_lookup;");

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
                throw new UnsupportedOperationException("Unknow uri: " + uri);
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
        return db.insert("contacts", Contacts.GIVEN_NAME, values);
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
            // Insert the data row itself
            id = db.insert("data", Data.DATA1, values);

            final String packageName = values.getAsString(Data.PACKAGE);
            // If it's a phone number add the normalized version to the lookup table
            if (CommonDataKinds.PACKAGE_COMMON.equals(packageName)) {
                final int kind = values.getAsInteger(Data.KIND);
                if (kind == Phone.KIND_PHONE) {
                    final ContentValues phoneValues = new ContentValues();
                    final String number = values.getAsString(Phone.NUMBER);
                    phoneValues.put("normalized_number",
                            PhoneNumberUtils.getStrippedReversed(number));
                    phoneValues.put("data_id", id);
                    phoneValues.put("contact_id", values.getAsLong(Phone.CONTACT_ID));
                    db.insert("phone_lookup", null, phoneValues);
                }
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return id;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        throw new UnsupportedOperationException();
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
                qb.setTables("contacts");
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                qb.setTables("contacts");
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case CONTACTS_DATA: {
                qb.setTables("data");
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("contact_id = " + uri.getPathSegments().get(1));
                break;
            }

            case DATA: {
                qb.setTables("data");
                qb.setProjectionMap(sDataProjectionMap);
                break;
            }

            case DATA_ID: {
                qb.setTables("data");
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
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }
}
