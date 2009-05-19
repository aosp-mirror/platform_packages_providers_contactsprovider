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

import com.android.providers.contacts2.ContactsContract.Aggregates;
import com.android.providers.contacts2.ContactsContract.CommonDataKinds;
import com.android.providers.contacts2.ContactsContract.Contacts;
import com.android.providers.contacts2.ContactsContract.Data;
import com.android.providers.contacts2.ContactsContract.CommonDataKinds.Phone;
import com.android.providers.contacts2.OpenHelper.ContactsColumns;
import com.android.providers.contacts2.OpenHelper.DataColumns;
import com.android.providers.contacts2.OpenHelper.PhoneLookupColumns;
import com.android.providers.contacts2.OpenHelper.Tables;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.provider.BaseColumns;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;

import java.util.HashMap;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends ContentProvider {
    // TODO: clean up debug tag and rename this class
    private static final String TAG = "ContactsProvider ~~~~";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int AGGREGATES = 1000;
    private static final int AGGREGATES_ID = 1001;
    private static final int AGGREGATES_DATA = 1002;

    private static final int CONTACTS = 2002;
    private static final int CONTACTS_ID = 2003;
    private static final int CONTACTS_DATA = 2004;
    private static final int CONTACTS_FILTER_EMAIL = 2005;

    private static final int DATA = 3000;
    private static final int DATA_ID = 3001;

    private static final int PHONE_LOOKUP = 4000;

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sAggregatesProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the data columns */
    private static final HashMap<String, String> sDataProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsProjectionMap;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates", AGGREGATES);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#", AGGREGATES_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#/data", AGGREGATES_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter_email/*", CONTACTS_FILTER_EMAIL);
        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "phone_lookup/*", PHONE_LOOKUP);

        HashMap<String, String> columns;

        // Aggregates projection map
        columns = new HashMap<String, String>();
        columns.put(Aggregates.DISPLAY_NAME, Aggregates.DISPLAY_NAME);
        columns.put(Aggregates.LAST_TIME_CONTACTED, Aggregates.LAST_TIME_CONTACTED);
        columns.put(Aggregates.STARRED, Aggregates.STARRED);
        sAggregatesProjectionMap = columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.AGGREGATE_ID, Contacts.AGGREGATE_ID);
        columns.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        columns.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
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
        columns.put(Data.CONTACT_ID, "data.contact_id");
        sDataContactsProjectionMap = columns;
    }

    private OpenHelper mOpenHelper;

    @Override
    public boolean onCreate() {
        final Context context = getContext();
        mOpenHelper = OpenHelper.getInstance(context);

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
            case AGGREGATES: {
                id = insertAggregate(values);
                break;
            }

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
     * Inserts an item in the aggregates table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertAggregate(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        return db.insert(Tables.AGGREGATES, Aggregates.DISPLAY_NAME, values);
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertContact(ContentValues values) {

        /*
         * The contact record is inserted in the contacts table, but it needs to
         * be processed by the aggregator before it will be returned by the
         * "aggregates" queries.
         */
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues augmentedValues = new ContentValues(values);
        augmentedValues.put(ContactsColumns.AGGREGATION_NEEDED, true);
        return db.insert(Tables.CONTACTS, Contacts.AGGREGATE_ID, augmentedValues);
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
            values.put(DataColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
            values.remove(Data.PACKAGE);

            final String mimeType = values.getAsString(Data.MIMETYPE);
            values.put(DataColumns.MIMETYPE_ID, mOpenHelper.getMimeTypeId(mimeType));
            values.remove(Data.MIMETYPE);

            // Insert the data row itself
            id = db.insert(Tables.DATA, Data.DATA1, values);

            // If it's a phone number add the normalized version to the lookup table
            if (CommonDataKinds.Phone.CONTENT_ITEM_TYPE.equals(mimeType)) {
                final ContentValues phoneValues = new ContentValues();
                final String number = values.getAsString(Phone.NUMBER);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER,
                        PhoneNumberUtils.getStrippedReversed(number));
                phoneValues.put(PhoneLookupColumns.DATA_ID, id);
                phoneValues.put(PhoneLookupColumns.CONTACT_ID, values.getAsLong(Phone.CONTACT_ID));
                db.insert(Tables.PHONE_LOOKUP, null, phoneValues);
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return id;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case AGGREGATES_ID: {
                long aggregateId = ContentUris.parseId(uri);

                // Remove references to the aggregate first
                ContentValues values = new ContentValues();
                values.putNull(Contacts.AGGREGATE_ID);
                db.update(Tables.CONTACTS, values, Contacts.AGGREGATE_ID + "=" + aggregateId, null);

                return db.delete(Tables.AGGREGATES, BaseColumns._ID + "=" + aggregateId, null);
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                int contactsDeleted = db.delete(Tables.CONTACTS, Contacts._ID + "=" + contactId, null);
                int dataDeleted = db.delete(Tables.DATA, Data.CONTACT_ID + "=" + contactId, null);
                return contactsDeleted + dataDeleted;
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                return db.delete("data", Data._ID + "=" + dataId, null);
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
            case AGGREGATES: {
                qb.setTables(Tables.AGGREGATES);
                qb.setProjectionMap(sAggregatesProjectionMap);
                break;
            }

            case AGGREGATES_ID: {
                qb.setTables(Tables.AGGREGATES);
                qb.setProjectionMap(sAggregatesProjectionMap);
                qb.appendWhere(BaseColumns._ID + " = " + uri.getLastPathSegment());
                break;
            }

            case AGGREGATES_DATA: {
                qb.setTables(Tables.DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Contacts.AGGREGATE_ID + " = " + uri.getPathSegments().get(1));
                break;
            }

            case CONTACTS: {
                qb.setTables(Tables.CONTACTS);
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                qb.setTables(Tables.CONTACTS);
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case CONTACTS_DATA: {
                qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("contact_id = " + uri.getPathSegments().get(1));
                break;
            }

            case CONTACTS_FILTER_EMAIL: {
                qb.setTables(Tables.DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataContactsProjectionMap);
                qb.appendWhere(Data.MIMETYPE + "=" + CommonDataKinds.Email.CONTENT_ITEM_TYPE);
                qb.appendWhere(" AND " + CommonDataKinds.Email.DATA + "=");
                qb.appendWhereEscapeString(uri.getPathSegments().get(2));
                break;
            }

            case DATA: {
                // TODO: enforce that caller has read access to this data
                qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                break;
            }

            case DATA_ID: {
                // TODO: enforce that caller has read access to this data
                qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case PHONE_LOOKUP: {
                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = Data.CONTACT_ID;
                }

                final String number = uri.getLastPathSegment();
                final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
                final StringBuilder tables = new StringBuilder();
                tables.append("contacts, (SELECT data_id FROM phone_lookup WHERE (phone_lookup.normalized_number GLOB '");
                tables.append(normalizedNumber);
                tables.append("*')) AS lookup, " + Tables.DATA_JOIN_PACKAGE_MIMETYPE);
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
            case AGGREGATES: return Aggregates.CONTENT_TYPE;
            case AGGREGATES_ID: return Aggregates.CONTENT_ITEM_TYPE;
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
            case DATA_ID:
                final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                long dataId = ContentUris.parseId(uri);
                return mOpenHelper.getDataMimeType(dataId);
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }
}
