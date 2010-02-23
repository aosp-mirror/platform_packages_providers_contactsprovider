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

package com.android.providers.contacts;

import com.android.providers.contacts.ContactsDatabaseHelper.ActivitiesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PackagesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.RawContacts;
import android.provider.SocialContract;
import android.provider.SocialContract.Activities;

import android.net.Uri;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Social activity content provider. The contract between this provider and
 * applications is defined in {@link SocialContract}.
 */
public class SocialProvider extends ContentProvider {
    // TODO: clean up debug tag
    private static final String TAG = "SocialProvider ~~~~";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int ACTIVITIES = 1000;
    private static final int ACTIVITIES_ID = 1001;
    private static final int ACTIVITIES_AUTHORED_BY = 1002;

    private static final int CONTACT_STATUS_ID = 3000;

    private static final String DEFAULT_SORT_ORDER = Activities.THREAD_PUBLISHED + " DESC, "
            + Activities.PUBLISHED + " ASC";

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sRawContactsProjectionMap;
    /** Contains just the activities columns */
    private static final HashMap<String, String> sActivitiesProjectionMap;

    /** Contains the activities, raw contacts, and contacts columns, for joined tables */
    private static final HashMap<String, String> sActivitiesContactsProjectionMap;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;

        matcher.addURI(SocialContract.AUTHORITY, "activities", ACTIVITIES);
        matcher.addURI(SocialContract.AUTHORITY, "activities/#", ACTIVITIES_ID);
        matcher.addURI(SocialContract.AUTHORITY, "activities/authored_by/#", ACTIVITIES_AUTHORED_BY);

        matcher.addURI(SocialContract.AUTHORITY, "contact_status/#", CONTACT_STATUS_ID);

        HashMap<String, String> columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        // TODO: fix display name reference (in fact, use the contacts view instead of the table)
        columns.put(Contacts.DISPLAY_NAME, "contact." + Contacts.DISPLAY_NAME + " AS "
                + Contacts.DISPLAY_NAME);
        sContactsProjectionMap = columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(RawContacts._ID, Tables.RAW_CONTACTS + "." + RawContacts._ID + " AS _id");
        columns.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        sRawContactsProjectionMap = columns;

        // Activities projection map
        columns = new HashMap<String, String>();
        columns.put(Activities._ID, "activities._id AS _id");
        columns.put(Activities.RES_PACKAGE, PackagesColumns.PACKAGE + " AS "
                + Activities.RES_PACKAGE);
        columns.put(Activities.MIMETYPE, Activities.MIMETYPE);
        columns.put(Activities.RAW_ID, Activities.RAW_ID);
        columns.put(Activities.IN_REPLY_TO, Activities.IN_REPLY_TO);
        columns.put(Activities.AUTHOR_CONTACT_ID, Activities.AUTHOR_CONTACT_ID);
        columns.put(Activities.TARGET_CONTACT_ID, Activities.TARGET_CONTACT_ID);
        columns.put(Activities.PUBLISHED, Activities.PUBLISHED);
        columns.put(Activities.THREAD_PUBLISHED, Activities.THREAD_PUBLISHED);
        columns.put(Activities.TITLE, Activities.TITLE);
        columns.put(Activities.SUMMARY, Activities.SUMMARY);
        columns.put(Activities.LINK, Activities.LINK);
        columns.put(Activities.THUMBNAIL, Activities.THUMBNAIL);
        sActivitiesProjectionMap = columns;

        // Activities, raw contacts, and contacts projection map for joins
        columns = new HashMap<String, String>();
        columns.putAll(sContactsProjectionMap);
        columns.putAll(sRawContactsProjectionMap);
        columns.putAll(sActivitiesProjectionMap); // Final _id will be from Activities
        sActivitiesContactsProjectionMap = columns;

    }

    private ContactsDatabaseHelper mDbHelper;

    /** {@inheritDoc} */
    @Override
    public boolean onCreate() {
        final Context context = getContext();
        mDbHelper = ContactsDatabaseHelper.getInstance(context);
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

    /** {@inheritDoc} */
    @Override
    public boolean isTemporary() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int match = sUriMatcher.match(uri);
        long id = 0;
        switch (match) {
            case ACTIVITIES: {
                id = insertActivity(values);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        final Uri result = ContentUris.withAppendedId(Activities.CONTENT_URI, id);
        onChange(result);
        return result;
    }

    /**
     * Inserts an item into the {@link Tables#ACTIVITIES} table.
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertActivity(ContentValues values) {

        // TODO verify that IN_REPLY_TO != RAW_ID

        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            // TODO: Consider enforcing Binder.getCallingUid() for package name
            // requested by this insert.

            // Replace package name and mime-type with internal mappings
            final String packageName = values.getAsString(Activities.RES_PACKAGE);
            if (packageName != null) {
                values.put(ActivitiesColumns.PACKAGE_ID, mDbHelper.getPackageId(packageName));
            }
            values.remove(Activities.RES_PACKAGE);

            final String mimeType = values.getAsString(Activities.MIMETYPE);
            values.put(ActivitiesColumns.MIMETYPE_ID, mDbHelper.getMimeTypeId(mimeType));
            values.remove(Activities.MIMETYPE);

            long published = values.getAsLong(Activities.PUBLISHED);
            long threadPublished = published;

            String inReplyTo = values.getAsString(Activities.IN_REPLY_TO);
            if (inReplyTo != null) {
                threadPublished = getThreadPublished(db, inReplyTo, published);
            }

            values.put(Activities.THREAD_PUBLISHED, threadPublished);

            // Insert the data row itself
            id = db.insert(Tables.ACTIVITIES, Activities.RAW_ID, values);

            // Adjust thread timestamps on replies that have already been inserted
            if (values.containsKey(Activities.RAW_ID)) {
                adjustReplyTimestamps(db, values.getAsString(Activities.RAW_ID), published);
            }

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return id;
    }

    /**
     * Finds the timestamp of the original message in the thread. If not found, returns
     * {@code defaultValue}.
     */
    private long getThreadPublished(SQLiteDatabase db, String rawId, long defaultValue) {
        String inReplyTo = null;
        long threadPublished = defaultValue;

        final Cursor c = db.query(Tables.ACTIVITIES,
                new String[]{Activities.IN_REPLY_TO, Activities.PUBLISHED},
                Activities.RAW_ID + " = ?", new String[]{rawId}, null, null, null);
        try {
            if (c.moveToFirst()) {
                inReplyTo = c.getString(0);
                threadPublished = c.getLong(1);
            }
        } finally {
            c.close();
        }

        if (inReplyTo != null) {

            // Call recursively to obtain the original timestamp of the entire thread
            return getThreadPublished(db, inReplyTo, threadPublished);
        }

        return threadPublished;
    }

    /**
     * In case the original message of a thread arrives after its reply messages, we need
     * to check if there are any replies in the database and if so adjust their thread_published.
     */
    private void adjustReplyTimestamps(SQLiteDatabase db, String inReplyTo, long threadPublished) {

        ContentValues values = new ContentValues();
        values.put(Activities.THREAD_PUBLISHED, threadPublished);

        /*
         * Issuing an exploratory update. If it updates nothing, we are done.  Otherwise,
         * we will run a query to find the updated records again and repeat recursively.
         */
        int replies = db.update(Tables.ACTIVITIES, values,
                Activities.IN_REPLY_TO + "= ?", new String[] {inReplyTo});

        if (replies == 0) {
            return;
        }

        /*
         * Presumably this code will be executed very infrequently since messages tend to arrive
         * in the order they get sent.
         */
        ArrayList<String> rawIds = new ArrayList<String>(replies);
        final Cursor c = db.query(Tables.ACTIVITIES,
                new String[]{Activities.RAW_ID},
                Activities.IN_REPLY_TO + " = ?", new String[] {inReplyTo}, null, null, null);
        try {
            while (c.moveToNext()) {
                rawIds.add(c.getString(0));
            }
        } finally {
            c.close();
        }

        for (String rawId : rawIds) {
            adjustReplyTimestamps(db, rawId, threadPublished);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACTIVITIES_ID: {
                final long activityId = ContentUris.parseId(uri);
                return db.delete(Tables.ACTIVITIES, Activities._ID + "=" + activityId, null);
            }

            case ACTIVITIES_AUTHORED_BY: {
                final long contactId = ContentUris.parseId(uri);
                return db.delete(Tables.ACTIVITIES, Activities.AUTHOR_CONTACT_ID + "=" + contactId, null);
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        final SQLiteDatabase db = mDbHelper.getReadableDatabase();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String limit = null;

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACTIVITIES: {
                qb.setTables(Tables.ACTIVITIES_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS);
                qb.setProjectionMap(sActivitiesContactsProjectionMap);
                break;
            }

            case ACTIVITIES_ID: {
                // TODO: enforce that caller has read access to this data
                long activityId = ContentUris.parseId(uri);
                qb.setTables(Tables.ACTIVITIES_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS);
                qb.setProjectionMap(sActivitiesContactsProjectionMap);
                qb.appendWhere(Activities._ID + "=" + activityId);
                break;
            }

            case ACTIVITIES_AUTHORED_BY: {
                long contactId = ContentUris.parseId(uri);
                qb.setTables(Tables.ACTIVITIES_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS);
                qb.setProjectionMap(sActivitiesContactsProjectionMap);
                qb.appendWhere(Activities.AUTHOR_CONTACT_ID + "=" + contactId);
                break;
            }

            case CONTACT_STATUS_ID: {
                long aggId = ContentUris.parseId(uri);
                qb.setTables(Tables.ACTIVITIES_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS);
                qb.setProjectionMap(sActivitiesContactsProjectionMap);

                // Latest status of a contact is any top-level status
                // authored by one of its children contacts.
                qb.appendWhere(Activities.IN_REPLY_TO + " IS NULL AND ");
                qb.appendWhere(Activities.AUTHOR_CONTACT_ID + " IN (SELECT " + BaseColumns._ID
                        + " FROM " + Tables.RAW_CONTACTS + " WHERE " + RawContacts.CONTACT_ID + "="
                        + aggId + ")");
                sortOrder = Activities.PUBLISHED + " DESC";
                limit = "1";
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        // Default to reverse-chronological sort if nothing requested
        if (sortOrder == null) {
            sortOrder = DEFAULT_SORT_ORDER;
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs, null, null, sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    @Override
    public String getType(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACTIVITIES:
            case ACTIVITIES_AUTHORED_BY:
                return Activities.CONTENT_TYPE;
            case ACTIVITIES_ID:
                final SQLiteDatabase db = mDbHelper.getReadableDatabase();
                long activityId = ContentUris.parseId(uri);
                return mDbHelper.getActivityMimeType(activityId);
            case CONTACT_STATUS_ID:
                return Contacts.CONTENT_ITEM_TYPE;
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }
}
