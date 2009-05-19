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
import com.android.providers.contacts2.OpenHelper.ActivitiesColumns;
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
import android.util.Log;

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

    private static final String DEFAULT_SORT_ORDER = Activities.PUBLISHED + " DESC";

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sAggregatesProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the activities columns */
    private static final HashMap<String, String> sActivitiesProjectionMap;
    /** Contains the activities, contacts, and aggregates columns, for joined tables */
    private static final HashMap<String, String> sActivitiesAggregatesProjectionMap;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;

        matcher.addURI(SocialContract.AUTHORITY, "activities", ACTIVITIES);
        matcher.addURI(SocialContract.AUTHORITY, "activities/#", ACTIVITIES_ID);
        matcher.addURI(SocialContract.AUTHORITY, "activities/authored_by/#", ACTIVITIES_AUTHORED_BY);

        HashMap<String, String> columns;

        // Aggregates projection map
        columns = new HashMap<String, String>();
        columns.put(Aggregates.DISPLAY_NAME, Aggregates.DISPLAY_NAME);
        sAggregatesProjectionMap = columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.AGGREGATE_ID, Contacts.AGGREGATE_ID);
        sContactsProjectionMap = columns;

        // Activities projection map
        columns = new HashMap<String, String>();
        columns.put(Activities._ID, "activities._id AS _id");
        columns.put(Activities.PACKAGE, Activities.PACKAGE);
        columns.put(Activities.MIMETYPE, Activities.MIMETYPE);
        columns.put(Activities.RAW_ID, Activities.RAW_ID);
        columns.put(Activities.IN_REPLY_TO, Activities.IN_REPLY_TO);
        columns.put(Activities.AUTHOR_CONTACT_ID, Activities.AUTHOR_CONTACT_ID);
        columns.put(Activities.TARGET_CONTACT_ID, Activities.TARGET_CONTACT_ID);
        columns.put(Activities.PUBLISHED, Activities.PUBLISHED);
        columns.put(Activities.TITLE, Activities.TITLE);
        columns.put(Activities.SUMMARY, Activities.SUMMARY);
        columns.put(Activities.THUMBNAIL, Activities.THUMBNAIL);
        sActivitiesProjectionMap = columns;

        // Activities, contacts, and aggregates projection map for joins
        columns = new HashMap<String, String>();
        columns.putAll(sAggregatesProjectionMap);
        columns.putAll(sContactsProjectionMap);
        columns.putAll(sActivitiesProjectionMap); // _id will be replaced with the one from aggregates
        sActivitiesAggregatesProjectionMap = columns;

    }

    private OpenHelper mOpenHelper;

    /** {@inheritDoc} */
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
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            // TODO: Consider enforcing Binder.getCallingUid() for package name
            // requested by this insert.

            // Replace package name and mime-type with internal mappings
            final String packageName = values.getAsString(Activities.PACKAGE);
            values.put(ActivitiesColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
            values.remove(Activities.PACKAGE);

            final String mimeType = values.getAsString(Activities.MIMETYPE);
            values.put(ActivitiesColumns.MIMETYPE_ID, mOpenHelper.getMimeTypeId(mimeType));
            values.remove(Activities.MIMETYPE);

            // Insert the data row itself
            id = db.insert(Tables.ACTIVITIES, Activities.RAW_ID, values);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

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
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACTIVITIES: {
                qb.setTables(Tables.ACTIVITIES_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sActivitiesAggregatesProjectionMap);
                break;
            }

            case ACTIVITIES_ID: {
                // TODO: enforce that caller has read access to this data
                qb.setTables(Tables.ACTIVITIES_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sActivitiesAggregatesProjectionMap);
                qb.appendWhere(Activities._ID + "=" + uri.getLastPathSegment());
                break;
            }

            case ACTIVITIES_AUTHORED_BY: {
                qb.setTables(Tables.ACTIVITIES_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sActivitiesAggregatesProjectionMap);
                qb.appendWhere(Activities.AUTHOR_CONTACT_ID + "=" + uri.getLastPathSegment());
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
            case ACTIVITIES:
            case ACTIVITIES_AUTHORED_BY:
                return Activities.CONTENT_TYPE;
            case ACTIVITIES_ID:
                final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                long activityId = ContentUris.parseId(uri);
                return mOpenHelper.getActivityMimeType(activityId);
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }
}
