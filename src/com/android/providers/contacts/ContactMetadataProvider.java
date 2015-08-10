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


import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.IContentProvider;
import android.content.OperationApplicationException;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Binder;
import android.provider.ContactsContract;
import android.provider.ContactsContract.MetadataSync;
import android.text.TextUtils;
import android.util.Log;
import com.android.common.content.ProjectionMap;
import com.android.providers.contacts.ContactsDatabaseHelper.MetadataSyncColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.ContactsDatabaseHelper.Views;
import com.android.providers.contacts.MetadataEntryParser.MetadataEntry;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.UserUtils;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static com.android.providers.contacts.ContactsProvider2.getLimit;
import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

/**
 * Simple content provider to handle directing contact metadata specific calls.
 */
public class ContactMetadataProvider extends ContentProvider {
    private static final String TAG = "ContactMetadata";
    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);
    private static final int METADATA_SYNC = 1;
    private static final int METADATA_SYNC_ID = 2;
    private static final int RAW_CONTACTS = 3;
    private static final int RAW_CONTACTS_ID = 4;
    private static final int DATA = 5;
    private static final int DATA_ID = 6;
    private static final int AGGREGATION_EXCEPTIONS = 7;

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    static {
        sURIMatcher.addURI(MetadataSync.METADATA_AUTHORITY, "metadata_sync", METADATA_SYNC);
        sURIMatcher.addURI(MetadataSync.METADATA_AUTHORITY, "metadata_sync/#", METADATA_SYNC_ID);
        sURIMatcher.addURI(ContactsContract.AUTHORITY, "raw_contacts", RAW_CONTACTS);
        sURIMatcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#", RAW_CONTACTS_ID);
        sURIMatcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        sURIMatcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        sURIMatcher.addURI(ContactsContract.AUTHORITY, "aggregation_exceptions",
                AGGREGATION_EXCEPTIONS);
    }

    private static final Map<String, String> sMetadataProjectionMap = ProjectionMap.builder()
            .add(MetadataSync._ID)
            .add(MetadataSync.RAW_CONTACT_BACKUP_ID)
            .add(MetadataSync.ACCOUNT_TYPE)
            .add(MetadataSync.ACCOUNT_NAME)
            .add(MetadataSync.DATA_SET)
            .add(MetadataSync.DATA)
            .add(MetadataSync.DELETED)
            .build();

    private ContactsDatabaseHelper mDbHelper;
    private ContactsProvider2 mContactsProvider;

    @Override
    public boolean onCreate() {
        final Context context = getContext();
        mDbHelper = getDatabaseHelper(context);
        final IContentProvider iContentProvider = context.getContentResolver().acquireProvider(
                ContactsContract.AUTHORITY);
        final ContentProvider provider = ContentProvider.coerceToLocalContentProvider(
                iContentProvider);
        mContactsProvider = (ContactsProvider2) provider;
        return true;
    }

    protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
        return ContactsDatabaseHelper.getInstance(context);
    }

    @VisibleForTesting
    protected void setDatabaseHelper(final ContactsDatabaseHelper helper) {
        mDbHelper = helper;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: uri=" + uri + "  projection=" + Arrays.toString(projection) +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  order=[" + sortOrder + "] CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }

        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String limit = getLimit(uri);
        qb.setTables(Views.METADATA_SYNC);
        qb.setProjectionMap(sMetadataProjectionMap);
        qb.setStrict(true);

        final SelectionBuilder selectionBuilder = new SelectionBuilder(selection);

        final int match = sURIMatcher.match(uri);
        switch (match) {
            case METADATA_SYNC:
                break;

            case METADATA_SYNC_ID: {
                selectionBuilder.addClause(getEqualityClause(MetadataSync._ID,
                        ContentUris.parseId(uri)));
                break;
            }

            case RAW_CONTACTS:
            case RAW_CONTACTS_ID:
            case DATA: // Includes data usage stats query.
            case DATA_ID:
            case AGGREGATION_EXCEPTIONS:
                return mContactsProvider.query(
                        uri, projection, selection, selectionArgs, sortOrder);

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();
        return qb.query(db, projection, selectionBuilder.build(), selectionArgs, null,
                null, sortOrder, limit);
    }

    @Override
    public String getType(Uri uri) {
        int match = sURIMatcher.match(uri);
        switch (match) {
            case METADATA_SYNC:
                return MetadataSync.CONTENT_TYPE;
            case METADATA_SYNC_ID:
                return MetadataSync.CONTENT_ITEM_TYPE;
            case RAW_CONTACTS:
                return ContactsContract.RawContacts.CONTENT_TYPE;
            case RAW_CONTACTS_ID:
                return ContactsContract.RawContacts.CONTENT_ITEM_TYPE;
            case DATA:
                return ContactsContract.Data.CONTENT_TYPE;
            case DATA_ID:
                return mContactsProvider.getType(uri);
            case AGGREGATION_EXCEPTIONS:
                return ContactsContract.AggregationExceptions.CONTENT_TYPE;
            default:
                throw new IllegalArgumentException("Unknown URI: " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            // Insert the new entry, and also parse the data column to update related tables.
            final long metadataSyncId = updateOrInsertDataToMetadataSync(
                    db, uri, values, /* isInsert = */ true);
            db.setTransactionSuccessful();
            return ContentUris.withAppendedId(uri, metadataSyncId);
        } finally {
            db.endTransaction();
        }
}

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            final int matchedUriId = sURIMatcher.match(uri);
            int numDeletes = 0;
            switch (matchedUriId) {
                case METADATA_SYNC:
                    Cursor c = db.query(Views.METADATA_SYNC, new String[]{MetadataSync._ID},
                            selection, selectionArgs, null, null, null);
                    try {
                        while (c.moveToNext()) {
                            final long contactMetadataId = c.getLong(0);
                            numDeletes += db.delete(Tables.METADATA_SYNC,
                                    MetadataSync._ID + "=" + contactMetadataId, null);
                        }
                    } finally {
                        c.close();
                    }
                    db.setTransactionSuccessful();
                    return numDeletes;
                default:
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Calling contact metadata delete on an unknown/invalid URI", uri));
            }
        } finally {
            db.endTransaction();
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            // Update the metadata entry and parse the data column to update related tables.
            updateOrInsertDataToMetadataSync(db, uri, values, /* isInsert = */ false);
            db.setTransactionSuccessful();
            return 1;
        } finally {
            db.endTransaction();
        }
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "applyBatch: " + operations.size() + " ops");
        }
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            ContentProviderResult[] results = super.applyBatch(operations);
            db.setTransactionSuccessful();
            return results;
        } finally {
            db.endTransaction();
        }
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "bulkInsert: " + values.length + " inserts");
        }
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            final int numValues = super.bulkInsert(uri, values);
            db.setTransactionSuccessful();
            return numValues;
        } finally {
            db.endTransaction();
        }
    }

    /**
     * Insert or update a non-deleted entry to MetadataSync table, and also parse the data column
     * to update related tables for the raw contact.
     * Set 'isInsert' as true if it's insert and false if update.
     * Returns new inserted metadataSyncId if it's insert, and returns 1 if it's update.
     */
    private long updateOrInsertDataToMetadataSync(SQLiteDatabase db, Uri uri, ContentValues values,
            boolean isInsert) {
        final int matchUri = sURIMatcher.match(uri);
        if ((isInsert && matchUri != METADATA_SYNC) ||
                (!isInsert && matchUri != METADATA_SYNC && matchUri != METADATA_SYNC_ID)) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Calling contact metadata insert or update on an unknown/invalid URI", uri));
        }

        // Don't insert or update a deleted metadata.
        Integer deleted = values.getAsInteger(MetadataSync.DELETED);
        if (deleted != null && deleted != 0) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Cannot insert or update deleted metadata:" + values.toString(), uri));
        }

        // Check if data column is empty or null.
        final String data = values.getAsString(MetadataSync.DATA);
        if (TextUtils.isEmpty(data)) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Data column cannot be empty.", uri));
        }

        long result = 0;
        if (matchUri == METADATA_SYNC_ID) {
            // Update for the metadataSyncId.
            final long metadataSyncId = ContentUris.parseId(uri);
            final String selection = MetadataSync._ID + "=?";
            final String[] selectionArgs = new String[1];
            selectionArgs[0] = String.valueOf(metadataSyncId);
            db.update(Tables.METADATA_SYNC, values, selection, selectionArgs);
            result = 1;
        } else {
            // Update or insert for backupId and account info.
            final Long accountId = replaceAccountInfoByAccountId(uri, values);
            final String rawContactBackupId = values.getAsString(MetadataSync.RAW_CONTACT_BACKUP_ID);
            deleted = 0; //Only insert or update non-deleted metadata
            if (accountId == null || rawContactBackupId == null) {
                throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                        "Invalid identifier is found: accountId=" + accountId + "; " +
                                "rawContactBackupId=" + rawContactBackupId, uri));
            }

            if (isInsert) {
                result = mDbHelper.insertMetadataSync(rawContactBackupId, accountId, data, deleted);
                if (result <= 0) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Metadata insertion failed. Values= " + values.toString(), uri));
                }
            } else {
                mDbHelper.updateMetadataSync(rawContactBackupId, accountId, data, deleted);
                result = 1;
            }
        }

        // Parse the data column and update other tables.
        // Data field will never be empty or null, since contacts prefs and usage stats
        // have default values.
        final MetadataEntry metadataEntry = MetadataEntryParser.parseDataToMetaDataEntry(data);
        mContactsProvider.updateFromMetaDataEntry(db, metadataEntry);

        return result;
    }

    /**
     *  Replace account_type, account_name and data_set with account_id. If a valid account_id
     *  cannot be found for this combination, return null.
     */
    private Long replaceAccountInfoByAccountId(Uri uri, ContentValues values) {
        String accountName = values.getAsString(MetadataSync.ACCOUNT_NAME);
        String accountType = values.getAsString(MetadataSync.ACCOUNT_TYPE);
        String dataSet = values.getAsString(MetadataSync.DATA_SET);
        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);
        if (partialUri) {
            // Throw when either account is incomplete.
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri));
        }

        final AccountWithDataSet account = AccountWithDataSet.get(
                accountName, accountType, dataSet);

        final Long id = mDbHelper.getAccountIdOrNull(account);
        if (id == null) {
            return null;
        }

        values.put(MetadataSyncColumns.ACCOUNT_ID, id);
        // Only remove the account information once the account ID is extracted (since these
        // fields are actually used by resolveAccountWithDataSet to extract the relevant ID).
        values.remove(MetadataSync.ACCOUNT_NAME);
        values.remove(MetadataSync.ACCOUNT_TYPE);
        values.remove(MetadataSync.DATA_SET);

        return id;
    }
}
