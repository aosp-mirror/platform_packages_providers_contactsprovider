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
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Binder;
import android.provider.ContactsContract.MetadataSync;
import android.text.TextUtils;
import android.util.Log;
import com.android.common.content.ProjectionMap;
import com.android.providers.contacts.ContactsDatabaseHelper.MetadataSyncColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Views;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.UserUtils;
import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.Map;

import static com.android.providers.contacts.ContactsProvider2.getLimit;
import static com.android.providers.contacts.ContactsProvider2.getQueryParameter;
import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

/**
 * Simple content provider to handle directing contact metadata specific calls.
 */
public class ContactMetadataProvider extends ContentProvider {
    private static final String TAG = "ContactMetadata";
    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);
    private static final int METADATA_SYNC = 1;
    private static final int METADATA_SYNC_ID = 2;

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    static {
        sURIMatcher.addURI(MetadataSync.METADATA_AUTHORITY, "metadata_sync", METADATA_SYNC);
        sURIMatcher.addURI(MetadataSync.METADATA_AUTHORITY, "metadata_sync/#", METADATA_SYNC_ID);
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

    @Override
    public boolean onCreate() {
        final Context context = getContext();
        mDbHelper = getDatabaseHelper(context);
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
            default:
                throw new IllegalArgumentException("Unknown URI: " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        if (sURIMatcher.match(uri) != METADATA_SYNC) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Calling contact metadata insert on an unknown/invalid URI" , uri));
        }

        // Don't insert deleted metadata.
        Integer deleted = values.getAsInteger(MetadataSync.DELETED);
        if (deleted != null && deleted != 0) {
            // Cannot insert deleted metadata
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Cannot insert deleted metadata:" + values.toString(), uri));
        }

        // Insert the new entry.
        // Populate the relevant values before inserting the new entry into the database.
        final Long accountId = replaceAccountInfoByAccountId(uri, values);
        final String rawContactBackupId = values.getAsString(MetadataSync.RAW_CONTACT_BACKUP_ID);
        final String data = values.getAsString(MetadataSync.DATA);
        deleted = 0; //Only insert non-deleted metadata

        if (accountId == null || rawContactBackupId == null) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Invalid identifier is found: accountId=" + accountId + "; " +
                            "rawContactBackupId=" + rawContactBackupId, uri));
        }

        Long metadataSyncId = mDbHelper.replaceMetadataSync(rawContactBackupId, accountId, data,
                deleted);
        if (metadataSyncId < 0) {
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Metadata insertion failed. Values= " + values.toString(), uri));
        }

        return ContentUris.withAppendedId(uri, metadataSyncId);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
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
