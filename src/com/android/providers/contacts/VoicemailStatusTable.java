/*
 * Copyright (C) 2011 The Android Open Source Project
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

import static com.android.providers.contacts.util.DbQueryUtils.concatenateClauses;

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.provider.VoicemailContract.Status;
import android.util.ArraySet;

import com.android.common.content.ProjectionMap;
import com.android.providers.contacts.VoicemailContentProvider.UriData;

/**
 * Implementation of {@link VoicemailTable.Delegate} for the voicemail status table.
 *
 * Public methods of this class are thread-safe as it is used in a content provider, which should
 * be thread-safe.
 */
public class VoicemailStatusTable implements VoicemailTable.Delegate {

    private static final ProjectionMap sStatusProjectionMap = new ProjectionMap.Builder()
            .add(Status._ID)
            .add(Status.PHONE_ACCOUNT_COMPONENT_NAME)
            .add(Status.PHONE_ACCOUNT_ID)
            .add(Status.CONFIGURATION_STATE)
            .add(Status.DATA_CHANNEL_STATE)
            .add(Status.NOTIFICATION_CHANNEL_STATE)
            .add(Status.SETTINGS_URI)
            .add(Status.SOURCE_PACKAGE)
            .add(Status.VOICEMAIL_ACCESS_URI)
            .add(Status.QUOTA_OCCUPIED)
            .add(Status.QUOTA_TOTAL)
            .add(Status.SOURCE_TYPE)
            .build();

    private static final Object DATABASE_LOCK = new Object();

    private final String mTableName;
    private final Context mContext;
    private final CallLogDatabaseHelper mDbHelper;
    private final VoicemailTable.DelegateHelper mDelegateHelper;

    public VoicemailStatusTable(String tableName, Context context, CallLogDatabaseHelper dbHelper,
            VoicemailTable.DelegateHelper delegateHelper) {
        mTableName = tableName;
        mContext = context;
        mDbHelper = dbHelper;
        mDelegateHelper = delegateHelper;
    }

    @Override
    public Uri insert(UriData uriData, ContentValues values) {
        synchronized (DATABASE_LOCK) {
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            // Try to update before insert.
            String combinedClause = uriData.getWhereClause();
            int rowsChanged = createDatabaseModifier(db)
                    .update(uriData.getUri(), mTableName, values, combinedClause, null);
            if (rowsChanged != 0) {
                final String[] selection = new String[] {Status._ID};
                Cursor c = db.query(mTableName, selection, combinedClause, null, null, null, null);
                c.moveToFirst();
                int rowId = c.getInt(0);
                c.close();
                return ContentUris.withAppendedId(uriData.getUri(), rowId);
            }
            ContentValues copiedValues = new ContentValues(values);
            mDelegateHelper.checkAndAddSourcePackageIntoValues(uriData, copiedValues);
            long rowId = createDatabaseModifier(db).insert(mTableName, null, copiedValues);
            if (rowId > 0) {
                return ContentUris.withAppendedId(uriData.getUri(), rowId);
            } else {
                return null;
            }
        }
    }

    @Override
    public int bulkInsert(UriData uriData, ContentValues[] values) {
        int count = 0;
        for (ContentValues value : values) {
            Uri uri = insert(uriData, value);
            if (uri != null) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int delete(UriData uriData, String selection, String[] selectionArgs) {
        synchronized (DATABASE_LOCK) {
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            String combinedClause = concatenateClauses(selection, uriData.getWhereClause());
            return createDatabaseModifier(db).delete(mTableName, combinedClause,
                    selectionArgs);
        }
    }

    @Override
    public Cursor query(UriData uriData, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        synchronized (DATABASE_LOCK) {
            SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(mTableName);
            qb.setProjectionMap(sStatusProjectionMap);
            qb.setStrict(true);

            String combinedClause = concatenateClauses(selection, uriData.getWhereClause());
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            Cursor c = qb
                    .query(db, projection, combinedClause, selectionArgs, null, null, sortOrder);
            if (c != null) {
                c.setNotificationUri(mContext.getContentResolver(), Status.CONTENT_URI);
            }
            return c;
        }
    }

    @Override
    public int update(UriData uriData, ContentValues values, String selection,
            String[] selectionArgs) {
        synchronized (DATABASE_LOCK) {
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            String combinedClause = concatenateClauses(selection, uriData.getWhereClause());
            return createDatabaseModifier(db)
                    .update(uriData.getUri(), mTableName, values, combinedClause, selectionArgs);
        }
    }

    @Override
    public String getType(UriData uriData) {
        if (uriData.hasId()) {
            return Status.ITEM_TYPE;
        } else {
            return Status.DIR_TYPE;
        }
    }

    @Override
    public ParcelFileDescriptor openFile(UriData uriData, String mode) {
        throw new UnsupportedOperationException("File operation is not supported for status table");
    }

    private DatabaseModifier createDatabaseModifier(SQLiteDatabase db) {
        return new DbModifierWithNotification(mTableName, db, mContext);
    }

    @Override
    public ArraySet<String> getSourcePackages() {
        return mDbHelper.selectDistinctColumn(mTableName, Status.SOURCE_PACKAGE);
    }
}
