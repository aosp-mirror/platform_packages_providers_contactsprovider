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

import static android.Manifest.permission.READ_VOICEMAIL;

import android.content.ComponentName;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.DatabaseUtils.InsertHelper;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.Binder;
import android.provider.CallLog.Calls;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.util.ArraySet;

import com.android.common.io.MoreCloseables;
import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.CallLogDatabaseHelper.Tables;
import com.android.providers.contacts.util.DbQueryUtils;

import com.google.android.collect.Lists;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Set;

/**
 * An implementation of {@link DatabaseModifier} for voicemail related tables which additionally
 * generates necessary notifications after the modification operation is performed.
 * The class generates notifications for both voicemail as well as call log URI depending on which
 * of then got affected by the change.
 */
public class DbModifierWithNotification implements DatabaseModifier {

    private static final String TAG = "DbModifierWithNotify";

    private static final String[] PROJECTION = new String[] {
            VoicemailContract.SOURCE_PACKAGE_FIELD
    };
    private static final int SOURCE_PACKAGE_COLUMN_INDEX = 0;
    private static final String NON_NULL_SOURCE_PACKAGE_SELECTION =
            VoicemailContract.SOURCE_PACKAGE_FIELD + " IS NOT NULL";
    private static final String NOT_DELETED_SELECTION =
            Voicemails.DELETED + " == 0";
    private final String mTableName;
    private final SQLiteDatabase mDb;
    private final InsertHelper mInsertHelper;
    private final Context mContext;
    private final Uri mBaseUri;
    private final boolean mIsCallsTable;
    private final VoicemailNotifier mVoicemailNotifier;

    private boolean mIsBulkOperation = false;

    private static VoicemailNotifier sVoicemailNotifierForTest;

    public DbModifierWithNotification(String tableName, SQLiteDatabase db, Context context) {
        this(tableName, db, null, context);
    }

    public DbModifierWithNotification(String tableName, InsertHelper insertHelper,
            Context context) {
        this(tableName, null, insertHelper, context);
    }

    private DbModifierWithNotification(String tableName, SQLiteDatabase db,
            InsertHelper insertHelper, Context context) {
        mTableName = tableName;
        mDb = db;
        mInsertHelper = insertHelper;
        mContext = context;
        mBaseUri = mTableName.equals(Tables.VOICEMAIL_STATUS) ?
                Status.CONTENT_URI : Voicemails.CONTENT_URI;
        mIsCallsTable = mTableName.equals(Tables.CALLS);
        mVoicemailNotifier = sVoicemailNotifierForTest != null ? sVoicemailNotifierForTest
                : new VoicemailNotifier(mContext, mBaseUri);
    }

    @Override
    public long insert(String table, String nullColumnHack, ContentValues values) {
        Set<String> packagesModified = getModifiedPackages(values);
        if (mIsCallsTable) {
            values.put(Calls.LAST_MODIFIED, getTimeMillis());
        }
        long rowId = mDb.insert(table, nullColumnHack, values);
        if (rowId > 0 && packagesModified.size() != 0) {
            notifyVoicemailChangeOnInsert(ContentUris.withAppendedId(mBaseUri, rowId),
                    packagesModified);
        }
        if (rowId > 0 && mIsCallsTable) {
            notifyCallLogChange();
        }
        return rowId;
    }

    @Override
    public long insert(ContentValues values) {
        Set<String> packagesModified = getModifiedPackages(values);
        if (mIsCallsTable) {
            values.put(Calls.LAST_MODIFIED, getTimeMillis());
        }
        long rowId = mInsertHelper.insert(values);
        if (rowId > 0 && packagesModified.size() != 0) {
            notifyVoicemailChangeOnInsert(
                    ContentUris.withAppendedId(mBaseUri, rowId), packagesModified);
        }
        if (rowId > 0 && mIsCallsTable) {
            notifyCallLogChange();
        }
        return rowId;
    }

    private void notifyCallLogChange() {
        mContext.getContentResolver().notifyChange(Calls.CONTENT_URI, null, false);

        Intent intent = new Intent("com.android.internal.action.CALL_LOG_CHANGE");
        intent.setComponent(new ComponentName("com.android.calllogbackup",
                "com.android.calllogbackup.CallLogChangeReceiver"));

        if (!mContext.getPackageManager().queryBroadcastReceivers(intent, 0).isEmpty()) {
            mContext.sendBroadcast(intent);
        }
    }

    private void notifyVoicemailChangeOnInsert(
            Uri notificationUri, Set<String> packagesModified) {
        if (mIsCallsTable) {
            mVoicemailNotifier.addIntentActions(VoicemailContract.ACTION_NEW_VOICEMAIL);
        }
        notifyVoicemailChange(notificationUri, packagesModified);
    }

    private void notifyVoicemailChange(Uri notificationUri,
            Set<String> modifiedPackages) {
        mVoicemailNotifier.addUri(notificationUri);
        mVoicemailNotifier.addModifiedPackages(modifiedPackages);
        mVoicemailNotifier.addIntentActions(Intent.ACTION_PROVIDER_CHANGED);
        if (!mIsBulkOperation) {
            mVoicemailNotifier.sendNotification();
        }
    }

    @Override
    public int update(Uri uri, String table, ContentValues values, String whereClause,
            String[] whereArgs) {
        Set<String> packagesModified = getModifiedPackages(whereClause, whereArgs);
        packagesModified.addAll(getModifiedPackages(values));

        boolean isVoicemailContent =
                packagesModified.size() != 0 && isUpdatingVoicemailColumns(values);

        boolean hasMarkedRead = false;
        if (mIsCallsTable) {
            if (values.containsKey(Voicemails.DELETED)
                    && !values.getAsBoolean(Voicemails.DELETED)) {
                values.put(Calls.LAST_MODIFIED, getTimeMillis());
            } else {
                updateLastModified(table, whereClause, whereArgs);
            }
            if (isVoicemailContent) {
                if (updateDirtyFlag(values, packagesModified)) {
                    if (values.containsKey(Calls.IS_READ)
                            && getAsBoolean(values,
                            Calls.IS_READ)) {
                        // If the server has set the IS_READ, it should also unset the new flag
                        if (!values.containsKey(Calls.NEW)) {
                            values.put(Calls.NEW, 0);
                            hasMarkedRead = true;
                        }
                    }
                }
            }
        }
        // updateDirtyFlag might remove the value and leave values empty.
        if (values.isEmpty()) {
            return 0;
        }
        int count = mDb.update(table, values, whereClause, whereArgs);
        if (count > 0 && isVoicemailContent || Tables.VOICEMAIL_STATUS.equals(table)) {
            notifyVoicemailChange(mBaseUri, packagesModified);
        }
        if (count > 0 && mIsCallsTable) {
            notifyCallLogChange();
        }
        if (hasMarkedRead) {
            // A "New" voicemail has been marked as read by the server. This voicemail is no longer
            // new but the content consumer might still think it is. ACTION_NEW_VOICEMAIL should
            // trigger a rescan of new voicemails.
            mContext.sendBroadcast(
                    new Intent(VoicemailContract.ACTION_NEW_VOICEMAIL, uri),
                    READ_VOICEMAIL);
        }
        return count;
    }

    private boolean updateDirtyFlag(ContentValues values, Set<String> packagesModified) {
        // If a calling package is modifying its own entries, it means that the change came
        // from the server and thus is synced or "clean". Otherwise, it means that a local
        // change is being made to the database, so the entries should be marked as "dirty"
        // so that the corresponding sync adapter knows they need to be synced.
        int isDirty;
        Integer callerSetDirty = values.getAsInteger(Voicemails.DIRTY);
        if (callerSetDirty != null) {
            // Respect the calling package if it sets the dirty flag
            if (callerSetDirty == Voicemails.DIRTY_RETAIN) {
                values.remove(Voicemails.DIRTY);
                return false;
            } else {
                isDirty = callerSetDirty == 0 ? 0 : 1;
            }
        } else {
            isDirty = isSelfModifyingOrInternal(packagesModified) ? 0 : 1;
        }

        values.put(Voicemails.DIRTY, isDirty);
        return isDirty == 0;
    }

    private boolean isUpdatingVoicemailColumns(ContentValues values) {
        for (String key : values.keySet()) {
            if (VoicemailContentTable.ALLOWED_COLUMNS.contains(key)) {
                return true;
            }
        }
        return false;
    }

    private void updateLastModified(String table, String whereClause, String[] whereArgs) {
        ContentValues values = new ContentValues();
        values.put(Calls.LAST_MODIFIED, getTimeMillis());

        mDb.update(table, values,
                DbQueryUtils.concatenateClauses(NOT_DELETED_SELECTION, whereClause),
                whereArgs);
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        Set<String> packagesModified = getModifiedPackages(whereClause, whereArgs);
        boolean isVoicemail = packagesModified.size() != 0;

        // If a deletion is made by a package that is not the package that inserted the voicemail,
        // this means that the user deleted the voicemail. However, we do not want to delete it from
        // the database until after the server has been notified of the deletion. To ensure this,
        // mark the entry as "deleted"--deleted entries should be hidden from the user.
        // Once the changes are synced to the server, delete will be called again, this time
        // removing the rows from the table.
        // If the deletion is being made by the package that inserted the voicemail or by
        // CP2 (cleanup after uninstall), then we don't need to wait for sync, so just delete it.
        final int count;
        if (mIsCallsTable && isVoicemail && !isSelfModifyingOrInternal(packagesModified)) {
            ContentValues values = new ContentValues();
            values.put(VoicemailContract.Voicemails.DIRTY, 1);
            values.put(VoicemailContract.Voicemails.DELETED, 1);
            values.put(VoicemailContract.Voicemails.LAST_MODIFIED, getTimeMillis());
            count = mDb.update(table, values, whereClause, whereArgs);
        } else {
            count = mDb.delete(table, whereClause, whereArgs);
        }

        if (count > 0 && isVoicemail) {
            notifyVoicemailChange(mBaseUri, packagesModified);
        }
        if (count > 0 && mIsCallsTable) {
            notifyCallLogChange();
        }
        return count;
    }

    @Override
    public void startBulkOperation() {
        mIsBulkOperation = true;
        mDb.beginTransaction();
    }

    @Override
    public void yieldBulkOperation() {
        mDb.yieldIfContendedSafely();
    }

    @Override
    public void finishBulkOperation() {
        mDb.setTransactionSuccessful();
        mDb.endTransaction();
        mIsBulkOperation = false;
        mVoicemailNotifier.sendNotification();
    }

    /**
     * Returns the set of packages affected when a modify operation is run for the specified
     * where clause. When called from an insert operation an empty set returned by this method
     * implies (indirectly) that this does not affect any voicemail entry, as a voicemail entry is
     * always expected to have the source package field set.
     */
    private Set<String> getModifiedPackages(String whereClause, String[] whereArgs) {
        Set<String> modifiedPackages = new ArraySet<>();
        Cursor cursor = mDb.query(mTableName, PROJECTION,
                DbQueryUtils.concatenateClauses(NON_NULL_SOURCE_PACKAGE_SELECTION, whereClause),
                whereArgs, null, null, null);
        while (cursor.moveToNext()) {
            modifiedPackages.add(cursor.getString(SOURCE_PACKAGE_COLUMN_INDEX));
        }
        MoreCloseables.closeQuietly(cursor);
        return modifiedPackages;
    }

    /**
     * Returns the source package that gets affected (in an insert/update operation) by the supplied
     * content values. An empty set returned by this method also implies (indirectly) that this does
     * not affect any voicemail entry, as a voicemail entry is always expected to have the source
     * package field set.
     */
    private Set<String> getModifiedPackages(ContentValues values) {
        Set<String> impactedPackages = new ArraySet<>();
        if (values.containsKey(VoicemailContract.SOURCE_PACKAGE_FIELD)) {
            impactedPackages.add(values.getAsString(VoicemailContract.SOURCE_PACKAGE_FIELD));
        }
        return impactedPackages;
    }

    /**
     * @param packagesModified source packages that inserted the voicemail that is being modified
     * @return {@code true} if the caller is modifying its own voicemail, or this is an internal
     * transaction, {@code false} otherwise.
     */
    private boolean isSelfModifyingOrInternal(Set<String> packagesModified) {
        final Collection<String> callingPackages = getCallingPackages();
        if (callingPackages == null) {
            return false;
        }
        // The last clause has the same effect as doing Process.myUid() == Binder.getCallingUid(),
        // but allows us to mock the results for testing.
        return packagesModified.size() == 1 && (callingPackages.contains(
                Iterables.getOnlyElement(packagesModified))
                || callingPackages.contains(mContext.getPackageName()));
    }

    /**
     * Returns the package names of the calling process. If the calling process has more than
     * one packages, this returns them all
     */
    private Collection<String> getCallingPackages() {
        int caller = Binder.getCallingUid();
        if (caller == 0) {
            return null;
        }
        return Lists.newArrayList(mContext.getPackageManager().getPackagesForUid(caller));
    }

    /**
     * A variant of {@link ContentValues#getAsBoolean(String)} that also treat the string "0" as
     * false and other integer string as true. 0, 1, false, true, "0", "1", "false", "true" might
     * all be inserted into the ContentValues as a boolean, but "0" and "1" are not handled by
     * {@link ContentValues#getAsBoolean(String)}
     */
    private static Boolean getAsBoolean(ContentValues values, String key) {
        Object value = values.get(key);
        if (value instanceof CharSequence) {
            try {
                int intValue = Integer.parseInt(value.toString());
                return intValue != 0;
            } catch (NumberFormatException nfe) {
                // Do nothing.
            }
        }
        return values.getAsBoolean(key);
    }

    private long getTimeMillis() {
        if (CallLogProvider.getTimeForTestMillis() == null) {
            return System.currentTimeMillis();
        }
        return CallLogProvider.getTimeForTestMillis();
    }

    @VisibleForTesting
    static void setVoicemailNotifierForTest(VoicemailNotifier notifier) {
        sVoicemailNotifierForTest = notifier;
    }
}
