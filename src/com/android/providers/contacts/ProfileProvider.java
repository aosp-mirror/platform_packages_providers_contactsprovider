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

import com.android.providers.contacts.util.ContactsPermissions;

import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.Binder;
import android.os.CancellationSignal;
import android.provider.ContactsContract.Intents;

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Locale;

/**
 * Simple content provider to handle directing profile-specific calls against a separate
 * database from the rest of contacts.
 */
public class ProfileProvider extends AbstractContactsProvider {
    private static final String READ_CONTACTS_PERMISSION = "android.permission.READ_CONTACTS";

    // The Contacts provider handles most of the logic - this provider is only invoked when the
    // URI belongs to a profile action, setting up the proper database.
    private final ContactsProvider2 mDelegate;

    public ProfileProvider(ContactsProvider2 delegate) {
        mDelegate = delegate;
    }

    @Override
    protected ProfileDatabaseHelper newDatabaseHelper(Context context) {
        return ProfileDatabaseHelper.getInstance(context);
    }

    public ProfileDatabaseHelper getDatabaseHelper() {
        return (ProfileDatabaseHelper) super.getDatabaseHelper();
    }

    @Override
    protected ThreadLocal<ContactsTransaction> getTransactionHolder() {
        return mDelegate.getTransactionHolder();
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        return query(uri, projection, selection, selectionArgs, sortOrder, null);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder, CancellationSignal cancellationSignal) {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementQueryStats(callingUid);
        try {
            return mDelegate.queryLocal(uri, projection, selection, selectionArgs, sortOrder, -1,
                    cancellationSignal);
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    protected Uri insertInTransaction(Uri uri, ContentValues values) {
        useProfileDbForTransaction();
        return mDelegate.insertInTransaction(uri, values);
    }

    @Override
    protected int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        useProfileDbForTransaction();
        return mDelegate.updateInTransaction(uri, values, selection, selectionArgs);
    }

    @Override
    protected int deleteInTransaction(Uri uri, String selection, String[] selectionArgs) {
        useProfileDbForTransaction();
        return mDelegate.deleteInTransaction(uri, selection, selectionArgs);
    }

    @Override
    public AssetFileDescriptor openAssetFile(Uri uri, String mode) throws FileNotFoundException {
        return mDelegate.openAssetFileLocal(uri, mode);
    }

    private void useProfileDbForTransaction() {
        ContactsTransaction transaction = getCurrentTransaction();
        SQLiteDatabase db = getDatabaseHelper().getWritableDatabase();
        transaction.startTransactionForDb(db, ContactsProvider2.PROFILE_DB_TAG, this);
    }

    @Override
    protected void notifyChange() {
        mDelegate.notifyChange();
    }

    protected void notifyChange(boolean syncToNetwork, boolean syncToMetadataNetWork) {
        mDelegate.notifyChange(syncToNetwork, syncToMetadataNetWork);
    }

    protected Locale getLocale() {
        return mDelegate.getLocale();
    }

    @Override
    public void onBegin() {
        mDelegate.onBeginTransactionInternal(true);
    }

    @Override
    public void onCommit() {
        mDelegate.onCommitTransactionInternal(true);
        sendProfileChangedBroadcast();
    }

    @Override
    public void onRollback() {
        mDelegate.onRollbackTransactionInternal(true);
    }

    @Override
    protected boolean yield(ContactsTransaction transaction) {
        return mDelegate.yield(transaction);
    }

    @Override
    public String getType(Uri uri) {
        return mDelegate.getType(uri);
    }

    /** Use only for debug logging */
    @Override
    public String toString() {
        return "ProfileProvider";
    }

    private void sendProfileChangedBroadcast() {
        final Intent intent = new Intent(Intents.ACTION_PROFILE_CHANGED);
        mDelegate.getContext().sendBroadcast(intent, READ_CONTACTS_PERMISSION);
        // TODO b/35323708 update user profile data here instead of notifying Settings
        intent.setPackage("com.android.settings");
        mDelegate.getContext().sendBroadcast(intent, READ_CONTACTS_PERMISSION);
    }

    @Override
    public void dump(FileDescriptor fd, PrintWriter pw, String[] args) {
        dump(pw, "Profile");
    }
}
