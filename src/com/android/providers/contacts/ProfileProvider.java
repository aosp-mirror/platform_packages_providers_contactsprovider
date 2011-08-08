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

import com.android.common.content.SQLiteContentProvider;

import android.accounts.Account;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.net.Uri;

import java.io.FileNotFoundException;
import java.util.Locale;

/**
 * Simple content provider to handle directing profile-specific calls against a separate
 * database from the rest of contacts.
 */
public class ProfileProvider extends SQLiteContentProvider {

    private static final String READ_PERMISSION = "android.permission.READ_PROFILE";
    private static final String WRITE_PERMISSION = "android.permission.WRITE_PROFILE";

    // The Contacts provider handles most of the logic - this provider is only invoked when the
    // URI belongs to a profile action, setting up the proper database.
    private final ContactsProvider2 mDelegate;

    public ProfileProvider(ContactsProvider2 delegate) {
        mDelegate = delegate;
    }

    private void enforceReadPermission() {
        mDelegate.getContext().enforceCallingOrSelfPermission(READ_PERMISSION, null);
    }

    private void enforceWritePermission() {
        mDelegate.getContext().enforceCallingOrSelfPermission(WRITE_PERMISSION, null);
    }

    @Override
    protected ProfileDatabaseHelper getDatabaseHelper(Context context) {
        return ProfileDatabaseHelper.getInstance(context);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        enforceReadPermission();
        if (mDb == null) {
            mDb = getDatabaseHelper().getReadableDatabase();
        }
        mDelegate.substituteDb(mDb);
        return mDelegate.queryLocal(uri, projection, selection, selectionArgs, sortOrder, -1);
    }

    @Override
    protected Uri insertInTransaction(Uri uri, ContentValues values) {
        enforceWritePermission();
        mDelegate.substituteDb(mDb);
        return mDelegate.insertInTransaction(uri, values);
    }

    @Override
    protected int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        enforceWritePermission();
        mDelegate.substituteDb(mDb);
        return mDelegate.updateInTransaction(uri, values, selection, selectionArgs);
    }

    @Override
    protected int deleteInTransaction(Uri uri, String selection, String[] selectionArgs) {
        enforceWritePermission();
        mDelegate.substituteDb(mDb);
        return mDelegate.deleteInTransaction(uri, selection, selectionArgs);
    }

    @Override
    public AssetFileDescriptor openAssetFile(Uri uri, String mode) throws FileNotFoundException {
        if (mode != null && mode.contains("w")) {
            enforceWritePermission();
            mDelegate.substituteDb(getDatabaseHelper().getWritableDatabase());
        } else {
            enforceReadPermission();
            mDelegate.substituteDb(getDatabaseHelper().getReadableDatabase());
        }
        return mDelegate.openAssetFileLocal(uri, mode);
    }

    @Override
    protected void notifyChange() {
        mDelegate.notifyChange();
    }

    protected void notifyChange(boolean syncToNetwork) {
        mDelegate.notifyChange(syncToNetwork);
    }

    protected void scheduleBackgroundTask(int task) {
        mDelegate.scheduleBackgroundTask(task);
    }

    protected void scheduleBackgroundTask(int task, Object arg) {
        mDelegate.scheduleBackgroundTask(task, arg);
    }

    protected void performBackgroundTask(int task, Object arg) {
        mDelegate.performBackgroundTask(task, arg);
    }

    protected void updateLocaleInBackground() {
        mDelegate.updateLocaleInBackground();
    }

    protected void updateDirectoriesInBackground(boolean rescan) {
        mDelegate.updateDirectoriesInBackground(rescan);
    }

    protected Account getDefaultAccount() {
        return mDelegate.getDefaultAccount();
    }

    protected boolean isContactsAccount(Account account) {
        return mDelegate.isContactsAccount(account);
    }

    protected Locale getLocale() {
        return mDelegate.getLocale();
    }

    protected boolean isWritableAccountWithDataSet(String accountTypeAndDataSet) {
        return mDelegate.isWritableAccountWithDataSet(accountTypeAndDataSet);
    }

    protected void onBeginTransaction() {
        mDelegate.onBeginTransaction();
    }

    protected void beforeTransactionCommit() {
        mDelegate.beforeTransactionCommit();
    }

    @Override
    public String getType(Uri uri) {
        return mDelegate.getType(uri);
    }
}
