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
 * limitations under the License.
 */

package com.android.providers.contacts;

import android.accounts.Account;
import android.content.Context;

import java.util.Locale;

/**
 * A version of {@link ProfileProvider} that performs all operations synchronously.
 */
public class SynchronousProfileProvider extends ProfileProvider {

    public static final String READ_ONLY_ACCOUNT_TYPE = "ro";

    private static ProfileDatabaseHelper mDbHelper;

    private Account mAccount;

    public SynchronousProfileProvider(ContactsProvider2 delegate) {
        super(delegate);
    }

    @Override
    protected ProfileDatabaseHelper getDatabaseHelper(final Context context) {
        if (mDbHelper == null) {
            mDbHelper = new ProfileDatabaseHelper(context);
        }
        return mDbHelper;
    }

    public static void resetOpenHelper() {
        mDbHelper = null;
    }

    @Override
    protected void onBeginTransaction() {
        super.onBeginTransaction();
    }

    @Override
    protected void scheduleBackgroundTask(int task) {
        performBackgroundTask(task, null);
    }

    @Override
    protected void scheduleBackgroundTask(int task, Object arg) {
        performBackgroundTask(task, arg);
    }

    @Override
    protected void updateLocaleInBackground() {
    }

    @Override
    protected void updateDirectoriesInBackground(boolean rescan) {
    }

    @Override
    protected Account getDefaultAccount() {
        if (mAccount == null) {
            mAccount = new Account("androidtest@gmail.com", "com.google");
        }
        return mAccount;
    }

    @Override
    protected boolean isContactsAccount(Account account) {
        return true;
    }

    @Override
    protected Locale getLocale() {
        return Locale.US;
    }

    @Override
    protected boolean isWritableAccountWithDataSet(String accountType) {
        return !READ_ONLY_ACCOUNT_TYPE.equals(accountType);
    }
}
