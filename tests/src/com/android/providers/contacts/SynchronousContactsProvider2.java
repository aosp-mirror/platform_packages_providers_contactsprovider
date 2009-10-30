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
 * limitations under the License.
 */

package com.android.providers.contacts;

import android.accounts.Account;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

/**
 * A version of {@link ContactsProvider2} class that performs aggregation
 * synchronously and wipes all data at construction time.
 */
public class SynchronousContactsProvider2 extends ContactsProvider2 {
    private static Boolean sDataWiped = false;
    private static ContactsDatabaseHelper mDbHelper;
    private boolean mDataWipeEnabled = true;
    private Account mAccount;
    private boolean mNetworkNotified;

    public SynchronousContactsProvider2() {
        this(new SynchronousAggregationScheduler());
    }

    public SynchronousContactsProvider2(ContactAggregationScheduler scheduler) {
        super(scheduler);
    }

    @Override
    protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
        if (mDbHelper == null) {
            mDbHelper = new ContactsDatabaseHelper(context);
        }
        return mDbHelper;
    }

    public static void resetOpenHelper() {
        mDbHelper = null;
    }

    public void setDataWipeEnabled(boolean flag) {
        mDataWipeEnabled = flag;
    }

    @Override
    protected void onBeginTransaction() {
        super.onBeginTransaction();
        mNetworkNotified = false;
    }

    @Override
    protected void notifyChange(boolean syncToNetwork) {
        mNetworkNotified |= syncToNetwork;
    }

    public boolean isNetworkNotified() {
        return mNetworkNotified;
    }

    @Override
    public boolean onCreate() {
        boolean created = super.onCreate();
        if (mDataWipeEnabled) {
            synchronized (sDataWiped) {
                if (!sDataWiped) {
                    sDataWiped = true;
                    wipeData();
                }
            }
        }
        return created;
    }

    @Override
    protected void verifyAccounts() {
    }

    @Override
    protected Account getDefaultAccount() {
        if (mAccount == null) {
            mAccount = new Account("androidtest@gmail.com", "com.google");
        }
        return mAccount;
    }

    @Override
    protected boolean isWritableAccount(Account account) {
        return true;
    }

    public void prepareForFullAggregation(int maxContact) {
        SQLiteDatabase db = getDatabaseHelper().getWritableDatabase();
        db.execSQL("UPDATE raw_contacts SET aggregation_mode=0,aggregation_needed=1;");
        long rowId =
            db.compileStatement("SELECT _id FROM raw_contacts LIMIT 1 OFFSET " + maxContact)
                .simpleQueryForLong();
        db.execSQL("DELETE FROM raw_contacts WHERE _id > " + rowId + ";");
    }

    public long getRawContactCount() {
        SQLiteDatabase db = getDatabaseHelper().getReadableDatabase();
        return db.compileStatement("SELECT COUNT(*) FROM raw_contacts").simpleQueryForLong();
    }

    public long getContactCount() {
        SQLiteDatabase db = getDatabaseHelper().getReadableDatabase();
        return db.compileStatement("SELECT COUNT(*) FROM contacts").simpleQueryForLong();
    }

    @Override
    public void wipeData() {
        super.wipeData();
        SQLiteDatabase db = getDatabaseHelper().getWritableDatabase();
        db.execSQL("replace into SQLITE_SEQUENCE (name,seq) values('raw_contacts', 42)");
        db.execSQL("replace into SQLITE_SEQUENCE (name,seq) values('contacts', 2009)");
        db.execSQL("replace into SQLITE_SEQUENCE (name,seq) values('data', 777)");
    }

    @Override
    protected boolean isLegacyContactImportNeeded() {

        // We have an explicit test for data conversion - no need to do it every time
        return false;
    }

    public void aggregate() {
        ContactAggregationScheduler scheduler = getContactAggregationScheduler();
        scheduler.run();
    }

    private static class SynchronousAggregationScheduler extends ContactAggregationScheduler {

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        long currentTime() {
            return 0;
        }

        @Override
        void runDelayed() {
//            super.run();
        }

    }
}
