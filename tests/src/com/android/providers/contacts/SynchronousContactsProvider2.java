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

/**
 * A version of {@link ContactsProvider2} class that performs aggregation
 * synchronously and wipes all data at construction time.
 */
public class SynchronousContactsProvider2 extends ContactsProvider2 {
    private static Boolean sDataWiped = false;
    private static OpenHelper mOpenHelper;
    private Account mAccount;

    public SynchronousContactsProvider2() {
        super(new SynchronousAggregationScheduler());
    }

    @Override
    protected OpenHelper getOpenHelper(final Context context) {
        if (mOpenHelper == null) {
            mOpenHelper = new OpenHelper(context);
        }
        return mOpenHelper;
    }

    public static void resetOpenHelper() {
        mOpenHelper = null;
    }

    @Override
    public boolean onCreate() {
        boolean created = super.onCreate();
        synchronized (sDataWiped) {
            if (!sDataWiped) {
                sDataWiped = true;
                wipeData();
            }
        }
        return created;
    }

    @Override
    protected Account getDefaultAccount() {
        if (mAccount == null) {
            mAccount = new Account("androidtest@gmail.com", "com.google.GAIA");
        }
        return mAccount;
    }

    @Override
    protected boolean isLegacyContactImportNeeded() {

        // We have an explicit test for data conversion - no need to do it every time
        return false;
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
            super.run();
        }

    }
}
