/*
 * Copyright (C) 2014 The Android Open Source Project
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

import android.content.Context;

/**
 * A subclass of {@link SynchronousContactsProvider2} that doesn't reuse the database helper.
 */
public class StandaloneContactsProvider2 extends SynchronousContactsProvider2 {
    private static ContactsDatabaseHelper mDbHelper;

    public StandaloneContactsProvider2() {
        // No need to wipe data for this instance since it doesn't reuse the db helper.
        setDataWipeEnabled(false);
    }

    @Override
    protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
        if (mDbHelper == null) {
            mDbHelper = ContactsDatabaseHelper.getNewInstanceForTest(context);
        }
        return mDbHelper;
    }

    @Override
    public void setDataWipeEnabled(boolean flag) {
        // No need to wipe data for this instance since it doesn't reuse the db helper.
        super.setDataWipeEnabled(false);
    }
}
