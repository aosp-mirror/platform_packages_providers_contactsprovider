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
 * A subclass of {@link SynchronousContactsProvider2} that uses a different DB for secondary users.
 */
public class SecondaryUserContactsProvider2 extends SynchronousContactsProvider2 {
    private final String mDbSuffix;
    private ContactsDatabaseHelper mDbHelper;

    public SecondaryUserContactsProvider2(int userId) {
        mDbSuffix = "-u" + userId;
    }

    @Override
    public ContactsDatabaseHelper newDatabaseHelper(final Context context) {
        if (mDbHelper == null) {
            mDbHelper = ContactsDatabaseHelper.getNewInstanceForTest(context,
                    TestUtils.getContactsDatabaseFilename(context, mDbSuffix));
        }
        return mDbHelper;
    }
}
