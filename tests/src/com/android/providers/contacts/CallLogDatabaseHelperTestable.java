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

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

public class CallLogDatabaseHelperTestable extends CallLogDatabaseHelper {
    private final SQLiteDatabase mContactsDb;

    public CallLogDatabaseHelperTestable(Context context,
            SQLiteDatabase contactsDb) {
        super(context, null /* "null" DB name to make it an in-memory DB */);
        mContactsDb = contactsDb;
    }

    @Override
    SQLiteDatabase getContactsWritableDatabaseForMigration() {
        return mContactsDb;
    }
}
