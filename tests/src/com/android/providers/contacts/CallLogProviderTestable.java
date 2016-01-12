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
 * limitations under the License.
 */
package com.android.providers.contacts;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.provider.CallLog.Calls;
import junit.framework.Assert;

public class CallLogProviderTestable extends CallLogProvider {
    private CallLogDatabaseHelperTestable mDbHelper;

    private SQLiteDatabase mContactsDbForMigration;

    public void setContactsDbForMigration(SQLiteDatabase db) {
        Assert.assertNull(
                "setContactsDbForMigration() must be called before DB helepr is instantiated",
                mDbHelper);
        mContactsDbForMigration = db;
    }

    @Override
    protected CallLogDatabaseHelper getDatabaseHelper(final Context context) {
        if (mDbHelper == null) {
            mDbHelper = new CallLogDatabaseHelperTestable(context, mContactsDbForMigration);
        }
        return mDbHelper;
    }

    @Override
    protected CallLogInsertionHelper createCallLogInsertionHelper(Context context) {
        return new CallLogInsertionHelper() {
            @Override
            public String getGeocodedLocationFor(String number, String countryIso) {
                return "usa";
            }

            @Override
            public void addComputedValues(ContentValues values) {
                values.put(Calls.COUNTRY_ISO, "us");
                values.put(Calls.GEOCODED_LOCATION, "usa");
            }
        };
    }
}
