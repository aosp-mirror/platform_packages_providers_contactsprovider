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

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteStatement;
import android.provider.ContactsContract.Contacts;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

/**
 * A version of the ContactAggregator for use against the profile database.
 */
public class ProfileAggregator extends ContactAggregator {

    private SQLiteStatement mProfileContactIdLookup;

    public ProfileAggregator(ContactsProvider2 contactsProvider,
            ContactsDatabaseHelper contactsDatabaseHelper,
            PhotoPriorityResolver photoPriorityResolver, NameSplitter nameSplitter,
            CommonNicknameCache commonNicknameCache) {
        super(contactsProvider, contactsDatabaseHelper, photoPriorityResolver, nameSplitter,
                commonNicknameCache);

        SQLiteDatabase db = contactsDatabaseHelper.getReadableDatabase();
        mProfileContactIdLookup = db.compileStatement(
                "SELECT " + Contacts._ID +
                " FROM " + Tables.CONTACTS +
                " ORDER BY " + Contacts._ID +
                " LIMIT 1");
    }

    @Override
    public String computeLookupKeyForContact(SQLiteDatabase db, long contactId) {
        return ContactLookupKey.PROFILE_LOOKUP_KEY;
    }

    @Override
    public long onRawContactInsert(TransactionContext txContext, SQLiteDatabase db,
            long rawContactId) {
        // Profile aggregation on raw contact insert is simple - find the single contact in the
        // database and attach to that.
        long contactId = -1;
        try {
            contactId = mProfileContactIdLookup.simpleQueryForLong();
            updateAggregateData(txContext, contactId);
        } catch (SQLiteDoneException e) {
            // No valid contact ID found, so create one.
            contactId = insertContact(db, rawContactId);
        }
        setContactId(rawContactId, contactId);
        return contactId;
    }
}
