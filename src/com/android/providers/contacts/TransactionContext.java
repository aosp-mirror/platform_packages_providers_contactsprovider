/*
 * Copyright (C) 2010 The Android Open Source Project
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

import static com.android.providers.contacts.flags.Flags.cp2SyncSearchIndexFlag;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.ArrayMap;
import android.util.ArraySet;

import java.util.Map.Entry;
import java.util.Set;

/**
 * Accumulates information for an entire transaction. {@link ContactsProvider2} consumes
 * it at commit time.
 */
public class TransactionContext  {

    private final boolean mForProfile;
    /** Map from raw contact id to account Id */
    private ArrayMap<Long, Long> mInsertedRawContactsAccounts;
    private ArraySet<Long> mUpdatedRawContacts;
    private ArraySet<Long> mBackupIdChangedRawContacts;
    private ArraySet<Long> mDirtyRawContacts;
    // Set used to track what has been changed and deleted. This is needed so we can update the
    // contact last touch timestamp.  Dirty set above is only set when sync adapter is false.
    // {@see android.provider.ContactsContract#CALLER_IS_SYNCADAPTER}. While the set below will
    // contain all changed contacts.
    private ArraySet<Long> mChangedRawContacts;
    private ArraySet<Long> mStaleSearchIndexRawContacts;
    private ArraySet<Long> mStaleSearchIndexContacts;
    private ArrayMap<Long, Object> mUpdatedSyncStates;

    private boolean mIsStaleSearchIndexTableCreated = false;

    public TransactionContext(boolean forProfile) {
        mForProfile = forProfile;
    }

    public boolean isForProfile() {
        return mForProfile;
    }

    public void rawContactInserted(long rawContactId, long accountId) {
        if (mInsertedRawContactsAccounts == null) mInsertedRawContactsAccounts = new ArrayMap<>();
        mInsertedRawContactsAccounts.put(rawContactId, accountId);

        markRawContactChangedOrDeletedOrInserted(rawContactId);
    }

    public void rawContactUpdated(long rawContactId) {
        if (mUpdatedRawContacts == null) mUpdatedRawContacts = new ArraySet<>();
        mUpdatedRawContacts.add(rawContactId);
    }

    public void markRawContactDirtyAndChanged(long rawContactId, boolean isSyncAdapter) {
        if (!isSyncAdapter) {
            if (mDirtyRawContacts == null) {
                mDirtyRawContacts = new ArraySet<>();
            }
            mDirtyRawContacts.add(rawContactId);
        }

        markRawContactChangedOrDeletedOrInserted(rawContactId);
    }

    public void markRawContactChangedOrDeletedOrInserted(long rawContactId) {
        if (mChangedRawContacts == null) {
            mChangedRawContacts = new ArraySet<>();
        }
        mChangedRawContacts.add(rawContactId);
    }

    public void syncStateUpdated(long rowId, Object data) {
        if (mUpdatedSyncStates == null) mUpdatedSyncStates = new ArrayMap<>();
        mUpdatedSyncStates.put(rowId, data);
    }

    public void invalidateSearchIndexForRawContact(SQLiteDatabase db, long rawContactId) {
        if (!cp2SyncSearchIndexFlag()) {
            if (mStaleSearchIndexRawContacts == null) {
                mStaleSearchIndexRawContacts = new ArraySet<>();
            }
            mStaleSearchIndexRawContacts.add(rawContactId);
            return;
        }
        createStaleSearchIndexTableIfNotExists(db);
        db.execSQL("""
                INSERT OR IGNORE INTO stale_search_index_contacts
                    SELECT raw_contacts.contact_id
                    FROM raw_contacts
                    WHERE raw_contacts._id = ?""",
                new Long[]{rawContactId});
    }

    public void invalidateSearchIndexForContact(SQLiteDatabase db, long contactId) {
        if (!cp2SyncSearchIndexFlag()) {
            if (mStaleSearchIndexContacts == null) mStaleSearchIndexContacts = new ArraySet<>();
            mStaleSearchIndexContacts.add(contactId);
            return;
        }
        createStaleSearchIndexTableIfNotExists(db);
        db.execSQL("INSERT OR IGNORE INTO stale_search_index_contacts VALUES (?)",
                new Long[]{contactId});
    }

    public Set<Long> getInsertedRawContactIds() {
        if (mInsertedRawContactsAccounts == null) mInsertedRawContactsAccounts = new ArrayMap<>();
        return mInsertedRawContactsAccounts.keySet();
    }

    public Set<Long> getUpdatedRawContactIds() {
        if (mUpdatedRawContacts == null) mUpdatedRawContacts = new ArraySet<>();
        return mUpdatedRawContacts;
    }

    public Set<Long> getDirtyRawContactIds() {
        if (mDirtyRawContacts == null) mDirtyRawContacts = new ArraySet<>();
        return mDirtyRawContacts;
    }

    public Set<Long> getChangedRawContactIds() {
        if (mChangedRawContacts == null) mChangedRawContacts = new ArraySet<>();
        return mChangedRawContacts;
    }

    public Set<Long> getStaleSearchIndexRawContactIds() {
        if (cp2SyncSearchIndexFlag()) {
            throw new UnsupportedOperationException();
        }
        if (mStaleSearchIndexRawContacts == null) mStaleSearchIndexRawContacts = new ArraySet<>();
        return mStaleSearchIndexRawContacts;
    }

    public Set<Long> getStaleSearchIndexContactIds() {
        if (cp2SyncSearchIndexFlag()) {
            throw new UnsupportedOperationException();
        }
        if (mStaleSearchIndexContacts == null) mStaleSearchIndexContacts = new ArraySet<>();
        return mStaleSearchIndexContacts;
    }

    public long getStaleSearchIndexContactIdsCount(SQLiteDatabase db) {
        createStaleSearchIndexTableIfNotExists(db);
        try (Cursor cursor =
                db.rawQuery("SELECT COUNT(*) FROM stale_search_index_contacts", null)) {
            return cursor.moveToFirst() ? cursor.getLong(0) : 0;
        }
    }

    public Set<Entry<Long, Object>> getUpdatedSyncStates() {
        if (mUpdatedSyncStates == null) mUpdatedSyncStates = new ArrayMap<>();
        return mUpdatedSyncStates.entrySet();
    }

    public Long getAccountIdOrNullForRawContact(long rawContactId) {
        if (mInsertedRawContactsAccounts == null) mInsertedRawContactsAccounts = new ArrayMap<>();
        return mInsertedRawContactsAccounts.get(rawContactId);
    }

    public boolean isNewRawContact(long rawContactId) {
        if (mInsertedRawContactsAccounts == null) mInsertedRawContactsAccounts = new ArrayMap<>();
        return mInsertedRawContactsAccounts.containsKey(rawContactId);
    }

    public void clearExceptSearchIndexUpdates() {
        mInsertedRawContactsAccounts = null;
        mUpdatedRawContacts = null;
        mUpdatedSyncStates = null;
        mDirtyRawContacts = null;
        mChangedRawContacts = null;
        mBackupIdChangedRawContacts = null;
    }

    public void clearSearchIndexUpdates(SQLiteDatabase db) {
        if (cp2SyncSearchIndexFlag()) {
            db.delete("stale_search_index_contacts", null, null);
        } else {
            mStaleSearchIndexRawContacts = null;
            mStaleSearchIndexContacts = null;
        }
    }

    public void clearAll(SQLiteDatabase db) {
        clearExceptSearchIndexUpdates();
        clearSearchIndexUpdates(db);
    }

    private void createStaleSearchIndexTableIfNotExists(SQLiteDatabase db) {
        if (!mIsStaleSearchIndexTableCreated) {
            db.execSQL("""
                    CREATE TEMP TABLE IF NOT EXISTS
                     stale_search_index_contacts (id INTEGER PRIMARY KEY)""");
            mIsStaleSearchIndexTableCreated = true;
        }
    }
}
