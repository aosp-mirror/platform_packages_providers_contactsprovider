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

import android.util.ArrayMap;
import android.util.ArraySet;

import com.google.android.collect.Maps;
import com.google.android.collect.Sets;

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
    private ArraySet<Long> mMetadataDirtyRawContacts;
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

    public void markRawContactMetadataDirty(long rawContactId, boolean isMetadataSyncAdapter) {
        if (!isMetadataSyncAdapter) {
            if (mMetadataDirtyRawContacts == null) {
                mMetadataDirtyRawContacts = new ArraySet<>();
            }
            mMetadataDirtyRawContacts.add(rawContactId);
        }
    }

    public void markBackupIdChangedRawContact(long rawContactId) {
        if (mBackupIdChangedRawContacts == null) {
            mBackupIdChangedRawContacts = new ArraySet<>();
        }
        mBackupIdChangedRawContacts.add(rawContactId);
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

    public void invalidateSearchIndexForRawContact(long rawContactId) {
        if (mStaleSearchIndexRawContacts == null) mStaleSearchIndexRawContacts = new ArraySet<>();
        mStaleSearchIndexRawContacts.add(rawContactId);
    }

    public void invalidateSearchIndexForContact(long contactId) {
        if (mStaleSearchIndexContacts == null) mStaleSearchIndexContacts = new ArraySet<>();
        mStaleSearchIndexContacts.add(contactId);
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

    public Set<Long> getMetadataDirtyRawContactIds() {
        if (mMetadataDirtyRawContacts == null) mMetadataDirtyRawContacts = new ArraySet<>();
        return mMetadataDirtyRawContacts;
    }

    public Set<Long> getBackupIdChangedRawContacts() {
        if (mBackupIdChangedRawContacts == null) mBackupIdChangedRawContacts = new ArraySet<>();
        return mBackupIdChangedRawContacts;
    }

    public Set<Long> getChangedRawContactIds() {
        if (mChangedRawContacts == null) mChangedRawContacts = new ArraySet<>();
        return mChangedRawContacts;
    }

    public Set<Long> getStaleSearchIndexRawContactIds() {
        if (mStaleSearchIndexRawContacts == null) mStaleSearchIndexRawContacts = new ArraySet<>();
        return mStaleSearchIndexRawContacts;
    }

    public Set<Long> getStaleSearchIndexContactIds() {
        if (mStaleSearchIndexContacts == null) mStaleSearchIndexContacts = new ArraySet<>();
        return mStaleSearchIndexContacts;
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
        mMetadataDirtyRawContacts = null;
        mChangedRawContacts = null;
        mBackupIdChangedRawContacts = null;
    }

    public void clearSearchIndexUpdates() {
        mStaleSearchIndexRawContacts = null;
        mStaleSearchIndexContacts = null;
    }

    public void clearAll() {
        clearExceptSearchIndexUpdates();
        clearSearchIndexUpdates();
    }
}
