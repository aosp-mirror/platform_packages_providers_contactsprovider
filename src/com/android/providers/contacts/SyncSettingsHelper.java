/*
 * Copyright (C) 2024 The Android Open Source Project
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

import com.android.providers.contacts.util.NeededForTesting;

import java.util.HashMap;
import java.util.Map;

@NeededForTesting
public class SyncSettingsHelper {
    @NeededForTesting
    public enum SyncState { ON, OFF }

    // TODO: Currently the sync state are stored in memory, which will be hooked up with the real
    // sync settings.
    private final Map<Account, SyncState> mSyncStates;

    public SyncSettingsHelper() {
        mSyncStates = new HashMap<>();
    }

    /**
     * Turns on sync for the given account.
     *
     * @param account The account for which sync should be turned on.
     */
    @NeededForTesting
    public void turnOnSync(Account account) {
        mSyncStates.put(account, SyncState.ON);
    }

    /**
     * Turns off sync for the given account.
     *
     * @param account The account for which sync should be turned off.
     */
    @NeededForTesting
    public void turnOffSync(Account account) {
        mSyncStates.put(account, SyncState.OFF);
    }

    /**
     * Checks if sync is turned off for the given account.
     *
     * @param account The account to check.
     * @return false if sync is off, true otherwise.
     */
    @NeededForTesting
    public boolean isSyncOff(Account account) {
        return mSyncStates.get(account) == SyncState.OFF;
    }
}

