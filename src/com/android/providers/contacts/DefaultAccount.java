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

/**
 * Represents a default account with a category (UNKNOWN, DEVICE, or CLOUD)
 * and an optional associated Android Account object.
 */
public class DefaultAccount {
    /**
     * The possible categories for a DefaultAccount.
     */
    public enum AccountCategory {
        /**
         * The account category is unknown. This is usually a temporary state.
         */
        UNKNOWN,

        /**
         * The account is a device-only account and not synced to the cloud.
         */
        DEVICE,

        /**
         * The account is synced to the cloud.
         */
        CLOUD
    }


    public static final DefaultAccount UNKNOWN_DEFAULT_ACCOUNT = new DefaultAccount(
            AccountCategory.UNKNOWN, null);
    public static final DefaultAccount DEVICE_DEFAULT_ACCOUNT = new DefaultAccount(
            AccountCategory.DEVICE, null);

    /**
     * Create a DefaultAccount object which points to the cloud.
     * @param cloudAccount The cloud account that is being set as the default account.
     * @return The DefaultAccount object.
     */
    public static DefaultAccount ofCloud(Account cloudAccount) {
        return new DefaultAccount(AccountCategory.CLOUD, cloudAccount);
    }

    private final AccountCategory mAccountCategory;
    private final Account mCloudAccount;

    /**
     * Constructs a DefaultAccount object.
     *
     * @param accountCategory The category of the default account.
     * @param cloudAccount    The account when mAccountCategory is CLOUD (null for
     *                        DEVICE/UNKNOWN).
     * @throws IllegalArgumentException If cloudAccount is null when accountCategory is
     *                                  CLOUD,
     *                                  or if cloudAccount is not null when accountCategory is not
     *                                  CLOUD.
     */
    public DefaultAccount(AccountCategory accountCategory, Account cloudAccount) {
        this.mAccountCategory = accountCategory;

        // Validate cloudAccount based on accountCategory
        if (accountCategory == AccountCategory.CLOUD && cloudAccount == null) {
            throw new IllegalArgumentException(
                    "Cloud account cannot be null when category is CLOUD");
        } else if (accountCategory != AccountCategory.CLOUD && cloudAccount != null) {
            throw new IllegalArgumentException(
                    "Cloud account should be null when category is not CLOUD");
        }

        this.mCloudAccount = cloudAccount;
    }

    /**
     * Gets the category of the account.
     *
     * @return The current category (UNKNOWN, DEVICE, or CLOUD).
     */
    public AccountCategory getAccountCategory() {
        return mAccountCategory;
    }

    /**
     * Gets the associated cloud account, if available.
     *
     * @return The Android Account object, or null if the category is not CLOUD.
     */
    public Account getCloudAccount() {
        return mCloudAccount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true; // Same object
        if (o == null || getClass() != o.getClass()) return false; // Null or different class

        DefaultAccount that = (DefaultAccount) o;

        // Compare account categories first for efficiency
        if (mAccountCategory != that.mAccountCategory) return false;

        // If categories match, compare cloud accounts depending on category
        if (mAccountCategory == AccountCategory.CLOUD) {
            return mCloudAccount.equals(that.mCloudAccount); // Use Account's equals
        } else {
            return true; // Categories match and cloud account is irrelevant
        }
    }

    @Override
    public int hashCode() {
        int result = mAccountCategory.hashCode();
        if (mAccountCategory == AccountCategory.CLOUD) {
            result = 31 * result + mCloudAccount.hashCode(); // Use Account's hashCode
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("{mAccountCategory: %s, mCloudAccount: %s}",
                mAccountCategory, mCloudAccount);
    }

}
