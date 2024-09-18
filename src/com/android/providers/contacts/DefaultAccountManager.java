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
import android.accounts.AccountManager;
import android.content.Context;
import android.content.res.Resources;
import android.util.Log;

import com.android.providers.contacts.util.NeededForTesting;

import java.util.Arrays;
import java.util.HashSet;

/**
 * A utility class to provide methods to load and set the default account.
 */
@NeededForTesting
public class DefaultAccountManager {
    private static final String TAG = "DefaultAccountManager";

    private static HashSet<String> sEligibleSystemCloudAccountTypes = null;

    private final Context mContext;
    private final ContactsDatabaseHelper mDbHelper;
    private final SyncSettingsHelper mSyncSettingsHelper;
    private final AccountManager mAccountManager;

    DefaultAccountManager(Context context, ContactsDatabaseHelper dbHelper) {
        this(context, dbHelper, new SyncSettingsHelper(), AccountManager.get(context));
    }

    // Keep it in proguard for testing: once it's used in production code, remove this annotation.
    @NeededForTesting
    DefaultAccountManager(Context context, ContactsDatabaseHelper dbHelper,
            SyncSettingsHelper syncSettingsHelper, AccountManager accountManager) {
        mContext = context;
        mDbHelper = dbHelper;
        mSyncSettingsHelper = syncSettingsHelper;
        mAccountManager = accountManager;
    }

    private static synchronized HashSet<String> getEligibleSystemAccountTypes(Context context) {
        if (sEligibleSystemCloudAccountTypes == null) {
            sEligibleSystemCloudAccountTypes = new HashSet<>();
            Resources res = context.getResources();
            String[] accountTypesArray = res.getStringArray(
                    R.array.eligible_system_cloud_account_types);
            sEligibleSystemCloudAccountTypes.addAll(Arrays.asList(accountTypesArray));
        }
        return sEligibleSystemCloudAccountTypes;
    }

    /**
     * Try to push an account as the default account.
     *
     * @param defaultAccount account to be set as the default account.
     * @return true if the default account is successfully updated.
     */
    @NeededForTesting
    public boolean tryPushDefaultAccount(DefaultAccount defaultAccount) {
        if (!isValidDefaultAccount(defaultAccount)) {
            Log.w(TAG, "Attempt to push an invalid default account.");
            return false;
        }

        DefaultAccount previousDefaultAccount = pullDefaultAccount();

        if (defaultAccount.equals(previousDefaultAccount)) {
            Log.w(TAG, "Account has already been set as default before");
            return false;
        }

        directlySetDefaultAccountInDb(defaultAccount);
        return true;
    }

    private boolean isValidDefaultAccount(DefaultAccount defaultAccount) {
        if (defaultAccount.getAccountCategory() == DefaultAccount.AccountCategory.CLOUD) {
            return defaultAccount.getCloudAccount() != null
                    && isSystemCloudAccount(defaultAccount.getCloudAccount())
                    && !mSyncSettingsHelper.isSyncOff(defaultAccount.getCloudAccount());
        }
        return defaultAccount.getCloudAccount() == null;
    }

    /**
     * Pull the default account from the DB.
     */
    @NeededForTesting
    public DefaultAccount pullDefaultAccount() {
        DefaultAccount defaultAccount = getDefaultAccountFromDb();

        if (isValidDefaultAccount(defaultAccount)) {
            return defaultAccount;
        } else {
            directlySetDefaultAccountInDb(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
            return DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT;
        }
    }

    private void directlySetDefaultAccountInDb(DefaultAccount defaultAccount) {
        switch (defaultAccount.getAccountCategory()) {
            case UNKNOWN: {
                mDbHelper.clearDefaultAccount();
                break;
            }
            case DEVICE: {
                mDbHelper.setDefaultAccount(AccountWithDataSet.LOCAL.getAccountName(),
                        AccountWithDataSet.LOCAL.getAccountType());
                break;
            }
            case CLOUD:
                mDbHelper.setDefaultAccount(defaultAccount.getCloudAccount().name,
                        defaultAccount.getCloudAccount().type);
                break;
            default:
                Log.e(TAG, "Incorrect default account category");
                break;
        }
    }

    private boolean isSystemCloudAccount(Account account) {
        if (account == null || !getEligibleSystemAccountTypes(mContext).contains(account.type)) {
            return false;
        }

        Account[] accountsInThisType = mAccountManager.getAccountsByType(account.type);
        for (Account currentAccount : accountsInThisType) {
            if (currentAccount.equals(account)) {
                return true;
            }
        }
        return false;
    }

    private DefaultAccount getDefaultAccountFromDb() {
        Account[] defaultAccountFromDb = mDbHelper.getDefaultAccountIfAny();
        if (defaultAccountFromDb.length == 0) {
            return DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT;
        }

        if (defaultAccountFromDb[0] == null) {
            return DefaultAccount.DEVICE_DEFAULT_ACCOUNT;
        }

        if (defaultAccountFromDb[0].name.equals(AccountWithDataSet.LOCAL.getAccountName())
                && defaultAccountFromDb[0].type.equals(AccountWithDataSet.LOCAL.getAccountType())) {
            return DefaultAccount.DEVICE_DEFAULT_ACCOUNT;
        }

        return DefaultAccount.ofCloud(defaultAccountFromDb[0]);
    }
}
