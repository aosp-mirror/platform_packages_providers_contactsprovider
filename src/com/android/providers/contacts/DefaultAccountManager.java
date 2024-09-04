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
import android.annotation.Nullable;
import android.content.Context;
import android.content.res.Resources;
import android.text.TextUtils;
import android.util.Log;
import android.util.Pair;

import com.android.providers.contacts.util.NeededForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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
     * @param account account to be set as the default account.
     * @return true if the default account is successfully updated.
     */
    @NeededForTesting
    public boolean tryPushDefaultAccount(@Nullable Account account) {
        Pair<Boolean, DefaultAccount> result = tryConvertToDefaultAccount(account);
        if (!result.first) {
            Log.w(TAG, "Account is not eligible to set as default account");
            return false;
        }

        return tryPushDefaultAccount(result.second);
    }

    private boolean tryPushDefaultAccount(DefaultAccount defaultAccount) {
        DefaultAccount previousDefaultAccount = pullDefaultAccount();

        if (defaultAccount.equals(previousDefaultAccount)) {
            Log.w(TAG, "Account has already been set as default before");
            return false;
        }

        if (checkNextDefaultAccount(previousDefaultAccount, defaultAccount)) {
            directlySetDefaultAccountInDb(defaultAccount);
            return true;
        }
        return false;
    }

    /**
     * Pull the default account from the DB.
     */
    @NeededForTesting
    public DefaultAccount pullDefaultAccount() {
        DefaultAccount defaultAccount = getDefaultAccountFromDb();

        Pair<Boolean, DefaultAccount> validationResult = validateCurrentDefaultAccount(
                defaultAccount);
        if (validationResult.first) {
            return defaultAccount;
        }

        directlySetDefaultAccountInDb(validationResult.second);
        return validationResult.second;
    }

    private Pair<Boolean, DefaultAccount> tryConvertToDefaultAccount(@Nullable Account account) {
        if (account == null
                || (TextUtils.isEmpty(account.name) && TextUtils.isEmpty(account.type))
                || (account.name.equals(AccountWithDataSet.LOCAL.getAccountName())
                    && account.type.equals(AccountWithDataSet.LOCAL.getAccountType()))) {
            return new Pair<>(true,
                    new DefaultAccount(DefaultAccount.AccountCategory.DEVICE, null));
        }

        Account[] systemCloudAccounts = getSystemCloudAccounts(/*includeSyncOffAccounts=*/true);
        for (Account systemCloudAccount : systemCloudAccounts) {
            if (account.equals(systemCloudAccount)) {
                return new Pair<>(true,
                        new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, account));
            }
        }
        return new Pair<>(false, null);
    }

    private void directlySetDefaultAccountInDb(DefaultAccount defaultAccount) {
        switch (defaultAccount.getAccountCategory()) {
            case UNKNOWN: {
                mDbHelper.clearDefaultAccount();
                break;
            }
            case DEVICE: {
                for (Account account : getSystemCloudAccounts(/*includeSyncOffAccounts=*/ false)) {
                    mSyncSettingsHelper.turnOffSync(account);
                }

                mDbHelper.setDefaultAccount(AccountWithDataSet.LOCAL.getAccountName(),
                        AccountWithDataSet.LOCAL.getAccountType());
                break;
            }
            case CLOUD:
                mSyncSettingsHelper.turnOnSync(defaultAccount.getCloudAccount());
                mDbHelper.setDefaultAccount(defaultAccount.getCloudAccount().name,
                        defaultAccount.getCloudAccount().type);
                break;
            default:
                Log.e(TAG, "Incorrect default account category");
                break;
        }
    }

    private boolean isSystemCloudAccounts(Account account) {
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

    private Account[] getSystemCloudAccounts(boolean includeSyncOffAccounts) {
        Account[] accountsOnDevice = mAccountManager.getAccounts();
        List<Account> systemCloudAccount = new ArrayList<>();

        for (Account account : accountsOnDevice) {
            if (getEligibleSystemAccountTypes(mContext).contains(account.type)) {
                if (includeSyncOffAccounts || !mSyncSettingsHelper.isSyncOff(account)) {
                    systemCloudAccount.add(account);
                }
            }
        }
        return systemCloudAccount.toArray(new Account[0]);
    }

    private DefaultAccount getDefaultAccountFromDb() {
        Account[] defaultAccountFromDb = mDbHelper.getDefaultAccountIfAny();
        if (defaultAccountFromDb.length == 0) {
            return new DefaultAccount(DefaultAccount.AccountCategory.UNKNOWN, null);
        }

        if (defaultAccountFromDb[0] == null) {
            return new DefaultAccount(DefaultAccount.AccountCategory.DEVICE, null);
        }

        if (defaultAccountFromDb[0].name.equals(AccountWithDataSet.LOCAL.getAccountName())
                && defaultAccountFromDb[0].type.equals(AccountWithDataSet.LOCAL.getAccountType())) {
            return new DefaultAccount(DefaultAccount.AccountCategory.DEVICE, null);
        }

        return new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, defaultAccountFromDb[0]);
    }

    /**
     * returns empty array if the currentDefaultAccount is valid. Or size-1 array with the validated
     * default account
     * as the only element
     */
    private Pair<Boolean, DefaultAccount> validateCurrentDefaultAccount(
            DefaultAccount currentDefaultAccount) {
        // Implement the validation.
        Account[] systemCloudAccountsWithSyncOn = getSystemCloudAccounts(false);

        // No system cloud account, the default account will be DEVICE.
        if (systemCloudAccountsWithSyncOn.length == 0) {
            return new Pair<>(currentDefaultAccount.equals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT),
                    DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        }

        if (currentDefaultAccount.getAccountCategory() == DefaultAccount.AccountCategory.CLOUD) {
            for (Account systemCloudAccount : systemCloudAccountsWithSyncOn) {
                if (systemCloudAccount.equals(currentDefaultAccount.getCloudAccount())) {
                    return new Pair<>(true, currentDefaultAccount);
                }
            }
        }

        // There are system cloud account, but the current default account is not pointing to any of
        // those, the default account will be UNKNOWN.
        return new Pair<>(currentDefaultAccount.equals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT),
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
    }

    private boolean checkNextDefaultAccount(DefaultAccount currentDefaultAccount,
            DefaultAccount nextDefaultAccount) {
        if (nextDefaultAccount.equals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT)) {
            // User can never set the next default account to UNKNOWN state. The UNKNOWN state is a
            // natural result of signing up or turning off/on sync.
            return false;
        }

        // TODO(b/356383714): revisit whether to allow set default account is DEVICE, even if it's
        // not unknown. To do this, we must turn off syncs for all cloud accounts before setting it
        // to DEVICE.
        if (nextDefaultAccount.equals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT)) {
            // User can only set to default account to DEVICE when the default account was unknown.
            return currentDefaultAccount.equals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        }

        return isSystemCloudAccounts(nextDefaultAccount.getCloudAccount());
    }
}
