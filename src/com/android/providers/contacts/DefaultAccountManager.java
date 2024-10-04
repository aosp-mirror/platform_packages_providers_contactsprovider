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
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;
import android.util.Log;

import com.android.internal.R;
import com.android.providers.contacts.util.NeededForTesting;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    private static synchronized Set<String> getEligibleSystemAccountTypes(Context context) {
        if (sEligibleSystemCloudAccountTypes == null) {
            sEligibleSystemCloudAccountTypes = new HashSet<>();

            Resources resources = Resources.getSystem();
            String[] accountTypesArray =
                    resources.getStringArray(R.array.config_rawContactsEligibleDefaultAccountTypes);

            sEligibleSystemCloudAccountTypes.addAll(Arrays.asList(accountTypesArray));
        }
        return sEligibleSystemCloudAccountTypes;
    }

    @NeededForTesting
    static synchronized void setEligibleSystemCloudAccountTypesForTesting(String[] accountTypes) {
        sEligibleSystemCloudAccountTypes = new HashSet<>(Arrays.asList(accountTypes));
    }

    /**
     * Try to push an account as the default account.
     *
     * @param defaultAccount account to be set as the default account.
     * @return true if the default account is successfully updated.
     */
    @NeededForTesting
    public boolean tryPushDefaultAccount(DefaultAccountAndState defaultAccount) {
        if (!isValidDefaultAccount(defaultAccount)) {
            Log.w(TAG, "Attempt to push an invalid default account.");
            return false;
        }

        DefaultAccountAndState previousDefaultAccount = pullDefaultAccount();

        if (defaultAccount.equals(previousDefaultAccount)) {
            Log.w(TAG, "Account has already been set as default before");
            return false;
        }

        directlySetDefaultAccountInDb(defaultAccount);
        return true;
    }

    private boolean isValidDefaultAccount(DefaultAccountAndState defaultAccount) {
        if (defaultAccount.getState() == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD) {
            return defaultAccount.getAccount() != null
                    && isCloudAccount(defaultAccount.getAccount());
        }
        return defaultAccount.getAccount() == null;
    }

    /**
     * Pull the default account from the DB.
     */
    @NeededForTesting
    public DefaultAccountAndState pullDefaultAccount() {
        DefaultAccountAndState defaultAccount = getDefaultAccountFromDb();

        if (isValidDefaultAccount(defaultAccount)) {
            return defaultAccount;
        } else {
            Log.w(TAG, "Default account stored in the DB is no longer valid.");
            directlySetDefaultAccountInDb(DefaultAccountAndState.ofNotSet());
            return DefaultAccountAndState.ofNotSet();
        }
    }

    private void directlySetDefaultAccountInDb(DefaultAccountAndState defaultAccount) {
        switch (defaultAccount.getState()) {
            case DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_NOT_SET: {
                mDbHelper.clearDefaultAccount();
                break;
            }
            case DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_LOCAL: {
                mDbHelper.setDefaultAccount(AccountWithDataSet.LOCAL.getAccountName(),
                        AccountWithDataSet.LOCAL.getAccountType());
                break;
            }
            case DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD:
                assert defaultAccount.getAccount() != null;
                mDbHelper.setDefaultAccount(defaultAccount.getAccount().name,
                        defaultAccount.getAccount().type);
                break;
            default:
                Log.e(TAG, "Incorrect default account category");
                break;
        }
    }

    private boolean isCloudAccount(Account account) {
        if (account == null) {
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

    private DefaultAccountAndState getDefaultAccountFromDb() {
        Account[] defaultAccountFromDb = mDbHelper.getDefaultAccountIfAny();
        if (defaultAccountFromDb.length == 0) {
            return DefaultAccountAndState.ofNotSet();
        }

        if (defaultAccountFromDb[0] == null) {
            return DefaultAccountAndState.ofLocal();
        }

        if (defaultAccountFromDb[0].name.equals(AccountWithDataSet.LOCAL.getAccountName())
                && defaultAccountFromDb[0].type.equals(AccountWithDataSet.LOCAL.getAccountType())) {
            return DefaultAccountAndState.ofLocal();
        }

        return DefaultAccountAndState.ofCloud(defaultAccountFromDb[0]);
    }
}
