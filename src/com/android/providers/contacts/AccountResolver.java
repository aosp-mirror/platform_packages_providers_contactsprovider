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
import android.content.ContentValues;
import android.net.Uri;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;
import android.provider.ContactsContract.SimAccount;
import android.text.TextUtils;

import java.util.List;

public class AccountResolver {
    private static final String TAG = "AccountResolver";

    private final ContactsDatabaseHelper mDbHelper;
    private final DefaultAccountManager mDefaultAccountManager;

    public AccountResolver(ContactsDatabaseHelper dbHelper,
            DefaultAccountManager defaultAccountManager) {
        mDbHelper = dbHelper;
        mDefaultAccountManager = defaultAccountManager;
    }

    private static Account getLocalAccount() {
        if (TextUtils.isEmpty(AccountWithDataSet.LOCAL.getAccountName())) {
            // AccountWithDataSet.LOCAL's getAccountType() must be null as well, thus we return
            // the NULL account.
            return null;
        } else {
            // AccountWithDataSet.LOCAL's getAccountType() must not be null as well, thus we return
            // the customized local account.
            return new Account(AccountWithDataSet.LOCAL.getAccountName(),
                    AccountWithDataSet.LOCAL.getAccountType());
        }
    }

    /**
     * Resolves the account and builds an {@link AccountWithDataSet} based on the data set specified
     * in the URI or values (if any).
     *
     * @param uri                 Current {@link Uri} being operated on.
     * @param values              {@link ContentValues} to read and possibly update.
     * @param applyDefaultAccount Whether to look up default account during account resolution.
     */
    public AccountWithDataSet resolveAccountWithDataSet(Uri uri, ContentValues values,
            boolean applyDefaultAccount) {
        final Account[] accounts = resolveAccount(uri, values);
        final Account account = applyDefaultAccount
                ? getAccountWithDefaultAccountApplied(uri, accounts)
                : getFirstAccountOrNull(accounts);

        AccountWithDataSet accountWithDataSet = null;
        if (account != null) {
            String dataSet = ContactsProvider2.getQueryParameter(uri, RawContacts.DATA_SET);
            if (dataSet == null) {
                dataSet = values.getAsString(RawContacts.DATA_SET);
            } else {
                values.put(RawContacts.DATA_SET, dataSet);
            }
            accountWithDataSet = AccountWithDataSet.get(account.name, account.type, dataSet);
        }

        return accountWithDataSet;
    }

    /**
     * Resolves the account to be used, taking into consideration the default account settings.
     *
     * @param accounts 1-size array which contains specified account, or empty array if account is
     *                 not specified.
     * @param uri      The URI used for resolving accounts.
     * @return The resolved account, or null if it's the default device (aka "NULL") account.
     * @throws IllegalArgumentException If there's an issue with the account resolution due to
     *                                  default account incompatible account types.
     */
    private Account getAccountWithDefaultAccountApplied(Uri uri, Account[] accounts)
            throws IllegalArgumentException {
        if (accounts.length == 0) {
            DefaultAccountAndState defaultAccountAndState =
                    mDefaultAccountManager.pullDefaultAccount();
            if (defaultAccountAndState.getState()
                    == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_NOT_SET
                    || defaultAccountAndState.getState()
                    == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_LOCAL) {
                return getLocalAccount();
            } else {
                return defaultAccountAndState.getAccount();
            }
        } else {
            checkAccountIsWritableInternal(accounts[0]);
            return accounts[0];
        }
    }

    /**
     * Checks if the specified account is writable.
     *
     * <p>This method verifies if contacts can be written to the given account based on the
     * current default account settings. It throws an {@link IllegalArgumentException} if
     * the account is not writable.</p>
     *
     * @param accountName The name of the account to check.
     * @param accountType The type of the account to check.
     * @throws IllegalArgumentException if either of the following conditions are met:
     *                                  <ul>
     *                                      <li>Only one of <code>accountName</code> or
     *                                      <code>accountType</code> is
     *                                          specified.</li>
     *                                      <li>The default account is set to cloud and the
     *                                      specified account is a local
     *                                          (device or SIM) account.</li>
     *                                  </ul>
     */
    public void checkAccountIsWritable(String accountName, String accountType) {
        if (TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType)) {
            throw new IllegalArgumentException(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE");
        }
        if (TextUtils.isEmpty(accountName)) {
            checkAccountIsWritableInternal(/*account=*/null);
        } else {
            checkAccountIsWritableInternal(new Account(accountName, accountType));
        }
    }

    private void checkAccountIsWritableInternal(Account account)
            throws IllegalArgumentException {
        DefaultAccountAndState defaultAccount = mDefaultAccountManager.pullDefaultAccount();

        if (defaultAccount.getState() == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD) {
            if (isDeviceOrSimAccount(account)) {
                throw new IllegalArgumentException("Cannot write contacts to local accounts "
                        + "when default account is set to cloud");
            }
        }
    }

    /**
     * Gets the first account from the array, or null if the array is empty.
     *
     * @param accounts The array of accounts.
     * @return The first account, or null if the array is empty.
     */
    private Account getFirstAccountOrNull(Account[] accounts) {
        return accounts.length > 0 ? accounts[0] : null;
    }


    private boolean isDeviceOrSimAccount(Account account) {
        AccountWithDataSet accountWithDataSet = account == null
                ? new AccountWithDataSet(null, null, null)
                : new AccountWithDataSet(account.name, account.type, null);

        List<SimAccount> simAccounts = mDbHelper.getAllSimAccounts();
        return accountWithDataSet.isLocalAccount() || accountWithDataSet.inSimAccounts(simAccounts);
    }

    /**
     * If account is non-null then store it in the values. If the account is
     * already specified in the values then it must be consistent with the
     * account, if it is non-null.
     *
     * @param uri    Current {@link Uri} being operated on.
     * @param values {@link ContentValues} to read and possibly update.
     * @return 1-size array which contains account specified by {@link Uri} and
     * {@link ContentValues}, or empty array if account is not specified.
     * @throws IllegalArgumentException when only one of
     *                                  {@link RawContacts#ACCOUNT_NAME} or
     *                                  {@link RawContacts#ACCOUNT_TYPE} is specified, leaving the
     *                                  other undefined.
     * @throws IllegalArgumentException when {@link RawContacts#ACCOUNT_NAME}
     *                                  and {@link RawContacts#ACCOUNT_TYPE} are inconsistent
     *                                  between
     *                                  the given {@link Uri} and {@link ContentValues}.
     */
    private Account[] resolveAccount(Uri uri, ContentValues values)
            throws IllegalArgumentException {
        String accountName = ContactsProvider2.getQueryParameter(uri, RawContacts.ACCOUNT_NAME);
        String accountType = ContactsProvider2.getQueryParameter(uri, RawContacts.ACCOUNT_TYPE);
        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);

        if (accountName == null && accountType == null
                && !values.containsKey(RawContacts.ACCOUNT_NAME)
                && !values.containsKey(RawContacts.ACCOUNT_TYPE)) {
            // Account is not specified.
            return new Account[0];
        }

        String valueAccountName = values.getAsString(RawContacts.ACCOUNT_NAME);
        String valueAccountType = values.getAsString(RawContacts.ACCOUNT_TYPE);

        final boolean partialValues = TextUtils.isEmpty(valueAccountName)
                ^ TextUtils.isEmpty(valueAccountType);

        if (partialUri || partialValues) {
            // Throw when either account is incomplete.
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri));
        }

        // Accounts are valid by only checking one parameter, since we've
        // already ruled out partial accounts.
        final boolean validUri = !TextUtils.isEmpty(accountName);
        final boolean validValues = !TextUtils.isEmpty(valueAccountName);

        if (validValues && validUri) {
            // Check that accounts match when both present
            final boolean accountMatch = TextUtils.equals(accountName, valueAccountName)
                    && TextUtils.equals(accountType, valueAccountType);
            if (!accountMatch) {
                throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                        "When both specified, ACCOUNT_NAME and ACCOUNT_TYPE must match", uri));
            }
        } else if (validUri) {
            // Fill values from the URI when not present.
            values.put(RawContacts.ACCOUNT_NAME, accountName);
            values.put(RawContacts.ACCOUNT_TYPE, accountType);
        } else if (validValues) {
            accountName = valueAccountName;
            accountType = valueAccountType;
        } else {
            return new Account[]{null};
        }

        return new Account[]{new Account(accountName, accountType)};
    }
}
