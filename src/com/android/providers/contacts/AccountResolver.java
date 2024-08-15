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
import android.text.TextUtils;

public class AccountResolver {
    /**
     * Resolves the account and builds an {@link AccountWithDataSet} based on the data set specified
     * in the URI or values (if any).
     * @param uri Current {@link Uri} being operated on.
     * @param values {@link ContentValues} to read and possibly update.
     */
    public static AccountWithDataSet resolveAccountWithDataSet(Uri uri, ContentValues values,
            ContactsDatabaseHelper dbHelper) {
        final Account account = resolveAccount(uri, values, dbHelper);
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
     * If account is non-null then store it in the values. If the account is
     * already specified in the values then it must be consistent with the
     * account, if it is non-null.
     *
     * @param uri Current {@link Uri} being operated on.
     * @param values {@link ContentValues} to read and possibly update.
     * @throws IllegalArgumentException when only one of
     *             {@link RawContacts#ACCOUNT_NAME} or
     *             {@link RawContacts#ACCOUNT_TYPE} is specified, leaving the
     *             other undefined.
     * @throws IllegalArgumentException when {@link RawContacts#ACCOUNT_NAME}
     *             and {@link RawContacts#ACCOUNT_TYPE} are inconsistent between
     *             the given {@link Uri} and {@link ContentValues}.
     */
    private static Account resolveAccount(Uri uri, ContentValues values,
            ContactsDatabaseHelper dbHelper) throws IllegalArgumentException {
        String accountName = ContactsProvider2.getQueryParameter(uri, RawContacts.ACCOUNT_NAME);
        String accountType = ContactsProvider2.getQueryParameter(uri, RawContacts.ACCOUNT_TYPE);
        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);

        String valueAccountName = values.getAsString(RawContacts.ACCOUNT_NAME);
        String valueAccountType = values.getAsString(RawContacts.ACCOUNT_TYPE);
        final boolean partialValues = TextUtils.isEmpty(valueAccountName)
                ^ TextUtils.isEmpty(valueAccountType);

        if (partialUri || partialValues) {
            // Throw when either account is incomplete.
            throw new IllegalArgumentException(dbHelper.exceptionMessage(
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
                throw new IllegalArgumentException(dbHelper.exceptionMessage(
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
            return null;
        }

        return new Account(accountName, accountType);
    }
}
