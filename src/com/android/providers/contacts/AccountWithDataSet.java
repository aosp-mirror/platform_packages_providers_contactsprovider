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

import android.accounts.Account;
import android.content.res.Resources;
import android.database.DatabaseUtils;
import android.provider.ContactsContract.SimAccount;
import android.text.TextUtils;

import com.android.internal.R;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

import java.util.List;

/**
 * Account information that includes the data set, if any.
 */
public class AccountWithDataSet {
    public static final AccountWithDataSet LOCAL;

    static {
        Resources resources = Resources.getSystem();
        String accountName = Strings.nullToEmpty(
                resources.getString(R.string.config_rawContactsLocalAccountName));
        String accountType = Strings.nullToEmpty(
                resources.getString(R.string.config_rawContactsLocalAccountType));
        LOCAL = new AccountWithDataSet(accountName, accountType, null);
    }

    private final String mAccountName;
    private final String mAccountType;
    private final String mDataSet;

    public AccountWithDataSet(String accountName, String accountType, String dataSet) {
        mAccountName = emptyToNull(accountName);
        mAccountType = emptyToNull(accountType);
        mDataSet = emptyToNull(dataSet);
    }

    private static final String emptyToNull(String text) {
        return TextUtils.isEmpty(text) ? null : text;
    }

    public static AccountWithDataSet get(String accountName, String accountType, String dataSet) {
        return new AccountWithDataSet(accountName, accountType, dataSet);
    }

    public static AccountWithDataSet get(Account account, String dataSet) {
        return new AccountWithDataSet(account.name, account.type, null);
    }

    public String getAccountName() {
        return mAccountName;
    }

    public String getAccountType() {
        return mAccountType;
    }

    public String getDataSet() {
        return mDataSet;
    }

    public boolean isLocalAccount() {
        return LOCAL.equals(this) || (
                mAccountName == null && mAccountType == null && mDataSet == null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AccountWithDataSet) {
            AccountWithDataSet other = (AccountWithDataSet) obj;
            return Objects.equal(mAccountName, other.getAccountName())
                    && Objects.equal(mAccountType, other.getAccountType())
                    && Objects.equal(mDataSet, other.getDataSet());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = mAccountName != null ? mAccountName.hashCode() : 0;
        result = 31 * result + (mAccountType != null ? mAccountType.hashCode() : 0);
        result = 31 * result + (mDataSet != null ? mDataSet.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AccountWithDataSet {name=" + mAccountName + ", type=" + mAccountType + ", dataSet="
                + mDataSet + "}";
    }

    /**
     * @return {@code true} if the owning {@link Account} is in the passed array.
     */
    public boolean inSystemAccounts(Account[] systemAccounts) {
        // Note we don't want to create a new Account object from this instance, as it may contain
        // null account name/type, which Account wouldn't accept.  So we need to compare field by
        // field.
        for (Account systemAccount : systemAccounts) {
            if (Objects.equal(systemAccount.name, getAccountName())
                    && Objects.equal(systemAccount.type, getAccountType())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return {@code true} if there is a {@link SimAccount} with a matching account name and type
     * in the passed list.
     * The list should be obtained from {@link ContactsDatabaseHelper#getAllSimAccounts()} so it
     * will already have valid SIM accounts so only name and type need to be compared.
     */
    public boolean inSimAccounts(List<SimAccount> simAccountList) {
        // Note we don't want to create a new SimAccount object from this instance, as this method
        // does not need to know about the SIM slot or the EF type. It only needs to know whether
        // the name and type match since the passed list will only contain valid SIM accounts.
        for (SimAccount simAccount : simAccountList) {
            if (Objects.equal(simAccount.getAccountName(), getAccountName())
                    && Objects.equal(simAccount.getAccountType(), getAccountType())) {
                return true;
            }
        }
        return false;
    }
}
