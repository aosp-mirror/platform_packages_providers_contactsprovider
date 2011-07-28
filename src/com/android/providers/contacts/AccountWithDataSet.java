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

import com.android.internal.util.Objects;

/**
 * Account information that includes the data set, if any.
 */
public class AccountWithDataSet {
    private final String mAccountName;
    private final String mAccountType;
    private final String mDataSet;

    public AccountWithDataSet(String accountName, String accountType, String dataSet) {
        mAccountName = accountName;
        mAccountType = accountType;
        mDataSet = dataSet;
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
}
