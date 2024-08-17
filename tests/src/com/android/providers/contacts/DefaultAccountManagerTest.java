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

import static org.mockito.Mockito.argThat;

import android.accounts.Account;
import android.accounts.AccountManager;

import androidx.test.filters.SmallTest;

import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SmallTest
public class DefaultAccountManagerTest extends BaseContactsProvider2Test {
    private static final String TAG = "DefaultAccountManagerTest";
    private static final Account SYSTEM_CLOUD_ACCOUNT_1 = new Account("user1@gmail.com",
            "com.google");
    private static final Account NON_SYSTEM_CLOUD_ACCOUNT_1 = new Account("user2@whatsapp.com",
            "com.whatsapp");

    private ContactsDatabaseHelper mDbHelper;
    private DefaultAccountManager mDefaultAccountManager;
    private SyncSettingsHelper mSyncSettingsHelper;
    private AccountManager mMockAccountManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mDbHelper = getContactsProvider().getDatabaseHelper();
        mSyncSettingsHelper = new SyncSettingsHelper();
        mMockAccountManager = Mockito.mock(AccountManager.class);
        mDefaultAccountManager = new DefaultAccountManager(getContactsProvider().getContext(),
                mDbHelper, mSyncSettingsHelper, mMockAccountManager); // Inject mockAccountManager

        DefaultAccountManager.setEligibleSystemAccountTypes(new String[]{"com.google"});
        setAccounts(new Account[0]);
    }

    private void setAccounts(Account[] accounts) {
        Mockito.when(mMockAccountManager.getAccounts()).thenReturn(accounts);

        // Construsts a map between the account type and account list, so that we could mock
        // mMockAccountManager.getAccountsByType below.
        Map<String, List<Account>> accountTypeMap = new HashMap<>();
        for (Account account : accounts) {
            if (accountTypeMap.containsKey(account.type)) {
                accountTypeMap.get(account.type).add(account);
            } else {
                List<Account> accountList = new ArrayList<>();
                accountList.add(account);
                accountTypeMap.put(account.type, accountList);
            }
        }

        // By default: getAccountsByType returns empty account list unless there is a match in
        // in accountTypeMap.
        Mockito.when(mMockAccountManager.getAccountsByType(
                argThat(str -> !accountTypeMap.containsKey(str)))).thenReturn(new Account[0]);

        for (Map.Entry<String, List<Account>> entry : accountTypeMap.entrySet()) {
            String accountType = entry.getKey();
            Mockito.when(mMockAccountManager.getAccountsByType(accountType)).thenReturn(
                    entry.getValue().toArray(new Account[0]));
        }
    }

    public void testPushDca_noCloudAccountsSignedIn() {
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Push the DCA which is same as the current DCA, expect failure.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(null));
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Push the DCA which is not signed in, expect failure.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());
    }

    public void testPushDeviceAccountAsDca_cloudSyncIsOff() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});

        // All cloud accounts are sync off, and thus there DCA is now DEVICE.
        mSyncSettingsHelper.turnOffSync(SYSTEM_CLOUD_ACCOUNT_1);
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA as DEVICE account, which shouldn't change DCA.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(null));
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA as the system cloud account, which should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));
    }

    public void testPushCustomizedDeviceAccountAsDca_cloudSyncIsOff() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOffSync(SYSTEM_CLOUD_ACCOUNT_1);

        // No cloud account remains sync on, and thus DCA reverts to the DEVICE.
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set DCA to be device account 1 more time, which would be a non-op.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(null));
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set DCA to be a system cloud account, which should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));
    }

    public void testPushDca_dcaWasUnknown_tryPushDeviceAndThenCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOnSync(SYSTEM_CLOUD_ACCOUNT_1);

        // 1 system cloud account with sync on. DCA was set to cloud before, and thus it's in
        // a UNKNOWN state.
        assertEquals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be local, which should succeed. In addition, it should turn
        // all system cloud account's sync off.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(null));
        assertEquals(DefaultAccount.DEVICE_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());
        assertTrue(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Try to set the DCA to be system cloud account, which should succeed. In addition,
        // it should turn the DCA account's sync on.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));
    }

    public void testPushDca_dcaWasCloud() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOnSync(SYSTEM_CLOUD_ACCOUNT_1);

        // DCA was a system cloud initially.
        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set DCA to a device (null) account, which is expected to fail, since not all
        // system cloud account's sync is off.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(null));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Try to set DCA to the same system cloud account, which should fail and thus make no
        // change.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));
    }

    public void testPushDca_dcaWasUnknown_tryPushAccountNotSignedIn() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        assertEquals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account not signed in, which should fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(
                new Account("unknown1@gmail.com", "com.google")));
        assertEquals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());
    }

    public void testPushDca_dcaWasUnknown_tryPushNonSystemCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, NON_SYSTEM_CLOUD_ACCOUNT_1});
        assertEquals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account which is not a system cloud account, which should
        // fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(NON_SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT,
                mDefaultAccountManager.pullDefaultAccount());
    }

    public void testPushDca_dcaWasCloud_tryPushAccountNotSignedIn() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account not signed in, which should fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(
                new Account("unknown1@gmail.com", "com.google")));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
    }

    public void testPushDca_dcaWasCloud_tryPushNonSystemCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, NON_SYSTEM_CLOUD_ACCOUNT_1});
        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account which is not a system cloud account, which should
        // fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(NON_SYSTEM_CLOUD_ACCOUNT_1));
        assertEquals(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
    }
}
