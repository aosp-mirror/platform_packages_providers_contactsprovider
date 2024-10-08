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
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;

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

        setAccounts(new Account[0]);
        DefaultAccountManager.setEligibleSystemCloudAccountTypesForTesting(
                new String[]{SYSTEM_CLOUD_ACCOUNT_1.type});
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
        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());

        // Push the DCA which is device account, which should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofLocal()));
        assertEquals(DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());

        // Push the DCA which is not signed in, expect failure.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());
    }

    public void testPushDeviceAccountAsDca_cloudSyncIsOff() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});

        mSyncSettingsHelper.turnOffSync(SYSTEM_CLOUD_ACCOUNT_1);

        // SYSTEM_CLOUD_ACCOUNT_1 is signed in, but sync is turned off, thus no account is eligible
        // to be set as cloud default account.
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());

        // The initial DCA should be unknown, regardless of the cloud account existence and their
        // sync status.
        mSyncSettingsHelper.turnOffSync(SYSTEM_CLOUD_ACCOUNT_1);
        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA as DEVICE account, which should succeed
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofLocal()));
        assertEquals(DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());

        // Sync-off system cloud account will be treated as non-eligible cloud account.
        // Despite that, setting DCA to be a non-eligible cloud account, should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Sync remains off.
        assertTrue(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());
    }

    public void testPushCustomizedDeviceAccountAsDca_cloudSyncIsOff() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOffSync(SYSTEM_CLOUD_ACCOUNT_1);

        // SYSTEM_CLOUD_ACCOUNT_1 is signed in, but sync is turned off, thus no account is eligible
        // to be set as cloud default account.
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());

        // No cloud account remains sync on, and thus DCA reverts to the DEVICE.
        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set DCA to be device account, which should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofLocal()));
        assertEquals(DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());

        // Sync-off system cloud account will be treated as non-eligible cloud account.
        // Despite that, setting DCA to be a non-eligible cloud account, should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Sync remains off.
        assertTrue(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(), mDefaultAccountManager.getEligibleCloudAccounts());
    }

    public void testPushDca_dcaWasUnknown_tryPushDeviceAndThenCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOnSync(SYSTEM_CLOUD_ACCOUNT_1);

        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        // 1 system cloud account with sync on. DCA was set to cloud before, and thus it's in
        // a UNKNOWN state.
        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be local, which should succeed. In addition, it should turn
        // all system cloud account's sync off.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofLocal()));
        assertEquals(DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());
        // Sync setting should remain to be on.
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Try to set the DCA to be system cloud account, which should succeed.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        // Sync setting should remain to be on.
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

    }

    public void testPushDca_dcaWasCloud() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mSyncSettingsHelper.turnOnSync(SYSTEM_CLOUD_ACCOUNT_1);

        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        // DCA was a system cloud initially.
        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set DCA to a device (null) account, which should succeed, and it shouldn't
        // change the cloud account's sync status.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofLocal()));
        assertEquals(
                DefaultAccountAndState.ofLocal(),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Try to set DCA to the same system cloud account again, which should succeed
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());
        assertFalse(mSyncSettingsHelper.isSyncOff(SYSTEM_CLOUD_ACCOUNT_1));

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

    }

    public void testPushDca_dcaWasUnknown_tryPushAccountNotSignedIn() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});

        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account not signed in, which should fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(new Account("unknown1@gmail.com", "com.google"))));
        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

    }

    public void testPushDca_dcaWasUnknown_tryPushNonSystemCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, NON_SYSTEM_CLOUD_ACCOUNT_1});

        // Only SYSTEM_CLOUD_ACCOUNT_1 is eligible to be set as cloud default account.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        assertEquals(DefaultAccountAndState.ofNotSet(),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account which is not a system cloud account, which should
        // fail.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

    }

    public void testPushDca_dcaWasCloud_tryPushAccountNotSignedIn() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});

        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account not signed in, which should fail.
        assertFalse(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(new Account("unknown1@gmail.com", "com.google"))));
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

    }

    public void testPushDca_dcaWasCloud_tryPushNonSystemCloudAccount() {
        setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, NON_SYSTEM_CLOUD_ACCOUNT_1});

        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());

        mDbHelper.setDefaultAccount(SYSTEM_CLOUD_ACCOUNT_1.name, SYSTEM_CLOUD_ACCOUNT_1.type);
        assertEquals(
                DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Try to set the DCA to be an account which is not a system cloud account, which should
        // fail.
        assertTrue(mDefaultAccountManager.tryPushDefaultAccount(
                DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1)));
        assertEquals(
                DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.pullDefaultAccount());

        // Cloud account eligible for default accounts doesn't change.
        assertEquals(List.of(SYSTEM_CLOUD_ACCOUNT_1),
                mDefaultAccountManager.getEligibleCloudAccounts());
    }
}
