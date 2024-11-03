/*
 * Copyright (C) 2009 The Android Open Source Project
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

import static android.provider.ContactsContract.SimAccount.SDN_EF_TYPE;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import android.accounts.Account;
import android.compat.testing.PlatformCompatChangeRule;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;
import android.os.Bundle;
import android.platform.test.annotations.RequiresFlagsDisabled;
import android.platform.test.annotations.RequiresFlagsEnabled;
import android.platform.test.flag.junit.CheckFlagsRule;
import android.platform.test.flag.junit.DeviceFlagsValueProvider;
import android.provider.ContactsContract;
import android.provider.ContactsContract.RawContacts.DefaultAccount;
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;
import android.provider.ContactsContract.Settings;
import android.provider.Flags;

import androidx.annotation.NonNull;
import androidx.test.filters.MediumTest;

import libcore.junit.util.compat.CoreCompatChangeRule.DisableCompatChanges;
import libcore.junit.util.compat.CoreCompatChangeRule.EnableCompatChanges;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Unit tests for {@link ContactsProvider2} Default Account Handling.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class
 * com.android.providers.contacts.ContactsProvider2DefaultAccountTest -w \
 * com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
@RunWith(JUnit4.class)
public class ContactsProvider2DefaultAccountTest extends BaseContactsProvider2Test {
    static final Account SYSTEM_CLOUD_ACCOUNT_1 = new Account("sourceName1", "com.google");
    static final Account SYSTEM_CLOUD_ACCOUNT_2 = new Account("sourceName2", "com.google");
    static final Account SYSTEM_CLOUD_ACCOUNT_NOT_SIGNED_IN = new Account("sourceName3",
            "com.google");
    static final Account NON_SYSTEM_CLOUD_ACCOUNT_1 = new Account("sourceName1", "com.whatsapp");
    static final Account SIM_ACCOUNT_1 = new Account("simName1", "SIM");

    static final String RES_PACKAGE = "testpackage";
    @Rule
    public final CheckFlagsRule mCheckFlagsRule = DeviceFlagsValueProvider.createCheckFlagsRule();

    @Rule
    public TestRule compatChangeRule = new PlatformCompatChangeRule();

    ContactsProvider2 mCp;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mCp = (ContactsProvider2) getContactsProvider();
        createSimAccount(SIM_ACCOUNT_1);
        DefaultAccountManager.setEligibleSystemCloudAccountTypesForTesting(
                new String[]{"com.google"});
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertResponseContainsDefaultAccount(DefaultAccountAndState expectedDefaultAccount,
            Bundle response) {
        assertEquals(expectedDefaultAccount.getState(),
                response.getInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE, -1));
        if (expectedDefaultAccount.getState()
                == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD
                || expectedDefaultAccount.getState()
                == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_SIM) {
            assertEquals(expectedDefaultAccount.getAccount().name,
                    response.getString(Settings.ACCOUNT_NAME));
            assertEquals(expectedDefaultAccount.getAccount().type,
                    response.getString(Settings.ACCOUNT_TYPE));
        } else {
            assertNull(response.getString(Settings.ACCOUNT_NAME));
            assertNull(response.getString(Settings.ACCOUNT_TYPE));
        }
    }

    private Bundle bundleToSetDefaultAccountForNewContacts(
            DefaultAccountAndState expectedDefaultAccount) {
        Bundle bundle = new Bundle();
        bundle.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE, expectedDefaultAccount.getState());
        if (expectedDefaultAccount.getState()
                == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD
                || expectedDefaultAccount.getState()
                == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_SIM) {
            bundle.putString(Settings.ACCOUNT_NAME, expectedDefaultAccount.getAccount().name);
            bundle.putString(Settings.ACCOUNT_TYPE, expectedDefaultAccount.getAccount().type);
        }
        return bundle;
    }

    private void createSimAccount(Account account) {
        AccountWithDataSet accountWithDataSet =
                new AccountWithDataSet(account.name, account.type, null);
        final SQLiteDatabase db = mCp.getDatabaseHelper().getWritableDatabase();
        db.beginTransaction();
        try {
            mCp.getDatabaseHelper().createSimAccountIdInTransaction(accountWithDataSet, 1,
                    SDN_EF_TYPE);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    @Test
    @RequiresFlagsDisabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testSetAndGetDefaultAccountForNewContacts_flagOff() throws Exception {
        // Default account is Unknown initially.
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);

        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null));

        // Attempt to set default account to a cloud account.
        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1))));
        // Default account is not changed.
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);

        // Attempt to set default account to local.
        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(DefaultAccountAndState.ofLocal())));
        // Default account is not changed.
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);

        // Attempt to set default account to "not set".
        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(DefaultAccountAndState.ofNotSet())));
        // Default account is not changed.
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testSetDefaultAccountForNewContacts_flagOn_permissionDenied() throws Exception {
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        assertThrows(SecurityException.class, () ->
                mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, null));
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testSetDefaultAccountForNewContacts_flagOn_invalidRequests() throws Exception {
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        mActor.addPermissions("android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");

        // Account name is null and account type is not null.
        Bundle bundleWithNoAccountType = new Bundle();
        bundleWithNoAccountType.putString(Settings.ACCOUNT_NAME, SYSTEM_CLOUD_ACCOUNT_1.name);
        bundleWithNoAccountType.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleWithNoAccountType));
        bundleWithNoAccountType.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_LOCAL);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleWithNoAccountType));
        bundleWithNoAccountType.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_NOT_SET);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleWithNoAccountType));

        // Account type is null and account name is not null.
        Bundle bundleAccountWithNoAccountName = new Bundle();
        bundleAccountWithNoAccountName.putString(Settings.ACCOUNT_TYPE,
                SYSTEM_CLOUD_ACCOUNT_1.type);
        bundleAccountWithNoAccountName.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleAccountWithNoAccountName));
        bundleAccountWithNoAccountName.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_LOCAL);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleAccountWithNoAccountName));

        bundleAccountWithNoAccountName.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_NOT_SET);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleAccountWithNoAccountName));

        // Cloud account with null account name and type
        Bundle bundleCloudAccountWithNoAccount = new Bundle();
        bundleCloudAccountWithNoAccount.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleCloudAccountWithNoAccount));

        // Non-cloud account with non-null account name and type
        Bundle bundleLocalDefaultAccountStateWithAccount = new Bundle();
        bundleLocalDefaultAccountStateWithAccount.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_LOCAL);
        bundleLocalDefaultAccountStateWithAccount.putString(Settings.ACCOUNT_TYPE,
                SYSTEM_CLOUD_ACCOUNT_1.name);
        bundleLocalDefaultAccountStateWithAccount.putString(Settings.ACCOUNT_TYPE,
                SYSTEM_CLOUD_ACCOUNT_1.type);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleLocalDefaultAccountStateWithAccount));

        Bundle bundleNotSetDefaultAccountStateWithAccount = new Bundle();
        bundleNotSetDefaultAccountStateWithAccount.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_NOT_SET);
        bundleNotSetDefaultAccountStateWithAccount.putString(Settings.ACCOUNT_TYPE,
                SYSTEM_CLOUD_ACCOUNT_1.name);
        bundleNotSetDefaultAccountStateWithAccount.putString(Settings.ACCOUNT_TYPE,
                SYSTEM_CLOUD_ACCOUNT_1.type);
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, bundleNotSetDefaultAccountStateWithAccount));
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testSetAndGetDefaultAccountForNewContacts_flagOn_normal() throws Exception {
        // Default account is Unknown initially.
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);

        Bundle response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofNotSet(), response);

        mActor.addPermissions("android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2,
                NON_SYSTEM_CLOUD_ACCOUNT_1});

        // Set the default account (for new contacts) to a cloud account and then query.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1),
                response);

        assertArrayEquals(new Account[]{SYSTEM_CLOUD_ACCOUNT_1},
                mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Set the default account (for new contacts) to a different cloud account and then query.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_2)));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_2),
                response);
        assertArrayEquals(new Account[]{SYSTEM_CLOUD_ACCOUNT_2},
                mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Attempt to set the default account (for new contacts) to a system cloud account which
        // is not signed in.
        assertThrows(IllegalArgumentException.class,
                () -> mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                        bundleToSetDefaultAccountForNewContacts(DefaultAccountAndState.ofCloud(
                                SYSTEM_CLOUD_ACCOUNT_NOT_SIGNED_IN))));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        // Default account for new contacts is not changed.
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_2),
                response);
        assertArrayEquals(new Account[]{SYSTEM_CLOUD_ACCOUNT_2},
                mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Attempt to set the default account (for new contacts) to a non-system cloud account.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1)));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        // Default account for new contacts is changed to non-system cloud account.
        assertResponseContainsDefaultAccount(
                DefaultAccountAndState.ofCloud(NON_SYSTEM_CLOUD_ACCOUNT_1),
                response);
        assertArrayEquals(new Account[]{NON_SYSTEM_CLOUD_ACCOUNT_1},
                mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Set the default account (for new contacts) to the local account and then query.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(DefaultAccountAndState.ofLocal()));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofLocal(), response);
        assertArrayEquals(new Account[]{null}, mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Set the default account (for new contacts) to a SIM account.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofSim(SIM_ACCOUNT_1)));
        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofSim(SIM_ACCOUNT_1), response);
        assertArrayEquals(new Account[]{SIM_ACCOUNT_1},
                mCp.getDatabaseHelper().getDefaultAccountIfAny());

        // Set the default account (for new contacts) to a "not set" state
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(DefaultAccountAndState.ofNotSet()));

        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null, null);
        assertResponseContainsDefaultAccount(DefaultAccountAndState.ofNotSet(), response);
        assertEquals(0, mCp.getDatabaseHelper().getDefaultAccountIfAny().length);
    }


    @Test
    @RequiresFlagsDisabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testGetEligibleCloudAccounts_flagOff() throws Exception {
        mActor.setAccounts(new Account[0]);
        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, null));

        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        assertNull(mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, null));
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testGetEligibleCloudAccounts_flagOn_permissionDenied() throws Exception {
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        assertThrows(SecurityException.class, () ->
                mResolver.call(ContactsContract.AUTHORITY_URI,
                        DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD,
                        null, null));
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testGetEligibleCloudAccounts_flagOn_normal() throws Exception {
        mActor.addPermissions("android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");
        Bundle response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_ELIGIBLE_DEFAULT_ACCOUNTS_METHOD, null, null);

        // No account is present on the device,
        List<Account> accounts = response.getParcelableArrayList(
                DefaultAccount.KEY_ELIGIBLE_DEFAULT_ACCOUNTS, Account.class);
        assertEquals(new ArrayList<>(), accounts);

        // 1 system cloud account is present on the device.
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});
        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_ELIGIBLE_DEFAULT_ACCOUNTS_METHOD, null, null);
        accounts = response.getParcelableArrayList(
                DefaultAccount.KEY_ELIGIBLE_DEFAULT_ACCOUNTS, Account.class);
        assertEquals(Arrays.asList(new Account[]{SYSTEM_CLOUD_ACCOUNT_1}), accounts);

        // 2 system cloud accounts are present on the device.
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2});
        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_ELIGIBLE_DEFAULT_ACCOUNTS_METHOD, null, null);
        accounts = response.getParcelableArrayList(
                DefaultAccount.KEY_ELIGIBLE_DEFAULT_ACCOUNTS, Account.class);
        assertEquals(Arrays.asList(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2}),
                accounts);

        // 2 system cloud and 1 non-system cloud account are present on the device.
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2,
                NON_SYSTEM_CLOUD_ACCOUNT_1});
        response = mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.QUERY_ELIGIBLE_DEFAULT_ACCOUNTS_METHOD, null, null);
        accounts = response.getParcelableArrayList(
                DefaultAccount.KEY_ELIGIBLE_DEFAULT_ACCOUNTS, Account.class);
        assertEquals(Arrays.asList(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2}),
                accounts);
    }


    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    @DisableCompatChanges({ContactsProvider2.RESTRICT_CONTACTS_CREATION_IN_ACCOUNTS})
    public void testRawContactInsert_whenDefaultAccountSetToCloud_contactCreationNotRestricted() {
        mActor.addPermissions("android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1});

        // Set the default account (for new contacts) to a cloud account.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));

        // Okay to insert raw contact in cloud account.
        long rawContactId1 = insertRawContact(SYSTEM_CLOUD_ACCOUNT_1);

        // Okay to insert raw contact in NULL account.
        long rawContactId2 = insertRawContact((Account) null);

        // Okay to update raw contact to a different cloud account.
        assertEquals(1, updateRawContactAccount(rawContactId1, SYSTEM_CLOUD_ACCOUNT_2));
        assertEquals(1, updateRawContactAccount(rawContactId2, SYSTEM_CLOUD_ACCOUNT_2));

        // Okay to update raw contact to NULL account.
        assertEquals(1, updateRawContactAccount(rawContactId2, null));

        // Okay to insert group in cloud account.
        long groupId1 = insertGroup(SYSTEM_CLOUD_ACCOUNT_1);

        // Okay to insert group in NULL account.
        long groupId2 = insertGroup((Account) null);

        // Okay to update raw contact to a different cloud account.
        assertEquals(1, updateGroupAccount(groupId1, SYSTEM_CLOUD_ACCOUNT_2));
        assertEquals(1, updateGroupAccount(groupId2, SYSTEM_CLOUD_ACCOUNT_2));

        // Okay to update raw contact to NULL account.
        assertEquals(1, updateGroupAccount(groupId1, null));
    }

    @Test
    @RequiresFlagsEnabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    @EnableCompatChanges({ContactsProvider2.RESTRICT_CONTACTS_CREATION_IN_ACCOUNTS})
    public void testRawContactInsert_whenDefaultAccountSetToCloud_contactCreationRestricted() {
        mActor.addPermissions("android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");
        mActor.setAccounts(new Account[]{SYSTEM_CLOUD_ACCOUNT_1, SYSTEM_CLOUD_ACCOUNT_2});

        // Set the default account (for new contacts) to a cloud account.
        mResolver.call(ContactsContract.AUTHORITY_URI,
                DefaultAccount.SET_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD, null,
                bundleToSetDefaultAccountForNewContacts(
                        DefaultAccountAndState.ofCloud(SYSTEM_CLOUD_ACCOUNT_1)));

        // Okay to insert raw contact in cloud account.
        long rawContactId1 = insertRawContact(SYSTEM_CLOUD_ACCOUNT_1);

        // Exception expected when inserting raw contact in NULL account.
        assertThrows(IllegalArgumentException.class, () ->
                insertRawContact((Account) null));

        // Okay to update the raw contact to a different cloud account
        assertEquals(1, updateRawContactAccount(rawContactId1, SYSTEM_CLOUD_ACCOUNT_2));

        // Exception expected when updating raw contact to NULL account.
        assertThrows(IllegalArgumentException.class,
                () -> updateRawContactAccount(rawContactId1, null));

        // Okay to insert group in cloud account.
        long groupId1 = insertGroup(SYSTEM_CLOUD_ACCOUNT_1);

        // Exception expected when inserting group in NULL account.
        assertThrows(IllegalArgumentException.class, () ->
                insertGroup((Account) null));

        // Okay to update the group to a different cloud account
        assertEquals(1, updateGroupAccount(groupId1, SYSTEM_CLOUD_ACCOUNT_2));

        // Exception expected when updating group to NULL account.
        assertThrows(IllegalArgumentException.class, () -> updateGroupAccount(groupId1, null));

    }

    private long insertRawContact(Account account) {
        ContentValues values = getRawContactContactValuesFromAccount(account);
        return ContentUris.parseId(
                Objects.requireNonNull(
                        mResolver.insert(ContactsContract.RawContacts.CONTENT_URI, values)));
    }

    private long insertGroup(Account account) {
        ContentValues values = getGroupContentValuesFromAccount(account);
        return ContentUris.parseId(
                Objects.requireNonNull(
                        mResolver.insert(ContactsContract.Groups.CONTENT_URI, values)));
    }

    private long updateRawContactAccount(long rawContactId, Account destinationAccount) {
        ContentValues values = getRawContactContactValuesFromAccount(destinationAccount);
        return mResolver.update(
                ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                values, null, null);
    }

    private long updateGroupAccount(long groupId, Account destinationAccount) {
        ContentValues values = getGroupContentValuesFromAccount(destinationAccount);
        return mResolver.update(
                ContentUris.withAppendedId(ContactsContract.Groups.CONTENT_URI, groupId), values,
                null, null);
    }

    @NonNull
    private static ContentValues getRawContactContactValuesFromAccount(Account account) {
        ContentValues values = new ContentValues();
        if (account == null) {
            values.put(ContactsContract.RawContacts.ACCOUNT_NAME, (String) null);
            values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, (String) null);
        } else {
            values.put(ContactsContract.RawContacts.ACCOUNT_NAME, account.name);
            values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, account.type);
        }
        return values;
    }


    @NonNull
    private static ContentValues getGroupContentValuesFromAccount(Account account) {
        ContentValues values = new ContentValues();
        if (account == null) {
            values.put(ContactsContract.RawContacts.ACCOUNT_NAME, (String) null);
            values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, (String) null);
        } else {
            values.put(ContactsContract.RawContacts.ACCOUNT_NAME, account.name);
            values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, account.type);
        }
        return values;
    }
}
