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

import static android.provider.ContactsContract.SimAccount.SDN_EF_TYPE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import android.accounts.Account;
import android.content.ContentValues;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.LocalSimContactsWriteException;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;

import androidx.test.filters.SmallTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

@SmallTest
@RunWith(JUnit4.class)
public class AccountResolverTest {
    @Mock
    private ContactsDatabaseHelper mDbHelper;
    @Mock
    private DefaultAccountManager mDefaultAccountManager;

    private AccountResolver mAccountResolver;

    private static final Account SIM_ACCOUNT_1 = new Account("simName1", "SIM");

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mAccountResolver = new AccountResolver(mDbHelper, mDefaultAccountManager);

        when(mDbHelper.getAllSimAccounts()).thenReturn(List.of(new ContactsContract.SimAccount(
                SIM_ACCOUNT_1.name, SIM_ACCOUNT_1.type, 1, SDN_EF_TYPE
        )));

    }

    private static final boolean FALSE_UNUSED = false;
    private static final boolean TRUE_UNUSED = true;

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_accountAndDataSetInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .appendQueryParameter(RawContacts.DATA_SET, "test_data_set")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsNotSet_accountAndDataSetInUri() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .appendQueryParameter(RawContacts.DATA_SET, "test_data_set")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_accountInUriDataSetInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_applyDefaultAccount_accountInUriDataSetInValues() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(DefaultAccountAndState.ofCloud(
                new Account("randomaccount1@gmail.com", "com.google")));

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_noAccount() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);

        // When default account is not used, uri/values without account is always resolved as
        // the local account, which is null AccountWithDataSet in this case.
        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsNotSet_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        // When default account is used and the default account is not set, uri/values without
        // account is always resolved as the local account, which is null AccountWithDataSet in this
        // case.
        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsDevice_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        // When default account is used and the default account is set to 'local', uri/values
        // without account is always resolved as the local account, which is null
        // AccountWithDataSet in this case.
        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(DefaultAccountAndState.ofCloud(
                new Account("test_account", "com.google")));

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        // When default account is used and the default account is set to 'cloud', uri/values
        // without account is always resolved as the cloud account, which is null
        // AccountWithDataSet in this case.
        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertNull(result.getDataSet());
    }

    @Test
    public void testResolveAccountWithDataSet_accountInValuesOnly() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts"); // No account in URI
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google");
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertEquals("test_account", result1.getAccountName());
        assertEquals("com.google", result1.getAccountType());
        assertEquals("test_data_set", result1.getDataSet());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        assertEquals("test_account", result2.getAccountName());
        assertEquals("com.google", result2.getAccountType());
        assertEquals("test_data_set", result2.getDataSet());
    }

    @Test
    public void testResolveAccountWithDataSet_invalidAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "invalid_account")
                .build(); // Missing ACCOUNT_TYPE
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google");
        values.put(RawContacts.DATA_SET, "test_data_set");

        when(mDbHelper.exceptionMessage(
                "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri))
                .thenReturn("Test Exception Message");

        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values,
                    /*applyDefaultAccount=*/false, /*shouldValidateAccountForContactAddition=*/
                    TRUE_UNUSED);
        });

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values,
                    /*applyDefaultAccount=*/true, /*shouldValidateAccountForContactAddition=*/true);
        });
    }

    @Test
    public void testResolveAccountWithDataSet_invalidAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "invalid_account"); // Invalid account
        values.put(RawContacts.DATA_SET, "test_data_set");

        when(mDbHelper.exceptionMessage(
                "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri))
                .thenReturn("Test Exception Message");

        // Expecting an exception due to the invalid account in the values
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);
        });

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
    }

    @Test
    public void testResolveAccountWithDataSet_matchingAccounts() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google");
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);

        assertEquals("test_account", result1.getAccountName());
        assertEquals("com.google", result1.getAccountType());
        assertEquals("test_data_set", result1.getDataSet());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());

        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);

        assertEquals("test_account", result2.getAccountName());
        assertEquals("com.google", result2.getAccountType());
        assertEquals("test_data_set", result2.getDataSet());
    }

    @Test
    public void testResolveAccountWithDataSet_invalidAccountsBoth() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "invalid_account_uri")
                .build(); // Missing ACCOUNT_TYPE
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "invalid_account_values");
        values.put(RawContacts.DATA_SET, "test_data_set");

        when(mDbHelper.exceptionMessage(
                "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri))
                .thenReturn("Test Exception Message");

        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);
        });

        // Expecting an exception due to the invalid account in the URI, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(new Account(
                        "test_account", "com.google"
                )));
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
    }

    @Test
    public void testResolveAccountWithDataSet_partialAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .build();
        ContentValues values = new ContentValues();

        when(mDbHelper.exceptionMessage(
                "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the partial account in uri, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(new Account(
                        "test_account", "com.google"
                )));
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
    }

    @Test
    public void testResolveAccountWithDataSet_partialAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");

        when(mDbHelper.exceptionMessage(
                "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the partial account in uri, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(new Account(
                        "test_account", "com.google"
                )));
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }

    @Test
    public void testResolveAccountWithDataSet_mismatchedAccounts() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account_uri")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google_uri")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account_values");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google_values");

        when(mDbHelper.exceptionMessage(
                "When both specified, ACCOUNT_NAME and ACCOUNT_TYPE must match", uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the uri and content value's account info mismatching,
        // regardless of what is the default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(new Account(
                        "test_account", "com.google"
                )));
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                    true, /*shouldValidateAccountForContactAddition=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_emptyAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsDeviceOrNotSet_emptyAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result2); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_emptyAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        IllegalArgumentException exception = assertThrows(LocalSimContactsWriteException.class,
                () -> {
                    mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                            true, /*shouldValidateAccountForContactAddition=*/true);
                });
        assertEquals(
                "Cannot add contacts to local or SIM accounts when default account is set to cloud",
                exception.getMessage());
    }

    @Test
    public void testResolveAccount_defaultAccountIsCloud_emptyAccountInUri_skipAccountValidation() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        AccountWithDataSet result =
                mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                        true, /*shouldValidateAccountForContactAddition=*/false);
        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_simAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, SIM_ACCOUNT_1.name)
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, SIM_ACCOUNT_1.type)
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        IllegalArgumentException exception = assertThrows(
                LocalSimContactsWriteException.class, () -> {
                    mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                            true, /*shouldValidateAccountForContactAddition=*/true);
                });
        assertEquals(
                "Cannot add contacts to local or SIM accounts when default account is set to cloud",
                exception.getMessage());
    }

    @Test
    public void testResolveAccount_defaultAccountIsCloud_simAccountInUri_skipAccountValidation() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, SIM_ACCOUNT_1.name)
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, SIM_ACCOUNT_1.type)
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        AccountWithDataSet result =
                mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                        true, /*shouldValidateAccountForContactAddition=*/false);
        assertEquals(SIM_ACCOUNT_1.name, result.getAccountName());
        assertEquals(SIM_ACCOUNT_1.type, result.getAccountType());
        assertNull(result.getDataSet());
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertNull(result); // Expect null result as account is effectively absent
    }


    @Test
    public void testResolveAccountWithDataSet_defaultAccountDeviceOrNotSet_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result2); // Expect null result as account is effectively absent
    }


    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        IllegalArgumentException exception = assertThrows(LocalSimContactsWriteException.class,
                () -> {
                    mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                            true, /*shouldValidateAccountForContactAddition=*/true);
                });
        assertEquals(
                "Cannot add contacts to local or SIM accounts when default account is set to cloud",
                exception.getMessage());
    }

    @Test
    public void testResolveAccount_defaultAccountIsCloud_emptyAccountInValues_skipAccountCheck() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(uri,
                values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/false);
        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_emptyAccount() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/TRUE_UNUSED);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultDeviceOrNotSet_emptyAccount() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/true);
        assertNull(result2); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_emptyAccount() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertNull(result); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        IllegalArgumentException exception = assertThrows(LocalSimContactsWriteException.class,
                () -> {
                    mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                            true, /*shouldValidateAccountForContactAddition=*/true);
                });
        assertEquals(
                "Cannot add contacts to local or SIM accounts when default account is set to cloud",
                exception.getMessage());
    }


    @Test
    public void testResolveAccount_defaultAccountIsCloud_emptyAccount_skipAccountCheck() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/
                false, /*shouldValidateAccountForContactAddition=*/FALSE_UNUSED);

        assertNull(result); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user2", "com.google")));

        result = mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/
                true, /*shouldValidateAccountForContactAddition=*/false);

        assertNull(result);
    }


    @Test
    public void testValidateAccountIsWritable_bothAccountNameAndTypeAreNullOrEmpty_NoException() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        mAccountResolver.validateAccountForContactAddition("", "");
        mAccountResolver.validateAccountForContactAddition(null, "");
        mAccountResolver.validateAccountForContactAddition("", null);
        mAccountResolver.validateAccountForContactAddition(null, null);
        // No exception expected
    }

    @Test
    public void testValidateAccountIsWritable_eitherAccountNameOrTypeEmpty_ThrowsException() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.validateAccountForContactAddition("accountName", "");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.validateAccountForContactAddition("accountName", null);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.validateAccountForContactAddition("", "accountType");
        });
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.validateAccountForContactAddition(null, "accountType");
        });
    }

    @Test
    public void testValidateAccountIsWritable_defaultAccountIsCloud() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofCloud(
                        new Account("test_user1", "com.google")));

        mAccountResolver.validateAccountForContactAddition("test_user1", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user2", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user3", "com.whatsapp");
        assertThrows(IllegalArgumentException.class, () ->
                mAccountResolver.validateAccountForContactAddition("", ""));
        assertThrows(IllegalArgumentException.class, () ->
                mAccountResolver.validateAccountForContactAddition(null, null));
        // No exception expected
    }

    @Test
    public void testValidateAccountIsWritable_defaultAccountIsDevice() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofLocal());

        mAccountResolver.validateAccountForContactAddition("test_user1", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user2", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user3", "com.whatsapp");
        mAccountResolver.validateAccountForContactAddition("", "");
        mAccountResolver.validateAccountForContactAddition(null, null);
        // No exception expected
    }


    @Test
    public void testValidateAccountIsWritable_defaultAccountIsNotSet() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccountAndState.ofNotSet());

        mAccountResolver.validateAccountForContactAddition("test_user1", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user2", "com.google");
        mAccountResolver.validateAccountForContactAddition("test_user3", "com.whatsapp");
        mAccountResolver.validateAccountForContactAddition("", "");
        mAccountResolver.validateAccountForContactAddition(null, null);
        // No exception expected
    }
}
