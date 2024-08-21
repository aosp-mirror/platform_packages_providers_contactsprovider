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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import android.accounts.Account;
import android.content.ContentValues;
import android.net.Uri;
import android.provider.ContactsContract.RawContacts;

import androidx.test.filters.SmallTest;

import com.android.providers.contacts.DefaultAccount.AccountCategory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SmallTest
@RunWith(JUnit4.class)
public class AccountResolverTest {
    @Mock
    private ContactsDatabaseHelper mDbHelper;
    @Mock
    private DefaultAccountManager mDefaultAccountManager;

    private AccountResolver mAccountResolver;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mAccountResolver = new AccountResolver(mDbHelper, mDefaultAccountManager);
    }

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
                uri, values, /*applyDefaultAccount=*/false);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsUnknown_accountAndDataSetInUri() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .appendQueryParameter(RawContacts.DATA_SET, "test_data_set")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);

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
                uri, values, /*applyDefaultAccount=*/false);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_applyDefaultAccount_accountInUriDataSetInValues() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(new DefaultAccount(
                AccountCategory.CLOUD, new Account("randomaccount1@gmail.com", "com.google")));

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

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
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsUnknown_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsDevice_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_noAccount() {
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(new DefaultAccount(
                AccountCategory.CLOUD, new Account("randomaccount1@gmail.com", "com.google")));

        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_accountInValuesOnly() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts"); // No account in URI
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google");
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertEquals("test_account", result1.getAccountName());
        assertEquals("com.google", result1.getAccountType());
        assertEquals("test_data_set", result1.getDataSet());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);

        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);

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
                    /*applyDefaultAccount=*/false);
        });

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values,
                    /*applyDefaultAccount=*/true);
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
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        // Expecting an exception due to the invalid account in the URI
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
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
                uri, values, /*applyDefaultAccount=*/false);

        assertEquals("test_account", result1.getAccountName());
        assertEquals("com.google", result1.getAccountType());
        assertEquals("test_data_set", result1.getDataSet());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);

        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

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
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });

        // Expecting an exception due to the invalid account in the URI, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, new Account(
                        "test_account", "com.google"
                )));
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
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
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the partial account in uri, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, new Account(
                        "test_account", "com.google"
                )));
        assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
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
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the partial account in uri, regardless of what is the
        // default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, new Account(
                        "test_account", "com.google"
                )));
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
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
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        // Expecting an exception due to the uri and content value's account info mismatching,
        // regardless of what is the default account
        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
        });
        assertEquals("Test Exception Message", exception.getMessage());

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(DefaultAccount.AccountCategory.CLOUD, new Account(
                        "test_account", "com.google"
                )));
        exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/false);
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
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsDeviceOrUnknown_emptyAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
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
                new DefaultAccount(AccountCategory.CLOUD,
                        new Account("test_user2", "com.google")));
        when(mDbHelper.exceptionMessage(
                "Cannot write contacts to local accounts when default account is set to cloud",
                    uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result); // Expect null result as account is effectively absent
    }


    @Test
    public void testResolveAccountWithDataSet_defaultAccountDeviceOrUnknown_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
        assertNull(result2); // Expect null result as account is effectively absent
    }


    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(AccountCategory.CLOUD,
                        new Account("test_user2", "com.google")));
        when(mDbHelper.exceptionMessage(
                "Cannot write contacts to local accounts when default account is set to cloud",
                uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }

    @Test
    public void testResolveAccountWithDataSet_ignoreDefaultAccount_emptyAccountInUriAndValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultDeviceOrUnknown_emptyAccountInUriAndValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.UNKNOWN_DEFAULT_ACCOUNT);
        AccountWithDataSet result1 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
        assertNull(result1); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                DefaultAccount.DEVICE_DEFAULT_ACCOUNT);
        AccountWithDataSet result2 = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/true);
        assertNull(result2); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_defaultAccountIsCloud_emptyAccountInUriAndValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = mAccountResolver.resolveAccountWithDataSet(
                uri, values, /*applyDefaultAccount=*/false);
        assertNull(result); // Expect null result as account is effectively absent

        when(mDefaultAccountManager.pullDefaultAccount()).thenReturn(
                new DefaultAccount(AccountCategory.CLOUD,
                        new Account("test_user2", "com.google")));
        when(mDbHelper.exceptionMessage(
                "Cannot write contacts to local accounts when default account is set to cloud",
                uri))
                .thenReturn("Test Exception Message");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            mAccountResolver.resolveAccountWithDataSet(uri, values, /*applyDefaultAccount=*/true);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }
}
