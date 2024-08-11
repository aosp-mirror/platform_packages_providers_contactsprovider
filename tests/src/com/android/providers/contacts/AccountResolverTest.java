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

import android.content.ContentValues;
import android.net.Uri;
import android.provider.ContactsContract.RawContacts;

import androidx.test.filters.SmallTest;

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

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testResolveAccountWithDataSet_accountAndDataSetInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .appendQueryParameter(RawContacts.DATA_SET, "test_data_set")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_accountInUriDataSetInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "test_account")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "com.google")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
        assertEquals("test_data_set", values.getAsString(RawContacts.DATA_SET));
    }

    @Test
    public void testResolveAccountWithDataSet_noAccount() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertNull(result);
    }

    @Test
    public void testResolveAccountWithDataSet_accountInValuesOnly() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts"); // No account in URI
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "test_account");
        values.put(RawContacts.ACCOUNT_TYPE, "com.google");
        values.put(RawContacts.DATA_SET, "test_data_set");

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
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

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertEquals("test_account", result.getAccountName());
        assertEquals("com.google", result.getAccountType());
        assertEquals("test_data_set", result.getDataSet());
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
        });
        assertEquals("Test Exception Message", exception.getMessage());
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
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
            AccountResolver.resolveAccountWithDataSet(uri, values, mDbHelper);
        });
        assertEquals("Test Exception Message", exception.getMessage());
    }

    @Test
    public void testResolveAccountWithDataSet_emptyAccountInUri() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_emptyAccountInValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertNull(result); // Expect null result as account is effectively absent
    }

    @Test
    public void testResolveAccountWithDataSet_emptyAccountInUriAndValues() {
        Uri uri = Uri.parse("content://com.android.contacts/raw_contacts")
                .buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, "")
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "")
                .build();
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, "");
        values.put(RawContacts.ACCOUNT_TYPE, "");

        AccountWithDataSet result = AccountResolver.resolveAccountWithDataSet(
                uri, values, mDbHelper);

        assertNull(result); // Expect null result as account is effectively absent
    }
}
