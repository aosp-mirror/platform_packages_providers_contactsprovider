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

import com.android.providers.contacts.ContactLookupKey.LookupKeySegment;

import android.content.ContentUris;
import android.net.Uri;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.RawContacts;
import android.test.suitebuilder.annotation.LargeTest;

import java.util.ArrayList;

/**
 * Unit tests for {@link ContactLookupKey}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.ContactLookupKeyTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactLookupKeyTest extends BaseContactsProvider2Test {

    public void testLookupKeyUsingDisplayNameAndNoAccount() {
        long rawContactId1 = createRawContactWithName("John", "Doe");
        long rawContactId2 = createRawContactWithName("johndoe", null);

        assertAggregated(rawContactId1, rawContactId2);

        // Normalized display name
        String expectedLookupKey = "0n3B4537432F4531.0n3B4537432F4531";

        long contactId = queryContactId(rawContactId1);
        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.LOOKUP_KEY, expectedLookupKey);

        // Find the contact using lookup key by itself
        Uri lookupUri = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, expectedLookupKey);
        assertStoredValue(lookupUri, Contacts._ID, contactId);

        // Find the contact using both the lookup key and the correct contact ID
        assertStoredValue(ContentUris.withAppendedId(lookupUri, contactId),
                Contacts._ID, contactId);

        // Find the contact using both the lookup key and an incorrect contact ID
        assertStoredValue(ContentUris.withAppendedId(lookupUri, contactId + 1),
                Contacts._ID, contactId);
    }

    public void testLookupKeyUsingSourceIdAndNoAccount() {
        long rawContactId1 = createRawContactWithName("John", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.SOURCE_ID, "123");

        long rawContactId2 = createRawContactWithName("johndoe", null);
        storeValue(RawContacts.CONTENT_URI, rawContactId2, RawContacts.SOURCE_ID, "4.5.6");

        assertAggregated(rawContactId1, rawContactId2);

        // Two source ids, of them escaped
        String expectedLookupKey = "0i123.0e4..5..6";

        long contactId = queryContactId(rawContactId1);
        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.LOOKUP_KEY, expectedLookupKey);

        Uri lookupUri = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, expectedLookupKey);
        assertStoredValue(lookupUri, Contacts._ID, contactId);
    }

    public void testLookupKeySameSourceIdDifferentAccounts() {
        long rawContactId1 = createRawContactWithName("Dear", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.ACCOUNT_TYPE, "foo");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.ACCOUNT_NAME, "FOO");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.SOURCE_ID, "1");

        long rawContactId2 = createRawContactWithName("Deer", "Dough");
        storeValue(RawContacts.CONTENT_URI, rawContactId2, RawContacts.ACCOUNT_TYPE, "bar");
        storeValue(RawContacts.CONTENT_URI, rawContactId2, RawContacts.ACCOUNT_NAME, "BAR");
        storeValue(RawContacts.CONTENT_URI, rawContactId2, RawContacts.SOURCE_ID, "1");

        assertNotAggregated(rawContactId1, rawContactId2);

        int accountHashCode1 = ContactLookupKey.getAccountHashCode("foo", "FOO");
        int accountHashCode2 = ContactLookupKey.getAccountHashCode("bar", "BAR");

        long contactId1 = queryContactId(rawContactId1);
        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId1),
                Contacts.LOOKUP_KEY, accountHashCode1 + "i1");

        long contactId2 = queryContactId(rawContactId2);
        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId2),
                Contacts.LOOKUP_KEY, accountHashCode2 + "i1");

        Uri lookupUri1 = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, accountHashCode1 + "i1");
        assertStoredValue(lookupUri1, Contacts._ID, contactId1);

        Uri lookupUri2 = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, accountHashCode2 + "i1");
        assertStoredValue(lookupUri2, Contacts._ID, contactId2);
    }

    public void testLookupKeyChoosingLargestContact() {
        long rawContactId1 = createRawContactWithName("John", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.SOURCE_ID, "1");

        long rawContactId2 = createRawContactWithName("John", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId2, RawContacts.SOURCE_ID, "2");

        long rawContactId3 = createRawContactWithName("John", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId3, RawContacts.SOURCE_ID, "3");

        forceAggregation();

        String lookupKey = "0i1.0i2.0i3";

        long contactId = queryContactId(rawContactId1);
        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.LOOKUP_KEY, lookupKey);

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE, rawContactId1,
                rawContactId3);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE, rawContactId2,
                rawContactId3);
        assertAggregated(rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);

        long largerContactId = queryContactId(rawContactId1);
        assertStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, largerContactId),
                Contacts.LOOKUP_KEY, "0i1.0i2");
        assertStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, queryContactId(rawContactId3)),
                Contacts.LOOKUP_KEY, "0i3");

        Uri lookupUri = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, lookupKey);
        assertStoredValue(lookupUri, Contacts._ID, largerContactId);
    }

    public void testGetLookupUri() {
        long rawContactId1 = createRawContactWithName("John", "Doe");
        storeValue(RawContacts.CONTENT_URI, rawContactId1, RawContacts.SOURCE_ID, "1");

        long contactId = queryContactId(rawContactId1);
        String lookupUri = "content://com.android.contacts/contacts/lookup/0i1/" + contactId;

        Uri contentUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertEquals(lookupUri,
                Contacts.getLookupUri(mResolver, contentUri).toString());

        Uri staleLookupUri = ContentUris.withAppendedId(
                Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, "0i1"),
                contactId+2);
        assertEquals(lookupUri,
                Contacts.getLookupUri(mResolver, staleLookupUri).toString());
    }

    public void testParseLookupKey() {
        assertLookupKey("123n1248AC",
                new int[]{123},
                new boolean[]{false},
                new String[]{"1248AC"});
        assertLookupKey("0i1248AC",
                new int[]{0},
                new boolean[]{true},
                new String[]{"1248AC"});
        assertLookupKey("432e12..48AC",
                new int[]{432},
                new boolean[]{true},
                new String[]{"12.48AC"});

        assertLookupKey("123n1248AC.0i1248AC.432e12..48AC.123n1248AC",
                new int[]{123, 0, 432, 123},
                new boolean[]{false, true, true, false},
                new String[]{"1248AC", "1248AC", "12.48AC", "1248AC"});
    }

    private void assertLookupKey(String lookupKey, int[] accountHashCodes, boolean[] types,
            String[] keys) {
        ContactLookupKey key = new ContactLookupKey();
        ArrayList<LookupKeySegment> list = key.parse(lookupKey);
        assertEquals(types.length, list.size());

        for (int i = 0; i < accountHashCodes.length; i++) {
            LookupKeySegment segment = list.get(i);
            assertEquals(accountHashCodes[i], segment.accountHashCode);
            assertEquals(types[i], segment.sourceIdLookup);
            assertEquals(keys[i], segment.key);
        }
    }
}

