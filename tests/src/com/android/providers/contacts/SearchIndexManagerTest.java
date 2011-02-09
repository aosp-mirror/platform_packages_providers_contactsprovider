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

import android.content.ContentValues;
import android.net.Uri;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.SearchSnippetColumns;
import android.test.suitebuilder.annotation.MediumTest;

/**
 * Unit tests for {@link SearchIndexManager}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.SearchIndexManagerTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class SearchIndexManagerTest extends BaseContactsProvider2Test {

    public void testSearchIndexForStructuredName() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertStructuredName(rawContactId, "John", "Doe");
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Bob I. Parr");
        insertStructuredName(rawContactId, values);
        values.clear();
        values.put(StructuredName.PREFIX, "Mrs.");
        values.put(StructuredName.GIVEN_NAME, "Helen");
        values.put(StructuredName.MIDDLE_NAME, "I.");
        values.put(StructuredName.FAMILY_NAME, "Parr");
        values.put(StructuredName.SUFFIX, "PhD");
        values.put(StructuredName.PHONETIC_FAMILY_NAME, "par");
        values.put(StructuredName.PHONETIC_GIVEN_NAME, "helen");
        insertStructuredName(rawContactId, values);

        assertSearchIndex(
                contactId, "Doe, John\nParr, Bob I.\nMrs. Parr, Helen I., PhD (par helen)", null);
    }

    public void testSearchIndexForOrganization() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();
        values.put(Organization.COMPANY, "Acme Inc.");
        values.put(Organization.TITLE, "Director");
        values.put(Organization.DEPARTMENT, "Phones and tablets");
        values.put(Organization.JOB_DESCRIPTION, "full text search");
        values.put(Organization.SYMBOL, "ACME");
        values.put(Organization.PHONETIC_NAME, "ack-me");
        values.put(Organization.OFFICE_LOCATION, "virtual");
        insertOrganization(rawContactId, values);

        assertSearchIndex(contactId,
                "Director, Acme Inc. (ack-me) (ACME)/Phones and tablets/virtual/full text search",
                null);
    }

    public void testSearchIndexForPhoneNumber() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertPhoneNumber(rawContactId, "800555GOOG");
        insertPhoneNumber(rawContactId, "8005551234");

        assertSearchIndex(contactId, null, "8005554664 +18005554664 8005551234 +18005551234");
    }

    public void testSearchIndexForEmail() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertEmail(rawContactId, "Bob Parr <incredible@android.com>");
        insertEmail(rawContactId, "bob_parr@android.com");

        assertSearchIndex(contactId, "Bob Parr <incredible@android.com>\nbob_parr@android.com",
                null);
    }

    public void testSearchIndexForNickname() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertNickname(rawContactId, "incredible");

        assertSearchIndex(contactId, "incredible", null);
    }

    public void testSearchIndexForStructuredPostal() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertPostalAddress(rawContactId, "1600 Amphitheatre Pkwy\nMountain View, CA 94043");
        ContentValues values = new ContentValues();
        values.put(StructuredPostal.CITY, "London");
        values.put(StructuredPostal.STREET, "76 Buckingham Palace Road");
        values.put(StructuredPostal.POSTCODE, "SW1W 9TQ");
        values.put(StructuredPostal.COUNTRY, "United Kingdom");
        insertPostalAddress(rawContactId, values);

        assertSearchIndex(contactId, "1600 Amphitheatre Pkwy Mountain View, CA 94043\n"
                + "76 Buckingham Palace Road London SW1W 9TQ United Kingdom", null);
    }

    public void testSearchIndexForIm() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertImHandle(rawContactId, Im.PROTOCOL_JABBER, null, "bp@android.com");
        insertImHandle(rawContactId, Im.PROTOCOL_CUSTOM, "android_im", "android@android.com");

        assertSearchIndex(contactId, "Jabber/bp@android.com\nandroid_im/android@android.com", null);
    }

    public void testSearchIndexForNote() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        insertNote(rawContactId, "Please note: three notes or more make up a chord.");

        assertSearchIndex(contactId, "Please note: three notes or more make up a chord.", null);
    }

    public void testSnippetArgs() {
        long rawContactId = createRawContact();
        insertNote(rawContactId, "Please note: three notes or more make up a chord.");

        assertStoredValue(
                buildSearchUri("thr", "[,],-,2"), SearchSnippetColumns.SNIPPET, "-note: [three]");
    }

    private Uri buildSearchUri(String filter, String args) {
        return Contacts.CONTENT_FILTER_URI.buildUpon().appendPath(filter).appendQueryParameter(
                SearchSnippetColumns.SNIPPET_ARGS_PARAM_KEY, args).build();
    }

    private void assertSearchIndex(long contactId, String expectedContent, String expectedTokens) {
        ContactsDatabaseHelper dbHelper = (ContactsDatabaseHelper) getContactsProvider()
                .getDatabaseHelper();
        assertEquals(expectedContent, dbHelper.querySearchIndexContent(contactId));
        assertEquals(expectedTokens, dbHelper.querySearchIndexTokens(contactId));
    }
}

