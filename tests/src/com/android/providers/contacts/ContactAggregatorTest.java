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

import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link ContactAggregator}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.ContactAggregatorTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactAggregatorTest extends BaseContactsProvider2Test {

    private static final String[] AGGREGATION_EXCEPTION_PROJECTION = new String[] {
            AggregationExceptions.TYPE,
            AggregationExceptions.RAW_CONTACT_ID1,
            AggregationExceptions.RAW_CONTACT_ID2
    };

    public void testCrudAggregationExceptions() throws Exception {
        long rawContactId1 = createRawContactWithName("zz", "top");
        long rawContactId2 = createRawContactWithName("aa", "bottom");

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        String selection = "(" + AggregationExceptions.RAW_CONTACT_ID1 + "=" + rawContactId1
                + " AND " + AggregationExceptions.RAW_CONTACT_ID2 + "=" + rawContactId2
                + ") OR (" + AggregationExceptions.RAW_CONTACT_ID1 + "=" + rawContactId2
                + " AND " + AggregationExceptions.RAW_CONTACT_ID2 + "=" + rawContactId1 + ")";

        // Refetch the row we have just inserted
        Cursor c = mResolver.query(AggregationExceptions.CONTENT_URI,
                AGGREGATION_EXCEPTION_PROJECTION, selection, null, null);

        assertTrue(c.moveToFirst());
        assertEquals(AggregationExceptions.TYPE_KEEP_TOGETHER, c.getInt(0));
        assertTrue((rawContactId1 == c.getLong(1) && rawContactId2 == c.getLong(2))
                || (rawContactId2 == c.getLong(1) && rawContactId1 == c.getLong(2)));
        assertFalse(c.moveToNext());
        c.close();

        // Change from TYPE_KEEP_IN to TYPE_KEEP_OUT
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        c = mResolver.query(AggregationExceptions.CONTENT_URI, AGGREGATION_EXCEPTION_PROJECTION,
                selection, null, null);

        assertTrue(c.moveToFirst());
        assertEquals(AggregationExceptions.TYPE_KEEP_SEPARATE, c.getInt(0));
        assertTrue((rawContactId1 == c.getLong(1) && rawContactId2 == c.getLong(2))
                || (rawContactId2 == c.getLong(1) && rawContactId1 == c.getLong(2)));
        assertFalse(c.moveToNext());
        c.close();

        // Delete the rule
        setAggregationException(AggregationExceptions.TYPE_AUTOMATIC,
                rawContactId1, rawContactId2);

        // Verify that the row is gone
        c = mResolver.query(AggregationExceptions.CONTENT_URI, AGGREGATION_EXCEPTION_PROJECTION,
                selection, null, null);
        assertFalse(c.moveToFirst());
        c.close();
    }

    public void testAggregationCreatesNewAggregate() {
        long rawContactId = createRawContact();

        Uri resultUri = insertStructuredName(rawContactId, "Johna", "Smitha");

        // Parse the URI and confirm that it contains an ID
        assertTrue(ContentUris.parseId(resultUri) != 0);

        long contactId = queryContactId(rawContactId);
        assertTrue(contactId != 0);

        String displayName = queryDisplayName(contactId);
        assertEquals("Johna Smitha", displayName);
    }

    public void testAggregationOfExactFullNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnb", "Smithb");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnb", "Smithb");

        assertAggregated(rawContactId1, rawContactId2, "Johnb Smithb");
    }

    public void testAggregationOfCaseInsensitiveFullNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnc", "Smithc");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnc", "smithc");

        assertAggregated(rawContactId1, rawContactId2, "Johnc Smithc");
    }

    public void testAggregationOfLastNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, null, "Johnd");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, null, "johnd");

        assertAggregated(rawContactId1, rawContactId2, "Johnd");
    }

    public void testNonAggregationOfFirstNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johne", "Smithe");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johne", null);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    // TODO: should this be allowed to match?
    public void testNonAggregationOfLastNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnf", "Smithf");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, null, "Smithf");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationOfConcatenatedFullNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johng", "Smithg");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "johngsmithg", null);

        assertAggregated(rawContactId1, rawContactId2, "Johng Smithg");
    }

    public void testAggregationOfNormalizedFullNameMatch() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "H\u00e9l\u00e8ne", "Bj\u00f8rn");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "helene bjorn", null);

        assertAggregated(rawContactId1, rawContactId2, "H\u00e9l\u00e8ne Bj\u00f8rn");
    }

    public void testAggregationOfNumericNames() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "123", null);

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "1-2-3", null);

        assertAggregated(rawContactId1, rawContactId2, "1-2-3");
    }

    public void testAggregationOfInconsistentlyParsedNames() {
        long rawContactId1 = createRawContact();

        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "604 Arizona Ave");
        values.put(StructuredName.GIVEN_NAME, "604");
        values.put(StructuredName.MIDDLE_NAME, "Arizona");
        values.put(StructuredName.FAMILY_NAME, "Ave");
        insertStructuredName(rawContactId1, values);

        long rawContactId2 = createRawContact();
        values.clear();
        values.put(StructuredName.DISPLAY_NAME, "604 Arizona Ave");
        values.put(StructuredName.GIVEN_NAME, "604");
        values.put(StructuredName.FAMILY_NAME, "Arizona Ave");
        insertStructuredName(rawContactId2, values);

        assertAggregated(rawContactId1, rawContactId2, "604 Arizona Ave");
    }

    public void testAggregationBasedOnMiddleName() {
        ContentValues values = new ContentValues();
        long rawContactId1 = createRawContact();
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.GIVEN_NAME, "Abigale");
        values.put(StructuredName.FAMILY_NAME, "James");

        insertStructuredName(rawContactId1, values);

        long rawContactId2 = createRawContact();
        values.clear();
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.GIVEN_NAME, "Marie");
        values.put(StructuredName.FAMILY_NAME, "James");
        insertStructuredName(rawContactId2, values);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberNoNameData() {
        long rawContactId1 = createRawContact();
        insertPhoneNumber(rawContactId1, "(888)555-1231");

        long rawContactId2 = createRawContact();
        insertPhoneNumber(rawContactId2, "1(888)555-1231");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenTargetAggregateHasNoName() {
        long rawContactId1 = createRawContact();
        insertPhoneNumber(rawContactId1, "(888)555-1232");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnl", "Smithl");
        insertPhoneNumber(rawContactId2, "1(888)555-1232");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenNewContactHasNoName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnm", "Smithm");
        insertPhoneNumber(rawContactId1, "(888)555-1233");

        long rawContactId2 = createRawContact();
        insertPhoneNumber(rawContactId2, "1(888)555-1233");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithDifferentNames() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Baby", "Bear");
        insertPhoneNumber(rawContactId1, "(888)555-1235");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Blind", "Mouse");
        insertPhoneNumber(rawContactId2, "1(888)555-1235");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithJustFirstName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Chick", "Notnull");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Chick", null);
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailNoNameData() {
        long rawContactId1 = createRawContact();
        insertEmail(rawContactId1, "lightning@android.com");

        long rawContactId2 = createRawContact();
        insertEmail(rawContactId2, "lightning@android.com");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailWhenTargetAggregateHasNoName() {
        long rawContactId1 = createRawContact();
        insertEmail(rawContactId1, "mcqueen@android.com");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Lightning", "McQueen");
        insertEmail(rawContactId2, "mcqueen@android.com");

        assertAggregated(rawContactId1, rawContactId2, "Lightning McQueen");
    }

    public void testAggregationBasedOnEmailWhenNewContactHasNoName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Doc", "Hudson");
        insertEmail(rawContactId1, "doc@android.com");

        long rawContactId2 = createRawContact();
        insertEmail(rawContactId2, "doc@android.com");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailWithDifferentNames() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Chick", "Hicks");
        insertEmail(rawContactId1, "hicky@android.com");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Luigi", "Guido");
        insertEmail(rawContactId2, "hicky@android.com");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationByCommonNicknameWithLastName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Bill", "Gore");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "William", "Gore");

        assertAggregated(rawContactId1, rawContactId2, "William Gore");
    }

    public void testAggregationByCommonNicknameOnly() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Lawrence", null);

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Larry", null);

        assertAggregated(rawContactId1, rawContactId2, "Lawrence");
    }

    public void testAggregationByNicknameNoStructuredName() {
        long rawContactId1 = createRawContact();
        insertNickname(rawContactId1, "Frozone");

        long rawContactId2 = createRawContact();
        insertNickname(rawContactId2, "Frozone");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationByNicknameWithDifferentNames() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Helen", "Parr");
        insertNickname(rawContactId1, "Elastigirl");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Shawn", "Johnson");
        insertNickname(rawContactId2, "Elastigirl");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationExceptionKeepIn() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnk", "Smithk");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnkx", "Smithkx");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        assertAggregated(rawContactId1, rawContactId2, "Johnkx Smithkx");

        // Assert that the empty aggregate got removed
        long newContactId1 = queryContactId(rawContactId1);
        if (contactId1 != newContactId1) {
            Cursor cursor = queryContact(contactId1);
            assertFalse(cursor.moveToFirst());
            cursor.close();
        } else {
            Cursor cursor = queryContact(contactId2);
            assertFalse(cursor.moveToFirst());
            cursor.close();
        }
    }

    public void testAggregationExceptionKeepOut() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johnh", "Smithh");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnh", "Smithh");

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationExceptionKeepOutCheckUpdatesDisplayName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Johni", "Smithi");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Johnj", "Smithj");

        long rawContactId3 = createRawContact();
        insertStructuredName(rawContactId3, "Johnm", "Smithm");

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId3);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId2, rawContactId3);

        assertAggregated(rawContactId1, rawContactId2, "Johnm Smithm");
        assertAggregated(rawContactId1, rawContactId3);

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId3);

        assertNotAggregated(rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId3);

        String displayName1 = queryDisplayName(queryContactId(rawContactId1));
        assertEquals("Johni Smithi", displayName1);

        assertAggregated(rawContactId2, rawContactId3, "Johnm Smithm");

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId2, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);

        String displayName2 = queryDisplayName(queryContactId(rawContactId1));
        assertEquals("Johni Smithi", displayName2);

        String displayName3 = queryDisplayName(queryContactId(rawContactId2));
        assertEquals("Johnj Smithj", displayName3);

        String displayName4 = queryDisplayName(queryContactId(rawContactId3));
        assertEquals("Johnm Smithm", displayName4);
    }

    public void testAggregationSuggestionsBasedOnName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Duane", null);

        // Exact name match
        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Duane", null);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        // Edit distance == 0.84
        long rawContactId3 = createRawContact();
        insertStructuredName(rawContactId3, "Dwayne", null);

        // Edit distance == 0.6
        long rawContactId4 = createRawContact();
        insertStructuredName(rawContactId4, "Donny", null);

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        long contactId3 = queryContactId(rawContactId3);

        assertSuggestions(contactId1, contactId2, contactId3);
    }

    public void testAggregationSuggestionsBasedOnPhoneNumber() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Lord", "Farquaad");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Talking", "Donkey");
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddress() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Carl", "Fredricksen");
        insertEmail(rawContactId1, "up@android.com");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Charles", "Muntz");
        insertEmail(rawContactId2, "up@android.com");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddressApproximateMatch() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Bob", null);
        insertEmail(rawContactId1, "incredible@android.com");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Lucius", "Best");
        insertEmail(rawContactId2, "incrediball@android.com");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnNickname() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Peter", "Parker");
        insertNickname(rawContactId1, "Spider-Man");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Manny", "Spider");

        long contactId1 = queryContactId(rawContactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnNicknameMatchingName() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Clark", "Kent");
        insertNickname(rawContactId1, "Superman");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Roy", "Williams");
        insertNickname(rawContactId2, "superman");

        long contactId1 = queryContactId(rawContactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnCommonNickname() {
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Dick", "Cherry");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Richard", "Cherry");

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnPhoneNumberWithFilter() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = createRawContact();
        insertStructuredName(rawContactId1, "Lord", "Farquaad");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = createRawContact();
        insertStructuredName(rawContactId2, "Talking", "Donkey");
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, "talk", contactId2);
        assertSuggestions(contactId1, "don", contactId2);
        assertSuggestions(contactId1, "", contactId2);
        assertSuggestions(contactId1, "eddie");
    }

    public void testChoosePhoto() {
        long rawContactId1 = createRawContact();
        setContactAccountName(rawContactId1, "donut");
        insertPhoto(rawContactId1);

        long rawContactId2 = createRawContact();
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        setContactAccountName(rawContactId2, "cupcake");
        long cupcakeId = ContentUris.parseId(insertPhoto(rawContactId2));

        long rawContactId3 = createRawContact();
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId3);
        setContactAccountName(rawContactId3, "flan");
        insertPhoto(rawContactId3);

        assertEquals(cupcakeId, queryPhotoId(queryContactId(rawContactId2)));
    }

    private void assertSuggestions(long contactId, long... suggestions) {
        forceAggregation();

        final Uri aggregateUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri uri = Uri.withAppendedPath(aggregateUri,
                Contacts.AggregationSuggestions.CONTENT_DIRECTORY);
        assertSuggestions(uri, suggestions);
    }

    private void assertSuggestions(long contactId, String filter, long... suggestions) {
        final Uri aggregateUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri uri = Uri.withAppendedPath(Uri.withAppendedPath(aggregateUri,
                Contacts.AggregationSuggestions.CONTENT_DIRECTORY), Uri.encode(filter));
        assertSuggestions(uri, suggestions);
    }

    private void assertSuggestions(Uri uri, long... suggestions) {
        final Cursor cursor = mResolver.query(uri,
                new String[] { Contacts._ID, Contacts.CONTACT_PRESENCE },
                null, null, null);

        assertEquals(suggestions.length, cursor.getCount());

        for (int i = 0; i < suggestions.length; i++) {
            cursor.moveToNext();
            assertEquals(suggestions[i], cursor.getLong(0));
        }

        cursor.close();
    }
}
