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
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link ContactAggregator}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactAggregatorTest extends BaseContactsProvider2Test {

    private static final String[] AGGREGATION_EXCEPTION_PROJECTION = new String[] {
            AggregationExceptions.TYPE,
            AggregationExceptions.AGGREGATE_ID,
            AggregationExceptions.CONTACT_ID
    };

    public void testCrudAggregationExceptions() throws Exception {
        long contactId1 = createContact();
        long aggregateId = queryAggregateId(contactId1);
        long contactId2 = createContact();

        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, aggregateId, contactId2);

        // Refetch the row we have just inserted
        Cursor c = mResolver.query(AggregationExceptions.CONTENT_URI,
                AGGREGATION_EXCEPTION_PROJECTION, AggregationExceptions.AGGREGATE_ID + "="
                        + aggregateId, null, null);

        assertTrue(c.moveToFirst());
        assertEquals(AggregationExceptions.TYPE_KEEP_IN, c.getInt(0));
        assertEquals(aggregateId, c.getLong(1));
        assertEquals(contactId2, c.getLong(2));
        assertFalse(c.moveToNext());
        c.close();

        // Change from TYPE_KEEP_IN to TYPE_KEEP_OUT
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, aggregateId, contactId2);

        c = mResolver.query(AggregationExceptions.CONTENT_URI,
                AGGREGATION_EXCEPTION_PROJECTION, AggregationExceptions.AGGREGATE_ID + "="
                        + aggregateId, null, null);

        assertTrue(c.moveToFirst());
        assertEquals(AggregationExceptions.TYPE_KEEP_OUT, c.getInt(0));
        assertEquals(aggregateId, c.getLong(1));
        assertEquals(contactId2, c.getLong(2));
        assertFalse(c.moveToNext());
        c.close();

        // Delete the rule
        setAggregationException(AggregationExceptions.TYPE_AUTOMATIC, aggregateId, contactId2);

        // Verify that the row is gone
        c = mResolver.query(AggregationExceptions.CONTENT_URI,
                AGGREGATION_EXCEPTION_PROJECTION, AggregationExceptions.AGGREGATE_ID + "="
                        + aggregateId, null, null);
        assertFalse(c.moveToFirst());
        c.close();
    }

    public void testAggregationCreatesNewAggregate() {
        long contactId = createContact();

        Uri resultUri = insertStructuredName(contactId, "Johna", "Smitha");

        // Parse the URI and confirm that it contains an ID
        assertTrue(ContentUris.parseId(resultUri) != 0);

        long aggregateId = queryAggregateId(contactId);
        assertTrue(aggregateId != 0);

        String displayName = queryDisplayName(aggregateId);
        assertEquals("Johna Smitha", displayName);
    }

    public void testAggregationOfExactFullNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnb", "Smithb");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnb", "Smithb");

        assertAggregated(contactId1, contactId2, "Johnb Smithb");
    }

    public void testAggregationOfCaseInsensitiveFullNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnc", "Smithc");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnc", "smithc");

        assertAggregated(contactId1, contactId2, "Johnc Smithc");
    }

    public void testAggregationOfLastNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, null, "Johnd");

        long contactId2 = createContact();
        insertStructuredName(contactId2, null, "johnd");

        assertAggregated(contactId1, contactId2, "Johnd");
    }

    public void testNonAggregationOfFirstNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johne", "Smithe");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johne", null);

        assertNotAggregated(contactId1, contactId2);
    }

    // TODO: should this be allowed to match?
    public void testNonAggregationOfLastNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnf", "Smithf");

        long contactId2 = createContact();
        insertStructuredName(contactId2, null, "Smithf");

        assertNotAggregated(contactId1, contactId2);
    }

    public void testAggregationOfConcatenatedFullNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johng", "Smithg");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "johngsmithg", null);

        assertAggregated(contactId1, contactId2, "Johng Smithg");
    }

    public void testAggregationOfNormalizedFullNameMatch() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "H\u00e9l\u00e8ne", "Bj\u00f8rn");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "helene bjorn", null);

        assertAggregated(contactId1, contactId2, "H\u00e9l\u00e8ne Bj\u00f8rn");
    }

    public void testAggregationBasedOnPhoneNumberNoNameData() {
        long contactId1 = createContact();
        insertPhoneNumber(contactId1, "(888)555-1231");

        long contactId2 = createContact();
        insertPhoneNumber(contactId2, "1(888)555-1231");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenTargetAggregateHasNoName() {
        long contactId1 = createContact();
        insertPhoneNumber(contactId1, "(888)555-1232");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnl", "Smithl");
        insertPhoneNumber(contactId2, "1(888)555-1232");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenNewContactHasNoName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnm", "Smithm");
        insertPhoneNumber(contactId1, "(888)555-1233");

        long contactId2 = createContact();
        insertPhoneNumber(contactId2, "1(888)555-1233");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithSimilarNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Ogre", "Hunter");
        insertPhoneNumber(contactId1, "(888)555-1234");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Opra", "Humper");
        insertPhoneNumber(contactId2, "1(888)555-1234");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithDifferentNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Baby", "Bear");
        insertPhoneNumber(contactId1, "(888)555-1235");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Blind", "Mouse");
        insertPhoneNumber(contactId2, "1(888)555-1235");

        assertNotAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnEmailNoNameData() {
        long contactId1 = createContact();
        insertEmail(contactId1, "lightning@android.com");

        long contactId2 = createContact();
        insertEmail(contactId2, "lightning@android.com");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnEmailWhenTargetAggregateHasNoName() {
        long contactId1 = createContact();
        insertEmail(contactId1, "mcqueen@android.com");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Lightning", "McQueen");
        insertEmail(contactId2, "mcqueen@android.com");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnEmailWhenNewContactHasNoName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Doc", "Hudson");
        insertEmail(contactId1, "doc@android.com");

        long contactId2 = createContact();
        insertEmail(contactId2, "doc@android.com");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnEmailWithSimilarNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Sally", "Carrera");
        insertEmail(contactId1, "sally@android.com");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Sallie", "Carerra");
        insertEmail(contactId2, "sally@android.com");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationBasedOnEmailWithDifferentNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Chick", "Hicks");
        insertEmail(contactId1, "hicky@android.com");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Luigi", "Guido");
        insertEmail(contactId2, "hicky@android.com");

        assertNotAggregated(contactId1, contactId2);
    }

    public void testAggregationByCommonNicknameWithLastName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Bill", "Gore");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "William", "Gore");

        assertAggregated(contactId1, contactId2, "William Gore");
    }

    public void testAggregationByCommonNicknameOnly() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Lawrence", null);

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Larry", null);

        assertAggregated(contactId1, contactId2, "Lawrence");
    }

    public void testAggregationByNicknameNoStructuredName() {
        long contactId1 = createContact();
        insertNickname(contactId1, "Frozone");

        long contactId2 = createContact();
        insertNickname(contactId2, "Frozone");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationByNicknameWithSimilarNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Buddy", "Pine");
        insertNickname(contactId1, "Syndrome");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Body", "Pane");
        insertNickname(contactId2, "Syndrome");

        assertAggregated(contactId1, contactId2);
    }

    public void testAggregationByNicknameWithDifferentNames() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Helen", "Parr");
        insertNickname(contactId1, "Elastigirl");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Shawn", "Johnson");
        insertNickname(contactId2, "Elastigirl");

        assertNotAggregated(contactId1, contactId2);
    }

    public void testAggregationExceptionKeepIn() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnk", "Smithk");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnkx", "Smithkx");

        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);

        setAggregationException(AggregationExceptions.TYPE_KEEP_IN,
                queryAggregateId(contactId1), contactId2);

        assertAggregated(contactId1, contactId2, "Johnkx Smithkx");

        // Assert that the empty aggregate got removed
        long newAggregateId1 = queryAggregateId(contactId1);
        if (aggregateId1 != newAggregateId1) {
            Cursor cursor = queryAggregate(aggregateId1);
            assertFalse(cursor.moveToFirst());
            cursor.close();
        } else {
            Cursor cursor = queryAggregate(aggregateId2);
            assertFalse(cursor.moveToFirst());
            cursor.close();
        }
    }

    public void testAggregationExceptionKeepOut() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johnh", "Smithh");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnh", "Smithh");

        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT,
                queryAggregateId(contactId1), contactId2);

        assertNotAggregated(contactId1, contactId2);
    }

    public void testAggregationExceptionKeepOutCheckUpdatesDisplayName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Johni", "Smithi");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Johnj", "Smithj");

        setAggregationException(AggregationExceptions.TYPE_KEEP_IN,
                queryAggregateId(contactId1), contactId2);

        assertAggregated(contactId1, contactId2, "Johnj Smithj");

        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT,
                queryAggregateId(contactId1), contactId2);

        assertNotAggregated(contactId1, contactId2);

        String displayName1 = queryDisplayName(queryAggregateId(contactId1));
        assertEquals("Johni Smithi", displayName1);

        String displayName2 = queryDisplayName(queryAggregateId(contactId2));
        assertEquals("Johnj Smithj", displayName2);
    }

    public void testAggregationSuggestionsBasedOnName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Duane", null);

        // Exact name match
        long contactId2 = createContact();
        insertStructuredName(contactId2, "Duane", null);
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT,
                queryAggregateId(contactId1), contactId2);

        // Edit distance == 0.84
        long contactId3 = createContact();
        insertStructuredName(contactId3, "Dwayne", null);

        // Edit distance == 0.6
        long contactId4 = createContact();
        insertStructuredName(contactId4, "Donny", null);

        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        long aggregateId3 = queryAggregateId(contactId3);

        assertSuggestions(aggregateId1, aggregateId2, aggregateId3);
    }

    public void testAggregationSuggestionsBasedOnPhoneNumber() {

        // Create two contacts that would not be aggregated because of name mismatch
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Lord", "Farquaad");
        insertPhoneNumber(contactId1, "(888)555-1236");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Talking", "Donkey");
        insertPhoneNumber(contactId2, "1(888)555-1236");

        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 != aggregateId2);

        assertSuggestions(aggregateId1, aggregateId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddress() {

        // Create two contacts that would not be aggregated because of name mismatch
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Carl", "Fredricksen");
        insertEmail(contactId1, "up@android.com");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Charles", "Muntz");
        insertEmail(contactId2, "up@android.com");

        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 != aggregateId2);

        assertSuggestions(aggregateId1, aggregateId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddressApproximateMatch() {

        // Create two contacts that would not be aggregated because of name mismatch
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Bob", null);
        insertEmail(contactId1, "incredible2004@android.com");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Lucius", "Best");
        insertEmail(contactId2, "incrediball@androidd.com");

        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 != aggregateId2);

        assertSuggestions(aggregateId1, aggregateId2);
    }

    public void testAggregationSuggestionsBasedOnNickname() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Peter", "Parker");
        insertNickname(contactId1, "Spider-Man");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Manny", "Spider");

        long aggregateId1 = queryAggregateId(contactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, aggregateId1, contactId2);

        long aggregateId2 = queryAggregateId(contactId2);
        assertSuggestions(aggregateId1, aggregateId2);
    }

    public void testAggregationSuggestionsBasedOnNicknameMatchingName() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Clark", "Kent");
        insertNickname(contactId1, "Superman");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Roy", "Williams");
        insertNickname(contactId2, "superman");

        long aggregateId1 = queryAggregateId(contactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, aggregateId1, contactId2);

        long aggregateId2 = queryAggregateId(contactId2);
        assertSuggestions(aggregateId1, aggregateId2);
    }

    public void testAggregationSuggestionsBasedOnCommonNickname() {
        long contactId1 = createContact();
        insertStructuredName(contactId1, "Dick", "Cherry");

        long contactId2 = createContact();
        insertStructuredName(contactId2, "Richard", "Cherry");

        long aggregateId1 = queryAggregateId(contactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, aggregateId1, contactId2);

        long aggregateId2 = queryAggregateId(contactId2);
        assertSuggestions(aggregateId1, aggregateId2);
    }

    private void assertSuggestions(long aggregateId, long... suggestions) {
        final Uri aggregateUri = ContentUris.withAppendedId(Aggregates.CONTENT_URI, aggregateId);
        Uri uri = Uri.withAppendedPath(aggregateUri,
                Aggregates.AggregationSuggestions.CONTENT_DIRECTORY);
        final Cursor cursor = mResolver.query(uri, new String[] { Aggregates._ID },
                null, null, null);

        assertEquals(suggestions.length, cursor.getCount());

        for (int i = 0; i < suggestions.length; i++) {
            cursor.moveToNext();
            assertEquals(suggestions[i], cursor.getLong(0));
        }

        cursor.close();
    }
}
