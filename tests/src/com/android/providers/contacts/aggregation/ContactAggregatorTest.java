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

package com.android.providers.contacts.aggregation;

import android.accounts.Account;
import android.app.ActivityManager;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.provider.ContactsContract.Contacts.Photo;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.test.MoreAsserts;
import android.test.suitebuilder.annotation.MediumTest;

import com.android.providers.contacts.BaseContactsProvider2Test;
import com.android.providers.contacts.ContactsProvider2;
import com.android.providers.contacts.TestUtils;
import com.android.providers.contacts.tests.R;
import com.android.providers.contacts.testutil.DataUtil;
import com.android.providers.contacts.testutil.RawContactUtil;

import com.google.android.collect.Lists;

/**
 * Unit tests for {@link ContactAggregator}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e \
 *         class com.android.providers.contacts.aggregation.ContactAggregatorTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class ContactAggregatorTest extends BaseContactsProvider2Test {

    private static final Account ACCOUNT_1 = new Account("account_name_1", "account_type_1");
    private static final Account ACCOUNT_2 = new Account("account_name_2", "account_type_2");
    private static final Account ACCOUNT_3 = new Account("account_name_3", "account_type_3");

    private static final String[] AGGREGATION_EXCEPTION_PROJECTION = new String[] {
            AggregationExceptions.TYPE,
            AggregationExceptions.RAW_CONTACT_ID1,
            AggregationExceptions.RAW_CONTACT_ID2
    };

    protected void setUp() throws Exception {
        super.setUp();
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        // Make sure to use ContactAggregator.java class
        cp.setNewAggregatorForTest(false);
    }

    public void testCrudAggregationExceptions() throws Exception {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "zz", "top");
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "aa", "bottom");

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
        long rawContactId = RawContactUtil.createRawContact(mResolver);

        Uri resultUri = DataUtil.insertStructuredName(mResolver, rawContactId, "Johna", "Smitha");

        // Parse the URI and confirm that it contains an ID
        assertTrue(ContentUris.parseId(resultUri) != 0);

        long contactId = queryContactId(rawContactId);
        assertTrue(contactId != 0);

        String displayName = queryDisplayName(contactId);
        assertEquals("Johna Smitha", displayName);
    }

    public void testAggregationOfExactFullNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnb", "Smithb");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnb", "Smithb");

        assertAggregated(rawContactId1, rawContactId2, "Johnb Smithb");
    }

    public void testAggregationIgnoresInvisibleContact() {
        Account account = new Account("accountName", "accountType");
        createAutoAddGroup(account);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Flynn", "Ryder");

        // Hide by removing from all groups
        removeGroupMemberships(rawContactId1);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Flynn", "Ryder");

        long rawContactId3 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Flynn", "Ryder");

        assertNotAggregated(rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertAggregated(rawContactId2, rawContactId3, "Flynn Ryder");
    }

    public void testAggregationOfCaseInsensitiveFullNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnc", "Smithc");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnc", "smithc");

        assertAggregated(rawContactId1, rawContactId2, "Johnc Smithc");
    }

    public void testAggregationOfLastNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, null, "Johnd");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, null, "johnd");

        assertAggregated(rawContactId1, rawContactId2, "Johnd");
    }

    public void testNonAggregationOfFirstNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johne", "Smithe");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johne", null);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationOfLastNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnf", "Smithf");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, null, "Smithf");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationOfConcatenatedFullNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johng", "Smithg");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "johngsmithg", null);

        assertAggregated(rawContactId1, rawContactId2, "Johng Smithg");
    }

    public void testAggregationOfNormalizedFullNameMatch() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "H\u00e9l\u00e8ne", "Bj\u00f8rn");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "helene bjorn", null);

        assertAggregated(rawContactId1, rawContactId2, "H\u00e9l\u00e8ne Bj\u00f8rn");
    }

    public void testAggregationOfNormalizedFullNameMatchWithReadOnlyAccount() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, new Account("acct",
                READ_ONLY_ACCOUNT_TYPE));
        DataUtil.insertStructuredName(mResolver, rawContactId1, "H\u00e9l\u00e8ne", "Bj\u00f8rn");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "helene bjorn", null);

        assertAggregated(rawContactId1, rawContactId2, "helene bjorn");
    }

    public void testAggregationOfNumericNames() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "123", null);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "1-2-3", null);

        assertAggregated(rawContactId1, rawContactId2, "1-2-3");
    }

    public void testAggregationOfInconsistentlyParsedNames() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);

        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "604 Arizona Ave");
        values.put(StructuredName.GIVEN_NAME, "604");
        values.put(StructuredName.MIDDLE_NAME, "Arizona");
        values.put(StructuredName.FAMILY_NAME, "Ave");
        DataUtil.insertStructuredName(mResolver, rawContactId1, values);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        values.clear();
        values.put(StructuredName.DISPLAY_NAME, "604 Arizona Ave");
        values.put(StructuredName.GIVEN_NAME, "604");
        values.put(StructuredName.FAMILY_NAME, "Arizona Ave");
        DataUtil.insertStructuredName(mResolver, rawContactId2, values);

        assertAggregated(rawContactId1, rawContactId2, "604 Arizona Ave");
    }

    public void testAggregationBasedOnMiddleName() {
        ContentValues values = new ContentValues();
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.GIVEN_NAME, "Abigale");
        values.put(StructuredName.FAMILY_NAME, "James");

        DataUtil.insertStructuredName(mResolver, rawContactId1, values);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        values.clear();
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.GIVEN_NAME, "Marie");
        values.put(StructuredName.FAMILY_NAME, "James");
        DataUtil.insertStructuredName(mResolver, rawContactId2, values);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberNoNameData() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "(888)555-1231");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertPhoneNumber(rawContactId2, "1(888)555-1231");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenTargetAggregateHasNoName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "(888)555-1232");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnl", "Smithl");
        insertPhoneNumber(rawContactId2, "1(888)555-1232");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWhenNewContactHasNoName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnm", "Smithm");
        insertPhoneNumber(rawContactId1, "(888)555-1233");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertPhoneNumber(rawContactId2, "1(888)555-1233");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithDifferentNames() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Baby", "Bear");
        insertPhoneNumber(rawContactId1, "(888)555-1235");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Blind", "Mouse");
        insertPhoneNumber(rawContactId2, "1(888)555-1235");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnPhoneNumberWithJustFirstName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Chick", "Notnull");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Chick", null);
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailNoNameData() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertEmail(rawContactId1, "lightning@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertEmail(rawContactId2, "lightning@android.com");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailWhenTargetAggregateHasNoName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertEmail(rawContactId1, "mcqueen@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Lightning", "McQueen");
        insertEmail(rawContactId2, "mcqueen@android.com");

        assertAggregated(rawContactId1, rawContactId2, "Lightning McQueen");
    }

    public void testAggregationBasedOnEmailWhenNewContactHasNoName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Doc", "Hudson");
        insertEmail(rawContactId1, "doc@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertEmail(rawContactId2, "doc@android.com");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationBasedOnEmailWithDifferentNames() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Chick", "Hicks");
        insertEmail(rawContactId1, "hicky@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Luigi", "Guido");
        insertEmail(rawContactId2, "hicky@android.com");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationByCommonNicknameWithLastName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Bill", "Gore");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "William", "Gore");


        if (ActivityManager.isLowRamDeviceStatic()) {
            // No common nickname DB on lowram devices.
            assertNotAggregated(rawContactId1, rawContactId2);
        } else {
            assertAggregated(rawContactId1, rawContactId2, "William Gore");
        }
    }

    public void testAggregationByCommonNicknameOnly() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Lawrence", null);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Larry", null);

        if (ActivityManager.isLowRamDeviceStatic()) {
            // No common nickname DB on lowram devices.
            assertNotAggregated(rawContactId1, rawContactId2);
        } else {
            assertAggregated(rawContactId1, rawContactId2, "Lawrence");
        }
    }

    public void testAggregationByNicknameNoStructuredName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertNickname(rawContactId1, "Frozone");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertNickname(rawContactId2, "Frozone");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationByNicknameWithDifferentNames() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Helen", "Parr");
        insertNickname(rawContactId1, "Elastigirl");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Shawn", "Johnson");
        insertNickname(rawContactId2, "Elastigirl");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationOnOrganization() {
        ContentValues values = new ContentValues();
        values.put(Organization.TITLE, "Monsters, Inc");
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertOrganization(rawContactId1, values);
        insertNickname(rawContactId1, "Boo");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertOrganization(rawContactId2, values);
        insertNickname(rawContactId2, "Rendall");   // To force reaggregation

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationByIdentity() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        insertIdentity(rawContactId1, "iden1", "namespace1");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertIdentity(rawContactId2, "iden1", "namespace1");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationExceptionKeepIn() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnk", "Smithk");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnkx", "Smithkx");

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
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johnh", "Smithh");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnh", "Smithh");

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationExceptionKeepOutCheckUpdatesDisplayName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Johni", "Smithi");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Johnj", "Smithj");

        long rawContactId3 = RawContactUtil.createRawContact(mResolver, ACCOUNT_3);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Johnm", "Smithm");

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

    public void testAggregationExceptionKeepOutCheckResultDisplayNames() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "c", "c", ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "b", "b", ACCOUNT_2);
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "a", "a", ACCOUNT_3);

        // Join all contacts
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId3);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId2, rawContactId3);

        // Separate all contacts. The order (2-3 , 1-2, 1-3) is important
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId2, rawContactId3);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId3);

        // Verify that we have three different contacts
        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        long contactId3 = queryContactId(rawContactId3);

        assertTrue(contactId1 != contactId2);
        assertTrue(contactId1 != contactId3);
        assertTrue(contactId2 != contactId3);

        // Verify that each raw contact contribute to the contact display name
        assertDisplayNameEquals(contactId1, rawContactId1);
        assertDisplayNameEquals(contactId2, rawContactId2);
        assertDisplayNameEquals(contactId3, rawContactId3);
    }

    public void testNonAggregationWithMultipleAffinities() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        assertNotAggregated(rawContactId1, rawContactId2);

        // There are two aggregates this raw contact could join, so it should join neither
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_2);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);

        // Just in case - let's make sure the original two did not get aggregated in the process
        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testReaggregateBecauseOfMultipleAffinities() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_2);
        assertAggregated(rawContactId1, rawContactId2);

        // The aggregate this raw contact could join has a raw contact from the same account,
        // The ambiguity will trigger re-aggregation. And since no data matching exists, all
        // three raw contacts are broken-up.
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregation_notAggregateByPhoneticName() {
        // Different names, but have the same phonetic name.  Shouldn't be aggregated.

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Sergey", null, "Yamada");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Lawrence", null, "Yamada");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregation_notAggregateByPhoneticName_2() {
        // Have the same phonetic name.  One has a regular name too.  Shouldn't be aggregated.

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, null, null, "Yamada");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Lawrence", null, "Yamada");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregation_PhoneticNameOnly() {
        // If a contact has no name but a phonetic name, then its display will be set from the
        // phonetic name.  In this case, we still aggregate by the display name.

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, null, null, "Yamada");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, null, null, "Yamada");

        assertAggregated(rawContactId1, rawContactId2, "Yamada");
    }

    public void testReaggregationWhenBecomesInvisible() {
        Account account = new Account("accountName", "accountType");
        createAutoAddGroup(account);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Flynn", "Ryder");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Flynn", "Ryder");

        long rawContactId3 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Flynn", "Ryder");

        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId2);

        // Hide by removing from all groups
        removeGroupMemberships(rawContactId3);

        assertAggregated(rawContactId1, rawContactId2, "Flynn Ryder");
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
    }

    public void testReaggregationWhenBecomesInvisibleSecondaryDataMatch() {
        Account account = new Account("accountName", "accountType");
        createAutoAddGroup(account);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Flynn", "Ryder");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Flynn", "Ryder");

        long rawContactId3 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Flynn", "Ryder");

        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId2);

        // Hide by removing from all groups
        removeGroupMemberships(rawContactId3);

        assertAggregated(rawContactId1, rawContactId2, "Flynn Ryder");
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
    }

    public void testReaggregationWhenBecomesVisible() {
        Account account = new Account("accountName", "accountType");
        long groupId = createAutoAddGroup(account);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Flynn", "Ryder");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Flynn", "Ryder");

        long rawContactId3 = RawContactUtil.createRawContact(mResolver, account);
        removeGroupMemberships(rawContactId3);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Flynn", "Ryder");

        assertAggregated(rawContactId1, rawContactId2, "Flynn Ryder");
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);

        insertGroupMembership(rawContactId3, groupId);

        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonSplitBecauseOfMultipleAffinitiesWhenOverridden() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_2);
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_3);
        assertAggregated(rawContactId1, rawContactId2);
        assertAggregated(rawContactId1, rawContactId3);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);
        assertAggregated(rawContactId1, rawContactId2);
        assertAggregated(rawContactId1, rawContactId3);

        // The aggregate this raw contact could join has a raw contact from the same account,
        // Let's re-aggregate the existing aggregate because of the ambiguity
        long rawContactId4 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        assertAggregated(rawContactId1, rawContactId2);     // Aggregation exception
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId4);
        assertNotAggregated(rawContactId3, rawContactId4);
    }

    public void testNonSplitWhenIdentityMatch() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId1, "iden", "namespace");
        insertIdentity(rawContactId1, "iden2", "namespace");
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_2);
        insertIdentity(rawContactId2, "iden", "namespace");
        assertAggregated(rawContactId1, rawContactId2);

        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        assertAggregated(rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId2, rawContactId3);
    }

    public void testReAggregateToConnectedComponent() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "111");
        setRawContactCustomization(rawContactId1, 1, 1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_2);
        insertPhoneNumber(rawContactId2, "111");
        setRawContactCustomization(rawContactId2, 1, 1);
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_3);
        insertIdentity(rawContactId3, "iden", "namespace");
        long rawContactId4 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                new Account("account_name_4", "account_type_4"));
        insertIdentity(rawContactId4, "iden", "namespace");

        assertAggregated(rawContactId1, rawContactId2);
        assertAggregated(rawContactId1, rawContactId3);
        assertAggregated(rawContactId1, rawContactId4);
        assertStoredValue(getContactUriForRawContact(rawContactId1),
                Contacts.STARRED, 1);
        assertStoredValue(getContactUriForRawContact(rawContactId4),
                Contacts.SEND_TO_VOICEMAIL, 0);

        long rawContactId5 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);

        assertAggregated(rawContactId1, rawContactId2);
        assertAggregated(rawContactId3, rawContactId4);
        assertNotAggregated(rawContactId1, rawContactId3);
        assertNotAggregated(rawContactId1, rawContactId5);
        assertNotAggregated(rawContactId3, rawContactId5);
        assertStoredValue(getContactUriForRawContact(rawContactId1),
                Contacts.STARRED, 1);
        assertStoredValue(getContactUriForRawContact(rawContactId1),
                Contacts.SEND_TO_VOICEMAIL, 1);

        assertStoredValue(getContactUriForRawContact(rawContactId3),
                Contacts.STARRED, 0);
        assertStoredValue(getContactUriForRawContact(rawContactId3),
                Contacts.SEND_TO_VOICEMAIL, 0);

        assertStoredValue(getContactUriForRawContact(rawContactId5),
                Contacts.STARRED, 0);
        assertStoredValue(getContactUriForRawContact(rawContactId5),
                Contacts.SEND_TO_VOICEMAIL, 0);
    }

    public void testNonAggregationFromDifferentAccountWithIdentityMisMatch() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId1, "iden1", "namespace");
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        insertIdentity(rawContactId2, "iden2", "namespace");
        DataUtil.insertStructuredName(mResolver, rawContactId2, "John", "Doe");

        // rawContact1 and rawContact2 have different identities on the same namespace,
        // which prevent them to aggregate.
        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationFromSameAccountWithoutAnyDataMatching() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationFromSameAccountNoCommonData() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertEmail(rawContactId1, "lightning1@android.com");
        insertPhoneNumber(rawContactId1, "111-222-3333");
        insertIdentity(rawContactId1, "iden1", "namespace");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertEmail(rawContactId2, "lightning2@android.com");
        insertPhoneNumber(rawContactId2, "555-666-7777");
        insertIdentity(rawContactId1, "iden2", "namespace");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationFromSameAccountEmailDifferent() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertEmail(rawContactId1, "lightning1@android.com");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertEmail(rawContactId2, "lightning2@android.com");
        insertEmail(rawContactId2, "lightning3@android.com");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationFromSameAccountIdentitySame() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId1, "iden", "namespace");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId2, "iden", "namespace");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationFromSameAccountIdentityDifferent() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId1, "iden1", "namespace1");
        insertIdentity(rawContactId1, "iden2", "namespace2");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertIdentity(rawContactId2, "iden2", "namespace1");
        insertIdentity(rawContactId2, "iden1", "namespace2");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationFromSameAccountPhoneNumberSame() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "111-222-3333");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId2, "111-222-3333");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationFromSameAccountPhoneNumberNormalizedSame() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "111-222-3333");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId2, "+1-111-222-3333");

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testNonAggregationFromSameAccountPhoneNumberDifferent() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "111-222-3333");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId2, "111-222-3334");

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationSuggestionsBasedOnName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Duane", null);

        // Exact name match
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Duane", null);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        // Edit distance == 0.84
        long rawContactId3 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId3, "Dwayne", null);

        // Edit distance == 0.6
        long rawContactId4 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId4, "Donny", null);

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        long contactId3 = queryContactId(rawContactId3);

        assertSuggestions(contactId1, contactId2, contactId3);
    }

    public void testAggregationSuggestionsBasedOnPhoneNumber() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Lord", "Farquaad");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Talking", "Donkey");
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddress() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Carl", "Fredricksen");
        insertEmail(rawContactId1, "up@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Charles", "Muntz");
        insertEmail(rawContactId2, "Up@Android.com");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnEmailAddressApproximateMatch() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Bob", null);
        insertEmail(rawContactId1, "incredible@android.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Lucius", "Best");
        insertEmail(rawContactId2, "incrediball@android.com");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnNickname() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Peter", "Parker");
        insertNickname(rawContactId1, "Spider-Man");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Manny", "Spider");

        long contactId1 = queryContactId(rawContactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnNicknameMatchingName() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Clark", "Kent");
        insertNickname(rawContactId1, "Superman");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Roy", "Williams");
        insertNickname(rawContactId2, "superman");

        long contactId1 = queryContactId(rawContactId1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnCommonNickname() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Dick", "Cherry");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Richard", "Cherry");

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertSuggestions(contactId1, contactId2);
    }

    public void testAggregationSuggestionsBasedOnPhoneNumberWithFilter() {

        // Create two contacts that would not be aggregated because of name mismatch
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Lord", "Farquaad");
        insertPhoneNumber(rawContactId1, "(888)555-1236");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Talking", "Donkey");
        insertPhoneNumber(rawContactId2, "1(888)555-1236");

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);
        assertTrue(contactId1 != contactId2);

        assertSuggestions(contactId1, "talk", contactId2);
        assertSuggestions(contactId1, "don", contactId2);
        assertSuggestions(contactId1, "", contactId2);
        assertSuggestions(contactId1, "eddie");
    }

    public void testAggregationSuggestionsDontSuggestInvisible() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "first", "last",
                ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "111-222-3333");
        insertNickname(rawContactId1, "Superman");
        insertEmail(rawContactId1, "incredible@android.com");

        // Create another with the exact same name, phone number, nickname and email.
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "first", "last",
                ACCOUNT_2);
        insertPhoneNumber(rawContactId2, "111-222-3333");
        insertNickname(rawContactId2, "Superman");
        insertEmail(rawContactId2, "incredible@android.com");

        // The aggregator should have joined them.  Split them up.
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        long contactId1 = queryContactId(rawContactId1);
        long contactId2 = queryContactId(rawContactId2);

        // Make sure they're different contacts.
        MoreAsserts.assertNotEqual(contactId1, contactId2);

        // Contact 2 should be suggested.
        assertSuggestions(contactId1, contactId2);

        // Make contact 2 invisible.
        markInvisible(contactId2);

        // Now contact 2 shuldn't be suggested.
        assertSuggestions(contactId1, new long[0]);
    }

    public void testChoosePhotoSetBeforeAggregation() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        insertPhoto(rawContactId1);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        long cupcakeId = ContentUris.parseId(insertPhoto(rawContactId2));

        long rawContactId3 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId3, "froyo", "froyo_act");
        insertPhoto(rawContactId3);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId3);
        assertEquals(cupcakeId, queryPhotoId(queryContactId(rawContactId2)));
    }

    public void testChoosePhotoSetAfterAggregation() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        insertPhoto(rawContactId1);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        long cupcakeId = ContentUris.parseId(insertPhoto(rawContactId2));

        long rawContactId3 = RawContactUtil.createRawContact(mResolver);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId3);
        setContactAccount(rawContactId3, "froyo", "froyo_act");
        insertPhoto(rawContactId3);

        assertEquals(cupcakeId, queryPhotoId(queryContactId(rawContactId2)));
    }

    // Note that for the following tests of photo aggregation, the accounts are being used to
    // set the typical photo priority that each raw contact would have, based on
    // SynchronousContactsProvider2.createPhotoPriorityResolver.  The relative priorities
    // specified there are:
    // cupcake: 3
    // donut: 2
    // froyo: 1
    // <other>: 0

    public void testChooseLargerPhotoByDimensions() {
        // Donut photo is 256x256.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        long normalEarthDataId = ContentUris.parseId(
                insertPhoto(rawContactId1, R.drawable.earth_normal));
        long normalEarthPhotoFileId = getStoredLongValue(
                ContentUris.withAppendedId(Data.CONTENT_URI, normalEarthDataId),
                Photo.PHOTO_FILE_ID);

        // Cupcake would normally have priority, but its photo is 200x200.
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        insertPhoto(rawContactId2, R.drawable.earth_200);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Larger photo (by dimensions) wins.
        assertEquals(normalEarthPhotoFileId, queryPhotoFileId(queryContactId(rawContactId1)));
    }

    public void testChooseLargerPhotoByFileSize() {
        // Donut photo is a 256x256 photo of a nebula.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        long nebulaDataId = ContentUris.parseId(
                insertPhoto(rawContactId1, R.drawable.nebula));
        long nebulaPhotoFileId = getStoredLongValue(
                ContentUris.withAppendedId(Data.CONTENT_URI, nebulaDataId),
                Photo.PHOTO_FILE_ID);

        // Cupcake would normally have priority, but its photo (of a galaxy) has the same dimensions
        // as Donut's, but a smaller filesize.
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        insertPhoto(rawContactId2, R.drawable.galaxy);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Larger photo (by filesize) wins.
        assertEquals(nebulaPhotoFileId, queryPhotoFileId(queryContactId(rawContactId1)));
    }

    public void testChooseFilePhotoOverThumbnail() {
        // Donut photo is a 256x256 photo of Earth.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        long normalEarthDataId = ContentUris.parseId(
                insertPhoto(rawContactId1, R.drawable.earth_normal));
        long normalEarthPhotoFileId = getStoredLongValue(
                ContentUris.withAppendedId(Data.CONTENT_URI, normalEarthDataId),
                Photo.PHOTO_FILE_ID);

        // Cupcake would normally have priority, but its photo of Earth is thumbnail-sized.
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        insertPhoto(rawContactId2, R.drawable.earth_small);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Larger photo (by filesize) wins.
        assertEquals(normalEarthPhotoFileId, queryPhotoFileId(queryContactId(rawContactId1)));
    }

    public void testFallbackToAccountPriorityForSamePhoto() {
        // Donut photo is a 256x256 photo of Earth.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        insertPhoto(rawContactId1, R.drawable.earth_normal);

        // Cupcake has the same 256x256 photo of Earth.
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        long cupcakeEarthDataId = ContentUris.parseId(
                insertPhoto(rawContactId2, R.drawable.earth_normal));
        long cupcakeEarthPhotoFileId = getStoredLongValue(
                ContentUris.withAppendedId(Data.CONTENT_URI, cupcakeEarthDataId),
                Photo.PHOTO_FILE_ID);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Cupcake's version of the photo wins, falling back to account priority.
        assertEquals(cupcakeEarthPhotoFileId, queryPhotoFileId(queryContactId(rawContactId1)));
    }

    public void testFallbackToAccountPriorityForDifferingThumbnails() {
        // Donut photo is a 96x96 thumbnail of Earth.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId1, "donut", "donut_act");
        insertPhoto(rawContactId1, R.drawable.earth_small);

        // Cupcake photo is the 96x96 "no contact" placeholder (smaller filesize than the Earth
        // picture, but thumbnail filesizes are ignored in the aggregator).
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        setContactAccount(rawContactId2, "cupcake", "cupcake_act");
        long cupcakeDataId = ContentUris.parseId(
                insertPhoto(rawContactId2, R.drawable.ic_contact_picture));

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // The Cupcake thumbnail wins, by account priority..
        assertEquals(cupcakeDataId, queryPhotoId(queryContactId(rawContactId1)));
    }

    public void testDisplayNameSources() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);

        assertNull(queryDisplayName(contactId));

        insertEmail(rawContactId, "eclair@android.com");
        assertEquals("eclair@android.com", queryDisplayName(contactId));

        insertPhoneNumber(rawContactId, "800-555-5555");
        assertEquals("800-555-5555", queryDisplayName(contactId));

        ContentValues values = new ContentValues();
        values.put(Organization.COMPANY, "Android");
        insertOrganization(rawContactId, values);
        assertEquals("Android", queryDisplayName(contactId));

        insertNickname(rawContactId, "Dro");
        assertEquals("Dro", queryDisplayName(contactId));

        values.clear();
        values.put(StructuredName.GIVEN_NAME, "Eclair");
        values.put(StructuredName.FAMILY_NAME, "Android");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
        assertEquals("Eclair Android", queryDisplayName(contactId));
    }

    public void testMergeSuperPrimaryName_rawContact1() {
        // Setup: raw contact #1 has a super primary name. #2 doesn't.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "name1", null, null,
                /* isSuperPrimary = */ true);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "name2", null, null,
                /* isSuperPrimary = */ false);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: the aggregate's name comes from raw contact #1
        long contactId = queryContactId(rawContactId1);
        assertEquals("name1", queryDisplayName(contactId));
    }

    public void testMergeSuperPrimaryName_rawContact2AndEdit() {
        // Setup: raw contact #2 has a super primary name. #1 doesn't.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        final Uri nameUri1 = DataUtil.insertStructuredName(mResolver, rawContactId1, "name1",
                null, null, /* isSuperPrimary = */ false);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        final Uri nameUri2 = DataUtil.insertStructuredName(mResolver, rawContactId2, "name2", null,
                null, /* isSuperPrimary = */ true);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: the aggregate's name comes from raw contact #2. This is the opposite of the check
        // inside testMergeSuperPrimaryName_rawContact1().
        long contactId = queryContactId(rawContactId1);
        assertEquals("name2", queryDisplayName(contactId));

        // Action: edit the super primary name
        final ContentValues values = new ContentValues();
        values.put(StructuredName.GIVEN_NAME, "edited name");
        mResolver.update(nameUri2, values, null, null);

        // Verify: editing the super primary name affects aggregate name
        assertEquals("edited name", queryDisplayName(contactId));

        // Action: edit the non primary name
        values.put(StructuredName.GIVEN_NAME, "edited name2");
        mResolver.update(nameUri1, values, null, null);

        // Verify: aggregate name is still based off the primary name
        assertEquals("edited name", queryDisplayName(contactId));
    }

    public void testMergedSuperPrimaryName_changeSuperPrimary() {
        // Setup: aggregated contact where raw contact #1 has a super primary name. #2 doesn't.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        final Uri nameUri1 = DataUtil.insertStructuredName(mResolver, rawContactId1, "name1",
                null, null, /* isSuperPrimary = */ true);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        final Uri nameUri2 = DataUtil.insertStructuredName(mResolver, rawContactId2, "name2", null,
                null, /* isSuperPrimary = */ false);
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Action: make raw contact 2's name super primary
        storeValue(nameUri2, Data.IS_SUPER_PRIMARY, 1);

        // Sanity check.
        assertStoredValue(nameUri1, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(nameUri2, Data.IS_SUPER_PRIMARY, 1);

        // Verify: aggregate name is based off of the newly super primary name
        long contactId = queryContactId(rawContactId1);
        assertEquals("name2", queryDisplayName(contactId));
    }

    public void testAggregationModeSuspendedSeparateTransactions() {

        // Setting aggregation mode to SUSPENDED should prevent aggregation from happening
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        storeValue(RawContacts.CONTENT_URI, rawContactId1,
                RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED);
        Uri name1 = DataUtil.insertStructuredName(mResolver, rawContactId1, "THE", "SAME");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        storeValue(RawContacts.CONTENT_URI, rawContactId2,
                RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "THE", "SAME");

        assertNotAggregated(rawContactId1, rawContactId2);

        // Changing aggregation mode to DEFAULT should change nothing
        storeValue(RawContacts.CONTENT_URI, rawContactId1,
                RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DEFAULT);
        storeValue(RawContacts.CONTENT_URI, rawContactId2,
                RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DEFAULT);
        assertNotAggregated(rawContactId1, rawContactId2);

        // Changing the name should trigger aggregation
        storeValue(name1, StructuredName.GIVEN_NAME, "the");
        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationModeInitializedAsSuspended() throws Exception {

        // Setting aggregation mode to SUSPENDED should prevent aggregation from happening
        ContentProviderOperation cpo1 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();
        ContentProviderOperation cpo2 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 0)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo3 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();
        ContentProviderOperation cpo4 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 2)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo5 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 0)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DEFAULT)
                .build();
        ContentProviderOperation cpo6 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 2)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DEFAULT)
                .build();

        ContentProviderResult[] results =
                mResolver.applyBatch(ContactsContract.AUTHORITY,
                        Lists.newArrayList(cpo1, cpo2, cpo3, cpo4, cpo5, cpo6));

        long rawContactId1 = ContentUris.parseId(results[0].uri);
        long rawContactId2 = ContentUris.parseId(results[2].uri);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationModeUpdatedToSuspended() throws Exception {

        // Setting aggregation mode to SUSPENDED should prevent aggregation from happening
        ContentProviderOperation cpo1 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValues(new ContentValues())
                .build();
        ContentProviderOperation cpo2 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 0)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo3 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValues(new ContentValues())
                .build();
        ContentProviderOperation cpo4 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 2)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo5 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 0)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();
        ContentProviderOperation cpo6 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 2)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();

        ContentProviderResult[] results =
                mResolver.applyBatch(ContactsContract.AUTHORITY,
                        Lists.newArrayList(cpo1, cpo2, cpo3, cpo4, cpo5, cpo6));

        long rawContactId1 = ContentUris.parseId(results[0].uri);
        long rawContactId2 = ContentUris.parseId(results[2].uri);

        assertNotAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationModeSuspendedOverriddenByAggException() throws Exception {
        ContentProviderOperation cpo1 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts.ACCOUNT_NAME, "a")
                .withValue(RawContacts.ACCOUNT_TYPE, "b")
                .build();
        ContentProviderOperation cpo2 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 0)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo3 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts.ACCOUNT_NAME, "c")
                .withValue(RawContacts.ACCOUNT_TYPE, "d")
                .build();
        ContentProviderOperation cpo4 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 2)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderOperation cpo5 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 0)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();
        ContentProviderOperation cpo6 = ContentProviderOperation.newUpdate(RawContacts.CONTENT_URI)
                .withSelection(RawContacts._ID + "=?", new String[1])
                .withSelectionBackReference(0, 2)
                .withValue(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_SUSPENDED)
                .build();

        // Checking that aggregation mode SUSPENDED should be overridden by inserting
        // an explicit aggregation exception
        ContentProviderOperation cpo7 =
                ContentProviderOperation.newUpdate(AggregationExceptions.CONTENT_URI)
                .withValueBackReference(AggregationExceptions.RAW_CONTACT_ID1, 0)
                .withValueBackReference(AggregationExceptions.RAW_CONTACT_ID2, 2)
                .withValue(AggregationExceptions.TYPE, AggregationExceptions.TYPE_KEEP_TOGETHER)
                .build();

        ContentProviderResult[] results =
                mResolver.applyBatch(ContactsContract.AUTHORITY,
                        Lists.newArrayList(cpo1, cpo2, cpo3, cpo4, cpo5, cpo6, cpo7));

        long rawContactId1 = ContentUris.parseId(results[0].uri);
        long rawContactId2 = ContentUris.parseId(results[2].uri);

        assertAggregated(rawContactId1, rawContactId2);
    }

    public void testAggregationSuggestionsQueryBuilderWithContactId() throws Exception {
        Uri uri = AggregationSuggestions.builder().setContactId(12).setLimit(7).build();
        assertEquals("content://com.android.contacts/contacts/12/suggestions?limit=7",
                uri.toString());
    }

    public void testAggregationSuggestionsQueryBuilderWithValues() throws Exception {
        Uri uri = AggregationSuggestions.builder()
                .addNameParameter("name1")
                .addNameParameter("name2")
                .setLimit(7)
                .build();
        assertEquals("content://com.android.contacts/contacts/0/suggestions?"
                + "limit=7"
                + "&query=name%3Aname1"
                + "&query=name%3Aname2", uri.toString());
    }

    public void testAggregatedStatusUpdate() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        Uri dataUri1 = DataUtil.insertStructuredName(mResolver, rawContactId1, "john", "doe");
        insertStatusUpdate(ContentUris.parseId(dataUri1), StatusUpdates.AWAY, "Green", 100,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        Uri dataUri2 = DataUtil.insertStructuredName(mResolver, rawContactId2, "john", "doe");
        insertStatusUpdate(ContentUris.parseId(dataUri2), StatusUpdates.AVAILABLE, "Red", 50,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);

        assertStoredValue(getContactUriForRawContact(rawContactId1),
                Contacts.CONTACT_STATUS, "Green");

        // When we split these two raw contacts, their respective statuses should be restored
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_SEPARATE, rawContactId1, rawContactId2);

        assertStoredValue(getContactUriForRawContact(rawContactId1),
                Contacts.CONTACT_STATUS, "Green");

        assertStoredValue(getContactUriForRawContact(rawContactId2),
                Contacts.CONTACT_STATUS, "Red");
    }

    public void testAggregationSuggestionsByName() throws Exception {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "first1", "last1");
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "first2", "last2");

        Uri uri = AggregationSuggestions.builder()
                .addNameParameter("last1 first1")
                .build();

        Cursor cursor = mResolver.query(
                uri, new String[] { Contacts._ID, Contacts.DISPLAY_NAME }, null, null, null);

        assertEquals(1, cursor.getCount());

        cursor.moveToFirst();

        ContentValues values = new ContentValues();
        values.put(Contacts._ID, queryContactId(rawContactId1));
        values.put(Contacts.DISPLAY_NAME, "first1 last1");
        assertCursorValues(cursor, values);
        cursor.close();
    }

    public void testAggregation_phoneticNamePriority1() {
        // Setup: one raw contact has a complex phonetic name and the other a simple given name
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertPhoneticName(mResolver, rawContactId1, "name phonetic");
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name", ACCOUNT_1);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: given name is used instead of phonetic, contrary to results of
        // testAggregation_nameComplexity
        long contactId = queryContactId(rawContactId1);
        assertEquals("name", queryDisplayName(contactId));
    }

    // Same as testAggregation_phoneticNamePriority1, but with setup order reversed
    public void testAggregation_phoneticNamePriority2() {
        // Setup: one raw contact has a complex phonetic name and the other a simple given name
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name", ACCOUNT_1);
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        DataUtil.insertPhoneticName(mResolver, rawContactId1, "name phonetic");

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: given name is used instead of phonetic, contrary to results of
        // testAggregation_nameComplexity
        long contactId = queryContactId(rawContactId1);
        assertEquals("name", queryDisplayName(contactId));
    }

    public void testAggregation_nameComplexity1() {
        // Setup: two names, one of which is unambiguously more complex
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name", ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name phonetic", ACCOUNT_1);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: more complex name is used
        long contactId = queryContactId(rawContactId1);
        assertEquals("name phonetic", queryDisplayName(contactId));
    }

    // Same as testAggregation_nameComplexity1, but with setup order reversed
    public void testAggregation_nameComplexity2() {
        // Setup: two names, one of which is unambiguously more complex
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name phonetic", ACCOUNT_1);
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, null,
                "name", ACCOUNT_1);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: more complex name is used
        long contactId = queryContactId(rawContactId1);
        assertEquals("name phonetic", queryDisplayName(contactId));
    }

    public void testAggregation_clearSuperPrimary() {
        // Three types of mime-type super primary merging are tested here
        // 1. both raw contacts have super primary phone numbers
        // 2. both raw contacts have emails, but only one has super primary email
        // 3. only raw contact1 has organizations and it has set the super primary organization
        ContentValues values = new ContentValues();
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        Uri uri_phone1a = insertPhoneNumber(rawContactId1, "(222)222-2222", true, true);
        Uri uri_phone1b = insertPhoneNumber(rawContactId1, "(555)555-5555", false, false);
        Uri uri_email1 = insertEmail(rawContactId1, "one@gmail.com", true, true);
        values.clear();
        values.put(Organization.COMPANY, "Monsters Inc");
        Uri uri_org1 = insertOrganization(rawContactId1, values, true, true);
        values.clear();
        values.put(Organization.TITLE, "CEO");
        Uri uri_org2 = insertOrganization(rawContactId1, values, false, false);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_2);
        Uri uri_phone2 = insertPhoneNumber(rawContactId2, "(333)333-3333", true, true);
        Uri uri_email2 = insertEmail(rawContactId2, "two@gmail.com", false, false);

        // Two raw contacts with same phone number will trigger the aggregation
        Uri uri_phone3 = insertPhoneNumber(rawContactId1, "(111)111-1111", true, true);
        Uri uri_phone4 = insertPhoneNumber(rawContactId2, "1(111)111-1111", true, true);

        // After aggregation, the super primary flag should only be cleared for case 1,
        // i.e., phone mime-type. Both case 2 and 3, i.e. organization and email mime-types,
        // have the super primary flag unchanged.
        assertAggregated(rawContactId1, rawContactId2);
        assertSuperPrimary(ContentUris.parseId(uri_phone1a), false);
        assertSuperPrimary(ContentUris.parseId(uri_phone1b), false);
        assertSuperPrimary(ContentUris.parseId(uri_phone2), false);
        assertSuperPrimary(ContentUris.parseId(uri_phone3), false);
        assertSuperPrimary(ContentUris.parseId(uri_phone4), false);

        assertSuperPrimary(ContentUris.parseId(uri_email1), true);
        assertSuperPrimary(ContentUris.parseId(uri_email2), false);

        assertSuperPrimary(ContentUris.parseId(uri_org1), true);
        assertSuperPrimary(ContentUris.parseId(uri_org2), false);
    }

    public void testAggregation_clearSuperPrimarySingleMimetype() {
        // Setup: two raw contacts, each has a single name. One of the names is super primary.
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, ACCOUNT_1);
        final Uri uri = DataUtil.insertStructuredName(mResolver, rawContactId1, "name1",
                null, null, /* isSuperPrimary = */ true);

        // Sanity check.
        assertStoredValue(uri, Data.IS_SUPER_PRIMARY, 1);

        // Action: aggregate
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1,
                rawContactId2);

        // Verify: name is still super primary
        assertStoredValue(uri, Data.IS_SUPER_PRIMARY, 1);
    }

    public void testNotAggregate_TooManyRawContactsInCandidate() {
        long preId= 0;
        for (int i = 0; i < ContactAggregator.AGGREGATION_CONTACT_SIZE_LIMIT; i++) {
            long id = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");
            if (i >  0) {
                setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, preId, id);
            }
            preId = id;
        }
        // Although the newly added raw contact matches the names with other raw contacts,
        // but the best matching contact has already meets the size limit, so keep the new raw
        // contact separate from other raw contacts.
        long newId = RawContactUtil.createRawContact(mResolver,
                new Account("account_new", "new account type"));
        DataUtil.insertStructuredName(mResolver, newId, "John", "Doe");
        assertNotAggregated(preId, newId);
        assertTrue(queryContactId(newId) > 0);
    }

    private void assertSuggestions(long contactId, long... suggestions) {
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

        try {
            assertEquals(suggestions.length, cursor.getCount());

            for (int i = 0; i < suggestions.length; i++) {
                cursor.moveToNext();
                assertEquals(suggestions[i], cursor.getLong(0));
            }
        } finally {
            TestUtils.dumpCursor(cursor);
        }

        cursor.close();
    }

    private void assertDisplayNameEquals(long contactId, long rawContactId) {

        String contactDisplayName = queryDisplayName(contactId);

        Cursor c = queryRawContact(rawContactId);
        assertTrue(c.moveToFirst());
        String rawDisplayName = c.getString(c.getColumnIndex(RawContacts.DISPLAY_NAME_PRIMARY));
        c.close();

        assertTrue(contactDisplayName != null);
        assertTrue(rawDisplayName != null);
        assertEquals(rawDisplayName, contactDisplayName);
    }
}
