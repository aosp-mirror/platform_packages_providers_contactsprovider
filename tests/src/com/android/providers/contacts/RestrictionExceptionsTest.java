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

import static com.android.providers.contacts.ContactsActor.PACKAGE_BLUE;
import static com.android.providers.contacts.ContactsActor.PACKAGE_GREEN;
import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;
import static com.android.providers.contacts.ContactsActor.PACKAGE_RED;

import android.content.ContentUris;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RestrictionExceptions;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link RestrictionExceptions}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class RestrictionExceptionsTest extends AndroidTestCase {
    private static final String TAG = "RestrictionExceptionsTest";

    private static ContactsActor mGrey;
    private static ContactsActor mRed;
    private static ContactsActor mGreen;
    private static ContactsActor mBlue;

    private static final String PHONE_GREY = "555-1111";
    private static final String PHONE_RED = "555-2222";
    private static final String PHONE_GREEN = "555-3333";
    private static final String PHONE_BLUE = "555-4444";

    private static final String GENERIC_NAME = "Smith";

    public RestrictionExceptionsTest() {
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        final Context overallContext = this.getContext();

        // Build each of our specific actors in their own Contexts
        mGrey = new ContactsActor(overallContext, PACKAGE_GREY,
                SynchronousContactsProvider2.class, ContactsContract.AUTHORITY);
        mRed = new ContactsActor(overallContext, PACKAGE_RED,
                SynchronousContactsProvider2.class, ContactsContract.AUTHORITY);
        mGreen = new ContactsActor(overallContext, PACKAGE_GREEN,
                SynchronousContactsProvider2.class, ContactsContract.AUTHORITY);
        mBlue = new ContactsActor(overallContext, PACKAGE_BLUE,
                SynchronousContactsProvider2.class, ContactsContract.AUTHORITY);

        // TODO make the provider wipe data automatically
        ((SynchronousContactsProvider2)mGrey.provider).wipeData();
    }

    /**
     * Create various contacts that are both open and restricted, and assert
     * that both {@link Contacts#IS_RESTRICTED} and
     * {@link RestrictionExceptions} are being applied correctly.
     */
    public void testDataRestriction() {

        // Grey creates an unprotected contact
        long greyContact = mGrey.createContact(false);
        long greyData = mGrey.createPhone(greyContact, PHONE_GREY);
        long greyAgg = mGrey.getAggregateForContact(greyContact);

        // Assert that both Grey and Blue can read contact
        assertTrue("Owner of unrestricted contact unable to read",
                (mGrey.getDataCountForAggregate(greyAgg) == 1));
        assertTrue("Non-owner of unrestricted contact unable to read",
                (mBlue.getDataCountForAggregate(greyAgg) == 1));

        // Red grants protected access to itself
        mRed.updateException(PACKAGE_RED, PACKAGE_RED, true);

        // Red creates a protected contact
        long redContact = mRed.createContact(true);
        long redData = mRed.createPhone(redContact, PHONE_RED);
        long redAgg = mRed.getAggregateForContact(redContact);

        // Assert that only Red can read contact
        assertTrue("Owner of restricted contact unable to read",
                (mRed.getDataCountForAggregate(redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (mBlue.getDataCountForAggregate(redAgg) == 0));
        assertTrue("Non-owner of restricted contact able to read",
                (mGreen.getDataCountForAggregate(redAgg) == 0));

        try {
            // Blue tries to grant an exception for Red data, which should throw
            // exception. If it somehow worked, fail this test.
            mBlue.updateException(PACKAGE_RED, PACKAGE_BLUE, true);
            fail("Non-owner able to grant restriction exception");

        } catch (RuntimeException e) {
        }

        // Red grants exception to Blue for contact
        mRed.updateException(PACKAGE_RED, PACKAGE_BLUE, true);

        // Both Blue and Red can read Red contact, but still not Green
        assertTrue("Owner of restricted contact unable to read",
                (mRed.getDataCountForAggregate(redAgg) == 1));
        assertTrue("Non-owner with restriction exception unable to read",
                (mBlue.getDataCountForAggregate(redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (mGreen.getDataCountForAggregate(redAgg) == 0));

        // Red revokes exception to Blue
        mRed.updateException(PACKAGE_RED, PACKAGE_BLUE, false);

        // Assert that only Red can read contact
        assertTrue("Owner of restricted contact unable to read",
                (mRed.getDataCountForAggregate(redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (mBlue.getDataCountForAggregate(redAgg) == 0));
        assertTrue("Non-owner of restricted contact able to read",
                (mGreen.getDataCountForAggregate(redAgg) == 0));

    }

    /**
     * Create an aggregate that has multiple contacts with various levels of
     * protected data, and ensure that {@link Aggregates#CONTENT_SUMMARY_URI}
     * details don't expose {@link Contacts#IS_RESTRICTED} data.
     */
    public void testAggregateSummary() {

        // Red grants exceptions to itself and Grey
        mRed.updateException(PACKAGE_RED, PACKAGE_RED, true);
        mRed.updateException(PACKAGE_RED, PACKAGE_GREY, true);

        // Red creates a protected contact
        long redContact = mRed.createContact(true);
        long redName = mRed.createName(redContact, GENERIC_NAME);
        long redPhone = mRed.createPhone(redContact, PHONE_RED);

        // Blue grants exceptions to itself and Grey
        mBlue.updateException(PACKAGE_BLUE, PACKAGE_BLUE, true);
        mBlue.updateException(PACKAGE_BLUE, PACKAGE_GREY, true);

        // Blue creates a protected contact
        long blueContact = mBlue.createContact(true);
        long blueName = mBlue.createName(blueContact, GENERIC_NAME);
        long bluePhone = mBlue.createPhone(blueContact, PHONE_BLUE);

        // Set the super-primary phone number to Red
        mRed.setSuperPrimaryPhone(redPhone);

        // Make sure both aggregates were joined
        long singleAgg;
        {
            long redAgg = mRed.getAggregateForContact(redContact);
            long blueAgg = mBlue.getAggregateForContact(blueContact);
            assertTrue("Two contacts with identical name not aggregated correctly",
                    (redAgg == blueAgg));
            singleAgg = redAgg;
        }

        // Grey and Red querying summary should see Red phone. Blue shouldn't
        // see any summary data, since it's own data is protected and it's not
        // the super-primary. Green shouldn't know this aggregate exists.
        assertTrue("Participant with restriction exception reading incorrect summary",
                (mGrey.getPrimaryPhoneId(singleAgg) == redPhone));
        assertTrue("Participant with super-primary restricted data reading incorrect summary",
                (mRed.getPrimaryPhoneId(singleAgg) == redPhone));
        assertTrue("Participant with non-super-primary restricted data reading incorrect summary",
                (mBlue.getPrimaryPhoneId(singleAgg) == 0));
        assertTrue("Non-participant able to discover aggregate existance",
                (mGreen.getPrimaryPhoneId(singleAgg) == 0));

        // Add an unprotected Grey contact into the mix
        long greyContact = mGrey.createContact(false);
        long greyName = mGrey.createName(greyContact, GENERIC_NAME);
        long greyPhone = mGrey.createPhone(greyContact, PHONE_GREY);

        // Set the super-primary phone number to Blue
        mBlue.setSuperPrimaryPhone(bluePhone);

        // Make sure all three aggregates were joined
        {
            long redAgg = mRed.getAggregateForContact(redContact);
            long blueAgg = mBlue.getAggregateForContact(blueContact);
            long greyAgg = mGrey.getAggregateForContact(greyContact);
            assertTrue("Three contacts with identical name not aggregated correctly",
                    (redAgg == blueAgg) && (blueAgg == greyAgg));
            singleAgg = redAgg;
        }

        // Grey and Blue querying summary should see Blue phone. Red should see
        // the Grey phone in its summary, since it's the unprotected fallback.
        // Red doesn't see its own phone number because it's not super-primary,
        // and is protected. Again, green shouldn't know this exists.
        assertTrue("Participant with restriction exception reading incorrect summary",
                (mGrey.getPrimaryPhoneId(singleAgg) == bluePhone));
        assertTrue("Participant with non-super-primary restricted data reading incorrect summary",
                (mRed.getPrimaryPhoneId(singleAgg) == greyPhone));
        assertTrue("Participant with super-primary restricted data reading incorrect summary",
                (mBlue.getPrimaryPhoneId(singleAgg) == bluePhone));
        assertTrue("Non-participant couldn't find unrestricted primary through summary",
                (mGreen.getPrimaryPhoneId(singleAgg) == greyPhone));

    }

    /**
     * Create a contact that is completely restricted and isolated in its own
     * aggregate, and make sure that another actor can't detect its existence.
     */
    public void testRestrictionSilence() {
        Cursor cursor;

        // Green grants exception to itself
        mGreen.updateException(PACKAGE_GREEN, PACKAGE_GREEN, true);

        // Green creates a protected contact
        long greenContact = mGreen.createContact(true);
        long greenData = mGreen.createPhone(greenContact, PHONE_GREEN);
        long greenAgg = mGreen.getAggregateForContact(greenContact);

        // AGGREGATES
        cursor = mRed.resolver
                .query(Aggregates.CONTENT_URI, Projections.PROJ_ID, null, null, null);
        while (cursor.moveToNext()) {
            assertTrue("Discovered restricted contact",
                    (cursor.getLong(Projections.COL_ID) != greenAgg));
        }
        cursor.close();

        // AGGREGATES_ID
        cursor = mRed.resolver.query(ContentUris.withAppendedId(Aggregates.CONTENT_URI, greenAgg),
                Projections.PROJ_ID, null, null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // AGGREGATES_DATA
        cursor = mRed.resolver.query(Uri.withAppendedPath(ContentUris.withAppendedId(
                Aggregates.CONTENT_URI, greenAgg), Aggregates.Data.CONTENT_DIRECTORY),
                Projections.PROJ_ID, null, null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // AGGREGATES_SUMMARY
        cursor = mRed.resolver.query(Aggregates.CONTENT_SUMMARY_URI, Projections.PROJ_ID, null,
                null, null);
        while (cursor.moveToNext()) {
            assertTrue("Discovered restricted contact",
                    (cursor.getLong(Projections.COL_ID) != greenAgg));
        }
        cursor.close();

        // AGGREGATES_SUMMARY_ID
        cursor = mRed.resolver.query(ContentUris.withAppendedId(Aggregates.CONTENT_SUMMARY_URI,
                greenAgg), Projections.PROJ_ID, null, null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // TODO: AGGREGATES_SUMMARY_FILTER
        // TODO: =========================

        // TODO: AGGREGATION_SUGGESTIONS
        // TODO: =======================

        // CONTACTS
        cursor = mRed.resolver.query(Contacts.CONTENT_URI, Projections.PROJ_ID, null, null, null);
        while (cursor.moveToNext()) {
            assertTrue("Discovered restricted contact",
                    (cursor.getLong(Projections.COL_ID) != greenContact));
        }
        cursor.close();

        // CONTACTS_ID
        cursor = mRed.resolver.query(ContentUris
                .withAppendedId(Contacts.CONTENT_URI, greenContact), Projections.PROJ_ID, null,
                null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // CONTACTS_DATA
        cursor = mRed.resolver.query(Uri.withAppendedPath(ContentUris.withAppendedId(
                Contacts.CONTENT_URI, greenContact), Contacts.Data.CONTENT_DIRECTORY),
                Projections.PROJ_ID, null, null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // TODO: CONTACTS_FILTER_EMAIL
        // TODO: =====================

        // DATA
        cursor = mRed.resolver.query(Data.CONTENT_URI, Projections.PROJ_ID, null, null, null);
        while (cursor.moveToNext()) {
            assertTrue("Discovered restricted contact",
                    (cursor.getLong(Projections.COL_ID) != greenData));
        }
        cursor.close();

        // DATA_ID
        cursor = mRed.resolver.query(ContentUris.withAppendedId(Data.CONTENT_URI, greenData),
                Projections.PROJ_ID, null, null, null);
        assertTrue("Discovered restricted contact", (cursor.getCount() == 0));
        cursor.close();

        // TODO: PHONE_LOOKUP
        // TODO: ============

    }

    private interface Projections {
        static final String[] PROJ_ID = new String[] {
                BaseColumns._ID,
        };

        static final int COL_ID = 0;
    }

}
