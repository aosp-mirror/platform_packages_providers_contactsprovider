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

package com.android.providers.contacts2;

import static com.android.providers.contacts2.ContactsActor.PACKAGE_BLUE;
import static com.android.providers.contacts2.ContactsActor.PACKAGE_GREEN;
import static com.android.providers.contacts2.ContactsActor.PACKAGE_GREY;
import static com.android.providers.contacts2.ContactsActor.PACKAGE_RED;

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.Contacts.Phones;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RestrictionExceptions;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

/**
 * Unit tests for {@link RestrictionExceptions}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts2.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class RestrictionExceptionsTest extends AndroidTestCase {
    private static final String TAG = "RestrictionExceptionsTest";
    private static final boolean LOGD = false;

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
        mGrey = new ContactsActor(overallContext, PACKAGE_GREY);
        mRed = new ContactsActor(overallContext, PACKAGE_RED);
        mGreen = new ContactsActor(overallContext, PACKAGE_GREEN);
        mBlue = new ContactsActor(overallContext, PACKAGE_BLUE);
    }

    /**
     * Create various contacts that are both open and restricted, and assert
     * that both {@link Contacts#IS_RESTRICTED} and
     * {@link RestrictionExceptions} are being applied correctly.
     */
    public void testDataRestriction() {

        // Clear all previous data before starting this test
        mGrey.provider.wipeData();

        // Grey creates an unprotected contact
        long greyContact = createContact(mGrey, false);
        long greyData = createPhone(mGrey, greyContact, PHONE_GREY);
        long greyAgg = getAggregateForContact(mGrey, greyContact);

        // Assert that both Grey and Blue can read contact
        assertTrue("Owner of unrestricted contact unable to read",
                (getDataCountForAggregate(mGrey, greyAgg) == 1));
        assertTrue("Non-owner of unrestricted contact unable to read",
                (getDataCountForAggregate(mBlue, greyAgg) == 1));

        // Red grants protected access to itself
        updateException(mRed, PACKAGE_RED, PACKAGE_RED, true);

        // Red creates a protected contact
        long redContact = createContact(mRed, true);
        long redData = createPhone(mRed, redContact, PHONE_RED);
        long redAgg = getAggregateForContact(mRed, redContact);

        // Assert that only Red can read contact
        assertTrue("Owner of restricted contact unable to read",
                (getDataCountForAggregate(mRed, redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (getDataCountForAggregate(mBlue, redAgg) == 0));
        assertTrue("Non-owner of restricted contact able to read",
                (getDataCountForAggregate(mGreen, redAgg) == 0));

        try {
            // Blue tries to grant an exception for Red data, which should throw
            // exception. If it somehow worked, fail this test.
            updateException(mBlue, PACKAGE_RED, PACKAGE_BLUE, true);
            fail("Non-owner able to grant restriction exception");

        } catch (RuntimeException e) {
        }

        // Red grants exception to Blue for contact
        updateException(mRed, PACKAGE_RED, PACKAGE_BLUE, true);

        // Both Blue and Red can read Red contact, but still not Green
        assertTrue("Owner of restricted contact unable to read",
                (getDataCountForAggregate(mRed, redAgg) == 1));
        assertTrue("Non-owner with restriction exception unable to read",
                (getDataCountForAggregate(mBlue, redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (getDataCountForAggregate(mGreen, redAgg) == 0));

        // Red revokes exception to Blue
        updateException(mRed, PACKAGE_RED, PACKAGE_BLUE, false);

        // Assert that only Red can read contact
        assertTrue("Owner of restricted contact unable to read",
                (getDataCountForAggregate(mRed, redAgg) == 1));
        assertTrue("Non-owner of restricted contact able to read",
                (getDataCountForAggregate(mBlue, redAgg) == 0));
        assertTrue("Non-owner of restricted contact able to read",
                (getDataCountForAggregate(mGreen, redAgg) == 0));

    }

    /**
     * Create an aggregate that has multiple contacts with various levels of
     * protected data, and ensure that {@link Aggregates#CONTENT_SUMMARY_URI}
     * details don't expose {@link Contacts#IS_RESTRICTED} data.
     */
    public void testAggregateSummary() {

        // Clear all previous data before starting this test
        mGrey.provider.wipeData();

        // Red grants exceptions to itself and Grey
        updateException(mRed, PACKAGE_RED, PACKAGE_RED, true);
        updateException(mRed, PACKAGE_RED, PACKAGE_GREY, true);

        // Red creates a protected contact
        long redContact = createContact(mRed, true);
        long redName = createName(mRed, redContact, GENERIC_NAME);
        long redPhone = createPhone(mRed, redContact, PHONE_RED);

        // Blue grants exceptions to itself and Grey
        updateException(mBlue, PACKAGE_BLUE, PACKAGE_BLUE, true);
        updateException(mBlue, PACKAGE_BLUE, PACKAGE_GREY, true);

        // Blue creates a protected contact
        long blueContact = createContact(mBlue, true);
        long blueName = createName(mBlue, blueContact, GENERIC_NAME);
        long bluePhone = createPhone(mBlue, blueContact, PHONE_BLUE);

        // Set the super-primary phone number to Red
        setSuperPrimaryPhone(mRed, redPhone);

        // Make sure both aggregates were joined
        long singleAgg;
        {
            long redAgg = getAggregateForContact(mRed, redContact);
            long blueAgg = getAggregateForContact(mBlue, blueContact);
            assertTrue("Two contacts with identical name not aggregated correctly",
                    (redAgg == blueAgg));
            singleAgg = redAgg;
        }

        // Grey and Red querying summary should see Red phone. Blue shouldn't
        // see any summary data, since it's own data is protected and it's not
        // the super-primary. Green shouldn't know this aggregate exists.
        assertTrue("Participant with restriction exception reading incorrect summary",
                (getPrimaryPhoneId(mGrey, singleAgg) == redPhone));
        assertTrue("Participant with super-primary restricted data reading incorrect summary",
                (getPrimaryPhoneId(mRed, singleAgg) == redPhone));
        assertTrue("Participant with non-super-primary restricted data reading incorrect summary",
                (getPrimaryPhoneId(mBlue, singleAgg) == 0));
        assertTrue("Non-participant able to discover aggregate existance",
                (getPrimaryPhoneId(mGreen, singleAgg) == 0));

        // Add an unprotected Grey contact into the mix
        long greyContact = createContact(mGrey, false);
        long greyName = createName(mGrey, greyContact, GENERIC_NAME);
        long greyPhone = createPhone(mGrey, greyContact, PHONE_GREY);

        // Set the super-primary phone number to Blue
        setSuperPrimaryPhone(mBlue, bluePhone);

        // Make sure all three aggregates were joined
        {
            long redAgg = getAggregateForContact(mRed, redContact);
            long blueAgg = getAggregateForContact(mBlue, blueContact);
            long greyAgg = getAggregateForContact(mGrey, greyContact);
            assertTrue("Three contacts with identical name not aggregated correctly",
                    (redAgg == blueAgg) && (blueAgg == greyAgg));
            singleAgg = redAgg;
        }

        // Grey and Blue querying summary should see Blue phone. Red should see
        // the Grey phone in its summary, since it's the unprotected fallback.
        // Red doesn't see its own phone number because it's not super-primary,
        // and is protected. Again, green shouldn't know this exists.
        assertTrue("Participant with restriction exception reading incorrect summary",
                (getPrimaryPhoneId(mGrey, singleAgg) == bluePhone));
        assertTrue("Participant with non-super-primary restricted data reading incorrect summary",
                (getPrimaryPhoneId(mRed, singleAgg) == greyPhone));
        assertTrue("Participant with super-primary restricted data reading incorrect summary",
                (getPrimaryPhoneId(mBlue, singleAgg) == bluePhone));
        assertTrue("Non-participant couldn't find unrestricted primary through summary",
                (getPrimaryPhoneId(mGreen, singleAgg) == greyPhone));

    }

    /**
     * Create a contact that is completely restricted and isolated in its own
     * aggregate, and make sure that another actor can't detect its existence.
     */
    public void testRestrictionSilence() {
        Cursor cursor;

        // Clear all previous data before starting this test
        mGrey.provider.wipeData();

        // Green grants exception to itself
        updateException(mGreen, PACKAGE_GREEN, PACKAGE_GREEN, true);

        // Green creates a protected contact
        long greenContact = createContact(mGreen, true);
        long greenData = createPhone(mGreen, greenContact, PHONE_GREEN);
        long greenAgg = getAggregateForContact(mGreen, greenContact);

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

    private long createContact(ContactsActor actor, boolean isRestricted) {
        final ContentValues values = new ContentValues();
        values.put(Contacts.PACKAGE, actor.packageName);
        if (isRestricted) {
            values.put(Contacts.IS_RESTRICTED, 1);
        }

        Uri contactUri = actor.resolver.insert(Contacts.CONTENT_URI, values);
        return ContentUris.parseId(contactUri);
    }

    private long createName(ContactsActor actor, long contactId, String name) {
        final ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.MIMETYPE, CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE);
        values.put(CommonDataKinds.StructuredName.FAMILY_NAME, name);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(Contacts.CONTENT_URI,
                contactId), Contacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = actor.resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    private long createPhone(ContactsActor actor, long contactId, String phoneNumber) {
        final ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.MIMETYPE, Phones.CONTENT_ITEM_TYPE);
        values.put(ContactsContract.CommonDataKinds.Phone.NUMBER, phoneNumber);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(Contacts.CONTENT_URI,
                contactId), Contacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = actor.resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    private void updateException(ContactsActor actor, String packageProvider,
            String packageClient, boolean allowAccess) {
        final ContentValues values = new ContentValues();
        values.put(RestrictionExceptions.PACKAGE_PROVIDER, packageProvider);
        values.put(RestrictionExceptions.PACKAGE_CLIENT, packageClient);
        values.put(RestrictionExceptions.ALLOW_ACCESS, allowAccess ? 1 : 0);
        actor.resolver.update(RestrictionExceptions.CONTENT_URI, values, null, null);
    }

    private long getAggregateForContact(ContactsActor actor, long contactId) {
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        final Cursor cursor = actor.resolver.query(contactUri, Projections.PROJ_CONTACTS, null,
                null, null);
        assertTrue("Contact didn't have an aggregate", cursor.moveToFirst());
        final long aggId = cursor.getLong(Projections.COL_CONTACTS_AGGREGATE);
        cursor.close();
        return aggId;
    }

    private int getDataCountForAggregate(ContactsActor actor, long aggId) {
        Uri contactUri = Uri.withAppendedPath(ContentUris.withAppendedId(Aggregates.CONTENT_URI,
                aggId), Aggregates.Data.CONTENT_DIRECTORY);
        final Cursor cursor = actor.resolver.query(contactUri, Projections.PROJ_ID, null, null,
                null);
        final int count = cursor.getCount();
        cursor.close();
        return count;
    }

    private void setSuperPrimaryPhone(ContactsActor actor, long dataId) {
        final ContentValues values = new ContentValues();
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        Uri updateUri = ContentUris.withAppendedId(Data.CONTENT_URI, dataId);
        actor.resolver.update(updateUri, values, null, null);
    }

    private long getPrimaryPhoneId(ContactsActor actor, long aggId) {
        Uri aggUri = ContentUris.withAppendedId(Aggregates.CONTENT_URI, aggId);
        final Cursor cursor = actor.resolver.query(aggUri, Projections.PROJ_AGGREGATES, null,
                null, null);
        long primaryPhoneId = -1;
        if (cursor.moveToFirst()) {
            primaryPhoneId = cursor.getLong(Projections.COL_AGGREGATES_PRIMARY_PHONE_ID);
        }
        if (LOGD) {
            Log.d(TAG, "for actor=" + actor.packageName + ", aggId=" + aggId
                    + ", found getCount()=" + cursor.getCount() + ", primaryPhoneId="
                    + primaryPhoneId);
        }
        cursor.close();
        return primaryPhoneId;
    }

    /**
     * Various internal database projections.
     */
    private interface Projections {
        static final String[] PROJ_ID = new String[] {
                BaseColumns._ID,
        };

        static final int COL_ID = 0;

        static final String[] PROJ_CONTACTS = new String[] {
                Contacts.AGGREGATE_ID
        };

        static final int COL_CONTACTS_AGGREGATE = 0;

        static final String[] PROJ_AGGREGATES = new String[] {
                Aggregates.PRIMARY_PHONE_ID
        };

        static final int COL_AGGREGATES_PRIMARY_PHONE_ID = 0;

    }

}
