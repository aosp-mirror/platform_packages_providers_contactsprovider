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

import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;
import static com.android.providers.contacts.ContactsActor.PACKAGE_RED;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.LiveFolders;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.InputStream;

/**
 * Unit tests for {@link RawContacts#IS_RESTRICTED}.
 */
@LargeTest
public class RestrictionExceptionsTest extends AndroidTestCase {
    private ContactsActor mGrey;
    private ContactsActor mRed;

    private static final String PHONE_GREY = "555-1111";
    private static final String PHONE_RED = "555-2222";

    private static final String EMAIL_GREY = "user@example.com";
    private static final String EMAIL_RED = "user@example.org";

    private static final String GENERIC_STATUS = "Status update";
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

        // TODO make the provider wipe data automatically
        ((SynchronousContactsProvider2)mGrey.provider).wipeData();
    }

    /**
     * Assert that {@link Contacts#CONTACT_STATUS} matches the given value, or
     * that no rows are returned when null.
     */
    void assertStatus(ContactsActor actor, long aggId, String status) {
        final Uri aggUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, aggId);

        actor.ensureCallingPackage();
        final Cursor cursor = actor.resolver.query(aggUri,
                new String[] { Contacts.CONTACT_STATUS }, null, null, null);
        try {
            if (status == null) {
                assertEquals(0, cursor.getCount());
            } else {
                while (cursor.moveToNext()) {
                    final String foundStatus = cursor.getString(0);
                    assertEquals(status, foundStatus);
                }
            }
        } finally {
            cursor.close();
        }
    }

    public void testRestrictedInsertRestrictedQuery() {
        // Restricted query can read restricted data
        final long rawContact = mGrey.createRawContact(true, GENERIC_NAME);
        final int count = mGrey.getDataCountForRawContact(rawContact);
        assertEquals(1, count);
    }

    public void testRestrictedInsertGenericQuery() {
        // Generic query is denied restricted data
        final long rawContact = mGrey.createRawContact(true, GENERIC_NAME);
        final int count = mRed.getDataCountForRawContact(rawContact);
        assertEquals(0, count);
    }

    public void testGenericInsertRestrictedQuery() {
        // Restricted query can read generic data
        final long rawContact = mRed.createRawContact(false, GENERIC_NAME);
        final int count = mGrey.getDataCountForRawContact(rawContact);
        assertEquals(1, count);
    }

    public void testGenericInsertGenericQuery() {
        // Generic query can read generic data
        final long rawContact = mRed.createRawContact(false, GENERIC_NAME);
        final int count = mRed.getDataCountForRawContact(rawContact);
        assertEquals(1, count);
    }

    public void testMixedAggregateRestrictedQuery() {
        // Create mixed aggregate with a restricted phone number
        final long greyRawContactId = mGrey.createRawContact(true, GENERIC_NAME);
        mGrey.createPhone(greyRawContactId, PHONE_GREY);
        final long redRawContactId = mRed.createRawContact(false, GENERIC_NAME);
        mRed.createPhone(redRawContactId, PHONE_RED);
        mGrey.setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, greyRawContactId, redRawContactId);

        // Make sure both aggregates were joined
        final long greyAgg = mGrey.getContactForRawContact(greyRawContactId);
        final long redAgg = mRed.getContactForRawContact(redRawContactId);
        assertEquals(greyAgg, redAgg);

        // Restricted reader should have access to both numbers
        final int greyCount = mGrey.getDataCountForContact(greyAgg);
        assertEquals(4, greyCount);

        // Generic reader should have limited access
        final int redCount = mRed.getDataCountForContact(redAgg);
        assertEquals(2, redCount);
    }

    public void testUpdateRestricted() {
        // Assert that we can't un-restrict something
        final long greyContact = mGrey.createRawContact(true, GENERIC_NAME);
        final long greyPhone = mGrey.createPhone(greyContact, PHONE_GREY);

        int count = mRed.getDataCountForRawContact(greyContact);
        assertEquals(0, count);

        // Try un-restricting that contact
        final Uri greyUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, greyContact);
        final ContentValues values = new ContentValues();
        values.put(RawContacts.IS_RESTRICTED, 0);
        mRed.ensureCallingPackage();
        mRed.provider.update(greyUri, values, null, null);

        count = mRed.getDataCountForRawContact(greyContact);
        assertEquals(0, count);
    }

    public void testExportVCard() throws Exception {
        // Create mixed aggregate with a restricted phone number
        final long greyRawContactId = mGrey.createRawContact(true, GENERIC_NAME);
        mGrey.createPhone(greyRawContactId, PHONE_GREY);
        final long redRawContactId = mRed.createRawContact(false, GENERIC_NAME);
        mRed.createPhone(redRawContactId, PHONE_RED);
        mGrey.setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, greyRawContactId, redRawContactId);

        // Make sure both aggregates were joined
        final long greyAgg = mGrey.getContactForRawContact(greyRawContactId);
        final long redAgg = mRed.getContactForRawContact(redRawContactId);
        assertEquals(greyAgg, redAgg);

        // Exported vCard shouldn't contain restricted phone
        mRed.ensureCallingPackage();
        final Uri aggUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, greyAgg);
        final Cursor cursor = mRed.resolver.query(aggUri,
                new String[] { Contacts.LOOKUP_KEY }, null, null, null);
        assertTrue(cursor.moveToFirst());
        final String lookupKey = cursor.getString(0);
        cursor.close();

        // Read vCard into buffer
        final Uri shareUri = Uri.withAppendedPath(Contacts.CONTENT_VCARD_URI, lookupKey);
        final AssetFileDescriptor file = mRed.resolver.openAssetFileDescriptor(shareUri, "r");
        final InputStream in = file.createInputStream();
        final byte[] buf = new byte[in.available()];
        in.read(buf);
        in.close();
        final String card = new String(buf);
        assertNotSame(0, card.length());

        // Make sure that only unrestricted phones appear
        assertTrue(card.indexOf(PHONE_RED) != -1);
        assertTrue(card.indexOf(PHONE_GREY) == -1);
    }

    public void testContactsLiveFolder() {
        final long greyContact = mGrey.createRawContact(true, GENERIC_NAME);
        final long greyPhone = mGrey.createPhone(greyContact, PHONE_GREY);

        // Protected contact should be omitted from live folder
        mRed.ensureCallingPackage();
        final Uri folderUri = Uri.withAppendedPath(ContactsContract.AUTHORITY_URI,
                "live_folders/contacts_with_phones");
        final Cursor cursor = mRed.resolver.query(folderUri,
                new String[] { LiveFolders._ID }, null, null, null);
        try {
            while (cursor.moveToNext()) {
                final long id = cursor.getLong(0);
                assertFalse(id == greyContact);
            }
        } finally {
            cursor.close();
        }
    }

    public void testRestrictedQueryParam() throws Exception {
        final long greyContact = mGrey.createRawContact(true, GENERIC_NAME);
        final long greyPhone = mGrey.createPhone(greyContact, PHONE_GREY);

        Uri greyUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, greyContact);
        greyUri = Uri.withAppendedPath(greyUri, RawContacts.Entity.CONTENT_DIRECTORY);
        Uri redUri = greyUri.buildUpon().appendQueryParameter(
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY, mRed.packageName).build();

        // When calling normally, we have access to protected
        mGrey.ensureCallingPackage();
        EntityIterator iterator = RawContacts.newEntityIterator(
                mGrey.resolver.query(greyUri, null, null, null, null));
        while (iterator.hasNext()) {
            final Entity entity = iterator.next();
            final long rawContactId = entity.getEntityValues().getAsLong(RawContacts._ID);
            assertTrue(rawContactId == greyContact);
        }
        iterator.close();

        // When calling on behalf of another package, protected is omitted
        mGrey.ensureCallingPackage();
        iterator = RawContacts.newEntityIterator(
                mGrey.resolver.query(redUri, null, null, null, null));
        while (iterator.hasNext()) {
            final Entity entity = iterator.next();
            final long rawContactId = entity.getEntityValues().getAsLong(RawContacts._ID);
            assertTrue(rawContactId != greyContact);
        }
        iterator.close();
    }

    public void testRestrictedEmailLookupRestricted() {
        final long greyContact = mGrey.createRawContact(true, GENERIC_NAME);
        final long greyEmail = mGrey.createEmail(greyContact, EMAIL_GREY);

        // Restricted caller should see protected data
        mGrey.ensureCallingPackage();
        final Uri lookupUri = Uri.withAppendedPath(Email.CONTENT_LOOKUP_URI, EMAIL_GREY);
        final Cursor cursor = mGrey.resolver.query(lookupUri,
                new String[] { Data._ID }, null, null, null);
        try {
            while (cursor.moveToNext()) {
                final long dataId = cursor.getLong(0);
                assertTrue(dataId == greyEmail);
            }
        } finally {
            cursor.close();
        }
    }

    public void testRestrictedEmailLookupGeneric() {
        final long greyContact = mGrey.createRawContact(true, GENERIC_NAME);
        final long greyEmail = mGrey.createEmail(greyContact, EMAIL_GREY);

        // Generic caller should never see protected data
        mRed.ensureCallingPackage();
        final Uri lookupUri = Uri.withAppendedPath(Email.CONTENT_LOOKUP_URI, EMAIL_GREY);
        final Cursor cursor = mRed.resolver.query(lookupUri,
                new String[] { Data._ID }, null, null, null);
        try {
            while (cursor.moveToNext()) {
                final long dataId = cursor.getLong(0);
                assertFalse(dataId == greyEmail);
            }
        } finally {
            cursor.close();
        }
    }

    public void testStatusRestrictedInsertRestrictedQuery() {
        final long rawContactId = mGrey.createRawContactWithStatus(true,
                GENERIC_NAME, EMAIL_GREY, GENERIC_STATUS);
        final long aggId = mGrey.getContactForRawContact(rawContactId);

        // Restricted query can read restricted status
        assertStatus(mGrey, aggId, GENERIC_STATUS);
    }

    public void testStatusRestrictedInsertGenericQuery() {
        final long rawContactId = mGrey.createRawContactWithStatus(true,
                GENERIC_NAME, EMAIL_GREY, GENERIC_STATUS);
        final long aggId = mGrey.getContactForRawContact(rawContactId);

        // Generic query is denied restricted status
        assertStatus(mRed, aggId, null);
    }

    public void testStatusGenericInsertRestrictedQuery() {
        final long rawContactId = mRed.createRawContactWithStatus(false,
                GENERIC_NAME, EMAIL_RED, GENERIC_STATUS);
        final long aggId = mRed.getContactForRawContact(rawContactId);

        // Restricted query can read generic status
        assertStatus(mGrey, aggId, GENERIC_STATUS);
    }

    public void testStatusGenericInsertGenericQuery() {
        final long rawContactId = mRed.createRawContactWithStatus(false,
                GENERIC_NAME, EMAIL_RED, GENERIC_STATUS);
        final long aggId = mRed.getContactForRawContact(rawContactId);

        // Generic query can read generic status
        assertStatus(mRed, aggId, GENERIC_STATUS);
    }

    public void testStrictProjectionMap() {
        try {
            mGrey.provider.query(ContactsContract.Contacts.CONTENT_URI,
                    new String[] { "_id as noname, * FROM contacts--" }, null, null, null);
            fail();
        } catch (Exception e) {
        }
    }
}
