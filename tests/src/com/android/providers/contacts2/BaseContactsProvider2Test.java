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

import static com.android.providers.contacts2.ContactsActor.PACKAGE_GREY;

import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.test.AndroidTestCase;
import android.test.mock.MockContentResolver;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * A common superclass for {@link ContactsProvider2}-related tests.
 */
@LargeTest
public abstract class BaseContactsProvider2Test extends AndroidTestCase {

    protected static final String PACKAGE = "ContactsProvider2Test";

    private ContactsActor mActor;
    protected MockContentResolver mResolver;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mActor = new ContactsActor(getContext(), PACKAGE_GREY);
        mResolver = mActor.resolver;
    }

    protected long createContact() {
        ContentValues values = new ContentValues();
        values.put(Contacts.PACKAGE, mActor.packageName);
        Uri contactUri = mResolver.insert(Contacts.CONTENT_URI, values);
        return ContentUris.parseId(contactUri);
    }

    protected Uri insertStructuredName(long contactId, String givenName, String familyName) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);
        StringBuilder sb = new StringBuilder();
        if (givenName != null) {
            sb.append(givenName);
        }
        if (givenName != null && familyName != null) {
            sb.append(" ");
        }
        if (familyName != null) {
            sb.append(familyName);
        }
        values.put(StructuredName.DISPLAY_NAME, sb.toString());
        values.put(StructuredName.GIVEN_NAME, givenName);
        values.put(StructuredName.FAMILY_NAME, familyName);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected void setAggregationException(int type, long aggregateId, long contactId) {
        ContentValues values = new ContentValues();
        values.put(AggregationExceptions.AGGREGATE_ID, aggregateId);
        values.put(AggregationExceptions.CONTACT_ID, contactId);
        values.put(AggregationExceptions.TYPE, type);
        mResolver.update(AggregationExceptions.CONTENT_URI, values, null, null);
    }

    protected Cursor queryContact(long contactId) {
        return mResolver.query(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId), null,
                null, null, null);
    }

    protected Cursor queryAggregate(long aggregateId) {
        return mResolver.query(ContentUris.withAppendedId(Aggregates.CONTENT_URI, aggregateId),
                null, null, null, null);
    }

    protected long queryAggregateId(long contactId) {
        Cursor c = queryContact(contactId);
        assertTrue(c.moveToFirst());
        long aggregateId = c.getLong(c.getColumnIndex(Contacts.AGGREGATE_ID));
        c.close();
        return aggregateId;
    }

    protected String queryDisplayName(long aggregateId) {
        Cursor c = queryAggregate(aggregateId);
        assertTrue(c.moveToFirst());
        String displayName = c.getString(c.getColumnIndex(Aggregates.DISPLAY_NAME));
        c.close();
        return displayName;
    }

    protected void assertAggregated(long contactId1, long contactId2, String expectedDisplayName) {
        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 == aggregateId2);

        String displayName = queryDisplayName(aggregateId1);
        assertEquals(expectedDisplayName, displayName);
    }

    protected void assertNotAggregated(long contactId1, long contactId2) {
        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 != aggregateId2);
    }
}
