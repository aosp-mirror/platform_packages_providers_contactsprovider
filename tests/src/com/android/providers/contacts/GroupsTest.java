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
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link Groups} and {@link GroupMembership}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class GroupsTest extends BaseContactsProvider2Test {

    private static final String GROUP_GREY = "Grey";
    private static final String GROUP_RED = "Red";
    private static final String GROUP_GREEN = "Green";
    private static final String GROUP_BLUE = "Blue";

    private static final String PERSON_ALPHA = "Alpha";
    private static final String PERSON_BRAVO = "Bravo";
    private static final String PERSON_CHARLIE = "Charlie";
    private static final String PERSON_DELTA = "Delta";

    private static final String PHONE_ALPHA = "555-1111";
    private static final String PHONE_BRAVO_1 = "555-2222";
    private static final String PHONE_BRAVO_2 = "555-3333";
    private static final String PHONE_CHARLIE_1 = "555-4444";
    private static final String PHONE_CHARLIE_2 = "555-5555";

    public void testGroupSummary() {

        // Clear any existing data before starting
        // TODO make the provider wipe data automatically
        ((SynchronousContactsProvider2)mActor.provider).wipeData();

        // Create a handful of groups
        long groupGrey = mActor.createGroup(GROUP_GREY);
        long groupRed = mActor.createGroup(GROUP_RED);
        long groupGreen = mActor.createGroup(GROUP_GREEN);
        long groupBlue = mActor.createGroup(GROUP_BLUE);

        // Create a handful of contacts
        long contactAlpha = mActor.createContact(false, PERSON_ALPHA);
        long contactBravo = mActor.createContact(false, PERSON_BRAVO);
        long contactCharlie = mActor.createContact(false, PERSON_CHARLIE);
        long contactCharlieDupe = mActor.createContact(false, PERSON_CHARLIE);
        long contactDelta = mActor.createContact(false, PERSON_DELTA);

        // Make sure that Charlie was aggregated
        {
            long aggCharlie = mActor.getContactForRawContact(contactCharlie);
            long aggCharlieDupe = mActor.getContactForRawContact(contactCharlieDupe);
            assertTrue("Didn't aggregate two contacts with identical names",
                    (aggCharlie == aggCharlieDupe));
        }

        // Add phone numbers to specific contacts
        mActor.createPhone(contactAlpha, PHONE_ALPHA);
        mActor.createPhone(contactBravo, PHONE_BRAVO_1);
        mActor.createPhone(contactBravo, PHONE_BRAVO_2);
        mActor.createPhone(contactCharlie, PHONE_CHARLIE_1);
        mActor.createPhone(contactCharlieDupe, PHONE_CHARLIE_2);

        // Add contacts to various mixture of groups. Grey will have all
        // contacts, Red only with phone numbers, Green with no phones, and Blue
        // with no contacts at all.
        mActor.createGroupMembership(contactAlpha, groupGrey);
        mActor.createGroupMembership(contactBravo, groupGrey);
        mActor.createGroupMembership(contactCharlie, groupGrey);
        mActor.createGroupMembership(contactDelta, groupGrey);

        mActor.createGroupMembership(contactAlpha, groupRed);
        mActor.createGroupMembership(contactBravo, groupRed);
        mActor.createGroupMembership(contactCharlie, groupRed);

        mActor.createGroupMembership(contactDelta, groupGreen);

        // Walk across groups summary cursor and verify returned counts.
        final Cursor cursor = mActor.resolver.query(Groups.CONTENT_SUMMARY_URI,
                Projections.PROJ_SUMMARY, null, null, null);

        // Require that each group has a summary row
        assertTrue("Didn't return summary for all groups", (cursor.getCount() == 4));

        while (cursor.moveToNext()) {
            final long groupId = cursor.getLong(Projections.COL_ID);
            final int summaryCount = cursor.getInt(Projections.COL_SUMMARY_COUNT);
            final int summaryWithPhones = cursor.getInt(Projections.COL_SUMMARY_WITH_PHONES);

            if (groupId == groupGrey) {
                // Grey should have four aggregates, three with phones.
                assertTrue("Incorrect Grey count", (summaryCount == 4));
                assertTrue("Incorrect Grey with phones count", (summaryWithPhones == 3));
            } else if (groupId == groupRed) {
                // Red should have 3 aggregates, all with phones.
                assertTrue("Incorrect Red count", (summaryCount == 3));
                assertTrue("Incorrect Red with phones count", (summaryWithPhones == 3));
            } else if (groupId == groupGreen) {
                // Green should have 1 aggregate, none with phones.
                assertTrue("Incorrect Green count", (summaryCount == 1));
                assertTrue("Incorrect Green with phones count", (summaryWithPhones == 0));
            } else if (groupId == groupBlue) {
                // Blue should have no contacts.
                assertTrue("Incorrect Blue count", (summaryCount == 0));
                assertTrue("Incorrect Blue with phones count", (summaryWithPhones == 0));
            } else {
                fail("Unrecognized group in summary cursor");
            }
        }

    }

    public void testGroupDirtySetOnChange() {
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI,
                createGroup(mAccount, "gsid1", "title1"));
        assertDirty(uri, true);
        clearDirty(uri);
        assertDirty(uri, false);
    }

    public void testGroupDirtyClearedWhenSetExplicitly() {
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI,
                createGroup(mAccount, "gsid1", "title1"));
        assertDirty(uri, true);

        ContentValues values = new ContentValues();
        values.put(Groups.DIRTY, 0);
        values.put(Groups.NOTES, "other notes");
        assertEquals(1, mResolver.update(uri, values, null, null));

        assertDirty(uri, false);
    }

    public void testGroupDeletion1() {
        long groupId = createGroup(mAccount, "g1", "gt1");
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI, groupId);

        assertEquals(1, getCount(uri, null, null));
        mResolver.delete(uri, null, null);
        assertEquals(1, getCount(uri, null, null));
        assertStoredValues(uri, Groups.DELETED, "1");

        Uri permanentDeletionUri =
                uri.buildUpon().appendQueryParameter(Groups.DELETE_PERMANENTLY, "true").build();
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
    }

    public void testGroupDeletion2() {
        long groupId = createGroup(mAccount, "g1", "gt1");
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI, groupId);

        assertEquals(1, getCount(uri, null, null));
        Uri permanentDeletionUri =
                uri.buildUpon().appendQueryParameter(Groups.DELETE_PERMANENTLY, "true").build();
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
    }

    public void testGroupVersionUpdates() {
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI,
                createGroup(mAccount, "gsid1", "title1"));
        long version = getVersion(uri);
        ContentValues values = new ContentValues();
        values.put(Groups.TITLE, "title2");
        mResolver.update(uri, values, null, null);
        assertEquals(version + 1, getVersion(uri));
    }

    private interface Projections {
        public static final String[] PROJ_SUMMARY = new String[] {
            Groups._ID,
            Groups.SUMMARY_COUNT,
            Groups.SUMMARY_WITH_PHONES,
        };

        public static final int COL_ID = 0;
        public static final int COL_SUMMARY_COUNT = 1;
        public static final int COL_SUMMARY_WITH_PHONES = 2;
    }

}
