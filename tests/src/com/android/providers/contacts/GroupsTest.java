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

import android.accounts.Account;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.RawContacts;
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

        assertAggregated(contactCharlie, contactCharlieDupe);

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
                assertEquals("Incorrect Grey count", 4, summaryCount);
                assertEquals("Incorrect Grey with phones count", 3, summaryWithPhones);
            } else if (groupId == groupRed) {
                // Red should have 3 aggregates, all with phones.
                assertEquals("Incorrect Red count", 3, summaryCount);
                assertEquals("Incorrect Red with phones count", 3, summaryWithPhones);
            } else if (groupId == groupGreen) {
                // Green should have 1 aggregate, none with phones.
                assertEquals("Incorrect Green count", 1, summaryCount);
                assertEquals("Incorrect Green with phones count", 0, summaryWithPhones);
            } else if (groupId == groupBlue) {
                // Blue should have no contacts.
                assertEquals("Incorrect Blue count", 0, summaryCount);
                assertEquals("Incorrect Blue with phones count", 0, summaryWithPhones);
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

    public void testMarkAsDirtyParameter() {
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI,
                createGroup(mAccount, "gsid1", "title1"));
        clearDirty(uri);
        Uri updateUri = uri.buildUpon()
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "true").build();

        ContentValues values = new ContentValues();
        values.put(Groups.NOTES, "New notes");
        mResolver.update(updateUri, values, null, null);
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
        assertStoredValue(uri, Groups.DELETED, "1");

        Uri permanentDeletionUri = uri.buildUpon()
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "true").build();
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
    }

    public void testGroupDeletion2() {
        long groupId = createGroup(mAccount, "g1", "gt1");
        Uri uri = ContentUris.withAppendedId(Groups.CONTENT_URI, groupId);

        assertEquals(1, getCount(uri, null, null));
        Uri permanentDeletionUri = uri.buildUpon()
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "true").build();
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

    private static final Account sTestAccount = new Account("user@example.com", "com.example");
    private static final String GROUP_ID = "testgroup";

    public boolean queryContactVisible(Uri contactUri) {
        final Cursor cursor = mResolver.query(contactUri, new String[] {
            Contacts.IN_VISIBLE_GROUP
        }, null, null, null);
        assertTrue("Contact not found", cursor.moveToFirst());
        final boolean visible = (cursor.getInt(0) != 0);
        cursor.close();
        return visible;
    }

    public void testDelayStarredUpdate() {
        final ContentValues values = new ContentValues();

        final long groupId = this.createGroup(sTestAccount, GROUP_ID, GROUP_ID, 1);
        final Uri groupUri = ContentUris.withAppendedId(Groups.CONTENT_URI, groupId);
        final Uri groupUriDelay = groupUri.buildUpon().appendQueryParameter(
                Contacts.DELAY_STARRED_UPDATE, "1").build();
        final Uri groupUriForce = Groups.CONTENT_URI.buildUpon().appendQueryParameter(
                Contacts.FORCE_STARRED_UPDATE, "1").build();

        // Create contact with specific membership in visible group
        final long rawContactId = this.createRawContact(sTestAccount);
        final long contactId = this.queryContactId(rawContactId);
        final Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        this.insertGroupMembership(rawContactId, groupId);

        // Update the group visibility normally
        assertTrue(queryContactVisible(contactUri));
        values.put(Groups.GROUP_VISIBLE, 0);
        mResolver.update(groupUri, values, null, null);
        assertFalse(queryContactVisible(contactUri));

        // Update visibility using delayed approach
        values.put(Groups.GROUP_VISIBLE, 1);
        mResolver.update(groupUriDelay, values, null, null);
        assertFalse(queryContactVisible(contactUri));

        // Force update and verify results
        values.clear();
        mResolver.update(groupUriForce, values, null, null);
        assertTrue(queryContactVisible(contactUri));
    }
}
