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
import android.app.SearchManager;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Intents;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.IOException;

/**
 * Unit tests for {@link GlobalSearchSupport}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.GlobalSearchSupportTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class GlobalSearchSupportTest extends BaseContactsProvider2Test {
    public void testSearchSuggestionsNotInVisibleGroup() throws Exception {
        Account account = new Account("actname", "acttype");
        long rawContactId = createRawContact(account);
        insertStructuredName(rawContactId, "Deer", "Dough");

        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath("D").build();

        // If the contact is not in the "my contacts" group, nothing should be found
        Cursor c = mResolver.query(searchUri, null, null, null, null);
        assertEquals(0, c.getCount());
        c.close();
    }

    public void testSearchSuggestionsByName() throws Exception {
        long groupId = createGroup(mAccount, "gsid1", "title1");

        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                true,  // photo
                false, // company
                false, // title
                false, // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", null);

        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                true,  // photo
                true,  // company
                false, // title
                false, // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", "Google");

        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                true,  // photo
                false, // company
                false, // title
                true,  // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", "1-800-4664-411");

        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                true,  // photo
                false, // company
                false, // title
                false, // phone
                true,  // email
                "D",   // query
                true,  // expect icon URI
                String.valueOf(StatusUpdates.getPresenceIconResourceId(StatusUpdates.OFFLINE)),
                "Deer Dough", "foo@acme.com");

        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                false, // photo
                true,  // company
                false, // title
                false, // phone
                false, // email
                "D",   // query
                false, // expect icon URI
                null, "Deer Dough", "Google");

        // Nickname is searchale
        assertSearchSuggestion(groupId,
                true,  // name
                true,  // nickname
                false, // photo
                true,  // company
                false, // title
                false, // phone
                false, // email
                "L",   // query
                false, // expect icon URI
                null, "Deer Dough", "Google");

        // Company is searchable
        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                false, // photo
                true,  // company
                false, // title
                false, // phone
                false, // email
                "G",   // query
                false, // expect icon URI
                null, "Deer Dough", "Google");

        // Title is searchable
        assertSearchSuggestion(groupId,
                true,  // name
                false, // nickname
                false, // photo
                true,  // company
                true,  // title
                false, // phone
                false, // email
                "S",   // query
                false, // expect icon URI
                null, "Deer Dough", "Google");
    }

    private void assertSearchSuggestion(long groupId, boolean name, boolean nickname, boolean photo,
            boolean company, boolean title, boolean phone, boolean email, String query,
            boolean expectIcon1Uri, String expectedIcon2, String expectedText1,
            String expectedText2) throws IOException {
        ContentValues values = new ContentValues();

        long rawContactId = createRawContact();
        insertGroupMembership(rawContactId, groupId);

        if (name) {
            insertStructuredName(rawContactId, "Deer", "Dough");
        }

        if (nickname) {
            insertNickname(rawContactId, "Little Fawn");
        }

        final Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        if (photo) {
            values.clear();
            byte[] photoData = loadTestPhoto();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
            values.put(Photo.PHOTO, photoData);
            mResolver.insert(Data.CONTENT_URI, values);
        }

        if (company || title) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE);
            values.put(Organization.TYPE, Organization.TYPE_WORK);
            if (company) {
                values.put(Organization.COMPANY, "Google");
            }
            if (title) {
                values.put(Organization.TITLE, "Software Engineer");
            }
            mResolver.insert(Data.CONTENT_URI, values);
        }

        if (email) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
            values.put(Email.TYPE, Email.TYPE_WORK);
            values.put(Email.DATA, "foo@acme.com");
            mResolver.insert(Data.CONTENT_URI, values);

            int protocol = Im.PROTOCOL_GOOGLE_TALK;

            values.clear();
            values.put(StatusUpdates.PROTOCOL, protocol);
            values.put(StatusUpdates.IM_HANDLE, "foo@acme.com");
            values.put(StatusUpdates.IM_ACCOUNT, "foo");
            values.put(StatusUpdates.PRESENCE_STATUS, StatusUpdates.OFFLINE);
            values.put(StatusUpdates.PRESENCE_CUSTOM_STATUS, "Coding for Android");
            mResolver.insert(StatusUpdates.CONTENT_URI, values);
        }

        if (phone) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
            values.put(Data.IS_PRIMARY, 1);
            values.put(Phone.TYPE, Phone.TYPE_HOME);
            values.put(Phone.NUMBER, "1-800-4664-411");
            mResolver.insert(Data.CONTENT_URI, values);
        }

        long contactId = queryContactId(rawContactId);
        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath(query).build();

        Cursor c = mResolver.query(searchUri, null, null, null, null);
        assertEquals(1, c.getCount());
        c.moveToFirst();
        values.clear();

        // SearchManager does not declare a constant for _id
        values.put("_id", contactId);
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, expectedText1);
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, expectedText2);

        String icon1 = c.getString(c.getColumnIndex(SearchManager.SUGGEST_COLUMN_ICON_1));
        if (expectIcon1Uri) {
            assertTrue(icon1.startsWith("content:"));
        } else {
            assertEquals(String.valueOf(com.android.internal.R.drawable.ic_contact_picture), icon1);
        }

        values.put(SearchManager.SUGGEST_COLUMN_ICON_2, expectedIcon2);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID, contactId);
        values.put(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID, contactId);
        assertCursorValues(c, values);
        c.close();

        // See if the same result is returned by a shortcut refresh
        Uri shortcutsUri = ContactsContract.AUTHORITY_URI.buildUpon()
                .appendPath(SearchManager.SUGGEST_URI_PATH_SHORTCUT).build();
        Uri refreshUri = ContentUris.withAppendedId(shortcutsUri, contactId);

        String[] projection = new String[]{
                SearchManager.SUGGEST_COLUMN_ICON_1,
                SearchManager.SUGGEST_COLUMN_ICON_2,
                SearchManager.SUGGEST_COLUMN_TEXT_1,
                SearchManager.SUGGEST_COLUMN_TEXT_2,
                SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID,
                SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
                "_id",
        };

        c = mResolver.query(refreshUri, projection, null, null, null);
        try {
            assertEquals("Record count", 1, c.getCount());
            c.moveToFirst();
            assertCursorValues(c, values);
        } finally {
            c.close();
        }

        // Cleanup
        mResolver.delete(rawContactUri, null, null);
    }

    public void testSearchSuggestionsByPhoneNumber() throws Exception {
        ContentValues values = new ContentValues();

        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath("12345").build();

        Cursor c = mResolver.query(searchUri, null, null, null, null);
        DatabaseUtils.dumpCursor(c);
        assertEquals(2, c.getCount());
        c.moveToFirst();

        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, "Dial number");
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, "using 12345");
        values.put(SearchManager.SUGGEST_COLUMN_ICON_1,
                String.valueOf(com.android.internal.R.drawable.call_contact));
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
                Intents.SEARCH_SUGGESTION_DIAL_NUMBER_CLICKED);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA, "tel:12345");
        values.putNull(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID);
        assertCursorValues(c, values);

        c.moveToNext();
        values.clear();
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, "Create contact");
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, "using 12345");
        values.put(SearchManager.SUGGEST_COLUMN_ICON_1,
                String.valueOf(com.android.internal.R.drawable.create_contact));
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
                Intents.SEARCH_SUGGESTION_CREATE_CONTACT_CLICKED);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA, "tel:12345");
        values.put(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
                SearchManager.SUGGEST_NEVER_MAKE_SHORTCUT);
        assertCursorValues(c, values);
        c.close();
    }
}

