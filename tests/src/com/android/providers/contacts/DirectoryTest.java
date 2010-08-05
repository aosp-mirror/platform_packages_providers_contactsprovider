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
import android.provider.ContactsContract;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Directory;
import android.test.suitebuilder.annotation.LargeTest;


/**
 * Unit tests for {@link ContactsProvider2}, directory functionality.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.DirectoryTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class DirectoryTest extends BaseContactsProvider2Test {

    public void testDefaultDirectory() {
        ContentValues values = new ContentValues();
        Uri defaultDirectoryUri =
            ContentUris.withAppendedId(Directory.CONTENT_URI, Directory.DEFAULT);

        values.put(Directory.PACKAGE_NAME, "contactsTestPackage");
        values.put(Directory.DIRECTORY_AUTHORITY, ContactsContract.AUTHORITY);
        values.put(Directory.TYPE_RESOURCE_ID, R.string.default_directory);
        values.put(Directory.EXPORT_SUPPORT, Directory.EXPORT_SUPPORT_NONE);
        values.putNull(Directory.ACCOUNT_NAME);
        values.putNull(Directory.ACCOUNT_TYPE);
        values.putNull(Directory.DISPLAY_NAME);

        assertStoredValues(defaultDirectoryUri, values);
    }

    public void testInvisibleLocalDirectory() {
        ContentValues values = new ContentValues();
        Uri defaultDirectoryUri =
            ContentUris.withAppendedId(Directory.CONTENT_URI, Directory.LOCAL_INVISIBLE);

        values.put(Directory.PACKAGE_NAME, "contactsTestPackage");
        values.put(Directory.DIRECTORY_AUTHORITY, ContactsContract.AUTHORITY);
        values.put(Directory.TYPE_RESOURCE_ID, R.string.local_invisible_directory);
        values.put(Directory.EXPORT_SUPPORT, Directory.EXPORT_SUPPORT_NONE);
        values.putNull(Directory.ACCOUNT_NAME);
        values.putNull(Directory.ACCOUNT_TYPE);
        values.putNull(Directory.DISPLAY_NAME);

        assertStoredValues(defaultDirectoryUri, values);
    }

    public void testForwardingToLocalContacts() {
        long contactId = queryContactId(createRawContactWithName());

        Uri contentUri = Contacts.CONTENT_URI.buildUpon().appendQueryParameter(
                ContactsContract.DIRECTORY_PARAM_KEY, String.valueOf(Directory.DEFAULT)).build();

        Cursor cursor = mResolver.query(contentUri,
                new String[]{Contacts._ID, Contacts.DISPLAY_NAME}, null, null, null);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        cursor.moveToFirst();
        assertEquals(contactId, cursor.getLong(0));
        assertEquals("John Doe", cursor.getString(1));
        cursor.close();
    }

    public void testForwardingToLocalInvisibleContacts() {
        long visibleContactId = queryContactId(createRawContactWithName("Bob", "Parr"));

        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, visibleContactId),
                Contacts.IN_VISIBLE_GROUP, "1");

        long hiddenContactId = queryContactId(createRawContactWithName("Helen", "Parr",
                new Account("accountName", "accountType")));

        assertStoredValue(ContentUris.withAppendedId(Contacts.CONTENT_URI, hiddenContactId),
                Contacts.IN_VISIBLE_GROUP, "0");

        Uri contentUri = Contacts.CONTENT_URI.buildUpon().appendQueryParameter(
                ContactsContract.DIRECTORY_PARAM_KEY, String.valueOf(Directory.LOCAL_INVISIBLE))
                .build();

        Cursor cursor = mResolver.query(contentUri,
                new String[]{Contacts._ID, Contacts.DISPLAY_NAME}, null, null, null);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        cursor.moveToFirst();
        assertEquals(hiddenContactId, cursor.getLong(0));
        assertEquals("Helen Parr", cursor.getString(1));
        cursor.close();

        Uri filterUri = Contacts.CONTENT_FILTER_URI.buildUpon().appendEncodedPath("parr")
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY,
                        String.valueOf(Directory.LOCAL_INVISIBLE)).build();

        cursor = mResolver.query(filterUri,
                new String[]{Contacts._ID, Contacts.DISPLAY_NAME}, null, null, null);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        cursor.moveToFirst();
        assertEquals(hiddenContactId, cursor.getLong(0));
        assertEquals("Helen Parr", cursor.getString(1));
        cursor.close();
    }
}

