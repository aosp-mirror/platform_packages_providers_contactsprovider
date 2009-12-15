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

import com.android.internal.telephony.CallerInfo;
import com.android.internal.telephony.Connection;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.provider.CallLog;
import android.provider.ContactsContract;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.test.suitebuilder.annotation.MediumTest;

/**
 * Unit tests for {@link CallLogProvider}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.CallLogProviderTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class CallLogProviderTest extends BaseContactsProvider2Test {

    @Override
    protected Class<? extends ContentProvider> getProviderClass() {
       return SynchronousContactsProvider2.class;
    }

    @Override
    protected String getAuthority() {
        return ContactsContract.AUTHORITY;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        addProvider(TestCallLogProvider.class, CallLog.AUTHORITY);
    }

    public void testInsert() {
        ContentValues values = new ContentValues();
        putCallValues(values);
        Uri uri = mResolver.insert(Calls.CONTENT_URI, values);
        assertStoredValues(uri, values);
        assertSelection(uri, values, Calls._ID, ContentUris.parseId(uri));
    }

    public void testUpdate() {
        ContentValues values = new ContentValues();
        putCallValues(values);
        Uri uri = mResolver.insert(Calls.CONTENT_URI, values);

        values.clear();
        values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
        values.put(Calls.NUMBER, "1-800-263-7643");
        values.put(Calls.DATE, 2000);
        values.put(Calls.DURATION, 40);
        values.put(Calls.CACHED_NAME, "1-800-GOOG-411");
        values.put(Calls.CACHED_NUMBER_TYPE, Phone.TYPE_CUSTOM);
        values.put(Calls.CACHED_NUMBER_LABEL, "Directory");

        int count = mResolver.update(uri, values, null, null);
        assertEquals(1, count);
        assertStoredValues(uri, values);
    }

    public void testDelete() {
        ContentValues values = new ContentValues();
        putCallValues(values);
        Uri uri = mResolver.insert(Calls.CONTENT_URI, values);
        try {
            mResolver.delete(uri, null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
            // Expected
        }

        int count = mResolver.delete(Calls.CONTENT_URI, Calls._ID + "="
                + ContentUris.parseId(uri), null);
        assertEquals(1, count);
        assertEquals(0, getCount(uri, null, null));
    }

    public void testCallLogFilter() {
        ContentValues values = new ContentValues();
        putCallValues(values);
        mResolver.insert(Calls.CONTENT_URI, values);

        Uri filterUri = Uri.withAppendedPath(Calls.CONTENT_FILTER_URI, "1-800-4664-411");
        Cursor c = mResolver.query(filterUri, null, null, null, null);
        assertEquals(1, c.getCount());
        c.moveToFirst();
        assertCursorValues(c, values);
        c.close();

        filterUri = Uri.withAppendedPath(Calls.CONTENT_FILTER_URI, "1-888-4664-411");
        c = mResolver.query(filterUri, null, null, null, null);
        assertEquals(0, c.getCount());
        c.close();
    }

    public void testAddCall() {
        CallerInfo ci = new CallerInfo();
        ci.name = "1-800-GOOG-411";
        ci.numberType = Phone.TYPE_CUSTOM;
        ci.numberLabel = "Directory";
        Uri uri = Calls.addCall(ci, getMockContext(), "1-800-263-7643",
                Connection.PRESENTATION_ALLOWED, Calls.OUTGOING_TYPE, 2000, 40);

        ContentValues values = new ContentValues();
        values.put(Calls.TYPE, Calls.OUTGOING_TYPE);
        values.put(Calls.NUMBER, "1-800-263-7643");
        values.put(Calls.DATE, 2000);
        values.put(Calls.DURATION, 40);
        values.put(Calls.CACHED_NAME, "1-800-GOOG-411");
        values.put(Calls.CACHED_NUMBER_TYPE, Phone.TYPE_CUSTOM);
        values.put(Calls.CACHED_NUMBER_LABEL, "Directory");
        assertStoredValues(uri, values);
    }

    private void putCallValues(ContentValues values) {
        values.put(Calls.TYPE, Calls.INCOMING_TYPE);
        values.put(Calls.NUMBER, "1-800-4664-411");
        values.put(Calls.DATE, 1000);
        values.put(Calls.DURATION, 30);
        values.put(Calls.NEW, 1);
    }

    public static class TestCallLogProvider extends CallLogProvider {
        private static ContactsDatabaseHelper mDbHelper;

        @Override
        protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
            if (mDbHelper == null) {
                mDbHelper = new ContactsDatabaseHelper(context);
            }
            return mDbHelper;
        }
    }
}

