/*
 * Copyright (C) 2011 The Android Open Source Project
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

import static com.android.providers.contacts.EvenMoreAsserts.assertThrows;
import static com.android.providers.contacts.TestUtils.cv;

import android.database.Cursor;
import android.net.Uri;
import android.net.Uri.Builder;
import android.provider.ContactsContract;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.test.suitebuilder.annotation.MediumTest;

import com.android.providers.contacts.testutil.RawContactUtil;

/**
 * Unit tests for {@link ContactsProvider2}, to make sure the queries don't allow sql injection.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.SqlInjectionDetectionTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class SqlInjectionDetectionTest extends BaseContactsProvider2Test {
    private static final String[] PHONE_ID_PROJECTION = new String[] { Phone._ID };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testQueryValid() {
        assertQueryValid(Phone.CONTENT_URI, PHONE_ID_PROJECTION,
                Phone.NUMBER + "='555-123-4567'", null);

        // The following tables are whitelisted.
        assertQueryValid(Data.CONTENT_URI, null,
                "data._id in default_directory", null);
    }

    public void testPhoneQueryBadProjection() {
        assertQueryThrows(Phone.CONTENT_URI,
                new String[] { "0 UNION SELECT _id FROM view_data--" }, null, null);

        // Invalid column names should be detected too.
        assertQueryThrows(Phone.CONTENT_URI, new String[] { "a" }, null, null);
        assertQueryThrows(Phone.CONTENT_URI, new String[] { " _id" }, null, null);

        // This is still invalid because we only allow exact column names in projections.
        assertQueryThrows(Phone.CONTENT_URI, new String[] { "[_id]" }, null, null);
    }

    public void testPhoneQueryBadSelection() {
        assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION,
                "0=1) UNION SELECT _id FROM view_data--", null);
        assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION, ";delete from contacts", null);
        if (ContactsDatabaseHelper.DISALLOW_SUB_QUERIES) {
            assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION,
                    "_id in data_usage_stat", null);
            assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION,
                    "_id in (select _id from default_directory)", null);
        }
    }

    public void testPhoneQueryBadSortOrder() {
        assertQueryThrows(Phone.CONTENT_URI,
                PHONE_ID_PROJECTION, null, "_id UNION SELECT _id FROM view_data--");
        assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION, null, ";delete from contacts");
        if (ContactsDatabaseHelper.DISALLOW_SUB_QUERIES) {
            assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION, null,
                    "_id in data_usage_stat");
            assertQueryThrows(Phone.CONTENT_URI, PHONE_ID_PROJECTION,
                    null, "exists (select _id from default_directory)");
        }
    }

    public void testPhoneQueryBadLimit() {
        // Non-numeric query parameters are ignored by the provider
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Hot", "Tamale");
        insertPhoneNumber(rawContactId, "555-123-4567");

        Builder builder = Contacts.CONTENT_FILTER_URI.buildUpon();
        builder.appendQueryParameter(ContactsContract.LIMIT_PARAM_KEY,
                "0 UNION SELECT -50");
        assertQueryValid(Phone.CONTENT_URI,
                PHONE_ID_PROJECTION, null, null);

        final Cursor c = mResolver.query(Phone.CONTENT_URI, PHONE_ID_PROJECTION, null, null, null);
        // the current implementation ignores every non-numeric limit. so we should see the
        // contact as the only result
        assertEquals(1, c.getCount());
        c.moveToFirst();
        assertNotSame(-50, c.getLong(0));
        c.close();
    }

    private void assertQueryValid(final Uri uri, final String[] projection,
            final String selection, final String sortOrder) {
        final Cursor c = mResolver.query(uri, projection, selection, null, sortOrder);
        c.close();
    }

    private <T extends Exception> void assertQueryThrows(final Uri uri,
            final String[] projection, final String selection, final String sortOrder) {
        assertThrows(IllegalArgumentException.class, () -> {
                final Cursor c = mResolver.query(uri, projection, selection, null, sortOrder);
                c.close();
        });
    }

    public void testBadDelete() {
        assertThrows(IllegalArgumentException.class, () -> {
            mResolver.delete(Contacts.CONTENT_URI, ";delete from contacts;--", null);
        });
        if (ContactsDatabaseHelper.DISALLOW_SUB_QUERIES) {
            assertThrows(IllegalArgumentException.class, () -> {
                mResolver.delete(Contacts.CONTENT_URI, "_id in data_usage_stat", null);
            });
        }
    }

    public void testBadUpdate() {
        assertThrows(IllegalArgumentException.class, () -> {
            mResolver.update(Data.CONTENT_URI, cv(), ";delete from contacts;--", null);
        });
        if (ContactsDatabaseHelper.DISALLOW_SUB_QUERIES) {
            assertThrows(IllegalArgumentException.class, () -> {
                mResolver.update(Data.CONTENT_URI, cv(), "_id in data_usage_stat", null);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                mResolver.update(Data.CONTENT_URI, cv("_id/**/", 1), null, null);
            });

            mResolver.update(Data.CONTENT_URI, cv("[data1]", 1), null, null);
        }
    }

    public void testBadInsert() {
        if (ContactsDatabaseHelper.DISALLOW_SUB_QUERIES) {
            assertThrows(IllegalArgumentException.class, () -> {
                mResolver.insert(Data.CONTENT_URI, cv("_id/**/", 1));
            });
        }
    }
}
