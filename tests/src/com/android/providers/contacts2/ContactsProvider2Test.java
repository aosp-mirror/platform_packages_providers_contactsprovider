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

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.test.ProviderTestCase2;
import android.test.mock.MockContentResolver;
import android.test.suitebuilder.annotation.LargeTest;

import com.android.providers.contacts2.ContactsContract.Aggregates;

/**
 * Unit tests for ContactsProvider2
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts2.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactsProvider2Test extends ProviderTestCase2<ContactsProvider2> {

    private Context mContext;
    private MockContentResolver mResolver;

    public ContactsProvider2Test() {
        super(ContactsProvider2.class, ContactsContract.AUTHORITY);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mContext = getMockContext();
        mResolver = getMockContentResolver();
    }

    public void testInsertAggregate() {
        ContentValues values = new ContentValues();
        values.put(Aggregates.DISPLAY_NAME, "Bob Smith");
        Uri aggregatesUri = mResolver.insert(Aggregates.CONTENT_URI, values);

        // Parse the URI and confirm that it contains an ID
        assertTrue(ContentUris.parseId(aggregatesUri) > 0);
    }

    // TODO: move relevant tests from GoogleContactsProvider and add more tests
}
