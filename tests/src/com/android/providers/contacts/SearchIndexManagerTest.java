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

import android.content.ContentValues;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.test.suitebuilder.annotation.MediumTest;

/**
 * Unit tests for {@link SearchIndexManager}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.SearchIndexManagerTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
public class SearchIndexManagerTest extends BaseContactsProvider2Test {

    public void testSearchIndexForOrganization() {
        long rawContactId = createRawContact();
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();
        values.put(Organization.COMPANY, "Acme Inc.");
        values.put(Organization.TITLE, "Director");
        values.put(Organization.DEPARTMENT, "Phones and tablets");
        values.put(Organization.JOB_DESCRIPTION, "full text search");
        values.put(Organization.SYMBOL, "ACME");
        values.put(Organization.PHONETIC_NAME, "ack-me");
        values.put(Organization.OFFICE_LOCATION, "virtual");
        insertOrganization(rawContactId, values);

        assertSearchIndex(contactId,
                "Director, Acme Inc. (ack-me) (ACME)/Phones and tablets/virtual/full text search",
                null);
    }

    private void assertSearchIndex(long contactId, String expectedContent, String expectedTokens) {
        ContactsDatabaseHelper dbHelper = (ContactsDatabaseHelper) getContactsProvider()
                .getDatabaseHelper();
        assertEquals(expectedContent, dbHelper.querySearchIndexContent(contactId));
        assertEquals(expectedTokens, dbHelper.querySearchIndexTokens(contactId));
    }
}

