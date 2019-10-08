/*
 * Copyright (C) 2016 The Android Open Source Project
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
 * limitations under the License
 */

package com.android.providers.contacts.sqlite;


import com.android.providers.contacts.ContactsDatabaseHelper;
import com.android.providers.contacts.FixedAndroidTestCase;
import com.android.providers.contacts.TestUtils;

import java.util.List;

public class DatabaseAnalyzerTest extends FixedAndroidTestCase {
    public void testFindTableViewsAllowingColumns() {
        final ContactsDatabaseHelper dbh =
                ContactsDatabaseHelper.getNewInstanceForTest(getContext(),
                        TestUtils.getContactsDatabaseFilename(getContext()));
        try {
            final List<String> list =  DatabaseAnalyzer.findTableViewsAllowingColumns(
                    dbh.getReadableDatabase());

            assertTrue(list.contains("contacts"));
            assertTrue(list.contains("raw_contacts"));
            assertTrue(list.contains("view_contacts"));
            assertTrue(list.contains("view_raw_contacts"));
            assertTrue(list.contains("view_data"));

            assertFalse(list.contains("data"));
            assertFalse(list.contains("_id"));

        } finally {
            dbh.close();
        }
    }
}