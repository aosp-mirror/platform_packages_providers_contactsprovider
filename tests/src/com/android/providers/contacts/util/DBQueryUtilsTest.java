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
 * limitations under the License
 */

package com.android.providers.contacts.util;

import static com.android.providers.contacts.util.DbQueryUtils.checkForSupportedColumns;
import static com.android.providers.contacts.util.DbQueryUtils.concatenateClauses;

import android.content.ContentValues;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;

import com.android.providers.contacts.EvenMoreAsserts;
import com.android.providers.contacts.ProjectionMap;
import com.android.providers.contacts.util.DbQueryUtils;

/**
 * Unit tests for the {@link DbQueryUtils} class.
 * Run the test like this:
 * <code>
 * runtest -c com.android.providers.contacts.util.DBQueryUtilsTest contactsprov
 * </code>
 */
@SmallTest
public class DBQueryUtilsTest extends AndroidTestCase {
    public void testGetEqualityClause() {
        assertEquals("(foo = 'bar')", DbQueryUtils.getEqualityClause("foo", "bar"));
    }

    public void testGetInEqualityClause() {
        assertEquals("(foo != 'bar')", DbQueryUtils.getInequalityClause("foo", "bar"));
    }

    public void testConcatenateClauses() {
        assertEquals("(first)", concatenateClauses("first"));
        assertEquals("(first) AND (second)", concatenateClauses("first", "second"));
        assertEquals("(second)", concatenateClauses("second", null));
        assertEquals("(second)", concatenateClauses(null, "second"));
        assertEquals("(second)", concatenateClauses(null, "second", null));
        assertEquals("(a) AND (b) AND (c)", concatenateClauses(null, "a", "b", null, "c"));
        assertEquals("(WHERE \"a\" = \"b\")", concatenateClauses(null, "WHERE \"a\" = \"b\""));
    }

    public void testCheckForSupportedColumns() {
        final ProjectionMap projectionMap = new ProjectionMap.Builder()
                .add("A").add("B").add("C").build();
        final ContentValues values = new ContentValues();
        values.put("A", "?");
        values.put("C", "?");
        // No exception expected.
        checkForSupportedColumns(projectionMap, values);
        // Expect exception for invalid column.
        EvenMoreAsserts.assertThrows(IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
                values.put("D", "?");
                checkForSupportedColumns(projectionMap, values);
            }
        });
    }
}
