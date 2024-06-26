/*
 * Copyright (C) 2013 The Android Open Source Project
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

package com.android.providers.contacts.database;

import androidx.test.filters.SmallTest;

import junit.framework.TestCase;

/**
 * Unit tests for MoreDatabaseutil.
 */
@SmallTest
public class MoreDatabaseUtilTest extends TestCase {

    public void testBuildBindArgString() {
        assertEquals("?", MoreDatabaseUtils.buildBindArgString(1));
        assertEquals("?,?", MoreDatabaseUtils.buildBindArgString(2));
        assertEquals("?,?,?", MoreDatabaseUtils.buildBindArgString(3));
        assertEquals("?,?,?,?", MoreDatabaseUtils.buildBindArgString(4));
    }

    public void testBuildIndex() {
        String expected = "create index testtable_testfield_index on testtable(testfield)";
        String actual = MoreDatabaseUtils.buildCreateIndexSql("testtable", "testfield")
                .toLowerCase();
        assertEquals(expected, actual);

        expected = "create index test_table_test_field_index on test_table(test_field)";
        actual = MoreDatabaseUtils.buildCreateIndexSql("test_table", "test_field").toLowerCase();
        assertEquals(expected, actual);
    }

    public void testDropIndex() {
        String expected = "drop index if exists testtable_testfield_index";
        String actual = MoreDatabaseUtils.buildDropIndexSql("testtable", "testfield").toLowerCase();
        assertEquals(expected, actual);
    }

    public void testBuildIndexName() {
        assertEquals("testtable_testfield_index",
                MoreDatabaseUtils.buildIndexName("testtable", "testfield"));
    }

    public void testSqlEscapeNullableString() {
        assertEquals("NULL", MoreDatabaseUtils.sqlEscapeNullableString(null));
        assertEquals("'foo'", MoreDatabaseUtils.sqlEscapeNullableString("foo"));
    }

    public void testAppendEscapedSQLStringOrLiteralNull() {
        StringBuilder sb1 = new StringBuilder();
        MoreDatabaseUtils.appendEscapedSQLStringOrLiteralNull(sb1, null);
        assertEquals("NULL", sb1.toString());

        StringBuilder sb2 = new StringBuilder();
        MoreDatabaseUtils.appendEscapedSQLStringOrLiteralNull(sb2, "foo");
        assertEquals("'foo'", sb2.toString());
    }
}
