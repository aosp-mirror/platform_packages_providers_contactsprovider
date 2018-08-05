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

import android.test.MoreAsserts;

import com.android.providers.contacts.FixedAndroidTestCase;
import com.android.providers.contacts.sqlite.SqlChecker.InvalidSqlException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlCheckerTest extends FixedAndroidTestCase {
    private ArrayList<String> getTokens(String sql) {
        final ArrayList<String> tokens = new ArrayList<>();

        SqlChecker.findTokens(sql, SqlChecker.OPTION_NONE,  token -> tokens.add(token));

        return tokens;
    }

    private void checkTokens(String sql, String spaceSeparatedExpectedTokens) {
        final List<String> expected = spaceSeparatedExpectedTokens == null
                ? new ArrayList<>()
                : Arrays.asList(spaceSeparatedExpectedTokens.split(" +"));

        assertEquals(expected, getTokens(sql));
    }

    private void assertInvalidSql(String sql, String message) {
        try {
            getTokens(sql);
            fail("Didn't throw InvalidSqlException");
        } catch (InvalidSqlException e) {
            MoreAsserts.assertContainsRegex(message, e.getMessage());
        }
    }

    public void testWhitespaces() {
        checkTokens("  select  \t\r\n a\n\n  ", "select a");
        checkTokens("a b", "a b");
    }

    public void testComment() {
        checkTokens("--\n", null);
        checkTokens("a--\n", "a");
        checkTokens("a--abcdef\n", "a");
        checkTokens("a--abcdef\nx", "a x");
        checkTokens("a--\nx", "a x");
        assertInvalidSql("a--abcdef", "Unterminated comment");
        assertInvalidSql("a--abcdef\ndef--", "Unterminated comment");

        checkTokens("/**/", null);
        assertInvalidSql("/*", "Unterminated comment");
        assertInvalidSql("/*/", "Unterminated comment");
        assertInvalidSql("/*\n* /*a", "Unterminated comment");
        checkTokens("a/**/", "a");
        checkTokens("/**/b", "b");
        checkTokens("a/**/b", "a b");
        checkTokens("a/* -- \n* /* **/b", "a b");
    }

    public void testStrings() {
        assertInvalidSql("'", "Unterminated quote");
        assertInvalidSql("a'", "Unterminated quote");
        assertInvalidSql("a'''", "Unterminated quote");
        assertInvalidSql("a''' ", "Unterminated quote");
        checkTokens("''", null);
        checkTokens("''''", null);
        checkTokens("a''''b", "a b");
        checkTokens("a' '' 'b", "a b");
        checkTokens("'abc'", null);
        checkTokens("'abc\ndef'", null);
        checkTokens("a'abc\ndef'", "a");
        checkTokens("'abc\ndef'b", "b");
        checkTokens("a'abc\ndef'b", "a b");
        checkTokens("a'''abc\nd''ef'''b", "a b");
    }

    public void testDoubleQuotes() {
        assertInvalidSql("\"", "Unterminated quote");
        assertInvalidSql("a\"", "Unterminated quote");
        assertInvalidSql("a\"\"\"", "Unterminated quote");
        assertInvalidSql("a\"\"\" ", "Unterminated quote");
        checkTokens("\"\"", "");
        checkTokens("\"\"\"\"", "\"");
        checkTokens("a\"\"\"\"b", "a \" b");
        checkTokens("a\"\t\"\"\t\"b", "a  \t\"\t  b");
        checkTokens("\"abc\"", "abc");
        checkTokens("\"abc\ndef\"", "abc\ndef");
        checkTokens("a\"abc\ndef\"", "a abc\ndef");
        checkTokens("\"abc\ndef\"b", "abc\ndef b");
        checkTokens("a\"abc\ndef\"b", "a abc\ndef b");
        checkTokens("a\"\"\"abc\nd\"\"ef\"\"\"b", "a \"abc\nd\"ef\" b");
    }

    public void testBackQuotes() {
        assertInvalidSql("`", "Unterminated quote");
        assertInvalidSql("a`", "Unterminated quote");
        assertInvalidSql("a```", "Unterminated quote");
        assertInvalidSql("a``` ", "Unterminated quote");
        checkTokens("``", "");
        checkTokens("````", "`");
        checkTokens("a````b", "a ` b");
        checkTokens("a`\t``\t`b", "a  \t`\t  b");
        checkTokens("`abc`", "abc");
        checkTokens("`abc\ndef`", "abc\ndef");
        checkTokens("a`abc\ndef`", "a abc\ndef");
        checkTokens("`abc\ndef`b", "abc\ndef b");
        checkTokens("a`abc\ndef`b", "a abc\ndef b");
        checkTokens("a```abc\nd``ef```b", "a `abc\nd`ef` b");
    }

    public void testBrackets() {
        assertInvalidSql("[", "Unterminated quote");
        assertInvalidSql("a[", "Unterminated quote");
        assertInvalidSql("a[ ", "Unterminated quote");
        assertInvalidSql("a[[ ", "Unterminated quote");
        checkTokens("[]", "");
        checkTokens("[[]", "[");
        checkTokens("a[[]b", "a [ b");
        checkTokens("a[\t[\t]b", "a  \t[\t  b");
        checkTokens("[abc]", "abc");
        checkTokens("[abc\ndef]", "abc\ndef");
        checkTokens("a[abc\ndef]", "a abc\ndef");
        checkTokens("[abc\ndef]b", "abc\ndef b");
        checkTokens("a[abc\ndef]b", "a abc\ndef b");
        checkTokens("a[[abc\nd[ef[]b", "a [abc\nd[ef[ b");
    }

    public void testSemicolons() {
        assertInvalidSql(";", "Semicolon is not allowed");
        assertInvalidSql("  ;", "Semicolon is not allowed");
        assertInvalidSql(";  ", "Semicolon is not allowed");
        assertInvalidSql("-;-", "Semicolon is not allowed");
        checkTokens("--;\n", null);
        checkTokens("/*;*/", null);
        checkTokens("';'", null);
        checkTokens("[;]", ";");
        checkTokens("`;`", ";");
    }

    public void testTokens() {
        checkTokens("a,abc,a00b,_1,_123,abcdef", "a abc a00b _1 _123 abcdef");
        checkTokens("a--\nabc/**/a00b''_1'''ABC'''`_123`abc[d]\"e\"f",
                "a abc a00b _1 _123 abc d e f");
    }

    private SqlChecker getChecker(String... tokens) {
        return new SqlChecker(Arrays.asList(tokens));
    }

    private void checkEnsureNoInvalidTokens(boolean ok, String sql, String... tokens) {
        if (ok) {
            getChecker(tokens).ensureNoInvalidTokens(sql);
        } else {
            try {
                getChecker(tokens).ensureNoInvalidTokens(sql);
                fail("Should have thrown");
            } catch (InvalidSqlException e) {
                // okay
            }
        }
    }

    public void testEnsureNoInvalidTokens() {
        checkEnsureNoInvalidTokens(true, "a b c", "Select");

        checkEnsureNoInvalidTokens(false, "a b ;c", "Select");
        checkEnsureNoInvalidTokens(false, "a b seLeCt", "Select");

        checkEnsureNoInvalidTokens(true, "a b select", "x");

        checkEnsureNoInvalidTokens(false, "A b select", "x", "a");
        checkEnsureNoInvalidTokens(false, "A b select", "a", "x");

        checkEnsureNoInvalidTokens(true, "a /*select*/ b c ", "select");
        checkEnsureNoInvalidTokens(true, "a 'select' b c ", "select");

        checkEnsureNoInvalidTokens(true, "a b ';' c");
        checkEnsureNoInvalidTokens(true, "a b /*;*/ c");

        checkEnsureNoInvalidTokens(false, "a b x_ c");
        checkEnsureNoInvalidTokens(false, "a b [X_OK] c");
        checkEnsureNoInvalidTokens(true, "a b 'x_' c");
        checkEnsureNoInvalidTokens(true, "a b /*x_*/ c");
    }

    private void checkEnsureSingleTokenOnly(boolean ok, String sql, String... tokens) {
        if (ok) {
            getChecker(tokens).ensureSingleTokenOnly(sql);
        } else {
            try {
                getChecker(tokens).ensureSingleTokenOnly(sql);
                fail("Should have thrown");
            } catch (InvalidSqlException e) {
                // okay
            }
        }
    }

    public void testEnsureSingleTokenOnly() {
        checkEnsureSingleTokenOnly(true, "a", "select");
        checkEnsureSingleTokenOnly(true, "ab", "select");
        checkEnsureSingleTokenOnly(true, "selec", "select");
        checkEnsureSingleTokenOnly(true, "selectx", "select");

        checkEnsureSingleTokenOnly(false, "select", "select");
        checkEnsureSingleTokenOnly(false, "select", "a", "select");
        checkEnsureSingleTokenOnly(false, "select", "select", "b");
        checkEnsureSingleTokenOnly(false, "select", "a", "select", "b");


        checkEnsureSingleTokenOnly(true, "`a`", "select");
        checkEnsureSingleTokenOnly(true, "[a]", "select");
        checkEnsureSingleTokenOnly(true, "\"a\"", "select");

        checkEnsureSingleTokenOnly(false, "'a'", "select");

        checkEnsureSingleTokenOnly(false, "b`a`", "select");
        checkEnsureSingleTokenOnly(false, "b[a]", "select");
        checkEnsureSingleTokenOnly(false, "b\"a\"", "select");
        checkEnsureSingleTokenOnly(false, "b'a'", "select");

        checkEnsureSingleTokenOnly(false, "`a`c", "select");
        checkEnsureSingleTokenOnly(false, "[a]c", "select");
        checkEnsureSingleTokenOnly(false, "\"a\"c", "select");
        checkEnsureSingleTokenOnly(false, "'a'c", "select");

        checkEnsureSingleTokenOnly(false, "", "select");
        checkEnsureSingleTokenOnly(false, "--", "select");
        checkEnsureSingleTokenOnly(false, "/**/", "select");
        checkEnsureSingleTokenOnly(false, "  \n", "select");
        checkEnsureSingleTokenOnly(false, "a--", "select");
        checkEnsureSingleTokenOnly(false, "a/**/", "select");
        checkEnsureSingleTokenOnly(false, "a  \n", "select");
    }
}
