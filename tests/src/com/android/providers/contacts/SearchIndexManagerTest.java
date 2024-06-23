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
import android.net.Uri;
import android.net.Uri.Builder;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.SearchSnippets;
import android.test.MoreAsserts;

import androidx.test.filters.MediumTest;
import androidx.test.filters.Suppress;

import com.android.providers.contacts.testutil.DataUtil;
import com.android.providers.contacts.testutil.RawContactUtil;

import java.text.Collator;
import java.util.Arrays;
import java.util.Locale;

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

    public void testSearchIndexForStructuredName() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Bob I. Parr");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
        values.clear();
        values.put(StructuredName.PREFIX, "Mrs.");
        values.put(StructuredName.GIVEN_NAME, "Helen");
        values.put(StructuredName.MIDDLE_NAME, "I.");
        values.put(StructuredName.FAMILY_NAME, "Parr");
        values.put(StructuredName.SUFFIX, "PhD");
        values.put(StructuredName.PHONETIC_FAMILY_NAME, "par");
        values.put(StructuredName.PHONETIC_GIVEN_NAME, "helen");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertSearchIndex(
                contactId, null, "John Doe Bob I Parr Helen I Parr PhD par helen parhelen", null);
    }

    public void testSearchIndexForStructuredName_phoneticOnly() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Bob I. Parr");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
        values.clear();
        values.put(StructuredName.PREFIX, "Mrs.");
        values.put(StructuredName.GIVEN_NAME, "Helen");
        values.put(StructuredName.MIDDLE_NAME, "I.");
        values.put(StructuredName.FAMILY_NAME, "Parr");
        values.put(StructuredName.SUFFIX, "PhD");
        values.put(StructuredName.PHONETIC_FAMILY_NAME, "yamada");
        values.put(StructuredName.PHONETIC_GIVEN_NAME, "taro");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertSearchIndex(contactId, null, "yamada taro", null);
    }

    public void testSearchIndexForChineseName() {
        // Only run this test when Chinese collation is supported
        if (!Arrays.asList(Collator.getAvailableLocales()).contains(Locale.CHINA)) {
            return;
        }

        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "\u695A\u8FAD");    // CHUCI
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertSearchIndex(
                contactId, null, "\u695A\u8FAD \u695A\u8FAD CI \u8FAD CHUCI CC C", null);
    }

    public void testSearchByChineseName() {
        // Only run this test when Chinese collation is supported
        if (!Arrays.asList(Collator.getAvailableLocales()).contains(Locale.CHINA)) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.SIMPLIFIED_CHINESE);

        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "\u695A\u8FAD");    // CHUCI
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertStoredValue(buildSearchUri("\u695A\u8FAD"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("\u8FAD"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("CI"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("CHUCI"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("CC"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("C"), SearchSnippets.SNIPPET, null);
    }

    public void testSearchIndexForKoreanName() {
        // Only run this test when Korean collation is supported
        if (!Arrays.asList(Collator.getAvailableLocales()).contains(Locale.KOREA)) {
            return;
        }

        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "\uC774\uC0C1\uC77C");    // Lee Sang Il
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertSearchIndex(contactId, null,
                "\uC774\uC0C1\uC77C \uC0C1\uC77C \u1109\u110B \u110B\u1109\u110B", null);
    }

    public void testSearchByKoreanName() {
        // Only run this test when Korean collation is supported
        if (!Arrays.asList(Collator.getAvailableLocales()).contains(Locale.KOREA)) {
            return;
        }

        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "\uC774\uC0C1\uC77C");   // Lee Sang Il
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        // Full name: Lee Sang Il
        assertStoredValue(buildSearchUri("\uC774\uC0C1\uC77C"), SearchSnippets.SNIPPET, null);

        // Given name: Sang Il
        assertStoredValue(buildSearchUri("\uC0C1\uC77C"), SearchSnippets.SNIPPET, null);

        // Consonants of given name: SIOS IEUNG
        assertStoredValue(buildSearchUri("\u1109\u110B"), SearchSnippets.SNIPPET, null);

        // Consonants of full name: RIEUL SIOS IEUNG
        assertStoredValue(buildSearchUri("\u110B\u1109\u110B"), SearchSnippets.SNIPPET, null);
    }

    public void testSearchByKoreanNameWithTwoCharactersFamilyName() {
        // Only run this test when Korean collation is supported.
        if (!Arrays.asList(Collator.getAvailableLocales()).contains(Locale.KOREA)) {
            return;
        }

        long rawContactId = RawContactUtil.createRawContact(mResolver);

        // Sun Woo Young Nyeu
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "\uC120\uC6B0\uC6A9\uB140");

        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        // Full name: Sun Woo Young Nyeu
        assertStoredValue(
                buildSearchUri("\uC120\uC6B0\uC6A9\uB140"), SearchSnippets.SNIPPET, null);

        // Given name: Young Nyeu
        assertStoredValue(buildSearchUri("\uC6A9\uB140"), SearchSnippets.SNIPPET, null);

        // Consonants of given name: IEUNG NIEUN
        assertStoredValue(buildSearchUri("\u110B\u1102"), SearchSnippets.SNIPPET, null);

        // Consonants of full name: SIOS IEUNG IEUNG NIEUN
        assertStoredValue(
                buildSearchUri("\u1109\u110B\u110B\u1102"), SearchSnippets.SNIPPET, null);
    }

    public void testSearchIndexForOrganization() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
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
                null, null);
    }

    public void testSearchIndexForPhoneNumber() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertPhoneNumber(rawContactId, "800555GOOG");
        insertPhoneNumber(rawContactId, "8005551234");

        assertSearchIndex(contactId, null, null, "8005554664 +18005554664 8005551234 +18005551234");
    }

    public void testSearchIndexForEmail() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertEmail(rawContactId, "Bob Parr <incredible@android.com>");
        insertEmail(rawContactId, "bob_parr@android.com");

        assertSearchIndex(contactId, "Bob Parr <incredible@android.com>\nbob_parr@android.com",
                null, null);
    }

    public void testSearchIndexForNickname() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertNickname(rawContactId, "incredible");

        assertSearchIndex(contactId, "incredible", null, null);
    }

    public void testSearchIndexForStructuredPostal() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertPostalAddress(rawContactId, "1600 Amphitheatre Pkwy\nMountain View, CA 94043");
        ContentValues values = new ContentValues();
        values.put(StructuredPostal.CITY, "London");
        values.put(StructuredPostal.STREET, "76 Buckingham Palace Road");
        values.put(StructuredPostal.POSTCODE, "SW1W 9TQ");
        values.put(StructuredPostal.COUNTRY, "United Kingdom");
        insertPostalAddress(rawContactId, values);

        assertSearchIndex(contactId, "1600 Amphitheatre Pkwy Mountain View, CA 94043\n"
                + "76 Buckingham Palace Road London SW1W 9TQ United Kingdom", null, null);
    }

    public void testSearchIndexForIm() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertImHandle(rawContactId, Im.PROTOCOL_JABBER, null, "bp@android.com");
        insertImHandle(rawContactId, Im.PROTOCOL_CUSTOM, "android_im", "android@android.com");

        assertSearchIndex(
                contactId, "Jabber/bp@android.com\nandroid_im/android@android.com", null, null);
    }

    public void testSearchIndexForNote() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertNote(rawContactId, "Please note: three notes or more make up a chord.");

        assertSearchIndex(
                contactId, "Please note: three notes or more make up a chord.", null, null);
    }

    public void testSnippetArgs() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertNote(rawContactId, "Please note: three notes or more make up a chord.");

        assertStoredValue(
                buildSearchUri("thr", "[,],-,2", false), SearchSnippets.SNIPPET,
                "-note: [three]-");
    }

    public void testEmptyFilter() {
        RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");
        assertEquals(0, getCount(buildSearchUri(""), null, null));
    }

    public void testSearchByName() {
        RawContactUtil.createRawContactWithName(mResolver, "John Jay", "Doe");

        // We are supposed to find the contact, but return a null snippet
        assertStoredValue(buildSearchUri("john"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("jay"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("doe"), SearchSnippets.SNIPPET, null);
    }

    public void testSearchByPrefixName() {
        RawContactUtil.createRawContactWithName(mResolver, "John Jay", "Doe");

        // prefix searches
        assertStoredValue(buildSearchUri("jo ja"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("J D"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Doe, John"), SearchSnippets.SNIPPET, null);
    }

    public void testGermanUmlautFullameCapitalizationSearch() {
        RawContactUtil.createRawContactWithName(mResolver, "Matthäus BJÖRN Bünyamin", "Reißer");

        // make sure we can find those, independent of the capitalization
        assertStoredValue(buildSearchUri("matthäus"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Matthäus"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("MATTHÄUS"), SearchSnippets.SNIPPET, null);

        assertStoredValue(buildSearchUri("björn"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Björn"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("BJÖRN"), SearchSnippets.SNIPPET, null);

        assertStoredValue(buildSearchUri("bünyamin"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Bünyamin"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("BUNYAMIN"), SearchSnippets.SNIPPET, null);

        // There is no capital version of ß. It is capitalized as double-S instead
        assertStoredValue(buildSearchUri("Reißer"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Reisser"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("REISSER"), SearchSnippets.SNIPPET, null);
    }

    public void testHangulNameLeadConsonantAsYouTypeSearch() {
        createRawContactWithDisplayName("홍길동");
        // the korean name uses three compound characters. this test makes sure
        // that the name can be found by typing in only the lead consonant
        assertStoredValue(buildSearchUri("ㅎ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㄱㄷ"), SearchSnippets.SNIPPET, null);

        // same again, this time only for the first name
        assertStoredValue(buildSearchUri("ㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㄷ"), SearchSnippets.SNIPPET, null);
    }

    public void testHangulNameFullAsYouTypeSearch() {
        createRawContactWithDisplayName("홍길동");

        // the korean name uses three compound characters. this test makes sure
        // that the name can be found by typing in the full nine letters. the search string
        // shows the name is being built "as you type"
        assertStoredValue(buildSearchUri("ㅎ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("호"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍ㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍기"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍길"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍길ㄷ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍길도"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("홍길동"), SearchSnippets.SNIPPET, null);

        // same again, this time only for the first name
        assertStoredValue(buildSearchUri("ㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("기"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("길"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("길ㄷ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("길도"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("길동"), SearchSnippets.SNIPPET, null);
    }


    /** Decomposed Hangul is not yet supported. This text is how we would test it */
    @Suppress
    public void testHangulNameDecomposedSearch() {
        createRawContactWithDisplayName("홍길동");

        // the korean name uses three compound characters. this test makes sure
        // that the name can be found by typing each syllable as a single character.
        // This can be achieved using the Korean IM by pressing ㅎ, space, backspace, ㅗ and so on
        assertStoredValue(buildSearchUri("ㅎ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱㅣ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱㅣㄹ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱㅣㄹㄷ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱㅣㄹㄷㅗ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㅎㅗㅇㄱㅣㄹㄷㅗㅇ"), SearchSnippets.SNIPPET, null);

        // same again, this time only for the first name
        assertStoredValue(buildSearchUri("ㄱ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㅣ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㅣㄹ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㅣㄹㄷ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㅣㄹㄷㅗ"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("ㄱㅣㄹㄷㅗㅇ"), SearchSnippets.SNIPPET, null);
    }

    public void testNameWithHyphen() {
        RawContactUtil.createRawContactWithName(mResolver, "First", "Last-name");

        assertStoredValue(buildSearchUri("First"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-n"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-name"), SearchSnippets.SNIPPET, null);

        // This will work too.
        assertStoredValue(buildSearchUri("Lastname"), SearchSnippets.SNIPPET, null);

        // This doesn't have to work, but it does with the current implementation.
        assertStoredValue(buildSearchUri("name"), SearchSnippets.SNIPPET, null);
    }

    /** Same as {@link #testNameWithHyphen} except the name has double hyphens. */
    public void testNameWithDoubleHyphens() {
        RawContactUtil.createRawContactWithName(mResolver, "First", "Last--name");

        assertStoredValue(buildSearchUri("First"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-n"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("Last-name"), SearchSnippets.SNIPPET, null);

        // This will work too.
        assertStoredValue(buildSearchUri("Lastname"), SearchSnippets.SNIPPET, null);
    }

    public void testNameWithPunctuations() {
        RawContactUtil.createRawContactWithName(mResolver, "First", "O'Neill");

        assertStoredValue(buildSearchUri("first"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("oneill"), SearchSnippets.SNIPPET, null);
        assertStoredValue(buildSearchUri("o'neill"), SearchSnippets.SNIPPET, null);
    }

    public void testSearchByEmailAddress() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId, "1234567890");
        insertEmail(rawContactId, "john@doe.com");
        insertNote(rawContactId, "a hundred dollar note for doe@john.com and bob parr");

        assertStoredValue(buildSearchUri("john@d"), SearchSnippets.SNIPPET,
                "[john@doe.com]");
    }

    public void testSearchByPhoneNumber() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId, "330142685300");
        insertPhoneNumber(rawContactId, "(800)GOOG-123");
        insertEmail(rawContactId, "john@doe.com");
        insertNote(rawContactId, "the eighteenth episode of Seinfeld, 650-253-0000");

        assertStoredValue(buildSearchUri("33 (0)1 42 68 53 00"), SearchSnippets.SNIPPET,
                "[330142685300]");
        assertStoredValue(buildSearchUri("8004664"), SearchSnippets.SNIPPET,
                "[(800)GOOG-123]");
        assertStoredValue(buildSearchUri("650-2"), SearchSnippets.SNIPPET,
                "\u2026of Seinfeld, [650]-[253]-0000");

        // for numbers outside of the real phone field, any order (and prefixing) is allowed
        assertStoredValue(buildSearchUri("25 650"), SearchSnippets.SNIPPET,
                "\u2026of Seinfeld, [650]-[253]-0000");
    }

    /**
     * Test case for bug 5904515
     */
    public void testSearchByPhoneNumber_diferSnippetting() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId, "505-123-4567");

        // If snippeting is deferred, the returned snippet will not contain any markers.
        assertStoredValue(buildSearchUri("505", "\u0001,\u0001,\u2026,5", true),
                SearchSnippets.SNIPPET, "505-123-4567");
    }

    /**
     * Equivalent to {@link #testSearchByPhoneNumber_diferSnippetting} for email addresses
     */
    public void testSearchByEmail_diferSnippetting() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertEmail(rawContactId, "john@doe.com");

        assertStoredValue(buildSearchUri("john", "\u0001,\u0001,\u2026,5", true),
                SearchSnippets.SNIPPET, "john@doe.com");
    }

    public void testSplitIntoFtsTokens() {
        checkSplitIntoFtsTokens("a", "a");
        checkSplitIntoFtsTokens("a_b c%d-e'f", "a_b", "c", "d", "e", "f");
        checkSplitIntoFtsTokens("  ", new String[0]);
        // There's are all "control" characters, but treated as "letters".
        // (See http://en.wikipedia.org/wiki/C1_Controls_and_Latin-1_Supplement for what they are)
        checkSplitIntoFtsTokens("\u0080 \u0081 \u0082", "\u0080", "\u0081", "\u0082");

        // FFF0 is also a token.
        checkSplitIntoFtsTokens(" \ufff0  ", "\ufff0");
    }

    private void checkSplitIntoFtsTokens(String input, String... expectedTokens) {
        MoreAsserts.assertEquals(expectedTokens,
                SearchIndexManager.splitIntoFtsTokens(input).toArray(new String[0]));
    }

    private Uri buildSearchUri(String filter) {
        return buildSearchUri(filter, false);
    }

    private Uri buildSearchUri(String filter, boolean deferredSnippeting) {
        return buildSearchUri(filter, null, deferredSnippeting);
    }

    private Uri buildSearchUri(String filter, String args, boolean deferredSnippeting) {
        Builder builder = Contacts.CONTENT_FILTER_URI.buildUpon().appendPath(filter);
        if (args != null) {
            builder.appendQueryParameter(SearchSnippets.SNIPPET_ARGS_PARAM_KEY, args);
        }
        if (deferredSnippeting) {
            builder.appendQueryParameter(SearchSnippets.DEFERRED_SNIPPETING_KEY, "1");
        }
        return builder.build();
    }

    private void createRawContactWithDisplayName(String name) {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, name);
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
    }

    // TODO: expectedName must be tested. Many tests in here are quite useless at the moment
    private void assertSearchIndex(
            long contactId, String expectedContent, String expectedName, String expectedTokens) {
        ContactsDatabaseHelper dbHelper = (ContactsDatabaseHelper) getContactsProvider()
                .getDatabaseHelper();
        assertEquals(expectedContent, dbHelper.querySearchIndexContentForTest(contactId));
        assertEquals(expectedTokens, dbHelper.querySearchIndexTokensForTest(contactId));
    }
}

