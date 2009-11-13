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

import com.android.providers.contacts.NameSplitter.Name;

import android.test.suitebuilder.annotation.SmallTest;

import java.util.Locale;

import junit.framework.TestCase;

/**
 * Tests for {@link NameSplitter}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.NameSplitterTest -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@SmallTest
public class NameSplitterTest extends TestCase {
    private NameSplitter mNameSplitter;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mNameSplitter = new NameSplitter("Mr, Ms, Mrs", "d', st, st., von", "Jr, M.D., MD, D.D.S.",
                "&, AND", Locale.getDefault());
    }

    public void testNull() {
        assertSplitName(null, null, null, null, null, null);
        assertJoinedName(null, null, null, null, null, null);
    }

    public void testEmpty() {
        assertSplitName("", null, null, null, null, null);
        assertJoinedName(null, null, null, null, null, null);
    }

    public void testSpaces() {
        assertSplitName(" ", null, null, null, null, null);
        assertJoinedName(null, null, null, null, null, null);
    }

    public void testLastName() {
        assertSplitName("Smith", null, "Smith", null, null, null);
        assertJoinedName("Smith", null, "Smith", null, null, null);
    }

    public void testIgnoreSuffix() {
        assertSplitName("Ms MD", "Ms", null, null, "MD", null);
        assertJoinedName("MD", "Ms", null, null, "MD", null);
    }

    public void testFirstLastName() {
        assertSplitName("John Smith", null, "John", null, "Smith", null);
        assertJoinedName("John Smith", null, "John", null, "Smith", null);
    }

    public void testFirstMiddleLastName() {
        assertSplitName("John Edward Smith", null, "John", "Edward", "Smith", null);
        assertJoinedName("John Smith", null, "John", "Edward", "Smith", null);
    }

    public void testThreeNamesAndLastName() {
        assertSplitName("John Edward Kevin Smith", null, "John Edward", "Kevin", "Smith", null);
        assertJoinedName("John Edward Smith", null, "John Edward", "Kevin", "Smith", null);
    }

    public void testPrefixFirstLastName() {
        assertSplitName("Mr. John Smith", "Mr", "John", null, "Smith", null);
        assertJoinedName("John Smith", "Mr", "John", null, "Smith", null);
        assertSplitName("Mr.John Smith", "Mr", "John", null, "Smith", null);
        assertJoinedName("John Smith", "Mr", "John", null, "Smith", null);
    }

    public void testFirstLastNameSuffix() {
        assertSplitName("John Smith Jr.", null, "John", null, "Smith", "Jr");
        assertJoinedName("John Smith", null, "John", null, "Smith", "Jr");
    }

    public void testFirstLastNameSuffixWithDot() {
        assertSplitName("John Smith M.D.", null, "John", null, "Smith", "M.D.");
        assertJoinedName("John Smith", null, "John", null, "Smith", "M.D.");
        assertSplitName("John Smith D D S", null, "John", null, "Smith", "D D S");
        assertJoinedName("John Smith", null, "John", null, "Smith", "D D S");
    }

    public void testFirstSuffixLastName() {
        assertSplitName("John von Smith", null, "John", null, "von Smith", null);
        assertJoinedName("John von Smith", null, "John", null, "von Smith", null);
    }

    public void testFirstSuffixLastNameWithDot() {
        assertSplitName("John St.Smith", null, "John", null, "St. Smith", null);
        assertJoinedName("John St. Smith", null, "John", null, "St. Smith", null);
    }

    public void testPrefixFirstMiddleLast() {
        assertSplitName("Mr. John Kevin Smith", "Mr", "John", "Kevin", "Smith", null);
        assertJoinedName("John Smith", "Mr", "John", "Kevin", "Smith", null);
        assertSplitName("Mr.John Kevin Smith", "Mr", "John", "Kevin", "Smith", null);
        assertJoinedName("John Smith", "Mr", "John", "Kevin", "Smith", null);
    }

    public void testPrefixFirstMiddleLastSuffix() {
        assertSplitName("Mr. John Kevin Smith Jr.", "Mr", "John", "Kevin", "Smith", "Jr");
        assertJoinedName("John Smith", "Mr", "John", "Kevin", "Smith", "Jr");
    }

    public void testPrefixFirstMiddlePrefixLastSuffixWrongCapitalization() {
        assertSplitName("MR. john keVin VON SmiTh JR.", "MR", "john", "keVin", "VON SmiTh", "JR");
        assertJoinedName("john VON SmiTh", "MR", "john", "keVin", "VON SmiTh", "JR");
    }

    public void testPrefixLastSuffix() {
        assertSplitName("von Smith Jr.", null, null, null, "von Smith", "Jr");
        assertJoinedName("von Smith", null, null, null, "von Smith", "Jr");
    }

    public void testTwoNamesAndLastNameWithAmpersand() {
        assertSplitName("John & Edward Smith", null, "John & Edward", null, "Smith", null);
        assertJoinedName("John & Edward Smith", null, "John & Edward", null, "Smith", null);
        assertSplitName("John and Edward Smith", null, "John and Edward", null, "Smith", null);
        assertJoinedName("John and Edward Smith", null, "John and Edward", null, "Smith", null);
    }

    public void testWithMiddleInitialAndNoDot() {
        assertSplitName("John E. Smith", null, "John", "E", "Smith", null);
        assertJoinedName("John Smith", null, "John", "E", "Smith", null);
    }

    public void testWithLongFirstNameAndDot() {
        assertSplitName("John Ed. K. Smith", null, "John Ed.", "K", "Smith", null);
        assertJoinedName("John Ed. Smith", null, "John Ed.", "K", "Smith", null);
    }

    private void assertSplitName(String fullName, String prefix, String givenNames,
            String middleName, String lastName, String suffix) {
        final Name name = new Name();
        mNameSplitter.split(name, fullName);
        assertEquals(prefix, name.getPrefix());
        assertEquals(givenNames, name.getGivenNames());
        assertEquals(middleName, name.getMiddleName());
        assertEquals(lastName, name.getFamilyName());
        assertEquals(suffix, name.getSuffix());
    }

    private void assertJoinedName(String fullName, String prefix, String givenNames, String middleName,
            String lastName, String suffix) {
        final Name name = new Name(prefix, givenNames, middleName, lastName, suffix);
        final String joined = mNameSplitter.join(name);
        assertEquals(fullName, joined);
    }
}
