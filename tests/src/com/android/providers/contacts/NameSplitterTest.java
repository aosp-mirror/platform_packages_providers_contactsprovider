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

import junit.framework.TestCase;

import com.android.providers.contacts.NameSplitter.Name;

import android.test.suitebuilder.annotation.SmallTest;

/**
 * Tests for {@link NameSplitter}.
 */
@SmallTest
public class NameSplitterTest extends TestCase {
    private NameSplitter mNameSplitter;
    private Name mName;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mNameSplitter = new NameSplitter("Mr, Ms, Mrs", "d', st, st., von", "Jr, M.D., MD, D.D.S.",
                "&, AND");
        mName = new Name();
    }

    public void testNull() {
        assertSplitName(null, null, null, null, null, null);
    }

    public void testEmpty() {
        assertSplitName("", null, null, null, null, null);
    }

    public void testSpaces() {
        assertSplitName(" ", null, null, null, null, null);
    }

    public void testLastName() {
        assertSplitName("Smith", null, null, null, "Smith", null);
    }

    public void testIgnoreSuffix() {
        assertSplitName("Ms MD", "Ms", null, null, "MD", null);
    }

    public void testFirstLastName() {
        assertSplitName("John Smith", null, "John", null, "Smith", null);
    }

    public void testFirstMiddleLastName() {
        assertSplitName("John Edward Smith", null, "John", "Edward", "Smith", null);
    }

    public void testThreeNamesAndLastName() {
        assertSplitName("John Edward Kevin Smith", null, "John Edward", "Kevin", "Smith", null);
    }

    public void testPrefixFirstLastName() {
        assertSplitName("Mr. John Smith", "Mr", "John", null, "Smith", null);
        assertSplitName("Mr.John Smith", "Mr", "John", null, "Smith", null);
    }

    public void testFirstLastNameSuffix() {
        assertSplitName("John Smith Jr.", null, "John", null, "Smith", "Jr");
    }

    public void testFirstLastNameSuffixWithDot() {
        assertSplitName("John Smith M.D.", null, "John", null, "Smith", "M.D.");
        assertSplitName("John Smith D D S", null, "John", null, "Smith", "D D S");
    }

    public void testFirstSuffixLastName() {
        assertSplitName("John von Smith", null, "John", null, "von Smith", null);
    }

    public void testFirstSuffixLastNameWithDot() {
        assertSplitName("John St.Smith", null, "John", null, "St. Smith", null);
    }

    public void testPrefixFirstMiddleLast() {
        assertSplitName("Mr. John Kevin Smith", "Mr", "John", "Kevin", "Smith", null);
        assertSplitName("Mr.John Kevin Smith", "Mr", "John", "Kevin", "Smith", null);
    }

    public void testPrefixFirstMiddleLastSuffix() {
        assertSplitName("Mr. John Kevin Smith Jr.", "Mr", "John", "Kevin", "Smith", "Jr");
    }

    public void testPrefixFirstMiddlePrefixLastSuffixWrongCapitalization() {
        assertSplitName("MR. john keVin VON SmiTh JR.", "MR", "john", "keVin", "VON SmiTh", "JR");
    }

    public void testPrefixLastSuffix() {
        assertSplitName("von Smith Jr.", null, null, null, "von Smith", "Jr");
    }

    public void testTwoNamesAndLastNameWithAmpersand() {
        assertSplitName("John & Edward Smith", null, "John & Edward", null, "Smith", null);
        assertSplitName("John and Edward Smith", null, "John and Edward", null, "Smith", null);
    }

    public void testWithMiddleInitialAndNoDot() {
        assertSplitName("John E. Smith", null, "John", "E", "Smith", null);
    }

    public void testWithLongFirstNameAndDot() {
        assertSplitName("John Ed. K. Smith", null, "John Ed.", "K", "Smith", null);
      }

    private void assertSplitName(String fullName, String prefix, String givenNames,
            String middleName, String lastName, String suffix) {
        mNameSplitter.split(mName, fullName);
        assertEquals(prefix, mName.getPrefix());
        assertEquals(givenNames, mName.getGivenNames());
        assertEquals(middleName, mName.getMiddleName());
        assertEquals(lastName, mName.getFamilyName());
        assertEquals(suffix, mName.getSuffix());
    }
}
