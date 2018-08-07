/*
 * Copyright (C) 2018 The Android Open Source Project
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

import android.test.suitebuilder.annotation.SmallTest;

import junit.framework.TestCase;

/**
 * Run with:
 atest /android/pi-dev/packages/providers/ContactsProvider/tests/src/com/android/providers/contacts/util/CappedStringBuilderTest.java
 */
@SmallTest
public class CappedStringBuilderTest extends TestCase {
    public void testCappedChar() {
        CappedStringBuilder csb = new CappedStringBuilder(8);

        csb.append("abcd");
        csb.append("efgh");

        csb.append('x');
        assertEquals("abcdefgh", csb.toString());

        csb.append("y");
        csb.append("yz");

        assertEquals("abcdefgh", csb.toString());
    }

    public void testCappedString() {
        CappedStringBuilder csb = new CappedStringBuilder(8);

        csb.append("abcd");
        csb.append("efgh");

        csb.append("x");
        assertEquals("abcdefgh", csb.toString());
    }

    public void testClear() {
        CappedStringBuilder csb = new CappedStringBuilder(8);

        csb.append("abcd");
        csb.append("efgh");

        csb.append("x");

        assertEquals("abcdefgh", csb.toString());

        csb.clear();

        assertEquals("", csb.toString());

        csb.append("abcd");
        assertEquals("abcd", csb.toString());
    }

    public void testAlreadyCapped() {
        CappedStringBuilder csb = new CappedStringBuilder(4);

        csb.append("abc");

        csb.append("xy");

        // Once capped, further append() will all be blocked.
        csb.append('z');
        csb.append("z");

        assertEquals("abc", csb.toString());
    }
}
