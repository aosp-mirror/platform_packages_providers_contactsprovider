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

import android.test.suitebuilder.annotation.SmallTest;

import junit.framework.TestCase;

/**
 * Unit tests for {@link NameNormalizer}.
 *
 * Run the test like this:
 * <code>
   adb shell am instrument -e class com.android.providers.contacts.NameNormalizerTest -w \
           com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@SmallTest
public class NameNormalizerTest extends TestCase {

    public void testDifferent() {
        final String name1 = NameNormalizer.normalize("Helene");
        final String name2 = NameNormalizer.normalize("Francesca");
        assertFalse(name2.equals(name1));
    }

    public void testAccents() {
        final String name1 = NameNormalizer.normalize("Helene");
        final String name2 = NameNormalizer.normalize("H\u00e9l\u00e8ne");
        assertTrue(name2.equals(name1));
    }

    public void testMixedCase() {
        final String name1 = NameNormalizer.normalize("Helene");
        final String name2 = NameNormalizer.normalize("hEL\uFF25NE"); // FF25 = FULL WIDTH E
        assertTrue(name2.equals(name1));
    }

    public void testNonLetters() {
        // U+FF1E: 'FULLWIDTH GREATER-THAN SIGN'
        // U+FF03: 'FULLWIDTH NUMBER SIGN'
        final String name1 = NameNormalizer.normalize("h-e?l \uFF1ee+\uFF03n=e");
        final String name2 = NameNormalizer.normalize("helene");
        assertTrue(name2.equals(name1));
    }

    public void testComplexityCase() {
        assertTrue(NameNormalizer.compareComplexity("Helene", "helene") > 0);
    }

    public void testComplexityAccent() {
        assertTrue(NameNormalizer.compareComplexity("H\u00e9lene", "Helene") > 0);
    }

    public void testComplexityLength() {
        assertTrue(NameNormalizer.compareComplexity("helene2009", "helene") > 0);
    }
}
