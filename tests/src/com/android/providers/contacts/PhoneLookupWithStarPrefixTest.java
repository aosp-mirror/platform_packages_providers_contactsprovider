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

import android.test.suitebuilder.annotation.LargeTest;
import android.test.suitebuilder.annotation.SmallTest;

@LargeTest
public class PhoneLookupWithStarPrefixTest extends FixedAndroidTestCase {

    @SmallTest
    public void testNormalizeNumberWithStar() {
        assertEquals("6502910000", PhoneLookupWithStarPrefix.normalizeNumberWithStar(
                "650 2910000"));
        assertEquals("1234567", PhoneLookupWithStarPrefix.normalizeNumberWithStar("12,3#4*567"));
        assertEquals("8004664114", PhoneLookupWithStarPrefix.normalizeNumberWithStar(
                "800-GOOG-114"));
        assertEquals("+16502910000", PhoneLookupWithStarPrefix.normalizeNumberWithStar(
                "+1 650 2910000"));
        assertEquals("*16502910000", PhoneLookupWithStarPrefix.normalizeNumberWithStar(
                "*1 650 2910000"));
        assertEquals("*123", PhoneLookupWithStarPrefix.normalizeNumberWithStar("*1-23"));
        assertEquals("*123", PhoneLookupWithStarPrefix.normalizeNumberWithStar("*+1-23"));
    }

}
