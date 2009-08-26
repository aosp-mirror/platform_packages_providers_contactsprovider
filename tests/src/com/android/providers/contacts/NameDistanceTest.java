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
 * limitations under the License
 */
package com.android.providers.contacts;

import android.test.suitebuilder.annotation.SmallTest;

import junit.framework.TestCase;

@SmallTest
public class NameDistanceTest extends TestCase {

    private NameDistance mNameDistance;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mNameDistance = new NameDistance(30);
    }

    public void testExactMatch() {
        assertFloat(1, "Dwayne", "Dwayne");
    }

    public void testMismatches() {
        assertFloat(0.8f, "Abcdef", "Abcdex");
        assertFloat(0.6f, "Abcdef", "Abcxex");
        assertFloat(0.0f, "Abcdef", "Abxxex");
    }

    public void testTranspositions() {
        assertFloat(0.8f, "Abcdef", "Cbadef");
        assertFloat(0.6f, "Abcdef", "Abfdec");
        assertFloat(0.6f, "Abcdef", "Cbafed");
        assertFloat(0.0f, "Abcdef", "Fedcba");
    }

    public void testNoMatches() {
        assertFloat(0, "Abcd", "Efgh");
    }

    public void testSimilarNames() {
        assertFloat(0.3f, "SallyCarrera", "SallieCarerra");
    }

    private void assertFloat(float expected, String name1, String name2) {
        byte[] s1 = Hex.decodeHex(NameNormalizer.normalize(name1));
        byte[] s2 = Hex.decodeHex(NameNormalizer.normalize(name2));

        float actual = mNameDistance.getDistance(s1, s2);
        assertTrue("Expected Jaro-Winkler distance: " + expected + ", actual: " + actual,
                Math.abs(actual - expected) < 0.001);
    }
}
