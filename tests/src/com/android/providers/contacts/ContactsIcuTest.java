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
 * limitations under the License.
 */
package com.android.providers.contacts;

import android.icu.text.AlphabeticIndex;
import android.icu.text.AlphabeticIndex.ImmutableIndex;
import android.test.suitebuilder.annotation.Suppress;
import android.util.Log;

import java.util.Arrays;
import java.util.Locale;

public class ContactsIcuTest extends FixedAndroidTestCase {
    private static final String TAG = "ContactsIcuTest";

    private static ImmutableIndex buildIndex(String... localeTags) {
        final AlphabeticIndex ai = new AlphabeticIndex(Locale.forLanguageTag(localeTags[0]));

        // Add secondary locales, if any.
        for (int i = 1; i < localeTags.length; i++) {
            ai.addLabels(Locale.forLanguageTag(localeTags[i]));
        }

        final ImmutableIndex index = ai.buildImmutableIndex();

        Log.d(TAG, "Locales=" + Arrays.asList(localeTags));
        ContactLocaleUtils.dumpIndex(index);

        return index;
    }

    private static String getBucket(ImmutableIndex index, String name) {
        return index.getBucket(index.getBucketIndex(name)).getLabel();
    }

    private static boolean checkBucket(ImmutableIndex index, String expectedBucket, String str) {

        boolean okay = true;

        // Test each unicode character in the given string.
        final int length = str.length();
        int offset = 0;
        while (offset < length) {
            final int codePoint = Character.codePointAt(str, offset);

            final String ch = new String(new int[]{codePoint}, 0, 1);

            final String actual = getBucket(index, ch);

            if (!expectedBucket.equals(actual)) {
                Log.e(TAG, "Bucket for '" + ch + "' expected to be '"
                        + expectedBucket + "', but was '" + actual + "'");
                okay = false;
            }

            offset += Character.charCount(codePoint);
        }

        return okay;
    }

    @Suppress // This test doesn't pass since android's ICU data doesn't cover rarely used chars.
    public void testTraditionalChineseStrokeCounts() {
        final ImmutableIndex index = buildIndex("zh-Hant-TW");

        boolean okay = true;
        // Data generated from: https://en.wiktionary.org/wiki/Index:Chinese_total_strokes
        okay &= checkBucket(index, "1劃", "一丨丶丿乀乁乙乚乛亅");
        okay &= checkBucket(index, "2劃", "㐅丁丂七丄丅丆丩丷乂乃乄乜九了二亠人亻儿入八冂冖冫几");
        okay &= checkBucket(index, "3劃", "㐃㐄㐇㐈㐉㔾㔿万丈三上下丌个丫丸久乆乇么义乊乞也习乡");
        okay &= checkBucket(index, "4劃", "㐊㐋㐧㓀㓁㓅㔫㔹㕕㕚㕛㝉㞢㠪㢧㲸㸦不");
        okay &= checkBucket(index, "5劃", "㐀㐌㐍㐎㐏㐰㐱㐲㐳㐴㐵㐶㐷㒰㒱㓚㓛㓜");
        okay &= checkBucket(index, "10劃", "㑣㑥㑦㑧㑨㑩㑪㑫㑬㑭㒭㓐㓑㓒");
        okay &= checkBucket(index, "20劃", "㒤㒥㒦㒹㔒㘓㘔㘥㚀㜶㜷㜸㠤");
        okay &= checkBucket(index, "39劃", "靐");
        okay &= checkBucket(index, "48劃", "龘");

        assertTrue("Some tests failed.  See logcat for details", okay);

        /*
D ContactsIcuTest: Locales=[zh-Hant-TW]
D ContactLocale: Labels=[…,1劃,2劃,3劃,4劃,5劃,6劃,7劃,8劃,9劃,10劃,11劃,12劃,13劃,14劃,15劃,16劃,17劃,18劃,19劃,20劃,21劃,22劃,23劃,24劃,25劃,26劃,27劃,28劃,29劃,30劃,31劃,32劃,33劃,35劃,36劃,39劃,48劃,…]
E ContactsIcuTest: Bucket for '㐅' expected to be '2劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐃' expected to be '3劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐇' expected to be '3劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐈' expected to be '3劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐉' expected to be '3劃', but was '48劃'
E ContactsIcuTest: Bucket for '㔿' expected to be '3劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐊' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐋' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐧' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓀' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓅' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㕕' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㕚' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㕛' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㝉' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㞢' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㠪' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㢧' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㲸' expected to be '4劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐌' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐍' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐎' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㐏' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㒱' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓚' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓛' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓜' expected to be '5劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑣' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑧' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑨' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑩' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑪' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑫' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑬' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㑭' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㒭' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓐' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓑' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㓒' expected to be '10劃', but was '48劃'
E ContactsIcuTest: Bucket for '㒤' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㒦' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㒹' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㔒' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㘓' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㘔' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㚀' expected to be '20劃', but was '48劃'
E ContactsIcuTest: Bucket for '㠤' expected to be '20劃', but was '48劃'
         */
    }
}
