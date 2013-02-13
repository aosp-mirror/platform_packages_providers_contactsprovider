/*
 * Copyright (C) 2010 The Android Open Source Project
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

import android.provider.ContactsContract.FullNameStyle;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

@SmallTest
public class ContactLocaleUtilsTest extends AndroidTestCase {
    private static final String PHONE_NUMBER_1 = "+1 (650) 555-1212";
    private static final String PHONE_NUMBER_2 = "650-555-1212";
    private static final String LATIN_NAME = "John Smith";
    private static final String LATIN_NAME_2 = "John Paul Jones";
    private static final String KANJI_NAME = "\u65e5";
    private static final String CHINESE_NAME = "\u675C\u9D51";
    private static final String CHINESE_LATIN_MIX_NAME_1 = "D\u675C\u9D51";
    private static final String CHINESE_LATIN_MIX_NAME_2 = "MARY \u675C\u9D51";
    private static final String[] CHINESE_NAME_KEY = {"\u9D51", "\u675C\u9D51", "JUAN", "DUJUAN",
            "J", "DJ"};
    private static final String[] CHINESE_LATIN_MIX_NAME_1_KEY = {"\u9D51", "\u675C\u9D51",
        "D \u675C\u9D51", "JUAN", "DUJUAN", "J", "DJ", "D DUJUAN", "DDJ"};
    private static final String[] CHINESE_LATIN_MIX_NAME_2_KEY = {"\u9D51", "\u675C\u9D51",
        "MARY \u675C\u9D51", "JUAN", "DUJUAN", "MARY DUJUAN", "J", "DJ", "MDJ"};
    private static final String[] LATIN_NAME_KEY = {"John Smith", "Smith", "JS", "S"};
    private static final String[] LATIN_NAME_KEY_2 = {
        "John Paul Jones", "Paul Jones", "Jones", "JPJ", "PJ", "J"};
    private static final String[] LABELS_EN_US = {
        "", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};
    private static final String[] LABELS_JA_JP = {
        "", "\u3042", "\u304B", "\u3055", "\u305F", "\u306A", "\u306F",
        "\u307E", "\u3084", "\u3089", "\u308F", "\u4ED6",
        "", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};
    private static final String[] LABELS_ZH_TW = {
        "", "1\u5283", "2\u5283", "3\u5283", "4\u5283", "5\u5283", "6\u5283",
        "7\u5283", "8\u5283", "9\u5283", "10\u5283", "11\u5283", "12\u5283",
        "13\u5283", "14\u5283", "15\u5283", "16\u5283", "17\u5283", "18\u5283",
        "19\u5283", "20\u5283", "21\u5283", "22\u5283", "23\u5283", "24\u5283",
        "25\u5283",
        "", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};
    private static final String[] LABELS_KO = {
        "",  "\u1100", "\u1102", "\u1103", "\u1105", "\u1106", "\u1107",
        "\u1109", "\u110B", "\u110C", "\u110E", "\u110F", "\u1110", "\u1111",
        "\u1112",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};

    private static final String JAPANESE_MISC = "\u4ed6";

    private String getLabel(String name) {
        ContactLocaleUtils utils = ContactLocaleUtils.getInstance();
        int bucketIndex = utils.getBucketIndex(name);
        return utils.getBucketLabel(bucketIndex);
    }

    private Iterator<String> getNameLookupKeys(String name, int nameStyle) {
        ContactLocaleUtils utils = ContactLocaleUtils.getInstance();
        return utils.getNameLookupKeys(name, nameStyle);
    }

    private ArrayList<String> getLabels() {
        ContactLocaleUtils utils = ContactLocaleUtils.getInstance();
        return utils.getLabels();
    }

    public void testEnglishContactLocaleUtils() throws Exception {
        ContactLocaleUtils.setLocale(Locale.ENGLISH);
        assertEquals("#", getLabel(PHONE_NUMBER_1));
        assertEquals("#", getLabel(PHONE_NUMBER_2));
        assertEquals("J", getLabel(LATIN_NAME));
        assertEquals("", getLabel(CHINESE_NAME));
        assertEquals("D", getLabel(CHINESE_LATIN_MIX_NAME_1));
        assertEquals("B", getLabel("Bob Smith"));

        assertNull(getNameLookupKeys(LATIN_NAME, FullNameStyle.UNDEFINED));
        verifyLabels(getLabels(), LABELS_EN_US);
    }

    public void testJapaneseContactLocaleUtils() throws Exception {
        if (!hasJapaneseCollator()) {
            return;
        }

        ContactLocaleUtils.setLocale(Locale.JAPAN);
        assertEquals("#", getLabel(PHONE_NUMBER_1));
        assertEquals("#", getLabel(PHONE_NUMBER_2));
        assertEquals(JAPANESE_MISC, getLabel(KANJI_NAME));
        assertEquals("J", getLabel(LATIN_NAME));
        assertEquals(JAPANESE_MISC, getLabel(CHINESE_NAME));
        assertEquals("D", getLabel(CHINESE_LATIN_MIX_NAME_1));

        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK));
        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);

        // Following two tests are broken with ICU 4.9
        verifyLabels(getLabels(), LABELS_JA_JP);
        assertEquals("B", getLabel("Bob Smith"));
    }

    public void testChineseContactLocaleUtils() throws Exception {
        if (!hasChineseCollator()) {
            return;
        }

        ContactLocaleUtils.setLocale(Locale.SIMPLIFIED_CHINESE);
        assertEquals("#", getLabel(PHONE_NUMBER_1));
        assertEquals("#", getLabel(PHONE_NUMBER_2));
        assertEquals("J", getLabel(LATIN_NAME));
        assertEquals("D", getLabel(CHINESE_NAME));
        assertEquals("D", getLabel(CHINESE_LATIN_MIX_NAME_1));
        assertEquals("B", getLabel("Bob Smith"));
        verifyLabels(getLabels(), LABELS_EN_US);

        ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
        assertEquals("#", getLabel(PHONE_NUMBER_1));
        assertEquals("#", getLabel(PHONE_NUMBER_2));
        assertEquals("J", getLabel(LATIN_NAME));
        assertEquals("12\u5283", getLabel(CHINESE_NAME));
        assertEquals("D", getLabel(CHINESE_LATIN_MIX_NAME_1));

        ContactLocaleUtils.setLocale(Locale.SIMPLIFIED_CHINESE);
        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);

        keys = getNameLookupKeys(CHINESE_LATIN_MIX_NAME_1, FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_1_KEY);

        keys = getNameLookupKeys(CHINESE_LATIN_MIX_NAME_2, FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_2_KEY);

        // Following test broken with ICU 4.9
        ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
        verifyLabels(getLabels(), LABELS_ZH_TW);
        assertEquals("B", getLabel("Bob Smith"));
    }

    public void testChineseStyleNameWithDifferentLocale() throws Exception {
        if (!hasChineseCollator()) {
            return;
        }

        ContactLocaleUtils.setLocale(Locale.ENGLISH);
        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);
        keys = getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);

        ContactLocaleUtils.setLocale(Locale.CHINESE);
        keys = getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);
        keys = getNameLookupKeys(LATIN_NAME, FullNameStyle.WESTERN);
        verifyKeys(keys, LATIN_NAME_KEY);
        keys = getNameLookupKeys(LATIN_NAME_2, FullNameStyle.WESTERN);
        verifyKeys(keys, LATIN_NAME_KEY_2);

        ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
        keys = getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);
    }

    public void testKoreanContactLocaleUtils() throws Exception {
        ContactLocaleUtils.setLocale(Locale.KOREA);
        assertEquals("B", getLabel("Bob Smith"));
        verifyLabels(getLabels(), LABELS_KO);
    }

    private void verifyKeys(final Iterator<String> resultKeys, final String[] expectedKeys)
            throws Exception {
        HashSet<String> allKeys = new HashSet<String>();
        while (resultKeys.hasNext()) {
            allKeys.add(resultKeys.next());
        }
        assertEquals(new HashSet<String>(Arrays.asList(expectedKeys)), allKeys);
    }

    private void verifyLabels(final ArrayList<String> resultLabels,
                              final String[] expectedLabels)
            throws Exception {
        assertEquals(new ArrayList<String>(Arrays.asList(expectedLabels)),
                     resultLabels);
    }

    private boolean hasChineseCollator() {
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.CHINA)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasJapaneseCollator() {
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.JAPAN)) {
                return true;
            }
        }
        return false;
    }
}
