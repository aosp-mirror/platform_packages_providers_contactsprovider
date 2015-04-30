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
import android.util.Log;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

@SmallTest
public class ContactLocaleUtilsTest extends AndroidTestCase {
    private static final String TAG = "ContactLocaleUtilsTest";

    private static final String PHONE_NUMBER_1 = "+1 (650) 555-1212";
    private static final String PHONE_NUMBER_2 = "650-555-1212";
    private static final String LATIN_NAME = "John Smith";
    private static final String LATIN_NAME_2 = "John Paul Jones";
    private static final String KANJI_NAME = "\u65e5";
    private static final String ARABIC_NAME = "\u0646\u0648\u0631"; /* Noor */
    private static final String CHINESE_NAME = "\u675C\u9D51";
    private static final String SERBIAN_NAME = "\u0408\u0435\u043B\u0435\u043D\u0430";
    private static final String UKRAINIAN_NAME = "\u0406";
    private static final String UKRAINIAN_NAME_2 = "\u0407";
    private static final String UKRAINIAN_NAME_3 = "\u0490";
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
        "25\u5283", "26\u5283", "27\u5283", "28\u5283", "29\u5283", "30\u5283",
        "31\u5283", "32\u5283", "33\u5283",
        "35\u5283", "36\u5283", "39\u5283", "48\u5283",
        "", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};
    private static final String[] LABELS_KO = {
        "", "\u3131", "\u3134", "\u3137", "\u3139", "\u3141", "\u3142",
        "\u3145", "\u3147", "\u3148", "\u314A", "\u314B", "\u314C", "\u314D",
        "\u314E",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};
    private static final String[] LABELS_AR = {
        "", "\u0627", "\u0628", "\u062a", "\u062b", "\u062c", "\u062d",
        "\u062e", "\u062f", "\u0630", "\u0631", "\u0632", "\u0633", "\u0634",
        "\u0635", "\u0636", "\u0637", "\u0638", "\u0639", "\u063a", "\u0641",
        "\u0642", "\u0643", "\u0644", "\u0645", "\u0646", "\u0647", "\u0648",
        "\u064a",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "#", ""};

    private static final String JAPANESE_MISC = "\u4ed6";

    private static final Locale LOCALE_ARABIC = new Locale("ar");
    private static final Locale LOCALE_SERBIAN = new Locale("sr");
    private static final Locale LOCALE_UKRAINIAN = new Locale("uk");
    private boolean hasSimplifiedChineseCollator;
    private boolean hasTraditionalChineseCollator;
    private boolean hasJapaneseCollator;
    private boolean hasKoreanCollator;
    private boolean hasArabicCollator;
    private boolean hasGermanCollator;
    private boolean hasSerbianCollator;
    private boolean hasUkrainianCollator;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (LocaleSet.isLocaleSimplifiedChinese(locale[i])) {
                hasSimplifiedChineseCollator = true;
            } else if (LocaleSet.isLocaleTraditionalChinese(locale[i])) {
                hasTraditionalChineseCollator = true;
            } else if (locale[i].equals(Locale.JAPAN)) {
                hasJapaneseCollator = true;
            } else if (locale[i].equals(Locale.KOREA)) {
                hasKoreanCollator = true;
            } else if (locale[i].equals(LOCALE_ARABIC)) {
                hasArabicCollator = true;
            } else if (locale[i].equals(Locale.GERMANY)) {
                hasGermanCollator = true;
            } else if (locale[i].equals(LOCALE_SERBIAN)) {
                hasSerbianCollator = true;
            } else if (locale[i].equals(LOCALE_UKRAINIAN)) {
                hasUkrainianCollator = true;
            }
        }
    }

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

        if (hasArabicCollator) {
            assertEquals("\u0646", getLabel(ARABIC_NAME));
        }
        if (hasSerbianCollator) {
            assertEquals("\u0408", getLabel(SERBIAN_NAME));
        }
        if (hasUkrainianCollator) {
            assertEquals("\u0406", getLabel(UKRAINIAN_NAME));
        }

        assertNull(getNameLookupKeys(LATIN_NAME, FullNameStyle.UNDEFINED));
        verifyLabels(getLabels(), LABELS_EN_US);
    }

    public void testJapaneseContactLocaleUtils() throws Exception {
        if (!hasJapaneseCollator) {
            Log.w(TAG, "Japanese collator not found; skipping test");
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
        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CHINESE));

        assertEquals("B", getLabel("Bob Smith"));
        verifyLabels(getLabels(), LABELS_JA_JP);
    }

    public void testChineseContactLocaleUtils() throws Exception {
        if (!hasSimplifiedChineseCollator) {
            Log.w(TAG, "Simplified Chinese collator not found; skipping test");
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

        if (hasTraditionalChineseCollator) {
            ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
            assertEquals("#", getLabel(PHONE_NUMBER_1));
            assertEquals("#", getLabel(PHONE_NUMBER_2));
            assertEquals("J", getLabel(LATIN_NAME));
            assertEquals("7\u5283", getLabel(CHINESE_NAME));
            assertEquals("D", getLabel(CHINESE_LATIN_MIX_NAME_1));
        } else {
            Log.w(TAG, "Traditional Chinese collator not found");
        }

        ContactLocaleUtils.setLocale(Locale.SIMPLIFIED_CHINESE);
        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);

        keys = getNameLookupKeys(CHINESE_LATIN_MIX_NAME_1, FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_1_KEY);

        keys = getNameLookupKeys(CHINESE_LATIN_MIX_NAME_2, FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_2_KEY);

        if (hasTraditionalChineseCollator) {
            ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
            assertEquals("B", getLabel("Bob Smith"));
            verifyLabels(getLabels(), LABELS_ZH_TW);
        }
    }

    public void testPinyinEnabledSecondaryLocale() throws Exception {
        if (!hasSimplifiedChineseCollator) {
            Log.w(TAG, "Simplified Chinese collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocales(
                new LocaleSet(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE));
        assertEquals("D", getLabel(CHINESE_NAME));

        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);
    }

    public void testPinyinDisabledSecondaryLocale() throws Exception {
        if (!hasSimplifiedChineseCollator) {
            Log.w(TAG, "Simplified Chinese collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocales(
                new LocaleSet(Locale.ENGLISH, Locale.JAPAN));
        assertEquals("", getLabel(CHINESE_NAME));

        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CHINESE));
        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK));
    }

    public void testChineseStyleNameWithDifferentLocale() throws Exception {
        if (!hasSimplifiedChineseCollator) {
            Log.w(TAG, "Simplified Chinese collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocale(Locale.ENGLISH);
        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CHINESE));
        assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK));

        ContactLocaleUtils.setLocale(Locale.SIMPLIFIED_CHINESE);
        Iterator<String> keys = getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);
        keys = getNameLookupKeys(LATIN_NAME, FullNameStyle.WESTERN);
        verifyKeys(keys, LATIN_NAME_KEY);
        keys = getNameLookupKeys(LATIN_NAME_2, FullNameStyle.WESTERN);
        verifyKeys(keys, LATIN_NAME_KEY_2);

        if (hasTraditionalChineseCollator) {
            ContactLocaleUtils.setLocale(Locale.TRADITIONAL_CHINESE);
            assertNull(getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK));
        }
    }

    public void testKoreanContactLocaleUtils() throws Exception {
        if (!hasKoreanCollator) {
            Log.w(TAG, "Korean collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocale(Locale.KOREA);
        assertEquals("\u3131", getLabel("\u1100"));
        assertEquals("\u3131", getLabel("\u3131"));
        assertEquals("\u3131", getLabel("\u1101"));
        assertEquals("\u314e", getLabel("\u1161"));
        assertEquals("B", getLabel("Bob Smith"));
        verifyLabels(getLabels(), LABELS_KO);
    }

    public void testArabicContactLocaleUtils() throws Exception {
        if (!hasArabicCollator) {
            Log.w(TAG, "Arabic collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocale(LOCALE_ARABIC);
        assertEquals("\u0646", getLabel(ARABIC_NAME));
        assertEquals("B", getLabel("Bob Smith"));
        verifyLabels(getLabels(), LABELS_AR);
    }

    public void testSerbianContactLocaleUtils() throws Exception {
        if (!hasSerbianCollator) {
            Log.w(TAG, "Serbian collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocale(LOCALE_SERBIAN);
        assertEquals("\u0408", getLabel(SERBIAN_NAME));
        assertEquals("B", getLabel("Bob Smith"));
    }

    public void testUkrainianContactLocaleUtils() throws Exception {
        if (!hasUkrainianCollator) {
            Log.w(TAG, "Ukrainian collator not found; skipping test");
            return;
        }

        ContactLocaleUtils.setLocale(LOCALE_UKRAINIAN);
        assertEquals("\u0406", getLabel(UKRAINIAN_NAME));
        // ICU 55 has a bug whereby these letters have buckets created only if
        // Ukrainian is the primary language. Once this is fixed also test
        // these labels when in English locale.
        assertEquals("\u0407", getLabel(UKRAINIAN_NAME_2));
        assertEquals("\u0490", getLabel(UKRAINIAN_NAME_3));
        assertEquals("B", getLabel("Bob Smith"));
    }

    public void testGermanContactLocaleUtils() throws Exception {
        if (!hasGermanCollator) {
            return;
        }

        ContactLocaleUtils.setLocale(Locale.GERMANY);
        assertEquals("S", getLabel("Sacher"));

        // ICU 51 has labels Sch and St. These were removed in ICU 52
        assertEquals("S", getLabel("Schiller"));
        assertEquals("S", getLabel("Steiff"));
        verifyLabels(getLabels(), LABELS_EN_US);
    }

    private void verifyKeys(final Iterator<String> resultKeys, final String[] expectedKeys)
            throws Exception {
        HashSet<String> allKeys = new HashSet<String>();
        while (resultKeys.hasNext()) {
            allKeys.add(resultKeys.next());
        }
        assertEquals(new HashSet<String>(Arrays.asList(expectedKeys)), allKeys);
    }

    // Verify that the initial set of resultLabels matches the expectedLabels.
    // Ignore the (large) number of secondary locale labels that make up the
    // tail labels in the result set right before the final "#" and "" buckets.
    private void verifyLabels(final ArrayList<String> resultLabels,
            final String[] expectedLabels) throws Exception {
        final List<String> expectedLabelList = Arrays.asList(expectedLabels);
        final int numLabels = expectedLabelList.size() - 2;
        assertEquals(expectedLabelList.subList(0, numLabels),
                resultLabels.subList(0, numLabels));
    }
}
