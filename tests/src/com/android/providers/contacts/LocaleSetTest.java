/*
 * Copyright (C) 2014 The Android Open Source Project
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
import java.util.Locale;
import junit.framework.TestCase;

@SmallTest
public class LocaleSetTest extends TestCase {
    public void testPrimaryLocale() {
        final Locale previousDefault = Locale.getDefault();
        try {
            assertEquals(Locale.CANADA, LocaleSet.newForTest(Locale.CANADA).getPrimaryLocale());
            assertEquals(Locale.GERMAN, LocaleSet.newForTest(Locale.GERMAN).getPrimaryLocale());
            assertEquals(Locale.GERMAN, LocaleSet.newForTest(Locale.GERMAN, Locale.CANADA)
                    .getPrimaryLocale());

            assertEquals(Locale.getDefault(), LocaleSet.newDefault().getPrimaryLocale());

            Locale.setDefault(Locale.JAPANESE);

            assertEquals(Locale.JAPANESE, LocaleSet.newDefault().getPrimaryLocale());
        } finally {
            Locale.setDefault(previousDefault);
        }
    }

    public void testIsLanguageChinese() {
        assertTrue(LocaleSet.isLanguageChinese(Locale.CHINESE));
        assertTrue(LocaleSet.isLanguageChinese(Locale.TRADITIONAL_CHINESE));
        assertTrue(LocaleSet.isLanguageChinese(Locale.SIMPLIFIED_CHINESE));

        assertFalse(LocaleSet.isLanguageChinese(Locale.JAPANESE));
        assertFalse(LocaleSet.isLanguageChinese(Locale.ENGLISH));
    }

    public void testGetScriptIfChinese() {
        assertEquals("Hans", LocaleSet.getScriptIfChinese(Locale.forLanguageTag("zh")));
        assertEquals("Hans", LocaleSet.getScriptIfChinese(Locale.forLanguageTag("zh-SG")));

        assertEquals("Hant", LocaleSet.getScriptIfChinese(Locale.forLanguageTag("zh-TW")));
        assertEquals("Hant", LocaleSet.getScriptIfChinese(Locale.forLanguageTag("zh-HK")));

        assertEquals(null, LocaleSet.getScriptIfChinese(Locale.ENGLISH));
        assertEquals(null, LocaleSet.getScriptIfChinese(Locale.forLanguageTag("ja")));
        assertEquals(null, LocaleSet.getScriptIfChinese(Locale.forLanguageTag("ja-JP")));
    }

    public void testIsLocaleSimplifiedChinese() {
        assertTrue(LocaleSet.isLocaleSimplifiedChinese(Locale.SIMPLIFIED_CHINESE));
        assertTrue(LocaleSet.isLocaleSimplifiedChinese(Locale.forLanguageTag("zh-SG")));

        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.TRADITIONAL_CHINESE));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.CHINESE));
        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.JAPANESE));
        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.JAPAN));
        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.KOREA));
        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.KOREAN));
        assertFalse(LocaleSet.isLocaleSimplifiedChinese(Locale.ENGLISH));
    }

    public void testIsLocaleTraditionalChinese() {
        assertTrue(LocaleSet.isLocaleTraditionalChinese(Locale.TRADITIONAL_CHINESE));
        assertTrue(LocaleSet.isLocaleTraditionalChinese(Locale.forLanguageTag("zh-TW")));

        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.SIMPLIFIED_CHINESE));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.CHINESE));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.JAPANESE));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.JAPAN));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.KOREA));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.KOREAN));
        assertFalse(LocaleSet.isLocaleTraditionalChinese(Locale.ENGLISH));
    }

    public void testIsLanguageJapanese() {
        assertTrue(LocaleSet.isLanguageJapanese(Locale.JAPANESE));
        assertTrue(LocaleSet.isLanguageJapanese(Locale.JAPAN));

        assertFalse(LocaleSet.isLanguageJapanese(Locale.TRADITIONAL_CHINESE));
        assertFalse(LocaleSet.isLanguageJapanese(Locale.SIMPLIFIED_CHINESE));
        assertFalse(LocaleSet.isLanguageJapanese(Locale.ENGLISH));
    }

    public void testIsLanguageKorean() {
        assertTrue(LocaleSet.isLanguageKorean(Locale.KOREAN));
        assertTrue(LocaleSet.isLanguageKorean(Locale.KOREA));

        assertFalse(LocaleSet.isLanguageKorean(Locale.TRADITIONAL_CHINESE));
        assertFalse(LocaleSet.isLanguageKorean(Locale.SIMPLIFIED_CHINESE));
        assertFalse(LocaleSet.isLanguageKorean(Locale.JAPANESE));
        assertFalse(LocaleSet.isLanguageKorean(Locale.JAPAN));
        assertFalse(LocaleSet.isLanguageKorean(Locale.ENGLISH));
    }

    public void testIsLocaleCJK() {
        assertTrue(LocaleSet.isLocaleCJK(Locale.TRADITIONAL_CHINESE));
        assertTrue(LocaleSet.isLocaleCJK(Locale.SIMPLIFIED_CHINESE));
        assertTrue(LocaleSet.isLocaleCJK(Locale.JAPANESE));
        assertTrue(LocaleSet.isLocaleCJK(Locale.JAPAN));
        assertTrue(LocaleSet.isLocaleCJK(Locale.KOREA));
        assertTrue(LocaleSet.isLocaleCJK(Locale.KOREAN));

        assertFalse(LocaleSet.isLocaleCJK(Locale.ENGLISH));
    }

    public void testShouldPreferJapanese() {
        assertFalse(LocaleSet.newForTest(Locale.ENGLISH)
                .shouldPreferJapanese());

        assertTrue(LocaleSet.newForTest(Locale.JAPAN)
                .shouldPreferJapanese());
        assertTrue(LocaleSet.newForTest(Locale.ENGLISH, Locale.KOREAN, Locale.JAPAN)
                .shouldPreferJapanese());
        assertTrue(LocaleSet.newForTest(Locale.JAPAN, Locale.TRADITIONAL_CHINESE)
                .shouldPreferJapanese());
        assertTrue(LocaleSet.newForTest(Locale.JAPAN, Locale.SIMPLIFIED_CHINESE)
                .shouldPreferJapanese());
        assertFalse(LocaleSet.newForTest(Locale.TRADITIONAL_CHINESE, Locale.JAPAN)
                .shouldPreferJapanese());

        // Simplified Chinese wins.
        assertFalse(LocaleSet.newForTest(Locale.SIMPLIFIED_CHINESE, Locale.JAPAN)
                .shouldPreferJapanese());
        assertFalse(LocaleSet.newForTest(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.JAPAN)
                .shouldPreferJapanese());
    }

    public void testShouldPreferSimplifiedChinese() {
        assertFalse(LocaleSet.newForTest(Locale.ENGLISH)
                .shouldPreferSimplifiedChinese());
        assertFalse(LocaleSet.newForTest(Locale.TRADITIONAL_CHINESE)
                .shouldPreferSimplifiedChinese());

        assertTrue(LocaleSet.newForTest(Locale.SIMPLIFIED_CHINESE)
                .shouldPreferSimplifiedChinese());
        assertTrue(LocaleSet.newForTest(Locale.ENGLISH, Locale.KOREAN, Locale.SIMPLIFIED_CHINESE)
                .shouldPreferSimplifiedChinese());
        assertTrue(LocaleSet.newForTest(Locale.SIMPLIFIED_CHINESE, Locale.JAPANESE)
                .shouldPreferSimplifiedChinese());
        assertTrue(LocaleSet.newForTest(Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)
                .shouldPreferSimplifiedChinese());

        // Simplified Chinese wins.
        assertFalse(LocaleSet.newForTest(Locale.JAPAN, Locale.SIMPLIFIED_CHINESE)
                .shouldPreferSimplifiedChinese());
        assertFalse(LocaleSet.newForTest(Locale.TRADITIONAL_CHINESE, Locale.SIMPLIFIED_CHINESE)
                .shouldPreferSimplifiedChinese());
    }
}
