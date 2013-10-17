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
    private void testLocaleStringsHelper(Locale primaryLocale,
            Locale secondaryLocale, final String expectedString) throws Exception {
        final LocaleSet locales = new LocaleSet(primaryLocale, secondaryLocale);
        final String localeString = locales.toString();
        assertEquals(expectedString, localeString);

        final LocaleSet parseLocales = LocaleSet.getLocaleSet(localeString);
        assertEquals(locales, parseLocales);
    }

    @SmallTest
    public void testLocaleStrings() throws Exception {
        testLocaleStringsHelper(Locale.US, null, "en-US");
        testLocaleStringsHelper(Locale.US, Locale.CHINA, "en-US;zh-CN");
        testLocaleStringsHelper(Locale.JAPAN, Locale.GERMANY, "ja-JP;de-DE");
    }

    private void testNormalizationHelper(String localeString,
            Locale expectedPrimary, Locale expectedSecondary) throws Exception {
        final LocaleSet expected = new LocaleSet(expectedPrimary, expectedSecondary);
        final LocaleSet actual = LocaleSet.getLocaleSet(localeString).normalize();
        assertEquals(expected, actual);
    }

    @SmallTest
    public void testNormalization() throws Exception {
        // Single locale
        testNormalizationHelper("en-US", Locale.US, null);
        // Disallow secondary with same language as primary
        testNormalizationHelper("fr-CA;fr-FR", Locale.CANADA_FRENCH, null);
        testNormalizationHelper("en-US;zh-CN", Locale.US, Locale.CHINA);
        // Disallow both locales CJK
        testNormalizationHelper("ja-JP;zh-CN", Locale.JAPAN, null);
        // Disallow en as secondary (happens by default)
        testNormalizationHelper("zh-CN;en-US", Locale.CHINA, null);
        testNormalizationHelper("zh-CN;de-DE", Locale.CHINA, Locale.GERMANY);
    }
}
