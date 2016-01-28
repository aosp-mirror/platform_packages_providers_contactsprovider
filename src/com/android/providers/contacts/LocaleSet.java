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
 * limitations under the License
 */

package com.android.providers.contacts;

import android.annotation.NonNull;
import android.annotation.Nullable;
import android.text.TextUtils;
import android.util.LocaleList;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;

public class LocaleSet {
    private final Locale mDefaultLocaleOverrideForTest;
    private final LocaleList mLocaleList;

    private LocaleSet(LocaleList localeList, Locale defaultLocaleOverrideForTest) {
        mLocaleList = localeList;
        mDefaultLocaleOverrideForTest = defaultLocaleOverrideForTest;
    }

    public static LocaleSet newDefault() {
        return new LocaleSet(LocaleList.getDefault(),
                /*defaultLocaleOverrideForTest= */ null);
    }

    public static LocaleSet newForTest(Locale... locales) {
        return new LocaleSet(new LocaleList(locales), locales[0]);
    }

    private static boolean areLanguagesEqual(@Nullable Locale locale1, @Nullable Locale locale2) {
        if (locale1 == locale2) {
            return true;
        }
        if (locale1 == null || locale2 == null) {
            return false;
        }
        return TextUtils.equals(locale1.getLanguage(), locale2.getLanguage());
    }

    @VisibleForTesting
    static boolean isLanguageChinese(@Nullable Locale locale) {
        return areLanguagesEqual(Locale.CHINESE, locale);
    }

    @VisibleForTesting
    static boolean isLanguageJapanese(@Nullable Locale locale) {
        return areLanguagesEqual(Locale.JAPANESE, locale);
    }

    @VisibleForTesting
    static boolean isLanguageKorean(@Nullable Locale locale) {
        return areLanguagesEqual(Locale.KOREAN, locale);
    }

    @VisibleForTesting
    static boolean isLocaleCJK(@Nullable Locale locale) {
        return isLanguageChinese(locale) ||
                isLanguageJapanese(locale) ||
                isLanguageKorean(locale);
    }

    private static final String SCRIPT_SIMPLIFIED_CHINESE = "Hans";
    private static final String SCRIPT_TRADITIONAL_CHINESE = "Hant";

    @VisibleForTesting
    static boolean isLocaleSimplifiedChinese(@Nullable Locale locale) {
        // language must match
        if (!areLanguagesEqual(Locale.CHINESE, locale)) {
            return false;
        }
        // script is optional but if present must match
        if (!TextUtils.isEmpty(locale.getScript())) {
            return locale.getScript().equals(SCRIPT_SIMPLIFIED_CHINESE);
        }
        // if no script, must match known country
        return locale.equals(Locale.SIMPLIFIED_CHINESE);
    }

    @VisibleForTesting
    static boolean isLocaleTraditionalChinese(@Nullable Locale locale) {
        // language must match
        if (!areLanguagesEqual(Locale.CHINESE, locale)) {
            return false;
        }
        // script is optional but if present must match
        if (!TextUtils.isEmpty(locale.getScript())) {
            return locale.getScript().equals(SCRIPT_TRADITIONAL_CHINESE);
        }
        // if no script, must match known country
        return locale.equals(Locale.TRADITIONAL_CHINESE);
    }

    /**
     * Returns the primary locale, which may not be the first item of {@link #getAllLocales}.
     * (See {@link LocaleList})
     */
    public @NonNull Locale getPrimaryLocale() {
        if (mDefaultLocaleOverrideForTest != null) {
            return mDefaultLocaleOverrideForTest;
        }
        return Locale.getDefault();
    }

    public @NonNull LocaleList getAllLocales() {
        return mLocaleList;
    }

    public boolean isPrimaryLocaleCJK() {
        return isLocaleCJK(getPrimaryLocale());
    }

    /**
     * @return true if Japanese is found in the list before simplified Chinese.
     */
    public boolean shouldPreferJapanese() {
        if (isLanguageJapanese(getPrimaryLocale())) {
            return true;
        }
        for (int i = 0; i < mLocaleList.size(); i++) {
            final Locale l = mLocaleList.get(i);
            if (isLanguageJapanese(l)) {
                return true;
            }
            if (isLocaleSimplifiedChinese(l)) {
                return false;
            }
        }
        return false;
    }

    /**
     * @return true if simplified Chinese is found before Japanese or traditional Chinese.
     */
    public boolean shouldPreferSimplifiedChinese() {
        if (isLocaleSimplifiedChinese(getPrimaryLocale())) {
            return true;
        }
        for (int i = 0; i < mLocaleList.size(); i++) {
            final Locale l = mLocaleList.get(i);
            if (isLocaleSimplifiedChinese(l)) {
                return true;
            }
            if (isLanguageJapanese(l)) {
                return false;
            }
            if (isLocaleTraditionalChinese(l)) { // Traditional chinese wins here.
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof LocaleSet) {
            final LocaleSet other = (LocaleSet) object;
            return mLocaleList.equals(other.mLocaleList);
        }
        return false;
    }

    @Override
    public final String toString() {
        return mLocaleList.toString();
    }
}
