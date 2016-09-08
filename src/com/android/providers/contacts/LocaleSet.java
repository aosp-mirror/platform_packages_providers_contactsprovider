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
import android.icu.util.ULocale;
import android.os.LocaleList;
import android.text.TextUtils;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.Objects;

public class LocaleSet {
    private static final String SCRIPT_SIMPLIFIED_CHINESE = "Hans";
    private static final String SCRIPT_TRADITIONAL_CHINESE = "Hant";

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

    @VisibleForTesting
    public static LocaleSet newForTest(Locale... locales) {
        return new LocaleSet(new LocaleList(locales), locales[0]);
    }

    @VisibleForTesting
    static boolean isLanguageChinese(@Nullable Locale locale) {
        return locale != null && "zh".equals(locale.getLanguage());
    }

    @VisibleForTesting
    static boolean isLanguageJapanese(@Nullable Locale locale) {
        return locale != null && "ja".equals(locale.getLanguage());
    }

    @VisibleForTesting
    static boolean isLanguageKorean(@Nullable Locale locale) {
        return locale != null && "ko".equals(locale.getLanguage());
    }

    @VisibleForTesting
    static boolean isLocaleCJK(@Nullable Locale locale) {
        return isLanguageChinese(locale) ||
                isLanguageJapanese(locale) ||
                isLanguageKorean(locale);
    }

    private static String getLikelyScript(Locale locale) {
        final String script = locale.getScript();
        if (!script.isEmpty()) {
            return script;
        } else {
            return ULocale.addLikelySubtags(ULocale.forLocale(locale)).getScript();
        }
    }

    /**
     * @return the script if the language is Chinese, and otherwise null.
     */
    @VisibleForTesting
    static String getScriptIfChinese(@Nullable Locale locale) {
        return isLanguageChinese(locale) ? getLikelyScript(locale) : null;
    }

    static boolean isLocaleSimplifiedChinese(@Nullable Locale locale) {
        return SCRIPT_SIMPLIFIED_CHINESE.equals(getScriptIfChinese(locale));
    }

    @VisibleForTesting
    static boolean isLocaleTraditionalChinese(@Nullable Locale locale) {
        return SCRIPT_TRADITIONAL_CHINESE.equals(getScriptIfChinese(locale));
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
            if (isLanguageChinese(l)) {
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

    /**
     * @return true if the instance contains the current system locales.
     */
    public boolean isCurrent() {
        return Objects.equals(mLocaleList, LocaleList.getDefault());
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
