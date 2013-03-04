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
 * limitations under the License
 */

package com.android.providers.contacts;

import android.provider.ContactsContract.FullNameStyle;
import android.util.Log;

import com.android.providers.contacts.HanziToPinyin.Token;

import java.lang.Character.UnicodeBlock;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import libcore.icu.AlphabeticIndex;
import libcore.icu.AlphabeticIndex.ImmutableIndex;

/**
 * This utility class provides specialized handling for locale specific
 * information: labels, name lookup keys.
 */
public class ContactLocaleUtils {
    public static final String TAG = "ContactLocale";

    /**
     * This class is the default implementation and should be the base class
     * for other locales.
     *
     * sortKey: same as name
     * nameLookupKeys: none
     * labels: uses ICU AlphabeticIndex for labels and extends by labeling
     *     phone numbers "#".  Eg English labels are: [A-Z], #, " "
     */
    private static class ContactLocaleUtilsBase {
        private static final String EMPTY_STRING = "";
        private static final String NUMBER_STRING = "#";

        protected final ImmutableIndex mAlphabeticIndex;
        private final int mAlphabeticIndexBucketCount;
        private final int mNumberBucketIndex;

        public ContactLocaleUtilsBase(Locale locale) {
            mAlphabeticIndex = new AlphabeticIndex(locale)
                .addLabels(Locale.US).getImmutableIndex();
            mAlphabeticIndexBucketCount = mAlphabeticIndex.getBucketCount();
            mNumberBucketIndex = mAlphabeticIndexBucketCount - 1;
        }

        public String getSortKey(String name) {
            return name;
        }

        /**
         * Returns the bucket index for the specified string. AlphabeticIndex
         * sorts strings into buckets numbered in order from 0 to N, where the
         * exact value of N depends on how many representative index labels are
         * used in a particular locale. This routine adds one additional bucket
         * for phone numbers. It attempts to detect phone numbers and shifts
         * the bucket indexes returned by AlphabeticIndex in order to make room
         * for the new # bucket, so the returned range becomes 0 to N+1.
         */
        public int getBucketIndex(String name) {
            boolean prefixIsNumeric = false;
            final int length = name.length();
            int offset = 0;
            while (offset < length) {
                int codePoint = Character.codePointAt(name, offset);
                // Ignore standard phone number separators and identify any
                // string that otherwise starts with a number.
                if (Character.isDigit(codePoint)) {
                    prefixIsNumeric = true;
                    break;
                } else if (!Character.isSpaceChar(codePoint) &&
                           codePoint != '+' && codePoint != '(' &&
                           codePoint != ')' && codePoint != '.' &&
                           codePoint != '-' && codePoint != '#') {
                    break;
                }
                offset += Character.charCount(codePoint);
            }
            if (prefixIsNumeric) {
                return mNumberBucketIndex;
            }

            final int bucket = mAlphabeticIndex.getBucketIndex(name);
            if (bucket < 0) {
                return -1;
            }
            if (bucket >= mNumberBucketIndex) {
                return bucket + 1;
            }
            return bucket;
        }

        /**
         * Returns the number of buckets in use (one more than AlphabeticIndex
         * uses, because this class adds a bucket for phone numbers).
         */
        public int getBucketCount() {
            return mAlphabeticIndexBucketCount + 1;
        }

        /**
         * Returns the label for the specified bucket index if a valid index,
         * otherwise returns an empty string. '#' is returned for the phone
         * number bucket; for all others, the AlphabeticIndex label is returned.
         */
        public String getBucketLabel(int bucketIndex) {
            if (bucketIndex < 0 || bucketIndex >= getBucketCount()) {
                return EMPTY_STRING;
            } else if (bucketIndex == mNumberBucketIndex) {
                return NUMBER_STRING;
            } else if (bucketIndex > mNumberBucketIndex) {
                --bucketIndex;
            }
            return mAlphabeticIndex.getBucketLabel(bucketIndex);
        }

        @SuppressWarnings("unused")
        public Iterator<String> getNameLookupKeys(String name) {
            return null;
        }

        public ArrayList<String> getLabels() {
            final int bucketCount = getBucketCount();
            final ArrayList<String> labels = new ArrayList<String>(bucketCount);
            for(int i = 0; i < bucketCount; ++i) {
                labels.add(getBucketLabel(i));
            }
            return labels;
        }
    }

    /**
     * Japanese specific locale overrides.
     *
     * sortKey: unchanged (same as name)
     * nameLookupKeys: unchanged (none)
     * labels: extends default labels by labeling unlabeled CJ characters
     *     with the Japanese character 他 ("misc"). Japanese labels are:
     *     あ, か, さ, た, な, は, ま, や, ら, わ, 他, [A-Z], #, " "
     */
    private static class JapaneseContactUtils extends ContactLocaleUtilsBase {
        // \u4ed6 is Japanese character 他 ("misc")
        private static final String JAPANESE_MISC_LABEL = "\u4ed6";
        private final int mMiscBucketIndex;

        public JapaneseContactUtils(Locale locale) {
            super(locale);
            // Determine which bucket AlphabeticIndex is lumping unclassified
            // Japanese characters into by looking up the bucket index for
            // a representative Kanji/CJK unified ideograph (\u65e5 is the
            // character '日').
            mMiscBucketIndex = super.getBucketIndex("\u65e5");
        }

        // Set of UnicodeBlocks for unified CJK (Chinese) characters and
        // Japanese characters. This includes all code blocks that might
        // contain a character used in Japanese (which is why unified CJK
        // blocks are included but Korean Hangul and jamo are not).
        private static final Set<Character.UnicodeBlock> CJ_BLOCKS;
        static {
            Set<UnicodeBlock> set = new HashSet<UnicodeBlock>();
            set.add(UnicodeBlock.HIRAGANA);
            set.add(UnicodeBlock.KATAKANA);
            set.add(UnicodeBlock.KATAKANA_PHONETIC_EXTENSIONS);
            set.add(UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS);
            set.add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS);
            set.add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A);
            set.add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B);
            set.add(UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION);
            set.add(UnicodeBlock.CJK_RADICALS_SUPPLEMENT);
            set.add(UnicodeBlock.CJK_COMPATIBILITY);
            set.add(UnicodeBlock.CJK_COMPATIBILITY_FORMS);
            set.add(UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS);
            set.add(UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT);
            CJ_BLOCKS = Collections.unmodifiableSet(set);
        }

        /**
         * Helper routine to identify unlabeled Chinese or Japanese characters
         * to put in a 'misc' bucket.
         *
         * @return true if the specified Unicode code point is Chinese or
         *              Japanese
         */
        private static boolean isChineseOrJapanese(int codePoint) {
            return CJ_BLOCKS.contains(UnicodeBlock.of(codePoint));
        }

        /**
         * Returns the bucket index for the specified string. Adds an
         * additional 'misc' bucket for Kanji characters to the base class set.
         */
        @Override
        public int getBucketIndex(String name) {
            final int bucketIndex = super.getBucketIndex(name);
            if ((bucketIndex == mMiscBucketIndex &&
                 !isChineseOrJapanese(Character.codePointAt(name, 0))) ||
                bucketIndex > mMiscBucketIndex) {
                return bucketIndex + 1;
            }
            return bucketIndex;
        }

        /**
         * Returns the number of buckets in use (one more than the base class
         * uses, because this class adds a bucket for Kanji).
         */
        @Override
        public int getBucketCount() {
            return super.getBucketCount() + 1;
        }

        /**
         * Returns the label for the specified bucket index if a valid index,
         * otherwise returns an empty string. '他' is returned for unclassified
         * Kanji; for all others, the label determined by the base class is
         * returned.
         */
        @Override
        public String getBucketLabel(int bucketIndex) {
            if (bucketIndex == mMiscBucketIndex) {
                return JAPANESE_MISC_LABEL;
            } else if (bucketIndex > mMiscBucketIndex) {
                --bucketIndex;
            }
            return super.getBucketLabel(bucketIndex);
        }
    }

    /**
     * Chinese specific locale overrides. Uses ICU Transliterator for
     * generating pinyin transliteration.
     *
     * sortKey: unchanged (same as name)
     * nameLookupKeys: adds additional name lookup keys
     *     - Chinese character's pinyin and pinyin's initial character.
     *     - Latin word and initial character.
     * labels: unchanged
     *     Simplified Chinese labels are the same as English: [A-Z], #, " "
     *     Traditional Chinese labels are stroke count, then English labels:
     *         [1-33, 35, 36, 48]劃, [A-Z], #, " "
     */
    private static class ChineseContactUtils extends ContactLocaleUtilsBase {
        public ChineseContactUtils(Locale locale) {
            super(locale);
        }

        @Override
        public Iterator<String> getNameLookupKeys(String name) {
            return getPinyinNameLookupKeys(name);
        }

        public static Iterator<String> getPinyinNameLookupKeys(String name) {
            // TODO : Reduce the object allocation.
            HashSet<String> keys = new HashSet<String>();
            ArrayList<Token> tokens = HanziToPinyin.getInstance().get(name);
            final int tokenCount = tokens.size();
            final StringBuilder keyPinyin = new StringBuilder();
            final StringBuilder keyInitial = new StringBuilder();
            // There is no space among the Chinese Characters, the variant name
            // lookup key wouldn't work for Chinese. The keyOriginal is used to
            // build the lookup keys for itself.
            final StringBuilder keyOriginal = new StringBuilder();
            for (int i = tokenCount - 1; i >= 0; i--) {
                final Token token = tokens.get(i);
                if (Token.PINYIN == token.type) {
                    keyPinyin.insert(0, token.target);
                    keyInitial.insert(0, token.target.charAt(0));
                } else if (Token.LATIN == token.type) {
                    // Avoid adding space at the end of String.
                    if (keyPinyin.length() > 0) {
                        keyPinyin.insert(0, ' ');
                    }
                    if (keyOriginal.length() > 0) {
                        keyOriginal.insert(0, ' ');
                    }
                    keyPinyin.insert(0, token.source);
                    keyInitial.insert(0, token.source.charAt(0));
                }
                keyOriginal.insert(0, token.source);
                keys.add(keyOriginal.toString());
                keys.add(keyPinyin.toString());
                keys.add(keyInitial.toString());
            }
            return keys.iterator();
        }
    }

    private static final String CHINESE_LANGUAGE = Locale.CHINESE.getLanguage().toLowerCase();
    private static final String JAPANESE_LANGUAGE = Locale.JAPANESE.getLanguage().toLowerCase();
    private static final String KOREAN_LANGUAGE = Locale.KOREAN.getLanguage().toLowerCase();

    private static ContactLocaleUtils sSingleton;

    private final Locale mLocale;
    private final String mLanguage;
    private final ContactLocaleUtilsBase mUtils;

    private ContactLocaleUtils(Locale locale) {
        if (locale == null) {
            mLocale = Locale.getDefault();
        } else {
            mLocale = locale;
        }
        mLanguage = mLocale.getLanguage().toLowerCase();
        if (mLanguage.equals(JAPANESE_LANGUAGE)) {
            mUtils = new JapaneseContactUtils(mLocale);
        } else if (mLanguage.equals(CHINESE_LANGUAGE)) {
            mUtils = new ChineseContactUtils(mLocale);
        } else {
            mUtils = new ContactLocaleUtilsBase(mLocale);
        }
        Log.i(TAG, "AddressBook Labels [" + mLocale.toString() + "]: "
              + getLabels().toString());
    }

    public boolean isLocale(Locale locale) {
        return mLocale.equals(locale);
    }

    public static synchronized ContactLocaleUtils getInstance() {
        if (sSingleton == null) {
            sSingleton = new ContactLocaleUtils(null);
        }
        return sSingleton;
    }

    public static synchronized void setLocale(Locale locale) {
        if (sSingleton == null || !sSingleton.isLocale(locale)) {
            sSingleton = new ContactLocaleUtils(locale);
        }
    }

    public String getSortKey(String name, int nameStyle) {
        return mUtils.getSortKey(name);
    }

    public int getBucketIndex(String name) {
        return mUtils.getBucketIndex(name);
    }

    public int getBucketCount() {
        return mUtils.getBucketCount();
    }

    public String getBucketLabel(int bucketIndex) {
        return mUtils.getBucketLabel(bucketIndex);
    }

    public String getLabel(String name) {
        return getBucketLabel(getBucketIndex(name));
    }

    public ArrayList<String> getLabels() {
        return mUtils.getLabels();
    }

    /**
     *  Determine which utility should be used for generating NameLookupKey.
     *  (ie, whether we generate Pinyin lookup keys or not)
     *
     *  a. For unclassified CJK name, if current locale language is neither
     *     Japanese nor Korean, use ChineseContactUtils.
     *  b. If we're sure this is a Chinese name, always use ChineseContactUtils.
     *  c. Otherwise, use whichever ContactUtils are appropriate for the locale
     *     (so, Western names in Chinese locale will use ChineseContactUtils)
     */
    public Iterator<String> getNameLookupKeys(String name, int nameStyle) {
        if (nameStyle == FullNameStyle.CJK &&
            !JAPANESE_LANGUAGE.equals(mLanguage) &&
            !KOREAN_LANGUAGE.equals(mLanguage)) {
            return ChineseContactUtils.getPinyinNameLookupKeys(name);
        }
        if (nameStyle == FullNameStyle.CHINESE) {
            return ChineseContactUtils.getPinyinNameLookupKeys(name);
        }
        return mUtils.getNameLookupKeys(name);
    }

}
