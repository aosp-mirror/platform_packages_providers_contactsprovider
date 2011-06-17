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

import com.android.providers.contacts.HanziToPinyin.Token;

import android.provider.ContactsContract.FullNameStyle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

/**
 * This utility class provides customized sort key and name lookup key according the locale.
 */
public class ContactLocaleUtils {

    /**
     * This class is the default implementation.
     * <p>
     * It should be the base class for other locales' implementation.
     */
    public class ContactLocaleUtilsBase {
        public String getSortKey(String displayName) {
            return displayName;
        }
        public Iterator<String> getNameLookupKeys(String name) {
            return null;
        }
    }

    /**
     * The classes to generate the Chinese style sort and search keys.
     * <p>
     * The sorting key is generated as each Chinese character' pinyin proceeding with
     * space and character itself. If the character's pinyin unable to find, the character
     * itself will be used.
     * <p>
     * The below additional name lookup keys will be generated.
     * a. Chinese character's pinyin and pinyin's initial character.
     * b. Latin word and the initial character for Latin word.
     * The name lookup keys are generated to make sure the name can be found by from any
     * initial character.
     */
    private class ChineseContactUtils extends ContactLocaleUtilsBase {
        @Override
        public String getSortKey(String displayName) {
            ArrayList<Token> tokens = HanziToPinyin.getInstance().get(displayName);
            if (tokens != null && tokens.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Token token : tokens) {
                    // Put Chinese character's pinyin, then proceed with the
                    // character itself.
                    if (Token.PINYIN == token.type) {
                        if (sb.length() > 0) {
                            sb.append(' ');
                        }
                        sb.append(token.target);
                        sb.append(' ');
                        sb.append(token.source);
                    } else {
                        if (sb.length() > 0) {
                            sb.append(' ');
                        }
                        sb.append(token.source);
                    }
                }
                return sb.toString();
            }
            return super.getSortKey(displayName);
        }

        @Override
        public Iterator<String> getNameLookupKeys(String name) {
            // TODO : Reduce the object allocation.
            HashSet<String> keys = new HashSet<String>();
            ArrayList<Token> tokens = HanziToPinyin.getInstance().get(name);
            final int tokenCount = tokens.size();
            final StringBuilder keyPinyin = new StringBuilder();
            final StringBuilder keyInitial = new StringBuilder();
            // There is no space among the Chinese Characters, the variant name
            // lookup key wouldn't work for Chinese. The keyOrignal is used to
            // build the lookup keys for itself.
            final StringBuilder keyOrignal = new StringBuilder();
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
                    if (keyOrignal.length() > 0) {
                        keyOrignal.insert(0, ' ');
                    }
                    keyPinyin.insert(0, token.source);
                    keyInitial.insert(0, token.source.charAt(0));
                }
                keyOrignal.insert(0, token.source);
                keys.add(keyOrignal.toString());
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
    private HashMap<Integer, ContactLocaleUtilsBase> mUtils =
            new HashMap<Integer, ContactLocaleUtilsBase>();

    private ContactLocaleUtilsBase mBase = new ContactLocaleUtilsBase();

    private String mLanguage;

    private ContactLocaleUtils() {
        setLocale(null);
    }

    public void setLocale(Locale currentLocale) {
        if (currentLocale == null) {
            mLanguage = Locale.getDefault().getLanguage().toLowerCase();
        } else {
            mLanguage = currentLocale.getLanguage().toLowerCase();
        }
    }

    public String getSortKey(String displayName, int nameStyle) {
        return getForSort(Integer.valueOf(nameStyle)).getSortKey(displayName);
    }

    public Iterator<String> getNameLookupKeys(String name, int nameStyle) {
        return getForNameLookup(Integer.valueOf(nameStyle)).getNameLookupKeys(name);
    }

    /**
     *  Determine which utility should be used for generating NameLookupKey.
     *  <p>
     *  a. For Western style name, if the current language is Chinese, the
     *     ChineseContactUtils should be used.
     *  b. For Chinese and CJK style name if current language is neither Japanese or Korean,
     *     the ChineseContactUtils should be used.
     */
    private ContactLocaleUtilsBase getForNameLookup(Integer nameStyle) {
        int nameStyleInt = nameStyle.intValue();
        Integer adjustedUtil = Integer.valueOf(getAdjustedStyle(nameStyleInt));
        if (CHINESE_LANGUAGE.equals(mLanguage) && nameStyleInt == FullNameStyle.WESTERN) {
            adjustedUtil = Integer.valueOf(FullNameStyle.CHINESE);
        }
        return get(adjustedUtil);
    }

    private synchronized ContactLocaleUtilsBase get(Integer nameStyle) {
        ContactLocaleUtilsBase utils = mUtils.get(nameStyle);
        if (utils == null) {
            if (nameStyle.intValue() == FullNameStyle.CHINESE) {
                utils = new ChineseContactUtils();
                mUtils.put(nameStyle, utils);
            }
        }
        return (utils == null) ? mBase: utils;
    }

    /**
     *  Determine the which utility should be used for generating sort key.
     *  <p>
     *  For Chinese and CJK style name if current language is neither Japanese or Korean,
     *  the ChineseContactUtils should be used.
     */
    private ContactLocaleUtilsBase getForSort(Integer nameStyle) {
        return get(Integer.valueOf(getAdjustedStyle(nameStyle.intValue())));
    }

    public static synchronized ContactLocaleUtils getIntance() {
        if (sSingleton == null) {
            sSingleton = new ContactLocaleUtils();
        }
        return sSingleton;
    }

    private int getAdjustedStyle(int nameStyle) {
        if (nameStyle == FullNameStyle.CJK  && !JAPANESE_LANGUAGE.equals(mLanguage) &&
                !KOREAN_LANGUAGE.equals(mLanguage)) {
            return FullNameStyle.CHINESE;
        } else {
            return nameStyle;
        }
    }
}
