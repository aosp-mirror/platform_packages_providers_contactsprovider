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

import java.text.Collator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

import com.android.providers.contacts.ContactLocaleUtils;

public class ContactLocaleUtilsTest extends AndroidTestCase {
    private static final String LATIN_NAME = "John Smith";
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


    private ContactLocaleUtils mContactLocaleUtils = ContactLocaleUtils.getIntance();

    public void testContactLocaleUtilsBase() throws Exception {
        assertEquals(mContactLocaleUtils.getSortKey(LATIN_NAME, FullNameStyle.UNDEFINED),
                LATIN_NAME);
        assertNull(mContactLocaleUtils.getNameLookupKeys(LATIN_NAME,
                FullNameStyle.UNDEFINED));
    }

    public void testChineseContactLocaleUtils() throws Exception {
        boolean hasChineseCollator = false;
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.CHINA)) {
                hasChineseCollator = true;
                break;
            }
        }

        if (!hasChineseCollator) {
            return;
        }

        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_NAME,
                FullNameStyle.CHINESE).equalsIgnoreCase("DU \u675C JUAN \u9D51"));

        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_LATIN_MIX_NAME_1,
                FullNameStyle.CHINESE).equalsIgnoreCase("d DU \u675C JUAN \u9D51"));

        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_LATIN_MIX_NAME_2,
                FullNameStyle.CHINESE).equalsIgnoreCase("mary DU \u675C JUAN \u9D51"));

        Iterator<String> keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);

        keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_LATIN_MIX_NAME_1,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_1_KEY);

        keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_LATIN_MIX_NAME_2,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_LATIN_MIX_NAME_2_KEY);
    }

    private void verifyKeys(final Iterator<String> resultKeys, final String[] expectedKeys)
            throws Exception {
        HashSet<String> allKeys = new HashSet<String>();
        while (resultKeys.hasNext()) {
            allKeys.add(resultKeys.next());
        }
        assertEquals(allKeys, new HashSet<String>(Arrays.asList(expectedKeys)));
    }

    private void testChineseStyleNameWithDifferentLocale() throws Exception {
        mContactLocaleUtils.setLocale(Locale.ENGLISH);
        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_NAME,
                FullNameStyle.CHINESE).equalsIgnoreCase("DU \u675C JUAN \u9D51"));
        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_NAME,
                FullNameStyle.CJK).equalsIgnoreCase("DU \u675C JUAN \u9D51"));
        mContactLocaleUtils.setLocale(Locale.CHINESE);
        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_NAME,
                FullNameStyle.CHINESE).equalsIgnoreCase("DU \u675C JUAN \u9D51"));
        assertTrue(mContactLocaleUtils.getSortKey(CHINESE_NAME,
                FullNameStyle.CJK).equalsIgnoreCase("DU \u675C JUAN \u9D51"));
        assertTrue(mContactLocaleUtils.getSortKey(LATIN_NAME,
                FullNameStyle.WESTERN).equalsIgnoreCase(LATIN_NAME));

        mContactLocaleUtils.setLocale(Locale.ENGLISH);
        Iterator<String> keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_NAME,
                FullNameStyle.CHINESE);
        verifyKeys(keys, CHINESE_NAME_KEY);
        keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);
        mContactLocaleUtils.setLocale(Locale.CHINESE);
        keys = mContactLocaleUtils.getNameLookupKeys(CHINESE_NAME, FullNameStyle.CJK);
        verifyKeys(keys, CHINESE_NAME_KEY);
        keys = mContactLocaleUtils.getNameLookupKeys(LATIN_NAME, FullNameStyle.WESTERN);
        verifyKeys(keys, LATIN_NAME_KEY);

    }
}
