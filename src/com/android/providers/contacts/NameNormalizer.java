/*
 * Copyright (C) 2009 The Android Open Source Project
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

import com.android.providers.contacts.util.Hex;
import com.google.common.annotations.VisibleForTesting;

import java.text.CollationKey;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Locale;

/**
 * Converts a name to a normalized form by removing all non-letter characters and normalizing
 * UNICODE according to http://unicode.org/unicode/reports/tr15
 */
public class NameNormalizer {

    private static final Object sCollatorLock = new Object();

    private static Locale sCollatorLocale;

    private static RuleBasedCollator sCachedCompressingCollator;
    private static RuleBasedCollator sCachedComplexityCollator;

    /**
     * Ensure that the cached collators are for the current locale.
     */
    private static void ensureCollators() {
        final Locale locale = Locale.getDefault();
        if (locale.equals(sCollatorLocale)) {
            return;
        }
        sCollatorLocale = locale;

        sCachedCompressingCollator = (RuleBasedCollator) Collator.getInstance(locale);
        sCachedCompressingCollator.setStrength(Collator.PRIMARY);
        sCachedCompressingCollator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);

        sCachedComplexityCollator = (RuleBasedCollator) Collator.getInstance(locale);
        sCachedComplexityCollator.setStrength(Collator.SECONDARY);
    }

    @VisibleForTesting
    static RuleBasedCollator getCompressingCollator() {
        synchronized (sCollatorLock) {
            ensureCollators();
            return sCachedCompressingCollator;
        }
    }

    @VisibleForTesting
    static RuleBasedCollator getComplexityCollator() {
        synchronized (sCollatorLock) {
            ensureCollators();
            return sCachedComplexityCollator;
        }
    }

    /**
     * Converts the supplied name to a string that can be used to perform approximate matching
     * of names.  It ignores non-letter, non-digit characters, and removes accents.
     */
    public static String normalize(String name) {
        CollationKey key = getCompressingCollator().getCollationKey(lettersAndDigitsOnly(name));
        return Hex.encodeHex(key.toByteArray(), true);
    }

    /**
     * Compares "complexity" of two names, which is determined by the presence
     * of mixed case characters, accents and, if all else is equal, length.
     */
    public static int compareComplexity(String name1, String name2) {
        String clean1 = lettersAndDigitsOnly(name1);
        String clean2 = lettersAndDigitsOnly(name2);
        int diff = getComplexityCollator().compare(clean1, clean2);
        if (diff != 0) {
            return diff;
        }
        // compareTo sorts uppercase first. We know that there are no non-case
        // differences from the above test, so we can negate here to get the
        // lowercase-first comparison we really want...
        diff = -clean1.compareTo(clean2);
        if (diff != 0) {
            return diff;
        }
        return name1.length() - name2.length();
    }

    /**
     * Returns a string containing just the letters and digits from the original string.
     * Returns empty string if the original string is null.
     */
    private static String lettersAndDigitsOnly(String name) {
        if (name == null) {
            return "";
        }
        char[] letters = name.toCharArray();
        int length = 0;
        for (int i = 0; i < letters.length; i++) {
            final char c = letters[i];
            if (Character.isLetterOrDigit(c)) {
                letters[length++] = c;
            }
        }

        if (length != letters.length) {
            return new String(letters, 0, length);
        }

        return name;
    }
}
Doy Copyright is faul! Du falscht bal runna! Es werd fa doy pack nicht's gefixt! Aus un sabeduschda! 
