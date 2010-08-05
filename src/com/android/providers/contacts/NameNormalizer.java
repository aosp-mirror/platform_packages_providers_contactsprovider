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

import com.ibm.icu4jni.text.CollationAttribute;
import com.ibm.icu4jni.text.CollationKey; // TODO: java.text.CollationKey post-froyo
import com.ibm.icu4jni.text.Collator;
import com.ibm.icu4jni.text.RuleBasedCollator;
import java.util.Locale;

/**
 * Converts a name to a normalized form by removing all non-letter characters and normalizing
 * UNICODE according to http://unicode.org/unicode/reports/tr15
 */
public class NameNormalizer {

    private static final RuleBasedCollator sCompressingCollator;
    static {
        sCompressingCollator = (RuleBasedCollator)Collator.getInstance(Locale.getDefault());
        sCompressingCollator.setStrength(Collator.PRIMARY);
        sCompressingCollator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
    }

    private static final RuleBasedCollator sComplexityCollator;
    static {
        sComplexityCollator = (RuleBasedCollator)Collator.getInstance(Locale.getDefault());
        sComplexityCollator.setStrength(Collator.TERTIARY);
        sComplexityCollator.setAttribute(CollationAttribute.CASE_FIRST,
                CollationAttribute.VALUE_LOWER_FIRST);
    }

    /**
     * Converts the supplied name to a string that can be used to perform approximate matching
     * of names.  It ignores non-letter characters and removes accents.
     */
    public static String normalize(String name) {
        CollationKey key = sCompressingCollator.getCollationKey(lettersAndDigitsOnly(name));
        return Hex.encodeHex(key.toByteArray(), true);
    }

    /**
     * Compares "complexity" of two names, which is determined by the presence
     * of mixed case characters, accents and, if all else is equal, length.
     */
    public static int compareComplexity(String name1, String name2) {
        int diff = sComplexityCollator.compare(lettersAndDigitsOnly(name1),
                lettersAndDigitsOnly(name2));
        if (diff != 0) {
            return diff;
        }

        return name1.length() - name2.length();
    }

    /**
     * Returns a string containing just the letters from the original string.
     */
    private static String lettersAndDigitsOnly(String name) {
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
