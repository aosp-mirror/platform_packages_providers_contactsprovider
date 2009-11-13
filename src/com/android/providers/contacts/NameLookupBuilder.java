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

import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Given a full name, constructs all possible variants of the name.
 */
public abstract class NameLookupBuilder {

    private static final int MAX_NAME_TOKENS = 4;

    private final NameSplitter mSplitter;
    private String[][] mNicknameClusters = new String[MAX_NAME_TOKENS][];
    private StringBuilder mStringBuilder = new StringBuilder();
    private String[] mNames = new String[NameSplitter.MAX_TOKENS];

    public NameLookupBuilder(NameSplitter splitter) {
        mSplitter = splitter;
    }

    /**
     * Inserts a name lookup record with the supplied column values.
     */
    protected abstract void insertNameLookup(long rawContactId, long dataId, int lookupType,
            String string);

    /**
     * Returns common nickname cluster IDs for a given name. For example, it
     * will return the same value for "Robert", "Bob" and "Rob". Some names belong to multiple
     * clusters, e.g. Leo could be Leonard or Leopold.
     *
     * May return null.
     *
     * @param normalizedName A normalized first name, see {@link NameNormalizer#normalize}.
     */
    protected abstract String[] getCommonNicknameClusters(String normalizedName);

    /**
     * Inserts name lookup records for the given structured name.
     */
    public void insertNameLookup(long rawContactId, long dataId, String name) {
        int tokenCount = mSplitter.tokenize(mNames, name);
        if (tokenCount == 0) {
            return;
        }

        for (int i = 0; i < tokenCount; i++) {
            mNames[i] = normalizeName(mNames[i]);
        }

        boolean tooManyTokens = tokenCount > MAX_NAME_TOKENS;
        if (tooManyTokens) {
            insertNameVariant(rawContactId, dataId, tokenCount, NameLookupType.NAME_EXACT, true);

            // Favor longer parts of the name
            Arrays.sort(mNames, 0, tokenCount, new Comparator<String>() {

                public int compare(String s1, String s2) {
                    return s2.length() - s1.length();
                }
            });

            // Insert a collation key for each extra word - useful for contact filtering
            // and suggestions
            String firstToken = mNames[0];
            for (int i = MAX_NAME_TOKENS; i < tokenCount; i++) {
                mNames[0] = mNames[i];
                insertCollationKey(rawContactId, dataId, MAX_NAME_TOKENS);
            }
            mNames[0] = firstToken;

            tokenCount = MAX_NAME_TOKENS;
        }

        // Phase I: insert all variants not involving nickname clusters
        for (int i = 0; i < tokenCount; i++) {
            mNicknameClusters[i] = getCommonNicknameClusters(mNames[i]);
        }

        insertNameVariants(rawContactId, dataId, 0, tokenCount, !tooManyTokens, true);
        insertNicknamePermutations(rawContactId, dataId, 0, tokenCount);
    }

    protected String normalizeName(String name) {
        return NameNormalizer.normalize(name);
    }

    /**
     * Inserts all name variants based on permutations of tokens between
     * fromIndex and toIndex
     *
     * @param initiallyExact true if the name without permutations is the exact
     *            original name
     * @param buildCollationKey true if a collation key makes sense for these
     *            permutations (false if at least one of the tokens is a
     *            nickname cluster key)
     */
    private void insertNameVariants(long rawContactId, long dataId, int fromIndex, int toIndex,
            boolean initiallyExact, boolean buildCollationKey) {
        if (fromIndex == toIndex) {
            insertNameVariant(rawContactId, dataId, toIndex,
                    initiallyExact ? NameLookupType.NAME_EXACT : NameLookupType.NAME_VARIANT,
                    buildCollationKey);
            return;
        }

        // Swap the first token with each other token (including itself, which is a no-op)
        // and recursively insert all permutations for the remaining tokens
        String firstToken = mNames[fromIndex];
        for (int i = fromIndex; i < toIndex; i++) {
            mNames[fromIndex] = mNames[i];
            mNames[i] = firstToken;

            insertNameVariants(rawContactId, dataId, fromIndex + 1, toIndex,
                    initiallyExact && i == fromIndex, buildCollationKey);

            mNames[i] = mNames[fromIndex];
            mNames[fromIndex] = firstToken;
        }
    }

    /**
     * Inserts a single name variant and optionally its collation key counterpart.
     */
    private void insertNameVariant(long rawContactId, long dataId, int tokenCount,
            int lookupType, boolean buildCollationKey) {
        mStringBuilder.setLength(0);

        for (int i = 0; i < tokenCount; i++) {
            if (i != 0) {
                mStringBuilder.append('.');
            }
            mStringBuilder.append(mNames[i]);
        }

        insertNameLookup(rawContactId, dataId, lookupType, mStringBuilder.toString());

        if (buildCollationKey) {
            insertCollationKey(rawContactId, dataId, tokenCount);
        }
    }

    /**
     * Inserts a collation key for the current contents of {@link #mNames}.
     */
    private void insertCollationKey(long rawContactId, long dataId, int tokenCount) {
        mStringBuilder.setLength(0);

        for (int i = 0; i < tokenCount; i++) {
            mStringBuilder.append(mNames[i]);
        }

        insertNameLookup(rawContactId, dataId, NameLookupType.NAME_COLLATION_KEY,
                mStringBuilder.toString());
    }

    /**
     * For all tokens that correspond to nickname clusters, substitutes each cluster key
     * and inserts all permutations with that key.
     */
    private void insertNicknamePermutations(long rawContactId, long dataId, int fromIndex,
            int tokenCount) {
        for (int i = fromIndex; i < tokenCount; i++) {
            String[] clusters = mNicknameClusters[i];
            if (clusters != null) {
                String token = mNames[i];
                for (int j = 0; j < clusters.length; j++) {
                    mNames[i] = clusters[j];

                    // Insert all permutations with this nickname cluster
                    insertNameVariants(rawContactId, dataId, 0, tokenCount, false, false);

                    // Repeat recursively for other nickname clusters
                    insertNicknamePermutations(rawContactId, dataId, i + 1, tokenCount);
                }
                mNames[i] = token;
            }
        }
    }
}
