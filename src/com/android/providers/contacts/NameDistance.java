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

import java.util.Arrays;

/**
 * A string distance calculator, particularly suited for name matching.
 * We are calculating the number of mismatched characters and the number of transpositions
 * there should be fewer than 4 mismatched characters between the two strings and no more
 * than 5 total differences between the strings to yield a non-zero score.
 */
public class NameDistance {

    private static final int MIN_EXACT_PREFIX_LENGTH = 3;

    private final int mMaxLength;
    private final boolean[] mMatchFlags1;
    private final boolean[] mMatchFlags2;

    /**
     * Constructor.
     *
     * @param maxLength byte arrays are truncate if longer than this number
     */
    public NameDistance(int maxLength) {
        mMaxLength = maxLength;
        mMatchFlags1 = new boolean[maxLength];
        mMatchFlags2 = new boolean[maxLength];
    }

    /**
     * Computes a string distance between two normalized strings passed as byte arrays.
     */
    public float getDistance(byte bytes1[], byte bytes2[]) {
        byte[] array1, array2;

        if (bytes1.length > bytes2.length) {
            array2 = bytes1;
            array1 = bytes2;
        } else {
            array2 = bytes2;
            array1 = bytes1;
        }

        int length1 = array1.length;
        if (length1 >= MIN_EXACT_PREFIX_LENGTH) {
            boolean prefix = true;
            for (int i = 0; i < array1.length; i++) {
                if (array1[i] != array2[i]) {
                    prefix = false;
                    break;
                }
            }
            if (prefix) {
                return 1.0f;
            }
        }

        if (length1 > mMaxLength) {
            length1 = mMaxLength;
        }

        int length2 = array2.length;
        if (length2 > mMaxLength) {
            length2 = mMaxLength;
        }

        Arrays.fill(mMatchFlags1, 0, length1, false);
        Arrays.fill(mMatchFlags2, 0, length2, false);

        int range = length2 / 2 - 1;
        if (range < 0) {
            range = 0;
        }

        int matches = 0;
        for (int i = 0; i < length1; i++) {
            byte c1 = array1[i];

            int from = i - range;
            if (from < 0) {
                from = 0;
            }

            int to = i + range + 1;
            if (to > length2) {
                to = length2;
            }

            for (int j = from; j < to; j++) {
                if (!mMatchFlags2[j] && c1 == array2[j]) {
                    mMatchFlags1[i] = mMatchFlags2[j] = true;
                    matches++;
                    break;
                }
            }
        }

        int mismatches = (length1 - matches) + (length2 - matches);
        if (mismatches > 4) {
            return 0f;
        }

        int transpositions = 0;
        int j = 0;
        for (int i = 0; i < length1; i++) {
            if (mMatchFlags1[i]) {
                while (!mMatchFlags2[j]) {
                    j++;
                }
                if (array1[i] != array2[j]) {
                    transpositions++;
                }
                j++;
            }
        }

        float differences = (mismatches + transpositions)/2.0f;

        float score = (1.0f - differences/5.0f);
        if (score < 0) {
            score = 0;
        }
        return score;
    }
}
