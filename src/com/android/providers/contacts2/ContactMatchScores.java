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
package com.android.providers.contacts2;

import com.android.providers.contacts2.OpenHelper.NameLookupType;

import android.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A data structure used for accumulating match scores.  It maintains a map of aggregate ID to
 * {@link MatchScore}'s.
 */
public class ContactMatchScores {

    public static final int NEVER_MATCH = -1;
    public static final int ALWAYS_MATCH = 100;

    /**
     * Name matching scores: a matrix by name type vs. lookup type. For example, if
     * the name type is "full name" while we are looking for a "full name", the
     * score may be 100. If we are looking for a "nickname" but find
     * "first name", the score may be 50 (see specific scores defined below.)
     */
    private static int[] sScores = new int[NameLookupType.TYPE_COUNT * NameLookupType.TYPE_COUNT];

    /*
     * Note: the reverse names ({@link NameLookupType#FULL_NAME_REVERSE},
     * {@link NameLookupType#FULL_NAME_REVERSE_CONCATENATED} may appear to be redundant. They are
     * not!  They are useful in three-way aggregation cases when we have, for example, both
     * John Smith and Smith John.  A third contact with the name John Smith should be aggregated
     * with the former rather than the latter.  This is why "reverse" matches have slightly lower
     * scores than direct matches.
     */
    static {
        setNameMatchScore(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME, 99);
        setNameMatchScore(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME_REVERSE, 90);

        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME, 90);
        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME_REVERSE, 99);

        setNameMatchScore(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 80);
        setNameMatchScore(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);

        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 70);
        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 80);

        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FAMILY_NAME_ONLY, 75);
        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 72);
        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);

        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.GIVEN_NAME_ONLY, 70);
        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 72);
        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);
    }

    /**
     * Populates the cell of the score matrix corresponding to the {@code nameType} and
     * {@code lookupType}.
     */
    private static void setNameMatchScore(int lookupType, int nameType, int score) {
        int index = nameType * NameLookupType.TYPE_COUNT + lookupType;
        sScores[index] = score;
    }

    /**
     * Returns the match score for the given {@code nameType} and {@code lookupType}.
     */
    private static int getNameMatchScore(int lookupType, int nameType) {
        int index = nameType * NameLookupType.TYPE_COUNT + lookupType;
        return sScores[index];
    }

    /**
     * Captures the max score and match count for a specific aggregate.  Used in an
     * aggregateId - MatchScore map.
     */
    public static class MatchScore implements Comparable<MatchScore> {
        private long mAggregateId;
        private int mScore;
        private int mMatchCount;

        public MatchScore(long aggregateId) {
            this.mAggregateId = aggregateId;
        }

        public long getAggregateId() {
            return mAggregateId;
        }

        public void updateScore(int score) {
            if (mScore == NEVER_MATCH) {
                return;
            }

            if ((score == NEVER_MATCH) || (score > mScore)) {
                mScore = score;
            }

            mMatchCount++;
        }

        /**
         * Descending order of match score.
         */
        public int compareTo(MatchScore another) {
            if (mScore == another.mScore) {
                return another.mMatchCount - mMatchCount;
            }

            return another.mScore - mScore;
        }

        @Override
        public String toString() {
            return String.valueOf(mAggregateId) + ": "
                    + String.valueOf(mScore) + "(" + mMatchCount + ")";
        }
    }

    private HashMap<Long, MatchScore> mScores = new HashMap<Long, MatchScore>();

    /**
     * Updates the overall score for the specified aggregate for a discovered
     * match. The new score is determined by the prior score, by the type of
     * name we were looking for and the type of name we found.
     */
    public void updateScore(long aggregateId, int nameLookupType, int nameType) {
        int score = getNameMatchScore(nameLookupType, nameType);
        if (score == 0) {
            return;
        }

        updateScore(aggregateId, score);
    }

    public void updateScore(long aggregateId, int score) {
        MatchScore matchingScore = mScores.get(aggregateId);
        if (matchingScore == null) {
            matchingScore = new MatchScore(aggregateId);
            mScores.put(aggregateId, matchingScore);
        }

        matchingScore.updateScore(score);
    }

    public void clear() {
        mScores.clear();
    }

    public void remove(long aggregateId) {
        mScores.remove(aggregateId);
    }

    /**
     * Returns the aggregateId with the best match score over the specified threshold or -1
     * if no such aggregate is found.
     */
    public long pickBestMatch(int threshold) {
        long aggregateId = -1;
        int maxScore = 0;
        int maxMatchCount = 0;
        for (Map.Entry<Long, MatchScore> entry : mScores.entrySet()) {
            MatchScore score = entry.getValue();
            if (score.mScore >= threshold
                    && (score.mScore > maxScore
                            || (score.mScore == maxScore && score.mMatchCount > maxMatchCount))) {
                aggregateId = score.mAggregateId;
                maxScore = score.mScore;
                maxMatchCount = score.mMatchCount;
            }
        }
        return aggregateId;
    }

    /**
     * Returns up to {@code maxSuggestions} best scoring matches.
     */
    public ArrayList<MatchScore> pickBestMatches(int maxSuggestions) {
        ArrayList<MatchScore> matches = new ArrayList<MatchScore>(mScores.values());
        Collections.sort(matches);
        if (matches.size() > maxSuggestions) {
            matches = (ArrayList<MatchScore>)matches.subList(0, maxSuggestions);
        }
        return matches;
    }

    @Override
    public String toString() {
        return pickBestMatches(10).toString();
    }
}
