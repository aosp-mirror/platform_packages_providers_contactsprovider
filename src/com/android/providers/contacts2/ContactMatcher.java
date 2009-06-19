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
import java.util.List;

/**
 * Logic for matching contacts' data and accumulating match scores.
 */
public class ContactMatcher {

    public static final int NEVER_MATCH = -1;
    public static final int ALWAYS_MATCH = 100;

    public static final float APPROXIMATE_MATCH_THRESHOLD = 0.7f;

    /**
     * Maximum number of characters in a name to be considered by the matching algorithm.
     */
    private static final int MAX_MATCHED_NAME_LENGTH = 12;

    /**
     * Name matching scores: a matrix by name type vs. candidate lookup type.
     * For example, if the name type is "full name" while we are looking for a
     * "full name", the score may be 99. If we are looking for a "nickname" but
     * find "first name", the score may be 50 (see specific scores defined
     * below.)
     * <p>
     * For approximate matching, we have a range of scores, let's say 40-70.  Depending one how
     * similar the two strings are, the score will be somewhere between 40 and 70, with the exact
     * match producing the score of 70.  The score may also be 0 if the similarity (distance)
     * between the strings is below the threshold.
     * <p>
     * We use the Jaro-Winkler algorithm, which is particularly suited for
     * name matching. See {@link JaroWinklerDistance}.
     */
    private static int[] sMinScore =
            new int[NameLookupType.TYPE_COUNT * NameLookupType.TYPE_COUNT];
    private static int[] sMaxScore =
            new int[NameLookupType.TYPE_COUNT * NameLookupType.TYPE_COUNT];

    /*
     * Note: the reverse names ({@link NameLookupType#FULL_NAME_REVERSE},
     * {@link NameLookupType#FULL_NAME_REVERSE_CONCATENATED} may appear to be redundant. They are
     * not!  They are useful in three-way aggregation cases when we have, for example, both
     * John Smith and Smith John.  A third contact with the name John Smith should be aggregated
     * with the former rather than the latter.  This is why "reverse" matches have slightly lower
     * scores than direct matches.
     */
    static {
        setScoreRange(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME, 99, 99);
        setScoreRange(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME_REVERSE, 90, 90);

        setScoreRange(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME, 90, 90);
        setScoreRange(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME_REVERSE, 99, 99);

        setScoreRange(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 40, 80);
        setScoreRange(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 70);

        setScoreRange(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 30, 70);
        setScoreRange(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 40, 80);

        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FAMILY_NAME_ONLY, 45, 75);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 32, 72);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 70);

        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.GIVEN_NAME_ONLY, 40, 70);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 32, 72);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 70);
    }

    /**
     * Populates the cells of the score matrix and score span matrix
     * corresponding to the {@code candidateNameType} and {@code nameType}.
     */
    private static void setScoreRange(int candidateNameType, int nameType, int scoreFrom, int scoreTo) {
        int index = nameType * NameLookupType.TYPE_COUNT + candidateNameType;
        sMinScore[index] = scoreFrom;
        sMaxScore[index] = scoreTo;
    }

    /**
     * Returns the lower range for the match score for the given {@code candidateNameType} and
     * {@code nameType}.
     */
    private static int getMinScore(int candidateNameType, int nameType) {
        int index = nameType * NameLookupType.TYPE_COUNT + candidateNameType;
        return sMinScore[index];
    }

    /**
     * Returns the upper range for the match score for the given {@code candidateNameType} and
     * {@code nameType}.
     */
    private static int getMaxScore(int candidateNameType, int nameType) {
        int index = nameType * NameLookupType.TYPE_COUNT + candidateNameType;
        return sMaxScore[index];
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

    private final HashMap<Long, MatchScore> mScores = new HashMap<Long, MatchScore>();
    private final JaroWinklerDistance mJaroWinklerDistance =
            new JaroWinklerDistance(MAX_MATCHED_NAME_LENGTH);

    /**
     * Checks if there is a match and updates the overall score for the
     * specified aggregate for a discovered match. The new score is determined
     * by the prior score, by the type of name we were looking for, the type
     * of name we found and, if the match is approximate, the distance between the candidate and
     * actual name.
     */
    public void match(long aggregateId, int candidateNameType, String candidateName,
            int nameType, String name, boolean approximate) {

        int maxScore = getMaxScore(candidateNameType, nameType);
        if (maxScore == 0) {
            return;
        }

        if (candidateName.equals(name)) {
            updateScore(aggregateId, maxScore);
            return;
        }

        if (!approximate) {
            return;
        }

        int minScore = getMinScore(candidateNameType, nameType);
        if (minScore == maxScore) {
            return;
        }

        float distance = mJaroWinklerDistance.getDistance(
                Hex.decodeHex(candidateName), Hex.decodeHex(name));

        if (distance > APPROXIMATE_MATCH_THRESHOLD) {
            float adjustedDistance = (distance - APPROXIMATE_MATCH_THRESHOLD)
                    / (1f - APPROXIMATE_MATCH_THRESHOLD);
            updateScore(aggregateId, (int)(minScore +  (maxScore - minScore) * adjustedDistance));
        }
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
        for (MatchScore score : mScores.values()) {
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
    public List<MatchScore> pickBestMatches(int maxSuggestions, int threshold) {
        ArrayList<MatchScore> matches = new ArrayList<MatchScore>(mScores.values());
        Collections.sort(matches);
        int count = 0;
        for (MatchScore matchScore : matches) {
            if (matchScore.mScore >= threshold) {
                count++;
            } else {
                break;
            }
        }

        if (count > maxSuggestions) {
            count = maxSuggestions;
        }

        return matches.subList(0, count);
    }
}
