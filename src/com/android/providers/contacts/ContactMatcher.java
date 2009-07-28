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

import com.android.providers.contacts.OpenHelper.NameLookupType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Logic for matching contacts' data and accumulating match scores.
 */
public class ContactMatcher {

    // Best possible match score
    public static final int MAX_SCORE = 100;

    // Suggest to aggregate contacts if their match score is equal or greater than this threshold
    public static final int SCORE_THRESHOLD_SUGGEST = 50;

    // Automatically aggregate contacts if their match score is equal or greater than this threshold
    public static final int SCORE_THRESHOLD_PRIMARY = 70;

    // Automatically aggregate contacts if the match score is equal or greater than this threshold
    // and there is a secondary match (phone number, email etc).
    public static final int SCORE_THRESHOLD_SECONDARY = 50;

    // Score for missing data (as opposed to present data but a bad match)
    private static final int NO_DATA_SCORE = -1;

    // Score for matching phone numbers
    private static final int PHONE_MATCH_SCORE = 71;

    // Score for matching email addresses
    private static final int EMAIL_MATCH_SCORE = 71;

    // Score for matching nickname
    private static final int NICKNAME_MATCH_SCORE = 71;

    // Minimum edit distance between two names to be considered an approximate match
    public static final float APPROXIMATE_MATCH_THRESHOLD = 0.7f;

    // Maximum number of characters in a name to be considered by the matching algorithm.
    private static final int MAX_MATCHED_NAME_LENGTH = 12;

    // Scores a multiplied by this number to allow room for "fractional" scores
    private static final int SCORE_SCALE = 1000;


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
        setScoreRange(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
        setScoreRange(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.NICKNAME, 30, 60);

        setScoreRange(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 30, 70);
        setScoreRange(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 40, 80);

        setScoreRange(NameLookupType.FULL_NAME_WITH_NICKNAME,
                NameLookupType.FULL_NAME_WITH_NICKNAME, 75, 75);
        setScoreRange(NameLookupType.FULL_NAME_WITH_NICKNAME_REVERSE,
                NameLookupType.FULL_NAME_WITH_NICKNAME_REVERSE, 73, 73);

        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FAMILY_NAME_ONLY, 45, 75);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 32, 72);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 70);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.NICKNAME, 30, 60);

        setScoreRange(NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME,
                NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME, 71, 71);
        setScoreRange(NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME,
                NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME, 70, 70);

        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.GIVEN_NAME_ONLY, 40, 70);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 32, 72);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 70);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.NICKNAME, 30, 60);

        setScoreRange(NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME,
                NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME, 73, 73);
        setScoreRange(NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME,
                NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME, 70, 70);

        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.GIVEN_NAME_ONLY, 30, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.FAMILY_NAME_ONLY, 30, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.FULL_NAME_CONCATENATED, 30, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.NICKNAME, 30, 60);

        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.NICKNAME, 30, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.GIVEN_NAME_ONLY, 30, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.FAMILY_NAME_ONLY, 30, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.FULL_NAME_CONCATENATED, 30, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 30, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
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
     * Captures the max score and match count for a specific contact.  Used in an
     * contactId - MatchScore map.
     */
    public static class MatchScore implements Comparable<MatchScore> {
        private long mContactId;
        private boolean mKeepIn;
        private boolean mKeepOut;
        private int mPrimaryScore;
        private int mSecondaryScore;
        private int mMatchCount;

        public MatchScore(long contactId) {
            this.mContactId = contactId;
        }

        public void reset(long contactId) {
            this.mContactId = contactId;
            mKeepIn = false;
            mKeepOut = false;
            mPrimaryScore = 0;
            mSecondaryScore = 0;
            mMatchCount = 0;
        }

        public long getContactId() {
            return mContactId;
        }

        public void updatePrimaryScore(int score) {
            if (score > mPrimaryScore) {
                mPrimaryScore = score;
            }
            mMatchCount++;
        }

        public void updateSecondaryScore(int score) {
            if (score > mSecondaryScore) {
                mSecondaryScore = score;
            }
            mMatchCount++;
        }

        public void keepIn() {
            mKeepIn = true;
        }

        public void keepOut() {
            mKeepOut = true;
        }

        public int getScore() {
            if (mKeepOut) {
                return 0;
            }

            if (mKeepIn) {
                return MAX_SCORE;
            }

            int score = (mPrimaryScore > mSecondaryScore ? mPrimaryScore : mSecondaryScore);

            // Ensure that of two contacts with the same match score the one with more matching
            // data elements wins.
            return score * SCORE_SCALE + mMatchCount;
        }

        /**
         * Descending order of match score.
         */
        public int compareTo(MatchScore another) {
            return another.getScore() - getScore();
        }

        @Override
        public String toString() {
            return mContactId + ": " + mPrimaryScore + "/" + mSecondaryScore + "(" + mMatchCount
                    + ")";
        }
    }

    private final HashMap<Long, MatchScore> mScores = new HashMap<Long, MatchScore>();
    private final ArrayList<MatchScore> mScoreList = new ArrayList<MatchScore>();
    private int mScoreCount = 0;

    private final JaroWinklerDistance mJaroWinklerDistance =
            new JaroWinklerDistance(MAX_MATCHED_NAME_LENGTH);

    private MatchScore getMatchingScore(long contactId) {
        MatchScore matchingScore = mScores.get(contactId);
        if (matchingScore == null) {
            if (mScoreList.size() > mScoreCount) {
                matchingScore = mScoreList.get(mScoreCount);
                matchingScore.reset(contactId);
            } else {
                matchingScore = new MatchScore(contactId);
                mScoreList.add(matchingScore);
            }
            mScoreCount++;
            mScores.put(contactId, matchingScore);
        }
        return matchingScore;
    }

    /**
     * Checks if there is a match and updates the overall score for the
     * specified contact for a discovered match. The new score is determined
     * by the prior score, by the type of name we were looking for, the type
     * of name we found and, if the match is approximate, the distance between the candidate and
     * actual name.
     */
    public void matchName(long contactId, int candidateNameType, String candidateName,
            int nameType, String name, boolean approximate) {
        int maxScore = getMaxScore(candidateNameType, nameType);
        if (maxScore == 0) {
            return;
        }

        if (candidateName.equals(name)) {
            updatePrimaryScore(contactId, maxScore);
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

        int score;
        if (distance > APPROXIMATE_MATCH_THRESHOLD) {
            float adjustedDistance = (distance - APPROXIMATE_MATCH_THRESHOLD)
                    / (1f - APPROXIMATE_MATCH_THRESHOLD);
            score = (int)(minScore +  (maxScore - minScore) * adjustedDistance);
        } else {
            score = 0;
        }

        updatePrimaryScore(contactId, score);
    }

    public void updateScoreWithPhoneNumberMatch(long contactId) {
        updateSecondaryScore(contactId, PHONE_MATCH_SCORE);
    }

    public void updateScoreWithEmailMatch(long contactId) {
        updateSecondaryScore(contactId, EMAIL_MATCH_SCORE);
    }

    public void updateScoreWithNicknameMatch(long contactId) {
        updateSecondaryScore(contactId, NICKNAME_MATCH_SCORE);
    }

    private void updatePrimaryScore(long contactId, int score) {
        getMatchingScore(contactId).updatePrimaryScore(score);
    }

    private void updateSecondaryScore(long contactId, int score) {
        getMatchingScore(contactId).updateSecondaryScore(score);
    }

    public void keepIn(long contactId) {
        getMatchingScore(contactId).keepIn();
    }

    public void keepOut(long contactId) {
        getMatchingScore(contactId).keepOut();
    }

    public void clear() {
        mScores.clear();
        mScoreCount = 0;
    }

    /**
     * Returns a list of IDs for contacts that are matched on secondary data elements
     * (phone number, email address, nickname). We still need to obtain the approximate
     * primary score for those contacts to determine if any of them should be contactd.
     * <p>
     * May return null.
     */
    public List<Long> prepareSecondaryMatchCandidates(int threshold) {
        ArrayList<Long> contactIds = null;

        for (int i = 0; i < mScoreCount; i++) {
            MatchScore score = mScoreList.get(i);
            if (score.mKeepOut) {
                continue;
            }

            int s = score.mSecondaryScore;
            if (s >= threshold) {
                if (contactIds == null) {
                    contactIds = new ArrayList<Long>();
                }
                contactIds.add(score.mContactId);
                score.mPrimaryScore = NO_DATA_SCORE;
            }
        }
        return contactIds;
    }

    /**
     * Returns the contactId with the best match score over the specified threshold or -1
     * if no such contact is found.
     */
    public long pickBestMatch(int threshold) {
        long contactId = -1;
        int maxScore = 0;
        for (int i = 0; i < mScoreCount; i++) {
            MatchScore score = mScoreList.get(i);
            if (score.mKeepIn) {
                return score.mContactId;
            }

            if (score.mKeepOut) {
                continue;
            }

            int s = score.mPrimaryScore;
            if (s == NO_DATA_SCORE) {
                s = score.mSecondaryScore;
            }

            if (s >= threshold && s > maxScore) {
                contactId = score.mContactId;
                maxScore = s;
            }
        }
        return contactId;
    }

    /**
     * Returns up to {@code maxSuggestions} best scoring matches.
     */
    public List<MatchScore> pickBestMatches(int maxSuggestions, int threshold) {
        int scaledThreshold = threshold * SCORE_SCALE;
        List<MatchScore> matches = mScoreList.subList(0, mScoreCount);
        Collections.sort(matches);
        int count = 0;
        for (int i = 0; i < mScoreCount; i++) {
            MatchScore matchScore = matches.get(i);
            if (matchScore.getScore() >= scaledThreshold) {
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
