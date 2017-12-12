/*
 * Copyright (C) 2015 The Android Open Source Project
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
package com.android.providers.contacts.aggregation.util;

import android.util.ArrayMap;
import android.util.Log;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.util.Hex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Logic for matching raw contacts' data.
 */
public class RawContactMatcher {
    private static final String TAG = "ContactMatcher";

    // Suggest to aggregate contacts if their match score is equal or greater than this threshold
    public static final int SCORE_THRESHOLD_SUGGEST = 50;

    public static final int SCORE_THRESHOLD_NO_NAME = 50;

    // Automatically aggregate contacts if their match score is equal or greater than this threshold
    public static final int SCORE_THRESHOLD_PRIMARY = 70;

    // Automatically aggregate contacts if the match score is equal or greater than this threshold
    // and there is a secondary match (phone number, email etc).
    public static final int SCORE_THRESHOLD_SECONDARY = 50;

    // Score for matching phone numbers
    private static final int PHONE_MATCH_SCORE = 71;

    // Score for matching email addresses
    private static final int EMAIL_MATCH_SCORE = 71;

    // Score for matching identity
    private static final int IDENTITY_MATCH_SCORE = 71;

    // Score for matching nickname
    private static final int NICKNAME_MATCH_SCORE = 71;

    // Maximum number of characters in a name to be considered by the matching algorithm.
    private static final int MAX_MATCHED_NAME_LENGTH = 30;

    // Scores a multiplied by this number to allow room for "fractional" scores
    private static final int SCORE_SCALE = 1000;

    public static final int MATCHING_ALGORITHM_EXACT = 0;
    public static final int MATCHING_ALGORITHM_CONSERVATIVE = 1;
    public static final int MATCHING_ALGORITHM_APPROXIMATE = 2;

    // Minimum edit distance between two names to be considered an approximate match
    public static final float APPROXIMATE_MATCH_THRESHOLD = 0.82f;

    // Minimum edit distance between two email ids to be considered an approximate match
    public static final float APPROXIMATE_MATCH_THRESHOLD_FOR_EMAIL = 0.95f;

    // Returned value when we found multiple matches and that was not allowed
    public static final long MULTIPLE_MATCHES = -2;

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
     * We use a string matching algorithm, which is particularly suited for
     * name matching. See {@link NameDistance}.
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
        setScoreRange(NameLookupType.NAME_EXACT,
                NameLookupType.NAME_EXACT, 99, 99);
        setScoreRange(NameLookupType.NAME_VARIANT,
                NameLookupType.NAME_VARIANT, 90, 90);
        setScoreRange(NameLookupType.NAME_COLLATION_KEY,
                NameLookupType.NAME_COLLATION_KEY, 50, 80);

        setScoreRange(NameLookupType.NAME_COLLATION_KEY,
                NameLookupType.EMAIL_BASED_NICKNAME, 30, 60);
        setScoreRange(NameLookupType.NAME_COLLATION_KEY,
                NameLookupType.NICKNAME, 50, 60);

        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.EMAIL_BASED_NICKNAME, 50, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.NAME_COLLATION_KEY, 50, 60);
        setScoreRange(NameLookupType.EMAIL_BASED_NICKNAME,
                NameLookupType.NICKNAME, 50, 60);

        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.NICKNAME, 50, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.NAME_COLLATION_KEY, 50, 60);
        setScoreRange(NameLookupType.NICKNAME,
                NameLookupType.EMAIL_BASED_NICKNAME, 50, 60);
    }

    /**
     * Populates the cells of the score matrix and score span matrix
     * corresponding to the {@code candidateNameType} and {@code nameType}.
     */
    private static void setScoreRange(int candidateNameType, int nameType, int scoreFrom,
            int scoreTo) {
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

    private final ArrayMap<Long, MatchScore> mScores = new ArrayMap<>();
    private final ArrayList<MatchScore> mScoreList = new ArrayList<MatchScore>();
    private int mScoreCount = 0;

    private final NameDistance mNameDistanceConservative = new NameDistance();
    private final NameDistance mNameDistanceApproximate = new NameDistance(MAX_MATCHED_NAME_LENGTH);

    private MatchScore getMatchingScore(long rawContactId, long contactId, long accountId) {
        MatchScore matchingScore = mScores.get(rawContactId);
        if (matchingScore == null) {
            if (mScoreList.size() > mScoreCount) {
                matchingScore = mScoreList.get(mScoreCount);
                matchingScore.reset(rawContactId, contactId, accountId);
            } else {
                matchingScore = new MatchScore(rawContactId, contactId, accountId);
                mScoreList.add(matchingScore);
            }
            mScoreCount++;
            mScores.put(rawContactId, matchingScore);
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
    public void matchName(long rawContactId, long contactId, long accountId, int
            candidateNameType, String candidateName, int nameType, String name, int algorithm) {
        int maxScore = getMaxScore(candidateNameType, nameType);
        if (maxScore == 0) {
            return;
        }

        if (candidateName.equals(name)) {
            updatePrimaryScore(rawContactId, contactId, accountId, maxScore);
            return;
        }

        if (algorithm == MATCHING_ALGORITHM_EXACT) {
            return;
        }

        int minScore = getMinScore(candidateNameType, nameType);
        if (minScore == maxScore) {
            return;
        }

        final byte[] decodedCandidateName;
        final byte[] decodedName;
        try {
            decodedCandidateName = Hex.decodeHex(candidateName);
            decodedName = Hex.decodeHex(name);
        } catch (RuntimeException e) {
            // How could this happen??  See bug 6827136
            Log.e(TAG, "Failed to decode normalized name.  Skipping.", e);
            return;
        }

        NameDistance nameDistance = algorithm == MATCHING_ALGORITHM_CONSERVATIVE ?
                mNameDistanceConservative : mNameDistanceApproximate;

        int score;
        float distance = nameDistance.getDistance(decodedCandidateName, decodedName);
        boolean emailBased = candidateNameType == NameLookupType.EMAIL_BASED_NICKNAME
                || nameType == NameLookupType.EMAIL_BASED_NICKNAME;
        float threshold = emailBased
                ? APPROXIMATE_MATCH_THRESHOLD_FOR_EMAIL
                : APPROXIMATE_MATCH_THRESHOLD;
        if (distance > threshold) {
            score = (int)(minScore +  (maxScore - minScore) * (1.0f - distance));
        } else {
            score = 0;
        }

        updatePrimaryScore(rawContactId, contactId, accountId, score);
    }

    public void matchIdentity(long rawContactId, long contactId, long accountId) {
        updateSecondaryScore(rawContactId, contactId, accountId, IDENTITY_MATCH_SCORE);
    }

    public void updateScoreWithPhoneNumberMatch(long rawContactId, long contactId, long accountId) {
        updateSecondaryScore(rawContactId, contactId, accountId, PHONE_MATCH_SCORE);
    }

    public void updateScoreWithEmailMatch(long rawContactId, long contactId, long accountId) {
        updateSecondaryScore(rawContactId, contactId, accountId, EMAIL_MATCH_SCORE);
    }

    public void updateScoreWithNicknameMatch(long rawContactId, long contactId, long accountId) {
        updateSecondaryScore(rawContactId, contactId, accountId, NICKNAME_MATCH_SCORE);
    }

    private void updatePrimaryScore(long rawContactId, long contactId, long accountId, int score) {
        getMatchingScore(rawContactId, contactId, accountId).updatePrimaryScore(score);
    }

    private void updateSecondaryScore(long rawContactId, long contactId, long accountId,
            int score) {
        getMatchingScore(rawContactId, contactId, accountId).updateSecondaryScore(score);
    }

    public void keepIn(long rawContactId, long contactId, long accountId) {
        getMatchingScore(rawContactId, contactId, accountId).keepIn();
    }

    public void keepOut(long rawContactId, long contactId, long accountId) {
        getMatchingScore(rawContactId, contactId, accountId).keepOut();
    }

    public void clear() {
        mScores.clear();
        mScoreCount = 0;
    }
    /**
     * Returns a list of IDs for raw contacts that are only matched on secondary data elements
     * (phone number, email address, nickname, identity). We need to check if they are missing
     * structured name or not to decide if they should be aggregated.
     * <p>
     * May return null.
     */
    public List<Long> prepareSecondaryMatchCandidates() {
        ArrayList<Long> rawContactIds = null;

        for (int i = 0; i < mScoreCount; i++) {
            MatchScore score = mScoreList.get(i);
            if (score.isKeepOut() ||  score.getPrimaryScore() > SCORE_THRESHOLD_PRIMARY){
                continue;
            }

            if (score.getSecondaryScore() >= SCORE_THRESHOLD_PRIMARY) {
                if (rawContactIds == null) {
                    rawContactIds = new ArrayList<>();
                }
                rawContactIds.add(score.getRawContactId());
            }
            score.setPrimaryScore(0);
        }
        return rawContactIds;
    }

    /**
     * Returns the list of raw contact Ids with the match score over threshold.
     */
    public List<MatchScore> pickBestMatches() {
        final List<MatchScore> matches = new ArrayList<>();
        for (int i = 0; i < mScoreCount; i++) {
            MatchScore score = mScoreList.get(i);
            if (score.isKeepOut()) {
                continue;
            }

            if (score.isKeepIn()) {
                matches.add(score);
                continue;
            }

            if (score.getPrimaryScore() >= SCORE_THRESHOLD_PRIMARY ||
                    (score.getPrimaryScore() == SCORE_THRESHOLD_NO_NAME &&
                            score.getSecondaryScore() > SCORE_THRESHOLD_SECONDARY)) {
                matches.add(score);
            }
        }
        return matches;
    }

    /**
     * Returns matches in the order of descending score.
     */
    public List<MatchScore> pickBestMatches(int threshold) {
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

        return matches.subList(0, count);
    }

    @Override
    public String toString() {
        return mScoreList.subList(0, mScoreCount).toString();
    }

    public void matchNoName(Long rawContactId, Long contactId, Long accountId) {
        updatePrimaryScore(rawContactId, contactId, accountId, SCORE_THRESHOLD_NO_NAME);
    }
}
