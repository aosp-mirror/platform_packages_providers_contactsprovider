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

/**
 * Captures the max score and match count for a specific raw contact or contact.
 */
public class MatchScore implements Comparable<MatchScore> {
    // Scores a multiplied by this number to allow room for "fractional" scores
    public static final int SCORE_SCALE = 1000;
    // Best possible match score
    public static final int MAX_SCORE = 100;

    private long mRawContactId;
    private long mContactId;
    private long mAccountId;

    private boolean mKeepIn;
    private boolean mKeepOut;

    private int mPrimaryScore;
    private int mSecondaryScore;
    private int mMatchCount;

    public MatchScore(long rawContactId, long contactId, long accountId) {
        this.mRawContactId = rawContactId;
        this.mContactId = contactId;
        this.mAccountId = accountId;
    }

    public MatchScore(long contactId) {
        this.mRawContactId = 0;
        this.mContactId = contactId;
        this.mAccountId = 0;
    }

    public void reset(long rawContactId, long contactId, long accountId) {
        this.mRawContactId = rawContactId;
        this.mContactId = contactId;
        this.mAccountId = accountId;
        mKeepIn = false;
        mKeepOut = false;
        mPrimaryScore = 0;
        mSecondaryScore = 0;
        mMatchCount = 0;
    }

    public void reset(long contactId) {
        this.reset(0l, contactId, 0l);
    }


    public long getRawContactId() {
        return mRawContactId;
    }

    public long getContactId() {
        return mContactId;
    }

    public long getAccountId() {
        return mAccountId;
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

    public boolean isKeepIn() {
        return mKeepIn;
    }

    public boolean isKeepOut() {
        return mKeepOut;
    }

    public int getPrimaryScore() {
        return mPrimaryScore;
    }

    public int getSecondaryScore() {
        return mSecondaryScore;
    }

    public void setPrimaryScore(int mPrimaryScore) {
        this.mPrimaryScore = mPrimaryScore;
    }

    /**
     * Descending order of match score.
     */
    @Override
    public int compareTo(MatchScore another) {
        return another.getScore() - getScore();
    }

    @Override
    public String toString() {
        return mRawContactId + "/" + mContactId + "/" + mAccountId + ": " + mPrimaryScore +
                "/" + mSecondaryScore + "(" + mMatchCount + ")";
    }
}
