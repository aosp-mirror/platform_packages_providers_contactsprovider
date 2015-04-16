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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.android.internal.util.Preconditions.checkNotNull;

/**
 * Matching candidates for a raw contact, used in the contact aggregator.
 */
public class RawContactMatchingCandidates {
    private List<MatchScore> mBestMatches;
    private Set<Long> mRawContactIds = null;
    private Map<Long, Long> mRawContactToContact = null;
    private Map<Long, Long> mRawContactToAccount = null;

    public RawContactMatchingCandidates(List<MatchScore> mBestMatches) {
        checkNotNull(mBestMatches);
        this.mBestMatches = mBestMatches;
    }

    public RawContactMatchingCandidates() {
        mBestMatches = new ArrayList<MatchScore>();
    }

    public int getCount() {
        return mBestMatches.size();
    }

    public void add(MatchScore score) {
        mBestMatches.add(score);
        if (mRawContactIds != null) {
            mRawContactIds.add(score.getRawContactId());
        }
        if (mRawContactToAccount != null) {
            mRawContactToAccount.put(score.getRawContactId(), score.getAccountId());
        }
        if (mRawContactToContact != null) {
            mRawContactToContact.put(score.getRawContactId(), score.getContactId());
        }
    }

    public Set<Long> getRawContactIdSet() {
        if (mRawContactIds == null) {
            createRawContactIdSet();
        }
        return mRawContactIds;
    }

    public Map<Long, Long> getRawContactToAccount() {
        if (mRawContactToAccount == null) {
            createRawContactToAccountMap();
        }
        return mRawContactToAccount;
    }

    public Long getContactId(Long rawContactId) {
        if (mRawContactToContact == null) {
            createRawContactToContactMap();
        }
        return mRawContactToContact.get(rawContactId);
    }

    public Long getAccountId(Long rawContactId) {
        if (mRawContactToAccount == null) {
            createRawContactToAccountMap();
        }
        return mRawContactToAccount.get(rawContactId);
    }

    private void createRawContactToContactMap() {
        mRawContactToContact = new HashMap<Long, Long>();
        for (int i = 0; i < mBestMatches.size(); i++) {
            mRawContactToContact.put(mBestMatches.get(i).getRawContactId(),
                    mBestMatches.get(i).getContactId());
        }
    }

    private void createRawContactToAccountMap() {
        mRawContactToAccount = new HashMap<Long, Long>();
        for (int i = 0; i <  mBestMatches.size(); i++) {
            mRawContactToAccount.put(mBestMatches.get(i).getRawContactId(),
                    mBestMatches.get(i).getAccountId());
        }
    }

    private void createRawContactIdSet() {
        mRawContactIds = new HashSet<Long>();
        for (int i = 0; i < mBestMatches.size(); i++) {
            mRawContactIds.add(mBestMatches.get(i).getRawContactId());
        }
    }
}
