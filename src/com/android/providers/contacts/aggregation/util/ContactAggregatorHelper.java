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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for contacts aggregation.
 */
public class ContactAggregatorHelper {

    private ContactAggregatorHelper() {}

    /**
     * If two connected components have disjoint accounts, merge them.
     * If there is any uncertainty, keep them separate.
     */
    @VisibleForTesting
    public static void mergeComponentsWithDisjointAccounts(Set<Set<Long>> connectedRawContactSets,
            Map<Long, Long> rawContactsToAccounts) {
        // Index to rawContactIds mapping
        final Map<Integer, Set<Long>> rawContactIds = new HashMap<>();
        // AccountId to indices mapping
        final Map<Long, Set<Integer>> accounts = new HashMap<>();

        int index = 0;
        for (Set<Long> rIds : connectedRawContactSets) {
            rawContactIds.put(index, rIds);
            for (Long rId : rIds) {
                long acctId = rawContactsToAccounts.get(rId);
                Set<Integer> s = accounts.get(acctId);
                if (s == null) {
                    s = new HashSet<Integer>();
                }
                s.add(index);
                accounts.put(acctId, s);
            }
            index++;
        }

        connectedRawContactSets.clear();
        for (Long accountId : accounts.keySet()) {
            final Set<Integer> s = accounts.get(accountId);
            if (s.size() > 1) {
                for (Integer i : s) {
                    final Set<Long> rIdSet = rawContactIds.get(i);
                    if (rIdSet != null && !rIdSet.isEmpty()) {
                        connectedRawContactSets.add(rIdSet);
                        rawContactIds.remove(i);
                    }
                }
            }
        }

        final Set<Long> mergedSet = new HashSet<>();
        for (Long accountId : accounts.keySet()) {
            final Set<Integer> s = accounts.get(accountId);
            if (s.size() == 1) {
                Set<Long> ids = rawContactIds.get(Iterables.getOnlyElement(s));
                if (ids != null && !ids.isEmpty()) {
                    mergedSet.addAll(ids);
                }
            }
        }
        connectedRawContactSets.add(mergedSet);
    }

    /**
     * Given a set of raw contact ids {@code rawContactIdSet} and the connection among them
     * {@code matchingRawIdPairs}, find the connected components.
     */
    @VisibleForTesting
    public static Set<Set<Long>> findConnectedComponents(Set<Long> rawContactIdSet, Multimap<Long,
            Long> matchingRawIdPairs) {
        Set<Set<Long>> connectedRawContactSets = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        for (Long id : rawContactIdSet) {
            if (!visited.contains(id)) {
                Set<Long> set = new HashSet<>();
                findConnectedComponentForRawContact(matchingRawIdPairs, visited, id, set);
                connectedRawContactSets.add(set);
            }
        }
        return connectedRawContactSets;
    }

    private static void findConnectedComponentForRawContact(Multimap<Long, Long> connections,
            Set<Long> visited, Long rawContactId, Set<Long> results) {
        visited.add(rawContactId);
        results.add(rawContactId);
        for (long match : connections.get(rawContactId)) {
            if (!visited.contains(match)) {
                findConnectedComponentForRawContact(connections, visited, match, results);
            }
        }
    }
}
