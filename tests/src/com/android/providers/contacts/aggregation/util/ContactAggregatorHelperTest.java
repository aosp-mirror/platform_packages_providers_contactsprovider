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

import android.test.MoreAsserts;
import android.test.suitebuilder.annotation.SmallTest;
import com.google.android.collect.Sets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SmallTest
public class ContactAggregatorHelperTest extends TestCase {

    private static final long ACCOUNT_1 = 1;
    private static final long ACCOUNT_2 = 2;
    private static final long ACCOUNT_3 = 3;
    private static final long ACCOUNT_4 = 4;
    private static final long ACCOUNT_5 = 5;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testMergeComponentsWithDisjointAccounts() {
        Set<Set<Long>> connectedRawContactSets = new HashSet<>();
        Map<Long, Long> rawContactsToAccounts = new HashMap<>();
        for (long i = 100; i < 108; ) {
            Set<Long> rawContactSet = new HashSet<>();
            rawContactSet.add(i++);
            rawContactSet.add(i++);
            connectedRawContactSets.add(rawContactSet);
        }

        rawContactsToAccounts.put(100l, ACCOUNT_1);
        rawContactsToAccounts.put(101l, ACCOUNT_1);
        rawContactsToAccounts.put(102l, ACCOUNT_1);
        rawContactsToAccounts.put(103l, ACCOUNT_2);
        rawContactsToAccounts.put(104l, ACCOUNT_3);
        rawContactsToAccounts.put(105l, ACCOUNT_4);
        rawContactsToAccounts.put(106l, ACCOUNT_5);
        rawContactsToAccounts.put(107l, ACCOUNT_5);
        // Component1: [rawContactId=100, accountId=1; raw_contactId=101, accountId=1]
        // Component2: [rawContactId=102, accountId=1; raw_contactId=103, accountId=2]
        // Component3: [rawContactId=104, accountId=3; raw_contactId=105, accountId=4]
        // Component4: [rawContactId=106, accountId=5; raw_contactId=107, accountId=5]
        // Component3 and component4 can be merged because they have disjoint accounts, but cannot
        // merge with either component1 or component2 due to the uncertainty.

        ContactAggregatorHelper.mergeComponentsWithDisjointAccounts(connectedRawContactSets,
                rawContactsToAccounts);

        MoreAsserts.assertContentsInAnyOrder(connectedRawContactSets, Sets.newHashSet(100l,
                101l), Sets.newHashSet(102l, 103l), Sets.newHashSet(104l, 105l, 106l, 107l));
    }

    public void testMergeComponentsWithDisjointAccounts2() {
        Set<Set<Long>> connectedRawContactSets = new HashSet<>();
        Map<Long, Long> rawContactsToAccounts = new HashMap<>();

        Set<Long> rawContactSet1 = new HashSet<>();
        rawContactSet1.add(102l);
        connectedRawContactSets.add(rawContactSet1);
        Set<Long> rawContactSet2 = new HashSet<>();
        rawContactSet2.add(100l);
        rawContactSet2.add(101l);
        connectedRawContactSets.add(rawContactSet2);
        Set<Long> rawContactSet3 = new HashSet<>();
        rawContactSet3.add(103l);;
        connectedRawContactSets.add(rawContactSet3);

        rawContactsToAccounts.put(100l, ACCOUNT_1);
        rawContactsToAccounts.put(101l, ACCOUNT_2);
        rawContactsToAccounts.put(102l, ACCOUNT_3);
        rawContactsToAccounts.put(103l, ACCOUNT_1);
        // Component1: [rawContactId=100, accountId=1; raw_contactId=101, accountId=2]
        // Component2: [rawContactId=102, accountId=3]
        // Component3: [rawContactId=103, accountId=1]
        // None of them can be merged
        ContactAggregatorHelper.mergeComponentsWithDisjointAccounts(connectedRawContactSets,
                rawContactsToAccounts);

        MoreAsserts.assertContentsInAnyOrder(connectedRawContactSets, Sets.newHashSet(100l,
                101l), Sets.newHashSet(102l), Sets.newHashSet(103l));
    }

    public void testFindConnectedRawContacts() {
        Set<Long> rawContactIdSet = new HashSet<>();
        rawContactIdSet.addAll(Arrays.asList(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l));

        Multimap<Long, Long> matchingrawIdPairs = HashMultimap.create();
        matchingrawIdPairs.put(1l, 2l);
        matchingrawIdPairs.put(2l, 1l);

        matchingrawIdPairs.put(1l, 7l);
        matchingrawIdPairs.put(7l, 1l);

        matchingrawIdPairs.put(2l, 3l);
        matchingrawIdPairs.put(3l, 2l);

        matchingrawIdPairs.put(2l, 8l);
        matchingrawIdPairs.put(8l, 2l);

        matchingrawIdPairs.put(8l, 9l);
        matchingrawIdPairs.put(9l, 8l);

        matchingrawIdPairs.put(4l, 5l);
        matchingrawIdPairs.put(5l, 4l);

        Set<Set<Long>> actual = ContactAggregatorHelper.findConnectedComponents(rawContactIdSet,
                matchingrawIdPairs);

        Set<Set<Long>> expected = new HashSet<>();
        Set<Long> result1 = new HashSet<>();
        result1.addAll(Arrays.asList(1l, 2l, 3l, 7l, 8l, 9l));
        Set<Long> result2 = new HashSet<>();
        result2.addAll(Arrays.asList(4l, 5l));
        Set<Long> result3 = new HashSet<>();
        result3.addAll(Arrays.asList(6l));
        expected.addAll(Arrays.asList(result1, result2, result3));

        assertEquals(expected, actual);
    }
}
