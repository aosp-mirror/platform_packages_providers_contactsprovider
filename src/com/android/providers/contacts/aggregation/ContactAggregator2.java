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

package com.android.providers.contacts.aggregation;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.FullNameStyle;
import android.provider.ContactsContract.PhotoFiles;
import android.provider.ContactsContract.RawContacts;
import android.text.TextUtils;
import android.util.Log;
import com.android.providers.contacts.ContactsDatabaseHelper;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.ContactsProvider2;
import com.android.providers.contacts.NameSplitter;
import com.android.providers.contacts.PhotoPriorityResolver;
import com.android.providers.contacts.TransactionContext;
import com.android.providers.contacts.aggregation.util.CommonNicknameCache;
import com.android.providers.contacts.aggregation.util.ContactAggregatorHelper;
import com.android.providers.contacts.aggregation.util.MatchScore;
import com.android.providers.contacts.aggregation.util.RawContactMatcher;
import com.android.providers.contacts.aggregation.util.RawContactMatchingCandidates;
import com.android.providers.contacts.database.ContactsTableUtil;
import com.google.android.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.android.providers.contacts.aggregation.util.RawContactMatcher
        .SCORE_THRESHOLD_PRIMARY;
import static com.android.providers.contacts.aggregation.util.RawContactMatcher
        .SCORE_THRESHOLD_SECONDARY;
import static com.android.providers.contacts.aggregation.util.RawContactMatcher
        .SCORE_THRESHOLD_SUGGEST;

/**
 * ContactAggregator2 deals with aggregating contact information with sufficient matching data
 * points. E.g., two John Doe contacts with same phone numbers are presumed to be the same
 * person unless the user declares otherwise.
 */
public class ContactAggregator2 extends AbstractContactAggregator {

    // Possible operation types for contacts aggregation.
    private static final int CREATE_NEW_CONTACT = 1;
    private static final int KEEP_INTACT = 0;
    private static final int RE_AGGREGATE = -1;

    private final RawContactMatcher mMatcher = new RawContactMatcher();

    /**
     * Constructor.
     */
    public ContactAggregator2(ContactsProvider2 contactsProvider,
            ContactsDatabaseHelper contactsDatabaseHelper,
            PhotoPriorityResolver photoPriorityResolver, NameSplitter nameSplitter,
            CommonNicknameCache commonNicknameCache) {
        super(contactsProvider, contactsDatabaseHelper, photoPriorityResolver, nameSplitter,
                commonNicknameCache);
    }

    @SuppressWarnings("deprecation")
    public void triggerAggregation(TransactionContext txContext, long rawContactId) {
        if (!mEnabled) {
            return;
        }

        int aggregationMode = mDbHelper.getAggregationMode(rawContactId);
        switch (aggregationMode) {
            case RawContacts.AGGREGATION_MODE_DISABLED:
                break;

            case RawContacts.AGGREGATION_MODE_DEFAULT: {
                markForAggregation(rawContactId, aggregationMode, false);
                break;
            }

            case RawContacts.AGGREGATION_MODE_SUSPENDED: {
                long contactId = mDbHelper.getContactId(rawContactId);

                if (contactId != 0) {
                    updateAggregateData(txContext, contactId);
                }
                break;
            }

            case RawContacts.AGGREGATION_MODE_IMMEDIATE: {
                aggregateContact(txContext, mDbHelper.getWritableDatabase(), rawContactId);
                break;
            }
        }
    }

    private static class RawContactIdAndAggregationModeQuery {
        public static final String TABLE = Tables.RAW_CONTACTS;

        public static final String[] COLUMNS = { RawContacts._ID, RawContacts.AGGREGATION_MODE };

        public static final String SELECTION = RawContacts.CONTACT_ID + "=?";

        public static final int _ID = 0;
        public static final int AGGREGATION_MODE = 1;
    }

    /**
     * Given a specific raw contact, finds all matching raw contacts and re-aggregate them
     * based on the matching connectivity.
     */
     synchronized void aggregateContact(TransactionContext txContext, SQLiteDatabase db,
             long rawContactId, long accountId, long currentContactId,
             MatchCandidateList candidates) {

         if (VERBOSE_LOGGING) {
            Log.v(TAG, "aggregateContact: rid=" + rawContactId + " cid=" + currentContactId);
        }

        int aggregationMode = RawContacts.AGGREGATION_MODE_DEFAULT;

        Integer aggModeObject = mRawContactsMarkedForAggregation.remove(rawContactId);
        if (aggModeObject != null) {
            aggregationMode = aggModeObject;
        }

        RawContactMatcher matcher = new RawContactMatcher();
        RawContactMatchingCandidates matchingCandidates = new RawContactMatchingCandidates();
        if (aggregationMode == RawContacts.AGGREGATION_MODE_DEFAULT) {
            // Find the set of matching candidates
            matchingCandidates = findRawContactMatchingCandidates(db, rawContactId, candidates,
                    matcher);
        } else if (aggregationMode == RawContacts.AGGREGATION_MODE_DISABLED) {
            return;
        }

        // # of raw_contacts in the [currentContactId] contact excluding the [rawContactId]
        // raw_contact.
        long currentContactContentsCount = 0;

        if (currentContactId != 0) {
            mRawContactCountQuery.bindLong(1, currentContactId);
            mRawContactCountQuery.bindLong(2, rawContactId);
            currentContactContentsCount = mRawContactCountQuery.simpleQueryForLong();
        }

        // Set aggregation operation, i.e., re-aggregate, keep intact, or create new contact based
        // on the number of matching candidates and the number of raw_contacts in the
        // [currentContactId] excluding the [rawContactId].
        final int operation;
        final int candidatesCount = matchingCandidates.getCount();
        if (candidatesCount >= AGGREGATION_CONTACT_SIZE_LIMIT) {
            operation = KEEP_INTACT;
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Too many matching raw contacts (" + candidatesCount
                        + ") are found, so skip aggregation");
            }
        } else if (candidatesCount > 0) {
            operation = RE_AGGREGATE;
        } else {
            // When there is no matching raw contact found, if there are no other raw contacts in
            // the current aggregate, we might as well reuse it. Also, if the aggregation mode is
            // SUSPENDED, we must reuse the same aggregate.
            if (currentContactId != 0
                    && (currentContactContentsCount == 0
                    || aggregationMode == RawContacts.AGGREGATION_MODE_SUSPENDED)) {
                operation = KEEP_INTACT;
            } else {
                operation = CREATE_NEW_CONTACT;
            }
        }

        if (operation == KEEP_INTACT) {
            // Aggregation unchanged
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Aggregation unchanged");
            }
            markAggregated(rawContactId);
        } else if (operation == CREATE_NEW_CONTACT) {
            // create new contact for [rawContactId]
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "create new contact for rid=" + rawContactId);
            }
            createContactForRawContacts(db, txContext, Sets.newHashSet(rawContactId), null);
            if (currentContactContentsCount > 0) {
                updateAggregateData(txContext, currentContactId);
            }
        } else {
            // re-aggregate
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Re-aggregating rids=" + rawContactId + ","
                        + TextUtils.join(",", matchingCandidates.getRawContactIdSet()));
            }
            reAggregateRawContacts(txContext, db, currentContactId, rawContactId, accountId,
                    currentContactContentsCount, matchingCandidates);
        }
    }

    /**
     * Find the set of matching raw contacts for given rawContactId. Add all the raw contact
     * candidates with matching scores > threshold to RawContactMatchingCandidates. Keep doing
     * this for every raw contact in RawContactMatchingCandidates until is it not changing.
     */
    private RawContactMatchingCandidates findRawContactMatchingCandidates(SQLiteDatabase db, long
            rawContactId, MatchCandidateList candidates, RawContactMatcher matcher) {
        updateMatchScoresForSuggestionsBasedOnDataMatches(db, rawContactId, candidates,
                matcher);
        final RawContactMatchingCandidates matchingCandidates = new RawContactMatchingCandidates(
                matcher.pickBestMatches(SCORE_THRESHOLD_SUGGEST));

        Set<Long> newIds = new HashSet<>();
        newIds.addAll(matchingCandidates.getRawContactIdSet());
        // Keep doing the following until no new raw contact candidate is found.
        // TODO: may need to cache the matching score to improve performance.
        while (!newIds.isEmpty()) {
            final Set<Long> tmpIdSet = new HashSet<>();
            for (long rId : newIds) {
                final RawContactMatcher rMatcher = new RawContactMatcher();
                updateMatchScoresForSuggestionsBasedOnDataMatches(db, rId, new MatchCandidateList(),
                        rMatcher);
                List<MatchScore> newMatches = rMatcher.pickBestMatches(SCORE_THRESHOLD_SUGGEST);
                for (MatchScore newMatch : newMatches) {
                    final long newRawContactId = newMatch.getRawContactId();
                    if (!matchingCandidates.getRawContactIdSet().contains(newRawContactId)) {
                        tmpIdSet.add(newRawContactId);
                        matchingCandidates.add(newMatch);
                    }
                }
            }
            newIds.clear();
            newIds.addAll(tmpIdSet);
        }
        return matchingCandidates;
    }

    /**
     * Find out which mime-types are shared by more than one contacts for {@code rawContactIds}.
     * Clear the is_super_primary settings for these mime-types.
     * {@code rawContactIds} should be a comma separated ID list.
     */
     private void clearSuperPrimarySetting(SQLiteDatabase db, String rawContactIds) {
        final String sql =
                "SELECT d." + DataColumns.MIMETYPE_ID + ", count(DISTINCT r." +
                        RawContacts.CONTACT_ID + ") c  FROM " + Tables.DATA + " d JOIN " +
                        Tables.RAW_CONTACTS + " r on d." + Data.RAW_CONTACT_ID + " = r." +
                        RawContacts._ID +" WHERE d." + Data.IS_SUPER_PRIMARY + " = 1 AND d." +
                        Data.RAW_CONTACT_ID + " IN (" + rawContactIds + ") group by d." +
                        DataColumns.MIMETYPE_ID + " having c > 1";

        // Find out which mime-types exist with is_super_primary=true on more then one contacts.
        int index = 0;
        final StringBuilder mimeTypeCondition = new StringBuilder();
        mimeTypeCondition.append(" AND " + DataColumns.MIMETYPE_ID + " IN (");

        final Cursor c = db.rawQuery(sql, null);
        try {
            c.moveToPosition(-1);
            while (c.moveToNext()) {
                if (index > 0) {
                    mimeTypeCondition.append(',');
                }
                mimeTypeCondition.append(c.getLong((0)));
                index++;
            }
        } finally {
            c.close();
        }

        if (index == 0) {
            return;
        }

        // Clear is_super_primary setting for all the mime-types with is_super_primary=true
        // in both raw contact of rawContactId and raw contacts of contactId
        String superPrimaryUpdateSql = "UPDATE " + Tables.DATA +
                " SET " + Data.IS_SUPER_PRIMARY + "=0" +
                " WHERE (" +  Data.RAW_CONTACT_ID +
                " IN (SELECT " + RawContacts._ID +  " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + Data.RAW_CONTACT_ID + " IN (" + rawContactIds + ")";

        mimeTypeCondition.append(')');
        superPrimaryUpdateSql += mimeTypeCondition.toString();
        db.execSQL(superPrimaryUpdateSql, null);
    }

    private String buildExceptionMatchingSql(String rawContactIdSet1, String rawContactIdSet2,
            int aggregationType, boolean countOnly) {
        final String idPairSelection =  "SELECT " + AggregationExceptions.RAW_CONTACT_ID1 + ", " +
                AggregationExceptions.RAW_CONTACT_ID2;
        final String sql =
                " FROM " + Tables.AGGREGATION_EXCEPTIONS +
                " WHERE " + AggregationExceptions.RAW_CONTACT_ID1 + " IN (" +
                        rawContactIdSet1 + ")" +
                " AND " + AggregationExceptions.RAW_CONTACT_ID2 + " IN (" + rawContactIdSet2 + ")" +
                " AND " + AggregationExceptions.TYPE + "=" + aggregationType;
        return (countOnly) ? RawContactMatchingSelectionStatement.SELECT_COUNT + sql :
                idPairSelection + sql;
    }

    /**
     * Re-aggregate rawContact of {@code rawContactId} and all the raw contacts of
     * {@code matchingCandidates} into connected components. This only happens when a given
     * raw contacts cannot be joined with its best matching contacts directly.
     *
     *  Two raw contacts are considered connected if they share at least one email address, phone
     *  number or identity. Create new contact for each connected component except the very first
     *  one that doesn't contain rawContactId of {@code rawContactId}.
     */
    private void reAggregateRawContacts(TransactionContext txContext, SQLiteDatabase db,
            long currentCidForRawContact, long rawContactId, long accountId,
            long currentContactContentsCount, RawContactMatchingCandidates matchingCandidates) {
        // Find the connected component based on the aggregation exceptions or
        // identity/email/phone matching for all the raw contacts of [contactId] and the give
        // raw contact.
        final Set<Long> allIds = new HashSet<>();
        allIds.add(rawContactId);
        allIds.addAll(matchingCandidates.getRawContactIdSet());
        final Set<Set<Long>> connectedRawContactSets = findConnectedRawContacts(db, allIds);

        final Map<Long, Long> rawContactsToAccounts = matchingCandidates.getRawContactToAccount();
        rawContactsToAccounts.put(rawContactId, accountId);
        ContactAggregatorHelper.mergeComponentsWithDisjointAccounts(connectedRawContactSets,
                rawContactsToAccounts);
        breakComponentsByExceptions(db, connectedRawContactSets);

        // Create new contact for each connected component. Use the first reusable contactId if
        // possible. If no reusable contactId found, create new contact for the connected component.
        // Update aggregate data for all the contactIds touched by this connected component,
        for (Set<Long> connectedRawContactIds : connectedRawContactSets) {
            Long contactId = null;
            Set<Long> cidsNeedToBeUpdated = new HashSet<>();
            if (connectedRawContactIds.contains(rawContactId)) {
                // If there is no other raw contacts aggregated with the given raw contact currently
                // or all the raw contacts in [currentCidForRawContact] are still in the same
                // connected component, we might as well reuse it.
                if (currentCidForRawContact != 0 &&
                        (currentContactContentsCount == 0) ||
                        canBeReused(db, currentCidForRawContact, connectedRawContactIds)) {
                    contactId = currentCidForRawContact;
                } else if (currentCidForRawContact != 0){
                    cidsNeedToBeUpdated.add(currentCidForRawContact);
                }
            } else {
                boolean foundContactId = false;
                for (Long connectedRawContactId : connectedRawContactIds) {
                    Long currentContactId = matchingCandidates.getContactId(connectedRawContactId);
                    if (!foundContactId && currentContactId != null &&
                            canBeReused(db, currentContactId, connectedRawContactIds)) {
                        contactId = currentContactId;
                        foundContactId = true;
                    } else {
                        cidsNeedToBeUpdated.add(currentContactId);
                    }
                }
            }
            createContactForRawContacts(db, txContext, connectedRawContactIds, contactId);
            clearSuperPrimarySetting(db, TextUtils.join(",", connectedRawContactIds));
            for (Long cid : cidsNeedToBeUpdated) {
                long currentRcCount = 0;
                if (cid != 0) {
                    mRawContactCountQuery.bindLong(1, cid);
                    mRawContactCountQuery.bindLong(2, 0);
                    currentRcCount = mRawContactCountQuery.simpleQueryForLong();
                }

                if (currentRcCount == 0) {
                    // Delete a contact if it doesn't contain anything
                    ContactsTableUtil.deleteContact(db, cid);
                    mAggregatedPresenceDelete.bindLong(1, cid);
                    mAggregatedPresenceDelete.execute();
                } else {
                    updateAggregateData(txContext, cid);
                }
            }
        }
    }

    /**
     * Check if contactId can be reused as the contact Id for new aggregation of all the
     * connectedRawContactIds. If connectedRawContactIds set contains all the raw contacts
     * currently aggregated under contactId, return true; Otherwise, return false.
     */
    private boolean canBeReused(SQLiteDatabase db, Long contactId,
            Set<Long> connectedRawContactIds) {
        final String sql = "SELECT " + RawContactsColumns.CONCRETE_ID + " FROM " +
                Tables.RAW_CONTACTS + " WHERE " + RawContacts.CONTACT_ID + "=? AND " +
                RawContacts.DELETED + "=0";
        mSelectionArgs1[0] = String.valueOf(contactId);
        final Cursor cursor = db.rawQuery(sql, mSelectionArgs1);
        try {
            cursor.moveToPosition(-1);
            while (cursor.moveToNext()) {
                if (!connectedRawContactIds.contains(cursor.getLong(0))) {
                    return false;
                }
            }
        } finally {
            cursor.close();
        }
        return true;
    }

    /**
     * Separate all the raw_contacts which has "SEPARATE" aggregation exception to another
     * raw_contacts in the same component.
     */
    private void breakComponentsByExceptions(SQLiteDatabase db,
            Set<Set<Long>> connectedRawContacts) {
        final Set<Set<Long>> tmpSets = new HashSet<>(connectedRawContacts);
        for (Set<Long> component : tmpSets) {
            final String rawContacts = TextUtils.join(",", component);
            // If "SEPARATE" exception is found inside an connected component [component],
            // remove the [component] from [connectedRawContacts], and create a new connected
            // component for each raw contact of [component] and add to [connectedRawContacts].
            if (isFirstColumnGreaterThanZero(db, buildExceptionMatchingSql(rawContacts, rawContacts,
                    AggregationExceptions.TYPE_KEEP_SEPARATE, /* countOnly =*/true))) {
                connectedRawContacts.remove(component);
                for (Long rId : component) {
                    final Set<Long> s= new HashSet<>();
                    s.add(rId);
                    connectedRawContacts.add(s);
                }
            }
        }
    }

    /**
     * Ensures that automatic aggregation rules are followed after a contact
     * becomes visible or invisible. Specifically, consider this case: there are
     * three contacts named Foo. Two of them come from account A1 and one comes
     * from account A2. The aggregation rules say that in this case none of the
     * three Foo's should be aggregated: two of them are in the same account, so
     * they don't get aggregated; the third has two affinities, so it does not
     * join either of them.
     * <p>
     * Consider what happens if one of the "Foo"s from account A1 becomes
     * invisible. Nothing stands in the way of aggregating the other two
     * anymore, so they should get joined.
     * <p>
     * What if the invisible "Foo" becomes visible after that? We should split the
     * aggregate between the other two.
     */
    public void updateAggregationAfterVisibilityChange(long contactId) {
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        boolean visible = mDbHelper.isContactInDefaultDirectory(db, contactId);
        if (visible) {
            markContactForAggregation(db, contactId);
        } else {
            // Find all contacts that _could be_ aggregated with this one and
            // rerun aggregation for all of them
            mSelectionArgs1[0] = String.valueOf(contactId);
            Cursor cursor = db.query(RawContactIdQuery.TABLE, RawContactIdQuery.COLUMNS,
                    RawContactIdQuery.SELECTION, mSelectionArgs1, null, null, null);
            try {
                while (cursor.moveToNext()) {
                    long rawContactId = cursor.getLong(RawContactIdQuery.RAW_CONTACT_ID);
                    mMatcher.clear();

                    updateMatchScoresBasedOnIdentityMatch(db, rawContactId, mMatcher);
                    updateMatchScoresBasedOnNameMatches(db, rawContactId, mMatcher);
                    List<MatchScore> bestMatches =
                            mMatcher.pickBestMatches(SCORE_THRESHOLD_PRIMARY);
                    for (MatchScore matchScore : bestMatches) {
                        markContactForAggregation(db, matchScore.getContactId());
                    }

                    mMatcher.clear();
                    updateMatchScoresBasedOnEmailMatches(db, rawContactId, mMatcher);
                    updateMatchScoresBasedOnPhoneMatches(db, rawContactId, mMatcher);
                    bestMatches =
                            mMatcher.pickBestMatches(SCORE_THRESHOLD_SECONDARY);
                    for (MatchScore matchScore : bestMatches) {
                        markContactForAggregation(db, matchScore.getContactId());
                    }
                }
            } finally {
                cursor.close();
            }
        }
    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     */
    private void updateMatchScoresBasedOnExceptions(SQLiteDatabase db, long rawContactId,
            RawContactMatcher matcher) {
        if (!mAggregationExceptionIdsValid) {
            prefetchAggregationExceptionIds(db);
        }

        // If there are no aggregation exceptions involving this raw contact, there is no need to
        // run a query and we can just return -1, which stands for "nothing found"
        if (!mAggregationExceptionIds.contains(rawContactId)) {
            return;
        }

        final Cursor c = db.query(AggregateExceptionQuery.TABLE,
                AggregateExceptionQuery.COLUMNS,
                AggregationExceptions.RAW_CONTACT_ID1 + "=" + rawContactId
                        + " OR " + AggregationExceptions.RAW_CONTACT_ID2 + "=" + rawContactId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int type = c.getInt(AggregateExceptionQuery.TYPE);
                long rawContactId1 = c.getLong(AggregateExceptionQuery.RAW_CONTACT_ID1);
                long contactId = -1;
                long rId = -1;
                long accountId = -1;
                if (rawContactId == rawContactId1) {
                    if (c.getInt(AggregateExceptionQuery.AGGREGATION_NEEDED_2) == 0
                            && !c.isNull(AggregateExceptionQuery.RAW_CONTACT_ID2)) {
                        rId = c.getLong(AggregateExceptionQuery.RAW_CONTACT_ID2);
                        contactId = c.getLong(AggregateExceptionQuery.CONTACT_ID2);
                        accountId = c.getLong(AggregateExceptionQuery.ACCOUNT_ID2);
                    }
                } else {
                    if (c.getInt(AggregateExceptionQuery.AGGREGATION_NEEDED_1) == 0
                            && !c.isNull(AggregateExceptionQuery.RAW_CONTACT_ID1)) {
                        rId = c.getLong(AggregateExceptionQuery.RAW_CONTACT_ID1);
                        contactId = c.getLong(AggregateExceptionQuery.CONTACT_ID1);
                        accountId = c.getLong(AggregateExceptionQuery.ACCOUNT_ID1);
                    }
                }
                if (rId != -1) {
                    if (type == AggregationExceptions.TYPE_KEEP_TOGETHER) {
                        matcher.keepIn(rId, contactId, accountId);
                    } else {
                        matcher.keepOut(rId, contactId, accountId);
                    }
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds contacts with exact identity matches to the the specified raw contact.
     */
    private void updateMatchScoresBasedOnIdentityMatch(SQLiteDatabase db, long rawContactId,
            RawContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = String.valueOf(mMimeTypeIdIdentity);
        Cursor c = db.query(IdentityLookupMatchQuery.TABLE, IdentityLookupMatchQuery.COLUMNS,
                IdentityLookupMatchQuery.SELECTION,
                mSelectionArgs2, RawContacts.CONTACT_ID, null, null);
        try {
            while (c.moveToNext()) {
                final long rId = c.getLong(IdentityLookupMatchQuery.RAW_CONTACT_ID);
                final long contactId = c.getLong(IdentityLookupMatchQuery.CONTACT_ID);
                final long accountId = c.getLong(IdentityLookupMatchQuery.ACCOUNT_ID);
                matcher.matchIdentity(rId, contactId, accountId);
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds contacts with names matching the name of the specified raw contact.
     */
    private void updateMatchScoresBasedOnNameMatches(SQLiteDatabase db, long rawContactId,
            RawContactMatcher matcher) {
        mSelectionArgs1[0] = String.valueOf(rawContactId);
        Cursor c = db.query(NameLookupMatchQuery.TABLE, NameLookupMatchQuery.COLUMNS,
                NameLookupMatchQuery.SELECTION,
                mSelectionArgs1, null, null, null, PRIMARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long rId =  c.getLong(NameLookupMatchQuery.RAW_CONTACT_ID);
                long contactId = c.getLong(NameLookupMatchQuery.CONTACT_ID);
                long accountId = c.getLong(NameLookupMatchQuery.ACCOUNT_ID);
                String name = c.getString(NameLookupMatchQuery.NAME);
                int nameTypeA = c.getInt(NameLookupMatchQuery.NAME_TYPE_A);
                int nameTypeB = c.getInt(NameLookupMatchQuery.NAME_TYPE_B);
                matcher.matchName(rId, contactId, accountId, nameTypeA, name,
                        nameTypeB, name, RawContactMatcher.MATCHING_ALGORITHM_EXACT);
                if (nameTypeA == NameLookupType.NICKNAME &&
                        nameTypeB == NameLookupType.NICKNAME) {
                    matcher.updateScoreWithNicknameMatch(rId, contactId, accountId);
                }
            }
        } finally {
            c.close();
        }
    }

    private void updateMatchScoresBasedOnEmailMatches(SQLiteDatabase db, long rawContactId,
            RawContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = String.valueOf(mMimeTypeIdEmail);
        Cursor c = db.query(EmailLookupQuery.TABLE, EmailLookupQuery.COLUMNS,
                EmailLookupQuery.SELECTION,
                mSelectionArgs2, null, null, null, SECONDARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long rId = c.getLong(EmailLookupQuery.RAW_CONTACT_ID);
                long contactId = c.getLong(EmailLookupQuery.CONTACT_ID);
                long accountId = c.getLong(EmailLookupQuery.ACCOUNT_ID);
                matcher.updateScoreWithEmailMatch(rId, contactId, accountId);
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds contacts with names matching the specified name.
     */
    private void updateMatchScoresBasedOnNameMatches(SQLiteDatabase db, String query,
            MatchCandidateList candidates, RawContactMatcher matcher) {
        candidates.clear();
        NameLookupSelectionBuilder builder = new NameLookupSelectionBuilder(
                mNameSplitter, candidates);
        builder.insertNameLookup(0, 0, query, FullNameStyle.UNDEFINED);
        if (builder.isEmpty()) {
            return;
        }

        Cursor c = db.query(NameLookupMatchQueryWithParameter.TABLE,
                NameLookupMatchQueryWithParameter.COLUMNS, builder.getSelection(), null, null, null,
                null, PRIMARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long rId = c.getLong(NameLookupMatchQueryWithParameter.RAW_CONTACT_ID);
                long contactId = c.getLong(NameLookupMatchQueryWithParameter.CONTACT_ID);
                long accountId = c.getLong(NameLookupMatchQueryWithParameter.ACCOUNT_ID);
                String name = c.getString(NameLookupMatchQueryWithParameter.NAME);
                int nameTypeA = builder.getLookupType(name);
                int nameTypeB = c.getInt(NameLookupMatchQueryWithParameter.NAME_TYPE);
                matcher.matchName(rId, contactId, accountId, nameTypeA, name, nameTypeB, name,
                        RawContactMatcher.MATCHING_ALGORITHM_EXACT);
                if (nameTypeA == NameLookupType.NICKNAME && nameTypeB == NameLookupType.NICKNAME) {
                    matcher.updateScoreWithNicknameMatch(rId, contactId, accountId);
                }
            }
        } finally {
            c.close();
        }
    }

    private void updateMatchScoresBasedOnPhoneMatches(SQLiteDatabase db, long rawContactId,
            RawContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = mDbHelper.getUseStrictPhoneNumberComparisonParameter();
        Cursor c = db.query(PhoneLookupQuery.TABLE, PhoneLookupQuery.COLUMNS,
                PhoneLookupQuery.SELECTION,
                mSelectionArgs2, null, null, null, SECONDARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long rId = c.getLong(PhoneLookupQuery.RAW_CONTACT_ID);
                long contactId = c.getLong(PhoneLookupQuery.CONTACT_ID);
                long accountId = c.getLong(PhoneLookupQuery.ACCOUNT_ID);
                matcher.updateScoreWithPhoneNumberMatch(rId, contactId, accountId);
            }
        } finally {
            c.close();
        }
    }

    /**
     * Loads name lookup rows for approximate name matching and updates match scores based on that
     * data.
     */
    private void lookupApproximateNameMatches(SQLiteDatabase db, MatchCandidateList candidates,
            RawContactMatcher matcher) {
        HashSet<String> firstLetters = new HashSet<>();
        for (int i = 0; i < candidates.mCount; i++) {
            final NameMatchCandidate candidate = candidates.mList.get(i);
            if (candidate.mName.length() >= 2) {
                String firstLetter = candidate.mName.substring(0, 2);
                if (!firstLetters.contains(firstLetter)) {
                    firstLetters.add(firstLetter);
                    final String selection = "(" + NameLookupColumns.NORMALIZED_NAME + " GLOB '"
                            + firstLetter + "*') AND "
                            + "(" + NameLookupColumns.NAME_TYPE + " IN("
                                    + NameLookupType.NAME_COLLATION_KEY + ","
                                    + NameLookupType.EMAIL_BASED_NICKNAME + ","
                                    + NameLookupType.NICKNAME + ")) AND "
                            + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY;
                    matchAllCandidates(db, selection, candidates, matcher,
                            RawContactMatcher.MATCHING_ALGORITHM_APPROXIMATE,
                            String.valueOf(FIRST_LETTER_SUGGESTION_HIT_LIMIT));
                }
            }
        }
    }

    private interface ContactNameLookupQuery {
        String TABLE = Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                RawContacts._ID,
                RawContacts.CONTACT_ID,
                RawContactsColumns.ACCOUNT_ID,
                NameLookupColumns.NORMALIZED_NAME,
                NameLookupColumns.NAME_TYPE
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
        int NORMALIZED_NAME = 3;
        int NAME_TYPE = 4;
    }

    /**
     * Loads all candidate rows from the name lookup table and updates match scores based
     * on that data.
     */
    private void matchAllCandidates(SQLiteDatabase db, String selection,
            MatchCandidateList candidates, RawContactMatcher matcher, int algorithm, String limit) {
        final Cursor c = db.query(ContactNameLookupQuery.TABLE, ContactNameLookupQuery.COLUMNS,
                selection, null, null, null, null, limit);

        try {
            while (c.moveToNext()) {
                Long rawContactId = c.getLong(ContactNameLookupQuery.RAW_CONTACT_ID);
                Long contactId = c.getLong(ContactNameLookupQuery.CONTACT_ID);
                Long accountId = c.getLong(ContactNameLookupQuery.ACCOUNT_ID);
                String name = c.getString(ContactNameLookupQuery.NORMALIZED_NAME);
                int nameType = c.getInt(ContactNameLookupQuery.NAME_TYPE);

                // Note the N^2 complexity of the following fragment. This is not a huge concern
                // since the number of candidates is very small and in general secondary hits
                // in the absence of primary hits are rare.
                for (int i = 0; i < candidates.mCount; i++) {
                    NameMatchCandidate candidate = candidates.mList.get(i);
                    matcher.matchName(rawContactId, contactId, accountId, candidate.mLookupType,
                            candidate.mName, nameType, name, algorithm);
                }
            }
        } finally {
            c.close();
        }
    }

    private interface PhotoFileQuery {
        final String[] COLUMNS = new String[] {
                PhotoFiles.HEIGHT,
                PhotoFiles.WIDTH,
                PhotoFiles.FILESIZE
        };

        int HEIGHT = 0;
        int WIDTH = 1;
        int FILESIZE = 2;
    }

    private class PhotoEntry implements Comparable<PhotoEntry> {
        // Pixel count (width * height) for the image.
        final int pixelCount;

        // File size (in bytes) of the image.  Not populated if the image is a thumbnail.
        final int fileSize;

        private PhotoEntry(int pixelCount, int fileSize) {
            this.pixelCount = pixelCount;
            this.fileSize = fileSize;
        }

        @Override
        public int compareTo(PhotoEntry pe) {
            if (pe == null) {
                return -1;
            }
            if (pixelCount == pe.pixelCount) {
                return pe.fileSize - fileSize;
            } else {
                return pe.pixelCount - pixelCount;
            }
        }
    }

    private PhotoEntry getPhotoMetadata(SQLiteDatabase db, long photoFileId) {
        if (photoFileId == 0) {
            // Assume standard thumbnail size.  Don't bother getting a file size for priority;
            // we should fall back to photo priority resolver if all we have are thumbnails.
            int thumbDim = mContactsProvider.getMaxThumbnailDim();
            return new PhotoEntry(thumbDim * thumbDim, 0);
        } else {
            Cursor c = db.query(Tables.PHOTO_FILES, PhotoFileQuery.COLUMNS, PhotoFiles._ID + "=?",
                    new String[]{String.valueOf(photoFileId)}, null, null, null);
            try {
                if (c.getCount() == 1) {
                    c.moveToFirst();
                    int pixelCount =
                            c.getInt(PhotoFileQuery.HEIGHT) * c.getInt(PhotoFileQuery.WIDTH);
                    return new PhotoEntry(pixelCount, c.getInt(PhotoFileQuery.FILESIZE));
                }
            } finally {
                c.close();
            }
        }
        return new PhotoEntry(0, 0);
    }
    /**
     * Finds contacts with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     * @param parameters
     */
    protected List<MatchScore> findMatchingContacts(final SQLiteDatabase db, long contactId,
            ArrayList<AggregationSuggestionParameter> parameters) {

        MatchCandidateList candidates = new MatchCandidateList();
        RawContactMatcher matcher = new RawContactMatcher();

        if (parameters == null || parameters.size() == 0) {
            final Cursor c = db.query(RawContactIdQuery.TABLE, RawContactIdQuery.COLUMNS,
                    RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
            try {
                while (c.moveToNext()) {
                    long rawContactId = c.getLong(RawContactIdQuery.RAW_CONTACT_ID);
                    long accountId = c.getLong(RawContactIdQuery.ACCOUNT_ID);
                    // Don't aggregate a contact with its own raw contacts.
                    matcher.keepOut(rawContactId, contactId, accountId);
                    updateMatchScoresForSuggestionsBasedOnDataMatches(db, rawContactId, candidates,
                            matcher);
                }
            } finally {
                c.close();
            }
        } else {
            updateMatchScoresForSuggestionsBasedOnDataMatches(db, candidates,
                    matcher, parameters);
        }

        return matcher.pickBestMatches(SCORE_THRESHOLD_SUGGEST);
    }

    /**
     * Computes scores for contacts that have matching data rows.
     */
    private void updateMatchScoresForSuggestionsBasedOnDataMatches(SQLiteDatabase db,
            long rawContactId, MatchCandidateList candidates, RawContactMatcher matcher) {

        updateMatchScoresBasedOnExceptions(db, rawContactId, matcher);
        updateMatchScoresBasedOnIdentityMatch(db, rawContactId, matcher);
        updateMatchScoresBasedOnNameMatches(db, rawContactId, matcher);
        updateMatchScoresBasedOnEmailMatches(db, rawContactId, matcher);
        updateMatchScoresBasedOnPhoneMatches(db, rawContactId, matcher);
        loadNameMatchCandidates(db, rawContactId, candidates, false);
        lookupApproximateNameMatches(db, candidates, matcher);
    }

    private void updateMatchScoresForSuggestionsBasedOnDataMatches(SQLiteDatabase db,
            MatchCandidateList candidates, RawContactMatcher matcher,
            ArrayList<AggregationSuggestionParameter> parameters) {
        for (AggregationSuggestionParameter parameter : parameters) {
            if (AggregationSuggestions.PARAMETER_MATCH_NAME.equals(parameter.kind)) {
                updateMatchScoresBasedOnNameMatches(db, parameter.value, candidates, matcher);
            }

            // TODO: add support for other parameter kinds
        }
    }
}
