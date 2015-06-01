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

package com.android.providers.contacts.aggregation;

import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Identity;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.provider.ContactsContract.Data;
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
import com.android.providers.contacts.aggregation.util.ContactMatcher;
import com.android.providers.contacts.aggregation.util.MatchScore;
import com.android.providers.contacts.database.ContactsTableUtil;
import com.google.android.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ContactAggregator deals with aggregating contact information coming from different sources.
 * Two John Doe contacts from two disjoint sources are presumed to be the same
 * person unless the user declares otherwise.
 */
public class ContactAggregator extends AbstractContactAggregator {

    // Return code for the canJoinIntoContact method.
    private static final int JOIN = 1;
    private static final int KEEP_SEPARATE = 0;
    private static final int RE_AGGREGATE = -1;

    private final ContactMatcher mMatcher = new ContactMatcher();

    /**
     * Constructor.
     */
    public ContactAggregator(ContactsProvider2 contactsProvider,
            ContactsDatabaseHelper contactsDatabaseHelper,
            PhotoPriorityResolver photoPriorityResolver, NameSplitter nameSplitter,
            CommonNicknameCache commonNicknameCache) {
        super(contactsProvider, contactsDatabaseHelper, photoPriorityResolver, nameSplitter,
                commonNicknameCache);
    }

  /**
     * Given a specific raw contact, finds all matching aggregate contacts and chooses the one
     * with the highest match score.  If no such contact is found, creates a new contact.
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

        long contactId = -1; // Best matching contact ID.
        boolean needReaggregate = false;

        final ContactMatcher matcher = new ContactMatcher();
        final Set<Long> rawContactIdsInSameAccount = new HashSet<Long>();
        final Set<Long> rawContactIdsInOtherAccount = new HashSet<Long>();
        if (aggregationMode == RawContacts.AGGREGATION_MODE_DEFAULT) {
            candidates.clear();
            matcher.clear();

            contactId = pickBestMatchBasedOnExceptions(db, rawContactId, matcher);
            if (contactId == -1) {

                // If this is a newly inserted contact or a visible contact, look for
                // data matches.
                if (currentContactId == 0
                        || mDbHelper.isContactInDefaultDirectory(db, currentContactId)) {
                    contactId = pickBestMatchBasedOnData(db, rawContactId, candidates, matcher);
                }

                // If we found an best matched contact, find out if the raw contact can be joined
                // into it
                if (contactId != -1 && contactId != currentContactId) {
                    // List all raw contact ID and their account ID mappings in contact
                    // [contactId] excluding raw_contact [rawContactId].

                    // Based on the mapping, create two sets of raw contact IDs in
                    // [rawContactAccountId] and not in [rawContactAccountId]. We don't always
                    // need them, so lazily initialize them.
                    mSelectionArgs2[0] = String.valueOf(contactId);
                    mSelectionArgs2[1] = String.valueOf(rawContactId);
                    final Cursor rawContactsToAccountsCursor = db.rawQuery(
                            "SELECT " + RawContacts._ID + ", " + RawContactsColumns.ACCOUNT_ID +
                                    " FROM " + Tables.RAW_CONTACTS +
                                    " WHERE " + RawContacts.CONTACT_ID + "=?" +
                                    " AND " + RawContacts._ID + "!=?",
                            mSelectionArgs2);
                    try {
                        rawContactsToAccountsCursor.moveToPosition(-1);
                        while (rawContactsToAccountsCursor.moveToNext()) {
                            final long rcId = rawContactsToAccountsCursor.getLong(0);
                            final long rc_accountId = rawContactsToAccountsCursor.getLong(1);
                            if (rc_accountId == accountId) {
                                rawContactIdsInSameAccount.add(rcId);
                            } else {
                                rawContactIdsInOtherAccount.add(rcId);
                            }
                        }
                    } finally {
                        rawContactsToAccountsCursor.close();
                    }
                    final int actionCode;
                    final int totalNumOfRawContactsInCandidate = rawContactIdsInSameAccount.size()
                            + rawContactIdsInOtherAccount.size();
                    if (totalNumOfRawContactsInCandidate >= AGGREGATION_CONTACT_SIZE_LIMIT) {
                        if (VERBOSE_LOGGING) {
                            Log.v(TAG, "Too many raw contacts (" + totalNumOfRawContactsInCandidate
                                    + ") in the best matching contact, so skip aggregation");
                        }
                        actionCode = KEEP_SEPARATE;
                    } else {
                        actionCode = canJoinIntoContact(db, rawContactId,
                                rawContactIdsInSameAccount, rawContactIdsInOtherAccount);
                    }
                    if (actionCode == KEEP_SEPARATE) {
                        contactId = -1;
                    } else if (actionCode == RE_AGGREGATE) {
                        needReaggregate = true;
                    }
                }
            }
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

        // If there are no other raw contacts in the current aggregate, we might as well reuse it.
        // Also, if the aggregation mode is SUSPENDED, we must reuse the same aggregate.
        if (contactId == -1
                && currentContactId != 0
                && (currentContactContentsCount == 0
                        || aggregationMode == RawContacts.AGGREGATION_MODE_SUSPENDED)) {
            contactId = currentContactId;
        }

        if (contactId == currentContactId) {
            // Aggregation unchanged
            markAggregated(db, String.valueOf(rawContactId));
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Aggregation unchanged");
            }
        } else if (contactId == -1) {
            // create new contact for [rawContactId]
            createContactForRawContacts(db, txContext, Sets.newHashSet(rawContactId), null);
            if (currentContactContentsCount > 0) {
                updateAggregateData(txContext, currentContactId);
            }
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "create new contact for rid=" + rawContactId);
            }
        } else if (needReaggregate) {
            // re-aggregate
            final Set<Long> allRawContactIdSet = new HashSet<Long>();
            allRawContactIdSet.addAll(rawContactIdsInSameAccount);
            allRawContactIdSet.addAll(rawContactIdsInOtherAccount);
            // If there is no other raw contacts aggregated with the given raw contact currently,
            // we might as well reuse it.
            currentContactId = (currentContactId != 0 && currentContactContentsCount == 0)
                    ? currentContactId : 0;
            reAggregateRawContacts(txContext, db, contactId, currentContactId, rawContactId,
                    allRawContactIdSet);
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Re-aggregating rid=" + rawContactId + " and cid=" + contactId);
            }
        } else {
            // Joining with an existing aggregate
            if (currentContactContentsCount == 0) {
                // Delete a previous aggregate if it only contained this raw contact
                ContactsTableUtil.deleteContact(db, currentContactId);

                mAggregatedPresenceDelete.bindLong(1, currentContactId);
                mAggregatedPresenceDelete.execute();
            }

            clearSuperPrimarySetting(db, contactId, rawContactId);
            setContactIdAndMarkAggregated(rawContactId, contactId);
            computeAggregateData(db, contactId, mContactUpdate);
            mContactUpdate.bindLong(ContactReplaceSqlStatement.CONTACT_ID, contactId);
            mContactUpdate.execute();
            mDbHelper.updateContactVisible(txContext, contactId);
            updateAggregatedStatusUpdate(contactId);
            // Make sure the raw contact does not contribute to the current contact
            if (currentContactId != 0) {
                updateAggregateData(txContext, currentContactId);
            }
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "Join rid=" + rawContactId + " with cid=" + contactId);
            }
        }
    }

    /**
     * Find out which mime-types are shared by raw contact of {@code rawContactId} and raw contacts
     * of {@code contactId}. Clear the is_super_primary settings for these mime-types.
     */
    private void clearSuperPrimarySetting(SQLiteDatabase db, long contactId, long rawContactId) {
        final String[] args = {String.valueOf(contactId), String.valueOf(rawContactId)};

        // Find out which mime-types exist with is_super_primary=true on both the raw contact of
        // rawContactId and raw contacts of contactId
        int index = 0;
        final StringBuilder mimeTypeCondition = new StringBuilder();
        mimeTypeCondition.append(" AND " + DataColumns.MIMETYPE_ID + " IN (");

        final Cursor c = db.rawQuery(
                "SELECT DISTINCT(a." + DataColumns.MIMETYPE_ID + ")" +
                " FROM (SELECT " + DataColumns.MIMETYPE_ID + " FROM " + Tables.DATA + " WHERE " +
                        Data.IS_SUPER_PRIMARY + " =1 AND " +
                        Data.RAW_CONTACT_ID + " IN (SELECT " + RawContacts._ID + " FROM " +
                        Tables.RAW_CONTACTS + " WHERE " + RawContacts.CONTACT_ID + "=?1)) AS a" +
                " JOIN  (SELECT " + DataColumns.MIMETYPE_ID + " FROM " + Tables.DATA + " WHERE " +
                        Data.IS_SUPER_PRIMARY + " =1 AND " +
                        Data.RAW_CONTACT_ID + "=?2) AS b" +
                " ON a." + DataColumns.MIMETYPE_ID + "=b." + DataColumns.MIMETYPE_ID,
                args);
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
                        " WHERE " + RawContacts.CONTACT_ID + "=?1)" +
                        " OR " +  Data.RAW_CONTACT_ID + "=?2)";

        mimeTypeCondition.append(')');
        superPrimaryUpdateSql += mimeTypeCondition.toString();
        db.execSQL(superPrimaryUpdateSql, args);
    }

    /**
     * @return JOIN if the raw contact of {@code rawContactId} can be joined into the existing
     * contact of {@code contactId}. KEEP_SEPARATE if the raw contact of {@code rawContactId}
     * cannot be joined into the existing contact of {@code contactId}. RE_AGGREGATE if raw contact
     * of {@code rawContactId} and all the raw contacts of contact of {@code contactId} need to be
     * re-aggregated.
     *
     * If contact of {@code contactId} doesn't contain any raw contacts from the same account as
     * raw contact of {@code rawContactId}, join raw contact with contact if there is no identity
     * mismatch between them on the same namespace, otherwise, keep them separate.
     *
     * If contact of {@code contactId} contains raw contacts from the same account as raw contact of
     * {@code rawContactId}, join raw contact with contact if there's at least one raw contact in
     * those raw contacts that shares at least one email address, phone number, or identity;
     * otherwise, re-aggregate raw contact and all the raw contacts of contact.
     */
    private int canJoinIntoContact(SQLiteDatabase db, long rawContactId,
            Set<Long> rawContactIdsInSameAccount, Set<Long> rawContactIdsInOtherAccount ) {

        if (rawContactIdsInSameAccount.isEmpty()) {
            final String rid = String.valueOf(rawContactId);
            final String ridsInOtherAccts = TextUtils.join(",", rawContactIdsInOtherAccount);
            // If there is no identity match between raw contact of [rawContactId] and
            // any raw contact in other accounts on the same namespace, and there is at least
            // one identity mismatch exist, keep raw contact separate from contact.
            if (DatabaseUtils.longForQuery(db, buildIdentityMatchingSql(rid, ridsInOtherAccts,
                    /* isIdentityMatching =*/ true, /* countOnly =*/ true), null) == 0 &&
                    DatabaseUtils.longForQuery(db, buildIdentityMatchingSql(rid, ridsInOtherAccts,
                            /* isIdentityMatching =*/ false, /* countOnly =*/ true), null) > 0) {
                if (VERBOSE_LOGGING) {
                    Log.v(TAG, "canJoinIntoContact: no duplicates, but has no matching identity " +
                            "and has mis-matching identity on the same namespace between rid=" +
                            rid + " and ridsInOtherAccts=" + ridsInOtherAccts);
                }
                return KEEP_SEPARATE; // has identity and identity doesn't match
            } else {
                if (VERBOSE_LOGGING) {
                    Log.v(TAG, "canJoinIntoContact: can join the first raw contact from the same " +
                            "account without any identity mismatch.");
                }
                return JOIN; // no identity or identity match
            }
        }
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "canJoinIntoContact: " + rawContactIdsInSameAccount.size() +
                    " duplicate(s) found");
        }


        final Set<Long> rawContactIdSet = new HashSet<Long>();
        rawContactIdSet.add(rawContactId);
        if (rawContactIdsInSameAccount.size() > 0 &&
                isDataMaching(db, rawContactIdSet, rawContactIdsInSameAccount)) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "canJoinIntoContact: join if there is a data matching found in the " +
                        "same account");
            }
            return JOIN;
        } else {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "canJoinIntoContact: re-aggregate rid=" + rawContactId +
                        " with its best matching contact to connected component");
            }
            return RE_AGGREGATE;
        }
    }

    /**
     * If there's any identity, email address or a phone number matching between two raw contact
     * sets.
     */
    private boolean isDataMaching(SQLiteDatabase db, Set<Long> rawContactIdSet1,
            Set<Long> rawContactIdSet2) {
        final String rawContactIds1 = TextUtils.join(",", rawContactIdSet1);
        final String rawContactIds2 = TextUtils.join(",", rawContactIdSet2);
        // First, check for the identity
        if (isFirstColumnGreaterThanZero(db, buildIdentityMatchingSql(
                rawContactIds1, rawContactIds2,  /* isIdentityMatching =*/ true,
                /* countOnly =*/true))) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "canJoinIntoContact: identity match found between " + rawContactIds1 +
                        " and " + rawContactIds2);
            }
            return true;
        }

        // Next, check for the email address.
        if (isFirstColumnGreaterThanZero(db,
                buildEmailMatchingSql(rawContactIds1, rawContactIds2, true))) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "canJoinIntoContact: email match found between " + rawContactIds1 +
                        " and " + rawContactIds2);
            }
            return true;
        }

        // Lastly, the phone number.
        if (isFirstColumnGreaterThanZero(db,
                buildPhoneMatchingSql(rawContactIds1, rawContactIds2, true))) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "canJoinIntoContact: phone match found between " + rawContactIds1 +
                        " and " + rawContactIds2);
            }
            return true;
        }
        return false;
    }

    /**
     * Re-aggregate rawContact of {@code rawContactId} and all the raw contacts of
     * {@code existingRawContactIds} into connected components. This only happens when a given
     * raw contacts cannot be joined with its best matching contacts directly.
     *
     *  Two raw contacts are considered connected if they share at least one email address, phone
     *  number or identity. Create new contact for each connected component except the very first
     *  one that doesn't contain rawContactId of {@code rawContactId}.
     */
    private void reAggregateRawContacts(TransactionContext txContext, SQLiteDatabase db,
            long contactId, long currentContactId, long rawContactId,
            Set<Long> existingRawContactIds) {
        // Find the connected component based on the aggregation exceptions or
        // identity/email/phone matching for all the raw contacts of [contactId] and the give
        // raw contact.
        final Set<Long> allIds = new HashSet<Long>();
        allIds.add(rawContactId);
        allIds.addAll(existingRawContactIds);
        final Set<Set<Long>> connectedRawContactSets = findConnectedRawContacts(db, allIds);

        if (connectedRawContactSets.size() == 1) {
            // If everything is connected, create one contact with [contactId]
            createContactForRawContacts(db, txContext, connectedRawContactSets.iterator().next(),
                    contactId);
        } else {
            for (Set<Long> connectedRawContactIds : connectedRawContactSets) {
                if (connectedRawContactIds.contains(rawContactId)) {
                    // crate contact for connect component containing [rawContactId], reuse
                    // [currentContactId] if possible.
                    createContactForRawContacts(db, txContext, connectedRawContactIds,
                            currentContactId == 0 ? null : currentContactId);
                    connectedRawContactSets.remove(connectedRawContactIds);
                    break;
                }
            }
            // Create new contact for each connected component except the last one. The last one
            // will reuse [contactId]. Only the last one can reuse [contactId] when all other raw
            // contacts has already been assigned new contact Id, so that the contact aggregation
            // stats could be updated correctly.
            int index = connectedRawContactSets.size();
            for (Set<Long> connectedRawContactIds : connectedRawContactSets) {
                if (index > 1) {
                    createContactForRawContacts(db, txContext, connectedRawContactIds, null);
                    index--;
                } else {
                    createContactForRawContacts(db, txContext, connectedRawContactIds, contactId);
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
                            mMatcher.pickBestMatches(ContactMatcher.SCORE_THRESHOLD_PRIMARY);
                    for (MatchScore matchScore : bestMatches) {
                        markContactForAggregation(db, matchScore.getContactId());
                    }

                    mMatcher.clear();
                    updateMatchScoresBasedOnEmailMatches(db, rawContactId, mMatcher);
                    updateMatchScoresBasedOnPhoneMatches(db, rawContactId, mMatcher);
                    bestMatches =
                            mMatcher.pickBestMatches(ContactMatcher.SCORE_THRESHOLD_SECONDARY);
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
     * Updates the contact ID for the specified contact and marks the raw contact as aggregated.
     */
    private void setContactIdAndMarkAggregated(long rawContactId, long contactId) {
        mContactIdAndMarkAggregatedUpdate.bindLong(1, contactId);
        mContactIdAndMarkAggregatedUpdate.bindLong(2, rawContactId);
        mContactIdAndMarkAggregatedUpdate.execute();
    }

    interface AggregateExceptionQuery {
        String TABLE = Tables.AGGREGATION_EXCEPTIONS
            + " JOIN raw_contacts raw_contacts1 "
                    + " ON (agg_exceptions.raw_contact_id1 = raw_contacts1._id) "
            + " JOIN raw_contacts raw_contacts2 "
                    + " ON (agg_exceptions.raw_contact_id2 = raw_contacts2._id) ";

        String[] COLUMNS = {
            AggregationExceptions.TYPE,
            AggregationExceptions.RAW_CONTACT_ID1,
            "raw_contacts1." + RawContacts.CONTACT_ID,
            "raw_contacts1." + RawContactsColumns.AGGREGATION_NEEDED,
            "raw_contacts2." + RawContacts.CONTACT_ID,
            "raw_contacts2." + RawContactsColumns.AGGREGATION_NEEDED,
        };

        int TYPE = 0;
        int RAW_CONTACT_ID1 = 1;
        int CONTACT_ID1 = 2;
        int AGGREGATION_NEEDED_1 = 3;
        int CONTACT_ID2 = 4;
        int AGGREGATION_NEEDED_2 = 5;
    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     * Returns the aggregate contact with the always match exception if any.
     */
    private long pickBestMatchBasedOnExceptions(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        if (!mAggregationExceptionIdsValid) {
            prefetchAggregationExceptionIds(db);
        }

        // If there are no aggregation exceptions involving this raw contact, there is no need to
        // run a query and we can just return -1, which stands for "nothing found"
        if (!mAggregationExceptionIds.contains(rawContactId)) {
            return -1;
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
                if (rawContactId == rawContactId1) {
                    if (c.getInt(AggregateExceptionQuery.AGGREGATION_NEEDED_2) == 0
                            && !c.isNull(AggregateExceptionQuery.CONTACT_ID2)) {
                        contactId = c.getLong(AggregateExceptionQuery.CONTACT_ID2);
                    }
                } else {
                    if (c.getInt(AggregateExceptionQuery.AGGREGATION_NEEDED_1) == 0
                            && !c.isNull(AggregateExceptionQuery.CONTACT_ID1)) {
                        contactId = c.getLong(AggregateExceptionQuery.CONTACT_ID1);
                    }
                }
                if (contactId != -1) {
                    if (type == AggregationExceptions.TYPE_KEEP_TOGETHER) {
                        matcher.keepIn(contactId);
                    } else {
                        matcher.keepOut(contactId);
                    }
                }
            }
        } finally {
            c.close();
        }

        return matcher.pickBestMatch(MatchScore.MAX_SCORE, true);
    }

    /**
     * Picks the best matching contact based on matches between data elements.  It considers
     * name match to be primary and phone, email etc matches to be secondary.  A good primary
     * match triggers aggregation, while a good secondary match only triggers aggregation in
     * the absence of a strong primary mismatch.
     * <p>
     * Consider these examples:
     * <p>
     * John Doe with phone number 111-111-1111 and Jon Doe with phone number 111-111-1111 should
     * be aggregated (same number, similar names).
     * <p>
     * John Doe with phone number 111-111-1111 and Deborah Doe with phone number 111-111-1111 should
     * not be aggregated (same number, different names).
     */
    private long pickBestMatchBasedOnData(SQLiteDatabase db, long rawContactId,
            MatchCandidateList candidates, ContactMatcher matcher) {

        // Find good matches based on name alone
        long bestMatch = updateMatchScoresBasedOnDataMatches(db, rawContactId, matcher);
        if (bestMatch == ContactMatcher.MULTIPLE_MATCHES) {
            // We found multiple matches on the name - do not aggregate because of the ambiguity
            return -1;
        } else if (bestMatch == -1) {
            // We haven't found a good match on name, see if we have any matches on phone, email etc
            bestMatch = pickBestMatchBasedOnSecondaryData(db, rawContactId, candidates, matcher);
            if (bestMatch == ContactMatcher.MULTIPLE_MATCHES) {
                return -1;
            }
        }

        return bestMatch;
    }


    /**
     * Picks the best matching contact based on secondary data matches.  The method loads
     * structured names for all candidate contacts and recomputes match scores using approximate
     * matching.
     */
    private long pickBestMatchBasedOnSecondaryData(SQLiteDatabase db,
            long rawContactId, MatchCandidateList candidates, ContactMatcher matcher) {
        List<Long> secondaryContactIds = matcher.prepareSecondaryMatchCandidates(
                ContactMatcher.SCORE_THRESHOLD_PRIMARY);
        if (secondaryContactIds == null || secondaryContactIds.size() > SECONDARY_HIT_LIMIT) {
            return -1;
        }

        loadNameMatchCandidates(db, rawContactId, candidates, true);

        mSb.setLength(0);
        mSb.append(RawContacts.CONTACT_ID).append(" IN (");
        for (int i = 0; i < secondaryContactIds.size(); i++) {
            if (i != 0) {
                mSb.append(',');
            }
            mSb.append(secondaryContactIds.get(i));
        }

        // We only want to compare structured names to structured names
        // at this stage, we need to ignore all other sources of name lookup data.
        mSb.append(") AND " + STRUCTURED_NAME_BASED_LOOKUP_SQL);

        matchAllCandidates(db, mSb.toString(), candidates, matcher,
                ContactMatcher.MATCHING_ALGORITHM_CONSERVATIVE, null);

        return matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_SECONDARY, false);
    }

    /**
     * Computes scores for contacts that have matching data rows.
     */
    private long updateMatchScoresBasedOnDataMatches(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {

        updateMatchScoresBasedOnIdentityMatch(db, rawContactId, matcher);
        updateMatchScoresBasedOnNameMatches(db, rawContactId, matcher);
        long bestMatch = matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_PRIMARY, false);
        if (bestMatch != -1) {
            return bestMatch;
        }

        updateMatchScoresBasedOnEmailMatches(db, rawContactId, matcher);
        updateMatchScoresBasedOnPhoneMatches(db, rawContactId, matcher);

        return -1;
    }

    private interface IdentityLookupMatchQuery {
        final String TABLE = Tables.DATA + " dataA"
                + " JOIN " + Tables.DATA + " dataB" +
                " ON (dataA." + Identity.NAMESPACE + "=dataB." + Identity.NAMESPACE +
                " AND dataA." + Identity.IDENTITY + "=dataB." + Identity.IDENTITY + ")"
                + " JOIN " + Tables.RAW_CONTACTS +
                " ON (dataB." + Data.RAW_CONTACT_ID + " = "
                + Tables.RAW_CONTACTS + "." + RawContacts._ID + ")";

        final String SELECTION = "dataA." + Data.RAW_CONTACT_ID + "=?1"
                + " AND dataA." + DataColumns.MIMETYPE_ID + "=?2"
                + " AND dataA." + Identity.NAMESPACE + " NOT NULL"
                + " AND dataA." + Identity.IDENTITY + " NOT NULL"
                + " AND dataB." + DataColumns.MIMETYPE_ID + "=?2"
                + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0"
                + " AND " + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY;

        final String[] COLUMNS = new String[] {
            RawContacts.CONTACT_ID
        };

        int CONTACT_ID = 0;
    }

    /**
     * Finds contacts with exact identity matches to the the specified raw contact.
     */
    private void updateMatchScoresBasedOnIdentityMatch(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = String.valueOf(mMimeTypeIdIdentity);
        Cursor c = db.query(IdentityLookupMatchQuery.TABLE, IdentityLookupMatchQuery.COLUMNS,
                IdentityLookupMatchQuery.SELECTION,
                mSelectionArgs2, RawContacts.CONTACT_ID, null, null);
        try {
            while (c.moveToNext()) {
                final long contactId = c.getLong(IdentityLookupMatchQuery.CONTACT_ID);
                matcher.matchIdentity(contactId);
            }
        } finally {
            c.close();
        }

    }

    private interface NameLookupMatchQuery {
        String TABLE = Tables.NAME_LOOKUP + " nameA"
                + " JOIN " + Tables.NAME_LOOKUP + " nameB" +
                " ON (" + "nameA." + NameLookupColumns.NORMALIZED_NAME + "="
                        + "nameB." + NameLookupColumns.NORMALIZED_NAME + ")"
                + " JOIN " + Tables.RAW_CONTACTS +
                " ON (nameB." + NameLookupColumns.RAW_CONTACT_ID + " = "
                        + Tables.RAW_CONTACTS + "." + RawContacts._ID + ")";

        String SELECTION = "nameA." + NameLookupColumns.RAW_CONTACT_ID + "=?"
                + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0"
                + " AND " + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY;

        String[] COLUMNS = new String[] {
            RawContacts.CONTACT_ID,
            "nameA." + NameLookupColumns.NORMALIZED_NAME,
            "nameA." + NameLookupColumns.NAME_TYPE,
            "nameB." + NameLookupColumns.NAME_TYPE,
        };

        int CONTACT_ID = 0;
        int NAME = 1;
        int NAME_TYPE_A = 2;
        int NAME_TYPE_B = 3;
    }

    /**
     * Finds contacts with names matching the name of the specified raw contact.
     */
    private void updateMatchScoresBasedOnNameMatches(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        mSelectionArgs1[0] = String.valueOf(rawContactId);
        Cursor c = db.query(NameLookupMatchQuery.TABLE, NameLookupMatchQuery.COLUMNS,
                NameLookupMatchQuery.SELECTION,
                mSelectionArgs1, null, null, null, PRIMARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(NameLookupMatchQuery.CONTACT_ID);
                String name = c.getString(NameLookupMatchQuery.NAME);
                int nameTypeA = c.getInt(NameLookupMatchQuery.NAME_TYPE_A);
                int nameTypeB = c.getInt(NameLookupMatchQuery.NAME_TYPE_B);
                matcher.matchName(contactId, nameTypeA, name,
                        nameTypeB, name, ContactMatcher.MATCHING_ALGORITHM_EXACT);
                if (nameTypeA == NameLookupType.NICKNAME &&
                        nameTypeB == NameLookupType.NICKNAME) {
                    matcher.updateScoreWithNicknameMatch(contactId);
                }
            }
        } finally {
            c.close();
        }
    }

    private void updateMatchScoresBasedOnEmailMatches(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = String.valueOf(mMimeTypeIdEmail);
        Cursor c = db.query(EmailLookupQuery.TABLE, EmailLookupQuery.COLUMNS,
                EmailLookupQuery.SELECTION,
                mSelectionArgs2, null, null, null, SECONDARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(EmailLookupQuery.CONTACT_ID);
                matcher.updateScoreWithEmailMatch(contactId);
            }
        } finally {
            c.close();
        }
    }

    private void updateMatchScoresBasedOnPhoneMatches(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        mSelectionArgs2[0] = String.valueOf(rawContactId);
        mSelectionArgs2[1] = mDbHelper.getUseStrictPhoneNumberComparisonParameter();
        Cursor c = db.query(PhoneLookupQuery.TABLE, PhoneLookupQuery.COLUMNS,
                PhoneLookupQuery.SELECTION,
                mSelectionArgs2, null, null, null, SECONDARY_HIT_LIMIT_STRING);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(PhoneLookupQuery.CONTACT_ID);
                matcher.updateScoreWithPhoneNumberMatch(contactId);
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
            ContactMatcher matcher) {
        HashSet<String> firstLetters = new HashSet<String>();
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
                            ContactMatcher.MATCHING_ALGORITHM_APPROXIMATE,
                            String.valueOf(FIRST_LETTER_SUGGESTION_HIT_LIMIT));
                }
            }
        }
    }

    private interface ContactNameLookupQuery {
        String TABLE = Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                RawContacts.CONTACT_ID,
                NameLookupColumns.NORMALIZED_NAME,
                NameLookupColumns.NAME_TYPE
        };

        int CONTACT_ID = 0;
        int NORMALIZED_NAME = 1;
        int NAME_TYPE = 2;
    }

    /**
     * Loads all candidate rows from the name lookup table and updates match scores based
     * on that data.
     */
    private void matchAllCandidates(SQLiteDatabase db, String selection,
            MatchCandidateList candidates, ContactMatcher matcher, int algorithm, String limit) {
        final Cursor c = db.query(ContactNameLookupQuery.TABLE, ContactNameLookupQuery.COLUMNS,
                selection, null, null, null, null, limit);

        try {
            while (c.moveToNext()) {
                Long contactId = c.getLong(ContactNameLookupQuery.CONTACT_ID);
                String name = c.getString(ContactNameLookupQuery.NORMALIZED_NAME);
                int nameType = c.getInt(ContactNameLookupQuery.NAME_TYPE);

                // Note the N^2 complexity of the following fragment. This is not a huge concern
                // since the number of candidates is very small and in general secondary hits
                // in the absence of primary hits are rare.
                for (int i = 0; i < candidates.mCount; i++) {
                    NameMatchCandidate candidate = candidates.mList.get(i);
                    matcher.matchName(contactId, candidate.mLookupType, candidate.mName,
                            nameType, name, algorithm);
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds contacts with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     * @param parameters
     */
     protected List<MatchScore> findMatchingContacts(final SQLiteDatabase db, long contactId,
            ArrayList<AggregationSuggestionParameter> parameters) {

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();

        // Don't aggregate a contact with itself
        matcher.keepOut(contactId);

        if (parameters == null || parameters.size() == 0) {
            final Cursor c = db.query(RawContactIdQuery.TABLE, RawContactIdQuery.COLUMNS,
                    RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
            try {
                while (c.moveToNext()) {
                    long rawContactId = c.getLong(RawContactIdQuery.RAW_CONTACT_ID);
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

        return matcher.pickBestMatches(ContactMatcher.SCORE_THRESHOLD_SUGGEST);
    }

    /**
     * Computes scores for contacts that have matching data rows.
     */
    private void updateMatchScoresForSuggestionsBasedOnDataMatches(SQLiteDatabase db,
            long rawContactId, MatchCandidateList candidates, ContactMatcher matcher) {

        updateMatchScoresBasedOnIdentityMatch(db, rawContactId, matcher);
        updateMatchScoresBasedOnNameMatches(db, rawContactId, matcher);
        updateMatchScoresBasedOnEmailMatches(db, rawContactId, matcher);
        updateMatchScoresBasedOnPhoneMatches(db, rawContactId, matcher);
        loadNameMatchCandidates(db, rawContactId, candidates, false);
        lookupApproximateNameMatches(db, candidates, matcher);
    }

    private void updateMatchScoresForSuggestionsBasedOnDataMatches(SQLiteDatabase db,
            MatchCandidateList candidates, ContactMatcher matcher,
            ArrayList<AggregationSuggestionParameter> parameters) {
        for (AggregationSuggestionParameter parameter : parameters) {
            if (AggregationSuggestions.PARAMETER_MATCH_NAME.equals(parameter.kind)) {
                updateMatchScoresBasedOnNameMatches(db, parameter.value, candidates, matcher);
            }

            // TODO: add support for other parameter kinds
        }
    }
}
