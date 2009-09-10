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

import com.android.providers.contacts.ContactMatcher.MatchScore;
import com.android.providers.contacts.OpenHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.OpenHelper.ContactsColumns;
import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.DisplayNameSources;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.NameLookupColumns;
import com.android.providers.contacts.OpenHelper.NameLookupType;
import com.android.providers.contacts.OpenHelper.PresenceColumns;
import com.android.providers.contacts.OpenHelper.RawContactsColumns;
import com.android.providers.contacts.OpenHelper.Tables;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.text.util.Rfc822Tokenizer;
import android.util.EventLog;
import android.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/**
 * ContactAggregator deals with aggregating contact information coming from different sources.
 * Two John Doe contacts from two disjoint sources are presumed to be the same
 * person unless the user declares otherwise.
 * <p>
 * ContactAggregator runs on a separate thread.
 */
public class ContactAggregator implements ContactAggregationScheduler.Aggregator {

    private static final String TAG = "ContactAggregator";

    // Data mime types used in the contact matching algorithm
    private static final String MIMETYPE_SELECTION_IN_CLAUSE = MimetypesColumns.MIMETYPE + " IN ('"
            + Email.CONTENT_ITEM_TYPE + "','"
            + Nickname.CONTENT_ITEM_TYPE + "','"
            + Phone.CONTENT_ITEM_TYPE + "','"
            + StructuredName.CONTENT_ITEM_TYPE + "')";

    private static final String[] DATA_JOIN_MIMETYPE_COLUMNS = new String[] {
            MimetypesColumns.MIMETYPE,
            Data.DATA1,
            Data.DATA2
    };

    private static final int COL_MIMETYPE = 0;
    private static final int COL_DATA1 = 1;
    private static final int COL_DATA2 = 2;

    private static final String[] DATA_JOIN_MIMETYPE_AND_CONTACT_COLUMNS = new String[] {
            Data.DATA1, Data.DATA2, RawContacts.CONTACT_ID
    };

    private static final int COL_DATA_CONTACT_DATA1 = 0;
    private static final int COL_DATA_CONTACT_DATA2 = 1;
    private static final int COL_DATA_CONTACT_CONTACT_ID = 2;

    private static final String[] NAME_LOOKUP_COLUMNS = new String[] {
            RawContacts.CONTACT_ID, NameLookupColumns.NORMALIZED_NAME, NameLookupColumns.NAME_TYPE
    };

    private static final int COL_NAME_LOOKUP_CONTACT_ID = 0;
    private static final int COL_NORMALIZED_NAME = 1;
    private static final int COL_NAME_TYPE = 2;

    private static final String[] CONTACT_ID_COLUMN = new String[] { RawContacts._ID };

    private interface EmailLookupQuery {
        String TABLE = Tables.DATA_JOIN_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
            RawContacts.CONTACT_ID
        };

        int CONTACT_ID = 0;
    }

    private static final String[] CONTACT_ID_COLUMNS = new String[]{ RawContacts.CONTACT_ID };
    private static final int COL_CONTACT_ID = 0;

    private static final int MODE_INSERT_LOOKUP_DATA = 0;
    private static final int MODE_AGGREGATION = 1;
    private static final int MODE_SUGGESTIONS = 2;

    /**
     * When yielding the transaction to another thread, sleep for this many milliseconds
     * to allow the other thread to build up a transaction before yielding back.
     */
    private static final int SLEEP_AFTER_YIELD_DELAY = 4000;

    /**
     * The maximum number of contacts aggregated in a single transaction.
     */
    private static final int MAX_TRANSACTION_SIZE = 50;

    // From system/core/logcat/event-log-tags
    // aggregator [time, count] will be logged for each aggregator cycle.
    // For the query (as opposed to the merge), count will be negative
    public static final int LOG_SYNC_CONTACTS_AGGREGATION = 2747;

    private final ContactsProvider2 mContactsProvider;
    private final OpenHelper mOpenHelper;
    private final ContactAggregationScheduler mScheduler;
    private boolean mEnabled = true;

    // Set if the current aggregation pass should be interrupted
    private volatile boolean mCancel;

    /** Precompiled sql statement for setting an aggregated presence */
    private SQLiteStatement mAggregatedPresenceReplace;
    private SQLiteStatement mRawContactCountQuery;
    private SQLiteStatement mContactDelete;
    private SQLiteStatement mMarkForAggregation;
    private SQLiteStatement mPhotoIdUpdate;
    private SQLiteStatement mDisplayNameUpdate;
    private SQLiteStatement mHasPhoneNumberUpdate;
    private SQLiteStatement mLookupKeyUpdate;
    private SQLiteStatement mStarredUpdate;

    private HashSet<Long> mRawContactsMarkedForAggregation = new HashSet<Long>();

    /**
     * Captures a potential match for a given name. The matching algorithm
     * constructs a bunch of NameMatchCandidate objects for various potential matches
     * and then executes the search in bulk.
     */
    private static class NameMatchCandidate {
        String mName;
        int mLookupType;

        public NameMatchCandidate(String name, int nameLookupType) {
            mName = name;
            mLookupType = nameLookupType;
        }
    }

    /**
     * A list of {@link NameMatchCandidate} that keeps its elements even when the list is
     * truncated. This is done for optimization purposes to avoid excessive object allocation.
     */
    private static class MatchCandidateList {
        private final ArrayList<NameMatchCandidate> mList = new ArrayList<NameMatchCandidate>();
        private int mCount;

        /**
         * Adds a {@link NameMatchCandidate} element or updates the next one if it already exists.
         */
        public void add(String name, int nameLookupType) {
            if (mCount >= mList.size()) {
                mList.add(new NameMatchCandidate(name, nameLookupType));
            } else {
                NameMatchCandidate candidate = mList.get(mCount);
                candidate.mName = name;
                candidate.mLookupType = nameLookupType;
            }
            mCount++;
        }

        public void clear() {
            mCount = 0;
        }
    }

    /**
     * Constructor.  Starts a contact aggregation thread.  Call {@link #quit} to kill the
     * aggregation thread.  Call {@link #schedule} to kick off the aggregation process after
     * a delay of {@link ContactAggregationScheduler#AGGREGATION_DELAY} milliseconds.
     */
    public ContactAggregator(ContactsProvider2 contactsProvider, OpenHelper openHelper,
            ContactAggregationScheduler scheduler) {
        mContactsProvider = contactsProvider;
        mOpenHelper = openHelper;

        SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        // Since we have no way of determining which custom status was set last,
        // we'll just pick one randomly.  We are using MAX as an approximation of randomness
        mAggregatedPresenceReplace = db.compileStatement(
                "INSERT OR REPLACE INTO " + Tables.AGGREGATED_PRESENCE + "("
                        + AggregatedPresenceColumns.CONTACT_ID + ", "
                        + Presence.PRESENCE_STATUS + ", "
                        + Presence.PRESENCE_CUSTOM_STATUS
                + ") SELECT ?, "
                            + "MAX(" + Presence.PRESENCE_STATUS + "), "
                            + "MAX(" + Presence.PRESENCE_CUSTOM_STATUS + ")"
                        + " FROM " + Tables.PRESENCE + "," + Tables.RAW_CONTACTS
                        + " WHERE " + PresenceColumns.RAW_CONTACT_ID + "="
                                + RawContactsColumns.CONCRETE_ID
                        + "   AND " + RawContacts.CONTACT_ID + "=?");

        mRawContactCountQuery = db.compileStatement(
                "SELECT COUNT(" + RawContacts._ID + ")" +
                " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + RawContacts.CONTACT_ID + "=?"
                        + " AND " + RawContacts._ID + "<>?");

        mContactDelete = db.compileStatement(
                "DELETE FROM " + Tables.CONTACTS +
                " WHERE " + Contacts._ID + "=?");

        mMarkForAggregation = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.AGGREGATION_NEEDED + "=1" +
                " WHERE " + RawContacts._ID + "=?"
                        + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0");

        mPhotoIdUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.PHOTO_ID + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mDisplayNameUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.DISPLAY_NAME + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mLookupKeyUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.LOOKUP_KEY + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mHasPhoneNumberUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.HAS_PHONE_NUMBER + "="
                        + "(SELECT (CASE WHEN COUNT(*)=0 THEN 0 ELSE 1 END)"
                        + " FROM " + Tables.DATA_JOIN_RAW_CONTACTS
                        + " WHERE " + DataColumns.MIMETYPE_ID + "=?"
                                + " AND " + Phone.NUMBER + " NOT NULL"
                                + " AND " + RawContacts.CONTACT_ID + "=?)" +
                " WHERE " + Contacts._ID + "=?");

        mStarredUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.STARRED + "=(SELECT (CASE WHEN COUNT(" + RawContacts.STARRED
                + ")=0 THEN 0 ELSE 1 END) FROM " + Tables.RAW_CONTACTS + " WHERE "
                + RawContacts.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + " AND "
                + RawContacts.STARRED + "=1)" + " WHERE " + Contacts._ID + "=?");

        mScheduler = scheduler;
        mScheduler.setAggregator(this);
        mScheduler.start();

        // Perform an aggregation pass in the beginning, which will most of the time
        // do nothing.  It will only be useful if the content provider has been killed
        // before completing aggregation.
        mScheduler.schedule();
    }

    public void setEnabled(boolean enabled) {
        mEnabled = enabled;
    }

    public boolean isEnabled() {
        return mEnabled;
    }

    /**
     * Schedules aggregation pass after a short delay.  This method should be called every time
     * the {@link RawContacts#CONTACT_ID} field is reset on any record.
     */
    public void schedule() {
        if (mEnabled) {
            mScheduler.schedule();
        }
    }

    /**
     * Kills the contact aggregation thread.
     */
    public void quit() {
        mScheduler.stop();
    }

    /**
     * Invoked by the scheduler to cancel aggregation.
     */
    public void interrupt() {
        mCancel = true;
    }

    private interface AggregationQuery {
        String TABLE = Tables.RAW_CONTACTS;

        String[] COLUMNS = {
                RawContacts._ID, RawContacts.CONTACT_ID
        };

        int _ID = 0;
        int CONTACT_ID = 1;

        String SELECTION = RawContactsColumns.AGGREGATION_NEEDED + "=1"
                + " AND " + RawContacts.AGGREGATION_MODE
                        + "=" + RawContacts.AGGREGATION_MODE_DEFAULT
                + " AND " + RawContacts.CONTACT_ID + " NOT NULL";
    }
    /**
     * Find all contacts that require aggregation and pass them through aggregation one by one.
     * Do not call directly.  It is invoked by the scheduler.
     */
    public void run() {
        if (!mEnabled) {
            return;
        }

        mCancel = false;

        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();
        long rawContactIds[] = new long[MAX_TRANSACTION_SIZE];
        long contactIds[] = new long[MAX_TRANSACTION_SIZE];
        while (!mCancel) {
            if (!aggregateBatch(db, candidates, matcher, values, rawContactIds, contactIds)) {
                break;
            }
        }
    }

    /**
     * Takes a batch of contacts and aggregates them. Returns the number of successfully
     * processed raw contacts.
     *
     * @return true if there are possibly more contacts to aggregate
     */
    private boolean aggregateBatch(SQLiteDatabase db, MatchCandidateList candidates,
            ContactMatcher matcher, ContentValues values, long[] rawContactIds, long[] contactIds) {
        boolean lastBatch = false;
        long elapsedTime = 0;
        int aggregatedCount = 0;
        while (!mCancel && aggregatedCount < MAX_TRANSACTION_SIZE) {
            db.beginTransaction();
            try {

                long start = System.currentTimeMillis();
                int count = findContactsToAggregate(db, rawContactIds, contactIds,
                        MAX_TRANSACTION_SIZE - aggregatedCount);
                if (mCancel || count == 0) {
                    lastBatch = true;
                    break;
                }

                Log.i(TAG, "Contact aggregation: " + count);
                EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION,
                        System.currentTimeMillis() - start, -count);

                for (int i = 0; i < count; i++) {
                    start = System.currentTimeMillis();
                    aggregateContact(db, rawContactIds[i], contactIds[i], candidates, matcher,
                            values);
                    long end = System.currentTimeMillis();
                    elapsedTime += (end - start);
                    aggregatedCount++;
                    if (db.yieldIfContendedSafely(SLEEP_AFTER_YIELD_DELAY)) {

                        // We have yielded the database, so the rawContactIds and contactIds
                        // arrays are no longer current - we need to refetch them
                        break;
                    }
                }
                db.setTransactionSuccessful();
            } finally {
                db.endTransaction();
            }
        }

        EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION, elapsedTime, aggregatedCount);
        String performance = aggregatedCount == 0 ? "" : ", " + (elapsedTime / aggregatedCount)
                + " ms per contact";
        if (aggregatedCount != 0) {
            Log.i(TAG, "Contact aggregation complete: " + aggregatedCount + performance);
        }

        if (aggregatedCount > 0) {
            mContactsProvider.notifyChange();
        }

        return !lastBatch;
    }

    /**
     * Finds a batch of contacts marked for aggregation. The maximum batch size
     * is {@link #MAX_TRANSACTION_SIZE} contacts.
     * @param limit
     */
    private int findContactsToAggregate(SQLiteDatabase db, long[] rawContactIds,
            long[] contactIds, int limit) {
        Cursor c = db.query(AggregationQuery.TABLE, AggregationQuery.COLUMNS,
                AggregationQuery.SELECTION, null, null, null, null,
                String.valueOf(limit));

        int count = 0;
        try {
            while (c.moveToNext()) {
                rawContactIds[count] = c.getLong(AggregationQuery._ID);
                contactIds[count] = c.getLong(AggregationQuery.CONTACT_ID);
                count++;
            }
        } finally {
            c.close();
        }
        return count;
    }

    public void markNewForAggregation(long rawContactId) {
        mRawContactsMarkedForAggregation.add(rawContactId);
    }

    public void markForAggregation(long rawContactId) {
        if (!mRawContactsMarkedForAggregation.contains(rawContactId)) {
            mRawContactsMarkedForAggregation.add(rawContactId);
            mMarkForAggregation.bindLong(1, rawContactId);
            mMarkForAggregation.execute();
        }
    }

    /**
     * Creates a new contact based on the given raw contact.  Does not perform aggregation.
     */
    public void insertContact(SQLiteDatabase db, long rawContactId) {
        ContentValues contactValues = new ContentValues();
        contactValues.put(Contacts.DISPLAY_NAME, "");
        contactValues.put(Contacts.IN_VISIBLE_GROUP, false);
        computeAggregateData(db,
                RawContactsColumns.CONCRETE_ID + "=" + rawContactId, contactValues);
        long contactId = db.insert(Tables.CONTACTS, Contacts.DISPLAY_NAME, contactValues);
        mOpenHelper.setContactId(rawContactId, contactId);
        mOpenHelper.updateContactVisible(contactId);
    }

    /**
     * Synchronously aggregate the specified contact assuming an open transaction.
     */
    public void aggregateContact(SQLiteDatabase db, long rawContactId, long currentContactId) {
        if (!mEnabled) {
            return;
        }

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

        aggregateContact(db, rawContactId, currentContactId, candidates, matcher, values);
    }

    public void updateAggregateData(long contactId) {
        if (!mEnabled) {
            return;
        }

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final ContentValues values = new ContentValues();
        computeAggregateData(db, contactId, values);
        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);

        mAggregatedPresenceReplace.bindLong(1, contactId);
        mAggregatedPresenceReplace.bindLong(2, contactId);
        mAggregatedPresenceReplace.execute();
    }

    /**
     * Given a specific raw contact, finds all matching aggregate contacts and chooses the one
     * with the highest match score.  If no such contact is found, creates a new contact.
     */
    private synchronized void aggregateContact(SQLiteDatabase db, long rawContactId,
            long currentContactId, MatchCandidateList candidates, ContactMatcher matcher,
            ContentValues values) {

        mRawContactsMarkedForAggregation.remove(rawContactId);

        candidates.clear();
        matcher.clear();

        long contactId = pickBestMatchBasedOnExceptions(db, rawContactId, matcher);
        if (contactId == -1) {
            contactId = pickBestMatchBasedOnData(db, rawContactId, candidates, matcher);
        }

        long currentContactContentsCount = 0;

        if (currentContactId != 0) {
            mRawContactCountQuery.bindLong(1, currentContactId);
            mRawContactCountQuery.bindLong(2, rawContactId);
            currentContactContentsCount = mRawContactCountQuery.simpleQueryForLong();
        }

        // If there are no other raw contacts in the current aggregate, we might as well reuse it.
        if (contactId == -1 && currentContactId != 0 && currentContactContentsCount == 0) {
            contactId = currentContactId;
        }

        if (contactId != currentContactId && currentContactContentsCount == 0) {
            mContactDelete.bindLong(1, currentContactId);
            mContactDelete.execute();
        }

        if (contactId == -1) {

            // Splitting an aggregate
            ContentValues contactValues = new ContentValues();
            contactValues.put(RawContactsColumns.DISPLAY_NAME, "");
            computeAggregateData(db,
                    RawContactsColumns.CONCRETE_ID + "=" + rawContactId, contactValues);
            contactId = db.insert(Tables.CONTACTS, Contacts.DISPLAY_NAME, contactValues);
            mOpenHelper.setContactIdAndMarkAggregated(rawContactId, contactId);
            mOpenHelper.updateContactVisible(contactId);

        } else {

            // Joining with an aggregate
            mOpenHelper.setContactIdAndMarkAggregated(rawContactId, contactId);
            computeAggregateData(db, contactId, values);
            db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
            mOpenHelper.updateContactVisible(contactId);
        }
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
                        return contactId;
                    } else {
                        matcher.keepOut(contactId);
                    }
                }
            }
        } finally {
            c.close();
        }

        return -1;
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

        updateMatchScoresBasedOnDataMatches(db, rawContactId, MODE_AGGREGATION, candidates, matcher);

        // See if we have already found a good match based on name matches alone
        long bestMatch = matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_PRIMARY);
        if (bestMatch == -1) {
            // We haven't found a good match on name, see if we have any matches on phone, email etc
            bestMatch = pickBestMatchBasedOnSecondaryData(db, candidates, matcher);
        }

        return bestMatch;
    }

    /**
     * Picks the best matching contact based on secondary data matches.  The method loads
     * structured names for all candidate contacts and recomputes match scores using approximate
     * matching.
     */
    private long pickBestMatchBasedOnSecondaryData(SQLiteDatabase db,
            MatchCandidateList candidates, ContactMatcher matcher) {
        List<Long> secondaryContactIds = matcher.prepareSecondaryMatchCandidates(
                ContactMatcher.SCORE_THRESHOLD_PRIMARY);
        if (secondaryContactIds == null) {
            return -1;
        }

        StringBuilder selection = new StringBuilder();
        selection.append(RawContacts.CONTACT_ID).append(" IN (");
        for (int i = 0; i < secondaryContactIds.size(); i++) {
            if (i != 0) {
                selection.append(',');
            }
            selection.append(secondaryContactIds.get(i));
        }
        selection.append(") AND " + MimetypesColumns.MIMETYPE + "='"
                + StructuredName.CONTENT_ITEM_TYPE + "'");

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS,
                DATA_JOIN_MIMETYPE_AND_CONTACT_COLUMNS,
                selection.toString(), null, null, null, null);

        MatchCandidateList nameCandidates = new MatchCandidateList();
        try {
            while (c.moveToNext()) {
                String givenName = c.getString(COL_DATA_CONTACT_DATA1);
                String familyName = c.getString(COL_DATA_CONTACT_DATA2);
                long contactId = c.getLong(COL_DATA_CONTACT_CONTACT_ID);

                nameCandidates.clear();
                addMatchCandidatesStructuredName(givenName, familyName, MODE_INSERT_LOOKUP_DATA,
                        nameCandidates);

                // Note the N^2 complexity of the following fragment. This is not a huge concern
                // since the number of candidates is very small and in general secondary hits
                // in the absence of primary hits are rare.
                for (int i = 0; i < candidates.mCount; i++) {
                    NameMatchCandidate candidate = candidates.mList.get(i);

                    // We only want to compare structured names to structured names
                    // at this stage, we need to ignore all other sources of name lookup data.
                    if (NameLookupType.isBasedOnStructuredName(candidate.mLookupType)) {
                        for (int j = 0; j < nameCandidates.mCount; j++) {
                            NameMatchCandidate nameCandidate = nameCandidates.mList.get(j);
                            matcher.matchName(contactId,
                                    nameCandidate.mLookupType, nameCandidate.mName,
                                    candidate.mLookupType, candidate.mName,
                                    ContactMatcher.MATCHING_ALGORITHM_CONSERVATIVE);
                        }
                    }
                }
            }
        } finally {
            c.close();
        }

        return matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_SECONDARY);
    }

    /**
     * Computes scores for contacts that have matching data rows.
     */
    private void updateMatchScoresBasedOnDataMatches(SQLiteDatabase db, long rawContactId,
            int mode, MatchCandidateList candidates, ContactMatcher matcher) {

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS,
                DATA_JOIN_MIMETYPE_COLUMNS,
                Data.RAW_CONTACT_ID + "=" + rawContactId + " AND ("
                        + MIMETYPE_SELECTION_IN_CLAUSE + ")",
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String mimeType = c.getString(COL_MIMETYPE);
                String data1 = c.getString(COL_DATA1);
                String data2 = c.getString(COL_DATA2);
                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    addMatchCandidatesStructuredName(data1, data2, mode, candidates);
                } else if (mimeType.equals(CommonDataKinds.Email.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data2)) {
                        addMatchCandidatesEmail(data2, mode, candidates);
                        lookupEmailMatches(db, data2, matcher);
                    }
                } else if (mimeType.equals(CommonDataKinds.Phone.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data2)) {
                        lookupPhoneMatches(db, data2, matcher);
                    }
                } else if (mimeType.equals(CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data2)) {
                        addMatchCandidatesNickname(data2, mode, candidates);
                        lookupNicknameMatches(db, data2, matcher);
                    }
                }
            }
        } finally {
            c.close();
        }

        lookupNameMatches(db, candidates, matcher);

        if (mode == MODE_SUGGESTIONS) {
            lookupApproximateNameMatches(db, candidates, matcher);
        }
    }

    /**
     * Looks for matches based on the full name (first + last).
     */
    private void addMatchCandidatesStructuredName(String givenName, String familyName, int mode,
            MatchCandidateList candidates) {
        if (TextUtils.isEmpty(givenName)) {

            // If neither the first nor last name are specified, we won't aggregate
            if (TextUtils.isEmpty(familyName)) {
                return;
            }

            addMatchCandidatesSingleName(familyName, candidates);
        } else if (TextUtils.isEmpty(familyName)) {
            addMatchCandidatesSingleName(givenName, candidates);
        } else {
            addMatchCandidatesFullName(givenName, familyName, mode, candidates);
        }
    }

    private void addMatchCandidatesSingleName(String name, MatchCandidateList candidates) {
        String nameN = NameNormalizer.normalize(name);
        candidates.add(nameN, NameLookupType.NAME_EXACT);
        candidates.add(nameN, NameLookupType.NAME_COLLATION_KEY);

        // Take care of first and last names swapped
        String[] clusters = mOpenHelper.getCommonNicknameClusters(nameN);
        if (clusters != null) {
            for (int i = 0; i < clusters.length; i++) {
                candidates.add(clusters[i], NameLookupType.NAME_VARIANT);
            }
        }
    }

    private void addMatchCandidatesFullName(String givenName, String familyName, int mode,
            MatchCandidateList candidates) {
        final String givenNameN = NameNormalizer.normalize(givenName);
        final String[] givenNameNicknames = mOpenHelper.getCommonNicknameClusters(givenNameN);
        final String familyNameN = NameNormalizer.normalize(familyName);
        final String[] familyNameNicknames = mOpenHelper.getCommonNicknameClusters(familyNameN);
        candidates.add(givenNameN + "." + familyNameN, NameLookupType.NAME_EXACT);
        candidates.add(givenNameN + familyNameN, NameLookupType.NAME_COLLATION_KEY);
        candidates.add(familyNameN + givenNameN, NameLookupType.NAME_COLLATION_KEY);
        if (givenNameNicknames != null) {
            for (int i = 0; i < givenNameNicknames.length; i++) {
                candidates.add(givenNameNicknames[i] + "." + familyNameN,
                        NameLookupType.NAME_VARIANT);
                candidates.add(familyNameN + "." + givenNameNicknames[i],
                        NameLookupType.NAME_VARIANT);
            }
        }
        candidates.add(familyNameN + "." + givenNameN, NameLookupType.NAME_VARIANT);
        if (familyNameNicknames != null) {
            for (int i = 0; i < familyNameNicknames.length; i++) {
                candidates.add(familyNameNicknames[i] + "." + givenNameN,
                        NameLookupType.NAME_VARIANT);
                candidates.add(givenNameN + "." + familyNameNicknames[i],
                        NameLookupType.NAME_VARIANT);
            }
        }
    }

    /**
     * Extracts the user name portion from an email address and normalizes it so that it
     * can be matched against names and nicknames.
     */
    private void addMatchCandidatesEmail(String email, int mode, MatchCandidateList candidates) {
        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return;
        }

        String address = tokens[0].getAddress();
        int at = address.indexOf('@');
        if (at != -1) {
            address = address.substring(0, at);
        }

        candidates.add(NameNormalizer.normalize(address), NameLookupType.EMAIL_BASED_NICKNAME);
    }


    /**
     * Normalizes the nickname and adds it to the list of candidates.
     */
    private void addMatchCandidatesNickname(String nickname, int mode,
            MatchCandidateList candidates) {
        candidates.add(NameNormalizer.normalize(nickname), NameLookupType.NICKNAME);
    }

    /**
     * Given a list of {@link NameMatchCandidate}'s, finds all matches and computes their scores.
     */
    private void lookupNameMatches(SQLiteDatabase db, MatchCandidateList candidates,
            ContactMatcher matcher) {

        if (candidates.mCount == 0) {
            return;
        }

        StringBuilder selection = new StringBuilder();
        selection.append(RawContactsColumns.AGGREGATION_NEEDED + "=0 AND ");
        selection.append(NameLookupColumns.NORMALIZED_NAME);
        selection.append(" IN (");
        for (int i = 0; i < candidates.mCount; i++) {
            DatabaseUtils.appendEscapedSQLString(selection, candidates.mList.get(i).mName);
            selection.append(",");
        }

        // Yank the last comma
        selection.setLength(selection.length() - 1);
        selection.append(")");

        matchAllCandidates(db, selection.toString(), candidates, matcher,
                ContactMatcher.MATCHING_ALGORITHM_EXACT);
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
                            + firstLetter + "*')";
                    matchAllCandidates(db, selection, candidates, matcher,
                            ContactMatcher.MATCHING_ALGORITHM_APPROXIMATE);
                }
            }
        }
    }

    /**
     * Loads all candidate rows from the name lookup table and updates match scores based
     * on that data.
     */
    private void matchAllCandidates(SQLiteDatabase db, String selection,
            MatchCandidateList candidates, ContactMatcher matcher, int algorithm) {
        final Cursor c = db.query(Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS, NAME_LOOKUP_COLUMNS,
                selection, null, null, null, null);

        try {
            while (c.moveToNext()) {
                Long contactId = c.getLong(COL_NAME_LOOKUP_CONTACT_ID);
                String name = c.getString(COL_NORMALIZED_NAME);
                int nameType = c.getInt(COL_NAME_TYPE);

                // Determine which candidate produced this match
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

    private void lookupPhoneMatches(SQLiteDatabase db, String phoneNumber, ContactMatcher matcher) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        mOpenHelper.buildPhoneLookupAndRawContactQuery(qb, phoneNumber);
        Cursor c = qb.query(db, CONTACT_ID_COLUMNS, RawContactsColumns.AGGREGATION_NEEDED + "=0",
                null, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(COL_CONTACT_ID);
                matcher.updateScoreWithPhoneNumberMatch(contactId);
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds exact email matches and updates their match scores.
     */
    private void lookupEmailMatches(SQLiteDatabase db, String address, ContactMatcher matcher) {
        long mimetypeId = mOpenHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);
        Cursor c = db.query(EmailLookupQuery.TABLE, EmailLookupQuery.COLUMNS,
                DataColumns.MIMETYPE_ID + "=" + mimetypeId
                        + " AND " + Email.DATA + "=?"
                        + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0",
                new String[] {address}, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(EmailLookupQuery.CONTACT_ID);
                matcher.updateScoreWithEmailMatch(contactId);
            }
        } finally {
            c.close();
        }
    }

    /**
     * Finds exact nickname matches in the name lookup table and updates their match scores.
     */
    private void lookupNicknameMatches(SQLiteDatabase db, String nickname, ContactMatcher matcher) {
        String normalized = NameNormalizer.normalize(nickname);
        Cursor c = db.query(true, Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS, CONTACT_ID_COLUMNS,
                NameLookupColumns.NAME_TYPE + "=" + NameLookupType.NICKNAME + " AND "
                        + NameLookupColumns.NORMALIZED_NAME + "='" + normalized + "' AND "
                        + RawContactsColumns.AGGREGATION_NEEDED + "=0",
                null, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(COL_CONTACT_ID);
                matcher.updateScoreWithNicknameMatch(contactId);
            }
        } finally {
            c.close();
        }
    }

    private interface RawContactsQuery {
        String[] COLUMNS = new String[] {
            RawContactsColumns.CONCRETE_ID,
            RawContactsColumns.DISPLAY_NAME,
            RawContactsColumns.DISPLAY_NAME_SOURCE,
            RawContacts.ACCOUNT_TYPE,
            RawContacts.ACCOUNT_NAME,
            RawContacts.SOURCE_ID,
            RawContacts.CUSTOM_RINGTONE,
            RawContacts.SEND_TO_VOICEMAIL,
            RawContacts.LAST_TIME_CONTACTED,
            RawContacts.TIMES_CONTACTED,
            RawContacts.STARRED,
            RawContacts.IS_RESTRICTED,
            DataColumns.CONCRETE_ID,
            DataColumns.CONCRETE_MIMETYPE_ID,
        };

        int RAW_CONTACT_ID = 0;
        int DISPLAY_NAME = 1;
        int DISPLAY_NAME_SOURCE = 2;
        int ACCOUNT_TYPE = 3;
        int ACCOUNT_NAME = 4;
        int SOURCE_ID = 5;
        int CUSTOM_RINGTONE = 6;
        int SEND_TO_VOICEMAIL = 7;
        int LAST_TIME_CONTACTED = 8;
        int TIMES_CONTACTED = 9;
        int STARRED = 10;
        int IS_RESTRICTED = 11;
        int DATA_ID = 12;
        int MIMETYPE_ID = 13;
    }

    /**
     * Computes aggregate-level data for the specified aggregate contact ID.
     */
    private void computeAggregateData(SQLiteDatabase db, long contactId, ContentValues values) {
        computeAggregateData(db, RawContacts.CONTACT_ID + "=" + contactId
                + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0", values);
    }

    /**
     * Computes aggregate-level data from constituent raw contacts.
     */
    private void computeAggregateData(final SQLiteDatabase db, String selection,
            final ContentValues values) {
        long currentRawContactId = -1;
        int bestDisplayNameSource = DisplayNameSources.UNDEFINED;
        String bestDisplayName = null;
        long bestPhotoId = -1;
        String photoAccount = null;
        int totalRowCount = 0;
        int contactSendToVoicemail = 0;
        String contactCustomRingtone = null;
        long contactLastTimeContacted = 0;
        int contactTimesContacted = 0;
        boolean contactStarred = false;
        int singleIsRestricted = 1;
        int hasPhoneNumber = 0;
        StringBuilder lookupKey = new StringBuilder();

        long photoMimeType = mOpenHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);
        long phoneMimeType = mOpenHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);

        String isPhotoSql = "(" + DataColumns.MIMETYPE_ID + "=" + photoMimeType + " AND "
                + Photo.PHOTO + " NOT NULL)";
        String isPhoneSql = "(" + DataColumns.MIMETYPE_ID + "=" + phoneMimeType + " AND "
                + Phone.NUMBER + " NOT NULL)";

        String tables = Tables.RAW_CONTACTS + " LEFT OUTER JOIN " + Tables.DATA + " ON("
                + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                + " AND (" + isPhotoSql + " OR " + isPhoneSql + "))";

        final Cursor c = db.query(tables, RawContactsQuery.COLUMNS, selection, null, null, null,
                null);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(RawContactsQuery.RAW_CONTACT_ID);
                if (rawContactId != currentRawContactId) {
                    currentRawContactId = rawContactId;
                    totalRowCount++;

                    // Display name
                    String displayName = c.getString(RawContactsQuery.DISPLAY_NAME);
                    int displayNameSource = c.getInt(RawContactsQuery.DISPLAY_NAME_SOURCE);
                    if (!TextUtils.isEmpty(displayName)) {
                        if (bestDisplayName == null) {
                            bestDisplayName = displayName;
                            bestDisplayNameSource = displayNameSource;
                        } else if (bestDisplayNameSource != displayNameSource) {
                            if (bestDisplayNameSource < displayNameSource) {
                                bestDisplayName = displayName;
                                bestDisplayNameSource = displayNameSource;
                            }
                        } else if (NameNormalizer.compareComplexity(displayName,
                                bestDisplayName) > 0) {
                            bestDisplayName = displayName;
                        }
                    }

                    // Contact options
                    if (!c.isNull(RawContactsQuery.SEND_TO_VOICEMAIL)) {
                        boolean sendToVoicemail =
                                (c.getInt(RawContactsQuery.SEND_TO_VOICEMAIL) != 0);
                        if (sendToVoicemail) {
                            contactSendToVoicemail++;
                        }
                    }

                    if (contactCustomRingtone == null
                            && !c.isNull(RawContactsQuery.CUSTOM_RINGTONE)) {
                        contactCustomRingtone = c.getString(RawContactsQuery.CUSTOM_RINGTONE);
                    }

                    long lastTimeContacted = c.getLong(RawContactsQuery.LAST_TIME_CONTACTED);
                    if (lastTimeContacted > contactLastTimeContacted) {
                        contactLastTimeContacted = lastTimeContacted;
                    }

                    int timesContacted = c.getInt(RawContactsQuery.TIMES_CONTACTED);
                    if (timesContacted > contactTimesContacted) {
                        contactTimesContacted = timesContacted;
                    }

                    contactStarred |= (c.getInt(RawContactsQuery.STARRED) != 0);

                    // Single restricted
                    if (totalRowCount > 1) {
                        // Not single
                        singleIsRestricted = 0;
                    } else {
                        int isRestricted = c.getInt(RawContactsQuery.IS_RESTRICTED);

                        if (isRestricted == 0) {
                            // Not restricted
                            singleIsRestricted = 0;
                        }
                    }

                    ContactLookupKey.appendToLookupKey(lookupKey,
                            c.getString(RawContactsQuery.ACCOUNT_TYPE),
                            c.getString(RawContactsQuery.ACCOUNT_NAME),
                            c.getString(RawContactsQuery.SOURCE_ID),
                            displayName);
                }

                if (!c.isNull(RawContactsQuery.DATA_ID)) {
                    long dataId = c.getLong(RawContactsQuery.DATA_ID);
                    int mimetypeId = c.getInt(RawContactsQuery.MIMETYPE_ID);
                    if (mimetypeId == photoMimeType) {

                        // For now, just choose the first photo in a list sorted by account name.
                        String account = c.getString(RawContactsQuery.ACCOUNT_NAME);
                        if (photoAccount == null) {
                            photoAccount = account;
                            bestPhotoId = dataId;
                        } else {
                            if (account.compareToIgnoreCase(photoAccount) < 0) {
                                photoAccount = account;
                                bestPhotoId = dataId;
                            }
                        }
                    } else if (mimetypeId == phoneMimeType) {
                        hasPhoneNumber = 1;
                    }
                }

            }
        } finally {
            c.close();
        }

        values.clear();

        // If don't have anything to base the display name on, let's just leave what was in
        // that field hoping that there was something there before and it is still valid.
        if (bestDisplayName != null) {
            values.put(Contacts.DISPLAY_NAME, bestDisplayName);
        }

        if (bestPhotoId != -1) {
            values.put(Contacts.PHOTO_ID, bestPhotoId);
        } else {
            values.putNull(Contacts.PHOTO_ID);
        }

        values.put(Contacts.SEND_TO_VOICEMAIL, totalRowCount == contactSendToVoicemail);
        values.put(Contacts.CUSTOM_RINGTONE, contactCustomRingtone);
        values.put(Contacts.LAST_TIME_CONTACTED, contactLastTimeContacted);
        values.put(Contacts.TIMES_CONTACTED, contactTimesContacted);
        values.put(Contacts.STARRED, contactStarred);
        values.put(Contacts.HAS_PHONE_NUMBER, hasPhoneNumber);
        values.put(ContactsColumns.SINGLE_IS_RESTRICTED, singleIsRestricted);
        values.put(Contacts.LOOKUP_KEY, Uri.encode(lookupKey.toString()));
    }

    private interface PhotoIdQuery {
        String[] COLUMNS = new String[] {
            RawContacts.ACCOUNT_NAME,
            DataColumns.CONCRETE_ID,
        };

        int ACCOUNT_NAME = 0;
        int DATA_ID = 1;
    }

    public void updatePhotoId(SQLiteDatabase db, long rawContactId) {

        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        long bestPhotoId = -1;
        String photoAccount = null;

        long photoMimeType = mOpenHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);

        String tables = Tables.RAW_CONTACTS + " JOIN " + Tables.DATA + " ON("
                + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                + " AND (" + DataColumns.MIMETYPE_ID + "=" + photoMimeType + " AND "
                        + Photo.PHOTO + " NOT NULL))";

        final Cursor c = db.query(tables, PhotoIdQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long dataId = c.getLong(PhotoIdQuery.DATA_ID);

                // For now, just choose the first photo in a list sorted by account name.
                String account = c.getString(PhotoIdQuery.ACCOUNT_NAME);
                if (photoAccount == null) {
                    photoAccount = account;
                    bestPhotoId = dataId;
                } else {
                    if (account.compareToIgnoreCase(photoAccount) < 0) {
                        photoAccount = account;
                        bestPhotoId = dataId;
                    }
                }
            }
        } finally {
            c.close();
        }

        if (bestPhotoId == -1) {
            mPhotoIdUpdate.bindNull(1);
        } else {
            mPhotoIdUpdate.bindLong(1, bestPhotoId);
        }
        mPhotoIdUpdate.bindLong(2, contactId);
        mPhotoIdUpdate.execute();
    }

    private interface DisplayNameQuery {
        String[] COLUMNS = new String[] {
            RawContactsColumns.DISPLAY_NAME,
            RawContactsColumns.DISPLAY_NAME_SOURCE,
        };

        int DISPLAY_NAME = 0;
        int DISPLAY_NAME_SOURCE = 1;
    }

    public void updateDisplayName(SQLiteDatabase db, long rawContactId) {

        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        int bestDisplayNameSource = DisplayNameSources.UNDEFINED;
        String bestDisplayName = null;


        final Cursor c = db.query(Tables.RAW_CONTACTS, DisplayNameQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                String displayName = c.getString(DisplayNameQuery.DISPLAY_NAME);
                int displayNameSource = c.getInt(DisplayNameQuery.DISPLAY_NAME_SOURCE);
                if (!TextUtils.isEmpty(displayName)) {
                    if (bestDisplayName == null) {
                        bestDisplayName = displayName;
                        bestDisplayNameSource = displayNameSource;
                    } else if (bestDisplayNameSource != displayNameSource) {
                        if (bestDisplayNameSource < displayNameSource) {
                            bestDisplayName = displayName;
                            bestDisplayNameSource = displayNameSource;
                        }
                    } else if (NameNormalizer.compareComplexity(displayName,
                            bestDisplayName) > 0) {
                        bestDisplayName = displayName;
                    }
                }
            }
        } finally {
            c.close();
        }

        if (bestDisplayName == null) {
            mDisplayNameUpdate.bindNull(1);
        } else {
            mDisplayNameUpdate.bindString(1, bestDisplayName);
        }
        mDisplayNameUpdate.bindLong(2, contactId);
        mDisplayNameUpdate.execute();
    }

    /**
     * Updates the {@link Contacts#HAS_PHONE_NUMBER} flag for the aggregate contact containing the
     * specified raw contact.
     */
    public void updateHasPhoneNumber(SQLiteDatabase db, long rawContactId) {

        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        mHasPhoneNumberUpdate.bindLong(1, mOpenHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE));
        mHasPhoneNumberUpdate.bindLong(2, contactId);
        mHasPhoneNumberUpdate.bindLong(3, contactId);
        mHasPhoneNumberUpdate.execute();
    }

    private interface LookupKeyQuery {
        String[] COLUMNS = new String[] {
            RawContactsColumns.DISPLAY_NAME,
            RawContacts.ACCOUNT_TYPE,
            RawContacts.ACCOUNT_NAME,
            RawContacts.SOURCE_ID,
        };

        int DISPLAY_NAME = 0;
        int ACCOUNT_TYPE = 1;
        int ACCOUNT_NAME = 2;
        int SOURCE_ID = 3;
    }

    public void updateLookupKey(SQLiteDatabase db, long rawContactId) {
        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        StringBuilder lookupKey = new StringBuilder();
        final Cursor c = db.query(Tables.RAW_CONTACTS, LookupKeyQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                ContactLookupKey.appendToLookupKey(lookupKey,
                        c.getString(LookupKeyQuery.ACCOUNT_TYPE),
                        c.getString(LookupKeyQuery.ACCOUNT_NAME),
                        c.getString(LookupKeyQuery.SOURCE_ID),
                        c.getString(LookupKeyQuery.DISPLAY_NAME));
            }
        } finally {
            c.close();
        }

        if (lookupKey.length() == 0) {
            mLookupKeyUpdate.bindNull(1);
        } else {
            mLookupKeyUpdate.bindString(1, lookupKey.toString());
        }
        mLookupKeyUpdate.bindLong(2, contactId);
        mLookupKeyUpdate.execute();
    }

    /**
     * Execute {@link SQLiteStatement} that will update the
     * {@link Contacts#STARRED} flag for the given {@link Contacts#_ID}.
     */
    protected void updateStarred(long contactId) {
        mStarredUpdate.bindLong(1, contactId);
        mStarredUpdate.execute();
    }

    /**
     * Finds matching contacts and returns a cursor on those.
     */
    public Cursor queryAggregationSuggestions(long contactId, String[] projection,
            HashMap<String, String> projectionMap, int maxSuggestions) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        Cursor c;

        // If this method is called in the middle of aggregation pass, we want to pause the
        // aggregation, but not kill it.
        db.beginTransaction();
        try {
            List<MatchScore> bestMatches = findMatchingContacts(db, contactId, maxSuggestions);
            c = queryMatchingContacts(db, contactId, projection, projectionMap, bestMatches);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return c;
    }

    /**
     * Loads contacts with specified IDs and returns them in the order of IDs in the
     * supplied list.
     */
    private Cursor queryMatchingContacts(final SQLiteDatabase db, long contactId,
            String[] projection, HashMap<String, String> projectionMap,
            List<MatchScore> bestMatches) {

        StringBuilder selection = new StringBuilder();
        selection.append(Contacts._ID);
        selection.append(" IN (");
        for (int i = 0; i < bestMatches.size(); i++) {
            MatchScore matchScore = bestMatches.get(i);
            if (i != 0) {
                selection.append(",");
            }
            selection.append(matchScore.getContactId());
        }
        selection.append(")");

        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(mOpenHelper.getContactView());
        qb.setProjectionMap(projectionMap);

        final Cursor cursor = qb.query(db, projection, selection.toString(), null, null, null,
                Contacts._ID);

        ArrayList<Long> sortedContactIds = new ArrayList<Long>(bestMatches.size());
        for (MatchScore matchScore : bestMatches) {
            sortedContactIds.add(matchScore.getContactId());
        }

        Collections.sort(sortedContactIds);

        int[] positionMap = new int[bestMatches.size()];
        for (int i = 0; i < positionMap.length; i++) {
            long id = bestMatches.get(i).getContactId();
            positionMap[i] = sortedContactIds.indexOf(id);
        }

        return new ReorderingCursorWrapper(cursor, positionMap);
    }

    /**
     * Finds contacts with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     */
    private List<MatchScore> findMatchingContacts(final SQLiteDatabase db,
            long contactId, int maxSuggestions) {

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();

        // Don't aggregate a contact with itself
        matcher.keepOut(contactId);

        final Cursor c = db.query(Tables.RAW_CONTACTS, CONTACT_ID_COLUMN,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(0);
                updateMatchScoresBasedOnDataMatches(db, rawContactId, MODE_SUGGESTIONS, candidates,
                        matcher);
            }
        } finally {
            c.close();
        }

        List<MatchScore> matches = matcher.pickBestMatches(maxSuggestions,
                ContactMatcher.SCORE_THRESHOLD_SUGGEST);

        // TODO: remove the debug logging
        Log.i(TAG, "MATCHES: " + matches);
        return matches;
    }
}
