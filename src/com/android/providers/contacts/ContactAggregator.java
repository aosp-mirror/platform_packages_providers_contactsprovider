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
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DisplayNameSources;
import com.android.providers.contacts.ContactsDatabaseHelper.MimetypesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

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
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
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
import java.util.HashSet;
import java.util.Iterator;
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

    private interface DataMimetypeQuery {

        // Data mime types used in the contact matching algorithm
        String MIMETYPE_SELECTION_IN_CLAUSE = MimetypesColumns.MIMETYPE + " IN ('"
                + Email.CONTENT_ITEM_TYPE + "','"
                + Nickname.CONTENT_ITEM_TYPE + "','"
                + Phone.CONTENT_ITEM_TYPE + "','"
                + StructuredName.CONTENT_ITEM_TYPE + "')";

        String[] COLUMNS = new String[] {
                MimetypesColumns.MIMETYPE, Data.DATA1
        };

        int MIMETYPE = 0;
        int DATA1 = 1;
    }

    private interface DataContactIdQuery {
        String TABLE = Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                Data.DATA1, RawContacts.CONTACT_ID
        };

        int DATA1 = 0;
        int CONTACT_ID = 1;
    }

    private interface NameLookupQuery {
        String TABLE = Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                RawContacts.CONTACT_ID, NameLookupColumns.NORMALIZED_NAME,
                NameLookupColumns.NAME_TYPE
        };

        int CONTACT_ID = 0;
        int NORMALIZED_NAME = 1;
        int NAME_TYPE = 2;
    }

    private interface EmailLookupQuery {
        String TABLE = Tables.DATA_JOIN_RAW_CONTACTS;

        String[] COLUMNS = new String[] {
            RawContacts.CONTACT_ID
        };

        int CONTACT_ID = 0;
    }

    private interface ContactIdQuery {
        String TABLE = Tables.CONTACTS;

        String[] COLUMNS = new String[] {
            Contacts._ID
        };

        int _ID = 0;
    }

    private static final String[] CONTACT_ID_COLUMN = new String[] { RawContacts._ID };
    private static final String[] CONTACT_ID_COLUMNS = new String[]{ RawContacts.CONTACT_ID };
    private static final int COL_CONTACT_ID = 0;

    private static final int MODE_AGGREGATION = 0;
    private static final int MODE_SUGGESTIONS = 1;

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

    // If we encounter more than this many contacts with matching names, aggregate only this many
    private static final int PRIMARY_HIT_LIMIT = 15;

    // If we encounter more than this many contacts with matching phone number or email,
    // don't attempt to aggregate - this is likely an error or a shared corporate data element.
    private static final int SECONDARY_HIT_LIMIT = 20;

    // If we encounter more than this many contacts with matching name during aggregation
    // suggestion lookup, ignore the remaining results.
    private static final int FIRST_LETTER_SUGGESTION_HIT_LIMIT = 100;

    private final ContactsProvider2 mContactsProvider;
    private final ContactsDatabaseHelper mDbHelper;
    private final ContactAggregationScheduler mScheduler;
    private boolean mEnabled = true;

    // Set if the current aggregation pass should be interrupted
    private volatile boolean mCancel;

    /** Precompiled sql statement for setting an aggregated presence */
    private SQLiteStatement mAggregatedPresenceReplace;
    private SQLiteStatement mPresenceContactIdUpdate;
    private SQLiteStatement mRawContactCountQuery;
    private SQLiteStatement mContactDelete;
    private SQLiteStatement mAggregatedPresenceDelete;
    private SQLiteStatement mMarkForAggregation;
    private SQLiteStatement mPhotoIdUpdate;
    private SQLiteStatement mDisplayNameUpdate;
    private SQLiteStatement mHasPhoneNumberUpdate;
    private SQLiteStatement mLookupKeyUpdate;
    private SQLiteStatement mStarredUpdate;
    private SQLiteStatement mContactIdAndMarkAggregatedUpdate;
    private SQLiteStatement mContactIdUpdate;
    private SQLiteStatement mMarkAggregatedUpdate;

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

    private class AggregationNameLookupBuilder extends NameLookupBuilder {

        private final MatchCandidateList mCandidates;

        public AggregationNameLookupBuilder(NameSplitter splitter, MatchCandidateList candidates) {
            super(splitter);
            mCandidates = candidates;
        }

        @Override
        protected void insertNameLookup(long rawContactId, long dataId, int lookupType,
                String name) {
            mCandidates.add(name, lookupType);
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return mContactsProvider.getCommonNicknameClusters(normalizedName);
        }
    }

    /**
     * Constructor.  Starts a contact aggregation thread.  Call {@link #quit} to kill the
     * aggregation thread.  Call {@link #schedule} to kick off the aggregation process after
     * a delay of {@link ContactAggregationScheduler#AGGREGATION_DELAY} milliseconds.
     */
    public ContactAggregator(ContactsProvider2 contactsProvider,
            ContactsDatabaseHelper contactsDatabaseHelper, ContactAggregationScheduler scheduler) {
        mContactsProvider = contactsProvider;
        mDbHelper = contactsDatabaseHelper;

        SQLiteDatabase db = mDbHelper.getReadableDatabase();

        // Since we have no way of determining which custom status was set last,
        // we'll just pick one randomly.  We are using MAX as an approximation of randomness
        mAggregatedPresenceReplace = db.compileStatement(
                "INSERT OR REPLACE INTO " + Tables.AGGREGATED_PRESENCE + "("
                        + AggregatedPresenceColumns.CONTACT_ID + ", "
                        + StatusUpdates.PRESENCE_STATUS
                + ") SELECT ?, MAX(" + StatusUpdates.PRESENCE_STATUS + ") "
                        + " FROM " + Tables.PRESENCE
                        + " WHERE " + PresenceColumns.CONTACT_ID + "=?");

        mRawContactCountQuery = db.compileStatement(
                "SELECT COUNT(" + RawContacts._ID + ")" +
                " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + RawContacts.CONTACT_ID + "=?"
                        + " AND " + RawContacts._ID + "<>?");

        mContactDelete = db.compileStatement(
                "DELETE FROM " + Tables.CONTACTS +
                " WHERE " + Contacts._ID + "=?");

        mAggregatedPresenceDelete = db.compileStatement(
                "DELETE FROM " + Tables.AGGREGATED_PRESENCE +
                " WHERE " + AggregatedPresenceColumns.CONTACT_ID + "=?");

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

        mContactIdAndMarkAggregatedUpdate = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContacts.CONTACT_ID + "=?, "
                        + RawContactsColumns.AGGREGATION_NEEDED + "=0" +
                " WHERE " + RawContacts._ID + "=?");

        mContactIdUpdate = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContacts.CONTACT_ID + "=?" +
                " WHERE " + RawContacts._ID + "=?");

        mMarkAggregatedUpdate = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.AGGREGATION_NEEDED + "=0" +
                " WHERE " + RawContacts._ID + "=?");

        mPresenceContactIdUpdate = db.compileStatement(
                "UPDATE " + Tables.PRESENCE +
                " SET " + PresenceColumns.CONTACT_ID + "=?" +
                " WHERE " + PresenceColumns.RAW_CONTACT_ID + "=?");

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

        SQLiteDatabase db = mDbHelper.getWritableDatabase();
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

            // Aggregation does not affect raw contacts and therefore there is no reason
            // to send a notification to sync adapters.  We do need to notify every one else though.
            mContactsProvider.notifyChange(false);
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


    /**
     * Aggregate all raw contacts that were marked for aggregation in the current transaction.
     * Call just before committing the transaction.
     */
    public void aggregateInTransaction(SQLiteDatabase db) {
        int count = mRawContactsMarkedForAggregation.size();
        if (count == 0) {
            return;
        }

        long start = System.currentTimeMillis();
        Log.i(TAG, "Contact aggregation: " + count);
        EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION, start, -count);

        long rawContactIds[] = new long[count];
        long contactIds[] = new long[count];

        StringBuilder sb = new StringBuilder();
        sb.append(RawContacts._ID + " IN(");
        for (long rawContactId : mRawContactsMarkedForAggregation) {
            sb.append(rawContactId);
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        sb.append(')');

        Cursor c = db.query(AggregationQuery.TABLE, AggregationQuery.COLUMNS, sb.toString(), null,
                null, null, null);

        int index = 0;
        try {
            while (c.moveToNext()) {
                rawContactIds[index] = c.getLong(AggregationQuery._ID);
                contactIds[index] = c.getLong(AggregationQuery.CONTACT_ID);
                index++;
            }
        } finally {
            c.close();
        }

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

        for (int i = 0; i < count; i++) {
            aggregateContact(db, rawContactIds[i], contactIds[i], candidates, matcher, values);
        }

        long elapsedTime = System.currentTimeMillis() - start;
        EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION, elapsedTime, count);
        String performance = count == 0 ? "" : ", " + (elapsedTime / count) + " ms per contact";
        Log.i(TAG, "Contact aggregation complete: " + count + performance);
    }

    public void clearPendingAggregations() {
        mRawContactsMarkedForAggregation.clear();
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
        setContactId(rawContactId, contactId);
        mDbHelper.updateContactVisible(contactId);
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

        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        final ContentValues values = new ContentValues();
        computeAggregateData(db, contactId, values);
        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);

        mDbHelper.updateContactVisible(contactId);
        updateAggregatedPresence(contactId);
    }

    private void updateAggregatedPresence(long contactId) {
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

        if (contactId == currentContactId) {
            // Aggregation unchanged
            markAggregated(rawContactId);
        } else if (contactId == -1) {
            // Splitting an aggregate
            ContentValues contactValues = new ContentValues();
            contactValues.put(RawContactsColumns.DISPLAY_NAME, "");
            computeAggregateData(db,
                    RawContactsColumns.CONCRETE_ID + "=" + rawContactId, contactValues);
            contactId = db.insert(Tables.CONTACTS, Contacts.DISPLAY_NAME, contactValues);
            setContactIdAndMarkAggregated(rawContactId, contactId);
            mDbHelper.updateContactVisible(contactId);

            setPresenceContactId(rawContactId, contactId);

            updateAggregatedPresence(contactId);

            if (currentContactContentsCount > 0) {
                updateAggregateData(currentContactId);
            }
        } else {
            // Joining with an existing aggregate
            if (currentContactContentsCount == 0) {
                // Delete a previous aggregate if it only contained this raw contact
                mContactDelete.bindLong(1, currentContactId);
                mContactDelete.execute();

                mAggregatedPresenceDelete.bindLong(1, currentContactId);
                mAggregatedPresenceDelete.execute();
            }

            setContactIdAndMarkAggregated(rawContactId, contactId);
            computeAggregateData(db, contactId, values);
            db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
            mDbHelper.updateContactVisible(contactId);
            updateAggregatedPresence(contactId);
        }
    }

    /**
     * Updates the contact ID for the specified contact.
     */
    private void setContactId(long rawContactId, long contactId) {
        mContactIdUpdate.bindLong(1, contactId);
        mContactIdUpdate.bindLong(2, rawContactId);
        mContactIdUpdate.execute();
    }

    /**
     * Marks the specified raw contact ID as aggregated
     */
    private void markAggregated(long rawContactId) {
        mMarkAggregatedUpdate.bindLong(1, rawContactId);
        mMarkAggregatedUpdate.execute();
    }

    /**
     * Updates the contact ID for the specified contact and marks the raw contact as aggregated.
     */
    private void setContactIdAndMarkAggregated(long rawContactId, long contactId) {
        mContactIdAndMarkAggregatedUpdate.bindLong(1, contactId);
        mContactIdAndMarkAggregatedUpdate.bindLong(2, rawContactId);
        mContactIdAndMarkAggregatedUpdate.execute();
    }

    private void setPresenceContactId(long rawContactId, long contactId) {
        mPresenceContactIdUpdate.bindLong(1, contactId);
        mPresenceContactIdUpdate.bindLong(2, rawContactId);
        mPresenceContactIdUpdate.execute();
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
                        matcher.keepIn(contactId);
                    } else {
                        matcher.keepOut(contactId);
                    }
                }
            }
        } finally {
            c.close();
        }

        return matcher.pickBestMatch(ContactMatcher.MAX_SCORE);
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
        if (secondaryContactIds == null || secondaryContactIds.size() > SECONDARY_HIT_LIMIT) {
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

        final Cursor c = db.query(DataContactIdQuery.TABLE, DataContactIdQuery.COLUMNS,
                selection.toString(), null, null, null, null);

        MatchCandidateList nameCandidates = new MatchCandidateList();
        AggregationNameLookupBuilder builder =
            new AggregationNameLookupBuilder(mContactsProvider.getNameSplitter(), nameCandidates);
        try {
            while (c.moveToNext()) {
                String name = c.getString(DataContactIdQuery.DATA1);
                long contactId = c.getLong(DataContactIdQuery.CONTACT_ID);

                nameCandidates.clear();
                builder.insertNameLookup(0, 0, name);

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
                DataMimetypeQuery.COLUMNS,
                Data.RAW_CONTACT_ID + "=" + rawContactId + " AND ("
                        + DataMimetypeQuery.MIMETYPE_SELECTION_IN_CLAUSE + ")",
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String mimeType = c.getString(DataMimetypeQuery.MIMETYPE);
                String data = c.getString(DataMimetypeQuery.DATA1);
                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    addMatchCandidatesStructuredName(data, candidates);
                } else if (mimeType.equals(CommonDataKinds.Email.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data)) {
                        addMatchCandidatesEmail(data, mode, candidates);
                        lookupEmailMatches(db, data, matcher);
                    }
                } else if (mimeType.equals(CommonDataKinds.Phone.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data)) {
                        lookupPhoneMatches(db, data, matcher);
                    }
                } else if (mimeType.equals(CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)) {
                    if (!TextUtils.isEmpty(data)) {
                        addMatchCandidatesNickname(data, mode, candidates);
                        lookupNicknameMatches(db, data, matcher);
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
     * Looks for matches based on the full name.
     */
    private void addMatchCandidatesStructuredName(String name, MatchCandidateList candidates) {
        AggregationNameLookupBuilder builder =
                new AggregationNameLookupBuilder(mContactsProvider.getNameSplitter(), candidates);
        builder.insertNameLookup(0, 0, name);
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
                ContactMatcher.MATCHING_ALGORITHM_EXACT, String.valueOf(PRIMARY_HIT_LIMIT));
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
                            + NameLookupColumns.NAME_TYPE + " IN("
                                    + NameLookupType.NAME_COLLATION_KEY + ","
                                    + NameLookupType.EMAIL_BASED_NICKNAME + ","
                                    + NameLookupType.NICKNAME + ")";
                    matchAllCandidates(db, selection, candidates, matcher,
                            ContactMatcher.MATCHING_ALGORITHM_APPROXIMATE,
                            String.valueOf(FIRST_LETTER_SUGGESTION_HIT_LIMIT));
                }
            }
        }
    }

    /**
     * Loads all candidate rows from the name lookup table and updates match scores based
     * on that data.
     */
    private void matchAllCandidates(SQLiteDatabase db, String selection,
            MatchCandidateList candidates, ContactMatcher matcher, int algorithm, String limit) {
        final Cursor c = db.query(NameLookupQuery.TABLE, NameLookupQuery.COLUMNS,
                selection, null, null, null, null, limit);

        try {
            while (c.moveToNext()) {
                Long contactId = c.getLong(NameLookupQuery.CONTACT_ID);
                String name = c.getString(NameLookupQuery.NORMALIZED_NAME);
                int nameType = c.getInt(NameLookupQuery.NAME_TYPE);

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
        mDbHelper.buildPhoneLookupAndRawContactQuery(qb, phoneNumber);
        Cursor c = qb.query(db, CONTACT_ID_COLUMNS, RawContactsColumns.AGGREGATION_NEEDED + "=0",
                null, null, null, null, String.valueOf(SECONDARY_HIT_LIMIT));
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
        long mimetypeId = mDbHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);
        Cursor c = db.query(EmailLookupQuery.TABLE, EmailLookupQuery.COLUMNS,
                DataColumns.MIMETYPE_ID + "=" + mimetypeId
                        + " AND " + Email.DATA + "=?"
                        + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0",
                new String[] {address}, null, null, null, String.valueOf(SECONDARY_HIT_LIMIT));
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
            Data.IS_SUPER_PRIMARY,
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
        int IS_SUPER_PRIMARY = 14;
    }

    /**
     * Computes aggregate-level data for the specified aggregate contact ID.
     */
    private void computeAggregateData(SQLiteDatabase db, long contactId, ContentValues values) {
        computeAggregateData(db, RawContacts.CONTACT_ID + "=" + contactId
                + " AND " + RawContacts.DELETED + "=0", values);
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
        boolean foundSuperPrimaryPhoto = false;
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

        long photoMimeType = mDbHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);
        long phoneMimeType = mDbHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);

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
                    boolean superprimary = c.getInt(RawContactsQuery.IS_SUPER_PRIMARY) != 0;
                    if (mimetypeId == photoMimeType) {

                        // For now, just choose the first photo in a list sorted by account name.
                        String account = c.getString(RawContactsQuery.ACCOUNT_NAME);
                        if (!foundSuperPrimaryPhoto
                                && (superprimary
                                        || photoAccount == null
                                        || account.compareToIgnoreCase(photoAccount) < 0)) {
                            photoAccount = account;
                            bestPhotoId = dataId;
                            foundSuperPrimaryPhoto |= superprimary;
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
            Data.IS_SUPER_PRIMARY,
        };

        int ACCOUNT_NAME = 0;
        int DATA_ID = 1;
        int IS_SUPER_PRIMARY = 2;
    }

    public void updatePhotoId(SQLiteDatabase db, long rawContactId) {

        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        long bestPhotoId = -1;
        String photoAccount = null;

        long photoMimeType = mDbHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);

        String tables = Tables.RAW_CONTACTS + " JOIN " + Tables.DATA + " ON("
                + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                + " AND (" + DataColumns.MIMETYPE_ID + "=" + photoMimeType + " AND "
                        + Photo.PHOTO + " NOT NULL))";

        final Cursor c = db.query(tables, PhotoIdQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long dataId = c.getLong(PhotoIdQuery.DATA_ID);
                boolean superprimary = c.getInt(PhotoIdQuery.IS_SUPER_PRIMARY) != 0;
                if (superprimary) {
                    bestPhotoId = dataId;
                    break;
                }

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

        long contactId = mDbHelper.getContactId(rawContactId);
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

        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        mHasPhoneNumberUpdate.bindLong(1, mDbHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE));
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
        long contactId = mDbHelper.getContactId(rawContactId);
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
     * {@link Contacts#STARRED} flag for the given {@link RawContacts#_ID}.
     */
    protected void updateStarred(long rawContactId) {
        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        mStarredUpdate.bindLong(1, contactId);
        mStarredUpdate.execute();
    }

    /**
     * Finds matching contacts and returns a cursor on those.
     */
    public Cursor queryAggregationSuggestions(SQLiteQueryBuilder qb, String[] projection,
            long contactId, int maxSuggestions, String filter) {
        final SQLiteDatabase db = mDbHelper.getReadableDatabase();

        List<MatchScore> bestMatches = findMatchingContacts(db, contactId);
        return queryMatchingContacts(qb, db, contactId, projection, bestMatches, maxSuggestions,
                filter);
    }

    /**
     * Loads contacts with specified IDs and returns them in the order of IDs in the
     * supplied list.
     */
    private Cursor queryMatchingContacts(SQLiteQueryBuilder qb, SQLiteDatabase db, long contactId,
            String[] projection, List<MatchScore> bestMatches, int maxSuggestions, String filter) {

        StringBuilder sb = new StringBuilder();
        sb.append(Contacts._ID);
        sb.append(" IN (");
        for (int i = 0; i < bestMatches.size(); i++) {
            MatchScore matchScore = bestMatches.get(i);
            if (i != 0) {
                sb.append(",");
            }
            sb.append(matchScore.getContactId());
        }
        sb.append(")");

        if (!TextUtils.isEmpty(filter)) {
            sb.append(" AND " + Contacts._ID + " IN ");
            mContactsProvider.appendContactFilterAsNestedQuery(sb, filter);
        }

        // Run a query and find ids of best matching contacts satisfying the filter (if any)
        HashSet<Long> foundIds = new HashSet<Long>();
        Cursor cursor = db.query(qb.getTables(), ContactIdQuery.COLUMNS, sb.toString(),
                null, null, null, null);
        try {
            while(cursor.moveToNext()) {
                foundIds.add(cursor.getLong(ContactIdQuery._ID));
            }
        } finally {
            cursor.close();
        }

        // Exclude all contacts that did not match the filter
        Iterator<MatchScore> iter = bestMatches.iterator();
        while (iter.hasNext()) {
            long id = iter.next().getContactId();
            if (!foundIds.contains(id)) {
                iter.remove();
            }
        }

        // Limit the number of returned suggestions
        if (bestMatches.size() > maxSuggestions) {
            bestMatches = bestMatches.subList(0, maxSuggestions);
        }

        // Build an in-clause with the remaining contact IDs
        sb.setLength(0);
        sb.append(Contacts._ID);
        sb.append(" IN (");
        for (int i = 0; i < bestMatches.size(); i++) {
            MatchScore matchScore = bestMatches.get(i);
            if (i != 0) {
                sb.append(",");
            }
            sb.append(matchScore.getContactId());
        }
        sb.append(")");

        // Run the final query with the required projection and contact IDs found by the first query
        cursor = qb.query(db, projection, sb.toString(), null, null, null, Contacts._ID);

        // Build a sorted list of discovered IDs
        ArrayList<Long> sortedContactIds = new ArrayList<Long>(bestMatches.size());
        for (MatchScore matchScore : bestMatches) {
            sortedContactIds.add(matchScore.getContactId());
        }

        Collections.sort(sortedContactIds);

        // Map cursor indexes according to the descending order of match scores
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
    private List<MatchScore> findMatchingContacts(final SQLiteDatabase db, long contactId) {

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

        return matcher.pickBestMatches(ContactMatcher.SCORE_THRESHOLD_SUGGEST);
    }
}
