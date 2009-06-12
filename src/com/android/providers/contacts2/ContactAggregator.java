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

package com.android.providers.contacts2;

import com.android.providers.contacts2.OpenHelper.MimetypeColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupType;
import com.android.providers.contacts2.OpenHelper.Tables;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.Process;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.text.TextUtils;
import android.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * ContactAggregator deals with aggregating contact information coming from different sources.
 * Two John Doe contacts from two disjoint sources are presumed to be the same
 * person unless the user declares otherwise.
 * <p>
 * ContactAggregator runs on a separate thread.
 */
public class ContactAggregator {

    private static final String TAG = "ContactAggregator";

    // Message ID used for communication with the aggregator
    private static final int START_AGGREGATION_MESSAGE_ID = 1;

    // Aggregation is delayed by this many milliseconds to allow changes to accumulate
    private static final int AGGREGATION_DELAY = 500;

    // Data mime types used in the contact matching algorithm
    private static final String MIMETYPE_SELECTION_IN_CLAUSE = MimetypeColumns.MIMETYPE + " IN ('"
            + Email.CONTENT_ITEM_TYPE + "','"
            + Nickname.CONTENT_ITEM_TYPE + "','"
            + Phone.CONTENT_ITEM_TYPE + "','"
            + StructuredName.CONTENT_ITEM_TYPE + "')";

    private static final String[] DATA_JOIN_MIMETYPE_COLUMNS = new String[] {
            MimetypeColumns.MIMETYPE,
            Tables.DATA + "." + Data._ID + " AS data_id",
            Data.DATA1,
            Data.DATA2
    };

    private static final int COL_MIMETYPE = 0;
    private static final int COL_DATA_ID = 1;
    private static final int COL_DATA1 = 2;
    private static final int COL_DATA2 = 3;

    private static final String[] NAME_LOOKUP_COLUMNS = new String[] {
            Contacts.AGGREGATE_ID, NameLookupColumns.NORMALIZED_NAME, NameLookupColumns.NAME_TYPE
    };

    private static final int COL_AGGREGATE_ID = 0;
    private static final int COL_NORMALIZED_NAME = 1;
    private static final int COL_NAME_TYPE = 2;

    private static final String[] AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS = new String[]{
            AggregationExceptions.TYPE,
            AggregationExceptions.CONTACT_ID1,
            AggregationExceptions.CONTACT_ID2,
            "contacts1." + Contacts.AGGREGATE_ID,
            "contacts2." + Contacts.AGGREGATE_ID
    };

    private static final int COL_TYPE = 0;
    private static final int COL_CONTACT_ID1 = 1;
    private static final int COL_CONTACT_ID2 = 2;
    private static final int COL_AGGREGATE_ID1 = 3;
    private static final int COL_AGGREGATE_ID2 = 4;

    private static final String[] CONTACT_ID_COLUMN = new String[] { Contacts._ID };

    // Aggregate contacts if their match score is equal or greater than this threshold
    private static final int SCORE_THRESHOLD_AGGREGATE = 70;

    private static final int SCORE_NEVER_MATCH = -1;
    private static final int SCORE_ALWAYS_MATCH = 100;

    private final boolean mAsynchronous;
    private final OpenHelper mOpenHelper;
    private HandlerThread mHandlerThread;
    private Handler mMessageHandler;

    private HashMap<Long, MatchScore> mScores = new HashMap<Long, MatchScore>();
    private ArrayList<NameMatchRequest> mMatchRequests = new ArrayList<NameMatchRequest>();
    private int mMatchRequestCount;
    private ContentValues mContentValues = new ContentValues();

    // If true, the aggregator is currently in the process of aggregation
    private volatile boolean mAggregating;

    /**
     * Name matching scores: a matrix by name type vs. lookup type. For example, if
     * the name type is "full name" while we are looking for a "full name", the
     * score may be 100. If we are looking for a "nickname" but find
     * "first name", the score may be 50 (see specific scores defined below.)
     */
    private static int[] scores = new int[NameLookupType.TYPE_COUNT * NameLookupType.TYPE_COUNT];

    /*
     * Note: the reverse names ({@link NameLookupType#FULL_NAME_REVERSE},
     * {@link NameLookupType#FULL_NAME_REVERSE_CONCATENATED} may appear to be redundant. They are
     * not!  They are useful in three-way aggregation cases when we have, for example, both
     * John Smith and Smith John.  A third contact with the name John Smith should be aggregated
     * with the former rather than the latter.  This is why "reverse" matches have slightly lower
     * scores than direct matches.
     */
    static {
        setNameMatchScore(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME, SCORE_ALWAYS_MATCH);
        setNameMatchScore(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME_REVERSE, 90);

        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME, 90);
        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME_REVERSE, SCORE_ALWAYS_MATCH);

        setNameMatchScore(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 80);
        setNameMatchScore(NameLookupType.FULL_NAME_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);

        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_CONCATENATED, 70);
        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE_CONCATENATED,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 80);

        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FAMILY_NAME_ONLY, 75);
        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 72);
        setNameMatchScore(NameLookupType.FAMILY_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);

        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.GIVEN_NAME_ONLY, 70);
        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_CONCATENATED, 72);
        setNameMatchScore(NameLookupType.GIVEN_NAME_ONLY,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED, 70);
    }

    /**
     * Populates the cell of the score matrix corresponding to the {@code nameType} and
     * {@code lookupType}.
     */
    private static void setNameMatchScore(int lookupType, int nameType, int score) {
        int index = nameType * NameLookupType.TYPE_COUNT + lookupType;
        scores[index] = score;
    }

    /**
     * Returns the match score for the given {@code nameType} and {@code lookupType}.
     */
    private static int getNameMatchScore(int lookupType, int nameType) {
        int index = nameType * NameLookupType.TYPE_COUNT + lookupType;
        return scores[index];
    }

    /**
     * Captures the max score and match count for a specific aggregate.  Used in an
     * aggregateId - MatchScore map.
     */
    private static class MatchScore implements Comparable<MatchScore> {
        long mAggregateId;
        int mScore;
        int mMatchCount;

        public MatchScore(long aggregateId) {
            this.mAggregateId = aggregateId;
        }

        public void updateScore(int score) {
            if (mScore == SCORE_NEVER_MATCH) {
                return;
            }

            if ((score == SCORE_NEVER_MATCH) || (score > mScore)) {
                mScore = score;
            }

            mMatchCount++;
        }

        /**
         * Descending order of match score.
         */
        public int compareTo(MatchScore another) {
            if (mScore == another.mScore) {
                return another.mMatchCount - mMatchCount;
            }

            return another.mScore - mScore;
        }

        public String toString() {
            return String.valueOf(mAggregateId) + ": "
                    + String.valueOf(mScore) + "(" + mMatchCount + ")";
        }
    }

    /**
     * Captures a request to find potential matches for a given name. The
     * matching algorithm constructs a bunch of MatchRequest objects for various
     * potential matches and then executes the search in bulk.
     */
    private static class NameMatchRequest {
        String mName;
        int mNameLookupType;

        public NameMatchRequest(String name, int nameLookupType) {
            mName = name;
            mNameLookupType = nameLookupType;
        }
    }

    /**
     * Constructor.  Starts a contact aggregation thread.  Call {@link #quit} to kill the
     * aggregation thread.  Call {@link #schedule} to kick off the aggregation process after
     * a delay of {@link #AGGREGATION_DELAY} milliseconds.
     */
    public ContactAggregator(Context context, boolean asynchronous, OpenHelper openHelper) {
        mAsynchronous = asynchronous;
        mOpenHelper = openHelper;
        if (asynchronous) {
            mHandlerThread = new HandlerThread("ContactAggregator", Process.THREAD_PRIORITY_BACKGROUND);
            mHandlerThread.start();
            mMessageHandler = new Handler(mHandlerThread.getLooper()) {

                @Override
                public void handleMessage(Message msg) {
                    switch (msg.what) {
                        case START_AGGREGATION_MESSAGE_ID:
                            aggregateContacts();
                            break;

                        default:
                            throw new IllegalStateException("Unhandled message: " + msg.what);
                    }
                }
            };
        }
    }

    /**
     * Schedules aggregation pass after a short delay.  This method should be called every time
     * the {@link Contacts#AGGREGATE_ID} field is reset on any record.
     */
    public void schedule() {
        if (mAsynchronous) {

            // If we are currently in the process of aggregating - cancel that
            mAggregating = false;

            // If aggregation has already been requested, cancel the previous request
            mMessageHandler.removeMessages(START_AGGREGATION_MESSAGE_ID);

            // TODO: we need to bound this delay.  If aggregation is delayed by a trickle of
            // requests, we should run it periodically anyway.

            // Schedule aggregation for AGGREGATION_DELAY milliseconds from now
            mMessageHandler.sendEmptyMessageDelayed(START_AGGREGATION_MESSAGE_ID, AGGREGATION_DELAY);
        } else {
            aggregateContacts();
        }
    }

    /**
     * Kills the contact aggregation thread.
     */
    public void quit() {
        if (mAsynchronous) {
            Looper looper = mHandlerThread.getLooper();
            if (looper != null) {
                looper.quit();
            }
            mAggregating = false;
        }
    }

    /**
     * Find all contacts that require aggregation and pass them through aggregation one by one.
     */
    /* package */ void aggregateContacts() {
        Log.i(TAG, "Aggregating contacts");
        mAggregating = true;

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final Cursor c = db.query(Tables.CONTACTS, new String[]{Contacts._ID},
                Contacts.AGGREGATE_ID + " IS NULL", null, null, null, null);
        try {
            if (c.moveToFirst()) {
                db.beginTransaction();
                try {
                    do {
                        if (!mAggregating) {
                            break;
                        }
                        aggregateContact(db, c.getInt(0));
                        db.yieldIfContendedSafely();
                    } while (c.moveToNext());

                    db.setTransactionSuccessful();
                } finally {
                    db.endTransaction();
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Synchronously aggregate the specified contact.
     */
    public void aggregateContact(long contactId) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            aggregateContact(db, contactId);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    /**
     * Marks the specified contact for (re)aggregation.
     *
     * @param contactId contact ID that needs to be (re)aggregated
     */
    public void markContactForAggregation(long contactId) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        long aggregateId = mOpenHelper.getAggregateId(contactId);
        if (aggregateId != 0) {

            // Clear out the aggregate ID field on the contact
            db.execSQL("UPDATE " + Tables.CONTACTS + " SET " + Contacts.AGGREGATE_ID
                    + " = NULL WHERE " + Contacts._ID + "=" + contactId + ";");

            // Clear out data used for aggregation - we will recreate it during aggregation
            db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + " WHERE "
                    + NameLookupColumns.CONTACT_ID + "=" + contactId);

            // Delete the aggregate itself if it no longer has consituent contacts
            db.execSQL("DELETE FROM " + Tables.AGGREGATES + " WHERE " + Aggregates._ID + "="
                    + aggregateId + " AND " + Aggregates._ID + " NOT IN (SELECT "
                    + Contacts.AGGREGATE_ID + " FROM " + Tables.CONTACTS + ");");
        }
    }

    /**
     * Given a specific contact, finds all matching aggregates and chooses the aggregate
     * with the highest match score.  If no such aggregate is found, creates a new aggregate.
     */
    /* package */ synchronized void aggregateContact(SQLiteDatabase db, long contactId) {
        mScores.clear();
        mMatchRequestCount = 0;

        updateMatchScoresBasedOnExceptions(db, contactId);
        updateMatchScoresBasedOnDataMatches(db, contactId);

        long aggregateId = pickBestMatchingAggregate(SCORE_THRESHOLD_AGGREGATE);
        if (aggregateId == -1) {
            ContentValues aggregateValues = new ContentValues();
            aggregateValues.put(Aggregates.DISPLAY_NAME, "");
            aggregateId = db.insert(Tables.AGGREGATES, Aggregates.DISPLAY_NAME, aggregateValues);
        }

        updateContactAggregationData(db, contactId);

        ContentValues contactValues = new ContentValues(1);
        contactValues.put(Contacts.AGGREGATE_ID, aggregateId);
        db.update(Tables.CONTACTS, contactValues, Contacts._ID + "=" + contactId, null);

        updateDisplayName(db, aggregateId);
    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     */
    private void updateMatchScoresBasedOnExceptions(SQLiteDatabase db, long contactId) {
         final Cursor c = db.query(Tables.AGGREGATION_EXCEPTIONS_JOIN_CONTACTS_TWICE,
                AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS,
                AggregationExceptions.CONTACT_ID1 + "=" + contactId
                        + " OR " + AggregationExceptions.CONTACT_ID2 + "=" + contactId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int type = c.getInt(COL_TYPE);
                int score = (type == AggregationExceptions.TYPE_ALWAYS_MATCH
                        ? SCORE_ALWAYS_MATCH : SCORE_NEVER_MATCH);
                long contactId1 = c.getLong(COL_CONTACT_ID1);
                long contactId2 = c.getLong(COL_CONTACT_ID2);
                if (contactId == contactId1) {
                    if (!c.isNull(COL_AGGREGATE_ID2)) {
                        long aggregateId = c.getLong(COL_AGGREGATE_ID2);
                        updateMatchScore(aggregateId, score);
                    }
                } else {
                    if (!c.isNull(COL_AGGREGATE_ID1)) {
                        long aggregateId = c.getLong(COL_AGGREGATE_ID1);
                        updateMatchScore(aggregateId, score);
                    }
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Computes scores for aggregates that have matching data rows.
     */
    private void updateMatchScoresBasedOnDataMatches(SQLiteDatabase db, long contactId) {
        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE,
                DATA_JOIN_MIMETYPE_COLUMNS,
                DatabaseUtils.concatenateWhere(Data.CONTACT_ID + "=" + contactId,
                        MIMETYPE_SELECTION_IN_CLAUSE),
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String mimeType = c.getString(COL_MIMETYPE);
                String data1 = c.getString(COL_DATA1);
                String data2 = c.getString(COL_DATA2);
                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    lookupByStructuredName(db, data1, data2, false);
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Looks for matches based on the full name (first + last).
     */
    private void lookupByStructuredName(SQLiteDatabase db, String givenName, String familyName,
            boolean fuzzy) {
        if (TextUtils.isEmpty(givenName)) {

            // If neither the first nor last name are specified, we won't aggregate
            if (TextUtils.isEmpty(familyName)) {
                return;
            }

            addMatchRequestsFamilyNameOnly(familyName);
        } else if (TextUtils.isEmpty(familyName)) {
            addMatchRequestsGivenNameOnly(givenName);
        } else {
            addMatchRequestsFullName(givenName, familyName);
        }

        lookupNameMatches(db);
    }

    private void addMatchRequestsGivenNameOnly(String givenName) {
        addMatchRequest(givenName.toLowerCase(), NameLookupType.GIVEN_NAME_ONLY);
    }

    private void addMatchRequestsFamilyNameOnly(String familyName) {
        addMatchRequest(familyName.toLowerCase(), NameLookupType.FAMILY_NAME_ONLY);
    }

    private void addMatchRequestsFullName(String givenName, String familyName) {
        final String givenNameLc = givenName.toLowerCase();
        final String familyNameLc = familyName.toLowerCase();
        addMatchRequest(givenNameLc + "." + familyNameLc, NameLookupType.FULL_NAME);
        addMatchRequest(familyNameLc + "." + givenNameLc, NameLookupType.FULL_NAME_REVERSE);
        addMatchRequest(givenNameLc + familyNameLc, NameLookupType.FULL_NAME_CONCATENATED);
        addMatchRequest(familyNameLc + givenNameLc, NameLookupType.FULL_NAME_REVERSE_CONCATENATED);
        addMatchRequest(givenNameLc, NameLookupType.GIVEN_NAME_ONLY);
        addMatchRequest(familyNameLc, NameLookupType.FAMILY_NAME_ONLY);
    }

    /**
     * Given a list of {@link NameMatchRequest}'s, finds all matches and computes their scores.
     */
    private void lookupNameMatches(SQLiteDatabase db) {

        StringBuilder selection = new StringBuilder();
        selection.append(NameLookupColumns.NORMALIZED_NAME);
        selection.append(" IN (");
        for (int i = 0; i < mMatchRequestCount; i++) {
            DatabaseUtils.appendEscapedSQLString(selection, mMatchRequests.get(i).mName);
            selection.append(",");
        }

        // Yank the last comma
        selection.setLength(selection.length() - 1);
        selection.append(") AND ");
        selection.append(Contacts.AGGREGATE_ID);
        selection.append(" NOT NULL");

        final Cursor c = db.query(Tables.NAME_LOOKUP_JOIN_CONTACTS, NAME_LOOKUP_COLUMNS,
                selection.toString(), null, null, null, null);

        try {
            while (c.moveToNext()) {
                Long aggregateId = c.getLong(COL_AGGREGATE_ID);
                String name = c.getString(COL_NORMALIZED_NAME);
                int nameType = c.getInt(COL_NAME_TYPE);

                // Determine which request produced this match
                for (int i = 0; i < mMatchRequestCount; i++) {
                    NameMatchRequest matchRequest = mMatchRequests.get(i);
                    if (matchRequest.mName.equals(name)) {
                        updateNameMatchScore(aggregateId, matchRequest.mNameLookupType, nameType);
                    }
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Updates the overall score for the specified aggregate for a discovered
     * match. The new score is determined by the prior score, by the type of
     * name we were looking for and the type of name we found.
     */
    private void updateNameMatchScore(long aggregateId, int nameLookupType, int nameType) {
        int score = getNameMatchScore(nameLookupType, nameType);
        if (score == 0) {
            return;
        }

        updateMatchScore(aggregateId, score);
    }

    private void updateMatchScore(long aggregateId, int score) {
        MatchScore matchingScore = mScores.get(aggregateId);
        if (matchingScore == null) {
            matchingScore = new MatchScore(aggregateId);
            mScores.put(aggregateId, matchingScore);
        }

        matchingScore.updateScore(score);
    }

    /**
     * Returns the aggregateId with the best match score over the specified threshold or -1
     * if no such aggregate is found.
     */
    private long pickBestMatchingAggregate(int threshold) {
        long contactId = -1;
        int maxScore = 0;
        int maxMatchCount = 0;
        for (Map.Entry<Long, MatchScore> entry : mScores.entrySet()) {
            MatchScore score = entry.getValue();
            if (score.mScore >= threshold
                    && (score.mScore > maxScore
                            || (score.mScore == maxScore && score.mMatchCount > maxMatchCount))) {
                contactId = entry.getKey();
                maxScore = score.mScore;
                maxMatchCount = score.mMatchCount;
            }
        }
        return contactId;
    }

    private void addMatchRequest(String name, int nameLookupType) {
        if (mMatchRequestCount >= mMatchRequests.size()) {
            mMatchRequests.add(new NameMatchRequest(name, nameLookupType));
        } else {
            NameMatchRequest request = mMatchRequests.get(mMatchRequestCount);
            request.mName = name;
            request.mNameLookupType = nameLookupType;
        }
        mMatchRequestCount++;
    }

    /**
     * Prepares the supplied contact for aggregation with other contacts by (re)computing
     * match lookup keys.
     */
    private void updateContactAggregationData(SQLiteDatabase db, long contactId) {
        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE,
                DATA_JOIN_MIMETYPE_COLUMNS,
                DatabaseUtils.concatenateWhere(Data.CONTACT_ID + "=" + contactId,
                        MIMETYPE_SELECTION_IN_CLAUSE),
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String mimeType = c.getString(COL_MIMETYPE);
                long dataId = c.getLong(COL_DATA_ID);
                String data1 = c.getString(COL_DATA1);
                String data2 = c.getString(COL_DATA2);
                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    insertStructuredNameLookup(db, dataId, contactId, data1, data2);
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Inserts various permutations of the contact name into a helper table (name_lookup) to
     * facilitate aggregation of contacts with slightly different names (e.g. first name and last
     * name swapped or concatenated.)
     */
    private void insertStructuredNameLookup(SQLiteDatabase db, long id, long contactId,
            String givenName, String familyName) {
        if (TextUtils.isEmpty(givenName)) {
            if (TextUtils.isEmpty(familyName)) {

                // Nothing specified - nothing to insert in the lookup table
                return;
            }

            insertFamilyNameLookup(db, id, contactId, familyName);
        } else if (TextUtils.isEmpty(familyName)) {
            insertGivenNameLookup(db, id, contactId, givenName);
        } else {
            insertFullNameLookup(db, id, contactId, givenName, familyName);
        }
    }

    /**
     * Populates the name_lookup table when only the first name is specified.
     */
    private void insertGivenNameLookup(SQLiteDatabase db, long id, Long contactId,
            String givenName) {
        final String givenNameLc = givenName.toLowerCase();
        insertNameLookup(db, id, contactId, givenNameLc,
                NameLookupType.GIVEN_NAME_ONLY);
    }

    /**
     * Populates the name_lookup table when only the last name is specified.
     */
    private void insertFamilyNameLookup(SQLiteDatabase db, long id, Long contactId,
            String familyName) {
        final String familyNameLc = familyName.toLowerCase();
        insertNameLookup(db, id, contactId, familyNameLc,
                NameLookupType.FAMILY_NAME_ONLY);
    }

    /**
     * Populates the name_lookup table when both the first and last names are specified.
     */
    private void insertFullNameLookup(SQLiteDatabase db, long id, Long contactId, String givenName,
            String familyName) {
        final String givenNameLc = givenName.toLowerCase();
        final String familyNameLc = familyName.toLowerCase();

        insertNameLookup(db, id, contactId, givenNameLc + "." + familyNameLc,
                NameLookupType.FULL_NAME);
        insertNameLookup(db, id, contactId, familyNameLc + "." + givenNameLc,
                NameLookupType.FULL_NAME_REVERSE);
        insertNameLookup(db, id, contactId, givenNameLc + familyNameLc,
                NameLookupType.FULL_NAME_CONCATENATED);
        insertNameLookup(db, id, contactId, familyNameLc + givenNameLc,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED);
    }

    /**
     * Inserts a single name permutation into the name_lookup table.
     */
    private void insertNameLookup(SQLiteDatabase db, long id, Long contactId, String name,
            int nameType) {
        ContentValues values = new ContentValues(4);
        values.put(NameLookupColumns.DATA_ID, id);
        values.put(NameLookupColumns.CONTACT_ID, contactId);
        values.put(NameLookupColumns.NORMALIZED_NAME, name);
        values.put(NameLookupColumns.NAME_TYPE, nameType);
        db.insert(Tables.NAME_LOOKUP, NameLookupColumns._ID, values);
    }

    /**
     * Updates the aggregate record's {@link Aggregates#DISPLAY_NAME} field. If none of the
     * constituent contacts has a suitable name, leaves the aggregate record unchanged.
     */
    private void updateDisplayName(SQLiteDatabase db, long aggregateId) {
        String displayName = getBestDisplayName(db, aggregateId);

        // If don't have anything to base the display name on, let's just leave what was in
        // that field hoping that there was something there before and it is still valid.
        if (displayName == null) {
            return;
        }

        mContentValues.clear();
        mContentValues.put(Aggregates.DISPLAY_NAME, displayName);
        db.update(Tables.AGGREGATES, mContentValues, Aggregates._ID + "=" + aggregateId, null);
    }

    /**
     * Computes display name for the given aggregate.  Chooses a longer name over a shorter name
     * and a mixed-case name over an all lowercase or uppercase name.
     */
    private String getBestDisplayName(SQLiteDatabase db, long aggregateId) {
        String bestDisplayName = null;

        final Cursor c = db.query(Tables.DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE,
                new String[] {StructuredName.DISPLAY_NAME},
                DatabaseUtils.concatenateWhere(Contacts.AGGREGATE_ID + "=" + aggregateId,
                        Data.MIMETYPE + "='" + StructuredName.CONTENT_ITEM_TYPE + "'"),
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String displayName = c.getString(0);
                if (bestDisplayName == null) {
                    bestDisplayName = displayName;
                } else {
                    int bestLength = bestDisplayName.length();
                    int length = displayName.length();
                    if (bestLength < length
                            || (bestLength == length
                                    && capitalizationScore(bestDisplayName) <
                                            capitalizationScore(displayName))) {
                        bestDisplayName = displayName;
                        bestLength = length;
                    }
                }
            }
        } finally {
            c.close();
        }
        return bestDisplayName;
    }

    /**
     * Computes the capitalization score for a given name giving preference to mixed case
     * display name, e.g. "John Doe" over "john doe" or "JOHN DOE".
     */
    private int capitalizationScore(String displayName) {
        int length = displayName.length();
        int lc = 0;
        int uc = 0;
        for (int i = 0; i < length; i++) {
            char c = displayName.charAt(i);
            if (Character.isLowerCase(c)) {
                lc++;
            } else if (Character.isUpperCase(c)) {
                uc++;
            }
        }

        if (lc != 0 && uc != 0) {
            return 1;
        }

        return 0;
    }

    /**
     * Finds matching aggregates and returns a cursor on those.
     */
    public Cursor queryAggregationSuggestions(long aggregateId, String[] projection,
            HashMap<String, String> projectionMap, int maxSuggestions) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        Cursor c;

        // If this method is called in the middle of aggregation pass, we want to pause the
        // aggregation, but not kill it.
        db.beginTransaction();
        try {
            ArrayList<MatchScore> scores = findMatchingAggregates(db, aggregateId, maxSuggestions);
            c = queryMatchingAggregates(db, aggregateId, projection, projectionMap, scores);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return c;
    }

    /**
     * Loads aggregates with specified IDs and returns them in the order of IDs in the
     * supplied list.
     */
    private Cursor queryMatchingAggregates(final SQLiteDatabase db, long aggregateId, String[] projection,
            HashMap<String, String> projectionMap, ArrayList<MatchScore> scores) {

        StringBuilder selection = new StringBuilder();
        selection.append(Aggregates._ID);
        selection.append(" IN (");
        for (MatchScore matchScore : scores) {
            selection.append(matchScore.mAggregateId);
            selection.append(",");
        }

        if (!scores.isEmpty()) {
            // Yank the last comma
            selection.setLength(selection.length() - 1);
        }
        selection.append(")");

        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Tables.AGGREGATES);
        qb.setProjectionMap(projectionMap);

        final Cursor cursor = qb.query(db, projection, selection.toString(), null, null, null,
                Aggregates._ID);

        ArrayList<Long> sortedAggregateIds = new ArrayList<Long>(scores.size());
        for (MatchScore matchScore : scores) {
            sortedAggregateIds.add(matchScore.mAggregateId);
        }

        Collections.sort(sortedAggregateIds);

        int[] positionMap = new int[scores.size()];
        for (int i = 0; i < positionMap.length; i++) {
            long id = scores.get(i).mAggregateId;
            positionMap[i] = sortedAggregateIds.indexOf(id);
        }

        return new ReorderingCursorWrapper(cursor, positionMap);
    }

    /**
     * Finds aggregates with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     */
    private ArrayList<MatchScore> findMatchingAggregates(final SQLiteDatabase db,
            long aggregateId, int maxSuggestions) {

        mScores.clear();
        mMatchRequestCount = 0;

        final Cursor c = db.query(Tables.CONTACTS, CONTACT_ID_COLUMN,
                Contacts.AGGREGATE_ID + "=" + aggregateId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(0);
                updateMatchScoresBasedOnDataMatches(db, contactId);
            }
        } finally {
            c.close();
        }

        // We don't want to aggregate an aggregate with itself
        mScores.remove(aggregateId);

        ArrayList<MatchScore> matches = new ArrayList<MatchScore>(mScores.values());
        Collections.sort(matches);
        if (matches.size() > maxSuggestions) {
            matches = (ArrayList<MatchScore>)matches.subList(0, maxSuggestions);
        }
        Log.i(TAG, "SCORES: " + matches);
        return matches;
    }
}
