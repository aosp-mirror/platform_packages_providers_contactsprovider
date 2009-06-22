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

import com.android.providers.contacts2.ContactMatcher.MatchScore;
import com.android.providers.contacts2.OpenHelper.AggregationExceptionColumns;
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
import java.util.HashSet;
import java.util.List;


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
            AggregationExceptionColumns.CONTACT_ID1,
            AggregationExceptionColumns.CONTACT_ID2,
            "contacts1." + Contacts.AGGREGATE_ID,
            "contacts2." + Contacts.AGGREGATE_ID
    };

    private static final int COL_TYPE = 0;
    private static final int COL_CONTACT_ID1 = 1;
    private static final int COL_CONTACT_ID2 = 2;
    private static final int COL_AGGREGATE_ID1 = 3;
    private static final int COL_AGGREGATE_ID2 = 4;

    private static final String[] CONTACT_ID_COLUMN = new String[] { Contacts._ID };

    // Automatically aggregate contacts if their match score is equal or greater than this threshold
    private static final int SCORE_THRESHOLD_AGGREGATE = 70;

    // Suggest to aggregate contacts if their match score is equal or greater than this threshold
    private static final int SCORE_THRESHOLD_SUGGEST = 50;

    private static final int MODE_INSERT_LOOKUP_DATA = 0;
    private static final int MODE_AGGREGATION = 1;
    private static final int MODE_SUGGESTIONS = 2;

    private final boolean mAsynchronous;
    private final OpenHelper mOpenHelper;
    private HandlerThread mHandlerThread;
    private Handler mMessageHandler;

    // If true, the aggregator is currently in the process of aggregation
    private volatile boolean mAggregating;

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

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

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
                        aggregateContact(db, c.getInt(0), candidates, matcher, values);
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
        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            aggregateContact(db, contactId, candidates, matcher, values);
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

    public void updateAggregateData(long aggregateId) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final ContentValues values = new ContentValues();
        updateDisplayName(db, aggregateId, values);
    }

    /**
     * Given a specific contact, finds all matching aggregates and chooses the aggregate
     * with the highest match score.  If no such aggregate is found, creates a new aggregate.
     */
    /* package */ synchronized void aggregateContact(SQLiteDatabase db, long contactId,
                MatchCandidateList candidates, ContactMatcher matcher, ContentValues values) {
        candidates.clear();
        matcher.clear();

        updateMatchScoresBasedOnExceptions(db, contactId, matcher);
        updateMatchScoresBasedOnDataMatches(db, contactId, MODE_AGGREGATION, candidates, matcher);

        long aggregateId = matcher.pickBestMatch(SCORE_THRESHOLD_AGGREGATE);
        if (aggregateId == -1) {
            ContentValues aggregateValues = new ContentValues();
            aggregateValues.put(Aggregates.DISPLAY_NAME, "");
            aggregateId = db.insert(Tables.AGGREGATES, Aggregates.DISPLAY_NAME, aggregateValues);
        }

        updateContactAggregationData(db, contactId, candidates, values);
        mOpenHelper.setAggregateId(contactId, aggregateId);

        updateDisplayName(db, aggregateId, values);
    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     */
    private void updateMatchScoresBasedOnExceptions(SQLiteDatabase db, long contactId,
            ContactMatcher matcher) {
         final Cursor c = db.query(Tables.AGGREGATION_EXCEPTIONS_JOIN_CONTACTS_TWICE,
                AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS,
                AggregationExceptionColumns.CONTACT_ID1 + "=" + contactId
                        + " OR " + AggregationExceptionColumns.CONTACT_ID2 + "=" + contactId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int type = c.getInt(COL_TYPE);
                int score = (type == AggregationExceptions.TYPE_KEEP_IN
                        ? ContactMatcher.ALWAYS_MATCH
                        : ContactMatcher.NEVER_MATCH);
                long contactId1 = c.getLong(COL_CONTACT_ID1);
                long contactId2 = c.getLong(COL_CONTACT_ID2);
                if (contactId == contactId1) {
                    if (!c.isNull(COL_AGGREGATE_ID2)) {
                        long aggregateId = c.getLong(COL_AGGREGATE_ID2);
                        matcher.updateScore(aggregateId, score);
                    }
                } else {
                    if (!c.isNull(COL_AGGREGATE_ID1)) {
                        long aggregateId = c.getLong(COL_AGGREGATE_ID1);
                        matcher.updateScore(aggregateId, score);
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
    private void updateMatchScoresBasedOnDataMatches(SQLiteDatabase db, long contactId, int mode,
            MatchCandidateList candidates, ContactMatcher matcher) {
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
                    candidates.clear();
                    addMatchCandidatesStructuredName(data1, data2, mode, candidates);
                    lookupNameMatches(db, candidates, matcher);
                    if (mode == MODE_SUGGESTIONS) {
                        lookupApproximateNameMatches(db, candidates, matcher);
                    }
                }
            }
        } finally {
            c.close();
        }

    }

    /**
     * Looks for matches based on the full name (first + last).
     */
    private void addMatchCandidatesStructuredName(String givenName, String familyName, int mode,
            MatchCandidateList candidates) {
        if (TextUtils.isEmpty(givenName)) {

            // If neither the first nor last name are specified, we won't
            // aggregate
            if (TextUtils.isEmpty(familyName)) {
                return;
            }

            addMatchCandidatesFamilyNameOnly(familyName, candidates);
        } else if (TextUtils.isEmpty(familyName)) {
            addMatchCandidatesGivenNameOnly(givenName, candidates);
        } else {
            addMatchCandidatesFullName(givenName, familyName, mode, candidates);
        }
    }

    private void addMatchCandidatesGivenNameOnly(String givenName,
            MatchCandidateList candidates) {
        candidates.add(NameNormalizer.normalize(givenName), NameLookupType.GIVEN_NAME_ONLY);
    }

    private void addMatchCandidatesFamilyNameOnly(String familyName,
            MatchCandidateList candidates) {
        candidates.add(NameNormalizer.normalize(familyName), NameLookupType.FAMILY_NAME_ONLY);
    }

    private void addMatchCandidatesFullName(String givenName, String familyName, int mode,
            MatchCandidateList candidates) {
        final String givenNameN = NameNormalizer.normalize(givenName);
        final String familyNameN = NameNormalizer.normalize(familyName);
        candidates.add(givenNameN + "." + familyNameN, NameLookupType.FULL_NAME);
        candidates.add(familyNameN + "." + givenNameN, NameLookupType.FULL_NAME_REVERSE);
        candidates.add(givenNameN + familyNameN, NameLookupType.FULL_NAME_CONCATENATED);
        candidates.add(familyNameN + givenNameN, NameLookupType.FULL_NAME_REVERSE_CONCATENATED);

        if (mode == MODE_AGGREGATION || mode == MODE_SUGGESTIONS) {
            candidates.add(givenNameN, NameLookupType.GIVEN_NAME_ONLY);
            candidates.add(familyNameN, NameLookupType.FAMILY_NAME_ONLY);
        }
    }

    /**
     * Given a list of {@link NameMatchCandidate}'s, finds all matches and computes their scores.
     */
    private void lookupNameMatches(SQLiteDatabase db, MatchCandidateList candidates,
            ContactMatcher matcher) {

        StringBuilder selection = new StringBuilder();
        selection.append(NameLookupColumns.NORMALIZED_NAME);
        selection.append(" IN (");
        for (int i = 0; i < candidates.mCount; i++) {
            DatabaseUtils.appendEscapedSQLString(selection, candidates.mList.get(i).mName);
            selection.append(",");
        }

        // Yank the last comma
        selection.setLength(selection.length() - 1);
        selection.append(") AND ");
        selection.append(Contacts.AGGREGATE_ID);
        selection.append(" NOT NULL");

        matchAllCandidates(db, selection.toString(), candidates, matcher, false);
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
                            + firstLetter + "*') AND " + Contacts.AGGREGATE_ID + " NOT NULL";
                    matchAllCandidates(db, selection, candidates, matcher, true);
                }
            }
        }
    }

    /**
     * Loads all candidate rows from the name lookup table and updates match scores based
     * on that data.
     */
    private void matchAllCandidates(SQLiteDatabase db, String selection,
            MatchCandidateList candidates, ContactMatcher matcher, boolean approximate) {
        final Cursor c = db.query(Tables.NAME_LOOKUP_JOIN_CONTACTS, NAME_LOOKUP_COLUMNS,
                selection, null, null, null, null);

        try {
            while (c.moveToNext()) {
                Long aggregateId = c.getLong(COL_AGGREGATE_ID);
                String name = c.getString(COL_NORMALIZED_NAME);
                int nameType = c.getInt(COL_NAME_TYPE);

                // Determine which candidate produced this match
                for (int i = 0; i < candidates.mCount; i++) {
                    NameMatchCandidate candidate = candidates.mList.get(i);
                    matcher.match(aggregateId, candidate.mLookupType, candidate.mName,
                            nameType, name, approximate);
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Prepares the supplied contact for aggregation with other contacts by (re)computing
     * match lookup keys.
     */
    private void updateContactAggregationData(SQLiteDatabase db, long contactId,
            MatchCandidateList candidates, ContentValues values) {
        candidates.clear();

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
                    addMatchCandidatesStructuredName(data1, data2, MODE_INSERT_LOOKUP_DATA,
                            candidates);
                }
            }
        } finally {
            c.close();
        }

        for (int i = 0; i < candidates.mCount; i++) {
            NameMatchCandidate candidate = candidates.mList.get(i);
            mOpenHelper.insertNameLookup(contactId, candidate.mLookupType, candidate.mName);
        }
    }

    /**
     * Updates the aggregate record's {@link Aggregates#DISPLAY_NAME} field. If none of the
     * constituent contacts has a suitable name, leaves the aggregate record unchanged.
     */
    private void updateDisplayName(SQLiteDatabase db, long aggregateId, ContentValues values) {
        String displayName = getBestDisplayName(db, aggregateId);

        // If don't have anything to base the display name on, let's just leave what was in
        // that field hoping that there was something there before and it is still valid.
        if (displayName == null) {
            return;
        }

        values.clear();
        values.put(Aggregates.DISPLAY_NAME, displayName);
        db.update(Tables.AGGREGATES, values, Aggregates._ID + "=" + aggregateId, null);
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
                    if (NameNormalizer.compareComplexity(displayName, bestDisplayName) > 0) {
                        bestDisplayName = displayName;
                    }
                }
            }
        } finally {
            c.close();
        }
        return bestDisplayName;
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
            List<MatchScore> bestMatches = findMatchingAggregates(db, aggregateId, maxSuggestions);
            c = queryMatchingAggregates(db, aggregateId, projection, projectionMap, bestMatches);
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
    private Cursor queryMatchingAggregates(final SQLiteDatabase db, long aggregateId,
            String[] projection, HashMap<String, String> projectionMap,
            List<MatchScore> bestMatches) {

        StringBuilder selection = new StringBuilder();
        selection.append(Aggregates._ID);
        selection.append(" IN (");
        for (MatchScore matchScore : bestMatches) {
            selection.append(matchScore.getAggregateId());
            selection.append(",");
        }

        if (!bestMatches.isEmpty()) {
            // Yank the last comma
            selection.setLength(selection.length() - 1);
        }
        selection.append(")");

        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Tables.AGGREGATES);
        qb.setProjectionMap(projectionMap);

        final Cursor cursor = qb.query(db, projection, selection.toString(), null, null, null,
                Aggregates._ID);

        ArrayList<Long> sortedAggregateIds = new ArrayList<Long>(bestMatches.size());
        for (MatchScore matchScore : bestMatches) {
            sortedAggregateIds.add(matchScore.getAggregateId());
        }

        Collections.sort(sortedAggregateIds);

        int[] positionMap = new int[bestMatches.size()];
        for (int i = 0; i < positionMap.length; i++) {
            long id = bestMatches.get(i).getAggregateId();
            positionMap[i] = sortedAggregateIds.indexOf(id);
        }

        return new ReorderingCursorWrapper(cursor, positionMap);
    }

    /**
     * Finds aggregates with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     */
    private List<MatchScore> findMatchingAggregates(final SQLiteDatabase db,
            long aggregateId, int maxSuggestions) {

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();

        final Cursor c = db.query(Tables.CONTACTS, CONTACT_ID_COLUMN,
                Contacts.AGGREGATE_ID + "=" + aggregateId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(0);
                updateMatchScoresBasedOnDataMatches(db, contactId, MODE_SUGGESTIONS, candidates,
                        matcher);
            }
        } finally {
            c.close();
        }

        // We don't want to aggregate an aggregate with itself
        matcher.remove(aggregateId);

        List<MatchScore> matches = matcher.pickBestMatches(maxSuggestions,
                SCORE_THRESHOLD_SUGGEST);

        // TODO: remove the debug logging
        Log.i(TAG, "MATCHES: " + matches);
        return matches;
    }
}
