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
import com.android.providers.contacts2.OpenHelper.AggregatesColumns;
import com.android.providers.contacts2.OpenHelper.AggregationExceptionColumns;
import com.android.providers.contacts2.OpenHelper.ContactOptionsColumns;
import com.android.providers.contacts2.OpenHelper.ContactsColumns;
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
public class ContactAggregator implements ContactAggregationScheduler.Aggregator {

    private static final String TAG = "ContactAggregator";

    // Data mime types used in the contact matching algorithm
    private static final String MIMETYPE_SELECTION_IN_CLAUSE = MimetypeColumns.MIMETYPE + " IN ('"
            + Email.CONTENT_ITEM_TYPE + "','"
            + Nickname.CONTENT_ITEM_TYPE + "','"
            + Phone.CONTENT_ITEM_TYPE + "','"
            + StructuredName.CONTENT_ITEM_TYPE + "')";

    private static final String[] DATA_JOIN_MIMETYPE_COLUMNS = new String[] {
            MimetypeColumns.MIMETYPE,
            Data.DATA1,
            Data.DATA2
    };

    private static final int COL_MIMETYPE = 0;
    private static final int COL_DATA1 = 1;
    private static final int COL_DATA2 = 2;

    private static final String[] DATA_JOIN_MIMETYPE_AND_CONTACT_COLUMNS = new String[] {
            Data.DATA1, Data.DATA2, Contacts.AGGREGATE_ID
    };

    private static final int COL_DATA_CONTACT_DATA1 = 0;
    private static final int COL_DATA_CONTACT_DATA2 = 1;
    private static final int COL_DATA_CONTACT_AGGREGATE_ID = 2;

    private static final String[] NAME_LOOKUP_COLUMNS = new String[] {
            Contacts.AGGREGATE_ID, NameLookupColumns.NORMALIZED_NAME, NameLookupColumns.NAME_TYPE
    };

    private static final int COL_AGGREGATE_ID = 0;
    private static final int COL_NORMALIZED_NAME = 1;
    private static final int COL_NAME_TYPE = 2;

    private static final String[] AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS = new String[]{
            AggregationExceptions.TYPE,
            AggregationExceptionColumns.CONTACT_ID1,
            "contacts1." + Contacts.AGGREGATE_ID,
            "contacts2." + Contacts.AGGREGATE_ID
    };

    private static final int COL_TYPE = 0;
    private static final int COL_CONTACT_ID1 = 1;
    private static final int COL_AGGREGATE_ID1 = 2;
    private static final int COL_AGGREGATE_ID2 = 3;

    private static final String[] CONTACT_ID_COLUMN = new String[] { Contacts._ID };
    private static final String[] CONTACTS_JOIN_CONTACT_OPTIONS_COLUMNS = new String[] {
            ContactOptionsColumns.CUSTOM_RINGTONE,
            ContactOptionsColumns.SEND_TO_VOICEMAIL,
    };

    private static final int COL_CUSTOM_RINGTONE = 0;
    private static final int COL_SEND_TO_VOICEMAIL = 1;

    private static final String[] PHONE_LOOKUP_COLUMNS = new String[]{ Contacts.AGGREGATE_ID };
    private static final int COL_PHONE_LOOKUP_AGGREGATE_ID = 0;

    private static final int MODE_INSERT_LOOKUP_DATA = 0;
    private static final int MODE_AGGREGATION = 1;
    private static final int MODE_SUGGESTIONS = 2;

    private final OpenHelper mOpenHelper;
    private final ContactAggregationScheduler mScheduler;

    // Set if the current aggregation pass should be interrupted
    private volatile boolean mCancel;

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
    public ContactAggregator(Context context, OpenHelper openHelper,
            ContactAggregationScheduler scheduler) {
        mOpenHelper = openHelper;
        mScheduler = scheduler;
        mScheduler.setAggregator(this);
        mScheduler.start();

        // Perform an aggregation pass in the beginning, which will most of the time
        // do nothing.  It will only be useful if the content provider has been killed
        // before completing aggregation.
        mScheduler.schedule();
    }

    /**
     * Schedules aggregation pass after a short delay.  This method should be called every time
     * the {@link Contacts#AGGREGATE_ID} field is reset on any record.
     */
    public void schedule() {
        mScheduler.schedule();
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

    /**
     * Find all contacts that require aggregation and pass them through aggregation one by one.
     * Do not call directly.  It is invoked by the scheduler.
     */
    public void run() {
        mCancel = false;
        Log.i(TAG, "Contact aggregation");

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final Cursor c = db.query(Tables.CONTACTS, new String[]{Contacts._ID},
                Contacts.AGGREGATE_ID + " IS NULL", null, null, null, null);

        int totalCount = c.getCount();
        int count = 0;
        try {
            if (c.moveToFirst()) {
                db.beginTransaction();
                try {
                    do {
                        if (mCancel) {
                            break;
                        }
                        aggregateContact(db, c.getInt(0), candidates, matcher, values);
                        count++;
                        db.yieldIfContendedSafely();
                    } while (c.moveToNext());

                    db.setTransactionSuccessful();
                } finally {
                    db.endTransaction();
                }
            }
        } finally {
            c.close();

            // Unless the aggregation pass was not interrupted, reset the last request timestamp
            if (count == totalCount) {
                Log.i(TAG, "Contact aggregation complete: " + totalCount);
            } else {
                Log.i(TAG, "Contact aggregation interrupted: " + count + "/" + totalCount);
            }
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

            // Delete the aggregate itself if it no longer has constituent contacts
            db.execSQL("DELETE FROM " + Tables.AGGREGATES + " WHERE " + Aggregates._ID + "="
                    + aggregateId + " AND " + Aggregates._ID + " NOT IN (SELECT "
                    + Contacts.AGGREGATE_ID + " FROM " + Tables.CONTACTS + ");");
        }
    }

    public void updateAggregateData(long aggregateId) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final ContentValues values = new ContentValues();
        updateAggregateData(db, aggregateId, values);
    }

    /**
     * Given a specific contact, finds all matching aggregates and chooses the aggregate
     * with the highest match score.  If no such aggregate is found, creates a new aggregate.
     */
    /* package */ synchronized void aggregateContact(SQLiteDatabase db, long contactId,
            MatchCandidateList candidates, ContactMatcher matcher, ContentValues values) {
        candidates.clear();
        matcher.clear();

        long aggregateId = pickBestMatchBasedOnExceptions(db, contactId, matcher);
        if (aggregateId == -1) {
            aggregateId = pickBestMatchBasedOnData(db, contactId, candidates, matcher);
        }

        boolean newAgg = false;

        if (aggregateId == -1) {
            newAgg = true;
            ContentValues aggregateValues = new ContentValues();
            aggregateValues.put(Aggregates.DISPLAY_NAME, "");
            aggregateId = db.insert(Tables.AGGREGATES, Aggregates.DISPLAY_NAME, aggregateValues);
        }

        updateContactAggregationData(db, contactId, candidates, values);
        mOpenHelper.setAggregateId(contactId, aggregateId);

        updateAggregateData(db, aggregateId, values);
        updatePrimaries(db, aggregateId, contactId, newAgg);
    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     * Returns the aggregate with the always match exception if any.
     */
    private long pickBestMatchBasedOnExceptions(SQLiteDatabase db, long contactId,
            ContactMatcher matcher) {
        final Cursor c = db.query(Tables.AGGREGATION_EXCEPTIONS_JOIN_CONTACTS_TWICE,
                AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS,
                AggregationExceptionColumns.CONTACT_ID1 + "=" + contactId
                        + " OR " + AggregationExceptionColumns.CONTACT_ID2 + "=" + contactId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int type = c.getInt(COL_TYPE);
                long contactId1 = c.getLong(COL_CONTACT_ID1);
                long aggregateId = -1;
                if (contactId == contactId1) {
                    if (!c.isNull(COL_AGGREGATE_ID2)) {
                        aggregateId = c.getLong(COL_AGGREGATE_ID2);
                    }
                } else {
                    if (!c.isNull(COL_AGGREGATE_ID1)) {
                        aggregateId = c.getLong(COL_AGGREGATE_ID1);
                    }
                }
                if (aggregateId != -1) {
                    if (type == AggregationExceptions.TYPE_KEEP_IN) {
                        return aggregateId;
                    } else {
                        matcher.keepOut(aggregateId);
                    }
                }
            }
        } finally {
            c.close();
        }

        return -1;
    }

    /**
     * Picks the best matching aggregate based on matches between data elements.  It considers
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
    private long pickBestMatchBasedOnData(SQLiteDatabase db, long contactId,
            MatchCandidateList candidates, ContactMatcher matcher) {

        updateMatchScoresBasedOnDataMatches(db, contactId, MODE_AGGREGATION, candidates, matcher);

        // See if we have already found a good match based on name matches alone
        long bestMatch = matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_PRIMARY);
        if (bestMatch == -1) {
            // We haven't found a good match on name, see if we have any matches on phone, email etc
            bestMatch = pickBestMatchBasedOnSecondaryData(db, candidates, matcher);
        }

        return bestMatch;
    }

    /**
     * Picks the best matching aggregate based on secondary data matches.  The method loads
     * structured names for all candidate aggregates and recomputes match scores using approximate
     * matching.
     */
    private long pickBestMatchBasedOnSecondaryData(SQLiteDatabase db,
            MatchCandidateList candidates, ContactMatcher matcher) {
        List<Long> secondaryAggregateIds = matcher.prepareSecondaryMatchCandidates(
                ContactMatcher.SCORE_THRESHOLD_PRIMARY);
        if (secondaryAggregateIds == null) {
            return -1;
        }

        StringBuilder selection = new StringBuilder();
        selection.append(Contacts.AGGREGATE_ID).append(" IN (");
        for (int i = 0; i < secondaryAggregateIds.size(); i++) {
            if (i != 0) {
                selection.append(',');
            }
            selection.append(secondaryAggregateIds.get(i));
        }
        selection.append(") AND ")
                .append(MimetypeColumns.MIMETYPE)
                .append("='")
                .append(StructuredName.CONTENT_ITEM_TYPE)
                .append("'");

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE_CONTACTS,
                DATA_JOIN_MIMETYPE_AND_CONTACT_COLUMNS,
                selection.toString(), null, null, null, null);

        MatchCandidateList nameCandidates = new MatchCandidateList();
        try {
            while (c.moveToNext()) {
                String givenName = c.getString(COL_DATA_CONTACT_DATA1);
                String familyName = c.getString(COL_DATA_CONTACT_DATA2);
                long aggregateId = c.getLong(COL_DATA_CONTACT_AGGREGATE_ID);

                nameCandidates.clear();
                addMatchCandidatesStructuredName(givenName, familyName, MODE_INSERT_LOOKUP_DATA,
                        nameCandidates);

                // Note the N^2 complexity of the following fragment. This is not a huge concern
                // since the number of candidates is very small and in general secondary hits
                // in the absence of primary hits are rare.
                for (int i = 0; i < candidates.mCount; i++) {
                    NameMatchCandidate candidate = candidates.mList.get(i);

                    for (int j = 0; j < nameCandidates.mCount; j++) {
                        NameMatchCandidate nameCandidate = nameCandidates.mList.get(j);
                        matcher.matchName(aggregateId,
                                nameCandidate.mLookupType, nameCandidate.mName,
                                candidate.mLookupType, candidate.mName, true);
                    }
                }
            }
        } finally {
            c.close();
        }

        return matcher.pickBestMatch(ContactMatcher.SCORE_THRESHOLD_SECONDARY);
    }

    /**
     * Computes scores for aggregates that have matching data rows.
     */
    private void updateMatchScoresBasedOnDataMatches(SQLiteDatabase db, long contactId,
            int mode, MatchCandidateList candidates, ContactMatcher matcher) {

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
                    addMatchCandidatesStructuredName(data1, data2, mode, candidates);
                } else if (mimeType.equals(CommonDataKinds.Phone.CONTENT_ITEM_TYPE)) {
                    lookupPhoneMatches(db, data2, matcher);
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
        String givenNameN = NameNormalizer.normalize(givenName);
        candidates.add(givenNameN, NameLookupType.GIVEN_NAME_ONLY);

        String[] clusters = mOpenHelper.getCommonNicknameClusters(givenNameN);
        if (clusters != null) {
            for (int i = 0; i < clusters.length; i++) {
                candidates.add(clusters[i], NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME);
            }
        }
    }

    private void addMatchCandidatesFamilyNameOnly(String familyName,
            MatchCandidateList candidates) {
        String familyNameN = NameNormalizer.normalize(familyName);
        candidates.add(familyNameN, NameLookupType.FAMILY_NAME_ONLY);

        // Take care of first and last names swapped
        String[] clusters = mOpenHelper.getCommonNicknameClusters(familyNameN);
        if (clusters != null) {
            for (int i = 0; i < clusters.length; i++) {
                candidates.add(clusters[i], NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME);
            }
        }
    }

    private void addMatchCandidatesFullName(String givenName, String familyName, int mode,
            MatchCandidateList candidates) {
        final String givenNameN = NameNormalizer.normalize(givenName);
        final String[] givenNameNicknames = mOpenHelper.getCommonNicknameClusters(givenNameN);
        final String familyNameN = NameNormalizer.normalize(familyName);
        final String[] familyNameNicknames = mOpenHelper.getCommonNicknameClusters(familyNameN);
        candidates.add(givenNameN + "." + familyNameN, NameLookupType.FULL_NAME);
        if (givenNameNicknames != null) {
            for (int i = 0; i < givenNameNicknames.length; i++) {
                candidates.add(givenNameNicknames[i] + "." + familyNameN,
                        NameLookupType.FULL_NAME_WITH_NICKNAME);
            }
        }
        candidates.add(familyNameN + "." + givenNameN, NameLookupType.FULL_NAME_REVERSE);
        if (familyNameNicknames != null) {
            for (int i = 0; i < familyNameNicknames.length; i++) {
                candidates.add(familyNameNicknames[i] + "." + givenNameN,
                        NameLookupType.FULL_NAME_WITH_NICKNAME_REVERSE);
            }
        }
        candidates.add(givenNameN + familyNameN, NameLookupType.FULL_NAME_CONCATENATED);
        candidates.add(familyNameN + givenNameN, NameLookupType.FULL_NAME_REVERSE_CONCATENATED);

        if (mode == MODE_AGGREGATION || mode == MODE_SUGGESTIONS) {
            candidates.add(givenNameN, NameLookupType.GIVEN_NAME_ONLY);
            if (givenNameNicknames != null) {
                for (int i = 0; i < givenNameNicknames.length; i++) {
                    candidates.add(givenNameNicknames[i],
                            NameLookupType.GIVEN_NAME_ONLY_AS_NICKNAME);
                }
            }

            candidates.add(familyNameN, NameLookupType.FAMILY_NAME_ONLY);
            if (familyNameNicknames != null) {
                for (int i = 0; i < familyNameNicknames.length; i++) {
                    candidates.add(familyNameNicknames[i],
                            NameLookupType.FAMILY_NAME_ONLY_AS_NICKNAME);
                }
            }
        }
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
                    matcher.matchName(aggregateId, candidate.mLookupType, candidate.mName,
                            nameType, name, approximate);
                }
            }
        } finally {
            c.close();
        }
    }

    private void lookupPhoneMatches(SQLiteDatabase db, String phoneNumber, ContactMatcher matcher) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        OpenHelper.buildPhoneLookupQuery(qb, phoneNumber);
        Cursor c = qb.query(db, PHONE_LOOKUP_COLUMNS,
                Contacts.AGGREGATE_ID + " NOT NULL", null, null, null, null);
        try {
            while (c.moveToNext()) {
                long aggregateId = c.getLong(COL_PHONE_LOOKUP_AGGREGATE_ID);
                matcher.updateScoreWithPhoneNumberMatch(aggregateId);
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
     * Updates aggregate-level data from constituent contacts.
     */
    private void updateAggregateData(final SQLiteDatabase db, long aggregateId,
            final ContentValues values) {
        updateDisplayName(db, aggregateId, values);
        updateSendToVoicemailAndRingtone(db, aggregateId);
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
     * Updates the various {@link AggregatesColumns} primary values based on the
     * newly joined {@link Contacts} entry. If some aggregate primary values are
     * unassigned, primary values from this contact will be promoted as the new
     * super-primaries.
     */
    private void updatePrimaries(SQLiteDatabase db, long aggId, long contactId, boolean newAgg) {
        Cursor cursor = null;

        boolean hasOptimalPhone = false;
        boolean hasFallbackPhone = false;
        boolean hasOptimalEmail = false;
        boolean hasFallbackEmail = false;

        // Read currently recorded aggregate primary values
        try {
            cursor = db.query(Tables.AGGREGATES, Projections.PROJ_AGGREGATE_PRIMARIES,
                    Aggregates._ID + "=" + aggId, null, null, null, null);
            if (cursor.moveToNext()) {
                hasOptimalPhone = (cursor.getLong(Projections.COL_OPTIMAL_PRIMARY_PHONE_ID) != 0);
                hasFallbackPhone = (cursor.getLong(Projections.COL_FALLBACK_PRIMARY_PHONE_ID) != 0);
                hasOptimalEmail = (cursor.getLong(Projections.COL_OPTIMAL_PRIMARY_EMAIL_ID) != 0);
                hasFallbackEmail = (cursor.getLong(Projections.COL_FALLBACK_PRIMARY_EMAIL_ID) != 0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        long candidatePhone = 0;
        long candidateEmail = 0;
        long candidatePackageId = 0;
        boolean candidateIsRestricted = false;

        // Find primary data items from newly-joined contact, returning one
        // candidate for each mimetype.
        try {
            cursor = db.query(Tables.DATA_JOIN_MIMETYPE_CONTACTS_PACKAGE, Projections.PROJ_DATA,
                    Data.CONTACT_ID + "=" + contactId + " AND " + Data.IS_PRIMARY + "=1 AND "
                            + Projections.PRIMARY_MIME_CLAUSE, null, Data.MIMETYPE, null, null);
            while (cursor.moveToNext()) {
                final long dataId = cursor.getLong(Projections.COL_DATA_ID);
                final String mimeType = cursor.getString(Projections.COL_DATA_MIMETYPE);

                candidatePackageId = cursor.getLong(Projections.COL_PACKAGE_ID);
                candidateIsRestricted = (cursor.getInt(Projections.COL_IS_RESTRICTED) == 1);

                if (CommonDataKinds.Phone.CONTENT_ITEM_TYPE.equals(mimeType)) {
                    candidatePhone = dataId;
                } else if (CommonDataKinds.Email.CONTENT_ITEM_TYPE.equals(mimeType)) {
                    candidateEmail = dataId;
                }

            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        final ContentValues values = new ContentValues();

        // If a new aggregate, and single child is restricted, then mark
        // aggregate as being protected by package. Otherwise set as null if
        // multiple under aggregate or not restricted.
        if (newAgg && candidateIsRestricted) {
            values.put(AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID, candidatePackageId);
        } else {
            values.putNull(AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID);
        }

        // If newly joined contact has a primary phone number, consider
        // promoting it up into aggregate as super-primary.
        if (candidatePhone != 0) {
            if (!hasOptimalPhone) {
                values.put(AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID, candidatePhone);
                values.put(AggregatesColumns.OPTIMAL_PRIMARY_PHONE_PACKAGE_ID, candidatePackageId);
            }

            // Also promote to unrestricted value, if none provided yet.
            if (!hasFallbackPhone && !candidateIsRestricted) {
                values.put(AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID, candidatePhone);
            }
        }

        // If newly joined contact has a primary email, consider promoting it up
        // into aggregate as super-primary.
        if (candidateEmail != 0) {
            if (!hasOptimalEmail) {
                values.put(AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID, candidateEmail);
                values.put(AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID, candidatePackageId);
            }

            // Also promote to unrestricted value, if none provided yet.
            if (!hasFallbackEmail && !candidateIsRestricted) {
                values.put(AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID, candidateEmail);
            }
        }

        // Only write updated aggregate values if we made changes.
        if (values.size() > 0) {
            Log.d(TAG, "some sort of promotion is going on: " + values.toString());
            db.update(Tables.AGGREGATES, values, Aggregates._ID + "=" + aggId, null);
        }

    }

    /**
     * Computes display name for the given aggregate.  Chooses a longer name over a shorter name
     * and a mixed-case name over an all lowercase or uppercase name.
     */
    private String getBestDisplayName(SQLiteDatabase db, long aggregateId) {
        String bestDisplayName = null;

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE_CONTACTS_PACKAGE_AGGREGATES,
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
     * Updates the aggregate's send-to-voicemail and custom-ringtone options based on
     * constituent contacts' options.
     */
    private void updateSendToVoicemailAndRingtone(SQLiteDatabase db, long aggregateId) {
        int totalContactCount = 0;
        int sendToVoiceMailCount = 0;
        String customRingtone = null;

        final Cursor c = db.query(Tables.CONTACTS_JOIN_CONTACT_OPTIONS,
                CONTACTS_JOIN_CONTACT_OPTIONS_COLUMNS,
                Contacts.AGGREGATE_ID + "=" + aggregateId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                totalContactCount++;
                if (!c.isNull(COL_SEND_TO_VOICEMAIL)) {
                    boolean sendToVoicemail = (c.getInt(COL_SEND_TO_VOICEMAIL) != 0);
                    if (sendToVoicemail) {
                        sendToVoiceMailCount++;
                    }
                }

                if (customRingtone == null && !c.isNull(COL_CUSTOM_RINGTONE)) {
                    customRingtone = c.getString(COL_CUSTOM_RINGTONE);
                }
            }
        } finally {
            c.close();
        }

        ContentValues values = new ContentValues(2);
        values.put(Aggregates.SEND_TO_VOICEMAIL, totalContactCount == sendToVoiceMailCount);
        values.put(Aggregates.CUSTOM_RINGTONE, customRingtone);

        db.update(Tables.AGGREGATES, values, Aggregates._ID + "=" + aggregateId, null);
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
        for (int i = 0; i < bestMatches.size(); i++) {
            MatchScore matchScore = bestMatches.get(i);
            if (i != 0) {
                selection.append(",");
            }
            selection.append(matchScore.getAggregateId());
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

        // Don't aggregate an aggregate with itself
        matcher.keepOut(aggregateId);

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

        List<MatchScore> matches = matcher.pickBestMatches(maxSuggestions,
                ContactMatcher.SCORE_THRESHOLD_SUGGEST);

        // TODO: remove the debug logging
        Log.i(TAG, "MATCHES: " + matches);
        return matches;
    }

    /**
     * Various database projections used internally.
     */
    private interface Projections {
        static final String[] PROJ_AGGREGATE_PRIMARIES = new String[] {
                AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID,
                AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID,
                AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID,
                AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID,
                AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID,
        };

        static final int COL_OPTIMAL_PRIMARY_PHONE_ID = 0;
        static final int COL_FALLBACK_PRIMARY_PHONE_ID = 1;
        static final int COL_OPTIMAL_PRIMARY_EMAIL_ID = 2;
        static final int COL_FALLBACK_PRIMARY_EMAIL_ID = 3;
        static final int COL_SINGLE_RESTRICTED_PACKAGE_ID = 4;

        static final String[] PROJ_DATA = new String[] {
                Tables.DATA + "." + Data._ID,
                Data.MIMETYPE,
                Contacts.IS_RESTRICTED,
                ContactsColumns.PACKAGE_ID,
        };

        static final int COL_DATA_ID = 0;
        static final int COL_DATA_MIMETYPE = 1;
        static final int COL_IS_RESTRICTED = 2;
        static final int COL_PACKAGE_ID = 3;

        static final String PRIMARY_MIME_CLAUSE = "(" + Data.MIMETYPE + "=\""
                + CommonDataKinds.Phone.CONTENT_ITEM_TYPE + "\" OR " + Data.MIMETYPE + "=\""
                + CommonDataKinds.Email.CONTENT_ITEM_TYPE + "\")";
    }
}
