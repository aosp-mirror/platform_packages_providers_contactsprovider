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
import com.android.providers.contacts.OpenHelper.AggregationExceptionColumns;
import com.android.providers.contacts.OpenHelper.Clauses;
import com.android.providers.contacts.OpenHelper.ContactsColumns;
import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.NameLookupColumns;
import com.android.providers.contacts.OpenHelper.NameLookupType;
import com.android.providers.contacts.OpenHelper.RawContactsColumns;
import com.android.providers.contacts.OpenHelper.Tables;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.text.util.Rfc822Tokenizer;
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

    private static final String[] AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS = new String[]{
            AggregationExceptions.TYPE,
            AggregationExceptionColumns.RAW_CONTACT_ID1,
            "raw_contacts1." + RawContacts.CONTACT_ID,
            "raw_contacts2." + RawContacts.CONTACT_ID
    };

    private static final int COL_TYPE = 0;
    private static final int COL_RAW_CONTACT_ID1 = 1;
    private static final int COL_CONTACT_ID1 = 2;
    private static final int COL_CONTACT_ID2 = 3;

    private static final String[] CONTACT_ID_COLUMN = new String[] { RawContacts._ID };

    private static final String[] CONTACT_OPTIONS_COLUMNS = new String[] {
            RawContacts.CUSTOM_RINGTONE,
            RawContacts.SEND_TO_VOICEMAIL,
            RawContacts.LAST_TIME_CONTACTED,
            RawContacts.TIMES_CONTACTED,
            RawContacts.STARRED,
    };

    private static final int COL_CUSTOM_RINGTONE = 0;
    private static final int COL_SEND_TO_VOICEMAIL = 1;
    private static final int COL_LAST_TIME_CONTACTED = 2;
    private static final int COL_TIMES_CONTACTED = 3;
    private static final int COL_STARRED = 4;

    private static final String[] CONTACT_ID_COLUMNS = new String[]{ RawContacts.CONTACT_ID };
    private static final int COL_CONTACT_ID = 0;

    private static final int MODE_INSERT_LOOKUP_DATA = 0;
    private static final int MODE_AGGREGATION = 1;
    private static final int MODE_SUGGESTIONS = 2;

    private final OpenHelper mOpenHelper;
    private final ContactAggregationScheduler mScheduler;
    private boolean mEnabled = true;

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

    public void setEnabled(boolean enabled) {
        mEnabled = enabled;
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

    /**
     * Find all contacts that require aggregation and pass them through aggregation one by one.
     * Do not call directly.  It is invoked by the scheduler.
     */
    public void run() {
        if (!mEnabled) {
            return;
        }

        mCancel = false;
        Log.i(TAG, "Contact aggregation");

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final Cursor c = db.query(Tables.RAW_CONTACTS, new String[]{RawContacts._ID},
                RawContacts.CONTACT_ID + " IS NULL AND "
                        + RawContacts.AGGREGATION_MODE + "=" + RawContacts.AGGREGATION_MODE_DEFAULT,
                null, null, null, null);

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
     * Synchronously aggregate the specified contact assuming an open transaction.
     */
    public void aggregateContact(SQLiteDatabase db, long rawContactId) {
        if (!mEnabled) {
            return;
        }

        MatchCandidateList candidates = new MatchCandidateList();
        ContactMatcher matcher = new ContactMatcher();
        ContentValues values = new ContentValues();
        aggregateContact(db, rawContactId, candidates, matcher, values);
    }

    /**
     * Marks the specified contact for (re)aggregation.
     *
     * @param rawContactId contact ID that needs to be (re)aggregated
     * @return The contact aggregation mode:
     *         {@link RawContacts#AGGREGATION_MODE_DEFAULT},
     *         {@link RawContacts#AGGREGATION_MODE_IMMEDIATE} or
     *         {@link RawContacts#AGGREGATION_MODE_DISABLED}.
     */
    public int markContactForAggregation(SQLiteDatabase db, long rawContactId) {
        if (!mEnabled) {
            return RawContacts.AGGREGATION_MODE_DISABLED;
        }

        int aggregationMode = mOpenHelper.getAggregationMode(rawContactId);
        if (aggregationMode == RawContacts.AGGREGATION_MODE_DISABLED) {
            return aggregationMode;
        }

        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId == 0) {
            // Not aggregated
            return aggregationMode;
        }

        mOpenHelper.removeContactIfSingleton(rawContactId);

        // TODO compiled statements

        // Clear out data used for aggregation - we will recreate it during aggregation
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + " WHERE "
                + NameLookupColumns.RAW_CONTACT_ID + "=" + rawContactId);

        // Clear out the contact ID field on the contact
        ContentValues values = new ContentValues();
        values.putNull(RawContacts.CONTACT_ID);
        db.update(Tables.RAW_CONTACTS, values, RawContacts._ID + "=" + rawContactId, null);

        return aggregationMode;
    }

    public void updateAggregateData(long contactId) {
        if (!mEnabled) {
            return;
        }

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final ContentValues values = new ContentValues();
        updateAggregateData(db, contactId, values);
    }

    /**
     * Given a specific raw contact, finds all matching aggregate contacts and chooses the one
     * with the highest match score.  If no such contact is found, creates a new contact.
     */
    private synchronized void aggregateContact(SQLiteDatabase db, long rawContactId,
            MatchCandidateList candidates, ContactMatcher matcher, ContentValues values) {
        candidates.clear();
        matcher.clear();

        long contactId = pickBestMatchBasedOnExceptions(db, rawContactId, matcher);
        if (contactId == -1) {
            contactId = pickBestMatchBasedOnData(db, rawContactId, candidates, matcher);
        }

        if (contactId == -1) {
            ContentValues contactValues = new ContentValues();
            contactValues.put(Contacts.DISPLAY_NAME, "");
            contactId = db.insert(Tables.CONTACTS, Contacts.DISPLAY_NAME, contactValues);
        }

        updateContactAggregationData(db, rawContactId, candidates, values);
        mOpenHelper.setContactId(rawContactId, contactId);

        updateAggregateData(db, contactId, values);
        updateContactValues(db, contactId);
        mOpenHelper.updateContactVisible(contactId);

    }

    /**
     * Computes match scores based on exceptions entered by the user: always match and never match.
     * Returns the aggregate contact with the always match exception if any.
     */
    private long pickBestMatchBasedOnExceptions(SQLiteDatabase db, long rawContactId,
            ContactMatcher matcher) {
        final Cursor c = db.query(Tables.AGGREGATION_EXCEPTIONS_JOIN_RAW_CONTACTS_TWICE,
                AGGREGATE_EXCEPTION_JOIN_CONTACT_TWICE_COLUMNS,
                AggregationExceptionColumns.RAW_CONTACT_ID1 + "=" + rawContactId
                        + " OR " + AggregationExceptionColumns.RAW_CONTACT_ID2 + "=" + rawContactId,
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int type = c.getInt(COL_TYPE);
                long rawContactId1 = c.getLong(COL_RAW_CONTACT_ID1);
                long contactId = -1;
                if (rawContactId == rawContactId1) {
                    if (!c.isNull(COL_CONTACT_ID2)) {
                        contactId = c.getLong(COL_CONTACT_ID2);
                    }
                } else {
                    if (!c.isNull(COL_CONTACT_ID1)) {
                        contactId = c.getLong(COL_CONTACT_ID1);
                    }
                }
                if (contactId != -1) {
                    if (type == AggregationExceptions.TYPE_KEEP_IN) {
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
                                    candidate.mLookupType, candidate.mName, true);
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
        selection.append(NameLookupColumns.NORMALIZED_NAME);
        selection.append(" IN (");
        for (int i = 0; i < candidates.mCount; i++) {
            DatabaseUtils.appendEscapedSQLString(selection, candidates.mList.get(i).mName);
            selection.append(",");
        }

        // Yank the last comma
        selection.setLength(selection.length() - 1);
        selection.append(") AND ");
        selection.append(RawContacts.CONTACT_ID);
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
                            + firstLetter + "*') AND " + RawContacts.CONTACT_ID + " NOT NULL";
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
        Cursor c = qb.query(db, CONTACT_ID_COLUMNS,
                RawContacts.CONTACT_ID + " NOT NULL", null, null, null, null);
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
        Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS, CONTACT_ID_COLUMNS,
                Clauses.WHERE_EMAIL_MATCHES + " AND " + RawContacts.CONTACT_ID + " NOT NULL",
                new String[]{address}, null, null, null);
        try {
            while (c.moveToNext()) {
                long contactId = c.getLong(COL_CONTACT_ID);
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
                        + RawContacts.CONTACT_ID + " NOT NULL",
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

    /**
     * Prepares the supplied contact for aggregation with other contacts by (re)computing
     * match lookup keys.
     */
    private void updateContactAggregationData(SQLiteDatabase db, long rawContactId,
            MatchCandidateList candidates, ContentValues values) {
        candidates.clear();

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPES,
                DATA_JOIN_MIMETYPE_COLUMNS,
                DatabaseUtils.concatenateWhere(Data.RAW_CONTACT_ID + "=" + rawContactId,
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
                } else if (mimeType.equals(CommonDataKinds.Email.CONTENT_ITEM_TYPE)) {
                    addMatchCandidatesEmail(data2, MODE_INSERT_LOOKUP_DATA, candidates);
                } else if (mimeType.equals(CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)) {
                    addMatchCandidatesNickname(data2, MODE_INSERT_LOOKUP_DATA, candidates);
                }
            }
        } finally {
            c.close();
        }

        for (int i = 0; i < candidates.mCount; i++) {
            NameMatchCandidate candidate = candidates.mList.get(i);
            mOpenHelper.insertNameLookup(rawContactId, candidate.mLookupType, candidate.mName);
        }
    }

    /**
     * Updates aggregate-level data from constituent contacts.
     */
    private void updateAggregateData(final SQLiteDatabase db, long contactId,
            final ContentValues values) {
        updateDisplayName(db, contactId, values);
        updateSendToVoicemailAndRingtone(db, contactId);
        updatePhotoId(db, contactId, values);
    }

    /**
     * Updates the contact record's {@link Contacts#DISPLAY_NAME} field. If none of the
     * constituent raw contacts has a suitable name, leaves the aggregate contact record unchanged.
     */
    private void updateDisplayName(SQLiteDatabase db, long contactId, ContentValues values) {
        String displayName = getBestDisplayName(db, contactId);

        // If don't have anything to base the display name on, let's just leave what was in
        // that field hoping that there was something there before and it is still valid.
        if (displayName == null) {
            return;
        }

        values.clear();
        values.put(Contacts.DISPLAY_NAME, displayName);
        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
    }

    private void updatePhotoId(SQLiteDatabase db, long contactId, ContentValues values) {
        int photoId = choosePhotoId(db, contactId);

        if (photoId == -1) {
            return;
        }

        values.clear();
        values.put(Contacts.PHOTO_ID, photoId);
        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
    }

    /**
     * Updates various {@link ContactsColumns} values based on the newly joined
     * {@link RawContacts} entry.
     */
    private void updateContactValues(SQLiteDatabase db, long contactId) {

        // TODO compiled statement
        String countRestrictedSql = "SELECT COUNT(_id) FROM " + Tables.RAW_CONTACTS + " WHERE "
                + RawContacts.CONTACT_ID + "=" + contactId + " AND " + RawContacts.IS_RESTRICTED + "=1";
        String countUnrestrictedSql = "SELECT COUNT(_id) FROM " + Tables.RAW_CONTACTS + " WHERE "
                + RawContacts.CONTACT_ID + "=" + contactId + " AND " + RawContacts.IS_RESTRICTED + "=0";

        String singleRestrictedSql = "(CASE WHEN (" + countRestrictedSql + ")=1"
                + " AND (" + countUnrestrictedSql + ")=0 THEN 1 ELSE 0 END)";

        String hasPhoneNumberSql = "(CASE WHEN EXISTS (SELECT " + DataColumns.CONCRETE_ID
                + " FROM " + Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS
                + " WHERE " + MimetypesColumns.MIMETYPE + "='" + Phone.CONTENT_ITEM_TYPE + "'"
                + " AND " + Phone.NUMBER + " NOT NULL"
                + " AND " + RawContacts.CONTACT_ID + "=" + contactId + ") THEN 1 ELSE 0 END)";

        String updateSql = "UPDATE " + Tables.CONTACTS + " SET "
                + ContactsColumns.SINGLE_IS_RESTRICTED + "=" + singleRestrictedSql + ","
                + Contacts.HAS_PHONE_NUMBER + "=" + hasPhoneNumberSql
                + " WHERE " + ContactsColumns.CONCRETE_ID + "=" + contactId;

        db.execSQL(updateSql);
    }

    /**
     * Computes display name for the given contact.  Chooses a longer name over a shorter name
     * and a mixed-case name over an all lowercase or uppercase name.
     */
    private String getBestDisplayName(SQLiteDatabase db, long contactId) {
        String bestDisplayName = null;

        final Cursor c = db.query(Tables.RAW_CONTACTS, new String[] {RawContactsColumns.DISPLAY_NAME},
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);

        try {
            while (c.moveToNext()) {
                String displayName = c.getString(0);
                if (!TextUtils.isEmpty(displayName)) {
                    if (bestDisplayName == null) {
                        bestDisplayName = displayName;
                    } else {
                        if (NameNormalizer.compareComplexity(displayName, bestDisplayName) > 0) {
                            bestDisplayName = displayName;
                        }
                    }
                }
            }
        } finally {
            c.close();
        }
        return bestDisplayName;
    }

    /**
     * Iterates over the photos associated with contact defined by contactId, and chooses one
     * to be associated with the contact. Initially this just chooses the first photo in a list
     * sorted by account name.
     */
    private int choosePhotoId(SQLiteDatabase db, long contactId) {
        int chosenPhotoId = -1;
        String chosenAccount = null;

        final Cursor c = db.query(Tables.DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS,
                new String[] {"data._id AS _id", RawContacts.ACCOUNT_NAME},
                DatabaseUtils.concatenateWhere(RawContacts.CONTACT_ID + "=" + contactId,
                        Data.MIMETYPE + "='" + Photo.CONTENT_ITEM_TYPE + "'"),
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                int photoId = c.getInt(0);
                String account = c.getString(1);
                if (chosenAccount == null) {
                    chosenAccount = account;
                    chosenPhotoId = photoId;
                } else {
                    if (account.compareToIgnoreCase(chosenAccount) < 0 ) {
                        chosenAccount = account;
                        chosenPhotoId = photoId;
                    }
                }
            }
        } finally {
            c.close();
        }
        return chosenPhotoId;
    }

    /**
     * Updates the contact's send-to-voicemail and custom-ringtone options based on
     * constituent contacts' options.
     */
    private void updateSendToVoicemailAndRingtone(SQLiteDatabase db, long contactId) {
        int totalContactCount = 0;
        int contactSendToVoicemail = 0;
        String contactCustomRingtone = null;
        long contactLastTimeContacted = 0;
        int contactTimesContacted = 0;
        boolean contactStarred = false;

        final Cursor c = db.query(Tables.RAW_CONTACTS, CONTACT_OPTIONS_COLUMNS,
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);

        try {
            while (c.moveToNext()) {
                totalContactCount++;
                if (!c.isNull(COL_SEND_TO_VOICEMAIL)) {
                    boolean sendToVoicemail = (c.getInt(COL_SEND_TO_VOICEMAIL) != 0);
                    if (sendToVoicemail) {
                        contactSendToVoicemail++;
                    }
                }

                if (contactCustomRingtone == null && !c.isNull(COL_CUSTOM_RINGTONE)) {
                    contactCustomRingtone = c.getString(COL_CUSTOM_RINGTONE);
                }

                long lastTimeContacted = c.getLong(COL_LAST_TIME_CONTACTED);
                if (lastTimeContacted > contactLastTimeContacted) {
                    contactLastTimeContacted = lastTimeContacted;
                }

                int timesContacted = c.getInt(COL_TIMES_CONTACTED);
                if (timesContacted > contactTimesContacted) {
                    contactTimesContacted = timesContacted;
                }

                contactStarred |= (c.getInt(COL_STARRED) != 0);
            }
        } finally {
            c.close();
        }

        ContentValues values = new ContentValues(2);
        values.put(Contacts.SEND_TO_VOICEMAIL, totalContactCount == contactSendToVoicemail);
        values.put(Contacts.CUSTOM_RINGTONE, contactCustomRingtone);
        values.put(Contacts.LAST_TIME_CONTACTED, contactLastTimeContacted);
        values.put(Contacts.TIMES_CONTACTED, contactTimesContacted);
        values.put(Contacts.STARRED, contactStarred);

        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
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

    /**
     * Various database projections used internally.
     */
    private interface Projections {
        static final String[] PROJ_DATA = new String[] {
                Tables.DATA + "." + Data._ID,
                Data.MIMETYPE,
                RawContacts.IS_RESTRICTED,
        };

        static final int COL_DATA_ID = 0;
        static final int COL_DATA_MIMETYPE = 1;
        static final int COL_IS_RESTRICTED = 2;

        static final String PRIMARY_MIME_CLAUSE = "(" + Data.MIMETYPE + "=\""
                + CommonDataKinds.Phone.CONTENT_ITEM_TYPE + "\" OR " + Data.MIMETYPE + "=\""
                + CommonDataKinds.Email.CONTENT_ITEM_TYPE + "\")";
    }
}
