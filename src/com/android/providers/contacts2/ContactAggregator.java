// Copyright 2009 Google Inc. All Rights Reserved.

package com.android.providers.contacts2;

import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import com.android.providers.contacts2.OpenHelper.MimetypeColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupType;
import com.android.providers.contacts2.OpenHelper.Tables;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.Process;
import android.text.TextUtils;
import android.util.Log;

import java.util.ArrayList;
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

    private static final String[] DATA_JOIN_MIMETYPE_AGGREGATION_COLUMNS = new String[] {
            MimetypeColumns.MIMETYPE, Data.DATA1, Data.DATA2
    };

    private static final String[] NAME_LOOKUP_COLUMNS = new String[] {
            Contacts.AGGREGATE_ID, NameLookupColumns.NORMALIZED_NAME, NameLookupColumns.NAME_TYPE
    };

    // Aggregate contacts if their match score is equal or greater than this threshold
    private static final int SCORE_THRESHOLD_AGGREGATE = 70;

    private final boolean mAsynchronous;
    private final OpenHelper mOpenHelper;
    private HandlerThread mHandlerThread;
    private Handler mMessageHandler;

    private HashMap<Long, MatchScore> mScores = new HashMap<Long, MatchScore>();
    private ArrayList<NameMatchRequest> mMatchRequests = new ArrayList<NameMatchRequest>();
    private int mMatchRequestCount;
    private ContentValues mContentValues = new ContentValues();

    // If true, the aggregator is currently in the process of aggregation
    private boolean mAggregating;

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
                NameLookupType.FULL_NAME, 100);
        setNameMatchScore(NameLookupType.FULL_NAME,
                NameLookupType.FULL_NAME_REVERSE, 90);

        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME, 90);
        setNameMatchScore(NameLookupType.FULL_NAME_REVERSE,
                NameLookupType.FULL_NAME_REVERSE, 100);

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
    private static class MatchScore {
        int score;
        int matchCount;

        public void updateScore(int score) {
            if (score > this.score) {
                this.score = score;
            }
            matchCount++;
        }

        @Override
        public String toString() {
            return String.valueOf(score) + "(" + matchCount + ")";
        }
    }

    /**
     * Captures a request to find potential matches for a given name. The
     * matching algorithm constructs a bunch of MatchRequest objects for various
     * potential matches and then executes the search in bulk.
     */
    private static class NameMatchRequest {
        String name;
        int nameLookupType;

        public NameMatchRequest(String name, int nameLookupType) {
            this.name = name;
            this.nameLookupType = nameLookupType;
        }
    }

    /**
     * Constructor.  Starts a contact aggregation thread.  Call {@link #quit} to kill the
     * aggregation thread.  Call {@link #schedule} to kick off the aggregation process after
     * a delay of {@link #AGGREGATION_DELAY} milliseconds.
     */
    public ContactAggregator(Context context, boolean asynchronous) {
        mAsynchronous = asynchronous;

        mOpenHelper = OpenHelper.getInstance(context);
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
    public synchronized void schedule() {
        if (mAsynchronous) {

            // If we are currently in the process of aggregating - cancel that
            mAggregating = false;

            // If aggregation has already been requested, cancel the previous request
            mMessageHandler.removeMessages(START_AGGREGATION_MESSAGE_ID);

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
                        synchronized (this) {
                            if (!mAggregating) {
                                break;
                            }
                            aggregateContact(db, c.getInt(0));
                            db.yieldIfContendedSafely();
                        }
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
     * Given a specific contact, finds all matching aggregates and chooses the aggregate
     * with the highest match score.  If no such aggregate is found, creates a new aggregate.
     */
    /* package */void aggregateContact(SQLiteDatabase db, int contactId) {

        mScores.clear();
        mMatchRequestCount = 0;

        final Cursor c = db.query(Tables.DATA_JOIN_MIMETYPE,
                DATA_JOIN_MIMETYPE_AGGREGATION_COLUMNS,
                DatabaseUtils.concatenateWhere(Data.CONTACT_ID + "=" + contactId,
                        MIMETYPE_SELECTION_IN_CLAUSE),
                null, null, null, null);

        try {
            while (c.moveToNext()) {
                String mimeType = c.getString(0);
                String data1 = c.getString(1);
                String data2 = c.getString(2);
                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    lookupByStructuredName(db, data1, data2, false);
                }
            }
        } finally {
            c.close();
        }

        long aggregateId = pickBestMatchingAggregate(SCORE_THRESHOLD_AGGREGATE);
        if (aggregateId == -1) {
            mContentValues.clear();
            mContentValues.put(Aggregates.DISPLAY_NAME, "");
            aggregateId = db.insert(Tables.AGGREGATES, Aggregates.DISPLAY_NAME, mContentValues);
        }

        mContentValues.clear();
        mContentValues.put(Contacts.AGGREGATE_ID, aggregateId);
        db.update(Tables.CONTACTS, mContentValues, Contacts._ID + "=" + contactId, null);

        updateDisplayName(db, aggregateId);
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
            DatabaseUtils.appendEscapedSQLString(selection, mMatchRequests.get(i).name);
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
                Long aggregateId = c.getLong(0);
                String name = c.getString(1);
                int nameType = c.getInt(2);

                // Determine which request produced this match
                for (int i = 0; i < mMatchRequestCount; i++) {
                    NameMatchRequest matchRequest = mMatchRequests.get(i);
                    if (matchRequest.name.equals(name)) {
                        updateNameMatchScore(aggregateId, matchRequest.nameLookupType, nameType);
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
    private void updateNameMatchScore(Long aggregateId, int nameLookupType, int nameType) {
        int score = getNameMatchScore(nameLookupType, nameType);
        if (score == 0) {
            return;
        }

        MatchScore matchingScore = mScores.get(aggregateId);
        if (matchingScore == null) {
            matchingScore = new MatchScore();
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
            if (score.score >= threshold
                    && (score.score > maxScore
                            || (score.score == maxScore && score.matchCount > maxMatchCount))) {
                contactId = entry.getKey();
                maxScore = score.score;
                maxMatchCount = score.matchCount;
            }
        }
        return contactId;
    }

    private void addMatchRequest(String name, int nameLookupType) {
        if (mMatchRequestCount >= mMatchRequests.size()) {
            mMatchRequests.add(new NameMatchRequest(name, nameLookupType));
        } else {
            NameMatchRequest request = mMatchRequests.get(mMatchRequestCount);
            request.name = name;
            request.nameLookupType = nameLookupType;
        }
        mMatchRequestCount++;
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
}
