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

import static com.android.providers.contacts.aggregation.util.RawContactMatcher.SCORE_THRESHOLD_PRIMARY;
import static com.android.providers.contacts.aggregation.util.RawContactMatcher.SCORE_THRESHOLD_SECONDARY;
import static com.android.providers.contacts.aggregation.util.RawContactMatcher.SCORE_THRESHOLD_SUGGEST;

import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Identity;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.DisplayNameSources;
import android.provider.ContactsContract.FullNameStyle;
import android.provider.ContactsContract.PhotoFiles;
import android.provider.ContactsContract.PinnedPositions;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.text.TextUtils;
import android.util.EventLog;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.ContactLookupKey;
import com.android.providers.contacts.ContactsDatabaseHelper;
import com.android.providers.contacts.ContactsDatabaseHelper.AccountsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.StatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.ContactsDatabaseHelper.Views;
import com.android.providers.contacts.ContactsProvider2;
import com.android.providers.contacts.NameLookupBuilder;
import com.android.providers.contacts.NameNormalizer;
import com.android.providers.contacts.NameSplitter;
import com.android.providers.contacts.PhotoPriorityResolver;
import com.android.providers.contacts.ReorderingCursorWrapper;
import com.android.providers.contacts.TransactionContext;
import com.android.providers.contacts.aggregation.util.CommonNicknameCache;
import com.android.providers.contacts.aggregation.util.ContactAggregatorHelper;
import com.android.providers.contacts.aggregation.util.RawContactMatcher;
import com.android.providers.contacts.aggregation.util.RawContactMatcher.MatchScore;
import com.android.providers.contacts.aggregation.util.RawContactMatchingCandidates;
import com.android.providers.contacts.database.ContactsTableUtil;
import com.android.providers.contacts.util.Clock;

import com.google.android.collect.Maps;
import com.google.android.collect.Sets;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * ContactAggregator2 deals with aggregating contact information with sufficient matching data
 * points. E.g., two John Doe contacts with same phone numbers are presumed to be the same
 * person unless the user declares otherwise.
 */
public class ContactAggregator2 {

    private static final String TAG = "ContactAggregator2";

    private static final boolean DEBUG_LOGGING = Log.isLoggable(TAG, Log.DEBUG);
    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    private static final String STRUCTURED_NAME_BASED_LOOKUP_SQL =
            NameLookupColumns.NAME_TYPE + " IN ("
                    + NameLookupType.NAME_EXACT + ","
                    + NameLookupType.NAME_VARIANT + ","
                    + NameLookupType.NAME_COLLATION_KEY + ")";


    /**
     * SQL statement that sets the {@link ContactsColumns#LAST_STATUS_UPDATE_ID} column
     * on the contact to point to the latest social status update.
     */
    private static final String UPDATE_LAST_STATUS_UPDATE_ID_SQL =
            "UPDATE " + Tables.CONTACTS +
            " SET " + ContactsColumns.LAST_STATUS_UPDATE_ID + "=" +
                    "(SELECT " + DataColumns.CONCRETE_ID +
                    " FROM " + Tables.STATUS_UPDATES +
                    " JOIN " + Tables.DATA +
                    "   ON (" + StatusUpdatesColumns.DATA_ID + "="
                            + DataColumns.CONCRETE_ID + ")" +
                    " JOIN " + Tables.RAW_CONTACTS +
                    "   ON (" + DataColumns.CONCRETE_RAW_CONTACT_ID + "="
                            + RawContactsColumns.CONCRETE_ID + ")" +
                    " WHERE " + RawContacts.CONTACT_ID + "=?" +
                    " ORDER BY " + StatusUpdates.STATUS_TIMESTAMP + " DESC,"
                            + StatusUpdates.STATUS +
                    " LIMIT 1)" +
            " WHERE " + ContactsColumns.CONCRETE_ID + "=?";

    // From system/core/logcat/event-log-tags
    // aggregator [time, count] will be logged for each aggregator cycle.
    // For the query (as opposed to the merge), count will be negative
    public static final int LOG_SYNC_CONTACTS_AGGREGATION = 2747;

    // If we encounter more than this many contacts with matching names, aggregate only this many
    private static final int PRIMARY_HIT_LIMIT = 15;
    private static final String PRIMARY_HIT_LIMIT_STRING = String.valueOf(PRIMARY_HIT_LIMIT);

    // If we encounter more than this many contacts with matching phone number or email,
    // don't attempt to aggregate - this is likely an error or a shared corporate data element.
    private static final int SECONDARY_HIT_LIMIT = 20;
    private static final String SECONDARY_HIT_LIMIT_STRING = String.valueOf(SECONDARY_HIT_LIMIT);

    // If we encounter no less than this many raw contacts in the best matching contact during
    // aggregation, don't attempt to aggregate - this is likely an error or a shared corporate
    // data element.
    @VisibleForTesting
    static final int AGGREGATION_CONTACT_SIZE_LIMIT = 50;

    // If we encounter more than this many contacts with matching name during aggregation
    // suggestion lookup, ignore the remaining results.
    private static final int FIRST_LETTER_SUGGESTION_HIT_LIMIT = 100;

    // Possible operation types for contacts aggregation.
    private static final int CREATE_NEW_CONTACT = 1;
    private static final int KEEP_INTACT = 0;
    private static final int RE_AGGREGATE = -1;

    private final ContactsProvider2 mContactsProvider;
    private final ContactsDatabaseHelper mDbHelper;
    private final PhotoPriorityResolver mPhotoPriorityResolver;
    private final NameSplitter mNameSplitter;
    private final CommonNicknameCache mCommonNicknameCache;
    private final MatchCandidateList mCandidates = new MatchCandidateList();
    private final RawContactMatcher mMatcher = new RawContactMatcher();
    private final DisplayNameCandidate mDisplayNameCandidate = new DisplayNameCandidate();

    private boolean mEnabled = true;

    /** Precompiled sql statement for setting an aggregated presence */
    private SQLiteStatement mAggregatedPresenceReplace;
    private SQLiteStatement mPresenceContactIdUpdate;
    private SQLiteStatement mRawContactCountQuery;
    private SQLiteStatement mAggregatedPresenceDelete;
    private SQLiteStatement mMarkForAggregation;
    private SQLiteStatement mPhotoIdUpdate;
    private SQLiteStatement mDisplayNameUpdate;
    private SQLiteStatement mLookupKeyUpdate;
    private SQLiteStatement mStarredUpdate;
    private SQLiteStatement mPinnedUpdate;
    private SQLiteStatement mContactIdAndMarkAggregatedUpdate;
    private SQLiteStatement mContactIdUpdate;
    private SQLiteStatement mMarkAggregatedUpdate;
    private SQLiteStatement mContactUpdate;
    private SQLiteStatement mContactInsert;
    private SQLiteStatement mResetPinnedForRawContact;

    private HashMap<Long, Integer> mRawContactsMarkedForAggregation = Maps.newHashMap();

    private String[] mSelectionArgs1 = new String[1];
    private String[] mSelectionArgs2 = new String[2];

    private long mMimeTypeIdIdentity;
    private long mMimeTypeIdEmail;
    private long mMimeTypeIdPhoto;
    private long mMimeTypeIdPhone;
    private String mRawContactsQueryByRawContactId;
    private String mRawContactsQueryByContactId;
    private StringBuilder mSb = new StringBuilder();


    /**
     * Parameter for the suggestion lookup query.
     */
    public static final class AggregationSuggestionParameter {
        public final String kind;
        public final String value;

        public AggregationSuggestionParameter(String kind, String value) {
            this.kind = kind;
            this.value = value;
        }
    }

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

        public boolean isEmpty() {
            return mCount == 0;
        }
    }

    /**
     * A convenience class used in the algorithm that figures out which of available
     * display names to use for an aggregate contact.
     */
    private static class DisplayNameCandidate {
        long rawContactId;
        String displayName;
        int displayNameSource;
        boolean isNameSuperPrimary;
        boolean writableAccount;

        public DisplayNameCandidate() {
            clear();
        }

        public void clear() {
            rawContactId = -1;
            displayName = null;
            displayNameSource = DisplayNameSources.UNDEFINED;
            isNameSuperPrimary = false;
            writableAccount = false;
        }
    }

    /**
     * Constructor.
     */
    public ContactAggregator2(ContactsProvider2 contactsProvider,
            ContactsDatabaseHelper contactsDatabaseHelper,
            PhotoPriorityResolver photoPriorityResolver, NameSplitter nameSplitter,
            CommonNicknameCache commonNicknameCache) {
        mContactsProvider = contactsProvider;
        mDbHelper = contactsDatabaseHelper;
        mPhotoPriorityResolver = photoPriorityResolver;
        mNameSplitter = nameSplitter;
        mCommonNicknameCache = commonNicknameCache;

        SQLiteDatabase db = mDbHelper.getReadableDatabase();

        // Since we have no way of determining which custom status was set last,
        // we'll just pick one randomly.  We are using MAX as an approximation of randomness
        final String replaceAggregatePresenceSql =
                "INSERT OR REPLACE INTO " + Tables.AGGREGATED_PRESENCE + "("
                + AggregatedPresenceColumns.CONTACT_ID + ", "
                + StatusUpdates.PRESENCE + ", "
                + StatusUpdates.CHAT_CAPABILITY + ")"
                + " SELECT " + PresenceColumns.CONTACT_ID + ","
                + StatusUpdates.PRESENCE + ","
                + StatusUpdates.CHAT_CAPABILITY
                + " FROM " + Tables.PRESENCE
                + " WHERE "
                + " (" + StatusUpdates.PRESENCE
                +       " * 10 + " + StatusUpdates.CHAT_CAPABILITY + ")"
                + " = (SELECT "
                + "MAX (" + StatusUpdates.PRESENCE
                +       " * 10 + " + StatusUpdates.CHAT_CAPABILITY + ")"
                + " FROM " + Tables.PRESENCE
                + " WHERE " + PresenceColumns.CONTACT_ID
                + "=?)"
                + " AND " + PresenceColumns.CONTACT_ID
                + "=?;";
        mAggregatedPresenceReplace = db.compileStatement(replaceAggregatePresenceSql);

        mRawContactCountQuery = db.compileStatement(
                "SELECT COUNT(" + RawContacts._ID + ")" +
                " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + RawContacts.CONTACT_ID + "=?"
                        + " AND " + RawContacts._ID + "<>?");

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
                " SET " + Contacts.PHOTO_ID + "=?," + Contacts.PHOTO_FILE_ID + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mDisplayNameUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.NAME_RAW_CONTACT_ID + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mLookupKeyUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.LOOKUP_KEY + "=? " +
                " WHERE " + Contacts._ID + "=?");

        mStarredUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.STARRED + "=(SELECT (CASE WHEN COUNT(" + RawContacts.STARRED
                + ")=0 THEN 0 ELSE 1 END) FROM " + Tables.RAW_CONTACTS + " WHERE "
                + RawContacts.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + " AND "
                + RawContacts.STARRED + "=1)" + " WHERE " + Contacts._ID + "=?");

        mPinnedUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.PINNED + " = IFNULL((SELECT MIN(" + RawContacts.PINNED + ") FROM "
                + Tables.RAW_CONTACTS + " WHERE " + RawContacts.CONTACT_ID + "="
                + ContactsColumns.CONCRETE_ID + " AND " + RawContacts.PINNED + ">"
                + PinnedPositions.UNPINNED + ")," + PinnedPositions.UNPINNED + ") "
                + "WHERE " + Contacts._ID + "=?");

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

        mContactUpdate = db.compileStatement(ContactReplaceSqlStatement.UPDATE_SQL);
        mContactInsert = db.compileStatement(ContactReplaceSqlStatement.INSERT_SQL);

        mResetPinnedForRawContact = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContacts.PINNED + "=" + PinnedPositions.UNPINNED +
                " WHERE " + RawContacts._ID + "=?");

        mMimeTypeIdEmail = mDbHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);
        mMimeTypeIdIdentity = mDbHelper.getMimeTypeId(Identity.CONTENT_ITEM_TYPE);
        mMimeTypeIdPhoto = mDbHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);
        mMimeTypeIdPhone = mDbHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);

        // Query used to retrieve data from raw contacts to populate the corresponding aggregate
        mRawContactsQueryByRawContactId = String.format(Locale.US,
                RawContactsQuery.SQL_FORMAT_BY_RAW_CONTACT_ID,
                mDbHelper.getMimeTypeIdForStructuredName(), mMimeTypeIdPhoto, mMimeTypeIdPhone);

        mRawContactsQueryByContactId = String.format(Locale.US,
                RawContactsQuery.SQL_FORMAT_BY_CONTACT_ID,
                mDbHelper.getMimeTypeIdForStructuredName(), mMimeTypeIdPhoto, mMimeTypeIdPhone);
    }

    public void setEnabled(boolean enabled) {
        mEnabled = enabled;
    }

    public boolean isEnabled() {
        return mEnabled;
    }

    private interface AggregationQuery {
        String SQL =
                "SELECT " + RawContacts._ID + "," + RawContacts.CONTACT_ID +
                        ", " + RawContactsColumns.ACCOUNT_ID +
                " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + RawContacts._ID + " IN(";

        int _ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
    }

    /**
     * Aggregate all raw contacts that were marked for aggregation in the current transaction.
     * Call just before committing the transaction.
     */
    public void aggregateInTransaction(TransactionContext txContext, SQLiteDatabase db) {
        final int markedCount = mRawContactsMarkedForAggregation.size();
        if (markedCount == 0) {
            return;
        }

        final long start = System.currentTimeMillis();
        if (DEBUG_LOGGING) {
            Log.d(TAG, "aggregateInTransaction for " + markedCount + " contacts");
        }

        EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION, start, -markedCount);

        int index = 0;

        // We don't use the cached string builder (namely mSb)  here, as this string can be very
        // long when upgrading (where we re-aggregate all visible contacts) and StringBuilder won't
        // shrink the internal storage.
        // Note: don't use selection args here.  We just include all IDs directly in the selection,
        // because there's a limit for the number of parameters in a query.
        final StringBuilder sbQuery = new StringBuilder();
        sbQuery.append(AggregationQuery.SQL);
        for (long rawContactId : mRawContactsMarkedForAggregation.keySet()) {
            if (index > 0) {
                sbQuery.append(',');
            }
            sbQuery.append(rawContactId);
            index++;
        }

        sbQuery.append(')');

        final long[] rawContactIds;
        final long[] contactIds;
        final long[] accountIds;
        final int actualCount;
        final Cursor c = db.rawQuery(sbQuery.toString(), null);
        try {
            actualCount = c.getCount();
            rawContactIds = new long[actualCount];
            contactIds = new long[actualCount];
            accountIds = new long[actualCount];

            index = 0;
            while (c.moveToNext()) {
                rawContactIds[index] = c.getLong(AggregationQuery._ID);
                contactIds[index] = c.getLong(AggregationQuery.CONTACT_ID);
                accountIds[index] = c.getLong(AggregationQuery.ACCOUNT_ID);
                index++;
            }
        } finally {
            c.close();
        }

        if (DEBUG_LOGGING) {
            Log.d(TAG, "aggregateInTransaction: initial query done.");
        }

        for (int i = 0; i < actualCount; i++) {
            aggregateContact(txContext, db, rawContactIds[i], accountIds[i], contactIds[i],
                    mCandidates, mMatcher);
        }

        long elapsedTime = System.currentTimeMillis() - start;
        EventLog.writeEvent(LOG_SYNC_CONTACTS_AGGREGATION, elapsedTime, actualCount);

        if (DEBUG_LOGGING) {
            Log.d(TAG, "Contact aggregation complete: " + actualCount +
                    (actualCount == 0 ? "" : ", " + (elapsedTime / actualCount)
                            + " ms per raw contact"));
        }
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

    public void clearPendingAggregations() {
        // HashMap woulnd't shrink the internal table once expands it, so let's just re-create
        // a new one instead of clear()ing it.
        mRawContactsMarkedForAggregation = Maps.newHashMap();
    }

    public void markNewForAggregation(long rawContactId, int aggregationMode) {
        mRawContactsMarkedForAggregation.put(rawContactId, aggregationMode);
    }

    public void markForAggregation(long rawContactId, int aggregationMode, boolean force) {
        final int effectiveAggregationMode;
        if (!force && mRawContactsMarkedForAggregation.containsKey(rawContactId)) {
            // As per ContactsContract documentation, default aggregation mode
            // does not override a previously set mode
            if (aggregationMode == RawContacts.AGGREGATION_MODE_DEFAULT) {
                effectiveAggregationMode = mRawContactsMarkedForAggregation.get(rawContactId);
            } else {
                effectiveAggregationMode = aggregationMode;
            }
        } else {
            mMarkForAggregation.bindLong(1, rawContactId);
            mMarkForAggregation.execute();
            effectiveAggregationMode = aggregationMode;
        }

        mRawContactsMarkedForAggregation.put(rawContactId, effectiveAggregationMode);
    }

    private static class RawContactIdAndAggregationModeQuery {
        public static final String TABLE = Tables.RAW_CONTACTS;

        public static final String[] COLUMNS = { RawContacts._ID, RawContacts.AGGREGATION_MODE };

        public static final String SELECTION = RawContacts.CONTACT_ID + "=?";

        public static final int _ID = 0;
        public static final int AGGREGATION_MODE = 1;
    }

    /**
     * Marks all constituent raw contacts of an aggregated contact for re-aggregation.
     */
    private void markContactForAggregation(SQLiteDatabase db, long contactId) {
        mSelectionArgs1[0] = String.valueOf(contactId);
        Cursor cursor = db.query(RawContactIdAndAggregationModeQuery.TABLE,
                RawContactIdAndAggregationModeQuery.COLUMNS,
                RawContactIdAndAggregationModeQuery.SELECTION, mSelectionArgs1, null, null, null);
        try {
            if (cursor.moveToFirst()) {
                long rawContactId = cursor.getLong(RawContactIdAndAggregationModeQuery._ID);
                int aggregationMode = cursor.getInt(
                        RawContactIdAndAggregationModeQuery.AGGREGATION_MODE);
                // Don't re-aggregate AGGREGATION_MODE_SUSPENDED / AGGREGATION_MODE_DISABLED.
                // (Also just ignore deprecated AGGREGATION_MODE_IMMEDIATE)
                if (aggregationMode == RawContacts.AGGREGATION_MODE_DEFAULT) {
                    markForAggregation(rawContactId, aggregationMode, true);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Mark all visible contacts for re-aggregation.
     *
     * - Set {@link RawContactsColumns#AGGREGATION_NEEDED} For all visible raw_contacts with
     *   {@link RawContacts#AGGREGATION_MODE_DEFAULT}.
     * - Also put them into {@link #mRawContactsMarkedForAggregation}.
     */
    public int markAllVisibleForAggregation(SQLiteDatabase db) {
        final long start = System.currentTimeMillis();

        // Set AGGREGATION_NEEDED for all visible raw_cotnacts with AGGREGATION_MODE_DEFAULT.
        // (Don't re-aggregate AGGREGATION_MODE_SUSPENDED / AGGREGATION_MODE_DISABLED)
        db.execSQL("UPDATE " + Tables.RAW_CONTACTS + " SET " +
                RawContactsColumns.AGGREGATION_NEEDED + "=1" +
                " WHERE " + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY +
                " AND " + RawContacts.AGGREGATION_MODE + "=" + RawContacts.AGGREGATION_MODE_DEFAULT
                );

        final int count;
        final Cursor cursor = db.rawQuery("SELECT " + RawContacts._ID +
                " FROM " + Tables.RAW_CONTACTS +
                " WHERE " + RawContactsColumns.AGGREGATION_NEEDED + "=1", null);
        try {
            count = cursor.getCount();
            cursor.moveToPosition(-1);
            while (cursor.moveToNext()) {
                final long rawContactId = cursor.getLong(0);
                mRawContactsMarkedForAggregation.put(rawContactId,
                        RawContacts.AGGREGATION_MODE_DEFAULT);
            }
        } finally {
            cursor.close();
        }

        final long end = System.currentTimeMillis();
        Log.i(TAG, "Marked all visible contacts for aggregation: " + count + " raw contacts, " +
                (end - start) + " ms");
        return count;
    }

    /**
     * Creates a new contact based on the given raw contact.  Does not perform aggregation.  Returns
     * the ID of the contact that was created.
     */
    public long onRawContactInsert(
            TransactionContext txContext, SQLiteDatabase db, long rawContactId) {
        long contactId = insertContact(db, rawContactId);
        setContactId(rawContactId, contactId);
        mDbHelper.updateContactVisible(txContext, contactId);
        return contactId;
    }

    protected long insertContact(SQLiteDatabase db, long rawContactId) {
        mSelectionArgs1[0] = String.valueOf(rawContactId);
        computeAggregateData(db, mRawContactsQueryByRawContactId, mSelectionArgs1, mContactInsert);
        return mContactInsert.executeInsert();
    }

    private static final class RawContactIdAndAccountQuery {
        public static final String TABLE = Tables.RAW_CONTACTS;

        public static final String[] COLUMNS = {
                RawContacts.CONTACT_ID,
                RawContactsColumns.ACCOUNT_ID
        };

        public static final String SELECTION = RawContacts._ID + "=?";

        public static final int CONTACT_ID = 0;
        public static final int ACCOUNT_ID = 1;
    }

    public void aggregateContact(
            TransactionContext txContext, SQLiteDatabase db, long rawContactId) {
        if (!mEnabled) {
            return;
        }

        MatchCandidateList candidates = new MatchCandidateList();
        RawContactMatcher matcher = new RawContactMatcher();

        long contactId = 0;
        long accountId = 0;
        mSelectionArgs1[0] = String.valueOf(rawContactId);
        Cursor cursor = db.query(RawContactIdAndAccountQuery.TABLE,
                RawContactIdAndAccountQuery.COLUMNS, RawContactIdAndAccountQuery.SELECTION,
                mSelectionArgs1, null, null, null);
        try {
            if (cursor.moveToFirst()) {
                contactId = cursor.getLong(RawContactIdAndAccountQuery.CONTACT_ID);
                accountId = cursor.getLong(RawContactIdAndAccountQuery.ACCOUNT_ID);
            }
        } finally {
            cursor.close();
        }

        aggregateContact(txContext, db, rawContactId, accountId, contactId,
                candidates, matcher);
    }

    public void updateAggregateData(TransactionContext txContext, long contactId) {
        if (!mEnabled) {
            return;
        }

        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        computeAggregateData(db, contactId, mContactUpdate);
        mContactUpdate.bindLong(ContactReplaceSqlStatement.CONTACT_ID, contactId);
        mContactUpdate.execute();

        mDbHelper.updateContactVisible(txContext, contactId);
        updateAggregatedStatusUpdate(contactId);
    }

    private void updateAggregatedStatusUpdate(long contactId) {
        mAggregatedPresenceReplace.bindLong(1, contactId);
        mAggregatedPresenceReplace.bindLong(2, contactId);
        mAggregatedPresenceReplace.execute();
        updateLastStatusUpdateId(contactId);
    }

    /**
     * Adjusts the reference to the latest status update for the specified contact.
     */
    public void updateLastStatusUpdateId(long contactId) {
        String contactIdString = String.valueOf(contactId);
        mDbHelper.getWritableDatabase().execSQL(UPDATE_LAST_STATUS_UPDATE_ID_SQL,
                new String[]{contactIdString, contactIdString});
    }

    /**
     * Given a specific raw contact, finds all matching raw contacts and re-aggregate them
     * based on the matching connectivity.
     */
    private synchronized void aggregateContact(TransactionContext txContext, SQLiteDatabase db,
            long rawContactId, long accountId, long currentContactId, MatchCandidateList candidates,
            RawContactMatcher matcher) {

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "aggregateContact: rid=" + rawContactId + " cid=" + currentContactId);
        }

        int aggregationMode = RawContacts.AGGREGATION_MODE_DEFAULT;

        Integer aggModeObject = mRawContactsMarkedForAggregation.remove(rawContactId);
        if (aggModeObject != null) {
            aggregationMode = aggModeObject;
        }

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

        Set<Long> newIds = matchingCandidates.getRawContactIdSet();
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
            newIds = tmpIdSet;
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

    private interface RawContactMatchingSelectionStatement {
        String SELECT_COUNT =  "SELECT count(*) " ;
        String SELECT_ID = "SELECT d1." + Data.RAW_CONTACT_ID + ",d2."  + Data.RAW_CONTACT_ID ;
    }

    /**
     * Build sql to check if there is any identity match/mis-match between two sets of raw contact
     * ids on the same namespace.
     */
    private String buildIdentityMatchingSql(String rawContactIdSet1, String rawContactIdSet2,
            boolean isIdentityMatching, boolean countOnly) {
        final String identityType = String.valueOf(mMimeTypeIdIdentity);
        final String matchingOperator = (isIdentityMatching) ? "=" : "!=";
        final String sql =
                " FROM " + Tables.DATA + " AS d1" +
                " JOIN " + Tables.DATA + " AS d2" +
                        " ON (d1." + Identity.IDENTITY + matchingOperator +
                        " d2." + Identity.IDENTITY + " AND" +
                        " d1." + Identity.NAMESPACE + " = d2." + Identity.NAMESPACE + " )" +
                " WHERE d1." + DataColumns.MIMETYPE_ID + " = " + identityType +
                " AND d2." + DataColumns.MIMETYPE_ID + " = " + identityType +
                " AND d1." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet1 + ")" +
                " AND d2." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet2 + ")";
        return (countOnly) ? RawContactMatchingSelectionStatement.SELECT_COUNT + sql :
                RawContactMatchingSelectionStatement.SELECT_ID + sql;
    }

    private String buildEmailMatchingSql(String rawContactIdSet1, String rawContactIdSet2,
            boolean countOnly) {
        final String emailType = String.valueOf(mMimeTypeIdEmail);
        final String sql =
                " FROM " + Tables.DATA + " AS d1" +
                " JOIN " + Tables.DATA + " AS d2" +
                        " ON lower(d1." + Email.ADDRESS + ")= lower(d2." + Email.ADDRESS + ")" +
                " WHERE d1." + DataColumns.MIMETYPE_ID + " = " + emailType +
                " AND d2." + DataColumns.MIMETYPE_ID + " = " + emailType +
                " AND d1." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet1 + ")" +
                " AND d2." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet2 + ")";
        return (countOnly) ? RawContactMatchingSelectionStatement.SELECT_COUNT + sql :
                RawContactMatchingSelectionStatement.SELECT_ID + sql;
    }

    private String buildPhoneMatchingSql(String rawContactIdSet1, String rawContactIdSet2,
            boolean countOnly) {
        // It's a bit tricker because it has to be consistent with
        // updateMatchScoresBasedOnPhoneMatches().
        final String phoneType = String.valueOf(mMimeTypeIdPhone);
        final String sql =
                " FROM " + Tables.PHONE_LOOKUP + " AS p1" +
                " JOIN " + Tables.DATA + " AS d1 ON " +
                        "(d1." + Data._ID + "=p1." + PhoneLookupColumns.DATA_ID + ")" +
                " JOIN " + Tables.PHONE_LOOKUP + " AS p2 ON (p1." + PhoneLookupColumns.MIN_MATCH +
                        "=p2." + PhoneLookupColumns.MIN_MATCH + ")" +
                " JOIN " + Tables.DATA + " AS d2 ON " +
                        "(d2." + Data._ID + "=p2." + PhoneLookupColumns.DATA_ID + ")" +
                " WHERE d1." + DataColumns.MIMETYPE_ID + " = " + phoneType +
                " AND d2." + DataColumns.MIMETYPE_ID + " = " + phoneType +
                " AND d1." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet1 + ")" +
                " AND d2." + Data.RAW_CONTACT_ID + " IN (" + rawContactIdSet2 + ")" +
                " AND PHONE_NUMBERS_EQUAL(d1." + Phone.NUMBER + ",d2." + Phone.NUMBER + "," +
                        String.valueOf(mDbHelper.getUseStrictPhoneNumberComparisonParameter()) +
                        ")";
        return (countOnly) ? RawContactMatchingSelectionStatement.SELECT_COUNT + sql :
                RawContactMatchingSelectionStatement.SELECT_ID + sql;
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

    private boolean isFirstColumnGreaterThanZero(SQLiteDatabase db, String query) {
        return DatabaseUtils.longForQuery(db, query, null) > 0;
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
     * Partition the given raw contact Ids to connected component based on aggregation exception,
     * identity matching, email matching or phone matching.
     */
    private Set<Set<Long>> findConnectedRawContacts(SQLiteDatabase db, Set<Long> rawContactIdSet) {
        // Connections between two raw contacts
       final Multimap<Long, Long> matchingRawIdPairs = HashMultimap.create();
        String rawContactIds = TextUtils.join(",", rawContactIdSet);
        findIdPairs(db, buildExceptionMatchingSql(rawContactIds, rawContactIds,
                AggregationExceptions.TYPE_KEEP_TOGETHER,  /* countOnly =*/false),
                matchingRawIdPairs);
        findIdPairs(db, buildIdentityMatchingSql(rawContactIds, rawContactIds,
                /* isIdentityMatching =*/ true, /* countOnly =*/false), matchingRawIdPairs);
        findIdPairs(db, buildEmailMatchingSql(rawContactIds, rawContactIds, /* countOnly =*/false),
                matchingRawIdPairs);
        findIdPairs(db, buildPhoneMatchingSql(rawContactIds, rawContactIds,  /* countOnly =*/false),
                matchingRawIdPairs);

        return ContactAggregatorHelper.findConnectedComponents(rawContactIdSet, matchingRawIdPairs);
    }

    /**
     * Given a query which will return two non-null IDs in the first two columns as results, this
     * method will put two entries into the given result map for each pair of different IDs, one
     * keyed by each ID.
     */
    private void findIdPairs(SQLiteDatabase db, String query, Multimap<Long, Long> results) {
        Cursor cursor = db.rawQuery(query, null);
        try {
            cursor.moveToPosition(-1);
            while (cursor.moveToNext()) {
                long idA = cursor.getLong(0);
                long idB = cursor.getLong(1);
                if (idA != idB) {
                    results.put(idA, idB);
                    results.put(idB, idA);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Creates a new Contact for a given set of the raw contacts of {@code rawContactIds} if the
     * given contactId is null. Otherwise, regroup them into contact with {@code contactId}.
     */
    private void createContactForRawContacts(SQLiteDatabase db, TransactionContext txContext,
            Set<Long> rawContactIds, Long contactId) {
        if (rawContactIds.isEmpty()) {
            // No raw contact id is provided.
            return;
        }

        // If contactId is not provided, generates a new one.
        if (contactId == null) {
            mSelectionArgs1[0]= String.valueOf(rawContactIds.iterator().next());
            computeAggregateData(db, mRawContactsQueryByRawContactId, mSelectionArgs1,
                    mContactInsert);
            contactId = mContactInsert.executeInsert();
        }
        for (Long rawContactId : rawContactIds) {
            // Regrouped contacts should automatically be unpinned.
            unpinRawContact(rawContactId);
            setContactIdAndMarkAggregated(rawContactId, contactId);
            setPresenceContactId(rawContactId, contactId);
        }
        updateAggregateData(txContext, contactId);
    }

    private static class RawContactIdQuery {
        public static final String TABLE = Tables.RAW_CONTACTS;
        public static final String[] COLUMNS = {RawContacts._ID, RawContactsColumns.ACCOUNT_ID };
        public static final String SELECTION = RawContacts.CONTACT_ID + "=?";
        public static final int RAW_CONTACT_ID = 0;
        public static final int ACCOUNT_ID = 1;
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
     * Updates the contact ID for the specified contact.
     */
    protected void setContactId(long rawContactId, long contactId) {
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

    private void unpinRawContact(long rawContactId) {
        mResetPinnedForRawContact.bindLong(1, rawContactId);
        mResetPinnedForRawContact.execute();
    }

    interface AggregateExceptionPrefetchQuery {
        String TABLE = Tables.AGGREGATION_EXCEPTIONS;

        String[] COLUMNS = {
            AggregationExceptions.RAW_CONTACT_ID1,
            AggregationExceptions.RAW_CONTACT_ID2,
        };

        int RAW_CONTACT_ID1 = 0;
        int RAW_CONTACT_ID2 = 1;
    }

    // A set of raw contact IDs for which there are aggregation exceptions
    private final HashSet<Long> mAggregationExceptionIds = new HashSet<>();
    private boolean mAggregationExceptionIdsValid;

    public void invalidateAggregationExceptionCache() {
        mAggregationExceptionIdsValid = false;
    }

    /**
     * Finds all raw contact IDs for which there are aggregation exceptions. The list of
     * ids is used as an optimization in aggregation: there is no point to run a query against
     * the agg_exceptions table if it is known that there are no records there for a given
     * raw contact ID.
     */
    private void prefetchAggregationExceptionIds(SQLiteDatabase db) {
        mAggregationExceptionIds.clear();
        final Cursor c = db.query(AggregateExceptionPrefetchQuery.TABLE,
                AggregateExceptionPrefetchQuery.COLUMNS,
                null, null, null, null, null);

        try {
            while (c.moveToNext()) {
                long rawContactId1 = c.getLong(AggregateExceptionPrefetchQuery.RAW_CONTACT_ID1);
                long rawContactId2 = c.getLong(AggregateExceptionPrefetchQuery.RAW_CONTACT_ID2);
                mAggregationExceptionIds.add(rawContactId1);
                mAggregationExceptionIds.add(rawContactId2);
            }
        } finally {
            c.close();
        }

        mAggregationExceptionIdsValid = true;
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
            "raw_contacts1." + RawContactsColumns.ACCOUNT_ID,
            "raw_contacts1." + RawContactsColumns.AGGREGATION_NEEDED,
            AggregationExceptions.RAW_CONTACT_ID2,
            "raw_contacts2." + RawContacts.CONTACT_ID,
            "raw_contacts2." + RawContactsColumns.ACCOUNT_ID,
            "raw_contacts2." + RawContactsColumns.AGGREGATION_NEEDED,
        };

        int TYPE = 0;
        int RAW_CONTACT_ID1 = 1;
        int CONTACT_ID1 = 2;
        int ACCOUNT_ID1 = 3;
        int AGGREGATION_NEEDED_1 = 4;
        int RAW_CONTACT_ID2 = 5;
        int CONTACT_ID2 = 6;
        int ACCOUNT_ID2 = 7;
        int AGGREGATION_NEEDED_2 = 8;
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


    private interface NameLookupQuery {
        String TABLE = Tables.NAME_LOOKUP;

        String SELECTION = NameLookupColumns.RAW_CONTACT_ID + "=?";
        String SELECTION_STRUCTURED_NAME_BASED =
                SELECTION + " AND " + STRUCTURED_NAME_BASED_LOOKUP_SQL;

        String[] COLUMNS = new String[] {
                NameLookupColumns.NORMALIZED_NAME,
                NameLookupColumns.NAME_TYPE
        };

        int NORMALIZED_NAME = 0;
        int NAME_TYPE = 1;
    }

    private void loadNameMatchCandidates(SQLiteDatabase db, long rawContactId,
            MatchCandidateList candidates, boolean structuredNameBased) {
        candidates.clear();
        mSelectionArgs1[0] = String.valueOf(rawContactId);
        Cursor c = db.query(NameLookupQuery.TABLE, NameLookupQuery.COLUMNS,
                structuredNameBased
                        ? NameLookupQuery.SELECTION_STRUCTURED_NAME_BASED
                        : NameLookupQuery.SELECTION,
                mSelectionArgs1, null, null, null);
        try {
            while (c.moveToNext()) {
                String normalizedName = c.getString(NameLookupQuery.NORMALIZED_NAME);
                int type = c.getInt(NameLookupQuery.NAME_TYPE);
                candidates.add(normalizedName, type);
            }
        } finally {
            c.close();
        }
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
            RawContacts._ID, RawContacts.CONTACT_ID, RawContactsColumns.ACCOUNT_ID
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
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
            RawContacts._ID,
            RawContacts.CONTACT_ID,
            RawContactsColumns.ACCOUNT_ID,
            "nameA." + NameLookupColumns.NORMALIZED_NAME,
            "nameA." + NameLookupColumns.NAME_TYPE,
            "nameB." + NameLookupColumns.NAME_TYPE,
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
        int NAME = 3;
        int NAME_TYPE_A = 4;
        int NAME_TYPE_B = 5;
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

    private interface NameLookupMatchQueryWithParameter {
        String TABLE = Tables.NAME_LOOKUP
                + " JOIN " + Tables.RAW_CONTACTS +
                " ON (" + NameLookupColumns.RAW_CONTACT_ID + " = "
                        + Tables.RAW_CONTACTS + "." + RawContacts._ID + ")";

        String[] COLUMNS = new String[] {
            RawContacts._ID,
            RawContacts.CONTACT_ID,
            RawContactsColumns.ACCOUNT_ID,
            NameLookupColumns.NORMALIZED_NAME,
            NameLookupColumns.NAME_TYPE,
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
        int NAME = 3;
        int NAME_TYPE = 4;
    }

    private final class NameLookupSelectionBuilder extends NameLookupBuilder {

        private final MatchCandidateList mNameLookupCandidates;

        private StringBuilder mSelection = new StringBuilder(
                NameLookupColumns.NORMALIZED_NAME + " IN(");


        public NameLookupSelectionBuilder(NameSplitter splitter, MatchCandidateList candidates) {
            super(splitter);
            this.mNameLookupCandidates = candidates;
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return mCommonNicknameCache.getCommonNicknameClusters(normalizedName);
        }

        @Override
        protected void insertNameLookup(
                long rawContactId, long dataId, int lookupType, String string) {
            mNameLookupCandidates.add(string, lookupType);
            DatabaseUtils.appendEscapedSQLString(mSelection, string);
            mSelection.append(',');
        }

        public boolean isEmpty() {
            return mNameLookupCandidates.isEmpty();
        }

        public String getSelection() {
            mSelection.setLength(mSelection.length() - 1);      // Strip last comma
            mSelection.append(')');
            return mSelection.toString();
        }

        public int getLookupType(String name) {
            for (int i = 0; i < mNameLookupCandidates.mCount; i++) {
                if (mNameLookupCandidates.mList.get(i).mName.equals(name)) {
                    return mNameLookupCandidates.mList.get(i).mLookupType;
                }
            }
            throw new IllegalStateException();
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

    private interface EmailLookupQuery {
        String TABLE = Tables.DATA + " dataA"
                + " JOIN " + Tables.DATA + " dataB" +
                " ON lower(" + "dataA." + Email.DATA + ")=lower(dataB." + Email.DATA + ")"
                + " JOIN " + Tables.RAW_CONTACTS +
                " ON (dataB." + Data.RAW_CONTACT_ID + " = "
                        + Tables.RAW_CONTACTS + "." + RawContacts._ID + ")";

        String SELECTION = "dataA." + Data.RAW_CONTACT_ID + "=?1"
                + " AND dataA." + DataColumns.MIMETYPE_ID + "=?2"
                + " AND dataA." + Email.DATA + " NOT NULL"
                + " AND dataB." + DataColumns.MIMETYPE_ID + "=?2"
                + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0"
                + " AND " + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY;

        String[] COLUMNS = new String[] {
            RawContacts._ID,
            RawContacts.CONTACT_ID,
            RawContactsColumns.ACCOUNT_ID
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
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

    private interface PhoneLookupQuery {
        String TABLE = Tables.PHONE_LOOKUP + " phoneA"
                + " JOIN " + Tables.DATA + " dataA"
                + " ON (dataA." + Data._ID + "=phoneA." + PhoneLookupColumns.DATA_ID + ")"
                + " JOIN " + Tables.PHONE_LOOKUP + " phoneB"
                + " ON (phoneA." + PhoneLookupColumns.MIN_MATCH + "="
                        + "phoneB." + PhoneLookupColumns.MIN_MATCH + ")"
                + " JOIN " + Tables.DATA + " dataB"
                + " ON (dataB." + Data._ID + "=phoneB." + PhoneLookupColumns.DATA_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS
                + " ON (dataB." + Data.RAW_CONTACT_ID + " = "
                        + Tables.RAW_CONTACTS + "." + RawContacts._ID + ")";

        String SELECTION = "dataA." + Data.RAW_CONTACT_ID + "=?"
                + " AND PHONE_NUMBERS_EQUAL(dataA." + Phone.NUMBER + ", "
                        + "dataB." + Phone.NUMBER + ",?)"
                + " AND " + RawContactsColumns.AGGREGATION_NEEDED + "=0"
                + " AND " + RawContacts.CONTACT_ID + " IN " + Tables.DEFAULT_DIRECTORY;

        String[] COLUMNS = new String[] {
            RawContacts._ID,
            RawContacts.CONTACT_ID,
            RawContactsColumns.ACCOUNT_ID
        };

        int RAW_CONTACT_ID = 0;
        int CONTACT_ID = 1;
        int ACCOUNT_ID = 2;
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

    private interface RawContactsQuery {
        String SQL_FORMAT_HAS_SUPER_PRIMARY_NAME =
                " EXISTS(SELECT 1 " +
                        " FROM " + Tables.DATA + " d " +
                        " WHERE d." + DataColumns.MIMETYPE_ID + "=%d " +
                        " AND d." + Data.RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID +
                        " AND d." + Data.IS_SUPER_PRIMARY + "=1)";

        String SQL_FORMAT =
                "SELECT "
                        + RawContactsColumns.CONCRETE_ID + ","
                        + RawContactsColumns.DISPLAY_NAME + ","
                        + RawContactsColumns.DISPLAY_NAME_SOURCE + ","
                        + AccountsColumns.CONCRETE_ACCOUNT_TYPE + ","
                        + AccountsColumns.CONCRETE_ACCOUNT_NAME + ","
                        + AccountsColumns.CONCRETE_DATA_SET + ","
                        + RawContacts.SOURCE_ID + ","
                        + RawContacts.CUSTOM_RINGTONE + ","
                        + RawContacts.SEND_TO_VOICEMAIL + ","
                        + RawContacts.LAST_TIME_CONTACTED + ","
                        + RawContacts.TIMES_CONTACTED + ","
                        + RawContacts.STARRED + ","
                        + RawContacts.PINNED + ","
                        + DataColumns.CONCRETE_ID + ","
                        + DataColumns.CONCRETE_MIMETYPE_ID + ","
                        + Data.IS_SUPER_PRIMARY + ","
                        + Photo.PHOTO_FILE_ID + ","
                        + SQL_FORMAT_HAS_SUPER_PRIMARY_NAME +
                " FROM " + Tables.RAW_CONTACTS +
                " JOIN " + Tables.ACCOUNTS + " ON ("
                    + AccountsColumns.CONCRETE_ID + "=" + RawContactsColumns.CONCRETE_ACCOUNT_ID
                    + ")" +
                " LEFT OUTER JOIN " + Tables.DATA +
                " ON (" + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                        + " AND ((" + DataColumns.MIMETYPE_ID + "=%d"
                                + " AND " + Photo.PHOTO + " NOT NULL)"
                        + " OR (" + DataColumns.MIMETYPE_ID + "=%d"
                                + " AND " + Phone.NUMBER + " NOT NULL)))";

        String SQL_FORMAT_BY_RAW_CONTACT_ID = SQL_FORMAT +
                " WHERE " + RawContactsColumns.CONCRETE_ID + "=?";

        String SQL_FORMAT_BY_CONTACT_ID = SQL_FORMAT +
                " WHERE " + RawContacts.CONTACT_ID + "=?"
                + " AND " + RawContacts.DELETED + "=0";

        int RAW_CONTACT_ID = 0;
        int DISPLAY_NAME = 1;
        int DISPLAY_NAME_SOURCE = 2;
        int ACCOUNT_TYPE = 3;
        int ACCOUNT_NAME = 4;
        int DATA_SET = 5;
        int SOURCE_ID = 6;
        int CUSTOM_RINGTONE = 7;
        int SEND_TO_VOICEMAIL = 8;
        int LAST_TIME_CONTACTED = 9;
        int TIMES_CONTACTED = 10;
        int STARRED = 11;
        int PINNED = 12;
        int DATA_ID = 13;
        int MIMETYPE_ID = 14;
        int IS_SUPER_PRIMARY = 15;
        int PHOTO_FILE_ID = 16;
        int HAS_SUPER_PRIMARY_NAME = 17;
    }

    private interface ContactReplaceSqlStatement {
        String UPDATE_SQL =
                "UPDATE " + Tables.CONTACTS +
                " SET "
                        + Contacts.NAME_RAW_CONTACT_ID + "=?, "
                        + Contacts.PHOTO_ID + "=?, "
                        + Contacts.PHOTO_FILE_ID + "=?, "
                        + Contacts.SEND_TO_VOICEMAIL + "=?, "
                        + Contacts.CUSTOM_RINGTONE + "=?, "
                        + Contacts.LAST_TIME_CONTACTED + "=?, "
                        + Contacts.TIMES_CONTACTED + "=?, "
                        + Contacts.STARRED + "=?, "
                        + Contacts.PINNED + "=?, "
                        + Contacts.HAS_PHONE_NUMBER + "=?, "
                        + Contacts.LOOKUP_KEY + "=?, "
                        + Contacts.CONTACT_LAST_UPDATED_TIMESTAMP + "=? " +
                " WHERE " + Contacts._ID + "=?";

        String INSERT_SQL =
                "INSERT INTO " + Tables.CONTACTS + " ("
                        + Contacts.NAME_RAW_CONTACT_ID + ", "
                        + Contacts.PHOTO_ID + ", "
                        + Contacts.PHOTO_FILE_ID + ", "
                        + Contacts.SEND_TO_VOICEMAIL + ", "
                        + Contacts.CUSTOM_RINGTONE + ", "
                        + Contacts.LAST_TIME_CONTACTED + ", "
                        + Contacts.TIMES_CONTACTED + ", "
                        + Contacts.STARRED + ", "
                        + Contacts.PINNED + ", "
                        + Contacts.HAS_PHONE_NUMBER + ", "
                        + Contacts.LOOKUP_KEY + ", "
                        + Contacts.CONTACT_LAST_UPDATED_TIMESTAMP
                        + ") " +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

        int NAME_RAW_CONTACT_ID = 1;
        int PHOTO_ID = 2;
        int PHOTO_FILE_ID = 3;
        int SEND_TO_VOICEMAIL = 4;
        int CUSTOM_RINGTONE = 5;
        int LAST_TIME_CONTACTED = 6;
        int TIMES_CONTACTED = 7;
        int STARRED = 8;
        int PINNED = 9;
        int HAS_PHONE_NUMBER = 10;
        int LOOKUP_KEY = 11;
        int CONTACT_LAST_UPDATED_TIMESTAMP = 12;
        int CONTACT_ID = 13;
    }

    /**
     * Computes aggregate-level data for the specified aggregate contact ID.
     */
    private void computeAggregateData(SQLiteDatabase db, long contactId,
            SQLiteStatement statement) {
        mSelectionArgs1[0] = String.valueOf(contactId);
        computeAggregateData(db, mRawContactsQueryByContactId, mSelectionArgs1, statement);
    }

    /**
     * Indicates whether the given photo entry and priority gives this photo a higher overall
     * priority than the current best photo entry and priority.
     */
    private boolean hasHigherPhotoPriority(PhotoEntry photoEntry, int priority,
            PhotoEntry bestPhotoEntry, int bestPriority) {
        int photoComparison = photoEntry.compareTo(bestPhotoEntry);
        return photoComparison < 0 || photoComparison == 0 && priority > bestPriority;
    }

    /**
     * Computes aggregate-level data from constituent raw contacts.
     */
    private void computeAggregateData(final SQLiteDatabase db, String sql, String[] sqlArgs,
            SQLiteStatement statement) {
        long currentRawContactId = -1;
        long bestPhotoId = -1;
        long bestPhotoFileId = 0;
        PhotoEntry bestPhotoEntry = null;
        boolean foundSuperPrimaryPhoto = false;
        int photoPriority = -1;
        int totalRowCount = 0;
        int contactSendToVoicemail = 0;
        String contactCustomRingtone = null;
        long contactLastTimeContacted = 0;
        int contactTimesContacted = 0;
        int contactStarred = 0;
        int contactPinned = Integer.MAX_VALUE;
        int hasPhoneNumber = 0;
        StringBuilder lookupKey = new StringBuilder();

        mDisplayNameCandidate.clear();

        Cursor c = db.rawQuery(sql, sqlArgs);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(RawContactsQuery.RAW_CONTACT_ID);
                if (rawContactId != currentRawContactId) {
                    currentRawContactId = rawContactId;
                    totalRowCount++;

                    // Assemble sub-account.
                    String accountType = c.getString(RawContactsQuery.ACCOUNT_TYPE);
                    String dataSet = c.getString(RawContactsQuery.DATA_SET);
                    String accountWithDataSet = (!TextUtils.isEmpty(dataSet))
                            ? accountType + "/" + dataSet
                            : accountType;

                    // Display name
                    String displayName = c.getString(RawContactsQuery.DISPLAY_NAME);
                    int displayNameSource = c.getInt(RawContactsQuery.DISPLAY_NAME_SOURCE);
                    int isNameSuperPrimary = c.getInt(RawContactsQuery.HAS_SUPER_PRIMARY_NAME);
                    processDisplayNameCandidate(rawContactId, displayName, displayNameSource,
                            mContactsProvider.isWritableAccountWithDataSet(accountWithDataSet),
                            isNameSuperPrimary != 0);

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

                    if (c.getInt(RawContactsQuery.STARRED) != 0) {
                        contactStarred = 1;
                    }

                    // contactPinned should be the lowest value of its constituent raw contacts,
                    // excluding negative integers
                    final int rawContactPinned = c.getInt(RawContactsQuery.PINNED);
                    if (rawContactPinned > PinnedPositions.UNPINNED) {
                        contactPinned = Math.min(contactPinned, rawContactPinned);
                    }

                    appendLookupKey(
                            lookupKey,
                            accountWithDataSet,
                            c.getString(RawContactsQuery.ACCOUNT_NAME),
                            rawContactId,
                            c.getString(RawContactsQuery.SOURCE_ID),
                            displayName);
                }

                if (!c.isNull(RawContactsQuery.DATA_ID)) {
                    long dataId = c.getLong(RawContactsQuery.DATA_ID);
                    long photoFileId = c.getLong(RawContactsQuery.PHOTO_FILE_ID);
                    int mimetypeId = c.getInt(RawContactsQuery.MIMETYPE_ID);
                    boolean superPrimary = c.getInt(RawContactsQuery.IS_SUPER_PRIMARY) != 0;
                    if (mimetypeId == mMimeTypeIdPhoto) {
                        if (!foundSuperPrimaryPhoto) {
                            // Lookup the metadata for the photo, if available.  Note that data set
                            // does not come into play here, since accounts are looked up in the
                            // account manager in the priority resolver.
                            PhotoEntry photoEntry = getPhotoMetadata(db, photoFileId);
                            String accountType = c.getString(RawContactsQuery.ACCOUNT_TYPE);
                            int priority = mPhotoPriorityResolver.getPhotoPriority(accountType);
                            if (superPrimary || hasHigherPhotoPriority(
                                    photoEntry, priority, bestPhotoEntry, photoPriority)) {
                                bestPhotoEntry = photoEntry;
                                photoPriority = priority;
                                bestPhotoId = dataId;
                                bestPhotoFileId = photoFileId;
                                foundSuperPrimaryPhoto |= superPrimary;
                            }
                        }
                    } else if (mimetypeId == mMimeTypeIdPhone) {
                        hasPhoneNumber = 1;
                    }
                }
            }
        } finally {
            c.close();
        }

        if (contactPinned == Integer.MAX_VALUE) {
            contactPinned = PinnedPositions.UNPINNED;
        }

        statement.bindLong(ContactReplaceSqlStatement.NAME_RAW_CONTACT_ID,
                mDisplayNameCandidate.rawContactId);

        if (bestPhotoId != -1) {
            statement.bindLong(ContactReplaceSqlStatement.PHOTO_ID, bestPhotoId);
        } else {
            statement.bindNull(ContactReplaceSqlStatement.PHOTO_ID);
        }

        if (bestPhotoFileId != 0) {
            statement.bindLong(ContactReplaceSqlStatement.PHOTO_FILE_ID, bestPhotoFileId);
        } else {
            statement.bindNull(ContactReplaceSqlStatement.PHOTO_FILE_ID);
        }

        statement.bindLong(ContactReplaceSqlStatement.SEND_TO_VOICEMAIL,
                totalRowCount == contactSendToVoicemail ? 1 : 0);
        DatabaseUtils.bindObjectToProgram(statement, ContactReplaceSqlStatement.CUSTOM_RINGTONE,
                contactCustomRingtone);
        statement.bindLong(ContactReplaceSqlStatement.LAST_TIME_CONTACTED,
                contactLastTimeContacted);
        statement.bindLong(ContactReplaceSqlStatement.TIMES_CONTACTED,
                contactTimesContacted);
        statement.bindLong(ContactReplaceSqlStatement.STARRED,
                contactStarred);
        statement.bindLong(ContactReplaceSqlStatement.PINNED,
                contactPinned);
        statement.bindLong(ContactReplaceSqlStatement.HAS_PHONE_NUMBER,
                hasPhoneNumber);
        statement.bindString(ContactReplaceSqlStatement.LOOKUP_KEY,
                Uri.encode(lookupKey.toString()));
        statement.bindLong(ContactReplaceSqlStatement.CONTACT_LAST_UPDATED_TIMESTAMP,
                Clock.getInstance().currentTimeMillis());
    }

    /**
     * Builds a lookup key using the given data.
     */
    protected void appendLookupKey(StringBuilder sb, String accountTypeWithDataSet,
            String accountName, long rawContactId, String sourceId, String displayName) {
        ContactLookupKey.appendToLookupKey(sb, accountTypeWithDataSet, accountName, rawContactId,
                sourceId, displayName);
    }

    /**
     * Uses the supplied values to determine if they represent a "better" display name
     * for the aggregate contact currently evaluated.  If so, it updates
     * {@link #mDisplayNameCandidate} with the new values.
     */
    private void processDisplayNameCandidate(long rawContactId, String displayName,
            int displayNameSource, boolean writableAccount, boolean isNameSuperPrimary) {

        boolean replace = false;
        if (mDisplayNameCandidate.rawContactId == -1) {
            // No previous values available
            replace = true;
        } else if (!TextUtils.isEmpty(displayName)) {
            if (isNameSuperPrimary) {
                // A super primary name is better than any other name
                replace = true;
            } else if (mDisplayNameCandidate.isNameSuperPrimary == isNameSuperPrimary) {
                if (mDisplayNameCandidate.displayNameSource < displayNameSource) {
                    // New values come from an superior source, e.g. structured name vs phone number
                    replace = true;
                } else if (mDisplayNameCandidate.displayNameSource == displayNameSource) {
                    if (!mDisplayNameCandidate.writableAccount && writableAccount) {
                        replace = true;
                    } else if (mDisplayNameCandidate.writableAccount == writableAccount) {
                        if (NameNormalizer.compareComplexity(displayName,
                                mDisplayNameCandidate.displayName) > 0) {
                            // New name is more complex than the previously found one
                            replace = true;
                        }
                    }
                }
            }
        }

        if (replace) {
            mDisplayNameCandidate.rawContactId = rawContactId;
            mDisplayNameCandidate.displayName = displayName;
            mDisplayNameCandidate.displayNameSource = displayNameSource;
            mDisplayNameCandidate.isNameSuperPrimary = isNameSuperPrimary;
            mDisplayNameCandidate.writableAccount = writableAccount;
        }
    }

    private interface PhotoIdQuery {
        final String[] COLUMNS = new String[] {
            AccountsColumns.CONCRETE_ACCOUNT_TYPE,
            DataColumns.CONCRETE_ID,
            Data.IS_SUPER_PRIMARY,
            Photo.PHOTO_FILE_ID,
        };

        int ACCOUNT_TYPE = 0;
        int DATA_ID = 1;
        int IS_SUPER_PRIMARY = 2;
        int PHOTO_FILE_ID = 3;
    }

    public void updatePhotoId(SQLiteDatabase db, long rawContactId) {

        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        long bestPhotoId = -1;
        long bestPhotoFileId = 0;
        int photoPriority = -1;

        long photoMimeType = mDbHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);

        String tables = Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                    + AccountsColumns.CONCRETE_ID + "=" + RawContactsColumns.CONCRETE_ACCOUNT_ID
                    + ")"
                + " JOIN " + Tables.DATA + " ON("
                + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                + " AND (" + DataColumns.MIMETYPE_ID + "=" + photoMimeType + " AND "
                        + Photo.PHOTO + " NOT NULL))";

        mSelectionArgs1[0] = String.valueOf(contactId);
        final Cursor c = db.query(tables, PhotoIdQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=?", mSelectionArgs1, null, null, null);
        try {
            PhotoEntry bestPhotoEntry = null;
            while (c.moveToNext()) {
                long dataId = c.getLong(PhotoIdQuery.DATA_ID);
                long photoFileId = c.getLong(PhotoIdQuery.PHOTO_FILE_ID);
                boolean superPrimary = c.getInt(PhotoIdQuery.IS_SUPER_PRIMARY) != 0;
                PhotoEntry photoEntry = getPhotoMetadata(db, photoFileId);

                // Note that data set does not come into play here, since accounts are looked up in
                // the account manager in the priority resolver.
                String accountType = c.getString(PhotoIdQuery.ACCOUNT_TYPE);
                int priority = mPhotoPriorityResolver.getPhotoPriority(accountType);
                if (superPrimary || hasHigherPhotoPriority(
                        photoEntry, priority, bestPhotoEntry, photoPriority)) {
                    bestPhotoEntry = photoEntry;
                    photoPriority = priority;
                    bestPhotoId = dataId;
                    bestPhotoFileId = photoFileId;
                    if (superPrimary) {
                        break;
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

        if (bestPhotoFileId == 0) {
            mPhotoIdUpdate.bindNull(2);
        } else {
            mPhotoIdUpdate.bindLong(2, bestPhotoFileId);
        }

        mPhotoIdUpdate.bindLong(3, contactId);
        mPhotoIdUpdate.execute();
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

    private interface DisplayNameQuery {
        String SQL_HAS_SUPER_PRIMARY_NAME =
                " EXISTS(SELECT 1 " +
                        " FROM " + Tables.DATA + " d " +
                        " WHERE d." + DataColumns.MIMETYPE_ID + "=? " +
                        " AND d." + Data.RAW_CONTACT_ID + "=" + Views.RAW_CONTACTS
                        + "." + RawContacts._ID +
                        " AND d." + Data.IS_SUPER_PRIMARY + "=1)";

        String SQL =
                "SELECT "
                        + RawContacts._ID + ","
                        + RawContactsColumns.DISPLAY_NAME + ","
                        + RawContactsColumns.DISPLAY_NAME_SOURCE + ","
                        + SQL_HAS_SUPER_PRIMARY_NAME + ","
                        + RawContacts.SOURCE_ID + ","
                        + RawContacts.ACCOUNT_TYPE_AND_DATA_SET +
                " FROM " + Views.RAW_CONTACTS +
                " WHERE " + RawContacts.CONTACT_ID + "=? ";

        int _ID = 0;
        int DISPLAY_NAME = 1;
        int DISPLAY_NAME_SOURCE = 2;
        int HAS_SUPER_PRIMARY_NAME = 3;
        int SOURCE_ID = 4;
        int ACCOUNT_TYPE_AND_DATA_SET = 5;
    }

    public void updateDisplayNameForRawContact(SQLiteDatabase db, long rawContactId) {
        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        updateDisplayNameForContact(db, contactId);
    }

    public void updateDisplayNameForContact(SQLiteDatabase db, long contactId) {
        boolean lookupKeyUpdateNeeded = false;

        mDisplayNameCandidate.clear();

        mSelectionArgs2[0] = String.valueOf(mDbHelper.getMimeTypeIdForStructuredName());
        mSelectionArgs2[1] = String.valueOf(contactId);
        final Cursor c = db.rawQuery(DisplayNameQuery.SQL, mSelectionArgs2);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(DisplayNameQuery._ID);
                String displayName = c.getString(DisplayNameQuery.DISPLAY_NAME);
                int displayNameSource = c.getInt(DisplayNameQuery.DISPLAY_NAME_SOURCE);
                int isNameSuperPrimary = c.getInt(DisplayNameQuery.HAS_SUPER_PRIMARY_NAME);
                String accountTypeAndDataSet = c.getString(
                        DisplayNameQuery.ACCOUNT_TYPE_AND_DATA_SET);
                processDisplayNameCandidate(rawContactId, displayName, displayNameSource,
                        mContactsProvider.isWritableAccountWithDataSet(accountTypeAndDataSet),
                        isNameSuperPrimary != 0);

                // If the raw contact has no source id, the lookup key is based on the display
                // name, so the lookup key needs to be updated.
                lookupKeyUpdateNeeded |= c.isNull(DisplayNameQuery.SOURCE_ID);
            }
        } finally {
            c.close();
        }

        if (mDisplayNameCandidate.rawContactId != -1) {
            mDisplayNameUpdate.bindLong(1, mDisplayNameCandidate.rawContactId);
            mDisplayNameUpdate.bindLong(2, contactId);
            mDisplayNameUpdate.execute();
        }

        if (lookupKeyUpdateNeeded) {
            updateLookupKeyForContact(db, contactId);
        }
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

        final SQLiteStatement hasPhoneNumberUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.HAS_PHONE_NUMBER + "="
                        + "(SELECT (CASE WHEN COUNT(*)=0 THEN 0 ELSE 1 END)"
                        + " FROM " + Tables.DATA_JOIN_RAW_CONTACTS
                        + " WHERE " + DataColumns.MIMETYPE_ID + "=?"
                                + " AND " + Phone.NUMBER + " NOT NULL"
                                + " AND " + RawContacts.CONTACT_ID + "=?)" +
                " WHERE " + Contacts._ID + "=?");
        try {
            hasPhoneNumberUpdate.bindLong(1, mDbHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE));
            hasPhoneNumberUpdate.bindLong(2, contactId);
            hasPhoneNumberUpdate.bindLong(3, contactId);
            hasPhoneNumberUpdate.execute();
        } finally {
            hasPhoneNumberUpdate.close();
        }
    }

    private interface LookupKeyQuery {
        String TABLE = Views.RAW_CONTACTS;
        String[] COLUMNS = new String[] {
            RawContacts._ID,
            RawContactsColumns.DISPLAY_NAME,
            RawContacts.ACCOUNT_TYPE_AND_DATA_SET,
            RawContacts.ACCOUNT_NAME,
            RawContacts.SOURCE_ID,
        };

        int ID = 0;
        int DISPLAY_NAME = 1;
        int ACCOUNT_TYPE_AND_DATA_SET = 2;
        int ACCOUNT_NAME = 3;
        int SOURCE_ID = 4;
    }

    public void updateLookupKeyForRawContact(SQLiteDatabase db, long rawContactId) {
        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        updateLookupKeyForContact(db, contactId);
    }

    private void updateLookupKeyForContact(SQLiteDatabase db, long contactId) {
        String lookupKey = computeLookupKeyForContact(db, contactId);

        if (lookupKey == null) {
            mLookupKeyUpdate.bindNull(1);
        } else {
            mLookupKeyUpdate.bindString(1, Uri.encode(lookupKey));
        }
        mLookupKeyUpdate.bindLong(2, contactId);

        mLookupKeyUpdate.execute();
    }

    protected String computeLookupKeyForContact(SQLiteDatabase db, long contactId) {
        StringBuilder sb = new StringBuilder();
        mSelectionArgs1[0] = String.valueOf(contactId);
        final Cursor c = db.query(LookupKeyQuery.TABLE, LookupKeyQuery.COLUMNS,
                RawContacts.CONTACT_ID + "=?", mSelectionArgs1, null, null, RawContacts._ID);
        try {
            while (c.moveToNext()) {
                ContactLookupKey.appendToLookupKey(sb,
                        c.getString(LookupKeyQuery.ACCOUNT_TYPE_AND_DATA_SET),
                        c.getString(LookupKeyQuery.ACCOUNT_NAME),
                        c.getLong(LookupKeyQuery.ID),
                        c.getString(LookupKeyQuery.SOURCE_ID),
                        c.getString(LookupKeyQuery.DISPLAY_NAME));
            }
        } finally {
            c.close();
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /**
     * Execute {@link SQLiteStatement} that will update the
     * {@link Contacts#STARRED} flag for the given {@link RawContacts#_ID}.
     */
    public void updateStarred(long rawContactId) {
        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }

        mStarredUpdate.bindLong(1, contactId);
        mStarredUpdate.execute();
    }

    /**
     * Execute {@link SQLiteStatement} that will update the
     * {@link Contacts#PINNED} flag for the given {@link RawContacts#_ID}.
     */
    public void updatePinned(long rawContactId) {
        long contactId = mDbHelper.getContactId(rawContactId);
        if (contactId == 0) {
            return;
        }
        mPinnedUpdate.bindLong(1, contactId);
        mPinnedUpdate.execute();
    }

    /**
     * Finds matching contacts and returns a cursor on those.
     */
    public Cursor queryAggregationSuggestions(SQLiteQueryBuilder qb,
            String[] projection, long contactId, int maxSuggestions, String filter,
            ArrayList<AggregationSuggestionParameter> parameters) {
        final SQLiteDatabase db = mDbHelper.getReadableDatabase();
        db.beginTransaction();
        try {
            List<MatchScore> bestMatches = findMatchingContacts(db, contactId, parameters);
            return queryMatchingContacts(qb, db, projection, bestMatches, maxSuggestions, filter);
        } finally {
            db.endTransaction();
        }
    }

    private interface ContactIdQuery {
        String[] COLUMNS = new String[] {
            Contacts._ID
        };

        int _ID = 0;
    }

    /**
     * Loads contacts with specified IDs and returns them in the order of IDs in the
     * supplied list.
     */
    private Cursor queryMatchingContacts(SQLiteQueryBuilder qb, SQLiteDatabase db,
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
        HashSet<Long> foundIds = new HashSet<>();
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
        final List<MatchScore> limitedMatches;
        if (bestMatches.size() > maxSuggestions) {
            limitedMatches = bestMatches.subList(0, maxSuggestions);
        } else {
            limitedMatches = bestMatches;
        }

        // Build an in-clause with the remaining contact IDs
        sb.setLength(0);
        sb.append(Contacts._ID);
        sb.append(" IN (");
        for (int i = 0; i < limitedMatches.size(); i++) {
            MatchScore matchScore = limitedMatches.get(i);
            if (i != 0) {
                sb.append(",");
            }
            sb.append(matchScore.getContactId());
        }
        sb.append(")");

        // Run the final query with the required projection and contact IDs found by the first query
        cursor = qb.query(db, projection, sb.toString(), null, null, null, Contacts._ID);

        // Build a sorted list of discovered IDs
        ArrayList<Long> sortedContactIds = new ArrayList<Long>(limitedMatches.size());
        for (MatchScore matchScore : limitedMatches) {
            sortedContactIds.add(matchScore.getContactId());
        }

        Collections.sort(sortedContactIds);

        // Map cursor indexes according to the descending order of match scores
        int[] positionMap = new int[limitedMatches.size()];
        for (int i = 0; i < positionMap.length; i++) {
            long id = limitedMatches.get(i).getContactId();
            positionMap[i] = sortedContactIds.indexOf(id);
        }

        return new ReorderingCursorWrapper(cursor, positionMap);
    }

    /**
     * Finds contacts with data matches and returns a list of {@link MatchScore}'s in the
     * descending order of match score.
     * @param parameters
     */
    private List<MatchScore> findMatchingContacts(final SQLiteDatabase db, long contactId,
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
