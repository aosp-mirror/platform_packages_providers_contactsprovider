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

import static com.android.providers.contacts.util.DbQueryUtils.checkForSupportedColumns;
import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;
import static com.android.providers.contacts.util.DbQueryUtils.getInequalityClause;

import android.app.AppOpsManager;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Binder;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Message;
import android.os.Process;
import android.os.UserHandle;
import android.os.UserManager;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.telecom.PhoneAccount;
import android.telecom.PhoneAccountHandle;
import android.telecom.TelecomManager;
import android.text.TextUtils;
import android.util.Log;
import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.CallLogDatabaseHelper.DbProperties;
import com.android.providers.contacts.CallLogDatabaseHelper.Tables;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.UserUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Call log content provider.
 */
public class CallLogProvider extends ContentProvider {
    private static final String TAG = CallLogProvider.class.getSimpleName();

    public static final boolean VERBOSE_LOGGING = false; // DO NOT SUBMIT WITH TRUE

    private static final int BACKGROUND_TASK_INITIALIZE = 0;
    private static final int BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT = 1;

    /** Selection clause for selecting all calls that were made after a certain time */
    private static final String MORE_RECENT_THAN_SELECTION = Calls.DATE + "> ?";
    /** Selection clause to use to exclude voicemail records.  */
    private static final String EXCLUDE_VOICEMAIL_SELECTION = getInequalityClause(
            Calls.TYPE, Calls.VOICEMAIL_TYPE);
    /** Selection clause to exclude hidden records. */
    private static final String EXCLUDE_HIDDEN_SELECTION = getEqualityClause(
            Calls.PHONE_ACCOUNT_HIDDEN, 0);

    @VisibleForTesting
    static final String[] CALL_LOG_SYNC_PROJECTION = new String[] {
        Calls.NUMBER,
        Calls.NUMBER_PRESENTATION,
        Calls.TYPE,
        Calls.FEATURES,
        Calls.DATE,
        Calls.DURATION,
        Calls.DATA_USAGE,
        Calls.PHONE_ACCOUNT_COMPONENT_NAME,
        Calls.PHONE_ACCOUNT_ID,
        Calls.ADD_FOR_ALL_USERS
    };

    static final String[] MINIMAL_PROJECTION = new String[] { Calls._ID };

    private static final int CALLS = 1;

    private static final int CALLS_ID = 2;

    private static final int CALLS_FILTER = 3;

    private static final String UNHIDE_BY_PHONE_ACCOUNT_QUERY =
            "UPDATE " + Tables.CALLS + " SET " + Calls.PHONE_ACCOUNT_HIDDEN + "=0 WHERE " +
            Calls.PHONE_ACCOUNT_COMPONENT_NAME + "=? AND " + Calls.PHONE_ACCOUNT_ID + "=?;";

    private static final String UNHIDE_BY_ADDRESS_QUERY =
            "UPDATE " + Tables.CALLS + " SET " + Calls.PHONE_ACCOUNT_HIDDEN + "=0 WHERE " +
            Calls.PHONE_ACCOUNT_ADDRESS + "=?;";

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);
    static {
        sURIMatcher.addURI(CallLog.AUTHORITY, "calls", CALLS);
        sURIMatcher.addURI(CallLog.AUTHORITY, "calls/#", CALLS_ID);
        sURIMatcher.addURI(CallLog.AUTHORITY, "calls/filter/*", CALLS_FILTER);

        // Shadow provider only supports "/calls".
        sURIMatcher.addURI(CallLog.SHADOW_AUTHORITY, "calls", CALLS);
    }

    private static final HashMap<String, String> sCallsProjectionMap;
    static {

        // Calls projection map
        sCallsProjectionMap = new HashMap<String, String>();
        sCallsProjectionMap.put(Calls._ID, Calls._ID);
        sCallsProjectionMap.put(Calls.NUMBER, Calls.NUMBER);
        sCallsProjectionMap.put(Calls.POST_DIAL_DIGITS, Calls.POST_DIAL_DIGITS);
        sCallsProjectionMap.put(Calls.VIA_NUMBER, Calls.VIA_NUMBER);
        sCallsProjectionMap.put(Calls.NUMBER_PRESENTATION, Calls.NUMBER_PRESENTATION);
        sCallsProjectionMap.put(Calls.DATE, Calls.DATE);
        sCallsProjectionMap.put(Calls.DURATION, Calls.DURATION);
        sCallsProjectionMap.put(Calls.DATA_USAGE, Calls.DATA_USAGE);
        sCallsProjectionMap.put(Calls.TYPE, Calls.TYPE);
        sCallsProjectionMap.put(Calls.FEATURES, Calls.FEATURES);
        sCallsProjectionMap.put(Calls.PHONE_ACCOUNT_COMPONENT_NAME, Calls.PHONE_ACCOUNT_COMPONENT_NAME);
        sCallsProjectionMap.put(Calls.PHONE_ACCOUNT_ID, Calls.PHONE_ACCOUNT_ID);
        sCallsProjectionMap.put(Calls.PHONE_ACCOUNT_ADDRESS, Calls.PHONE_ACCOUNT_ADDRESS);
        sCallsProjectionMap.put(Calls.NEW, Calls.NEW);
        sCallsProjectionMap.put(Calls.VOICEMAIL_URI, Calls.VOICEMAIL_URI);
        sCallsProjectionMap.put(Calls.TRANSCRIPTION, Calls.TRANSCRIPTION);
        sCallsProjectionMap.put(Calls.IS_READ, Calls.IS_READ);
        sCallsProjectionMap.put(Calls.CACHED_NAME, Calls.CACHED_NAME);
        sCallsProjectionMap.put(Calls.CACHED_NUMBER_TYPE, Calls.CACHED_NUMBER_TYPE);
        sCallsProjectionMap.put(Calls.CACHED_NUMBER_LABEL, Calls.CACHED_NUMBER_LABEL);
        sCallsProjectionMap.put(Calls.COUNTRY_ISO, Calls.COUNTRY_ISO);
        sCallsProjectionMap.put(Calls.GEOCODED_LOCATION, Calls.GEOCODED_LOCATION);
        sCallsProjectionMap.put(Calls.CACHED_LOOKUP_URI, Calls.CACHED_LOOKUP_URI);
        sCallsProjectionMap.put(Calls.CACHED_MATCHED_NUMBER, Calls.CACHED_MATCHED_NUMBER);
        sCallsProjectionMap.put(Calls.CACHED_NORMALIZED_NUMBER, Calls.CACHED_NORMALIZED_NUMBER);
        sCallsProjectionMap.put(Calls.CACHED_PHOTO_ID, Calls.CACHED_PHOTO_ID);
        sCallsProjectionMap.put(Calls.CACHED_PHOTO_URI, Calls.CACHED_PHOTO_URI);
        sCallsProjectionMap.put(Calls.CACHED_FORMATTED_NUMBER, Calls.CACHED_FORMATTED_NUMBER);
        sCallsProjectionMap.put(Calls.ADD_FOR_ALL_USERS, Calls.ADD_FOR_ALL_USERS);
        sCallsProjectionMap.put(Calls.LAST_MODIFIED, Calls.LAST_MODIFIED);
    }

    private static final String ALLOWED_PACKAGE_FOR_TESTING = "com.android.providers.contacts";

    @VisibleForTesting
    static final String PARAM_KEY_QUERY_FOR_TESTING = "query_for_testing";

    /**
     * A long to override the clock used for timestamps, or "null" to reset to the system clock.
     */
    @VisibleForTesting
    static final String PARAM_KEY_SET_TIME_FOR_TESTING = "set_time_for_testing";

    private static Long sTimeForTestMillis;

    private HandlerThread mBackgroundThread;
    private Handler mBackgroundHandler;
    private volatile CountDownLatch mReadAccessLatch;

    private CallLogDatabaseHelper mDbHelper;
    private DatabaseUtils.InsertHelper mCallsInserter;
    private boolean mUseStrictPhoneNumberComparation;
    private VoicemailPermissions mVoicemailPermissions;
    private CallLogInsertionHelper mCallLogInsertionHelper;

    protected boolean isShadow() {
        return false;
    }

    protected final String getProviderName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean onCreate() {
        setAppOps(AppOpsManager.OP_READ_CALL_LOG, AppOpsManager.OP_WRITE_CALL_LOG);
        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.DEBUG)) {
            Log.d(Constants.PERFORMANCE_TAG, getProviderName() + ".onCreate start");
        }
        final Context context = getContext();
        mDbHelper = getDatabaseHelper(context);
        mUseStrictPhoneNumberComparation =
            context.getResources().getBoolean(
                    com.android.internal.R.bool.config_use_strict_phone_number_comparation);
        mVoicemailPermissions = new VoicemailPermissions(context);
        mCallLogInsertionHelper = createCallLogInsertionHelper(context);

        mBackgroundThread = new HandlerThread(getProviderName() + "Worker",
                Process.THREAD_PRIORITY_BACKGROUND);
        mBackgroundThread.start();
        mBackgroundHandler = new Handler(mBackgroundThread.getLooper()) {
            @Override
            public void handleMessage(Message msg) {
                performBackgroundTask(msg.what, msg.obj);
            }
        };

        mReadAccessLatch = new CountDownLatch(1);

        scheduleBackgroundTask(BACKGROUND_TASK_INITIALIZE, null);

        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.DEBUG)) {
            Log.d(Constants.PERFORMANCE_TAG, getProviderName() + ".onCreate finish");
        }
        return true;
    }

    @VisibleForTesting
    protected CallLogInsertionHelper createCallLogInsertionHelper(final Context context) {
        return DefaultCallLogInsertionHelper.getInstance(context);
    }

    protected CallLogDatabaseHelper getDatabaseHelper(final Context context) {
        return CallLogDatabaseHelper.getInstance(context);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: uri=" + uri + "  projection=" + Arrays.toString(projection) +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  order=[" + sortOrder + "] CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }

        queryForTesting(uri);

        waitForAccess(mReadAccessLatch);
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Tables.CALLS);
        qb.setProjectionMap(sCallsProjectionMap);
        qb.setStrict(true);

        final SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        checkVoicemailPermissionAndAddRestriction(uri, selectionBuilder, true /*isQuery*/);
        selectionBuilder.addClause(EXCLUDE_HIDDEN_SELECTION);

        final int match = sURIMatcher.match(uri);
        switch (match) {
            case CALLS:
                break;

            case CALLS_ID: {
                selectionBuilder.addClause(getEqualityClause(Calls._ID,
                        parseCallIdFromUri(uri)));
                break;
            }

            case CALLS_FILTER: {
                List<String> pathSegments = uri.getPathSegments();
                String phoneNumber = pathSegments.size() >= 2 ? pathSegments.get(2) : null;
                if (!TextUtils.isEmpty(phoneNumber)) {
                    qb.appendWhere("PHONE_NUMBERS_EQUAL(number, ");
                    qb.appendWhereEscapeString(phoneNumber);
                    qb.appendWhere(mUseStrictPhoneNumberComparation ? ", 1)" : ", 0)");
                } else {
                    qb.appendWhere(Calls.NUMBER_PRESENTATION + "!="
                            + Calls.PRESENTATION_ALLOWED);
                }
                break;
            }

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        final int limit = getIntParam(uri, Calls.LIMIT_PARAM_KEY, 0);
        final int offset = getIntParam(uri, Calls.OFFSET_PARAM_KEY, 0);
        String limitClause = null;
        if (limit > 0) {
            limitClause = offset + "," + limit;
        }

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();
        final Cursor c = qb.query(db, projection, selectionBuilder.build(), selectionArgs, null,
                null, sortOrder, limitClause);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), CallLog.CONTENT_URI);
        }
        return c;
    }

    private void queryForTesting(Uri uri) {
        if (!uri.getBooleanQueryParameter(PARAM_KEY_QUERY_FOR_TESTING, false)) {
            return;
        }
        if (!getCallingPackage().equals(ALLOWED_PACKAGE_FOR_TESTING)) {
            throw new IllegalArgumentException("query_for_testing set from foreign package "
                    + getCallingPackage());
        }

        String timeString = uri.getQueryParameter(PARAM_KEY_SET_TIME_FOR_TESTING);
        if (timeString != null) {
            if (timeString.equals("null")) {
                sTimeForTestMillis = null;
            } else {
                sTimeForTestMillis = Long.parseLong(timeString);
            }
        }
    }

    @VisibleForTesting
    static Long getTimeForTestMillis() {
        return sTimeForTestMillis;
    }

    /**
     * Gets an integer query parameter from a given uri.
     *
     * @param uri The uri to extract the query parameter from.
     * @param key The query parameter key.
     * @param defaultValue A default value to return if the query parameter does not exist.
     * @return The value from the query parameter in the Uri.  Or the default value if the parameter
     * does not exist in the uri.
     * @throws IllegalArgumentException when the value in the query parameter is not an integer.
     */
    private int getIntParam(Uri uri, String key, int defaultValue) {
        String valueString = uri.getQueryParameter(key);
        if (valueString == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(valueString);
        } catch (NumberFormatException e) {
            String msg = "Integer required for " + key + " parameter but value '" + valueString +
                    "' was found instead.";
            throw new IllegalArgumentException(msg, e);
        }
    }

    @Override
    public String getType(Uri uri) {
        int match = sURIMatcher.match(uri);
        switch (match) {
            case CALLS:
                return Calls.CONTENT_TYPE;
            case CALLS_ID:
                return Calls.CONTENT_ITEM_TYPE;
            case CALLS_FILTER:
                return Calls.CONTENT_TYPE;
            default:
                throw new IllegalArgumentException("Unknown URI: " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insert: uri=" + uri + "  values=[" + values + "]" +
                    " CPID=" + Binder.getCallingPid());
        }
        waitForAccess(mReadAccessLatch);
        checkForSupportedColumns(sCallsProjectionMap, values);
        // Inserting a voicemail record through call_log requires the voicemail
        // permission and also requires the additional voicemail param set.
        if (hasVoicemailValue(values)) {
            checkIsAllowVoicemailRequest(uri);
            mVoicemailPermissions.checkCallerHasWriteAccess(getCallingPackage());
        }
        if (mCallsInserter == null) {
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            mCallsInserter = new DatabaseUtils.InsertHelper(db, Tables.CALLS);
        }

        ContentValues copiedValues = new ContentValues(values);

        // Add the computed fields to the copied values.
        mCallLogInsertionHelper.addComputedValues(copiedValues);

        long rowId = getDatabaseModifier(mCallsInserter).insert(copiedValues);
        if (rowId > 0) {
            return ContentUris.withAppendedId(uri, rowId);
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "update: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  values=[" + values + "] CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        waitForAccess(mReadAccessLatch);
        checkForSupportedColumns(sCallsProjectionMap, values);
        // Request that involves changing record type to voicemail requires the
        // voicemail param set in the uri.
        if (hasVoicemailValue(values)) {
            checkIsAllowVoicemailRequest(uri);
        }

        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        checkVoicemailPermissionAndAddRestriction(uri, selectionBuilder, false /*isQuery*/);

        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        final int matchedUriId = sURIMatcher.match(uri);
        switch (matchedUriId) {
            case CALLS:
                break;

            case CALLS_ID:
                selectionBuilder.addClause(getEqualityClause(Calls._ID, parseCallIdFromUri(uri)));
                break;

            default:
                throw new UnsupportedOperationException("Cannot update URL: " + uri);
        }

        return getDatabaseModifier(db).update(uri, Tables.CALLS, values, selectionBuilder.build(),
                selectionArgs);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "delete: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    " CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        waitForAccess(mReadAccessLatch);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        checkVoicemailPermissionAndAddRestriction(uri, selectionBuilder, false /*isQuery*/);

        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        final int matchedUriId = sURIMatcher.match(uri);
        switch (matchedUriId) {
            case CALLS:
                // TODO: Special case - We may want to forward the delete request on user 0 to the
                // shadow provider too.
                return getDatabaseModifier(db).delete(Tables.CALLS,
                        selectionBuilder.build(), selectionArgs);
            default:
                throw new UnsupportedOperationException("Cannot delete that URL: " + uri);
        }
    }

    void adjustForNewPhoneAccount(PhoneAccountHandle handle) {
        scheduleBackgroundTask(BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT, handle);
    }

    /**
     * Returns a {@link DatabaseModifier} that takes care of sending necessary notifications
     * after the operation is performed.
     */
    private DatabaseModifier getDatabaseModifier(SQLiteDatabase db) {
        return new DbModifierWithNotification(Tables.CALLS, db, getContext());
    }

    /**
     * Same as {@link #getDatabaseModifier(SQLiteDatabase)} but used for insert helper operations
     * only.
     */
    private DatabaseModifier getDatabaseModifier(DatabaseUtils.InsertHelper insertHelper) {
        return new DbModifierWithNotification(Tables.CALLS, insertHelper, getContext());
    }

    private static final Integer VOICEMAIL_TYPE = new Integer(Calls.VOICEMAIL_TYPE);
    private boolean hasVoicemailValue(ContentValues values) {
        return VOICEMAIL_TYPE.equals(values.getAsInteger(Calls.TYPE));
    }

    /**
     * Checks if the supplied uri requests to include voicemails and take appropriate
     * action.
     * <p> If voicemail is requested, then check for voicemail permissions. Otherwise
     * modify the selection to restrict to non-voicemail entries only.
     */
    private void checkVoicemailPermissionAndAddRestriction(Uri uri,
            SelectionBuilder selectionBuilder, boolean isQuery) {
        if (isAllowVoicemailRequest(uri)) {
            if (isQuery) {
                mVoicemailPermissions.checkCallerHasReadAccess(getCallingPackage());
            } else {
                mVoicemailPermissions.checkCallerHasWriteAccess(getCallingPackage());
            }
        } else {
            selectionBuilder.addClause(EXCLUDE_VOICEMAIL_SELECTION);
        }
    }

    /**
     * Determines if the supplied uri has the request to allow voicemails to be
     * included.
     */
    private boolean isAllowVoicemailRequest(Uri uri) {
        return uri.getBooleanQueryParameter(Calls.ALLOW_VOICEMAILS_PARAM_KEY, false);
    }

    /**
     * Checks to ensure that the given uri has allow_voicemail set. Used by
     * insert and update operations to check that ContentValues with voicemail
     * call type must use the voicemail uri.
     * @throws IllegalArgumentException if allow_voicemail is not set.
     */
    private void checkIsAllowVoicemailRequest(Uri uri) {
        if (!isAllowVoicemailRequest(uri)) {
            throw new IllegalArgumentException(
                    String.format("Uri %s cannot be used for voicemail record." +
                            " Please set '%s=true' in the uri.", uri,
                            Calls.ALLOW_VOICEMAILS_PARAM_KEY));
        }
    }

   /**
    * Parses the call Id from the given uri, assuming that this is a uri that
    * matches CALLS_ID. For other uri types the behaviour is undefined.
    * @throws IllegalArgumentException if the id included in the Uri is not a valid long value.
    */
    private long parseCallIdFromUri(Uri uri) {
        try {
            return Long.parseLong(uri.getPathSegments().get(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid call id in uri: " + uri, e);
        }
    }

    /**
     * Sync all calllog entries that were inserted
     */
    private void syncEntries() {
        if (isShadow()) {
            return; // It's the shadow provider itself.  No copying.
        }

        final UserManager userManager = UserUtils.getUserManager(getContext());

        // TODO: http://b/24944959
        if (!Calls.shouldHaveSharedCallLogEntries(getContext(), userManager,
                userManager.getUserHandle())) {
            return;
        }

        final int myUserId = userManager.getUserHandle();

        // See the comment in Calls.addCall() for the logic.

        if (userManager.isSystemUser()) {
            // If it's the system user, just copy from shadow.
            syncEntriesFrom(UserHandle.USER_SYSTEM, /* sourceIsShadow = */ true,
                    /* forAllUsersOnly =*/ false);
        } else {
            // Otherwise, copy from system's real provider, as well as self's shadow.
            syncEntriesFrom(UserHandle.USER_SYSTEM, /* sourceIsShadow = */ false,
                    /* forAllUsersOnly =*/ true);
            syncEntriesFrom(myUserId, /* sourceIsShadow = */ true,
                    /* forAllUsersOnly =*/ false);
        }
    }

    private void syncEntriesFrom(int sourceUserId, boolean sourceIsShadow,
            boolean forAllUsersOnly) {

        final Uri sourceUri = sourceIsShadow ? Calls.SHADOW_CONTENT_URI : Calls.CONTENT_URI;

        final long lastSyncTime = getLastSyncTime(sourceIsShadow);

        final Uri uri = ContentProvider.maybeAddUserId(sourceUri, sourceUserId);
        final long newestTimeStamp;
        final ContentResolver cr = getContext().getContentResolver();

        final StringBuilder selection = new StringBuilder();

        selection.append(
                "(" + EXCLUDE_VOICEMAIL_SELECTION + ") AND (" + MORE_RECENT_THAN_SELECTION + ")");

        if (forAllUsersOnly) {
            selection.append(" AND (" + Calls.ADD_FOR_ALL_USERS + "=1)");
        }

        final Cursor cursor = cr.query(
                uri,
                CALL_LOG_SYNC_PROJECTION,
                selection.toString(),
                new String[] {String.valueOf(lastSyncTime)},
                Calls.DATE + " ASC");
        if (cursor == null) {
            return;
        }
        try {
            newestTimeStamp = copyEntriesFromCursor(cursor, lastSyncTime, sourceIsShadow);
        } finally {
            cursor.close();
        }
        if (sourceIsShadow) {
            // delete all entries in shadow.
            cr.delete(uri, Calls.DATE + "<= ?", new String[] {String.valueOf(newestTimeStamp)});
        }
    }

    /**
     * Un-hides any hidden call log entries that are associated with the specified handle.
     *
     * @param handle The handle to the newly registered {@link android.telecom.PhoneAccount}.
     */
    private void adjustForNewPhoneAccountInternal(PhoneAccountHandle handle) {
        String[] handleArgs =
                new String[] { handle.getComponentName().flattenToString(), handle.getId() };

        // Check to see if any entries exist for this handle. If so (not empty), run the un-hiding
        // update. If not, then try to identify the call from the phone number.
        Cursor cursor = query(Calls.CONTENT_URI, MINIMAL_PROJECTION,
                Calls.PHONE_ACCOUNT_COMPONENT_NAME + " =? AND " + Calls.PHONE_ACCOUNT_ID + " =?",
                handleArgs, null);

        if (cursor != null) {
            try {
                if (cursor.getCount() >= 1) {
                    // run un-hiding process based on phone account
                    mDbHelper.getWritableDatabase().execSQL(
                            UNHIDE_BY_PHONE_ACCOUNT_QUERY, handleArgs);
                } else {
                    TelecomManager tm = TelecomManager.from(getContext());
                    if (tm != null) {

                        PhoneAccount account = tm.getPhoneAccount(handle);
                        if (account != null && account.getAddress() != null) {
                            // We did not find any items for the specific phone account, so run the
                            // query based on the phone number instead.
                            mDbHelper.getWritableDatabase().execSQL(UNHIDE_BY_ADDRESS_QUERY,
                                    new String[] { account.getAddress().toString() });
                        }

                    }
                }
            } finally {
                cursor.close();
            }
        }

    }

    /**
     * @param cursor to copy call log entries from
     */
    @VisibleForTesting
    long copyEntriesFromCursor(Cursor cursor, long lastSyncTime, boolean forShadow) {
        long latestTimestamp = 0;
        final ContentValues values = new ContentValues();
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            final String[] args = new String[2];
            cursor.moveToPosition(-1);
            while (cursor.moveToNext()) {
                values.clear();
                DatabaseUtils.cursorRowToContentValues(cursor, values);

                final String startTime = values.getAsString(Calls.DATE);
                final String number = values.getAsString(Calls.NUMBER);

                if (startTime == null || number == null) {
                    continue;
                }

                if (cursor.isLast()) {
                    try {
                        latestTimestamp = Long.valueOf(startTime);
                    } catch (NumberFormatException e) {
                        Log.e(TAG, "Call log entry does not contain valid start time: "
                                + startTime);
                    }
                }

                // Avoid duplicating an already existing entry (which is uniquely identified by
                // the number, and the start time)
                args[0] = startTime;
                args[1] = number;
                if (DatabaseUtils.queryNumEntries(db, Tables.CALLS,
                        Calls.DATE + " = ? AND " + Calls.NUMBER + " = ?", args) > 0) {
                    continue;
                }

                db.insert(Tables.CALLS, null, values);
            }

            if (latestTimestamp > lastSyncTime) {
                setLastTimeSynced(latestTimestamp, forShadow);
            }

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return latestTimestamp;
    }

    private static String getLastSyncTimePropertyName(boolean forShadow) {
        return forShadow
                ? DbProperties.CALL_LOG_LAST_SYNCED_FOR_SHADOW
                : DbProperties.CALL_LOG_LAST_SYNCED;
    }

    @VisibleForTesting
    long getLastSyncTime(boolean forShadow) {
        try {
            return Long.valueOf(mDbHelper.getProperty(getLastSyncTimePropertyName(forShadow), "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private void setLastTimeSynced(long time, boolean forShadow) {
        mDbHelper.setProperty(getLastSyncTimePropertyName(forShadow), String.valueOf(time));
    }

    private static void waitForAccess(CountDownLatch latch) {
        if (latch == null) {
            return;
        }

        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void scheduleBackgroundTask(int task, Object arg) {
        mBackgroundHandler.obtainMessage(task, arg).sendToTarget();
    }

    private void performBackgroundTask(int task, Object arg) {
        if (task == BACKGROUND_TASK_INITIALIZE) {
            try {
                syncEntries();
            } finally {
                mReadAccessLatch.countDown();
                mReadAccessLatch = null;
            }
        } else if (task == BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT) {
            adjustForNewPhoneAccountInternal((PhoneAccountHandle) arg);
        }
    }
}
