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
import static com.android.providers.contacts.util.PhoneAccountHandleMigrationUtils.TELEPHONY_COMPONENT_NAME;

import android.annotation.NonNull;
import android.annotation.Nullable;
import android.app.AppOpsManager;
import android.content.BroadcastReceiver;
import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.OperationApplicationException;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteTokenizer;
import android.net.Uri;
import android.os.Binder;
import android.os.Bundle;
import android.os.ParcelFileDescriptor;
import android.os.ParcelableException;
import android.os.StatFs;
import android.os.UserHandle;
import android.os.UserManager;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.telecom.PhoneAccount;
import android.telecom.PhoneAccountHandle;
import android.telecom.TelecomManager;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.ArrayMap;
import android.util.EventLog;
import android.util.LocalLog;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;
import com.android.internal.util.ProviderAccessStats;
import com.android.providers.contacts.CallLogDatabaseHelper.DbProperties;
import com.android.providers.contacts.CallLogDatabaseHelper.Tables;
import com.android.providers.contacts.util.FileUtilities;
import com.android.providers.contacts.util.NeededForTesting;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.UserUtils;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Call log content provider.
 */
public class CallLogProvider extends ContentProvider {
    private static final String TAG = "CallLogProvider";

    public static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    @VisibleForTesting
    protected static final int BACKGROUND_TASK_INITIALIZE = 0;
    private static final int BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT = 1;
    private static final int BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES = 2;

    /** Selection clause for selecting all calls that were made after a certain time */
    private static final String MORE_RECENT_THAN_SELECTION = Calls.DATE + "> ?";
    /** Selection clause to use to exclude voicemail records.  */
    private static final String EXCLUDE_VOICEMAIL_SELECTION = getInequalityClause(
            Calls.TYPE, Calls.VOICEMAIL_TYPE);
    /** Selection clause to exclude hidden records. */
    private static final String EXCLUDE_HIDDEN_SELECTION = getEqualityClause(
            Calls.PHONE_ACCOUNT_HIDDEN, 0);

    private static final String CALL_COMPOSER_PICTURE_DIRECTORY_NAME = "call_composer_pics";
    private static final String CALL_COMPOSER_ALL_USERS_DIRECTORY_NAME = "all_users";

    // Constants to be used with ContentProvider#call in order to sync call composer pics between
    // users. Defined here because they're for internal use only.
    /**
     * Method name used to get a list of {@link Uri}s for call composer pictures inserted for all
     * users after a certain date
     */
    private static final String GET_CALL_COMPOSER_IMAGE_URIS =
            "com.android.providers.contacts.GET_CALL_COMPOSER_IMAGE_URIS";

    /**
     * Long-valued extra containing the date to filter by expressed as milliseconds after the epoch.
     */
    private static final String EXTRA_SINCE_DATE =
            "com.android.providers.contacts.extras.SINCE_DATE";

    /**
     * Boolean-valued extra indicating whether to read from the shadow portion of the calllog
     * (i.e. device-encrypted storage rather than credential-encrypted)
     */
    private static final String EXTRA_IS_SHADOW =
            "com.android.providers.contacts.extras.IS_SHADOW";

    /**
     * Boolean-valued extra indicating whether to return Uris only for those images that are
     * supposed to be inserted for all users.
     */
    private static final String EXTRA_ALL_USERS_ONLY =
            "com.android.providers.contacts.extras.ALL_USERS_ONLY";

    private static final String EXTRA_RESULT_URIS =
            "com.android.provider.contacts.extras.EXTRA_RESULT_URIS";

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
            Calls.PRIORITY,
            Calls.SUBJECT,
            Calls.COMPOSER_PHOTO_URI,
            // Location is deliberately omitted
            Calls.ADD_FOR_ALL_USERS
    };

    static final String[] MINIMAL_PROJECTION = new String[] { Calls._ID };

    private static final int CALLS = 1;

    private static final int CALLS_ID = 2;

    private static final int CALLS_FILTER = 3;

    private static final int CALL_COMPOSER_NEW_PICTURE = 4;

    private static final int CALL_COMPOSER_PICTURE = 5;

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
        sURIMatcher.addURI(CallLog.AUTHORITY, CallLog.CALL_COMPOSER_SEGMENT,
                CALL_COMPOSER_NEW_PICTURE);
        sURIMatcher.addURI(CallLog.AUTHORITY, CallLog.CALL_COMPOSER_SEGMENT + "/*",
                CALL_COMPOSER_PICTURE);

        // Shadow provider only supports "/calls" and "/call_composer".
        sURIMatcher.addURI(CallLog.SHADOW_AUTHORITY, "calls", CALLS);
        sURIMatcher.addURI(CallLog.SHADOW_AUTHORITY, CallLog.CALL_COMPOSER_SEGMENT,
                CALL_COMPOSER_NEW_PICTURE);
        sURIMatcher.addURI(CallLog.SHADOW_AUTHORITY, CallLog.CALL_COMPOSER_SEGMENT + "/*",
                CALL_COMPOSER_PICTURE);
    }

    public static final ArrayMap<String, String> sCallsProjectionMap;
    static {

        // Calls projection map
        sCallsProjectionMap = new ArrayMap<>();
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
        sCallsProjectionMap.put(Calls.PHONE_ACCOUNT_HIDDEN, Calls.PHONE_ACCOUNT_HIDDEN);
        sCallsProjectionMap.put(Calls.PHONE_ACCOUNT_ADDRESS, Calls.PHONE_ACCOUNT_ADDRESS);
        sCallsProjectionMap.put(Calls.NEW, Calls.NEW);
        sCallsProjectionMap.put(Calls.VOICEMAIL_URI, Calls.VOICEMAIL_URI);
        sCallsProjectionMap.put(Calls.TRANSCRIPTION, Calls.TRANSCRIPTION);
        sCallsProjectionMap.put(Calls.TRANSCRIPTION_STATE, Calls.TRANSCRIPTION_STATE);
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
        sCallsProjectionMap
            .put(Calls.CALL_SCREENING_COMPONENT_NAME, Calls.CALL_SCREENING_COMPONENT_NAME);
        sCallsProjectionMap.put(Calls.CALL_SCREENING_APP_NAME, Calls.CALL_SCREENING_APP_NAME);
        sCallsProjectionMap.put(Calls.BLOCK_REASON, Calls.BLOCK_REASON);
        sCallsProjectionMap.put(Calls.MISSED_REASON, Calls.MISSED_REASON);
        sCallsProjectionMap.put(Calls.PRIORITY, Calls.PRIORITY);
        sCallsProjectionMap.put(Calls.COMPOSER_PHOTO_URI, Calls.COMPOSER_PHOTO_URI);
        sCallsProjectionMap.put(Calls.SUBJECT, Calls.SUBJECT);
        sCallsProjectionMap.put(Calls.LOCATION, Calls.LOCATION);
        sCallsProjectionMap.put(Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING,
                Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING);
    }

    /**
     * Subscription change will trigger ACTION_PHONE_ACCOUNT_REGISTERED that broadcasts new
     * PhoneAccountHandle that is created based on the new subscription. This receiver is used
     * for listening new subscription change and migrating phone account handle if any pending.
     *
     * It is then used by the call log to un-hide any entries which were previously hidden after
     * a backup-restore until its associated phone-account is registered with telecom. After a
     * restore, we hide call log entries until the user inserts the corresponding SIM, registers
     * the corresponding SIP account, or registers a corresponding alternative phone-account.
     */
    private final BroadcastReceiver mBroadcastReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (TelecomManager.ACTION_PHONE_ACCOUNT_REGISTERED.equals(intent.getAction())) {
                PhoneAccountHandle phoneAccountHandle =
                        (PhoneAccountHandle) intent.getParcelableExtra(
                                TelecomManager.EXTRA_PHONE_ACCOUNT_HANDLE);
                if (mDbHelper.getPhoneAccountHandleMigrationUtils()
                        .isPhoneAccountMigrationPending()
                        && TELEPHONY_COMPONENT_NAME.equals(
                                phoneAccountHandle.getComponentName().flattenToString())
                        && !mMigratedPhoneAccountHandles.contains(phoneAccountHandle)) {
                    mMigratedPhoneAccountHandles.add(phoneAccountHandle);
                    mTaskScheduler.scheduleTask(
                            BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES, phoneAccountHandle);
                } else {
                    mTaskScheduler.scheduleTask(BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT,
                            phoneAccountHandle);
                }
            }
        }
    };

    private static final String ALLOWED_PACKAGE_FOR_TESTING = "com.android.providers.contacts";

    @VisibleForTesting
    static final String PARAM_KEY_QUERY_FOR_TESTING = "query_for_testing";

    /**
     * A long to override the clock used for timestamps, or "null" to reset to the system clock.
     */
    @VisibleForTesting
    static final String PARAM_KEY_SET_TIME_FOR_TESTING = "set_time_for_testing";

    private static Long sTimeForTestMillis;

    private ContactsTaskScheduler mTaskScheduler;

    @VisibleForTesting
    protected volatile CountDownLatch mReadAccessLatch;

    private CallLogDatabaseHelper mDbHelper;
    private DatabaseUtils.InsertHelper mCallsInserter;
    private boolean mUseStrictPhoneNumberComparation;
    private int mMinMatch;
    private VoicemailPermissions mVoicemailPermissions;
    private CallLogInsertionHelper mCallLogInsertionHelper;
    private SubscriptionManager mSubscriptionManager;
    private LocalLog mLocalLog = new LocalLog(20);

    private final ThreadLocal<Boolean> mApplyingBatch = new ThreadLocal<>();
    private final ThreadLocal<Integer> mCallingUid = new ThreadLocal<>();
    private final ProviderAccessStats mStats = new ProviderAccessStats();
    private final Set<PhoneAccountHandle> mMigratedPhoneAccountHandles = new HashSet<>();

    protected boolean isShadow() {
        return false;
    }

    protected final String getProviderName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean onCreate() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "onCreate: " + this.getClass().getSimpleName()
                    + " user=" + android.os.Process.myUserHandle().getIdentifier());
        }

        setAppOps(AppOpsManager.OP_READ_CALL_LOG, AppOpsManager.OP_WRITE_CALL_LOG);
        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.DEBUG)) {
            Log.d(Constants.PERFORMANCE_TAG, getProviderName() + ".onCreate start");
        }
        final Context context = getContext();
        mDbHelper = getDatabaseHelper(context);
        mUseStrictPhoneNumberComparation =
            context.getResources().getBoolean(
                    com.android.internal.R.bool.config_use_strict_phone_number_comparation);
        mMinMatch =
            context.getResources().getInteger(
                    com.android.internal.R.integer.config_phonenumber_compare_min_match);
        mVoicemailPermissions = new VoicemailPermissions(context);
        mCallLogInsertionHelper = createCallLogInsertionHelper(context);

        mReadAccessLatch = new CountDownLatch(1);

        mTaskScheduler = new ContactsTaskScheduler(getClass().getSimpleName()) {
            @Override
            public void onPerformTask(int taskId, Object arg) {
                performBackgroundTask(taskId, arg);
            }
        };

        mTaskScheduler.scheduleTask(BACKGROUND_TASK_INITIALIZE, null);

        mSubscriptionManager = context.getSystemService(SubscriptionManager.class);

        // Register a receiver to hear sim change event for migrating pending
        // PhoneAccountHandle ID or/and unhides restored call logs
        IntentFilter filter = new IntentFilter(TelecomManager.ACTION_PHONE_ACCOUNT_REGISTERED);
        context.registerReceiver(mBroadcastReceiver, filter);

        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.DEBUG)) {
            Log.d(Constants.PERFORMANCE_TAG, getProviderName() + ".onCreate finish");
        }
        return true;
    }

    @VisibleForTesting
    protected CallLogInsertionHelper createCallLogInsertionHelper(final Context context) {
        return DefaultCallLogInsertionHelper.getInstance(context);
    }

    @VisibleForTesting
    public void setMinMatchForTest(int minMatch) {
        mMinMatch = minMatch;
    }

    @VisibleForTesting
    public int getMinMatchForTest() {
        return mMinMatch;
    }

    @NeededForTesting
    public CallLogDatabaseHelper getCallLogDatabaseHelperForTest() {
        return mDbHelper;
    }

    @NeededForTesting
    public void setCallLogDatabaseHelperForTest(CallLogDatabaseHelper callLogDatabaseHelper) {
        mDbHelper = callLogDatabaseHelper;
    }

    /**
     * @return the currently registered BroadcastReceiver for listening
     *         ACTION_PHONE_ACCOUNT_REGISTERED in the current process.
     */
    @NeededForTesting
    public BroadcastReceiver getBroadcastReceiverForTest() {
        return mBroadcastReceiver;
    }

    protected CallLogDatabaseHelper getDatabaseHelper(final Context context) {
        return CallLogDatabaseHelper.getInstance(context);
    }

    protected boolean applyingBatch() {
        final Boolean applying =  mApplyingBatch.get();
        return applying != null && applying;
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        final int callingUid = Binder.getCallingUid();
        mCallingUid.set(callingUid);

        mStats.incrementBatchStats(callingUid);
        mApplyingBatch.set(true);
        try {
            return super.applyBatch(operations);
        } finally {
            mApplyingBatch.set(false);
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values) {
        final int callingUid = Binder.getCallingUid();
        mCallingUid.set(callingUid);

        mStats.incrementBatchStats(callingUid);
        mApplyingBatch.set(true);
        try {
            return super.bulkInsert(uri, values);
        } finally {
            mApplyingBatch.set(false);
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // Note don't use mCallingUid here. That's only used by mutation functions.
        final int callingUid = Binder.getCallingUid();

        mStats.incrementQueryStats(callingUid);
        try {
            return queryInternal(uri, projection, selection, selectionArgs, sortOrder);
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    private Cursor queryInternal(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: uri=" + uri + "  projection=" + Arrays.toString(projection) +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  order=[" + sortOrder + "] CPID=" + Binder.getCallingPid() +
                    " CUID=" + Binder.getCallingUid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }

        queryForTesting(uri);

        waitForAccess(mReadAccessLatch);
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Tables.CALLS);
        qb.setProjectionMap(sCallsProjectionMap);
        qb.setStrict(true);
        // If the caller doesn't have READ_VOICEMAIL, make sure they can't
        // do any SQL shenanigans to get access to the voicemails. If the caller does have the
        // READ_VOICEMAIL permission, then they have sufficient permissions to access any data in
        // the database, so the strict check is unnecessary.
        if (!mVoicemailPermissions.callerHasReadAccess(getCallingPackage())) {
            qb.setStrictGrammar(true);
        }

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
                    qb.appendWhere("PHONE_NUMBERS_EQUAL(number, ?");
                    qb.appendWhere(mUseStrictPhoneNumberComparation ? ", 1)"
                            : ", 0, " + mMinMatch + ")");
                    selectionArgs = copyArrayAndAppendElement(selectionArgs,
                            "'" + phoneNumber + "'");
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

        if (match == CALLS_FILTER && selectionArgs.length > 0) {
            // throw SE if the user is sending requests that try to bypass voicemail permissions
            examineEmptyCursorCause(c, selectionArgs[selectionArgs.length - 1]);
        }

        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), CallLog.CONTENT_URI);
        }
        return c;
    }

    /**
     * Helper method for queryInternal that appends an extra argument to the existing selection
     * arguments array.
     *
     * @param oldSelectionArguments the existing selection argument array in queryInternal
     * @param phoneNumber           the phoneNumber that was passed into queryInternal
     * @return the new selection argument array with the phoneNumber as the last argument
     */
    private String[] copyArrayAndAppendElement(String[] oldSelectionArguments, String phoneNumber) {
        if (oldSelectionArguments == null) {
            return new String[]{phoneNumber};
        }
        String[] newSelectionArguments = new String[oldSelectionArguments.length + 1];
        System.arraycopy(oldSelectionArguments, 0, newSelectionArguments, 0,
                oldSelectionArguments.length);
        newSelectionArguments[oldSelectionArguments.length] = phoneNumber;
        return newSelectionArguments;
    }

    /**
     * Helper that throws a Security Exception if the Cursor object is empty && the phoneNumber
     * appears to have SQL.
     *
     * @param cursor      returned from the query.
     * @param phoneNumber string to check for SQL.
     */
    private void examineEmptyCursorCause(Cursor cursor, String phoneNumber) {
        // checks if the cursor is empty
        if ((cursor == null) || !cursor.moveToFirst()) {
            try {
                // tokenize the phoneNumber and run each token through a checker
                SQLiteTokenizer.tokenize(phoneNumber, SQLiteTokenizer.OPTION_NONE,
                        this::enforceStrictPhoneNumber);
            } catch (IllegalArgumentException e) {
                EventLog.writeEvent(0x534e4554, "224771921", Binder.getCallingUid(),
                        ("invalid phoneNumber passed to queryInternal"));
                throw new SecurityException("invalid phoneNumber passed to queryInternal");
            }
        }
    }

    private void enforceStrictPhoneNumber(String token) {
        boolean isAllowedKeyword = SQLiteTokenizer.isKeyword(token);
        Set<String> lookupTable = Set.of("UNION", "SELECT", "FROM", "WHERE",
                "GROUP", "HAVING", "WINDOW", "VALUES", "ORDER", "LIMIT");
        if (!isAllowedKeyword || lookupTable.contains(token.toUpperCase(Locale.US))) {
            throw new IllegalArgumentException("Invalid token " + token);
        }
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
            case CALL_COMPOSER_NEW_PICTURE:
                return null; // No type for newly created files
            case CALL_COMPOSER_PICTURE:
                // We don't know the exact image format, so this is as specific as we can be.
                return "application/octet-stream";
            default:
                throw new IllegalArgumentException("Unknown URI: " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int callingUid =
                applyingBatch() ? mCallingUid.get() : Binder.getCallingUid();

        mStats.incrementInsertStats(callingUid, applyingBatch());
        try {
            return insertInternal(uri, values);
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final int callingUid =
                applyingBatch() ? mCallingUid.get() : Binder.getCallingUid();

        mStats.incrementUpdateStats(callingUid, applyingBatch());
        try {
            return updateInternal(uri, values, selection, selectionArgs);
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final int callingUid =
                applyingBatch() ? mCallingUid.get() : Binder.getCallingUid();

        mStats.incrementDeleteStats(callingUid, applyingBatch());
        try {
            return deleteInternal(uri, selection, selectionArgs);
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    private Uri insertInternal(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insert: uri=" + uri + "  values=[" + values + "]" +
                    " CPID=" + Binder.getCallingPid() +
                    " CUID=" + Binder.getCallingUid());
        }
        waitForAccess(mReadAccessLatch);
        int match = sURIMatcher.match(uri);
        switch (match) {
            case CALL_COMPOSER_PICTURE: {
                String fileName = uri.getLastPathSegment();
                try {
                    return allocateNewCallComposerPicture(values,
                            CallLog.SHADOW_AUTHORITY.equals(uri.getAuthority()),
                            fileName);
                } catch (IOException e) {
                    throw new ParcelableException(e);
                }
            }
            case CALL_COMPOSER_NEW_PICTURE: {
                try {
                    return allocateNewCallComposerPicture(values,
                            CallLog.SHADOW_AUTHORITY.equals(uri.getAuthority()));
                } catch (IOException e) {
                    throw new ParcelableException(e);
                }
            }
            default:
                // Fall through and execute the rest of the method for ordinary call log insertions.
        }

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

        long rowId = createDatabaseModifier(mCallsInserter).insert(copiedValues);
        String insertLog = String.format(Locale.getDefault(),
                "insert uid/pid=%d/%d, uri=%s, rowId=%d",
                Binder.getCallingUid(), Binder.getCallingPid(), uri, rowId);
        Log.i(TAG, insertLog);
        mLocalLog.log(insertLog);
        if (rowId > 0) {
            return ContentUris.withAppendedId(uri, rowId);
        }
        return null;
    }

    @Override
    public ParcelFileDescriptor openFile(@NonNull Uri uri, @NonNull String mode)
            throws FileNotFoundException {
        int match = sURIMatcher.match(uri);
        if (match != CALL_COMPOSER_PICTURE) {
            throw new UnsupportedOperationException("The call log provider only supports opening"
                    + " call composer pictures.");
        }
        int modeInt;
        switch (mode) {
            case "r":
                modeInt = ParcelFileDescriptor.MODE_READ_ONLY;
                break;
            case "w":
                modeInt = ParcelFileDescriptor.MODE_WRITE_ONLY;
                break;
            default:
                throw new UnsupportedOperationException("The call log does not support opening"
                        + " a call composer picture with mode " + mode);
        }

        try {
            Path callComposerDir = getCallComposerPictureDirectory(getContext(), uri);
            Path pictureFile = callComposerDir.resolve(uri.getLastPathSegment());
            if (Files.notExists(pictureFile)) {
                throw new FileNotFoundException(uri.toString()
                        + " does not correspond to a valid file.");
            }
            enforceValidCallLogPath(callComposerDir, pictureFile,"openFile");
            return ParcelFileDescriptor.open(pictureFile.toFile(), modeInt);
        } catch (IOException e) {
            Log.e(TAG, "IOException while opening call composer file: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Bundle call(@NonNull String method, @Nullable String arg, @Nullable Bundle extras) {
        Log.i(TAG, "Fetching list of Uris to sync");
        if (!UserHandle.isSameApp(android.os.Process.myUid(), Binder.getCallingUid())) {
            throw new SecurityException("call() functionality reserved"
                    + " for internal use by the call log.");
        }
        if (!GET_CALL_COMPOSER_IMAGE_URIS.equals(method)) {
            throw new UnsupportedOperationException("Invalid method passed to call(): " + method);
        }
        if (!extras.containsKey(EXTRA_SINCE_DATE)) {
            throw new IllegalArgumentException("SINCE_DATE required");
        }
        if (!extras.containsKey(EXTRA_IS_SHADOW)) {
            throw new IllegalArgumentException("IS_SHADOW required");
        }
        if (!extras.containsKey(EXTRA_ALL_USERS_ONLY)) {
            throw new IllegalArgumentException("ALL_USERS_ONLY required");
        }
        boolean isShadow = extras.getBoolean(EXTRA_IS_SHADOW);
        boolean allUsers = extras.getBoolean(EXTRA_ALL_USERS_ONLY);
        long sinceDate = extras.getLong(EXTRA_SINCE_DATE);

        try {
            Path queryDir = allUsers
                    ? getCallComposerAllUsersPictureDirectory(getContext(), isShadow)
                    : getCallComposerPictureDirectory(getContext(), isShadow);
            List<Path> newestPics = new ArrayList<>();
            try (DirectoryStream<Path> dirStream =
                         Files.newDirectoryStream(queryDir, entry -> {
                             if (Files.isDirectory(entry)) {
                                 return false;
                             }
                             FileTime createdAt =
                                     (FileTime) Files.getAttribute(entry, "creationTime");
                             return createdAt.toMillis() > sinceDate;
                         })) {
                dirStream.forEach(newestPics::add);
            }
            List<Uri> fileUris = newestPics.stream().map((path) -> {
                String fileName = path.getFileName().toString();
                // We don't need to worry about if it's for all users -- anything that's for
                // all users is also stored in the regular location.
                Uri base = isShadow ? CallLog.SHADOW_CALL_COMPOSER_PICTURE_URI
                        : CallLog.CALL_COMPOSER_PICTURE_URI;
                return base.buildUpon().appendPath(fileName).build();
            }).collect(Collectors.toList());
            Bundle result = new Bundle();
            result.putParcelableList(EXTRA_RESULT_URIS, fileUris);
            Log.i(TAG, "Will sync following Uris:" + fileUris);
            return result;
        } catch (IOException e) {
            Log.e(TAG, "IOException while trying to fetch URI list: " + e);
            return null;
        }
    }

    private static @NonNull Path getCallComposerPictureDirectory(Context context, Uri uri)
            throws IOException {
        boolean isShadow = CallLog.SHADOW_AUTHORITY.equals(uri.getAuthority());
        return getCallComposerPictureDirectory(context, isShadow);
    }

    private static @NonNull Path getCallComposerPictureDirectory(Context context, boolean isShadow)
            throws IOException {
        if (isShadow) {
            context = context.createDeviceProtectedStorageContext();
        }
        Path path = context.getFilesDir().toPath().resolve(CALL_COMPOSER_PICTURE_DIRECTORY_NAME);
        if (!Files.isDirectory(path)) {
            Files.createDirectory(path);
        }
        return path;
    }

    private static @NonNull Path getCallComposerAllUsersPictureDirectory(
            Context context, boolean isShadow) throws IOException {
        Path pathToCallComposerDir = getCallComposerPictureDirectory(context, isShadow);
        Path path = pathToCallComposerDir.resolve(CALL_COMPOSER_ALL_USERS_DIRECTORY_NAME);
        if (!Files.isDirectory(path)) {
            Files.createDirectory(path);
        }
        return path;
    }

    private Uri allocateNewCallComposerPicture(ContentValues values, boolean isShadow)
            throws IOException {
        return allocateNewCallComposerPicture(values, isShadow, UUID.randomUUID().toString());
    }

    private Uri allocateNewCallComposerPicture(ContentValues values,
            boolean isShadow, String fileName) throws IOException {
        Uri baseUri = isShadow ?
                CallLog.CALL_COMPOSER_PICTURE_URI.buildUpon()
                        .authority(CallLog.SHADOW_AUTHORITY).build()
                : CallLog.CALL_COMPOSER_PICTURE_URI;

        boolean forAllUsers = values.containsKey(Calls.ADD_FOR_ALL_USERS)
                && (values.getAsInteger(Calls.ADD_FOR_ALL_USERS) == 1);
        Path pathToCallComposerDir = getCallComposerPictureDirectory(getContext(), isShadow);

        if (new StatFs(pathToCallComposerDir.toString()).getAvailableBytes()
                < TelephonyManager.getMaximumCallComposerPictureSize()) {
            return null;
        }
        Path pathToFile = pathToCallComposerDir.resolve(fileName);
        enforceValidCallLogPath(pathToCallComposerDir, pathToFile,
                "allocateNewCallComposerPicture");
        Files.createFile(pathToFile);

        if (forAllUsers) {
            // Create a symlink in a subdirectory for copying later.
            Path allUsersDir = getCallComposerAllUsersPictureDirectory(getContext(), isShadow);
            Files.createSymbolicLink(allUsersDir.resolve(fileName), pathToFile);
        }
        return baseUri.buildUpon().appendPath(fileName).build();
    }

    private int deleteCallComposerPicture(Uri uri) {
        try {
            Path pathToCallComposerDir = getCallComposerPictureDirectory(getContext(), uri);
            Path fileToDelete = pathToCallComposerDir.resolve(uri.getLastPathSegment());
            enforceValidCallLogPath(pathToCallComposerDir, fileToDelete,
                    "deleteCallComposerPicture");
            return Files.deleteIfExists(fileToDelete) ? 1 : 0;
        } catch (IOException e) {
            Log.e(TAG, "IOException encountered deleting the call composer pics dir " + e);
            return 0;
        }
    }

    private int updateInternal(Uri uri, ContentValues values,
            String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "update: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  values=[" + values + "] CPID=" + Binder.getCallingPid() +
                    " CUID=" + Binder.getCallingUid() +
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
        boolean hasReadVoicemailPermission = mVoicemailPermissions.callerHasReadAccess(
                getCallingPackage());
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

        int count = createDatabaseModifier(db, hasReadVoicemailPermission).update(uri, Tables.CALLS,
                values, selectionBuilder.build(), selectionArgs);

        String logStr = String.format(Locale. getDefault(),
                "update uid/pid=%d/%d, uri=%s, numChanged=%d",
                Binder.getCallingUid(), Binder.getCallingPid(), uri, count);
        Log.i(TAG, logStr);
        mLocalLog.log(logStr);

        return count;
    }

    private int deleteInternal(Uri uri, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "delete: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    " CPID=" + Binder.getCallingPid() +
                    " CUID=" + Binder.getCallingUid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        waitForAccess(mReadAccessLatch);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        checkVoicemailPermissionAndAddRestriction(uri, selectionBuilder, false /*isQuery*/);

        boolean hasReadVoicemailPermission =
                mVoicemailPermissions.callerHasReadAccess(getCallingPackage());
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        final int matchedUriId = sURIMatcher.match(uri);
        switch (matchedUriId) {
            case CALLS:
                int count =  createDatabaseModifier(db, hasReadVoicemailPermission).delete(
                        Tables.CALLS, selectionBuilder.build(), selectionArgs);
                String logStr = String.format(Locale. getDefault(),
                        "delete uid/pid=%d/%d, uri=%s, numChanged=%d",
                        Binder.getCallingUid(), Binder.getCallingPid(), uri, count);
                Log.i(TAG, logStr);
                mLocalLog.log(logStr);
                return count;
            case CALL_COMPOSER_PICTURE:
                // TODO(hallliu): implement deletion of file when the corresponding calllog entry
                // gets deleted as well.
                return deleteCallComposerPicture(uri);
            default:
                throw new UnsupportedOperationException("Cannot delete that URL: " + uri);
        }
    }

    /**
     * Returns a {@link DatabaseModifier} that takes care of sending necessary notifications
     * after the operation is performed.
     */
    private DatabaseModifier createDatabaseModifier(SQLiteDatabase db, boolean hasReadVoicemail) {
        return new DbModifierWithNotification(Tables.CALLS, db, null, hasReadVoicemail,
                getContext());
    }

    /**
     * Same as {@link #createDatabaseModifier(SQLiteDatabase)} but used for insert helper operations
     * only.
     */
    private DatabaseModifier createDatabaseModifier(DatabaseUtils.InsertHelper insertHelper) {
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
        final int myUserId = userManager.getProcessUserId();

        // TODO: http://b/24944959
        if (!Calls.shouldHaveSharedCallLogEntries(getContext(), userManager, myUserId)) {
            return;
        }

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
            Log.i(TAG, String.format(Locale.getDefault(),
                    "syncEntriesFrom: fromUserId=%d, srcIsShadow=%b, forAllUsers=%b; nothing to "
                            + "sync",
                    sourceUserId, sourceIsShadow, forAllUsersOnly));
            return;
        }
        try {
            newestTimeStamp = copyEntriesFromCursor(cursor, lastSyncTime, sourceIsShadow);
            Log.i(TAG,
                    String.format(Locale.getDefault(),
                            "syncEntriesFrom: fromUserId=%d, srcIsShadow=%b, forAllUsers=%b; "
                                    + "previousTimeStamp=%d, newTimeStamp=%d, entries=%d",
                            sourceUserId, sourceIsShadow, forAllUsersOnly, lastSyncTime,
                            newestTimeStamp,
                            cursor.getCount()));
        } finally {
            cursor.close();
        }
        if (sourceIsShadow) {
            // delete all entries in shadow.
            cr.delete(uri, Calls.DATE + "<= ?", new String[] {String.valueOf(newestTimeStamp)});
        }

        try {
            syncCallComposerPics(sourceUserId, sourceIsShadow, forAllUsersOnly, lastSyncTime);
        } catch (Exception e) {
            // Catch any exceptions to make sure we don't bring down the entire process if something
            // goes wrong
            StringWriter w = new StringWriter();
            PrintWriter pw = new PrintWriter(w);
            e.printStackTrace(pw);
            Log.e(TAG, "Caught exception syncing call composer pics: " + e
                    + "\n" + pw.toString());
        }
    }

    private void syncCallComposerPics(int sourceUserId, boolean sourceIsShadow,
            boolean forAllUsersOnly, long lastSyncTime) {
        Log.i(TAG, "Syncing call composer pics -- source user=" + sourceUserId + ","
                + " isShadow=" + sourceIsShadow + ", forAllUser=" + forAllUsersOnly);
        ContentResolver contentResolver = getContext().getContentResolver();
        Bundle args = new Bundle();
        args.putLong(EXTRA_SINCE_DATE, lastSyncTime);
        args.putBoolean(EXTRA_ALL_USERS_ONLY, forAllUsersOnly);
        args.putBoolean(EXTRA_IS_SHADOW, sourceIsShadow);
        Uri queryUri = ContentProvider.maybeAddUserId(
                sourceIsShadow
                        ? CallLog.SHADOW_CALL_COMPOSER_PICTURE_URI
                        : CallLog.CALL_COMPOSER_PICTURE_URI,
                sourceUserId);
        Bundle result = contentResolver.call(queryUri, GET_CALL_COMPOSER_IMAGE_URIS, null, args);
        if (result == null || !result.containsKey(EXTRA_RESULT_URIS)) {
            Log.e(TAG, "Failed to sync call composer pics -- invalid return from call()");
            return;
        }
        List<Uri> urisToCopy = result.getParcelableArrayList(EXTRA_RESULT_URIS);
        Log.i(TAG, "Syncing call composer pics -- got " + urisToCopy);
        for (Uri uri : urisToCopy) {
            try {
                Uri uriWithUser = ContentProvider.maybeAddUserId(uri, sourceUserId);
                Path callComposerDir = getCallComposerPictureDirectory(getContext(), false);
                Path newFilePath = callComposerDir.resolve(uri.getLastPathSegment());
                enforceValidCallLogPath(callComposerDir, newFilePath,"syncCallComposerPics");
                try (ParcelFileDescriptor remoteFile = contentResolver.openFile(uriWithUser,
                        "r", null);
                     OutputStream localOut =
                             Files.newOutputStream(newFilePath, StandardOpenOption.CREATE_NEW)) {
                    FileInputStream input = new FileInputStream(remoteFile.getFileDescriptor());
                    byte[] buffer = new byte[1 << 14]; // 16kb
                    while (true) {
                        int numRead = input.read(buffer);
                        if (numRead < 0) {
                            break;
                        }
                        localOut.write(buffer, 0, numRead);
                    }
                }
                contentResolver.delete(uriWithUser, null);
            } catch (IOException e) {
                Log.e(TAG, "IOException while syncing call composer pics: " + e);
                // Keep going and get as many as we can.
            }
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
                    TelecomManager tm = getContext().getSystemService(TelecomManager.class);
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

    @VisibleForTesting
    protected void performBackgroundTask(int task, Object arg) {
        if (task == BACKGROUND_TASK_INITIALIZE) {
            try {
                mDbHelper.updatePhoneAccountHandleMigrationPendingStatus();
                if (mDbHelper.getPhoneAccountHandleMigrationUtils()
                        .isPhoneAccountMigrationPending()) {
                    Log.i(TAG, "performBackgroundTask for pending PhoneAccountHandle migration");
                    mDbHelper.migrateIccIdToSubId();
                }
                syncEntries();
            } finally {
                mReadAccessLatch.countDown();
            }
        } else if (task == BACKGROUND_TASK_ADJUST_PHONE_ACCOUNT) {
            Log.i(TAG, "performBackgroundTask for unhide PhoneAccountHandles");
            adjustForNewPhoneAccountInternal((PhoneAccountHandle) arg);
        } else if (task == BACKGROUND_TASK_MIGRATE_PHONE_ACCOUNT_HANDLES) {
            PhoneAccountHandle phoneAccountHandle = (PhoneAccountHandle) arg;
            String iccId = null;
            try {
                SubscriptionInfo info = mSubscriptionManager.getActiveSubscriptionInfo(
                        Integer.parseInt(phoneAccountHandle.getId()));
                if (info != null) {
                    iccId = info.getIccId();
                }
            } catch (NumberFormatException nfe) {
                // Ignore the exception, iccId will remain null and be handled below.
            }
            if (iccId == null) {
                Log.i(TAG, "ACTION_PHONE_ACCOUNT_REGISTERED received null IccId.");
            } else {
                Log.i(TAG, "ACTION_PHONE_ACCOUNT_REGISTERED received for migrating phone"
                        + " account handle SubId: " + phoneAccountHandle.getId());
                mDbHelper.migratePendingPhoneAccountHandles(iccId, phoneAccountHandle.getId());
            }
        }
    }

    @Override
    public void shutdown() {
        mTaskScheduler.shutdownForTest();
    }

    @Override
    public void dump(FileDescriptor fd, PrintWriter writer, String[] args) {
        mStats.dump(writer, "  ");
        writer.println();
        writer.println("Latest call log activity:");
        mLocalLog.dump(writer);
    }

    /**
     *  Enforces a stricter check on what files the CallLogProvider can perform file operations on.
     * @param rootPath where all valid new/existing paths should pass through.
     * @param pathToCheck newly created path that is requesting a file op. (open, delete, etc.)
     * @param callingMethod the calling method.  Used only for debugging purposes.
     */
    private void enforceValidCallLogPath(Path rootPath, Path pathToCheck, String callingMethod){
        if (!FileUtilities.isSameOrSubDirectory(rootPath.toFile(), pathToCheck.toFile())) {
            EventLog.writeEvent(0x534e4554, "219015884", Binder.getCallingUid(),
                    (callingMethod + ": invalid uri passed"));
            throw new SecurityException(
                    FileUtilities.INVALID_CALL_LOG_PATH_EXCEPTION_MESSAGE + pathToCheck);
        }
    }
}
