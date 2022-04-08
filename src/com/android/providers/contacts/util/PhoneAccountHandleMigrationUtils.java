/*
 * Copyright (C) 2022 The Android Open Source Project
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
package com.android.providers.contacts.util;

import com.android.providers.contacts.CallLogDatabaseHelper;
import com.android.providers.contacts.ContactsDatabaseHelper;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.database.sqlite.SQLiteDatabase;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.provider.ContactsContract.Data;
import android.preference.PreferenceManager;
import android.telephony.SubscriptionManager;
import android.telephony.SubscriptionInfo;
import android.text.TextUtils;
import android.util.Log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils for PhoneAccountHandle Migration Operations in database providers.
 *
 * When the database is created and upgraded, PhoneAccountHandleMigrationUtils helps migrate IccId
 * to SubId. If the PhoneAccount haven't registered yet, we set the pending status for further
 * migration. Databases will listen to broadcast
 * {@link android.telecom.TelecomManager#ACTION_PHONE_ACCOUNT_REGISTERED} to identify a new sim
 * event and performing migration for pending status if possible.
 */
public class PhoneAccountHandleMigrationUtils {
    /**
     * Indicates type of ContactsDatabase.
     */
    public static final int TYPE_CONTACTS = 0;
    /**
     * Indicates type of CallLogDatabase.
     */
    public static final int TYPE_CALL_LOG = 1;

    public static final String TELEPHONY_COMPONENT_NAME =
            "com.android.phone/com.android.services.telephony.TelephonyConnectionService";
    private static final String[] TAGS = {
            "PhoneAccountHandleMigrationUtils_ContactsDatabaseHelper",
                    "PhoneAccountHandleMigrationUtils_CallLogDatabaseHelper"};
    private static final String[] TABLES = {ContactsDatabaseHelper.Tables.DATA,
            CallLogDatabaseHelper.Tables.CALLS};
    private static final String[] IDS = {Data._ID, Calls._ID};
    private static final String[] PENDING_STATUS_FIELDS = {
            Data.IS_PHONE_ACCOUNT_MIGRATION_PENDING, Calls.IS_PHONE_ACCOUNT_MIGRATION_PENDING};
    private static final String[] COMPONENT_NAME_FIELDS = {
            Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME, Calls.PHONE_ACCOUNT_COMPONENT_NAME};
    private static final String[] PHONE_ACCOUNT_ID_FIELDS = {
            Data.PREFERRED_PHONE_ACCOUNT_ID, Calls.PHONE_ACCOUNT_ID};

    private int mType;
    private SubscriptionManager mSubscriptionManager;
    private SharedPreferences mSharedPreferences;

    /**
     *  Constructor of the util.
     */
    public PhoneAccountHandleMigrationUtils(Context context, int type) {
        mType = type;
        mSharedPreferences = PreferenceManager.getDefaultSharedPreferences(context);
        mSubscriptionManager = context.getSystemService(SubscriptionManager.class);
    }

    /**
     * Mark all the telephony phone account handles as pending migration.
     */
    public void markAllTelephonyPhoneAccountsPendingMigration(SQLiteDatabase db) {
        ContentValues valuesForTelephonyPending = new ContentValues();
        valuesForTelephonyPending.put(PENDING_STATUS_FIELDS[mType], 1);
        String selection = COMPONENT_NAME_FIELDS[mType] + " = ?";
        String[] selectionArgs = {TELEPHONY_COMPONENT_NAME};
        db.beginTransaction();
        try {
            int count = db.update(
                    TABLES[mType], valuesForTelephonyPending, selection, selectionArgs);
            Log.i(TAGS[mType], "markAllTelephonyPhoneAccountsPendingMigration count: " + count);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    /**
     * Set phone account migration pending status, indicating if there is any phone account handle
     * that need to migrate. Store the value in the SharedPreference to prevent the need to query
     * the database in the future for pending migration.
     */
    public void setPhoneAccountMigrationStatusPending(boolean status) {
        mSharedPreferences.edit().putBoolean(PENDING_STATUS_FIELDS[mType], status).apply();
    }

    /**
     * Checks phone account migration pending status, indicating if there is any phone account
     * handle that need to migrate. Query the value in the SharedPreference to prevent the need
     * to query the database in the future for pending migration.
     */
    public boolean isPhoneAccountMigrationPending() {
        return mSharedPreferences.getBoolean(PENDING_STATUS_FIELDS[mType], false);
    }

    /**
     * Updates phone account migration pending status, indicating if there is any phone account
     * handle that need to migrate.
     */
    public void updatePhoneAccountHandleMigrationPendingStatus(SQLiteDatabase sqLiteDatabase) {
        // Check to see if any entries need phone account migration pending.
        long count = DatabaseUtils.longForQuery(sqLiteDatabase, "SELECT COUNT(DISTINCT "
                + IDS[mType] + ") FROM " + TABLES[mType] + " WHERE "
                + PENDING_STATUS_FIELDS[mType] + " == 1", null);
        if (count > 0) {
            Log.i(TAGS[mType], "updatePhoneAccountHandleMigrationPendingStatus true");
            setPhoneAccountMigrationStatusPending(true);
        } else {
            Log.i(TAGS[mType], "updatePhoneAccountHandleMigrationPendingStatus false");
            setPhoneAccountMigrationStatusPending(false);
        }
    }

    /**
     * Migrate all the pending phone account handles based on the given iccId and subId.
     */
    public void migratePendingPhoneAccountHandles(String iccId, String subId, SQLiteDatabase db) {
        ContentValues valuesForPhoneAccountId = new ContentValues();
        valuesForPhoneAccountId.put(PHONE_ACCOUNT_ID_FIELDS[mType], subId);
        valuesForPhoneAccountId.put(PENDING_STATUS_FIELDS[mType], 0);
        String selection = PHONE_ACCOUNT_ID_FIELDS[mType] + " LIKE ? AND "
                + PENDING_STATUS_FIELDS[mType] + " = ?";
        String[] selectionArgs = {iccId, "1"};
        db.beginTransaction();
        try {
            int count = db.update(TABLES[mType], valuesForPhoneAccountId, selection, selectionArgs);
            Log.i(TAGS[mType], "migrated pending PhoneAccountHandle subId: " + subId
                    + " count: " + count);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        updatePhoneAccountHandleMigrationPendingStatus(db);
    }

    /**
     * Try to migrate any PhoneAccountId to SubId from IccId.
     */
    public void migrateIccIdToSubId(SQLiteDatabase db) {
        final HashMap<String, String> phoneAccountIdsMigrateNow = new HashMap<>();
        final Cursor phoneAccountIdsCursor = db.rawQuery(
                "SELECT DISTINCT " + PHONE_ACCOUNT_ID_FIELDS[mType] + " FROM " + TABLES[mType]
                        + " WHERE " + PENDING_STATUS_FIELDS[mType] + " = 1", null);

        try {
            List<SubscriptionInfo> subscriptionInfoList = mSubscriptionManager
                    .getAllSubscriptionInfoList();
            phoneAccountIdsCursor.moveToPosition(-1);
            while (phoneAccountIdsCursor.moveToNext()) {
                if (phoneAccountIdsCursor.isNull(0)) {
                    continue;
                }
                final String iccId = phoneAccountIdsCursor.getString(0);
                String subId = null;
                if (mSubscriptionManager != null) {
                    subId = getSubIdForIccId(iccId, subscriptionInfoList);
                }

                if (!TextUtils.isEmpty(iccId)) {
                    if (subId != null) {
                        // If there is already a subId that maps to the corresponding iccid
                        // from an old phone account handle, migrate to the new phone account
                        // handle with sub id without pending.
                        phoneAccountIdsMigrateNow.put(iccId, subId);
                        Log.i(TAGS[mType], "migrateIccIdToSubId(db): found subId: " + subId);
                    }
                }
            }
        } finally {
            phoneAccountIdsCursor.close();
        }
        // Migrate to the new phone account handle with its sub ID that is already available.
        for (Map.Entry<String, String> set : phoneAccountIdsMigrateNow.entrySet()) {
            migratePendingPhoneAccountHandles(set.getKey(), set.getValue(), db);
        }
    }

    // Return a subId that maps to the given iccId, or null if the subId is not available.
    private String getSubIdForIccId(String iccId, List<SubscriptionInfo> subscriptionInfoList) {
        for (SubscriptionInfo subscriptionInfo : subscriptionInfoList) {
            // Some old version callog would store phone account handle id with the IccId
            // string plus "F", and the getIccId() returns IccId string itself without "F",
            // so here need to use "startsWith" to match.
            if (iccId.startsWith(subscriptionInfo.getIccId())) {
                Log.i(TAGS[mType], "getSubIdForIccId: Found subscription ID to migrate: "
                        + subscriptionInfo.getSubscriptionId());
                return Integer.toString(subscriptionInfo.getSubscriptionId());
            }
        }
        return null;
    }
}