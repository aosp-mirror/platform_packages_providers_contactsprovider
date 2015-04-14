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

package com.android.providers.contacts;

import android.text.TextUtils;
import com.android.providers.contacts.util.NeededForTesting;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

@NeededForTesting
public class MetadataEntryParser {

    private final static String UNIQUE_CONTACT_ID = "unique_contact_id";
    private final static String ACCOUNT_TYPE = "account_type";
    private final static String CUSTOM_ACCOUNT_TYPE = "custom_account_type";
    private final static String ENUM_VALUE_FOR_CUSTOM_ACCOUNT = "CUSTOM_ACCOUNT";
    private final static String ACCOUNT_NAME = "account_name";
    private final static String CONTACT_ID = "contact_id";
    private final static String CONTACT_PREFS = "contact_prefs";
    private final static String SEND_TO_VOICEMAIL = "send_to_voicemail";
    private final static String STARRED = "starred";
    private final static String PINNED = "pinned";
    private final static String AGGREGATION_DATA = "aggregation_data";
    private final static String CONTACT_IDS = "contact_ids";
    private final static String TYPE = "type";
    private final static String FIELD_DATA = "field_data";
    private final static String FIELD_DATA_ID = "field_data_id";
    private final static String FIELD_DATA_PREFS = "field_data_prefs";
    private final static String IS_PRIMARY = "is_primary";
    private final static String IS_SUPER_PRIMARY = "is_super_primary";
    private final static String USAGE_STATS = "usage_stats";
    private final static String USAGE_TYPE = "usage_type";
    private final static String LAST_TIME_USED = "last_time_used";
    private final static String USAGE_COUNT = "usage_count";

    @NeededForTesting
    public static class UsageStats {
        final String mUsageType;
        final long mLastTimeUsed;
        final int mTimesUsed;

        @NeededForTesting
        public UsageStats(String usageType, long lastTimeUsed, int timesUsed) {
            this.mUsageType = usageType;
            this.mLastTimeUsed = lastTimeUsed;
            this.mTimesUsed = timesUsed;
        }
    }

    @NeededForTesting
    public static class FieldData {
        final long mFieldDataId;
        final boolean mIsPrimary;
        final boolean mIsSuperPrimary;
        final ArrayList<UsageStats> mUsageStatsList;

        @NeededForTesting
        public FieldData(long fieldDataId, boolean isPrimary, boolean isSuperPrimary,
                ArrayList<UsageStats> usageStatsList) {
            this.mFieldDataId = fieldDataId;
            this.mIsPrimary = isPrimary;
            this.mIsSuperPrimary = isSuperPrimary;
            this.mUsageStatsList = usageStatsList;
        }
    }

    @NeededForTesting
    public static class AggregationData {
        final long mRawContactId1;
        final long mRawContactId2;
        final String mType;

        @NeededForTesting
        public AggregationData(long rawContactId1, long rawContactId2, String type) {
            this.mRawContactId1 = rawContactId1;
            this.mRawContactId2 = rawContactId2;
            this.mType = type;
        }
    }

    @NeededForTesting
    public static class MetadataEntry {
        final long mRawContactId;
        final String mAccountType;
        final String mAccountName;
        final int mSendToVoicemail;
        final int mStarred;
        final int mPinned;
        final ArrayList<FieldData> mFieldDatas;
        final ArrayList<AggregationData> mAggregationDatas;

        @NeededForTesting
        public MetadataEntry(long rawContactId, String accountType, String accountName,
                int sendToVoicemail, int starred, int pinned,
            ArrayList<FieldData> fieldDatas,
            ArrayList<AggregationData> aggregationDatas) {
            this.mRawContactId = rawContactId;
            this.mAccountType = accountType;
            this.mAccountName = accountName;
            this.mSendToVoicemail = sendToVoicemail;
            this.mStarred = starred;
            this.mPinned = pinned;
            this.mFieldDatas = fieldDatas;
            this.mAggregationDatas = aggregationDatas;
        }
    }

    @NeededForTesting
    static MetadataEntry parseDataToMetaDataEntry(String inputData) {
        if (TextUtils.isEmpty(inputData)) {
            throw new IllegalArgumentException("Input cannot be empty.");
        }

        try {
            final JSONObject root = new JSONObject(inputData);
            // Parse to get rawContactId and account info.
            final JSONObject uniqueContact = root.getJSONObject(UNIQUE_CONTACT_ID);
            final String rawContactId = uniqueContact.getString(CONTACT_ID);
            String accountType = uniqueContact.getString(ACCOUNT_TYPE);
            if (ENUM_VALUE_FOR_CUSTOM_ACCOUNT.equals(accountType)) {
                accountType = uniqueContact.getString(CUSTOM_ACCOUNT_TYPE);
            }
            final String accountName = uniqueContact.getString(ACCOUNT_NAME);
            if (TextUtils.isEmpty(rawContactId) || TextUtils.isEmpty(accountType)
                    || TextUtils.isEmpty(accountName)) {
                throw new IllegalArgumentException(
                        "contact_id, account_type, account_name cannot be empty.");
            }

            // Parse contactPrefs to get sendToVoicemail, starred, pinned.
            final JSONObject contactPrefs = root.getJSONObject(CONTACT_PREFS);
            final boolean sendToVoicemail = contactPrefs.getBoolean(SEND_TO_VOICEMAIL);
            final boolean starred = contactPrefs.getBoolean(STARRED);
            final int pinned = contactPrefs.getInt(PINNED);

            // Parse aggregationDatas
            final ArrayList<AggregationData> aggregationsList = new ArrayList<AggregationData>();
            if (root.has(AGGREGATION_DATA)) {
                final JSONArray aggregationDatas = root.getJSONArray(AGGREGATION_DATA);

                for (int i = 0; i < aggregationDatas.length(); i++) {
                    final JSONObject aggregationData = aggregationDatas.getJSONObject(i);
                    final JSONArray contacts = aggregationData.getJSONArray(CONTACT_IDS);

                    if (contacts.length() != 2) {
                        throw new IllegalArgumentException(
                                "There should be two contacts for each aggregation.");
                    }
                    final JSONObject rawContact1 = contacts.getJSONObject(0);
                    final long rawContactId1 = rawContact1.getLong(CONTACT_ID);
                    final JSONObject rawContact2 = contacts.getJSONObject(1);
                    final long rawContactId2 = rawContact2.getLong(CONTACT_ID);
                    final String type = aggregationData.getString(TYPE);
                    if (TextUtils.isEmpty(type)) {
                        throw new IllegalArgumentException("Aggregation type cannot be empty.");
                    }

                    final AggregationData aggregation = new AggregationData(
                            rawContactId1, rawContactId2, type);
                    aggregationsList.add(aggregation);
                }
            }

            // Parse fieldDatas
            final ArrayList<FieldData> fieldDatasList = new ArrayList<FieldData>();
            if (root.has(FIELD_DATA)) {
                final JSONArray fieldDatas = root.getJSONArray(FIELD_DATA);

                for (int i = 0; i < fieldDatas.length(); i++) {
                    final JSONObject fieldData = fieldDatas.getJSONObject(i);
                    final long fieldDataId = fieldData.getLong(FIELD_DATA_ID);
                    final JSONObject fieldDataPrefs = fieldData.getJSONObject(FIELD_DATA_PREFS);
                    final boolean isPrimary = fieldDataPrefs.getBoolean(IS_PRIMARY);
                    final boolean isSuperPrimary = fieldDataPrefs.getBoolean(IS_SUPER_PRIMARY);

                    final ArrayList<UsageStats> usageStatsList = new ArrayList<UsageStats>();
                    if (fieldData.has(USAGE_STATS)) {
                        final JSONArray usageStats = fieldData.getJSONArray(USAGE_STATS);
                        for (int j = 0; j < usageStats.length(); j++) {
                            final JSONObject usageStat = usageStats.getJSONObject(j);
                            final String usageType = usageStat.getString(USAGE_TYPE);
                            if (TextUtils.isEmpty(usageType)) {
                                throw new IllegalArgumentException("Usage type cannot be empty.");
                            }
                            final long lastTimeUsed = usageStat.getLong(LAST_TIME_USED);
                            final int usageCount = usageStat.getInt(USAGE_COUNT);

                            final UsageStats usageStatsParsed = new UsageStats(
                                    usageType, lastTimeUsed, usageCount);
                            usageStatsList.add(usageStatsParsed);
                        }
                    }

                    final FieldData fieldDataParse = new FieldData(fieldDataId, isPrimary,
                            isSuperPrimary, usageStatsList);
                    fieldDatasList.add(fieldDataParse);
                }
            }
            final MetadataEntry metaDataEntry = new MetadataEntry(Long.parseLong(rawContactId),
                    accountType, accountName, sendToVoicemail ? 1 : 0, starred ? 1 : 0, pinned,
                    fieldDatasList, aggregationsList);
            return metaDataEntry;
        } catch (JSONException e) {
            throw new IllegalArgumentException("JSON Exception.", e);
        }
    }
}
