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
    private final static String ENUM_VALUE_FOR_GOOGLE_ACCOUNT = "GOOGLE_ACCOUNT";
    private final static String GOOGLE_ACCOUNT_TYPE = "com.google";
    private final static String ENUM_VALUE_FOR_CUSTOM_ACCOUNT = "CUSTOM_ACCOUNT";
    private final static String ACCOUNT_NAME = "account_name";
    private final static String DATA_SET = "data_set";
    private final static String ENUM_FOR_PLUS_DATA_SET = "GOOGLE_PLUS";
    private final static String ENUM_FOR_CUSTOM_DATA_SET = "CUSTOM";
    private final static String PLUS_DATA_SET_TYPE = "plus";
    private final static String CUSTOM_DATA_SET = "custom_data_set";
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
        final String mDataHashId;
        final boolean mIsPrimary;
        final boolean mIsSuperPrimary;
        final ArrayList<UsageStats> mUsageStatsList;

        @NeededForTesting
        public FieldData(String dataHashId, boolean isPrimary, boolean isSuperPrimary,
                ArrayList<UsageStats> usageStatsList) {
            this.mDataHashId = dataHashId;
            this.mIsPrimary = isPrimary;
            this.mIsSuperPrimary = isSuperPrimary;
            this.mUsageStatsList = usageStatsList;
        }
    }

    @NeededForTesting
    public static class RawContactInfo {
        final String mBackupId;
        final String mAccountType;
        final String mAccountName;
        final String mDataSet;

        @NeededForTesting
        public RawContactInfo(String backupId, String accountType, String accountName,
                String dataSet) {
            this.mBackupId = backupId;
            this.mAccountType = accountType;
            this.mAccountName = accountName;
            mDataSet = dataSet;
        }
    }

    @NeededForTesting
    public static class AggregationData {
        final RawContactInfo mRawContactInfo1;
        final RawContactInfo mRawContactInfo2;
        final String mType;

        @NeededForTesting
        public AggregationData(RawContactInfo rawContactInfo1, RawContactInfo rawContactInfo2,
                String type) {
            this.mRawContactInfo1 = rawContactInfo1;
            this.mRawContactInfo2 = rawContactInfo2;
            this.mType = type;
        }
    }

    @NeededForTesting
    public static class MetadataEntry {
        final RawContactInfo mRawContactInfo;
        final int mSendToVoicemail;
        final int mStarred;
        final int mPinned;
        final ArrayList<FieldData> mFieldDatas;
        final ArrayList<AggregationData> mAggregationDatas;

        @NeededForTesting
        public MetadataEntry(RawContactInfo rawContactInfo,
                int sendToVoicemail, int starred, int pinned,
                ArrayList<FieldData> fieldDatas,
                ArrayList<AggregationData> aggregationDatas) {
            this.mRawContactInfo = rawContactInfo;
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
            final JSONObject uniqueContactJSON = root.getJSONObject(UNIQUE_CONTACT_ID);
            final RawContactInfo rawContactInfo = parseUniqueContact(uniqueContactJSON);

            // Parse contactPrefs to get sendToVoicemail, starred, pinned.
            final JSONObject contactPrefs = root.getJSONObject(CONTACT_PREFS);
            final boolean sendToVoicemail = contactPrefs.has(SEND_TO_VOICEMAIL)
                    ? contactPrefs.getBoolean(SEND_TO_VOICEMAIL) : false;
            final boolean starred = contactPrefs.has(STARRED)
                    ? contactPrefs.getBoolean(STARRED) : false;
            final int pinned = contactPrefs.has(PINNED) ? contactPrefs.getInt(PINNED) : 0;

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
                    final RawContactInfo aggregationContact1 = parseUniqueContact(rawContact1);
                    final JSONObject rawContact2 = contacts.getJSONObject(1);
                    final RawContactInfo aggregationContact2 = parseUniqueContact(rawContact2);
                    final String type = aggregationData.getString(TYPE);
                    if (TextUtils.isEmpty(type)) {
                        throw new IllegalArgumentException("Aggregation type cannot be empty.");
                    }

                    final AggregationData aggregation = new AggregationData(
                            aggregationContact1, aggregationContact2, type);
                    aggregationsList.add(aggregation);
                }
            }

            // Parse fieldDatas
            final ArrayList<FieldData> fieldDatasList = new ArrayList<FieldData>();
            if (root.has(FIELD_DATA)) {
                final JSONArray fieldDatas = root.getJSONArray(FIELD_DATA);

                for (int i = 0; i < fieldDatas.length(); i++) {
                    final JSONObject fieldData = fieldDatas.getJSONObject(i);
                    final String dataHashId = fieldData.getString(FIELD_DATA_ID);
                    if (TextUtils.isEmpty(dataHashId)) {
                        throw new IllegalArgumentException("Field data hash id cannot be empty.");
                    }
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

                    final FieldData fieldDataParse = new FieldData(dataHashId, isPrimary,
                            isSuperPrimary, usageStatsList);
                    fieldDatasList.add(fieldDataParse);
                }
            }
            final MetadataEntry metaDataEntry = new MetadataEntry(rawContactInfo,
                    sendToVoicemail ? 1 : 0, starred ? 1 : 0, pinned,
                    fieldDatasList, aggregationsList);
            return metaDataEntry;
        } catch (JSONException e) {
            throw new IllegalArgumentException("JSON Exception.", e);
        }
    }

    private static RawContactInfo parseUniqueContact(JSONObject uniqueContactJSON) {
        try {
            final String backupId = uniqueContactJSON.getString(CONTACT_ID);
            final String accountName = uniqueContactJSON.getString(ACCOUNT_NAME);
            String accountType = uniqueContactJSON.getString(ACCOUNT_TYPE);
            if (ENUM_VALUE_FOR_GOOGLE_ACCOUNT.equals(accountType)) {
                accountType = GOOGLE_ACCOUNT_TYPE;
            } else if (ENUM_VALUE_FOR_CUSTOM_ACCOUNT.equals(accountType)) {
                accountType = uniqueContactJSON.getString(CUSTOM_ACCOUNT_TYPE);
            } else {
                throw new IllegalArgumentException("Unknown account type.");
            }

            String dataSet = null;
            switch (uniqueContactJSON.getString(DATA_SET)) {
                case ENUM_FOR_PLUS_DATA_SET:
                    dataSet = PLUS_DATA_SET_TYPE;
                    break;
                case ENUM_FOR_CUSTOM_DATA_SET:
                    dataSet = uniqueContactJSON.getString(CUSTOM_DATA_SET);
                    break;
            }
            if (TextUtils.isEmpty(backupId) || TextUtils.isEmpty(accountType)
                    || TextUtils.isEmpty(accountName)) {
                throw new IllegalArgumentException(
                        "Contact backup id, account type, account name cannot be empty.");
            }
            final RawContactInfo rawContactInfo = new RawContactInfo(
                    backupId, accountType, accountName, dataSet);
            return rawContactInfo;
        } catch (JSONException e) {
            throw new IllegalArgumentException("JSON Exception.", e);
        }
    }
}
