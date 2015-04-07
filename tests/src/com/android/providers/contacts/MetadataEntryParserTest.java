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
 * limitations under the License.
 */

package com.android.providers.contacts;

import android.content.Context;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;
import com.android.providers.contacts.MetadataEntryParser.AggregationData;
import com.android.providers.contacts.MetadataEntryParser.FieldData;
import com.android.providers.contacts.MetadataEntryParser.MetadataEntry;
import com.android.providers.contacts.MetadataEntryParser.UsageStats;
import org.json.JSONException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Unit tests for {@link MetadataEntryParser}.
 *
 * Run the test like this:
 * <code>
 adb shell am instrument -e class com.android.providers.contacts.MetadataEntryParserTest -w \
         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@SmallTest
public class MetadataEntryParserTest extends AndroidTestCase {

    public void testErrorForEmptyInput() {
        try {
            MetadataEntryParser.parseDataToMetaDataEntry("");
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testParseDataToMetadataEntry() throws IOException {
        long rawContactId = 1111111;
        String accountType = "facebook";
        String accountName = "android-test";
        int sendToVoicemail = 1;
        int starred = 0;
        int pinned = 2;
        long fieldDataId1 = 1001;
        String usageType1_1 = "CALL";
        long lastTimeUsed1_1 = 10000001;
        int timesUsed1_1 = 10;
        String usageType1_2 = "SHORT_TEXT";
        long lastTimeUsed1_2 = 20000002;
        int timesUsed1_2 = 20;
        long fieldDataId2 = 1002;
        String usageType2 = "LONG_TEXT";
        long lastTimeUsed2 = 30000003;
        int timesUsed2 = 30;
        long aggregationContact1 = 2222222;
        long aggregationContact2 = 3333333;
        String type = "TOGETHER";
        String inputFile = "test1/testFileDeviceContactMetadataJSON.txt";

        AggregationData aggregationData = new AggregationData(
                aggregationContact1, aggregationContact2, type);
        ArrayList<AggregationData> aggregationDataList = new ArrayList<>();
        aggregationDataList.add(aggregationData);

        UsageStats usageStats1_1 = new UsageStats(usageType1_1, lastTimeUsed1_1, timesUsed1_1);
        UsageStats usageStats1_2 = new UsageStats(usageType1_2, lastTimeUsed1_2, timesUsed1_2);
        UsageStats usageStats2 = new UsageStats(usageType2, lastTimeUsed2, timesUsed2);

        ArrayList<UsageStats> usageStats1List = new ArrayList<>();
        usageStats1List.add(usageStats1_1);
        usageStats1List.add(usageStats1_2);
        FieldData fieldData1 = new FieldData(fieldDataId1, true, true, usageStats1List);

        ArrayList<UsageStats> usageStats2List = new ArrayList<>();
        usageStats2List.add(usageStats2);
        FieldData fieldData2 = new FieldData(fieldDataId2, false, false, usageStats2List);

        ArrayList<FieldData> fieldDataList = new ArrayList<>();
        fieldDataList.add(fieldData1);
        fieldDataList.add(fieldData2);

        MetadataEntry expectedResult = new MetadataEntry(rawContactId, accountType, accountName,
                sendToVoicemail, starred, pinned, fieldDataList, aggregationDataList);

        String inputJson = readAssetAsString(inputFile);
        MetadataEntry metadataEntry = MetadataEntryParser.parseDataToMetaDataEntry(
                inputJson.toString());
        assertMetaDataEntry(expectedResult, metadataEntry);
    }

    public void testErrorForMissingContactId() {
        String input = "{\"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"android-test\"\n" +
                "  }}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testErrorForNullContactId() throws JSONException {
        String input = "{\"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"android-test\",\n" +
                "    \"contact_id\": \"\"\n" +
                "  }}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testErrorForNullAccountType() throws JSONException {
        String input = "{\"unique_contact_id\": {\n" +
                "    \"account_type\": \"\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"android-test\",\n" +
                "    \"contact_id\": \"\"\n" +
                "  }}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testErrorForNullAccountName() throws JSONException {
        String input = "{\"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"\",\n" +
                "    \"contact_id\": \"1111111\"\n" +
                "  }}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testErrorForNullFieldDataId() throws JSONException {
        String input = "{\"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"android-test\",\n" +
                "    \"contact_id\": \"1111111\"\n" +
                "  },\n" +
                "    \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 2\n" +
                "  }," +
                "    \"field_data\": [{\n" +
                "      \"field_data_id\": \"\"}]" +
                "}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    public void testErrorForNullAggregationType() throws JSONException {
        String input = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": \"facebook\",\n" +
                "    \"account_name\": \"android-test\",\n" +
                "    \"contact_id\": \"1111111\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": false,\n" +
                "    \"pinned\": 2\n" +
                "  },\n" +
                "  \"aggregation_data\": [\n" +
                "    {\n" +
                "      \"type\": \"\",\n" +
                "      \"contact_ids\": [\n" +
                "        {\n" +
                "          \"contact_id\": \"2222222\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"contact_id\": \"3333333\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]}";
        try {
            MetadataEntryParser.parseDataToMetaDataEntry(input);
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    private String readAssetAsString(String fileName) throws IOException {
        Context context = getTestContext();
        InputStream input = context.getAssets().open(fileName);
        ByteArrayOutputStream contents = new ByteArrayOutputStream();
        int len;
        byte[] data = new byte[1024];
        do {
            len = input.read(data);
            if (len > 0) contents.write(data, 0, len);
        } while (len == data.length);
        return contents.toString();
    }

    private void assertMetaDataEntry(MetadataEntry entry1, MetadataEntry entry2) {
        assertEquals(entry1.mRawContactId, entry2.mRawContactId);
        assertEquals(entry1.mAccountType, entry2.mAccountType);
        assertEquals(entry1.mAccountName, entry2.mAccountName);
        assertEquals(entry1.mSendToVoicemail, entry2.mSendToVoicemail);
        assertEquals(entry1.mStarred, entry2.mStarred);
        assertEquals(entry1.mPinned, entry2.mPinned);
        assertAggregationDataListEquals(entry1.mAggregationDatas, entry2.mAggregationDatas);
        assertFieldDataListEquals(entry1.mFieldDatas, entry2.mFieldDatas);
    }

    private void assertAggregationDataListEquals(ArrayList<AggregationData> aggregationList1,
            ArrayList<AggregationData> aggregationList2) {
        assertEquals(aggregationList1.size(), aggregationList2.size());
        for (int i = 0; i < aggregationList1.size(); i++) {
            assertAggregationDataEquals(aggregationList1.get(i), aggregationList2.get(i));
        }
    }

    private void assertAggregationDataEquals(AggregationData aggregationData1,
            AggregationData aggregationData2) {
        assertEquals(aggregationData1.mRawContactId1, aggregationData2.mRawContactId1);
        assertEquals(aggregationData1.mRawContactId2, aggregationData2.mRawContactId2);
        assertEquals(aggregationData1.mType, aggregationData2.mType);
    }

    private void assertFieldDataListEquals(ArrayList<FieldData> fieldDataList1,
            ArrayList<FieldData> fieldDataList2) {
        assertEquals(fieldDataList1.size(), fieldDataList2.size());
        for (int i = 0; i < fieldDataList1.size(); i++) {
            assertFieldDataEquals(fieldDataList1.get(i), fieldDataList2.get(i));
        }
    }

    private void assertFieldDataEquals(FieldData fieldData1, FieldData fieldData2) {
        assertEquals(fieldData1.mFieldDataId, fieldData2.mFieldDataId);
        assertEquals(fieldData1.mIsPrimary, fieldData2.mIsPrimary);
        assertEquals(fieldData1.mIsSuperPrimary, fieldData2.mIsSuperPrimary);
        assertUsageStatsListEquals(fieldData1.mUsageStatsList, fieldData2.mUsageStatsList);
    }

    private void assertUsageStatsListEquals(ArrayList<UsageStats> usageStatsList1,
            ArrayList<UsageStats> usageStatsList2) {
        assertEquals(usageStatsList1.size(), usageStatsList2.size());
        for (int i = 0; i < usageStatsList1.size(); i++) {
            assertUsageStatsEquals(usageStatsList1.get(i), usageStatsList2.get(i));
        }
    }

    private void assertUsageStatsEquals(UsageStats usageStats1, UsageStats usageStats2) {
        assertEquals(usageStats1.mUsageType, usageStats2.mUsageType);
        assertEquals(usageStats1.mLastTimeUsed, usageStats2.mLastTimeUsed);
        assertEquals(usageStats1.mTimesUsed, usageStats2.mTimesUsed);
    }
}
