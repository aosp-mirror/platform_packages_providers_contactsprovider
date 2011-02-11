/*
 * Copyright (C) 2010 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License
 */
package com.android.providers.contacts;

import com.android.providers.contacts.ContactsDatabaseHelper.PhoneColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.SearchIndexManager.IndexBuilder;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;

/**
 * Handler for phone number data rows.
 */
public class DataRowHandlerForPhoneNumber extends DataRowHandlerForCommonDataKind {

    public DataRowHandlerForPhoneNumber(Context context,
            ContactsDatabaseHelper dbHelper, ContactAggregator aggregator) {
        super(context, dbHelper, aggregator, Phone.CONTENT_ITEM_TYPE, Phone.TYPE, Phone.LABEL);
    }

    @Override
    public long insert(SQLiteDatabase db, TransactionContext txContext, long rawContactId,
            ContentValues values) {
        long dataId;
        if (values.containsKey(Phone.NUMBER)) {
            String number = values.getAsString(Phone.NUMBER);

            String numberE164 =
                    PhoneNumberUtils.formatNumberToE164(number, mDbHelper.getCurrentCountryIso());
            if (numberE164 != null) {
                values.put(PhoneColumns.NORMALIZED_NUMBER, numberE164);
            }
            dataId = super.insert(db, txContext, rawContactId, values);

            updatePhoneLookup(db, rawContactId, dataId, number, numberE164);
            mContactAggregator.updateHasPhoneNumber(db, rawContactId);
            fixRawContactDisplayName(db, txContext, rawContactId);
            if (numberE164 != null) {
                triggerAggregation(txContext, rawContactId);
            }
        } else {
            dataId = super.insert(db, txContext, rawContactId, values);
        }
        return dataId;
    }

    @Override
    public boolean update(SQLiteDatabase db, TransactionContext txContext, ContentValues values,
            Cursor c, boolean callerIsSyncAdapter) {
        String number = null;
        String numberE164 = null;
        if (values.containsKey(Phone.NUMBER)) {
            number = values.getAsString(Phone.NUMBER);
            if (number != null) {
                numberE164 = PhoneNumberUtils.formatNumberToE164(number,
                        mDbHelper.getCurrentCountryIso());
            }
            if (numberE164 != null) {
                values.put(PhoneColumns.NORMALIZED_NUMBER, numberE164);
            }
        }

        if (!super.update(db, txContext, values, c, callerIsSyncAdapter)) {
            return false;
        }

        if (values.containsKey(Phone.NUMBER)) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            updatePhoneLookup(db, rawContactId, dataId, number, numberE164);
            mContactAggregator.updateHasPhoneNumber(db, rawContactId);
            fixRawContactDisplayName(db, txContext, rawContactId);
            triggerAggregation(txContext, rawContactId);
        }
        return true;
    }

    @Override
    public int delete(SQLiteDatabase db, TransactionContext txContext, Cursor c) {
        long dataId = c.getLong(DataDeleteQuery._ID);
        long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

        int count = super.delete(db, txContext, c);

        updatePhoneLookup(db, rawContactId, dataId, null, null);
        mContactAggregator.updateHasPhoneNumber(db, rawContactId);
        fixRawContactDisplayName(db, txContext, rawContactId);
        triggerAggregation(txContext, rawContactId);
        return count;
    }

    private void updatePhoneLookup(SQLiteDatabase db, long rawContactId, long dataId,
            String number, String numberE164) {
        mSelectionArgs1[0] = String.valueOf(dataId);
        db.delete(Tables.PHONE_LOOKUP, PhoneLookupColumns.DATA_ID + "=?", mSelectionArgs1);
        if (number != null) {
            String normalizedNumber = PhoneNumberUtils.normalizeNumber(number);
            if (!TextUtils.isEmpty(normalizedNumber)) {
                ContentValues phoneValues = new ContentValues();
                phoneValues.put(PhoneLookupColumns.RAW_CONTACT_ID, rawContactId);
                phoneValues.put(PhoneLookupColumns.DATA_ID, dataId);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER, normalizedNumber);
                phoneValues.put(PhoneLookupColumns.MIN_MATCH,
                        PhoneNumberUtils.toCallerIDMinMatch(normalizedNumber));
                db.insert(Tables.PHONE_LOOKUP, null, phoneValues);

                if (numberE164 != null && !numberE164.equals(normalizedNumber)) {
                    phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER, numberE164);
                    phoneValues.put(PhoneLookupColumns.MIN_MATCH,
                            PhoneNumberUtils.toCallerIDMinMatch(numberE164));
                    db.insert(Tables.PHONE_LOOKUP, null, phoneValues);
                }
            }
        }
    }

    @Override
    protected int getTypeRank(int type) {
        switch (type) {
            case Phone.TYPE_MOBILE: return 0;
            case Phone.TYPE_WORK: return 1;
            case Phone.TYPE_HOME: return 2;
            case Phone.TYPE_PAGER: return 3;
            case Phone.TYPE_CUSTOM: return 4;
            case Phone.TYPE_OTHER: return 5;
            case Phone.TYPE_FAX_WORK: return 6;
            case Phone.TYPE_FAX_HOME: return 7;
            default: return 1000;
        }
    }

    @Override
    public boolean containsSearchableColumns(ContentValues values) {
        return values.containsKey(Phone.NUMBER);
    }

    @Override
    public void appendSearchableData(IndexBuilder builder) {
        String number = builder.getString(Phone.NUMBER);
        if (TextUtils.isEmpty(number)) {
            return;
        }

        String normalizedNumber = PhoneNumberUtils.normalizeNumber(number);
        if (TextUtils.isEmpty(normalizedNumber)) {
            return;
        }

        builder.appendToken(normalizedNumber);

        String numberE164 = PhoneNumberUtils.formatNumberToE164(
                number, mDbHelper.getCurrentCountryIso());
        if (numberE164 != null && !numberE164.equals(normalizedNumber)) {
            builder.appendToken(numberE164);
        }
    }
}
