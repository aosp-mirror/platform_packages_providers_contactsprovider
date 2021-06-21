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

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.text.TextUtils;

import com.android.providers.contacts.SearchIndexManager.IndexBuilder;
import com.android.providers.contacts.aggregation.AbstractContactAggregator;

/**
 * Handler for postal address data rows.
 */
public class DataRowHandlerForStructuredPostal extends DataRowHandler {

    /**
     * Specific list of structured fields.
     */
    private final String[] STRUCTURED_FIELDS = new String[] {
            StructuredPostal.STREET,
            StructuredPostal.POBOX,
            StructuredPostal.NEIGHBORHOOD,
            StructuredPostal.CITY,
            StructuredPostal.REGION,
            StructuredPostal.POSTCODE,
            StructuredPostal.COUNTRY,
    };

    private final PostalSplitter mSplitter;

    public DataRowHandlerForStructuredPostal(Context context, ContactsDatabaseHelper dbHelper,
            AbstractContactAggregator aggregator, PostalSplitter splitter) {
        super(context, dbHelper, aggregator, StructuredPostal.CONTENT_ITEM_TYPE);
        mSplitter = splitter;
    }

    @Override
    public long insert(SQLiteDatabase db, TransactionContext txContext, long rawContactId,
            ContentValues values) {
        fixStructuredPostalComponents(values, values);
        return super.insert(db, txContext, rawContactId, values);
    }

    @Override
    public boolean update(SQLiteDatabase db, TransactionContext txContext, ContentValues values,
            Cursor c, boolean callerIsSyncAdapter) {
        final long dataId = c.getLong(DataUpdateQuery._ID);
        final ContentValues augmented = getAugmentedValues(db, dataId, values);
        if (augmented == null) {    // No change
            return false;
        }

        fixStructuredPostalComponents(augmented, values);
        super.update(db, txContext, values, c, callerIsSyncAdapter);
        return true;
    }

    /**
     * Prepares the given {@link StructuredPostal} row, building
     * {@link StructuredPostal#FORMATTED_ADDRESS} to match the structured
     * values when missing. When structured components are missing, the
     * unstructured value is assigned to {@link StructuredPostal#STREET}.
     */
    private void fixStructuredPostalComponents(ContentValues augmented, ContentValues update) {
        final String unstruct = update.getAsString(StructuredPostal.FORMATTED_ADDRESS);

        final boolean touchedUnstruct = !TextUtils.isEmpty(unstruct);
        final boolean touchedStruct = !areAllEmpty(update, STRUCTURED_FIELDS);

        final PostalSplitter.Postal postal = new PostalSplitter.Postal();

        if (touchedUnstruct && !touchedStruct) {
            mSplitter.split(postal, unstruct);
            postal.toValues(update);
        } else if (!touchedUnstruct
                && (touchedStruct || areAnySpecified(update, STRUCTURED_FIELDS))) {
            postal.fromValues(augmented);
            final String joined = mSplitter.join(postal);
            update.put(StructuredPostal.FORMATTED_ADDRESS, joined);
        }
    }


    @Override
    public boolean hasSearchableData() {
        return true;
    }

    @Override
    public boolean containsSearchableColumns(ContentValues values) {
        return values.containsKey(StructuredPostal.FORMATTED_ADDRESS);
    }

    @Override
    public void appendSearchableData(IndexBuilder builder) {
        builder.appendContentFromColumn(StructuredPostal.FORMATTED_ADDRESS);
    }
}
