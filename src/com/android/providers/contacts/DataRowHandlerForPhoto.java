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
import android.provider.ContactsContract.CommonDataKinds.Photo;

/**
 * Handler for photo data rows.
 */
public class DataRowHandlerForPhoto extends DataRowHandler {

    public DataRowHandlerForPhoto(
            Context context, ContactsDatabaseHelper dbHelper, ContactAggregator aggregator) {
        super(context, dbHelper, aggregator, Photo.CONTENT_ITEM_TYPE);
    }

    @Override
    public long insert(SQLiteDatabase db, TransactionContext txContext, long rawContactId,
            ContentValues values) {
        long dataId = super.insert(db, txContext, rawContactId, values);
        if (!txContext.isNewRawContact(rawContactId)) {
            mContactAggregator.updatePhotoId(db, rawContactId);
        }
        return dataId;
    }

    @Override
    public boolean update(SQLiteDatabase db, TransactionContext txContext, ContentValues values,
            Cursor c, boolean callerIsSyncAdapter) {
        long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
        if (!super.update(db, txContext, values, c, callerIsSyncAdapter)) {
            return false;
        }

        mContactAggregator.updatePhotoId(db, rawContactId);
        return true;
    }

    @Override
    public int delete(SQLiteDatabase db, TransactionContext txContext, Cursor c) {
        long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
        int count = super.delete(db, txContext, c);
        mContactAggregator.updatePhotoId(db, rawContactId);
        return count;
    }
}
