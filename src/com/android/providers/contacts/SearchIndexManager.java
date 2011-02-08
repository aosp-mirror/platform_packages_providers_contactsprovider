/*
 * Copyright (C) 2011 The Android Open Source Project
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

import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MimetypesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.SearchIndexColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract.Data;
import android.text.TextUtils;
import android.util.Log;

import java.util.HashSet;
import java.util.Set;

/**
 * Maintains a search index for comprehensive contact search.
 */
public class SearchIndexManager {

    private static final String TAG = "ContactsFTS";

    private static final class ContactIndexQuery {
        public static final String[] COLUMNS = {
                Data.CONTACT_ID,
                MimetypesColumns.MIMETYPE,
                Data.DATA1, Data.DATA2, Data.DATA3, Data.DATA4, Data.DATA5,
                Data.DATA6, Data.DATA7, Data.DATA8, Data.DATA9, Data.DATA10, Data.DATA11,
                Data.DATA12, Data.DATA13, Data.DATA14
        };

        public static final int MIMETYPE = 1;
    }

    public static class IndexBuilder {
        public static final int SEPARATOR_SPACE = 0;
        public static final int SEPARATOR_PARENTHESES = 1;
        public static final int SEPARATOR_SLASH = 2;
        public static final int SEPARATOR_COMMA = 3;

        private StringBuilder mSbContent = new StringBuilder();
        private StringBuilder mSbTokens = new StringBuilder();
        private StringBuilder mSbElementContent = new StringBuilder();
        private HashSet<String> mUniqueElements = new HashSet<String>();
        private Cursor mCursor;

        void setCursor(Cursor cursor) {
            this.mCursor = cursor;
        }

        void reset() {
            mSbContent.setLength(0);
            mSbTokens.setLength(0);
            mSbElementContent.setLength(0);
            mUniqueElements.clear();
        }

        public String getContent() {
            return mSbContent.length() == 0 ? null : mSbContent.toString();
        }

        public String getTokens() {
            return mSbTokens.length() == 0 ? null : mSbTokens.toString();
        }

        public String getString(String columnName) {
            return mCursor.getString(mCursor.getColumnIndex(columnName));
        }

        @Override
        public String toString() {
            return "Content: " + mSbContent + "\n Tokens: " + mSbTokens;
        }

        public void commit() {
            if (mSbElementContent.length() != 0) {
                String content = mSbElementContent.toString().replace('\n', ' ');
                if (!mUniqueElements.contains(content)) {
                    if (mSbContent.length() != 0) {
                        mSbContent.append('\n');
                    }
                    mSbContent.append(content);
                    mUniqueElements.add(content);
                }
                mSbElementContent.setLength(0);
            }
        }

        public void appendContentFromColumn(String columnName) {
            appendContentFromColumn(columnName, SEPARATOR_SPACE);
        }

        public void appendContentFromColumn(String columnName, int format) {
            appendContent(getString(columnName), format);
        }

        public void appendContent(String value) {
            appendContent(value, SEPARATOR_SPACE);
        }

        public void appendContent(String value, int format) {
            if (TextUtils.isEmpty(value)) {
                return;
            }

            switch (format) {
                case SEPARATOR_SPACE:
                    if (mSbElementContent.length() > 0) {
                        mSbElementContent.append(' ');
                    }
                    mSbElementContent.append(value);
                    break;

                case SEPARATOR_SLASH:
                    mSbElementContent.append('/').append(value);
                    break;

                case SEPARATOR_PARENTHESES:
                    if (mSbElementContent.length() > 0) {
                        mSbElementContent.append(' ');
                    }
                    mSbElementContent.append('(').append(value).append(')');
                    break;

                case SEPARATOR_COMMA:
                    if (mSbElementContent.length() > 0) {
                        mSbElementContent.append(", ");
                    }
                    mSbElementContent.append(value);
                    break;
            }
        }

        public void appendToken(String token) {
            if (mSbTokens.length() != 0) {
                mSbTokens.append(' ');
            }
            mSbTokens.append(token);
        }
    }

    private final ContactsProvider2 mContactsProvider;
    private final ContactsDatabaseHelper mDbHelper;
    private StringBuilder mSb = new StringBuilder();
    private IndexBuilder mIndexBuilder = new IndexBuilder();
    private ContentValues mValues = new ContentValues();
    private String[] mSelectionArgs1 = new String[1];

    public SearchIndexManager(ContactsProvider2 contactsProvider) {
        this.mContactsProvider = contactsProvider;
        mDbHelper = (ContactsDatabaseHelper) mContactsProvider.getDatabaseHelper();
    }

    public void updateIndexForRawContacts(Set<Long> rawContactIds) {
        mSb.setLength(0);
        mSb.append(Data.RAW_CONTACT_ID + " IN (");
        for (Long rawContactId : rawContactIds) {
            mSb.append(rawContactId).append(",");
        }
        mSb.setLength(mSb.length() - 1);
        mSb.append(')');

        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        Cursor cursor = db.query(Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS,
                ContactIndexQuery.COLUMNS, mSb.toString(), null, null, null,
                Data.CONTACT_ID + ", " + DataColumns.MIMETYPE_ID + ", " + Data.IS_SUPER_PRIMARY
                        + ", " + DataColumns.CONCRETE_ID);
        mIndexBuilder.setCursor(cursor);
        mIndexBuilder.reset();
        try {
            long currentContactId = -1;
            while (cursor.moveToNext()) {
                long contactId = cursor.getLong(0);
                if (contactId != currentContactId) {
                    if (currentContactId != -1) {
                        saveContactIndex(db, currentContactId, mIndexBuilder);
                    }
                    currentContactId = contactId;
                    mIndexBuilder.reset();
                }
                String mimetype = cursor.getString(ContactIndexQuery.MIMETYPE);
                DataRowHandler dataRowHandler = mContactsProvider.getDataRowHandler(mimetype);
                if (dataRowHandler.hasSearchableData()) {
                    dataRowHandler.appendSearchableData(mIndexBuilder);
                    mIndexBuilder.commit();
                }
            }
            if (currentContactId != -1) {
                saveContactIndex(db, currentContactId, mIndexBuilder);
            }
        } finally {
            cursor.close();
        }
    }

    private void saveContactIndex(SQLiteDatabase db, long contactId, IndexBuilder builder) {
        Log.d(TAG, "INDEX: " + contactId + ": " + builder.toString());

        mValues.clear();
        mValues.put(SearchIndexColumns.CONTENT, builder.getContent());
        mValues.put(SearchIndexColumns.TOKENS, builder.getTokens());
        mSelectionArgs1[0] = String.valueOf(contactId);
        int count = db.update(Tables.SEARCH_INDEX, mValues,
                SearchIndexColumns.CONTACT_ID + "=CAST(? AS int)", mSelectionArgs1);
        if (count == 0) {
            mValues.put(SearchIndexColumns.CONTACT_ID, contactId);
            db.insert(Tables.SEARCH_INDEX, null, mValues);
        }
    }
}
