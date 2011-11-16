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
import android.os.SystemClock;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.ProviderStatus;
import android.text.TextUtils;
import android.util.Log;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Maintains a search index for comprehensive contact search.
 */
public class SearchIndexManager {
    private static final String TAG = "ContactsFTS";

    public static final String PROPERTY_SEARCH_INDEX_VERSION = "search_index";
    private static final int SEARCH_INDEX_VERSION = 1;

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
        private StringBuilder mSbName = new StringBuilder();
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
            mSbName.setLength(0);
            mSbElementContent.setLength(0);
            mUniqueElements.clear();
        }

        public String getContent() {
            return mSbContent.length() == 0 ? null : mSbContent.toString();
        }

        public String getName() {
            return mSbName.length() == 0 ? null : mSbName.toString();
        }

        public String getTokens() {
            return mSbTokens.length() == 0 ? null : mSbTokens.toString();
        }

        public String getString(String columnName) {
            return mCursor.getString(mCursor.getColumnIndex(columnName));
        }

        public int getInt(String columnName) {
            return mCursor.getInt(mCursor.getColumnIndex(columnName));
        }

        @Override
        public String toString() {
            return "Content: " + mSbContent + "\n Name: " + mSbTokens + "\n Tokens: " + mSbTokens;
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
            if (TextUtils.isEmpty(token)) {
                return;
            }

            if (mSbTokens.length() != 0) {
                mSbTokens.append(' ');
            }
            mSbTokens.append(token);
        }

        private static final Pattern PATTERN_HYPHEN = Pattern.compile("\\-");

        public void appendName(String name) {
            if (TextUtils.isEmpty(name)) {
                return;
            }
            if (name.indexOf('-') < 0) {
                // Common case -- no hyphens in it.
                appendNameInternal(name);
            } else {
                // In order to make hyphenated names searchable, let's split names with '-'.
                for (String namePart : PATTERN_HYPHEN.split(name)) {
                    if (!TextUtils.isEmpty(namePart)) {
                        appendNameInternal(namePart);
                    }
                }
            }
        }

        private void appendNameInternal(String name) {
            if (mSbName.length() != 0) {
                mSbName.append(' ');
            }
            mSbName.append(NameNormalizer.normalize(name));
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

    public void updateIndex() {
        if (getSearchIndexVersion() == SEARCH_INDEX_VERSION) {
            return;
        }
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            if (getSearchIndexVersion() != SEARCH_INDEX_VERSION) {
                rebuildIndex(db);
                setSearchIndexVersion(SEARCH_INDEX_VERSION);
                db.setTransactionSuccessful();
            }
        } finally {
            db.endTransaction();
        }
    }

    private void rebuildIndex(SQLiteDatabase db) {
        mContactsProvider.setProviderStatus(ProviderStatus.STATUS_UPGRADING);
        long start = SystemClock.currentThreadTimeMillis();
        int count = 0;
        try {
            mDbHelper.createSearchIndexTable(db);
            count = buildIndex(db, null, false);
        } finally {
            mContactsProvider.setProviderStatus(ProviderStatus.STATUS_NORMAL);

            long end = SystemClock.currentThreadTimeMillis();
            Log.i(TAG, "Rebuild contact search index in " + (end - start) + "ms, "
                    + count + " contacts");
        }
    }

    public void updateIndexForRawContacts(Set<Long> contactIds, Set<Long> rawContactIds) {
        mSb.setLength(0);
        mSb.append("(");
        if (!contactIds.isEmpty()) {
            mSb.append(Data.CONTACT_ID + " IN (");
            for (Long contactId : contactIds) {
                mSb.append(contactId).append(",");
            }
            mSb.setLength(mSb.length() - 1);
            mSb.append(')');
        }

        if (!rawContactIds.isEmpty()) {
            if (!contactIds.isEmpty()) {
                mSb.append(" OR ");
            }
            mSb.append(Data.RAW_CONTACT_ID + " IN (");
            for (Long rawContactId : rawContactIds) {
                mSb.append(rawContactId).append(",");
            }
            mSb.setLength(mSb.length() - 1);
            mSb.append(')');
        }

        mSb.append(")");
        buildIndex(mDbHelper.getWritableDatabase(), mSb.toString(), true);
    }

    private int buildIndex(SQLiteDatabase db, String selection, boolean replace) {
        mSb.setLength(0);
        mSb.append(Data.CONTACT_ID + ", ");
        mSb.append("(CASE WHEN " + DataColumns.MIMETYPE_ID + "=");
        mSb.append(mDbHelper.getMimeTypeId(Nickname.CONTENT_ITEM_TYPE));
        mSb.append(" THEN -4 ");
        mSb.append(" WHEN " + DataColumns.MIMETYPE_ID + "=");
        mSb.append(mDbHelper.getMimeTypeId(Organization.CONTENT_ITEM_TYPE));
        mSb.append(" THEN -3 ");
        mSb.append(" WHEN " + DataColumns.MIMETYPE_ID + "=");
        mSb.append(mDbHelper.getMimeTypeId(StructuredPostal.CONTENT_ITEM_TYPE));
        mSb.append(" THEN -2");
        mSb.append(" WHEN " + DataColumns.MIMETYPE_ID + "=");
        mSb.append(mDbHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE));
        mSb.append(" THEN -1");
        mSb.append(" ELSE " + DataColumns.MIMETYPE_ID);
        mSb.append(" END), " + Data.IS_SUPER_PRIMARY + ", " + DataColumns.CONCRETE_ID);

        int count = 0;
        Cursor cursor = db.query(Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS, ContactIndexQuery.COLUMNS,
                selection, null, null, null, mSb.toString());
        mIndexBuilder.setCursor(cursor);
        mIndexBuilder.reset();
        try {
            long currentContactId = -1;
            while (cursor.moveToNext()) {
                long contactId = cursor.getLong(0);
                if (contactId != currentContactId) {
                    if (currentContactId != -1) {
                        saveContactIndex(db, currentContactId, mIndexBuilder, replace);
                        count++;
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
                saveContactIndex(db, currentContactId, mIndexBuilder, replace);
                count++;
            }
        } finally {
            cursor.close();
        }
        return count;
    }

    private void saveContactIndex(
            SQLiteDatabase db, long contactId, IndexBuilder builder, boolean replace) {
        mValues.clear();
        mValues.put(SearchIndexColumns.CONTENT, builder.getContent());
        mValues.put(SearchIndexColumns.NAME, builder.getName());
        mValues.put(SearchIndexColumns.TOKENS, builder.getTokens());
        if (replace) {
            mSelectionArgs1[0] = String.valueOf(contactId);
            int count = db.update(Tables.SEARCH_INDEX, mValues,
                    SearchIndexColumns.CONTACT_ID + "=CAST(? AS int)", mSelectionArgs1);
            if (count == 0) {
                mValues.put(SearchIndexColumns.CONTACT_ID, contactId);
                db.insert(Tables.SEARCH_INDEX, null, mValues);
            }
        } else {
            mValues.put(SearchIndexColumns.CONTACT_ID, contactId);
            db.insert(Tables.SEARCH_INDEX, null, mValues);
        }
    }
    private int getSearchIndexVersion() {
        return Integer.parseInt(mDbHelper.getProperty(PROPERTY_SEARCH_INDEX_VERSION, "0"));
    }

    private void setSearchIndexVersion(int version) {
        mDbHelper.setProperty(PROPERTY_SEARCH_INDEX_VERSION, String.valueOf(version));
    }

    /**
     * Tokenizes the query and normalizes/hex encodes each token. The tokenizer uses the same
     * rules as SQLite's "simple" tokenizer. Each token is added to the retokenizer and then
     * returned as a String.
     * @see FtsQueryBuilder#UNSCOPED_NORMALIZING
     * @see FtsQueryBuilder#SCOPED_NAME_NORMALIZING
     */
    public static String getFtsMatchQuery(String query, FtsQueryBuilder ftsQueryBuilder) {
        // SQLite's "simple" tokenizer uses the following rules to detect characters:
        //  - Unicode codepoints >= 128: Everything
        //  - Unicode codepoints < 128: Alphanumeric and "_"
        // Everything else is a separator of tokens
        int tokenStart = -1;
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i <= query.length(); i++) {
            final boolean isChar;
            if (i == query.length()) {
                isChar = false;
            } else {
                final char ch = query.charAt(i);
                if (ch >= 128) {
                    isChar = true;
                } else {
                    isChar = Character.isLetterOrDigit(ch) || ch == '_';
                }
            }
            if (isChar) {
                if (tokenStart == -1) {
                    tokenStart = i;
                }
            } else {
                if (tokenStart != -1) {
                    final String token = query.substring(tokenStart, i);
                    ftsQueryBuilder.addToken(result, token);
                    tokenStart = -1;
                }
            }
        }
        return result.toString();
    }

    public static abstract class FtsQueryBuilder {
        public abstract void addToken(StringBuilder builder, String token);

        /** Normalizes and space-concatenates each token. Example: "a1b2c1* a2b3c2*" */
        public static final FtsQueryBuilder UNSCOPED_NORMALIZING = new UnscopedNormalizingBuilder();

        /**
         * Scopes each token to a column and normalizes the name.
         * Example: "content:foo* name:a1b2c1* tokens:foo* content:bar* name:a2b3c2* tokens:bar*"
         */
        public static final FtsQueryBuilder SCOPED_NAME_NORMALIZING =
                new ScopedNameNormalizingBuilder();

        /**
         * Scopes each token to a the content column and also for name with normalization.
         * Also adds a user-defined expression to each token. This allows common criteria to be
         * concatenated to each token.
         * Example (commonCriteria=" OR tokens:123*"):
         * "content:650* OR name:1A1B1C* OR tokens:123* content:2A2B2C* OR name:foo* OR tokens:123*"
         */
        public static FtsQueryBuilder getDigitsQueryBuilder(final String commonCriteria) {
            return new FtsQueryBuilder() {
                @Override
                public void addToken(StringBuilder builder, String token) {
                    if (builder.length() != 0) builder.append(' ');

                    builder.append("content:");
                    builder.append(token);
                    builder.append("* ");

                    final String normalizedToken = NameNormalizer.normalize(token);
                    if (!TextUtils.isEmpty(normalizedToken)) {
                        builder.append(" OR name:");
                        builder.append(normalizedToken);
                        builder.append('*');
                    }

                    builder.append(commonCriteria);
                }
            };
        }
    }

    private static class UnscopedNormalizingBuilder extends FtsQueryBuilder {
        @Override
        public void addToken(StringBuilder builder, String token) {
            if (builder.length() != 0) builder.append(' ');

            // the token could be empty (if the search query was "_"). we should still emit it
            // here, as we otherwise risk to end up with an empty MATCH-expression MATCH ""
            builder.append(NameNormalizer.normalize(token));
            builder.append('*');
        }
    }

    private static class ScopedNameNormalizingBuilder extends FtsQueryBuilder {
        @Override
        public void addToken(StringBuilder builder, String token) {
            if (builder.length() != 0) builder.append(' ');

            builder.append("content:");
            builder.append(token);
            builder.append('*');

            final String normalizedToken = NameNormalizer.normalize(token);
            if (!TextUtils.isEmpty(normalizedToken)) {
                builder.append(" OR name:");
                builder.append(normalizedToken);
                builder.append('*');
            }

            builder.append(" OR tokens:");
            builder.append(token);
            builder.append("*");
        }
    }
}
