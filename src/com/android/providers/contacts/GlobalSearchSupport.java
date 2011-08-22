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

import android.app.SearchManager;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.SearchSnippetColumns;
import android.provider.ContactsContract.StatusUpdates;
import android.text.TextUtils;

import com.android.i18n.phonenumbers.PhoneNumberUtil;
import com.android.i18n.phonenumbers.NumberParseException;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.ContactsDatabaseHelper.Views;

import java.util.ArrayList;
import java.util.Locale;

/**
 * Support for global search integration for Contacts.
 */
public class GlobalSearchSupport {

    private static final String[] SEARCH_SUGGESTIONS_BASED_ON_PHONE_NUMBER_COLUMNS = {
            "_id",
            SearchManager.SUGGEST_COLUMN_TEXT_1,
            SearchManager.SUGGEST_COLUMN_TEXT_2,
            SearchManager.SUGGEST_COLUMN_ICON_1,
            SearchManager.SUGGEST_COLUMN_INTENT_DATA,
            SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
            SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
    };

    private static final String[] SEARCH_SUGGESTIONS_BASED_ON_NAME_COLUMNS = {
            "_id",
            SearchManager.SUGGEST_COLUMN_TEXT_1,
            SearchManager.SUGGEST_COLUMN_TEXT_2,
            SearchManager.SUGGEST_COLUMN_ICON_1,
            SearchManager.SUGGEST_COLUMN_ICON_2,
            SearchManager.SUGGEST_COLUMN_INTENT_DATA,
            SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
            SearchManager.SUGGEST_COLUMN_INTENT_EXTRA_DATA,
            SearchManager.SUGGEST_COLUMN_LAST_ACCESS_HINT,
    };

    private static final char SNIPPET_START_MATCH = '\u0001';
    private static final char SNIPPET_END_MATCH = '\u0001';
    private static final String SNIPPET_ELLIPSIS = "\u2026";
    private static final int SNIPPET_MAX_TOKENS = 5;

    private static final String PRESENCE_SQL =
        "(SELECT " + StatusUpdates.PRESENCE +
        " FROM " + Tables.AGGREGATED_PRESENCE +
        " WHERE " + AggregatedPresenceColumns.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + ")";

    // Current contacts - those contacted within the last 3 days (in seconds)
    private static final long CURRENT_CONTACTS = 3 * 24 * 60 * 60;

    // Recent contacts - those contacted within the last 30 days (in seconds)
    private static final long RECENT_CONTACTS = 30 * 24 * 60 * 60;

    private static final String TIME_SINCE_LAST_CONTACTED =
            "(strftime('%s', 'now') - contacts." + Contacts.LAST_TIME_CONTACTED + "/1000)";

    /*
     * See {@link ContactsProvider2#EMAIL_FILTER_SORT_ORDER} for the discussion of this
     * sorting order.
     */
    private static final String SORT_ORDER =
        "(CASE WHEN contacts." + Contacts.STARRED + "=1 THEN 0 ELSE 1 END), "
        + "(CASE WHEN " + TIME_SINCE_LAST_CONTACTED + " < " + CURRENT_CONTACTS + " THEN 0 "
        + " WHEN " + TIME_SINCE_LAST_CONTACTED + " < " + RECENT_CONTACTS + " THEN 1 "
        + " ELSE 2 END),"
        + "contacts." + Contacts.TIMES_CONTACTED + " DESC, "
        + "contacts." + Contacts.DISPLAY_NAME_PRIMARY + ", "
        + "contacts." + Contacts._ID;

    private static final String RECENTLY_CONTACTED =
        TIME_SINCE_LAST_CONTACTED + " < " + RECENT_CONTACTS;

    private static class SearchSuggestion {
        long contactId;
        String photoUri;
        String lookupKey;
        int presence = -1;
        String text1;
        String text2;
        String icon1;
        String icon2;
        String filter;
        String lastAccessTime;

        @SuppressWarnings({"unchecked"})
        public ArrayList asList(String[] projection) {
            if (photoUri != null) {
                icon1 = photoUri.toString();
            } else {
                icon1 = String.valueOf(com.android.internal.R.drawable.ic_contact_picture);
            }

            if (presence != -1) {
                icon2 = String.valueOf(StatusUpdates.getPresenceIconResourceId(presence));
            }

            ArrayList<Object> list = new ArrayList<Object>();
            if (projection == null) {
                list.add(contactId);
                list.add(text1);
                list.add(text2);
                list.add(icon1);
                list.add(icon2);
                list.add(buildUri());
                list.add(lookupKey);
                list.add(filter);
                list.add(lastAccessTime);
            } else {
                for (int i = 0; i < projection.length; i++) {
                    addColumnValue(list, projection[i]);
                }
            }
            return list;
        }

        private void addColumnValue(ArrayList<Object> list, String column) {
            if ("_id".equals(column)) {
                list.add(contactId);
            } else if (SearchManager.SUGGEST_COLUMN_TEXT_1.equals(column)) {
                list.add(text1);
            } else if (SearchManager.SUGGEST_COLUMN_TEXT_2.equals(column)) {
                list.add(text2);
            } else if (SearchManager.SUGGEST_COLUMN_ICON_1.equals(column)) {
                list.add(icon1);
            } else if (SearchManager.SUGGEST_COLUMN_ICON_2.equals(column)) {
                list.add(icon2);
            } else if (SearchManager.SUGGEST_COLUMN_INTENT_DATA.equals(column)) {
                list.add(buildUri());
            } else if (SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID.equals(column)) {
                list.add(lookupKey);
            } else if (SearchManager.SUGGEST_COLUMN_SHORTCUT_ID.equals(column)) {
                list.add(lookupKey);
            } else if (SearchManager.SUGGEST_COLUMN_INTENT_EXTRA_DATA.equals(column)) {
                list.add(filter);
            } else if (SearchManager.SUGGEST_COLUMN_LAST_ACCESS_HINT.equals(column)) {
                list.add(lastAccessTime);
            } else {
                throw new IllegalArgumentException("Invalid column name: " + column);
            }
        }

        private String buildUri() {
            return Contacts.getLookupUri(contactId, lookupKey).toString();
        }
    }

    private final ContactsProvider2 mContactsProvider;
    private final PhoneNumberUtil mPhoneNumberUtil;

    @SuppressWarnings("all")
    public GlobalSearchSupport(ContactsProvider2 contactsProvider) {
        mContactsProvider = contactsProvider;
        mPhoneNumberUtil = PhoneNumberUtil.getInstance();

        // To ensure the data column position. This is dead code if properly configured.
        if (Organization.COMPANY != Data.DATA1 || Phone.NUMBER != Data.DATA1
                || Email.DATA != Data.DATA1) {
            throw new AssertionError("Some of ContactsContract.CommonDataKinds class primary"
                    + " data is not in DATA1 column");
        }
    }

    public Cursor handleSearchSuggestionsQuery(
            SQLiteDatabase db, Uri uri, String[] projection, String limit) {
        final String searchClause;
        final String selection;
        if (uri.getPathSegments().size() <= 1) {
            searchClause = null;
            selection = RECENTLY_CONTACTED;
        } else {
            searchClause = uri.getLastPathSegment();
            selection = null;
        }

        if (mContactsProvider.isPhone() && isPhoneNumber(searchClause)) {
            return buildCursorForSearchSuggestionsBasedOnPhoneNumber(searchClause);
        } else {
            return buildCursorForSearchSuggestionsBasedOnFilter(
                    db, projection, selection, searchClause, limit);
        }
    }

    private boolean isPhoneNumber(String query) {
        return mPhoneNumberUtil.isPossibleNumber(query, Locale.getDefault().getCountry());
    }

    /**
     * Returns a search suggestions cursor for the contact bearing the provided lookup key.  If the
     * lookup key cannot be found in the database, the contact name is decoded from the lookup key
     * and used to re-identify the contact.  If the contact still cannot be found, an empty cursor
     * is returned.
     *
     * <p>Note that if {@code lookupKey} is not a valid lookup key, an empty cursor is returned
     * silently.  This would occur with old-style shortcuts that were created using the contact id
     * instead of the lookup key.
     */
    public Cursor handleSearchShortcutRefresh(SQLiteDatabase db, String[] projection,
            String lookupKey, String filter) {
        long contactId;
        try {
            contactId = mContactsProvider.lookupContactIdByLookupKey(db, lookupKey);
        } catch (IllegalArgumentException e) {
            contactId = -1L;
        }
        return buildCursorForSearchSuggestionsBasedOnFilter(
                db, projection, ContactsColumns.CONCRETE_ID + "=" + contactId, filter, null);
    }

    private boolean isVoiceCapable() {
        // this copied from com.android.phone.PhoneApp.onCreate():

        // "voice capable" flag.
        // This flag currently comes from a resource (which is
        // overrideable on a per-product basis):
        return mContactsProvider.getContext().getResources()
                .getBoolean(com.android.internal.R.bool.config_voice_capable);
        // ...but this might eventually become a PackageManager "system
        // feature" instead, in which case we'd do something like:
        // return
        //   getPackageManager().hasSystemFeature(PackageManager.FEATURE_TELEPHONY_VOICE_CALLS);
    }

    private Cursor buildCursorForSearchSuggestionsBasedOnPhoneNumber(String searchClause) {
        MatrixCursor cursor = new MatrixCursor(SEARCH_SUGGESTIONS_BASED_ON_PHONE_NUMBER_COLUMNS);
        Resources r = mContactsProvider.getContext().getResources();
        String s;
        int i;

        if (isVoiceCapable()) {
            ArrayList<Object> dialNumber = new ArrayList<Object>();
            dialNumber.add(0);  // _id
            s = r.getString(com.android.internal.R.string.dial_number_using, searchClause);
            i = s.indexOf('\n');
            if (i < 0) {
                dialNumber.add(s);
                dialNumber.add("");
            } else {
                dialNumber.add(s.substring(0, i));
                dialNumber.add(s.substring(i + 1));
            }
            dialNumber.add(String.valueOf(com.android.internal.R.drawable.call_contact));
            dialNumber.add("tel:" + searchClause);
            dialNumber.add(ContactsContract.Intents.SEARCH_SUGGESTION_DIAL_NUMBER_CLICKED);
            dialNumber.add(null);
            cursor.addRow(dialNumber);
        }

        ArrayList<Object> createContact = new ArrayList<Object>();
        createContact.add(1);  // _id
        s = r.getString(com.android.internal.R.string.create_contact_using, searchClause);
        i = s.indexOf('\n');
        if (i < 0) {
            createContact.add(s);
            createContact.add("");
        } else {
            createContact.add(s.substring(0, i));
            createContact.add(s.substring(i + 1));
        }
        createContact.add(String.valueOf(com.android.internal.R.drawable.create_contact));
        createContact.add("tel:" + searchClause);
        createContact.add(ContactsContract.Intents.SEARCH_SUGGESTION_CREATE_CONTACT_CLICKED);
        createContact.add(SearchManager.SUGGEST_NEVER_MAKE_SHORTCUT);
        cursor.addRow(createContact);

        return cursor;
    }

    private Cursor buildCursorForSearchSuggestionsBasedOnFilter(SQLiteDatabase db,
            String[] projection, String selection, String filter, String limit) {
        MatrixCursor cursor = new MatrixCursor(
                projection != null ? projection : SEARCH_SUGGESTIONS_BASED_ON_NAME_COLUMNS);
        StringBuilder sb = new StringBuilder();
        final boolean haveFilter = !TextUtils.isEmpty(filter);
        sb.append("SELECT "
                        + Contacts._ID + ", "
                        + Contacts.LOOKUP_KEY + ", "
                        + Contacts.PHOTO_THUMBNAIL_URI + ", "
                        + Contacts.DISPLAY_NAME + ", "
                        + PRESENCE_SQL + " AS " + Contacts.CONTACT_PRESENCE + ", "
                        + Contacts.LAST_TIME_CONTACTED);
        if (haveFilter) {
            sb.append(", " + SearchSnippetColumns.SNIPPET);
        }
        sb.append(" FROM ");
        sb.append(Views.CONTACTS);
        sb.append(" AS contacts");
        if (haveFilter) {
            mContactsProvider.appendSearchIndexJoin(sb, filter, true,
                    String.valueOf(SNIPPET_START_MATCH), String.valueOf(SNIPPET_END_MATCH),
                    SNIPPET_ELLIPSIS, SNIPPET_MAX_TOKENS, false);
        }
        if (selection != null) {
            sb.append(" WHERE ").append(selection);
        }
        sb.append(" ORDER BY " + SORT_ORDER);
        if (limit != null) {
            sb.append(" LIMIT " + limit);
        }
        Cursor c = db.rawQuery(sb.toString(), null);
        SearchSuggestion suggestion = new SearchSuggestion();
        suggestion.filter = filter;
        try {
            while (c.moveToNext()) {
                suggestion.contactId = c.getLong(0);
                suggestion.lookupKey = c.getString(1);
                suggestion.photoUri = c.getString(2);
                suggestion.text1 = c.getString(3);
                suggestion.presence = c.isNull(4) ? -1 : c.getInt(4);
                suggestion.lastAccessTime = c.getString(5);
                if (haveFilter) {
                    suggestion.text2 = shortenSnippet(c.getString(6));
                }
                cursor.addRow(suggestion.asList(projection));
            }
        } finally {
            c.close();
        }
        return cursor;
    }

    private String shortenSnippet(final String snippet) {
        if (snippet == null) {
            return null;
        }

        int from = 0;
        int to = snippet.length();
        int start = snippet.indexOf(SNIPPET_START_MATCH);
        if (start == -1) {
            return null;
        }

        int firstNl = snippet.lastIndexOf('\n', start);
        if (firstNl != -1) {
            from = firstNl + 1;
        }
        int end = snippet.lastIndexOf(SNIPPET_END_MATCH);
        if (end != -1) {
            int lastNl = snippet.indexOf('\n', end);
            if (lastNl != -1) {
                to = lastNl;
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++) {
            char c = snippet.charAt(i);
            if (c != SNIPPET_START_MATCH && c != SNIPPET_END_MATCH) {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
