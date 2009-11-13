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

import com.android.internal.database.ArrayListCursor;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MimetypesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import android.app.SearchManager;
import android.content.ContentUris;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.provider.Contacts.Intents;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.Contacts.Photo;
import android.text.TextUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

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
            SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID,
            SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
    };

    private interface SearchSuggestionQuery {
        public static final String JOIN_RAW_CONTACTS =
                " JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) ";

        public static final String JOIN_CONTACTS =
                " JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String JOIN_MIMETYPES =
                " JOIN mimetypes ON (data.mimetype_id = mimetypes._id AND mimetypes.mimetype IN ('"
                + StructuredName.CONTENT_ITEM_TYPE + "','" + Email.CONTENT_ITEM_TYPE + "','"
                + Phone.CONTENT_ITEM_TYPE + "','" + Organization.CONTENT_ITEM_TYPE + "','"
                + GroupMembership.CONTENT_ITEM_TYPE + "')) ";

        public static final String TABLE = "data " + JOIN_RAW_CONTACTS + JOIN_MIMETYPES
                + JOIN_CONTACTS;

        public static final String PRESENCE_SQL =
                "(SELECT " + StatusUpdates.PRESENCE_STATUS +
                " FROM " + Tables.AGGREGATED_PRESENCE +
                " WHERE " + AggregatedPresenceColumns.CONTACT_ID
                        + "=" + ContactsColumns.CONCRETE_ID + ")";

        public static final String[] COLUMNS = {
            ContactsColumns.CONCRETE_ID + " AS " + Contacts._ID,
            ContactsColumns.CONCRETE_DISPLAY_NAME + " AS " + Contacts.DISPLAY_NAME,
            PRESENCE_SQL + " AS " + Contacts.CONTACT_PRESENCE,
            DataColumns.CONCRETE_ID + " AS data_id",
            MimetypesColumns.MIMETYPE,
            Data.IS_SUPER_PRIMARY,
            Organization.COMPANY,
            Email.DATA,
            Phone.NUMBER,
            Contacts.PHOTO_ID,
        };

        public static final int CONTACT_ID = 0;
        public static final int DISPLAY_NAME = 1;
        public static final int PRESENCE_STATUS = 2;
        public static final int DATA_ID = 3;
        public static final int MIMETYPE = 4;
        public static final int IS_SUPER_PRIMARY = 5;
        public static final int ORGANIZATION = 6;
        public static final int EMAIL = 7;
        public static final int PHONE = 8;
        public static final int PHOTO_ID = 9;
    }

    private static class SearchSuggestion {
        String contactId;
        boolean titleIsName;
        String organization;
        String email;
        String phoneNumber;
        Uri photoUri;
        String normalizedName;
        int presence = -1;
        boolean processed;
        String text1;
        String text2;
        String icon1;
        String icon2;

        public SearchSuggestion(long contactId) {
            this.contactId = String.valueOf(contactId);
        }

        private void process() {
            if (processed) {
                return;
            }

            boolean hasOrganization = !TextUtils.isEmpty(organization);
            boolean hasEmail = !TextUtils.isEmpty(email);
            boolean hasPhone = !TextUtils.isEmpty(phoneNumber);

            boolean titleIsOrganization = !titleIsName && hasOrganization;
            boolean titleIsEmail = !titleIsName && !titleIsOrganization && hasEmail;
            boolean titleIsPhone = !titleIsName && !titleIsOrganization && !titleIsEmail
                    && hasPhone;

            if (!titleIsOrganization && hasOrganization) {
                text2 = organization;
            } else if (!titleIsPhone && hasPhone) {
                text2 = phoneNumber;
            } else if (!titleIsEmail && hasEmail) {
                text2 = email;
            }

            if (photoUri != null) {
                icon1 = photoUri.toString();
            } else {
                icon1 = String.valueOf(com.android.internal.R.drawable.ic_contact_picture);
            }

            if (presence != -1) {
                icon2 = String.valueOf(StatusUpdates.getPresenceIconResourceId(presence));
            }

            processed = true;
        }

        public String getSortKey() {
            if (normalizedName == null) {
                process();
                normalizedName = text1 == null ? "" : NameNormalizer.normalize(text1);
            }
            return normalizedName;
        }

        @SuppressWarnings({"unchecked"})
        public ArrayList asList(String[] projection) {
            process();

            ArrayList<Object> list = new ArrayList<Object>();
            if (projection == null) {
                list.add(contactId);
                list.add(text1);
                list.add(text2);
                list.add(icon1);
                list.add(icon2);
                list.add(contactId);
                list.add(contactId);
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
            } else if (SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID.equals(column)) {
                list.add(contactId);
            } else if (SearchManager.SUGGEST_COLUMN_SHORTCUT_ID.equals(column)) {
                list.add(contactId);
            } else {
                throw new IllegalArgumentException("Invalid column name: " + column);
            }
        }
    }

    private final ContactsProvider2 mContactsProvider;

    public GlobalSearchSupport(ContactsProvider2 contactsProvider) {
        mContactsProvider = contactsProvider;
    }

    public Cursor handleSearchSuggestionsQuery(SQLiteDatabase db, Uri uri, String limit) {
        if (uri.getPathSegments().size() <= 1) {
            return null;
        }

        final String searchClause = uri.getLastPathSegment();
        if (TextUtils.isDigitsOnly(searchClause)) {
            return buildCursorForSearchSuggestionsBasedOnPhoneNumber(searchClause);
        } else {
            return buildCursorForSearchSuggestionsBasedOnName(db, searchClause, limit);
        }
    }

    public Cursor handleSearchShortcutRefresh(SQLiteDatabase db, long contactId, String[] projection) {
        StringBuilder sb = new StringBuilder();
        sb.append(mContactsProvider.getContactsRestrictions());
        sb.append(" AND " + RawContacts.CONTACT_ID + "=" + contactId);
        return buildCursorForSearchSuggestions(db, sb.toString(), projection);
    }

    private Cursor buildCursorForSearchSuggestionsBasedOnPhoneNumber(String searchClause) {
        Resources r = mContactsProvider.getContext().getResources();
        String s;
        int i;

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
        dialNumber.add(Intents.SEARCH_SUGGESTION_DIAL_NUMBER_CLICKED);
        dialNumber.add(null);

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
        createContact.add(Intents.SEARCH_SUGGESTION_CREATE_CONTACT_CLICKED);
        createContact.add(SearchManager.SUGGEST_NEVER_MAKE_SHORTCUT);

        @SuppressWarnings({"unchecked"}) ArrayList<ArrayList> rows = new ArrayList<ArrayList>();
        rows.add(dialNumber);
        rows.add(createContact);

        return new ArrayListCursor(SEARCH_SUGGESTIONS_BASED_ON_PHONE_NUMBER_COLUMNS, rows);
    }

    private Cursor buildCursorForSearchSuggestionsBasedOnName(SQLiteDatabase db,
            String searchClause, String limit) {

        StringBuilder sb = new StringBuilder();
        sb.append(mContactsProvider.getContactsRestrictions());
        sb.append(" AND " + DataColumns.CONCRETE_RAW_CONTACT_ID + " IN ");
        mContactsProvider.appendRawContactsByFilterAsNestedQuery(sb, searchClause, limit);
        sb.append(" AND " + Contacts.IN_VISIBLE_GROUP + "=1");

        return buildCursorForSearchSuggestions(db, sb.toString(), null);
    }

    private Cursor buildCursorForSearchSuggestions(SQLiteDatabase db, String selection,
            String[] projection) {
        ArrayList<SearchSuggestion> suggestionList = new ArrayList<SearchSuggestion>();
        HashMap<Long, SearchSuggestion> suggestionMap = new HashMap<Long, SearchSuggestion>();
        Cursor c = db.query(true, SearchSuggestionQuery.TABLE,
                SearchSuggestionQuery.COLUMNS, selection, null, null, null, null, null);
        try {
            while (c.moveToNext()) {

                long contactId = c.getLong(SearchSuggestionQuery.CONTACT_ID);
                SearchSuggestion suggestion = suggestionMap.get(contactId);
                if (suggestion == null) {
                    suggestion = new SearchSuggestion(contactId);
                    suggestionList.add(suggestion);
                    suggestionMap.put(contactId, suggestion);
                }

                boolean isSuperPrimary = c.getInt(SearchSuggestionQuery.IS_SUPER_PRIMARY) != 0;
                suggestion.text1 = c.getString(SearchSuggestionQuery.DISPLAY_NAME);

                if (!c.isNull(SearchSuggestionQuery.PRESENCE_STATUS)) {
                    suggestion.presence = c.getInt(SearchSuggestionQuery.PRESENCE_STATUS);
                }

                String mimetype = c.getString(SearchSuggestionQuery.MIMETYPE);
                if (StructuredName.CONTENT_ITEM_TYPE.equals(mimetype)) {
                    suggestion.titleIsName = true;
                } else if (Organization.CONTENT_ITEM_TYPE.equals(mimetype)) {
                    if (isSuperPrimary || suggestion.organization == null) {
                        suggestion.organization = c.getString(SearchSuggestionQuery.ORGANIZATION);
                    }
                } else if (Email.CONTENT_ITEM_TYPE.equals(mimetype)) {
                    if (isSuperPrimary || suggestion.email == null) {
                        suggestion.email = c.getString(SearchSuggestionQuery.EMAIL);
                    }
                } else if (Phone.CONTENT_ITEM_TYPE.equals(mimetype)) {
                    if (isSuperPrimary || suggestion.phoneNumber == null) {
                        suggestion.phoneNumber = c.getString(SearchSuggestionQuery.PHONE);
                    }
                }

                if (!c.isNull(SearchSuggestionQuery.PHOTO_ID)) {
                    suggestion.photoUri = Uri.withAppendedPath(
                            ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                            Photo.CONTENT_DIRECTORY);
                }
            }
        } finally {
            c.close();
        }

        Collections.sort(suggestionList, new Comparator<SearchSuggestion>() {
            public int compare(SearchSuggestion row1, SearchSuggestion row2) {
                return row1.getSortKey().compareTo(row2.getSortKey());
            }
        });

        @SuppressWarnings({"unchecked"}) ArrayList<ArrayList> rows = new ArrayList<ArrayList>();
        for (int i = 0; i < suggestionList.size(); i++) {
            rows.add(suggestionList.get(i).asList(projection));
        }

        return new ArrayListCursor(projection != null ? projection
                : SEARCH_SUGGESTIONS_BASED_ON_NAME_COLUMNS, rows);
    }
}
