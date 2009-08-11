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

import com.android.internal.content.SyncStateContentProviderHelper;
import com.android.providers.contacts.OpenHelper.AggregationExceptionColumns;
import com.android.providers.contacts.OpenHelper.Clauses;
import com.android.providers.contacts.OpenHelper.ContactsColumns;
import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.GroupsColumns;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.PackagesColumns;
import com.android.providers.contacts.OpenHelper.PhoneColumns;
import com.android.providers.contacts.OpenHelper.PhoneLookupColumns;
import com.android.providers.contacts.OpenHelper.RawContactsColumns;
import com.android.providers.contacts.OpenHelper.Tables;
import com.google.android.collect.Lists;

import android.accounts.Account;
import android.app.SearchManager;
import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.OperationApplicationException;
import android.content.SharedPreferences;
import android.content.UriMatcher;
import android.content.SharedPreferences.Editor;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.Binder;
import android.os.RemoteException;
import android.preference.PreferenceManager;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.Contacts.People;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.BaseTypes;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Log;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends ContentProvider {
    // TODO: clean up debug tag and rename this class
    private static final String TAG = "ContactsProvider ~~~~";

    // TODO: carefully prevent all incoming nested queries; they can be gaping security holes
    // TODO: check for restricted flag during insert(), update(), and delete() calls

    /** Default for the maximum number of returned aggregation suggestions. */
    private static final int DEFAULT_MAX_SUGGESTIONS = 5;

    /**
     * Shared preference key for the legacy contact import version. The need for a version
     * as opposed to a boolean flag is that if we discover bugs in the contact import process,
     * we can trigger re-import by incrementing the import version.
     */
    private static final String PREF_CONTACTS_IMPORTED = "contacts_imported_v1";
    private static final int PREF_CONTACTS_IMPORT_VERSION = 1;

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final String STREQUENT_ORDER_BY = Contacts.STARRED + " DESC, "
            + Contacts.TIMES_CONTACTED + " DESC, "
            + Contacts.DISPLAY_NAME + " ASC";
    private static final String STREQUENT_LIMIT =
            "(SELECT COUNT(1) FROM " + Tables.CONTACTS + " WHERE "
            + Contacts.STARRED + "=1) + 25";

    private static final int CONTACTS = 1000;
    private static final int CONTACTS_ID = 1001;
    private static final int CONTACTS_DATA = 1002;
    private static final int CONTACTS_SUMMARY = 1003;
    private static final int CONTACTS_SUMMARY_ID = 1005;
    private static final int CONTACTS_SUMMARY_FILTER = 1006;
    private static final int CONTACTS_SUMMARY_STREQUENT = 1007;
    private static final int CONTACTS_SUMMARY_STREQUENT_FILTER = 1008;
    private static final int CONTACTS_SUMMARY_GROUP = 1009;

    private static final int RAW_CONTACTS = 2002;
    private static final int RAW_CONTACTS_ID = 2003;
    private static final int RAW_CONTACTS_DATA = 2004;

    private static final int DATA = 3000;
    private static final int DATA_ID = 3001;
    private static final int PHONES = 3002;
    private static final int PHONES_FILTER = 3003;
    private static final int EMAILS = 3004;
    private static final int EMAILS_FILTER = 3005;
    private static final int POSTALS = 3006;

    private static final int PHONE_LOOKUP = 4000;

    private static final int AGGREGATION_EXCEPTIONS = 6000;
    private static final int AGGREGATION_EXCEPTION_ID = 6001;

    private static final int PRESENCE = 7000;
    private static final int PRESENCE_ID = 7001;

    private static final int AGGREGATION_SUGGESTIONS = 8000;

    private static final int GROUPS = 10000;
    private static final int GROUPS_ID = 10001;
    private static final int GROUPS_SUMMARY = 10003;

    private static final int SYNCSTATE = 11000;

    private static final int SEARCH_SUGGESTIONS = 12001;
    private static final int SEARCH_SHORTCUT = 12002;

    private interface ContactsQuery {
        public static final String TABLE = Tables.RAW_CONTACTS;

        public static final String[] PROJECTION = new String[] {
            RawContactsColumns.CONCRETE_ID,
            RawContacts.ACCOUNT_NAME,
            RawContacts.ACCOUNT_TYPE,
        };

        public static final int RAW_CONTACT_ID = 0;
        public static final int ACCOUNT_NAME = 1;
        public static final int ACCOUNT_TYPE = 2;
    }

    private interface DataRawContactsQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS;

        public static final String[] PROJECTION = new String[] {
            RawContactsColumns.CONCRETE_ID,
            DataColumns.CONCRETE_ID,
            RawContacts.CONTACT_ID,
            RawContacts.IS_RESTRICTED,
            Data.MIMETYPE,
        };

        public static final int RAW_CONTACT_ID = 0;
        public static final int DATA_ID = 1;
        public static final int CONTACT_ID = 2;
        public static final int IS_RESTRICTED = 3;
        public static final int MIMETYPE = 4;
    }

    private interface DataContactsQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS;

        public static final String[] PROJECTION = new String[] {
            RawContactsColumns.CONCRETE_ID,
            DataColumns.CONCRETE_ID,
            ContactsColumns.CONCRETE_ID,
            MimetypesColumns.CONCRETE_ID,
            Phone.NUMBER,
            Email.DATA,
            ContactsColumns.OPTIMAL_PRIMARY_PHONE_ID,
            ContactsColumns.FALLBACK_PRIMARY_PHONE_ID,
            ContactsColumns.OPTIMAL_PRIMARY_EMAIL_ID,
            ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID,
        };

        public static final int RAW_CONTACT_ID = 0;
        public static final int DATA_ID = 1;
        public static final int CONTACT_ID = 2;
        public static final int MIMETYPE_ID = 3;
        public static final int PHONE_NUMBER = 4;
        public static final int EMAIL_DATA = 5;
        public static final int OPTIMAL_PHONE_ID = 6;
        public static final int FALLBACK_PHONE_ID = 7;
        public static final int OPTIMAL_EMAIL_ID = 8;
        public static final int FALLBACK_EMAIL_ID = 9;

    }

    private interface DisplayNameQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPES;

        public static final String[] COLUMNS = new String[] {
            MimetypesColumns.MIMETYPE,
            Data.IS_PRIMARY,
            Data.DATA2,
            StructuredName.DISPLAY_NAME,
        };

        public static final int MIMETYPE = 0;
        public static final int IS_PRIMARY = 1;
        public static final int DATA2 = 2;
        public static final int DISPLAY_NAME = 3;
    }

    private interface DataQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPES;

        public static final String[] COLUMNS = new String[] {
            DataColumns.CONCRETE_ID,
            MimetypesColumns.MIMETYPE,
            Data.RAW_CONTACT_ID,
            Data.IS_PRIMARY,
            Data.DATA1,
            Data.DATA2,
            Data.DATA3,
            Data.DATA4,
            Data.DATA5,
            Data.DATA6,
            Data.DATA7,
            Data.DATA8,
            Data.DATA9,
            Data.DATA10,
            Data.DATA11,
            Data.DATA12,
            Data.DATA13,
            Data.DATA14,
            Data.DATA15,
        };

        public static final int ID = 0;
        public static final int MIMETYPE = 1;
        public static final int RAW_CONTACT_ID = 2;
        public static final int IS_PRIMARY = 3;
        public static final int DATA1 = 4;
        public static final int DATA2 = 5;
        public static final int DATA3 = 6;
        public static final int DATA4 = 7;
        public static final int DATA5 = 8;
        public static final int DATA6 = 9;
        public static final int DATA7 = 10;
        public static final int DATA8 = 11;
        public static final int DATA9 = 12;
        public static final int DATA10 = 13;
        public static final int DATA11 = 14;
        public static final int DATA12 = 15;
        public static final int DATA13 = 16;
        public static final int DATA14 = 17;
        public static final int DATA15 = 18;
    }

    private interface DataIdQuery {
        String[] COLUMNS = { Data._ID, Data.RAW_CONTACT_ID, Data.MIMETYPE };

        int _ID = 0;
        int RAW_CONTACT_ID = 1;
        int MIMETYPE = 2;
    }

    // Higher number represents higher priority in choosing what data to use for the display name
    private static final int DISPLAY_NAME_PRIORITY_EMAIL = 1;
    private static final int DISPLAY_NAME_PRIORITY_PHONE = 2;
    private static final int DISPLAY_NAME_PRIORITY_ORGANIZATION = 3;
    private static final int DISPLAY_NAME_PRIORITY_STRUCTURED_NAME = 4;

    private static final HashMap<String, Integer> sDisplayNamePriorities;
    static {
        sDisplayNamePriorities = new HashMap<String, Integer>();
        sDisplayNamePriorities.put(StructuredName.CONTENT_ITEM_TYPE,
                DISPLAY_NAME_PRIORITY_STRUCTURED_NAME);
        sDisplayNamePriorities.put(Organization.CONTENT_ITEM_TYPE,
                DISPLAY_NAME_PRIORITY_ORGANIZATION);
        sDisplayNamePriorities.put(Phone.CONTENT_ITEM_TYPE,
                DISPLAY_NAME_PRIORITY_PHONE);
        sDisplayNamePriorities.put(Email.CONTENT_ITEM_TYPE,
                DISPLAY_NAME_PRIORITY_EMAIL);
    }

    /** Contains just the contacts columns
     * @deprecated*/
    @Deprecated
    private static final HashMap<String, String> sDeprecatedContactsProjectionMap;

    private static final HashMap<String, String> sContactsProjectionMap;


    /** Contains the contact columns along with primary phone */
    private static final HashMap<String, String> sContactsSummaryProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sRawContactsProjectionMap;

    /**
     * Contains just the contacts columns
     *
     * @deprecated
     */
    @Deprecated
    private static final HashMap<String, String> sDeprecatedRawContactsProjectionMap;

    /**
     * Contains just the data columns
     *
     * @deprecated
     */
    @Deprecated
    private static final HashMap<String, String> sDeprecatedDataGroupsProjectionMap;

    /** Contains columns from the data view */
    private static final HashMap<String, String> sDataProjectionMap;

    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataRawContactsGroupsProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataRawContactsProjectionMap;
    /** Contains the just the {@link Groups} columns */
    private static final HashMap<String, String> sGroupsProjectionMap;
    /** Contains {@link Groups} columns along with summary details */
    private static final HashMap<String, String> sGroupsSummaryProjectionMap;
    /** Contains the agg_exceptions columns */
    private static final HashMap<String, String> sAggregationExceptionsProjectionMap;
    /** Contains Presence columns */
    private static final HashMap<String, String> sPresenceProjectionMap;

    /** Sql select statement that returns the contact id associated with a data record. */
    private static final String sNestedRawContactIdSelect;
    /** Sql select statement that returns the mimetype id associated with a data record. */
    private static final String sNestedMimetypeSelect;
    /** Sql select statement that returns the contact id associated with a contact record. */
    private static final String sNestedContactIdSelect;
    /** Sql select statement that returns a list of contact ids associated with an contact record. */
    private static final String sNestedContactIdListSelect;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "primary" is selected.*/
    private static final String sSetPrimaryWhere;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "super primary" is selected.*/
    private static final String sSetSuperPrimaryWhere;
    /** Sql where statement for filtering on groups. */
    private static final String sContactsInGroupSelect;
    /** Precompiled sql statement for setting a data record to the primary. */
    private SQLiteStatement mSetPrimaryStatement;
    /** Precompiled sql statement for setting a data record to the super primary. */
    private SQLiteStatement mSetSuperPrimaryStatement;
    /** Precompiled sql statement for incrementing times contacted for an contact */
    private SQLiteStatement mLastTimeContactedUpdate;
    /** Precompiled sql statement for updating a contact display name */
    private SQLiteStatement mContactDisplayNameUpdate;
    /** Precompiled sql statement for marking a raw contact as dirty */
    private SQLiteStatement mRawContactDirtyUpdate;
    /** Precompiled sql statement for marking a group as dirty */
    private SQLiteStatement mGroupDirtyUpdate;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary", CONTACTS_SUMMARY);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary/#", CONTACTS_SUMMARY_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary/filter/*",
                CONTACTS_SUMMARY_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary/strequent/",
                CONTACTS_SUMMARY_STREQUENT);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary/strequent/filter/*",
                CONTACTS_SUMMARY_STREQUENT_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts_summary/group/*",
                CONTACTS_SUMMARY_GROUP);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/suggestions",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts", RAW_CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#", RAW_CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#/data", RAW_CONTACTS_DATA);

        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones", PHONES);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones/filter/*", PHONES_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails", EMAILS);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/filter/*", EMAILS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/postals", POSTALS);

        matcher.addURI(ContactsContract.AUTHORITY, "groups", GROUPS);
        matcher.addURI(ContactsContract.AUTHORITY, "groups/#", GROUPS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "groups_summary", GROUPS_SUMMARY);

        matcher.addURI(ContactsContract.AUTHORITY, SyncStateContentProviderHelper.PATH, SYNCSTATE);

        matcher.addURI(ContactsContract.AUTHORITY, "phone_lookup/*", PHONE_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregation_exceptions",
                AGGREGATION_EXCEPTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregation_exceptions/*",
                AGGREGATION_EXCEPTION_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "presence", PRESENCE);
        matcher.addURI(ContactsContract.AUTHORITY, "presence/#", PRESENCE_ID);

        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY,
                SEARCH_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY + "/*",
                SEARCH_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/#",
                SEARCH_SHORTCUT);

        sContactsProjectionMap = new HashMap<String, String>();
        sContactsProjectionMap.put(Contacts._ID, Contacts._ID);
        sContactsProjectionMap.put(Contacts.DISPLAY_NAME, Contacts.DISPLAY_NAME);
        sContactsProjectionMap.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sContactsProjectionMap.put(Contacts.TIMES_CONTACTED, Contacts.TIMES_CONTACTED);
        sContactsProjectionMap.put(Contacts.STARRED, Contacts.STARRED);
        sContactsProjectionMap.put(Contacts.IN_VISIBLE_GROUP, Contacts.IN_VISIBLE_GROUP);
        sContactsProjectionMap.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        sContactsProjectionMap.put(Contacts.PRIMARY_PHONE_ID, Contacts.PRIMARY_PHONE_ID);
        sContactsProjectionMap.put(Contacts.PRIMARY_EMAIL_ID, Contacts.PRIMARY_EMAIL_ID);
        sContactsProjectionMap.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        sContactsProjectionMap.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
        sContactsProjectionMap.put(Contacts.PRIMARY_PHONE_ID, Contacts.PRIMARY_PHONE_ID);
        sContactsProjectionMap.put(Contacts.PRIMARY_EMAIL_ID, Contacts.PRIMARY_EMAIL_ID);

        sContactsSummaryProjectionMap = new HashMap<String, String>();
        sContactsSummaryProjectionMap.putAll(sContactsProjectionMap);
        sContactsSummaryProjectionMap.put(Contacts.PRESENCE_STATUS,
                "MAX(" + Presence.PRESENCE_STATUS + ") AS " + Contacts.PRESENCE_STATUS);
        sContactsSummaryProjectionMap.put(Contacts.PRIMARY_PHONE_TYPE,
                Contacts.PRIMARY_PHONE_TYPE);
        sContactsSummaryProjectionMap.put(Contacts.PRIMARY_PHONE_LABEL,
                Contacts.PRIMARY_PHONE_LABEL);
        sContactsSummaryProjectionMap.put(Contacts.PRIMARY_PHONE_NUMBER,
                Contacts.PRIMARY_PHONE_NUMBER);

        HashMap<String, String> columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.DISPLAY_NAME, ContactsColumns.CONCRETE_DISPLAY_NAME + " AS "
                + Contacts.DISPLAY_NAME);
        columns.put(Contacts.LAST_TIME_CONTACTED, ContactsColumns.CONCRETE_LAST_TIME_CONTACTED
                + " AS " + Contacts.LAST_TIME_CONTACTED);
        columns.put(Contacts.TIMES_CONTACTED, ContactsColumns.CONCRETE_TIMES_CONTACTED + " AS "
                + Contacts.TIMES_CONTACTED);
        columns.put(Contacts.STARRED, ContactsColumns.CONCRETE_STARRED + " AS "
                + Contacts.STARRED);
        columns.put(Contacts.IN_VISIBLE_GROUP, Contacts.IN_VISIBLE_GROUP);
        columns.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        columns.put(Contacts.PRIMARY_PHONE_ID, Contacts.PRIMARY_PHONE_ID);
        columns.put(Contacts.PRIMARY_EMAIL_ID, Contacts.PRIMARY_EMAIL_ID);
        columns.put(Contacts.CUSTOM_RINGTONE, ContactsColumns.CONCRETE_CUSTOM_RINGTONE + " AS "
                + Contacts.CUSTOM_RINGTONE);
        columns.put(Contacts.SEND_TO_VOICEMAIL, ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                + " AS " + Contacts.SEND_TO_VOICEMAIL);
        columns.put(ContactsColumns.FALLBACK_PRIMARY_PHONE_ID,
                ContactsColumns.FALLBACK_PRIMARY_PHONE_ID);
        columns.put(ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID,
                ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID);
        sDeprecatedContactsProjectionMap = columns;

        columns = new HashMap<String, String>();
        columns.putAll(sDeprecatedContactsProjectionMap);

//        // Contacts primaries projection map. The overall presence status is
//        // the most-present value, as indicated by the largest value.
//        columns.put(Contacts.PRESENCE_STATUS, "MAX(" + Presence.PRESENCE_STATUS + ") AS "
//                + Contacts.PRESENCE_STATUS);
//        columns.put(Contacts.PRIMARY_PHONE_TYPE, CommonDataKinds.Phone.TYPE);
//        columns.put(Contacts.PRIMARY_PHONE_LABEL, CommonDataKinds.Phone.LABEL);
//        columns.put(Contacts.PRIMARY_PHONE_NUMBER, CommonDataKinds.Phone.NUMBER);
//        sContactsSummaryProjectionMap = columns;

        // RawContacts projection map
        columns = new HashMap<String, String>();
        columns.put(RawContacts._ID, Tables.RAW_CONTACTS + "." + RawContacts._ID + " AS _id");
        columns.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        columns.put(RawContacts.ACCOUNT_NAME,
                OpenHelper.RawContactsColumns.CONCRETE_ACCOUNT_NAME
                        + " AS " + RawContacts.ACCOUNT_NAME);
        columns.put(RawContacts.ACCOUNT_TYPE,
                OpenHelper.RawContactsColumns.CONCRETE_ACCOUNT_TYPE
                        + " AS " + RawContacts.ACCOUNT_TYPE);
        columns.put(RawContacts.SOURCE_ID,
                OpenHelper.RawContactsColumns.CONCRETE_SOURCE_ID
                        + " AS " + RawContacts.SOURCE_ID);
        columns.put(RawContacts.VERSION,
                OpenHelper.RawContactsColumns.CONCRETE_VERSION
                        + " AS " + RawContacts.VERSION);
        columns.put(RawContacts.DIRTY,
                OpenHelper.RawContactsColumns.CONCRETE_DIRTY
                        + " AS " + RawContacts.DIRTY);
        columns.put(RawContacts.DELETED,
                OpenHelper.RawContactsColumns.CONCRETE_DELETED
                        + " AS " + RawContacts.DELETED);
        columns.put(RawContacts.TIMES_CONTACTED,
                Tables.RAW_CONTACTS + "." + RawContacts.TIMES_CONTACTED
                        + " AS " + People.TIMES_CONTACTED);
        columns.put(RawContacts.LAST_TIME_CONTACTED,
                Tables.RAW_CONTACTS + "." + RawContacts.LAST_TIME_CONTACTED
                        + " AS " + People.LAST_TIME_CONTACTED);
        columns.put(RawContacts.CUSTOM_RINGTONE,
                Tables.RAW_CONTACTS + "." + RawContacts.CUSTOM_RINGTONE
                        + " AS " + People.CUSTOM_RINGTONE);
        columns.put(RawContacts.SEND_TO_VOICEMAIL,
                Tables.RAW_CONTACTS + "." + RawContacts.SEND_TO_VOICEMAIL
                        + " AS " + People.SEND_TO_VOICEMAIL);
        columns.put(RawContacts.STARRED,
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED
                        + " AS " + People.STARRED);
        columns.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE);
        columns.put(RawContacts.SYNC1,
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC1 + " AS " + RawContacts.SYNC1);
        columns.put(RawContacts.SYNC2,
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC2 + " AS " + RawContacts.SYNC2);
        columns.put(RawContacts.SYNC3,
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC3 + " AS " + RawContacts.SYNC3);
        columns.put(RawContacts.SYNC4,
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC4 + " AS " + RawContacts.SYNC4);
        sDeprecatedRawContactsProjectionMap = columns;

        sRawContactsProjectionMap = new HashMap<String, String>();
        sRawContactsProjectionMap.put(RawContacts._ID, RawContacts._ID);
        sRawContactsProjectionMap.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        sRawContactsProjectionMap.put(RawContacts.ACCOUNT_NAME, RawContacts.ACCOUNT_NAME);
        sRawContactsProjectionMap.put(RawContacts.ACCOUNT_TYPE, RawContacts.ACCOUNT_TYPE);
        sRawContactsProjectionMap.put(RawContacts.SOURCE_ID, RawContacts.SOURCE_ID);
        sRawContactsProjectionMap.put(RawContacts.VERSION, RawContacts.VERSION);
        sRawContactsProjectionMap.put(RawContacts.DIRTY, RawContacts.DIRTY);
        sRawContactsProjectionMap.put(RawContacts.DELETED, RawContacts.DELETED);
        sRawContactsProjectionMap.put(RawContacts.TIMES_CONTACTED, RawContacts.TIMES_CONTACTED);
        sRawContactsProjectionMap.put(RawContacts.LAST_TIME_CONTACTED,
                RawContacts.LAST_TIME_CONTACTED);
        sRawContactsProjectionMap.put(RawContacts.CUSTOM_RINGTONE, RawContacts.CUSTOM_RINGTONE);
        sRawContactsProjectionMap.put(RawContacts.SEND_TO_VOICEMAIL, RawContacts.SEND_TO_VOICEMAIL);
        sRawContactsProjectionMap.put(RawContacts.STARRED, RawContacts.STARRED);
        sRawContactsProjectionMap.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE);
        sRawContactsProjectionMap.put(RawContacts.SYNC1, RawContacts.SYNC1);
        sRawContactsProjectionMap.put(RawContacts.SYNC2, RawContacts.SYNC2);
        sRawContactsProjectionMap.put(RawContacts.SYNC3, RawContacts.SYNC3);
        sRawContactsProjectionMap.put(RawContacts.SYNC4, RawContacts.SYNC4);

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, Tables.DATA + "." + Data._ID + " AS _id");
        columns.put(Data.RAW_CONTACT_ID, Data.RAW_CONTACT_ID);
        columns.put(Data.RES_PACKAGE, PackagesColumns.PACKAGE + " AS " + Data.RES_PACKAGE);
        columns.put(Data.MIMETYPE, Data.MIMETYPE);
        columns.put(Data.IS_PRIMARY, Data.IS_PRIMARY);
        columns.put(Data.IS_SUPER_PRIMARY, Data.IS_SUPER_PRIMARY);
        columns.put(Data.DATA_VERSION, Data.DATA_VERSION);
        columns.put(Data.DATA1, "data.data1 as data1");
        columns.put(Data.DATA2, "data.data2 as data2");
        columns.put(Data.DATA3, "data.data3 as data3");
        columns.put(Data.DATA4, "data.data4 as data4");
        columns.put(Data.DATA5, "data.data5 as data5");
        columns.put(Data.DATA6, "data.data6 as data6");
        columns.put(Data.DATA7, "data.data7 as data7");
        columns.put(Data.DATA8, "data.data8 as data8");
        columns.put(Data.DATA9, "data.data9 as data9");
        columns.put(Data.DATA10, "data.data10 as data10");
        columns.put(Data.DATA11, "data.data11 as data11");
        columns.put(Data.DATA12, "data.data12 as data12");
        columns.put(Data.DATA13, "data.data13 as data13");
        columns.put(Data.DATA14, "data.data14 as data14");
        columns.put(Data.DATA15, "data.data15 as data15");
        columns.put(Data.SYNC1, Tables.DATA + "." + Data.SYNC1 + " AS " + Data.SYNC1);
        columns.put(Data.SYNC2, Tables.DATA + "." + Data.SYNC2 + " AS " + Data.SYNC2);
        columns.put(Data.SYNC3, Tables.DATA + "." + Data.SYNC3 + " AS " + Data.SYNC3);
        columns.put(Data.SYNC4, Tables.DATA + "." + Data.SYNC4 + " AS " + Data.SYNC4);
        columns.put(GroupMembership.GROUP_SOURCE_ID, GroupsColumns.CONCRETE_SOURCE_ID + " AS "
                + GroupMembership.GROUP_SOURCE_ID);

        // TODO: remove this projection
        // Mappings used for backwards compatibility.
        columns.put("number", Phone.NUMBER);
        sDeprecatedDataGroupsProjectionMap = columns;

        sDataProjectionMap = new HashMap<String, String>();
        sDataProjectionMap.put(Data._ID, Data._ID);
        sDataProjectionMap.put(Data.RAW_CONTACT_ID, Data.RAW_CONTACT_ID);
        sDataProjectionMap.put(Data.DATA_VERSION, Data.DATA_VERSION);
        sDataProjectionMap.put(Data.IS_PRIMARY, Data.IS_PRIMARY);
        sDataProjectionMap.put(Data.IS_SUPER_PRIMARY, Data.IS_SUPER_PRIMARY);
        sDataProjectionMap.put(Data.RES_PACKAGE, Data.RES_PACKAGE);
        sDataProjectionMap.put(Data.MIMETYPE, Data.MIMETYPE);
        sDataProjectionMap.put(Data.DATA1, Data.DATA1);
        sDataProjectionMap.put(Data.DATA2, Data.DATA2);
        sDataProjectionMap.put(Data.DATA3, Data.DATA3);
        sDataProjectionMap.put(Data.DATA4, Data.DATA4);
        sDataProjectionMap.put(Data.DATA5, Data.DATA5);
        sDataProjectionMap.put(Data.DATA6, Data.DATA6);
        sDataProjectionMap.put(Data.DATA7, Data.DATA7);
        sDataProjectionMap.put(Data.DATA8, Data.DATA8);
        sDataProjectionMap.put(Data.DATA9, Data.DATA9);
        sDataProjectionMap.put(Data.DATA10, Data.DATA10);
        sDataProjectionMap.put(Data.DATA11, Data.DATA11);
        sDataProjectionMap.put(Data.DATA12, Data.DATA12);
        sDataProjectionMap.put(Data.DATA13, Data.DATA13);
        sDataProjectionMap.put(Data.DATA14, Data.DATA14);
        sDataProjectionMap.put(Data.DATA15, Data.DATA15);
        sDataProjectionMap.put(Data.SYNC1, Data.SYNC1);
        sDataProjectionMap.put(Data.SYNC2, Data.SYNC2);
        sDataProjectionMap.put(Data.SYNC3, Data.SYNC3);
        sDataProjectionMap.put(Data.SYNC4, Data.SYNC4);
        sDataProjectionMap.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        sDataProjectionMap.put(RawContacts.ACCOUNT_NAME, RawContacts.ACCOUNT_NAME);
        sDataProjectionMap.put(RawContacts.ACCOUNT_TYPE, RawContacts.ACCOUNT_TYPE);
        sDataProjectionMap.put(RawContacts.SOURCE_ID, RawContacts.SOURCE_ID);
        sDataProjectionMap.put(RawContacts.VERSION, RawContacts.VERSION);
        sDataProjectionMap.put(RawContacts.DIRTY, RawContacts.DIRTY);
        sDataProjectionMap.put(Contacts.DISPLAY_NAME, Contacts.DISPLAY_NAME);
        sDataProjectionMap.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        sDataProjectionMap.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
        sDataProjectionMap.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sDataProjectionMap.put(Contacts.TIMES_CONTACTED, Contacts.TIMES_CONTACTED);
        sDataProjectionMap.put(Contacts.STARRED, Contacts.STARRED);
        sDataProjectionMap.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        sDataProjectionMap.put(GroupMembership.GROUP_SOURCE_ID, GroupMembership.GROUP_SOURCE_ID);

        // Data, groups and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sDeprecatedRawContactsProjectionMap);
        columns.putAll(sDeprecatedDataGroupsProjectionMap);
        columns.put(Data.RAW_CONTACT_ID, DataColumns.CONCRETE_RAW_CONTACT_ID);
        sDataRawContactsGroupsProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sDataRawContactsGroupsProjectionMap);
        columns.remove(GroupMembership.GROUP_SOURCE_ID);
        sDataRawContactsProjectionMap = columns;

        // Groups projection map
        columns = new HashMap<String, String>();
        columns.put(Groups._ID, "groups._id AS _id");
        columns.put(Groups.ACCOUNT_NAME, Groups.ACCOUNT_NAME);
        columns.put(Groups.ACCOUNT_TYPE, Groups.ACCOUNT_TYPE);
        columns.put(Groups.SOURCE_ID, Groups.SOURCE_ID);
        columns.put(Groups.DIRTY, Groups.DIRTY);
        columns.put(Groups.VERSION, Groups.VERSION);
        columns.put(Groups.RES_PACKAGE, PackagesColumns.PACKAGE + " AS " + Groups.RES_PACKAGE);
        columns.put(Groups.TITLE, Groups.TITLE);
        columns.put(Groups.TITLE_RES, Groups.TITLE_RES);
        columns.put(Groups.GROUP_VISIBLE, Groups.GROUP_VISIBLE);
        columns.put(Groups.SYSTEM_ID, Groups.SYSTEM_ID);
        columns.put(Groups.DELETED, Groups.DELETED);
        columns.put(Groups.NOTES, Groups.NOTES);
        columns.put(Groups.SYNC1, Tables.GROUPS + "." + Groups.SYNC1 + " AS " + Groups.SYNC1);
        columns.put(Groups.SYNC2, Tables.GROUPS + "." + Groups.SYNC2 + " AS " + Groups.SYNC2);
        columns.put(Groups.SYNC3, Tables.GROUPS + "." + Groups.SYNC3 + " AS " + Groups.SYNC3);
        columns.put(Groups.SYNC4, Tables.GROUPS + "." + Groups.SYNC4 + " AS " + Groups.SYNC4);
        sGroupsProjectionMap = columns;

        // RawContacts and groups projection map
        columns = new HashMap<String, String>();
        columns.putAll(sGroupsProjectionMap);

        columns.put(Groups.SUMMARY_COUNT, "(SELECT COUNT(DISTINCT " + ContactsColumns.CONCRETE_ID
                + ") FROM " + Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS + " WHERE "
                + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP + " AND " + Clauses.BELONGS_TO_GROUP
                + ") AS " + Groups.SUMMARY_COUNT);

        columns.put(Groups.SUMMARY_WITH_PHONES, "(SELECT COUNT(DISTINCT "
                + ContactsColumns.CONCRETE_ID + ") FROM "
                + Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS + " WHERE "
                + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP + " AND " + Clauses.BELONGS_TO_GROUP
                + " AND " + Clauses.HAS_PRIMARY_PHONE + ") AS " + Groups.SUMMARY_WITH_PHONES);

        sGroupsSummaryProjectionMap = columns;

        // Aggregate exception projection map
        columns = new HashMap<String, String>();
        columns.put(AggregationExceptionColumns._ID, Tables.AGGREGATION_EXCEPTIONS + "._id AS _id");
        columns.put(AggregationExceptions.TYPE, AggregationExceptions.TYPE);
        columns.put(AggregationExceptions.CONTACT_ID,
                "raw_contacts1." + RawContacts.CONTACT_ID
                + " AS " + AggregationExceptions.CONTACT_ID);
        columns.put(AggregationExceptions.RAW_CONTACT_ID, AggregationExceptionColumns.RAW_CONTACT_ID2);
        sAggregationExceptionsProjectionMap = columns;


        columns = new HashMap<String, String>();
        columns.put(Presence._ID, Presence._ID);
        columns.put(Presence.RAW_CONTACT_ID, Presence.RAW_CONTACT_ID);
        columns.put(Presence.DATA_ID, Presence.DATA_ID);
        columns.put(Presence.IM_ACCOUNT, Presence.IM_ACCOUNT);
        columns.put(Presence.IM_HANDLE, Presence.IM_HANDLE);
        columns.put(Presence.IM_PROTOCOL, Presence.IM_PROTOCOL);
        columns.put(Presence.PRESENCE_STATUS, Presence.PRESENCE_STATUS);
        columns.put(Presence.PRESENCE_CUSTOM_STATUS, Presence.PRESENCE_CUSTOM_STATUS);
        sPresenceProjectionMap = columns;

        sNestedRawContactIdSelect = "SELECT " + Data.RAW_CONTACT_ID + " FROM " + Tables.DATA + " WHERE "
                + Data._ID + "=?";
        sNestedMimetypeSelect = "SELECT " + DataColumns.MIMETYPE_ID + " FROM " + Tables.DATA
                + " WHERE " + Data._ID + "=?";
        sNestedContactIdSelect = "SELECT " + RawContacts.CONTACT_ID + " FROM " + Tables.RAW_CONTACTS
                + " WHERE " + RawContacts._ID + "=(" + sNestedRawContactIdSelect + ")";
        sNestedContactIdListSelect = "SELECT " + RawContacts._ID + " FROM " + Tables.RAW_CONTACTS
                + " WHERE " + RawContacts.CONTACT_ID + "=(" + sNestedContactIdSelect + ")";
        sSetPrimaryWhere = Data.RAW_CONTACT_ID + "=(" + sNestedRawContactIdSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
        sSetSuperPrimaryWhere = Data.RAW_CONTACT_ID + " IN (" + sNestedContactIdListSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
        sContactsInGroupSelect = Contacts._ID + " IN "
                + "(SELECT " + RawContacts.CONTACT_ID
                + " FROM " + Tables.RAW_CONTACTS
                + " WHERE " + RawContactsColumns.CONCRETE_ID + " IN "
                        + "(SELECT " + DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " FROM " + Tables.DATA_JOIN_MIMETYPES
                        + " WHERE " + Data.MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE
                                + "' AND " + GroupMembership.GROUP_ROW_ID + "="
                                + "(SELECT " + Tables.GROUPS + "." + Groups._ID
                                + " FROM " + Tables.GROUPS
                                + " WHERE " + Groups.TITLE + "=?)))";
    }

    /**
     * Handles inserts and update for a specific Data type.
     */
    private abstract class DataRowHandler {

        protected final String mMimetype;

        public DataRowHandler(String mimetype) {
            mMimetype = mimetype;
        }

        /**
         * Inserts a row into the {@link Data} table.
         */
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            final long dataId = db.insert(Tables.DATA, null, values);

            Integer primary = values.getAsInteger(Data.IS_PRIMARY);
            if (primary != null && primary != 0) {
                setIsPrimary(dataId);
            }

            fixContactDisplayName(db, rawContactId);
            return dataId;
        }

        /**
         * Validates data and updates a {@link Data} row using the cursor, which contains
         * the current data.
         */
        public void update(SQLiteDatabase db, ContentValues values, Cursor cursor) {
            throw new UnsupportedOperationException();
        }

        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataQuery.ID);
            long rawContactId = c.getLong(DataQuery.RAW_CONTACT_ID);
            boolean primary = c.getInt(DataQuery.IS_PRIMARY) != 0;
            int count = db.delete(Tables.DATA, Data._ID + "=" + dataId, null);
            if (count != 0 && primary) {
                fixPrimary(db, rawContactId);
                fixContactDisplayName(db, rawContactId);
            }
            return count;
        }

        private void fixPrimary(SQLiteDatabase db, long rawContactId) {
            long newPrimaryId = findNewPrimaryDataId(db, rawContactId);
            if (newPrimaryId != -1) {
                ContactsProvider2.this.setIsPrimary(newPrimaryId);
            }
        }

        protected long findNewPrimaryDataId(SQLiteDatabase db, long rawContactId) {
            long primaryId = -1;
            int primaryType = -1;
            Cursor c = queryData(db, rawContactId);
            try {
                while (c.moveToNext()) {
                    long dataId = c.getLong(DataQuery.ID);
                    int type = c.getInt(DataQuery.DATA2);
                    if (primaryType == -1 || getTypeRank(type) < getTypeRank(primaryType)) {
                        primaryId = dataId;
                        primaryType = type;
                    }
                }
            } finally {
                c.close();
            }
            return primaryId;
        }

        /**
         * Returns the rank of a specific record type to be used in determining the primary
         * row. Lower number represents higher priority.
         */
        protected int getTypeRank(int type) {
            return 0;
        }

        protected Cursor queryData(SQLiteDatabase db, long rawContactId) {
            // TODO Lookup integer mimetype IDs' instead of joining for speed
            return db.query(DataQuery.TABLE, DataQuery.COLUMNS, Data.RAW_CONTACT_ID + "="
                    + rawContactId + " AND " + MimetypesColumns.MIMETYPE + "='" + mMimetype + "'",
                    null, null, null, null);
        }

        protected void fixContactDisplayName(SQLiteDatabase db, long rawContactId) {
            if (!sDisplayNamePriorities.containsKey(mMimetype)) {
                return;
            }

            String bestDisplayName = null;
            Cursor c = db.query(DisplayNameQuery.TABLE, DisplayNameQuery.COLUMNS,
                    Data.RAW_CONTACT_ID + "=" + rawContactId, null, null, null, null);
            try {
                int maxPriority = -1;
                while (c.moveToNext()) {
                    String mimeType = c.getString(DisplayNameQuery.MIMETYPE);
                    boolean primary;
                    String name;

                    if (StructuredName.CONTENT_ITEM_TYPE.equals(mimeType)) {
                        name = c.getString(DisplayNameQuery.DISPLAY_NAME);
                        primary = true;
                    } else {
                        name = c.getString(DisplayNameQuery.DATA2);
                        primary = (c.getInt(DisplayNameQuery.IS_PRIMARY) != 0);
                    }

                    if (primary && name != null) {
                        Integer priority = sDisplayNamePriorities.get(mimeType);
                        if (priority != null && priority > maxPriority) {
                            maxPriority = priority;
                            bestDisplayName = name;
                        }
                    }
                }

            } finally {
                c.close();
            }

            ContactsProvider2.this.setDisplayName(rawContactId, bestDisplayName);
        }
    }

    public class CustomDataRowHandler extends DataRowHandler {

        public CustomDataRowHandler(String mimetype) {
            super(mimetype);
        }
    }

    public class StructuredNameRowHandler extends DataRowHandler {

        private final NameSplitter mNameSplitter;

        public StructuredNameRowHandler(NameSplitter nameSplitter) {
            super(StructuredName.CONTENT_ITEM_TYPE);
            mNameSplitter = nameSplitter;
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            fixStructuredNameComponents(values);
            return super.insert(db, rawContactId, values);
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor cursor) {
            // TODO Parse the full name if it has changed and replace pre-existing piece parts.
        }

        /**
         * Parses the supplied display name, but only if the incoming values do not already contain
         * structured name parts.  Also, if the display name is not provided, generate one by
         * concatenating first name and last name
         *
         * TODO see if the order of first and last names needs to be conditionally reversed for
         * some locales, e.g. China.
         */
        private void fixStructuredNameComponents(ContentValues values) {
            String fullName = values.getAsString(StructuredName.DISPLAY_NAME);
            if (!TextUtils.isEmpty(fullName)
                    && TextUtils.isEmpty(values.getAsString(StructuredName.PREFIX))
                    && TextUtils.isEmpty(values.getAsString(StructuredName.GIVEN_NAME))
                    && TextUtils.isEmpty(values.getAsString(StructuredName.MIDDLE_NAME))
                    && TextUtils.isEmpty(values.getAsString(StructuredName.FAMILY_NAME))
                    && TextUtils.isEmpty(values.getAsString(StructuredName.SUFFIX))) {
                NameSplitter.Name name = new NameSplitter.Name();
                mNameSplitter.split(name, fullName);

                values.put(StructuredName.PREFIX, name.getPrefix());
                values.put(StructuredName.GIVEN_NAME, name.getGivenNames());
                values.put(StructuredName.MIDDLE_NAME, name.getMiddleName());
                values.put(StructuredName.FAMILY_NAME, name.getFamilyName());
                values.put(StructuredName.SUFFIX, name.getSuffix());
            }

            if (TextUtils.isEmpty(fullName)) {
                String givenName = values.getAsString(StructuredName.GIVEN_NAME);
                String familyName = values.getAsString(StructuredName.FAMILY_NAME);
                if (TextUtils.isEmpty(givenName)) {
                    fullName = familyName;
                } else if (TextUtils.isEmpty(familyName)) {
                    fullName = givenName;
                } else {
                    fullName = givenName + " " + familyName;
                }

                if (!TextUtils.isEmpty(fullName)) {
                    values.put(StructuredName.DISPLAY_NAME, fullName);
                }
            }
        }
    }

    public class CommonDataRowHandler extends DataRowHandler {

        private final String mTypeColumn;
        private final String mLabelColumn;

        public CommonDataRowHandler(String mimetype, String typeColumn, String labelColumn) {
            super(mimetype);
            mTypeColumn = typeColumn;
            mLabelColumn = labelColumn;
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            int type;
            String label;
            if (values.containsKey(mTypeColumn)) {
                type = values.getAsInteger(mTypeColumn);
            } else {
                type = BaseTypes.TYPE_CUSTOM;
            }
            if (values.containsKey(mLabelColumn)) {
                label = values.getAsString(mLabelColumn);
            } else {
                label = null;
            }

            if (type != BaseTypes.TYPE_CUSTOM && label != null) {
                throw new IllegalArgumentException(mLabelColumn + " value can only be specified with "
                        + mTypeColumn + "=" + BaseTypes.TYPE_CUSTOM + "(custom)");
            }

            if (type == BaseTypes.TYPE_CUSTOM && label == null) {
                throw new IllegalArgumentException(mLabelColumn + " value must be specified when "
                        + mTypeColumn + "=" + BaseTypes.TYPE_CUSTOM + "(custom)");
            }

            return super.insert(db, rawContactId, values);
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor cursor) {
            // TODO read the data and check the constraint
        }
    }

    public class OrganizationDataRowHandler extends CommonDataRowHandler {

        public OrganizationDataRowHandler() {
            super(Organization.CONTENT_ITEM_TYPE, Organization.TYPE, Organization.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            long id = super.insert(db, rawContactId, values);
            fixContactDisplayName(db, rawContactId);
            return id;
        }

        @Override
        protected int getTypeRank(int type) {
            switch (type) {
                case Organization.TYPE_WORK: return 0;
                case Organization.TYPE_CUSTOM: return 1;
                case Organization.TYPE_OTHER: return 2;
                default: return 1000;
            }
        }
    }

    public class EmailDataRowHandler extends CommonDataRowHandler {

        public EmailDataRowHandler() {
            super(Email.CONTENT_ITEM_TYPE, Email.TYPE, Email.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            long id = super.insert(db, rawContactId, values);
            fixContactDisplayName(db, rawContactId);
            return id;
        }

        @Override
        protected int getTypeRank(int type) {
            switch (type) {
                case Email.TYPE_HOME: return 0;
                case Email.TYPE_WORK: return 1;
                case Email.TYPE_CUSTOM: return 2;
                case Email.TYPE_OTHER: return 3;
                default: return 1000;
            }
        }
    }

    public class PhoneDataRowHandler extends CommonDataRowHandler {

        public PhoneDataRowHandler() {
            super(Phone.CONTENT_ITEM_TYPE, Phone.TYPE, Phone.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            ContentValues phoneValues = new ContentValues();
            String number = values.getAsString(Phone.NUMBER);
            String normalizedNumber = null;
            if (number != null) {
                normalizedNumber = PhoneNumberUtils.getStrippedReversed(number);
                values.put(PhoneColumns.NORMALIZED_NUMBER, normalizedNumber);
            }

            long id = super.insert(db, rawContactId, values);

            if (number != null) {
                phoneValues.put(PhoneLookupColumns.RAW_CONTACT_ID, rawContactId);
                phoneValues.put(PhoneLookupColumns.DATA_ID, id);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER, normalizedNumber);
                db.insert(Tables.PHONE_LOOKUP, null, phoneValues);
            }

            return id;
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
    }

    private HashMap<String, DataRowHandler> mDataRowHandlers;
    private final ContactAggregationScheduler mAggregationScheduler;
    private OpenHelper mOpenHelper;

    private ContactAggregator mContactAggregator;
    private NameSplitter mNameSplitter;
    private LegacyApiSupport mLegacyApiSupport;
    private GlobalSearchSupport mGlobalSearchSupport;

    private ContentValues mValues = new ContentValues();

    private boolean mImportMode;

    public ContactsProvider2() {
        this(new ContactAggregationScheduler());
    }

    /**
     * Constructor for testing.
     */
    /* package */ ContactsProvider2(ContactAggregationScheduler scheduler) {
        mAggregationScheduler = scheduler;
    }

    @Override
    public boolean onCreate() {
        final Context context = getContext();

        mOpenHelper = getOpenHelper(context);
        mGlobalSearchSupport = new GlobalSearchSupport(this);
        mLegacyApiSupport = new LegacyApiSupport(context, mOpenHelper, this, mGlobalSearchSupport);
        mContactAggregator = new ContactAggregator(context, mOpenHelper, mAggregationScheduler);

        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        mSetPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_PRIMARY
                + "=(_id=?) WHERE " + sSetPrimaryWhere);
        mSetSuperPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_SUPER_PRIMARY
                + "=(_id=?) WHERE " + sSetSuperPrimaryWhere);
        mLastTimeContactedUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContacts.TIMES_CONTACTED + "=" + RawContacts.TIMES_CONTACTED + "+1,"
                + RawContacts.LAST_TIME_CONTACTED + "=? WHERE " + RawContacts.CONTACT_ID + "=?");

        mContactDisplayNameUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContactsColumns.DISPLAY_NAME + "=? WHERE " + RawContacts._ID + "=?");

        mRawContactDirtyUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContacts.DIRTY + "=1 WHERE " + RawContacts._ID + "=?");

        mGroupDirtyUpdate = db.compileStatement("UPDATE " + Tables.GROUPS + " SET "
                + Groups.DIRTY + "=1 WHERE " + Groups._ID + "=?");

        mNameSplitter = new NameSplitter(
                context.getString(com.android.internal.R.string.common_name_prefixes),
                context.getString(com.android.internal.R.string.common_last_name_prefixes),
                context.getString(com.android.internal.R.string.common_name_suffixes),
                context.getString(com.android.internal.R.string.common_name_conjunctions));

        mDataRowHandlers = new HashMap<String, DataRowHandler>();

        mDataRowHandlers.put(Email.CONTENT_ITEM_TYPE, new EmailDataRowHandler());
        mDataRowHandlers.put(Im.CONTENT_ITEM_TYPE,
                new CommonDataRowHandler(Im.CONTENT_ITEM_TYPE, Im.TYPE, Im.LABEL));
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE, new CommonDataRowHandler(
                StructuredPostal.CONTENT_ITEM_TYPE, StructuredPostal.TYPE, StructuredPostal.LABEL));
        mDataRowHandlers.put(Organization.CONTENT_ITEM_TYPE, new OrganizationDataRowHandler());
        mDataRowHandlers.put(Phone.CONTENT_ITEM_TYPE, new PhoneDataRowHandler());
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE, new CommonDataRowHandler(
                Nickname.CONTENT_ITEM_TYPE, Nickname.TYPE, Nickname.LABEL));
        mDataRowHandlers.put(StructuredName.CONTENT_ITEM_TYPE,
                new StructuredNameRowHandler(mNameSplitter));

        if (isLegacyContactImportNeeded()) {
            if (!importLegacyContacts()) {
                return false;
            }
        }
        return (db != null);
    }

    /* Visible for testing */
    protected OpenHelper getOpenHelper(final Context context) {
        return OpenHelper.getInstance(context);
    }

    protected boolean isLegacyContactImportNeeded() {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
        return prefs.getInt(PREF_CONTACTS_IMPORTED, 0) < PREF_CONTACTS_IMPORT_VERSION;
    }

    private boolean importLegacyContacts() {
        if (importLegacyContacts(getLegacyContactImporter())) {
            SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
            Editor editor = prefs.edit();
            editor.putInt(PREF_CONTACTS_IMPORTED, PREF_CONTACTS_IMPORT_VERSION);
            editor.commit();
            return true;
        } else {
            return false;
        }
    }

    protected LegacyContactImporter getLegacyContactImporter() {
        return new LegacyContactImporter(getContext(), this);
    }

    /* Visible for testing */
    /* package */ boolean importLegacyContacts(LegacyContactImporter importer) {
        mContactAggregator.setEnabled(false);
        mImportMode = true;
        try {
            importer.importContacts();
            mContactAggregator.setEnabled(true);
            mContactAggregator.run();
            return true;
        } catch (Throwable e) {
           Log.e(TAG, "Legacy contact import failed", e);
           return false;
        } finally {
            mImportMode = false;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (mContactAggregator != null) {
            mContactAggregator.quit();
        }

        super.finalize();
    }

    /**
     * Wipes all data from the contacts database.
     */
    /* package */ void wipeData() {
        mOpenHelper.wipeData();
    }

    /**
     * Called when a change has been made.
     *
     * @param uri the uri that the change was made to
     */
    private void onChange(Uri uri) {
        getContext().getContentResolver().notifyChange(ContactsContract.AUTHORITY_URI, null);
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

    private DataRowHandler getDataRowHandler(final String mimeType) {
        DataRowHandler handler = mDataRowHandlers.get(mimeType);
        if (handler == null) {
            handler = new CustomDataRowHandler(mimeType);
            mDataRowHandlers.put(mimeType, handler);
        }
        return handler;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int match = sUriMatcher.match(uri);
        long id = 0;

        switch (match) {
            case SYNCSTATE:
                id = mOpenHelper.getSyncState().insert(mOpenHelper.getWritableDatabase(), values);
                break;

            case CONTACTS: {
                insertContact(values);
                break;
            }

            case RAW_CONTACTS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertRawContact(values, account);
                break;
            }

            case RAW_CONTACTS_DATA: {
                values.put(Data.RAW_CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values, shouldMarkRawContactAsDirty(uri));
                break;
            }

            case DATA: {
                id = insertData(values, shouldMarkRawContactAsDirty(uri));
                break;
            }

            case GROUPS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertGroup(values, account, shouldMarkGroupAsDirty(uri));
                break;
            }

            case PRESENCE: {
                id = insertPresence(values);
                break;
            }

            default:
                return mLegacyApiSupport.insert(uri, values);
        }

        if (id < 0) {
            return null;
        }

        final Uri result = ContentUris.withAppendedId(uri, id);
        onChange(result);
        return result;
    }

    /**
     * If account is non-null then store it in the values. If the account is already
     * specified in the values then it must be consistent with the account, if it is non-null.
     * @param values the ContentValues to read from and update
     * @param account the explicitly provided Account
     * @return false if the accounts are inconsistent
     */
    private boolean resolveAccount(ContentValues values, Account account) {
        // If either is specified then both must be specified.
        final String accountName = values.getAsString(RawContacts.ACCOUNT_NAME);
        final String accountType = values.getAsString(RawContacts.ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName) || !TextUtils.isEmpty(accountType)) {
            final Account valuesAccount = new Account(accountName, accountType);
            if (account != null && !valuesAccount.equals(account)) {
                return false;
            }
            account = valuesAccount;
        }
        if (account != null) {
            values.put(RawContacts.ACCOUNT_NAME, account.mName);
            values.put(RawContacts.ACCOUNT_TYPE, account.mType);
        }
        return true;
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertContact(ContentValues values) {
        throw new UnsupportedOperationException("Aggregates are created automatically");
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @param account the account this contact should be associated with. may be null.
     * @return the row ID of the newly created row
     */
    private long insertRawContact(ContentValues values, Account account) {
        /*
         * The contact record is inserted in the contacts table, but it needs to
         * be processed by the aggregator before it will be returned by the
         * "aggregates" queries.
         */
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues overriddenValues = new ContentValues(values);
        overriddenValues.putNull(RawContacts.CONTACT_ID);
        if (!resolveAccount(overriddenValues, account)) {
            return -1;
        }

        if (values.containsKey(RawContacts.DELETED)
                && values.getAsInteger(RawContacts.DELETED) != 0) {
            overriddenValues.put(RawContacts.AGGREGATION_MODE,
                    RawContacts.AGGREGATION_MODE_DISABLED);
        }

        return db.insert(Tables.RAW_CONTACTS, RawContacts.CONTACT_ID, overriddenValues);
    }

    /**
     * Inserts an item in the data table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertData(ContentValues values, boolean markRawContactAsDirty) {
        int aggregationMode = RawContacts.AGGREGATION_MODE_DISABLED;

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            mValues.clear();
            mValues.putAll(values);

            long rawContactId = mValues.getAsLong(Data.RAW_CONTACT_ID);

            // Replace package with internal mapping
            final String packageName = mValues.getAsString(Data.RES_PACKAGE);
            if (packageName != null) {
                mValues.put(DataColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
            }
            mValues.remove(Data.RES_PACKAGE);

            // Replace mimetype with internal mapping
            final String mimeType = mValues.getAsString(Data.MIMETYPE);
            if (TextUtils.isEmpty(mimeType)) {
                throw new IllegalArgumentException(Data.MIMETYPE + " is required");
            }

            mValues.put(DataColumns.MIMETYPE_ID, mOpenHelper.getMimeTypeId(mimeType));
            mValues.remove(Data.MIMETYPE);

            // TODO create GroupMembershipRowHandler and move this code there
            resolveGroupSourceIdInValues(rawContactId, mimeType, db, mValues, true /* isInsert */);

            id = getDataRowHandler(mimeType).insert(db, rawContactId, mValues);
            if (markRawContactAsDirty) {
                setRawContactDirty(rawContactId);
            }

            aggregationMode = mContactAggregator.markContactForAggregation(rawContactId);

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        triggerAggregation(id, aggregationMode);
        return id;
    }

    private void triggerAggregation(long rawContactId, int aggregationMode) {
        switch (aggregationMode) {
            case RawContacts.AGGREGATION_MODE_DEFAULT:
                mContactAggregator.schedule();
                break;

            case RawContacts.AGGREGATION_MODE_IMMEDITATE:
                mContactAggregator.aggregateContact(rawContactId);
                break;

            case RawContacts.AGGREGATION_MODE_DISABLED:
                // Do nothing
                break;
        }
    }

    /**
     * Returns the group id of the group with sourceId and the same account as rawContactId.
     * If the group doesn't already exist then it is first created,
     * @param db SQLiteDatabase to use for this operation
     * @param rawContactId the contact this group is associated with
     * @param sourceId the sourceIf of the group to query or create
     * @return the group id of the existing or created group
     * @throws IllegalArgumentException if the contact is not associated with an account
     * @throws IllegalStateException if a group needs to be created but the creation failed
     */
    private long getOrMakeGroup(SQLiteDatabase db, long rawContactId, String sourceId) {
        Account account = null;
        Cursor c = db.query(ContactsQuery.TABLE, ContactsQuery.PROJECTION, RawContacts._ID + "="
                + rawContactId, null, null, null, null);
        try {
            if (c.moveToNext()) {
                final String accountName = c.getString(ContactsQuery.ACCOUNT_NAME);
                final String accountType = c.getString(ContactsQuery.ACCOUNT_TYPE);
                if (!TextUtils.isEmpty(accountName) && !TextUtils.isEmpty(accountType)) {
                    account = new Account(accountName, accountType);
                }
            }
        } finally {
            c.close();
        }
        if (account == null) {
            throw new IllegalArgumentException("if the groupmembership only "
                    + "has a sourceid the the contact must be associate with "
                    + "an account");
        }

        // look up the group that contains this sourceId and has the same account name and type
        // as the contact refered to by rawContactId
        c = db.query(Tables.GROUPS, new String[]{RawContacts._ID},
                Clauses.GROUP_HAS_ACCOUNT_AND_SOURCE_ID,
                new String[]{sourceId, account.mName, account.mType}, null, null, null);
        try {
            if (c.moveToNext()) {
                return c.getLong(0);
            } else {
                ContentValues groupValues = new ContentValues();
                groupValues.put(Groups.ACCOUNT_NAME, account.mName);
                groupValues.put(Groups.ACCOUNT_TYPE, account.mType);
                groupValues.put(Groups.SOURCE_ID, sourceId);
                long groupId = db.insert(Tables.GROUPS, Groups.ACCOUNT_NAME, groupValues);
                if (groupId < 0) {
                    throw new IllegalStateException("unable to create a new group with "
                            + "this sourceid: " + groupValues);
                }
                return groupId;
            }
        } finally {
            c.close();
        }
    }

    /**
     * Delete data row by row so that fixing of primaries etc work correctly.
     */
    private int deleteData(String selection, String[] selectionArgs,
            boolean markRawContactAsDirty) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int count = 0;
        db.beginTransaction();
        try {

            // Note that the query will return data according to the access restrictions,
            // so we don't need to worry about deleting data we don't have permission to read.
            Cursor c = query(Data.CONTENT_URI, DataIdQuery.COLUMNS, selection, selectionArgs, null);
            try {
                while(c.moveToNext()) {
                    long dataId = c.getLong(DataIdQuery._ID);
                    count += deleteData(dataId, markRawContactAsDirty);
                }
            } finally {
                c.close();
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        return count;
    }

    @Deprecated
    public int deleteData(long dataId, String[] allowedMimeTypes) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        Cursor c = db.query(DataQuery.TABLE, DataQuery.COLUMNS,
                DataColumns.CONCRETE_ID + "=" + dataId, null, null, null, null);
        // TODO apply restrictions
        try {
            if (!c.moveToFirst()) {
                return 0;
            }

            String mimeType = c.getString(DataQuery.MIMETYPE);
            boolean valid = false;
            for (int i = 0; i < allowedMimeTypes.length; i++) {
                if (TextUtils.equals(mimeType, allowedMimeTypes[i])) {
                    valid = true;
                    break;
                }
            }

            if (!valid) {
                throw new IllegalArgumentException("Data type mismatch: expected "
                        + Lists.newArrayList(allowedMimeTypes));
            }

            return getDataRowHandler(mimeType).delete(db, c);
        } finally {
            c.close();
        }
    }

    /**
     * Delete the given {@link Data} row, fixing up any {@link Contacts}
     * primaries that reference it.
     */
    private int deleteData(long dataId, boolean markRawContactAsDirty) {

        // TODO redo this method with the use of DataRowHandlers
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final long mimePhone = mOpenHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);
        final long mimeEmail = mOpenHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);

        // Check to see if the data about to be deleted was a super-primary on
        // the parent aggregate, and set flags to fix-up once deleted.
        long contactId = -1;
        long rawContactId = -1;
        long mimeId = -1;
        String dataRaw = null;
        boolean fixOptimal = false;
        boolean fixFallback = false;

        // TODO check security
        Cursor cursor = null;
        try {
            cursor = db.query(DataContactsQuery.TABLE, DataContactsQuery.PROJECTION,
                    DataColumns.CONCRETE_ID + "=" + dataId, null, null, null, null);
            if (!cursor.moveToFirst()) {
                return 0;
            }

            contactId = cursor.getLong(DataContactsQuery.CONTACT_ID);
            rawContactId = cursor.getLong(DataContactsQuery.RAW_CONTACT_ID);
            mimeId = cursor.getLong(DataContactsQuery.MIMETYPE_ID);
            if (mimeId == mimePhone) {
                dataRaw = cursor.getString(DataContactsQuery.PHONE_NUMBER);
                fixOptimal = (cursor.getLong(DataContactsQuery.OPTIMAL_PHONE_ID) == dataId);
                fixFallback = (cursor.getLong(DataContactsQuery.FALLBACK_PHONE_ID) == dataId);
            } else if (mimeId == mimeEmail) {
                dataRaw = cursor.getString(DataContactsQuery.EMAIL_DATA);
                fixOptimal = (cursor.getLong(DataContactsQuery.OPTIMAL_EMAIL_ID) == dataId);
                fixFallback = (cursor.getLong(DataContactsQuery.FALLBACK_EMAIL_ID) == dataId);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }

        // Delete the requested data item.
        int dataDeleted = db.delete(Tables.DATA, Data._ID + "=" + dataId, null);
        if (markRawContactAsDirty) {
            setRawContactDirty(rawContactId);
        }

        // Fix-up any super-primary values that are now invalid.
        if (fixOptimal || fixFallback) {
            final ContentValues values = new ContentValues();
            final StringBuilder scoreClause = new StringBuilder();

            final String SCORE = "score";

            // Build scoring clause that will first pick data items under the
            // same aggregate that have identical values, otherwise fall back to
            // normal primary scoring from the member contacts.
            scoreClause.append("(CASE WHEN ");
            if (mimeId == mimePhone) {
                scoreClause.append(Phone.NUMBER);
            } else if (mimeId == mimeEmail) {
                scoreClause.append(Email.DATA);
            }
            scoreClause.append("=");
            DatabaseUtils.appendEscapedSQLString(scoreClause, dataRaw);
            scoreClause.append(" THEN 2 ELSE " + Data.IS_PRIMARY + " END) AS " + SCORE);

            final String[] PROJ_PRIMARY = new String[] {
                    DataColumns.CONCRETE_ID,
                    RawContacts.IS_RESTRICTED,
                    scoreClause.toString(),
            };

            final int COL_DATA_ID = 0;
            final int COL_IS_RESTRICTED = 1;
            final int COL_SCORE = 2;

            cursor = db.query(Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS, PROJ_PRIMARY,
                    ContactsColumns.CONCRETE_ID + "=" + contactId + " AND " + DataColumns.MIMETYPE_ID
                            + "=" + mimeId, null, null, null, SCORE);

            if (fixOptimal) {
                String colId = null;
                String colIsRestricted = null;
                if (mimeId == mimePhone) {
                    colId = ContactsColumns.OPTIMAL_PRIMARY_PHONE_ID;
                    colIsRestricted = ContactsColumns.OPTIMAL_PRIMARY_PHONE_IS_RESTRICTED;
                } else if (mimeId == mimeEmail) {
                    colId = ContactsColumns.OPTIMAL_PRIMARY_EMAIL_ID;
                    colIsRestricted = ContactsColumns.OPTIMAL_PRIMARY_EMAIL_IS_RESTRICTED;
                }

                // Start by replacing with null, since fixOptimal told us that
                // the previous aggregate values are bad.
                values.putNull(colId);
                values.putNull(colIsRestricted);

                // When finding a new optimal primary, we only care about the
                // highest scoring value, regardless of source.
                if (cursor.moveToFirst()) {
                    final long newOptimal = cursor.getLong(COL_DATA_ID);
                    final long newIsRestricted = cursor.getLong(COL_IS_RESTRICTED);

                    if (newOptimal != 0) {
                        values.put(colId, newOptimal);
                    }
                    if (newIsRestricted != 0) {
                        values.put(colIsRestricted, newIsRestricted);
                    }
                }
            }

            if (fixFallback) {
                String colId = null;
                if (mimeId == mimePhone) {
                    colId = ContactsColumns.FALLBACK_PRIMARY_PHONE_ID;
                } else if (mimeId == mimeEmail) {
                    colId = ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID;
                }

                // Start by replacing with null, since fixFallback told us that
                // the previous aggregate values are bad.
                values.putNull(colId);

                // The best fallback value is the highest scoring data item that
                // hasn't been restricted.
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    final boolean isRestricted = (cursor.getInt(COL_IS_RESTRICTED) == 1);
                    if (!isRestricted) {
                        values.put(colId, cursor.getLong(COL_DATA_ID));
                        break;
                    }
                }
            }

            // Push through any contact updates we have
            if (values.size() > 0) {
                db.update(Tables.CONTACTS, values, ContactsColumns.CONCRETE_ID + "=" + contactId,
                        null);
            }
        }

        return dataDeleted;
    }

    /**
     * Inserts an item in the groups table
     */
    private long insertGroup(ContentValues values, Account account, boolean markAsDirty) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues overriddenValues = new ContentValues(values);
        if (!resolveAccount(overriddenValues, account)) {
            return -1;
        }

        // Replace package with internal mapping
        final String packageName = overriddenValues.getAsString(Groups.RES_PACKAGE);
        if (packageName != null) {
            overriddenValues.put(GroupsColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
        }
        overriddenValues.remove(Groups.RES_PACKAGE);

        if (markAsDirty) {
            overriddenValues.put(Groups.DIRTY, 1);
        }

        return db.insert(Tables.GROUPS, Groups.TITLE, overriddenValues);
    }

    /**
     * Inserts a presence update.
     */
    public long insertPresence(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final String handle = values.getAsString(Presence.IM_HANDLE);
        final String protocol = values.getAsString(Presence.IM_PROTOCOL);
        if (TextUtils.isEmpty(handle) || TextUtils.isEmpty(protocol)) {
            throw new IllegalArgumentException("IM_PROTOCOL and IM_HANDLE are required");
        }

        // TODO: generalize to allow other providers to match against email
        boolean matchEmail = Im.PROTOCOL_GOOGLE_TALK == Integer.parseInt(protocol);

        StringBuilder selection = new StringBuilder();
        String[] selectionArgs;
        if (matchEmail) {
            selection.append("(" + Clauses.WHERE_IM_MATCHES + ") OR ("
                    + Clauses.WHERE_EMAIL_MATCHES + ")");
            selectionArgs = new String[] { protocol, handle, handle };
        } else {
            selection.append(Clauses.WHERE_IM_MATCHES);
            selectionArgs = new String[] { protocol, handle };
        }

        if (values.containsKey(Presence.DATA_ID)) {
            selection.append(" AND " + DataColumns.CONCRETE_ID + "=")
                    .append(values.getAsLong(Presence.DATA_ID));
        }

        if (values.containsKey(Presence.RAW_CONTACT_ID)) {
            selection.append(" AND " + DataColumns.CONCRETE_RAW_CONTACT_ID + "=")
                    .append(values.getAsLong(Presence.RAW_CONTACT_ID));
        }

        selection.append(" AND ").append(getContactsRestrictions());

        long dataId = -1;
        long rawContactId = -1;

        Cursor cursor = null;
        try {
            cursor = db.query(DataContactsQuery.TABLE, DataContactsQuery.PROJECTION,
                    selection.toString(), selectionArgs, null, null, null);
            if (cursor.moveToFirst()) {
                dataId = cursor.getLong(DataContactsQuery.DATA_ID);
                rawContactId = cursor.getLong(DataContactsQuery.RAW_CONTACT_ID);
            } else {
                // No contact found, return a null URI
                return -1;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        values.put(Presence.DATA_ID, dataId);
        values.put(Presence.RAW_CONTACT_ID, rawContactId);

        // Insert the presence update
        long presenceId = db.replace(Tables.PRESENCE, null, values);
        return presenceId;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mOpenHelper.getSyncState().delete(db, selection, selectionArgs);

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);

                // Remove references to the contact first
                ContentValues values = new ContentValues();
                values.putNull(RawContacts.CONTACT_ID);
                db.update(Tables.RAW_CONTACTS, values,
                        RawContacts.CONTACT_ID + "=" + contactId, null);

                return db.delete(Tables.CONTACTS, BaseColumns._ID + "=" + contactId, null);
            }

            case RAW_CONTACTS_ID: {
                return deleteRawContact(uri);
            }

            case DATA: {
                return deleteData(selection, selectionArgs, shouldMarkRawContactAsDirty(uri));
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                return deleteData(dataId, shouldMarkRawContactAsDirty(uri));
            }

            case GROUPS_ID: {
                return deleteGroup(uri);
            }

            case PRESENCE: {
                return db.delete(Tables.PRESENCE, null, null);
            }

            default:
                return mLegacyApiSupport.delete(uri, selection, selectionArgs);
        }
    }

    private int deleteGroup(Uri uri) {
        final String flag = uri.getQueryParameter(Groups.DELETE_PERMANENTLY);
        final boolean permanently = flag != null && "true".equals(flag.toLowerCase());
        boolean markAsDirty = shouldMarkGroupAsDirty(uri);
        return deleteGroup(ContentUris.parseId(uri), markAsDirty, permanently);
    }

    private int deleteGroup(long groupId, boolean markAsDirty, boolean permanently) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final long groupMembershipMimetypeId = mOpenHelper
                .getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        db.delete(Tables.DATA, DataColumns.MIMETYPE_ID + "="
                + groupMembershipMimetypeId + " AND " + GroupMembership.GROUP_ROW_ID + "="
                + groupId, null);

        try {
            if (permanently) {
                return db.delete(Tables.GROUPS, Groups._ID + "=" + groupId, null);
            } else {
                mValues.clear();
                mValues.put(Groups.DELETED, 1);
                if (markAsDirty) {
                    mValues.put(Groups.DIRTY, 1);
                }
                return db.update(Tables.GROUPS, mValues, Groups._ID + "=" + groupId, null);
            }
        } finally {
            mOpenHelper.updateAllVisible();
        }
    }

    private int deleteRawContact(Uri uri) {
        final String flag = uri.getQueryParameter(RawContacts.DELETE_PERMANENTLY);
        final boolean permanently = flag != null && "true".equals(flag.toLowerCase());
        final long rawContactId = ContentUris.parseId(uri);
        return deleteRawContact(rawContactId, permanently);
    }

    public int deleteRawContact(long rawContactId, boolean permanently) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        // TODO delete aggregation exceptions
        mOpenHelper.removeContactIfSingleton(rawContactId);
        if (permanently) {
            db.delete(Tables.PRESENCE, Presence.RAW_CONTACT_ID + "=" + rawContactId, null);
            return db.delete(Tables.RAW_CONTACTS, RawContacts._ID + "=" + rawContactId, null);
        } else {
            mValues.clear();
            mValues.put(RawContacts.DELETED, 1);
            mValues.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DISABLED);
            mValues.putNull(RawContacts.CONTACT_ID);
            mValues.put(RawContacts.DIRTY, 1);
            return updateRawContact(rawContactId, mValues, null, null);
        }
    }

    private static Account readAccountFromQueryParams(Uri uri) {
        final String name = uri.getQueryParameter(RawContacts.ACCOUNT_NAME);
        final String type = uri.getQueryParameter(RawContacts.ACCOUNT_TYPE);
        if (TextUtils.isEmpty(name) || TextUtils.isEmpty(type)) {
            return null;
        }
        return new Account(name, type);
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int count = 0;

        final int match = sUriMatcher.match(uri);
        switch(match) {
            case SYNCSTATE:
                return mOpenHelper.getSyncState().update(db, values, selection, selectionArgs);

            // TODO(emillar): We will want to disallow editing the contacts table at some point.
            case CONTACTS: {
                count = db.update(Tables.CONTACTS, values, selection, selectionArgs);
                break;
            }

            case CONTACTS_ID: {
                count = updateContactData(db, ContentUris.parseId(uri), values);
                break;
            }

            case DATA: {
                count = updateData(uri, values, selection, selectionArgs,
                        shouldMarkRawContactAsDirty(uri));
                break;
            }

            case DATA_ID: {
                count = updateData(uri, values, selection, selectionArgs,
                        shouldMarkRawContactAsDirty(uri));
                break;
            }

            case RAW_CONTACTS: {

                // TODO: security checks
                count = db.update(Tables.RAW_CONTACTS, values, selection, selectionArgs);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                count = updateRawContact(rawContactId, values, selection, selectionArgs);
                break;
            }

            case GROUPS: {
                count = updateGroups(db, values, selection, selectionArgs,
                        shouldMarkGroupAsDirty(uri));
                break;
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                String selectionWithId = (Groups._ID + "=" + groupId + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = updateGroups(db, values, selectionWithId, selectionArgs,
                        shouldMarkGroupAsDirty(uri));
                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                count = updateAggregationException(db, values);
                break;
            }

            default:
                return mLegacyApiSupport.update(uri, values, selection, selectionArgs);
        }

        if (count > 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return count;
    }

    private int updateGroups(SQLiteDatabase db, ContentValues values, String selectionWithId,
            String[] selectionArgs, boolean markAsDirty) {

        ContentValues updatedValues;
        if (markAsDirty) {
            updatedValues = mValues;
            updatedValues.clear();
            updatedValues.putAll(values);
            updatedValues.put(Groups.DIRTY, 1);
        } else {
            updatedValues = values;
        }

        int count = db.update(Tables.GROUPS, values, selectionWithId, selectionArgs);

        // If changing visibility, then update contacts
        if (values.containsKey(Groups.GROUP_VISIBLE)) {
            mOpenHelper.updateAllVisible();
        }
        return count;
    }

    private int updateRawContact(long rawContactId, ContentValues values, String selection,
            String[] selectionArgs) {

        // TODO: security checks
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        String selectionWithId = (RawContacts._ID + " = " + rawContactId + " ")
                + (selection == null ? "" : " AND " + selection);
        return db.update(Tables.RAW_CONTACTS, values, selectionWithId, selectionArgs);
    }

    private int updateData(Uri uri, ContentValues values, String selection,
            String[] selectionArgs, boolean markRawContactAsDirty) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int count = 0;
        db.beginTransaction();
        try {
            // Note that the query will return data according to the access restrictions,
            // so we don't need to worry about updating data we don't have permission to read.
            Cursor c = query(uri, DataIdQuery.COLUMNS, selection, selectionArgs, null);
            try {
                while(c.moveToNext()) {
                    final long dataId = c.getLong(DataIdQuery._ID);
                    final long rawContactId = c.getLong(DataIdQuery.RAW_CONTACT_ID);
                    final String mimetype = c.getString(DataIdQuery.MIMETYPE);
                    count += updateData(dataId, rawContactId, mimetype, values,
                            markRawContactAsDirty);
                }
            } finally {
                c.close();
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        return count;
    }

    private int updateData(long dataId, long rawContactId, String mimeType, ContentValues values,
            boolean markRawContactAsDirty) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        mValues.clear();
        mValues.putAll(values);
        mValues.remove(Data._ID);
        mValues.remove(Data.RAW_CONTACT_ID);
        mValues.remove(Data.MIMETYPE);

        String packageName = values.getAsString(Data.RES_PACKAGE);
        if (packageName != null) {
            mValues.remove(Data.RES_PACKAGE);
            mValues.put(DataColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
        }

        boolean containsIsSuperPrimary = mValues.containsKey(Data.IS_SUPER_PRIMARY);
        boolean containsIsPrimary = mValues.containsKey(Data.IS_PRIMARY);

        // Remove primary or super primary values being set to 0. This is disallowed by the
        // content provider.
        if (containsIsSuperPrimary && mValues.getAsInteger(Data.IS_SUPER_PRIMARY) == 0) {
            containsIsSuperPrimary = false;
            mValues.remove(Data.IS_SUPER_PRIMARY);
        }
        if (containsIsPrimary && mValues.getAsInteger(Data.IS_PRIMARY) == 0) {
            containsIsPrimary = false;
            mValues.remove(Data.IS_PRIMARY);
        }

        if (containsIsSuperPrimary) {
            setIsSuperPrimary(dataId);
            setIsPrimary(dataId);

            // Now that we've taken care of setting these, remove them from "values".
            mValues.remove(Data.IS_SUPER_PRIMARY);
            if (containsIsPrimary) {
                mValues.remove(Data.IS_PRIMARY);
            }
        } else if (containsIsPrimary) {
            setIsPrimary(dataId);

            // Now that we've taken care of setting this, remove it from "values".
            mValues.remove(Data.IS_PRIMARY);
        }

        // TODO create GroupMembershipRowHandler and move this code there
        resolveGroupSourceIdInValues(rawContactId, mimeType, db, mValues, false /* isInsert */);

        if (mValues.size() > 0) {
            db.update(Tables.DATA, mValues, Data._ID + " = " + dataId, null);
            if (markRawContactAsDirty) {
                setRawContactDirty(rawContactId);
            }

            return 1;
        }
        return 0;
    }

    private void resolveGroupSourceIdInValues(long rawContactId, String mimeType, SQLiteDatabase db,
            ContentValues values, boolean isInsert) {
        if (GroupMembership.CONTENT_ITEM_TYPE.equals(mimeType)) {
            boolean containsGroupSourceId = values.containsKey(GroupMembership.GROUP_SOURCE_ID);
            boolean containsGroupId = values.containsKey(GroupMembership.GROUP_ROW_ID);
            if (containsGroupSourceId && containsGroupId) {
                throw new IllegalArgumentException(
                        "you are not allowed to set both the GroupMembership.GROUP_SOURCE_ID "
                                + "and GroupMembership.GROUP_ROW_ID");
            }

            if (!containsGroupSourceId && !containsGroupId) {
                if (isInsert) {
                    throw new IllegalArgumentException(
                            "you must set exactly one of GroupMembership.GROUP_SOURCE_ID "
                                    + "and GroupMembership.GROUP_ROW_ID");
                } else {
                    return;
                }
            }

            if (containsGroupSourceId) {
                final String sourceId = values.getAsString(GroupMembership.GROUP_SOURCE_ID);
                final long groupId = getOrMakeGroup(db, rawContactId, sourceId);
                values.remove(GroupMembership.GROUP_SOURCE_ID);
                values.put(GroupMembership.GROUP_ROW_ID, groupId);
            }
        }
    }

    private int updateContactData(SQLiteDatabase db, long contactId, ContentValues values) {

        // First update all constituent contacts
        ContentValues optionValues = new ContentValues(5);
        OpenHelper.copyStringValue(optionValues, RawContacts.CUSTOM_RINGTONE,
                values, Contacts.CUSTOM_RINGTONE);
        OpenHelper.copyLongValue(optionValues, RawContacts.SEND_TO_VOICEMAIL,
                values, Contacts.SEND_TO_VOICEMAIL);
        OpenHelper.copyLongValue(optionValues, RawContacts.LAST_TIME_CONTACTED,
                values, Contacts.LAST_TIME_CONTACTED);
        OpenHelper.copyLongValue(optionValues, RawContacts.TIMES_CONTACTED,
                values, Contacts.TIMES_CONTACTED);
        OpenHelper.copyLongValue(optionValues, RawContacts.STARRED,
                values, Contacts.STARRED);

        // Nothing to update - just return
        if (optionValues.size() == 0) {
            return 0;
        }

        db.update(Tables.RAW_CONTACTS, optionValues,
                RawContacts.CONTACT_ID + "=" + contactId, null);
        return db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
    }

    public void updateContactTime(long contactId, long lastTimeContacted) {
        mLastTimeContactedUpdate.bindLong(1, lastTimeContacted);
        mLastTimeContactedUpdate.bindLong(2, contactId);
        mLastTimeContactedUpdate.execute();
    }

    private static class RawContactPair {
        final long rawContactId1;
        final long rawContactId2;

        /**
         * Constructor that ensures that this.rawContactId1 &lt; this.rawContactId2
         */
        public RawContactPair(long rawContactId1, long rawContactId2) {
            if (rawContactId1 < rawContactId2) {
                this.rawContactId1 = rawContactId1;
                this.rawContactId2 = rawContactId2;
            } else {
                this.rawContactId2 = rawContactId1;
                this.rawContactId1 = rawContactId2;
            }
        }
    }

    private int updateAggregationException(SQLiteDatabase db, ContentValues values) {
        int exceptionType = values.getAsInteger(AggregationExceptions.TYPE);
        long contactId = values.getAsInteger(AggregationExceptions.CONTACT_ID);
        long rawContactId = values.getAsInteger(AggregationExceptions.RAW_CONTACT_ID);

        // First, we build a list of rawContactID-rawContactID pairs for the given contact.
        ArrayList<RawContactPair> pairs = new ArrayList<RawContactPair>();
        Cursor c = db.query(ContactsQuery.TABLE, ContactsQuery.PROJECTION, RawContacts.CONTACT_ID
                + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long aggregatedContactId = c.getLong(ContactsQuery.RAW_CONTACT_ID);
                if (aggregatedContactId != rawContactId) {
                    pairs.add(new RawContactPair(aggregatedContactId, rawContactId));
                }
            }
        } finally {
            c.close();
        }

        // Now we iterate through all contact pairs to see if we need to insert/delete/update
        // the corresponding exception
        ContentValues exceptionValues = new ContentValues(3);
        exceptionValues.put(AggregationExceptions.TYPE, exceptionType);
        for (RawContactPair pair : pairs) {
            final String whereClause =
                    AggregationExceptionColumns.RAW_CONTACT_ID1 + "=" + pair.rawContactId1 + " AND "
                    + AggregationExceptionColumns.RAW_CONTACT_ID2 + "=" + pair.rawContactId2;
            if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC) {
                db.delete(Tables.AGGREGATION_EXCEPTIONS, whereClause, null);
            } else {
                exceptionValues.put(AggregationExceptionColumns.RAW_CONTACT_ID1, pair.rawContactId1);
                exceptionValues.put(AggregationExceptionColumns.RAW_CONTACT_ID2, pair.rawContactId2);
                db.replace(Tables.AGGREGATION_EXCEPTIONS, AggregationExceptions._ID,
                        exceptionValues);
            }
        }

        int aggregationMode = mContactAggregator.markContactForAggregation(rawContactId);
        if (aggregationMode != RawContacts.AGGREGATION_MODE_DISABLED) {
            mContactAggregator.aggregateContact(db, rawContactId);
            if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC
                    || exceptionType == AggregationExceptions.TYPE_KEEP_OUT) {
                mContactAggregator.updateAggregateData(contactId);
            }
        }

        // The return value is fake - we just confirm that we made a change, not count actual
        // rows changed.
        return 1;
    }

    /**
     * Test if a {@link String} value appears in the given list.
     */
    private boolean isContained(String[] array, String value) {
        if (array != null) {
            for (String test : array) {
                if (value.equals(test)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Test if a {@link String} value appears in the given list, and add to the
     * array if the value doesn't already appear.
     */
    private String[] assertContained(String[] array, String value) {
        if (array != null && !isContained(array, value)) {
            String[] newArray = new String[array.length + 1];
            System.arraycopy(array, 0, newArray, 0, array.length);
            newArray[array.length] = value;
            array = newArray;
        }
        return array;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;
        String limit = getLimit(uri);

        // TODO: Consider writing a test case for RestrictionExceptions when you
        // write a new query() block to make sure it protects restricted data.
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mOpenHelper.getSyncState().query(db, projection, selection,  selectionArgs,
                        sortOrder);

            case CONTACTS: {
                qb.setTables(mOpenHelper.getContactView());
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                qb.setTables(mOpenHelper.getContactView());
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere(Contacts._ID + "=" + contactId);
                break;
            }

            case CONTACTS_SUMMARY: {
                // TODO: join into social status tables
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                groupBy = Contacts._ID;
                break;
            }

            case CONTACTS_SUMMARY_ID: {
                // TODO: join into social status tables
                long contactId = ContentUris.parseId(uri);
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                groupBy = Contacts._ID;
                qb.appendWhere(Contacts._ID + "=" + contactId);
                break;
            }

            case CONTACTS_SUMMARY_FILTER: {
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                groupBy = Contacts._ID;

                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append("raw_contact_id IN ");
                    appendRawContactsByFilterAsNestedQuery(sb, filterParam, null);
                    qb.appendWhere(sb.toString());
                }
                break;
            }

            case CONTACTS_SUMMARY_STREQUENT_FILTER:
            case CONTACTS_SUMMARY_STREQUENT: {
                String filterSql = null;
                if (match == CONTACTS_SUMMARY_STREQUENT_FILTER
                        && uri.getPathSegments().size() > 3) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append("raw_contact_id IN ");
                    appendRawContactsByFilterAsNestedQuery(sb, filterParam, null);
                    filterSql = sb.toString();
                }

                // Build the first query for starred
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                if (filterSql != null) {
                    qb.appendWhere(filterSql);
                }
                final String starredQuery = qb.buildQuery(projection, Contacts.STARRED + "=1",
                        null, Contacts._ID, null, null, null);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                if (filterSql != null) {
                    qb.appendWhere(filterSql);
                }
                final String frequentQuery = qb.buildQuery(projection,
                        Contacts.TIMES_CONTACTED + " > 0 AND (" + Contacts.STARRED
                        + " = 0 OR " + Contacts.STARRED + " IS NULL)",
                        null, Contacts._ID, null, null, null);

                // Put them together
                final String query = qb.buildUnionQuery(new String[] {starredQuery, frequentQuery},
                        STREQUENT_ORDER_BY, STREQUENT_LIMIT);
                Cursor c = db.rawQuery(query, null);
                if (c != null) {
                    c.setNotificationUri(getContext().getContentResolver(),
                            ContactsContract.AUTHORITY_URI);
                }
                return c;
            }

            case CONTACTS_SUMMARY_GROUP: {
                qb.setTables(mOpenHelper.getContactSummaryView());
                qb.setProjectionMap(sContactsSummaryProjectionMap);
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(sContactsInGroupSelect);
                    selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                }
                groupBy = Contacts._ID;
                break;
            }

            case CONTACTS_DATA: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));

                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                appendAccountFromParameter(qb, uri);
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=" + contactId);
                break;
            }

            case PHONES: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case PHONES_FILTER: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append(Data.RAW_CONTACT_ID + " IN ");
                    appendRawContactsByFilterAsNestedQuery(sb, filterParam, null);
                    qb.appendWhere(" AND " + sb);
                }
                break;
            }

            case EMAILS: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case EMAILS_FILTER: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(" AND " + CommonDataKinds.Email.DATA + "=");
                    qb.appendWhereEscapeString(uri.getLastPathSegment());
                }
                break;
            }

            case POSTALS: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + StructuredPostal.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case RAW_CONTACTS: {
                qb.setTables(mOpenHelper.getRawContactView());
                qb.setProjectionMap(sRawContactsProjectionMap);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                qb.setTables(mOpenHelper.getRawContactView());
                qb.setProjectionMap(sRawContactsProjectionMap);
                qb.appendWhere(RawContacts._ID + "=" + rawContactId);
                break;
            }

            case RAW_CONTACTS_DATA: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data.RAW_CONTACT_ID + "=" + rawContactId);
                break;
            }

            case DATA: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                appendAccountFromParameter(qb, uri);
                break;
            }

            case DATA_ID: {
                qb.setTables(mOpenHelper.getDataView());
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere(Data._ID + "=" + ContentUris.parseId(uri));
                break;
            }

            case PHONE_LOOKUP: {

                // TODO: optimize this query returning fewer fields and using an int mimetype
                // TODO: filter query based on callingUid
                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = Data.RAW_CONTACT_ID;
                }

                final String number = uri.getLastPathSegment();
                OpenHelper.buildPhoneLookupQuery(qb, number);
                qb.setProjectionMap(sDataRawContactsProjectionMap);
                break;
            }

            case GROUPS: {
                qb.setTables(Tables.GROUPS_JOIN_PACKAGES);
                qb.setProjectionMap(sGroupsProjectionMap);
                break;
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                qb.setTables(Tables.GROUPS_JOIN_PACKAGES);
                qb.setProjectionMap(sGroupsProjectionMap);
                qb.appendWhere(GroupsColumns.CONCRETE_ID + "=" + groupId);
                break;
            }

            case GROUPS_SUMMARY: {
                qb.setTables(Tables.GROUPS_JOIN_PACKAGES);
                qb.setProjectionMap(sGroupsSummaryProjectionMap);
                groupBy = GroupsColumns.CONCRETE_ID;
                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                qb.setTables(Tables.AGGREGATION_EXCEPTIONS_JOIN_RAW_CONTACTS);
                qb.setProjectionMap(sAggregationExceptionsProjectionMap);
                break;
            }

            case AGGREGATION_SUGGESTIONS: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                final int maxSuggestions;
                if (limit != null) {
                    maxSuggestions = Integer.parseInt(limit);
                } else {
                    maxSuggestions = DEFAULT_MAX_SUGGESTIONS;
                }

                return mContactAggregator.queryAggregationSuggestions(contactId, projection,
                        sContactsProjectionMap, maxSuggestions);
            }

            case PRESENCE: {
                qb.setTables(Tables.PRESENCE);
                qb.setProjectionMap(sPresenceProjectionMap);
                break;
            }

            case PRESENCE_ID: {
                qb.setTables(Tables.PRESENCE);
                qb.setProjectionMap(sPresenceProjectionMap);
                qb.appendWhere(Presence._ID + "=" + ContentUris.parseId(uri));
                break;
            }

            case SEARCH_SUGGESTIONS: {
                return mGlobalSearchSupport.handleSearchSuggestionsQuery(db, uri, limit);
            }

            case SEARCH_SHORTCUT: {
                // TODO
                break;
            }

            default:
                return mLegacyApiSupport.query(uri, projection, selection, selectionArgs,
                        sortOrder, limit);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs,
                groupBy, null, sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    private void appendAccountFromParameter(SQLiteQueryBuilder qb, Uri uri) {
        final String accountName = uri.getQueryParameter(RawContacts.ACCOUNT_NAME);
        final String accountType = uri.getQueryParameter(RawContacts.ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName)) {
            qb.appendWhere(RawContacts.ACCOUNT_NAME + "="
                    + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                    + RawContacts.ACCOUNT_TYPE + "="
                    + DatabaseUtils.sqlEscapeString(accountType));
        } else {
            qb.appendWhere("1");
        }
    }

    /**
     * Gets the value of the "limit" URI query parameter.
     *
     * @return A string containing a non-negative integer, or <code>null</code> if
     *         the parameter is not set, or is set to an invalid value.
     */
    private String getLimit(Uri url) {
        String limitParam = url.getQueryParameter("limit");
        if (limitParam == null) {
            return null;
        }
        // make sure that the limit is a non-negative integer
        try {
            int l = Integer.parseInt(limitParam);
            if (l < 0) {
                Log.w(TAG, "Invalid limit parameter: " + limitParam);
                return null;
            }
            return String.valueOf(l);
        } catch (NumberFormatException ex) {
            Log.w(TAG, "Invalid limit parameter: " + limitParam);
            return null;
        }
    }

    String getContactsRestrictions() {
        if (mOpenHelper.hasRestrictedAccess()) {
            return "1";
        } else {
            return RawContacts.IS_RESTRICTED + "=0";
        }
    }

    public String getContactsRestrictionExceptionAsNestedQuery(String contactIdColumn) {
        if (mOpenHelper.hasRestrictedAccess()) {
            return "1";
        } else {
            return "(SELECT " + RawContacts.IS_RESTRICTED + " FROM " + Tables.RAW_CONTACTS
                    + " WHERE " + RawContactsColumns.CONCRETE_ID + "=" + contactIdColumn + ")=0";
        }
    }

    /**
     * Find any exceptions that have been granted to the
     * {@link Binder#getCallingUid()}, and add a limiting clause to the given
     * {@link SQLiteQueryBuilder} to hide restricted data.
     */
    void applyDataRestrictionExceptions(SQLiteQueryBuilder qb) {
        String restrictions = getContactsRestrictions();
        qb.appendWhere(restrictions);
    }

    /**
     * An implementation of EntityIterator that joins the contacts and data tables
     * and consumes all the data rows for a contact in order to build the Entity for a contact.
     */
    private static class ContactsEntityIterator implements EntityIterator {
        private final Cursor mEntityCursor;
        private volatile boolean mIsClosed;

        private static final String[] DATA_KEYS = new String[]{
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4};

        private static final String[] PROJECTION = new String[]{
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.SOURCE_ID,
                RawContacts.VERSION,
                RawContacts.DIRTY,
                Data._ID,
                Data.RES_PACKAGE,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
                Data.RAW_CONTACT_ID,
                Data.IS_PRIMARY,
                Data.DATA_VERSION,
                GroupMembership.GROUP_SOURCE_ID,
                RawContacts.SYNC1,
                RawContacts.SYNC2,
                RawContacts.SYNC3,
                RawContacts.SYNC4,
                RawContacts.DELETED};

        private static final int COLUMN_ACCOUNT_NAME = 0;
        private static final int COLUMN_ACCOUNT_TYPE = 1;
        private static final int COLUMN_SOURCE_ID = 2;
        private static final int COLUMN_VERSION = 3;
        private static final int COLUMN_DIRTY = 4;
        private static final int COLUMN_DATA_ID = 5;
        private static final int COLUMN_RES_PACKAGE = 6;
        private static final int COLUMN_MIMETYPE = 7;
        private static final int COLUMN_DATA1 = 8;
        private static final int COLUMN_RAW_CONTACT_ID = 27;
        private static final int COLUMN_IS_PRIMARY = 28;
        private static final int COLUMN_DATA_VERSION = 29;
        private static final int COLUMN_GROUP_SOURCE_ID = 30;
        private static final int COLUMN_SYNC1 = 31;
        private static final int COLUMN_SYNC2 = 32;
        private static final int COLUMN_SYNC3 = 33;
        private static final int COLUMN_SYNC4 = 34;
        private static final int COLUMN_DELETED = 35;

        public ContactsEntityIterator(ContactsProvider2 provider, String contactsIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;

            final String updatedSortOrder = (sortOrder == null)
                    ? Data.RAW_CONTACT_ID
                    : (Data.RAW_CONTACT_ID + "," + sortOrder);

            final SQLiteDatabase db = provider.mOpenHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(Tables.CONTACT_ENTITIES);
            if (contactsIdString != null) {
                qb.appendWhere(Data.RAW_CONTACT_ID + "=" + contactsIdString);
            }
            final String accountName = uri.getQueryParameter(RawContacts.ACCOUNT_NAME);
            final String accountType = uri.getQueryParameter(RawContacts.ACCOUNT_TYPE);
            if (!TextUtils.isEmpty(accountName)) {
                qb.appendWhere(RawContacts.ACCOUNT_NAME + "="
                        + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                        + RawContacts.ACCOUNT_TYPE + "="
                        + DatabaseUtils.sqlEscapeString(accountType));
            }
            mEntityCursor = qb.query(db, PROJECTION, selection, selectionArgs,
                    null, null, updatedSortOrder);
            mEntityCursor.moveToFirst();
        }

        public void close() {
            if (mIsClosed) {
                throw new IllegalStateException("closing when already closed");
            }
            mIsClosed = true;
            mEntityCursor.close();
        }

        public boolean hasNext() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling hasNext() when the iterator is closed");
            }

            return !mEntityCursor.isAfterLast();
        }

        public Entity next() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling next() when the iterator is closed");
            }
            if (!hasNext()) {
                throw new IllegalStateException("you may only call next() if hasNext() is true");
            }

            final SQLiteCursor c = (SQLiteCursor) mEntityCursor;

            final long rawContactId = c.getLong(COLUMN_RAW_CONTACT_ID);

            // we expect the cursor is already at the row we need to read from
            ContentValues contactValues = new ContentValues();
            contactValues.put(RawContacts.ACCOUNT_NAME, c.getString(COLUMN_ACCOUNT_NAME));
            contactValues.put(RawContacts.ACCOUNT_TYPE, c.getString(COLUMN_ACCOUNT_TYPE));
            contactValues.put(RawContacts._ID, rawContactId);
            contactValues.put(RawContacts.DIRTY, c.getLong(COLUMN_DIRTY));
            contactValues.put(RawContacts.VERSION, c.getLong(COLUMN_VERSION));
            contactValues.put(RawContacts.SOURCE_ID, c.getString(COLUMN_SOURCE_ID));
            contactValues.put(RawContacts.SYNC1, c.getString(COLUMN_SYNC1));
            contactValues.put(RawContacts.SYNC2, c.getString(COLUMN_SYNC2));
            contactValues.put(RawContacts.SYNC3, c.getString(COLUMN_SYNC3));
            contactValues.put(RawContacts.SYNC4, c.getString(COLUMN_SYNC4));
            contactValues.put(RawContacts.DELETED, c.getLong(COLUMN_DELETED));
            Entity contact = new Entity(contactValues);

            // read data rows until the contact id changes
            do {
                if (rawContactId != c.getLong(COLUMN_RAW_CONTACT_ID)) {
                    break;
                }
                // add the data to to the contact
                ContentValues dataValues = new ContentValues();
                dataValues.put(Data._ID, c.getString(COLUMN_DATA_ID));
                dataValues.put(Data.RES_PACKAGE, c.getString(COLUMN_RES_PACKAGE));
                dataValues.put(Data.MIMETYPE, c.getString(COLUMN_MIMETYPE));
                dataValues.put(Data.IS_PRIMARY, c.getString(COLUMN_IS_PRIMARY));
                dataValues.put(Data.DATA_VERSION, c.getLong(COLUMN_DATA_VERSION));
                if (!c.isNull(COLUMN_GROUP_SOURCE_ID)) {
                    dataValues.put(GroupMembership.GROUP_SOURCE_ID,
                            c.getString(COLUMN_GROUP_SOURCE_ID));
                }
                dataValues.put(Data.DATA_VERSION, c.getLong(COLUMN_DATA_VERSION));
                for (int i = 0; i < DATA_KEYS.length; i++) {
                    final int columnIndex = i + COLUMN_DATA1;
                    String key = DATA_KEYS[i];
                    if (c.isNull(columnIndex)) {
                        // don't put anything
                    } else if (c.isLong(columnIndex)) {
                        dataValues.put(key, c.getLong(columnIndex));
                    } else if (c.isFloat(columnIndex)) {
                        dataValues.put(key, c.getFloat(columnIndex));
                    } else if (c.isString(columnIndex)) {
                        dataValues.put(key, c.getString(columnIndex));
                    } else if (c.isBlob(columnIndex)) {
                        dataValues.put(key, c.getBlob(columnIndex));
                    }
                }
                contact.addSubValue(Data.CONTENT_URI, dataValues);
            } while (mEntityCursor.moveToNext());

            return contact;
        }
    }

    /**
     * An implementation of EntityIterator that joins the contacts and data tables
     * and consumes all the data rows for a contact in order to build the Entity for a contact.
     */
    private static class GroupsEntityIterator implements EntityIterator {
        private final Cursor mEntityCursor;
        private volatile boolean mIsClosed;

        private static final String[] PROJECTION = new String[]{
                Groups._ID,
                Groups.ACCOUNT_NAME,
                Groups.ACCOUNT_TYPE,
                Groups.SOURCE_ID,
                Groups.DIRTY,
                Groups.VERSION,
                Groups.RES_PACKAGE,
                Groups.TITLE,
                Groups.TITLE_RES,
                Groups.GROUP_VISIBLE,
                Groups.SYNC1,
                Groups.SYNC2,
                Groups.SYNC3,
                Groups.SYNC4,
                Groups.SYSTEM_ID,
                Groups.NOTES,
                Groups.DELETED};

        private static final int COLUMN_ID = 0;
        private static final int COLUMN_ACCOUNT_NAME = 1;
        private static final int COLUMN_ACCOUNT_TYPE = 2;
        private static final int COLUMN_SOURCE_ID = 3;
        private static final int COLUMN_DIRTY = 4;
        private static final int COLUMN_VERSION = 5;
        private static final int COLUMN_RES_PACKAGE = 6;
        private static final int COLUMN_TITLE = 7;
        private static final int COLUMN_TITLE_RES = 8;
        private static final int COLUMN_GROUP_VISIBLE = 9;
        private static final int COLUMN_SYNC1 = 10;
        private static final int COLUMN_SYNC2 = 11;
        private static final int COLUMN_SYNC3 = 12;
        private static final int COLUMN_SYNC4 = 13;
        private static final int COLUMN_SYSTEM_ID = 14;
        private static final int COLUMN_NOTES = 15;
        private static final int COLUMN_DELETED = 16;

        public GroupsEntityIterator(ContactsProvider2 provider, String groupIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;

            final String updatedSortOrder = (sortOrder == null)
                    ? Groups._ID
                    : (Groups._ID + "," + sortOrder);

            final SQLiteDatabase db = provider.mOpenHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(Tables.GROUPS_JOIN_PACKAGES);
            qb.setProjectionMap(sGroupsProjectionMap);
            if (groupIdString != null) {
                qb.appendWhere(Groups._ID + "=" + groupIdString);
            }
            final String accountName = uri.getQueryParameter(Groups.ACCOUNT_NAME);
            final String accountType = uri.getQueryParameter(Groups.ACCOUNT_TYPE);
            if (!TextUtils.isEmpty(accountName)) {
                qb.appendWhere(Groups.ACCOUNT_NAME + "="
                        + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                        + Groups.ACCOUNT_TYPE + "="
                        + DatabaseUtils.sqlEscapeString(accountType));
            }
            mEntityCursor = qb.query(db, PROJECTION, selection, selectionArgs,
                    null, null, updatedSortOrder);
            mEntityCursor.moveToFirst();
        }

        public void close() {
            if (mIsClosed) {
                throw new IllegalStateException("closing when already closed");
            }
            mIsClosed = true;
            mEntityCursor.close();
        }

        public boolean hasNext() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling hasNext() when the iterator is closed");
            }

            return !mEntityCursor.isAfterLast();
        }

        public Entity next() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling next() when the iterator is closed");
            }
            if (!hasNext()) {
                throw new IllegalStateException("you may only call next() if hasNext() is true");
            }

            final SQLiteCursor c = (SQLiteCursor) mEntityCursor;

            final long groupId = c.getLong(COLUMN_ID);

            // we expect the cursor is already at the row we need to read from
            ContentValues groupValues = new ContentValues();
            groupValues.put(Groups.ACCOUNT_NAME, c.getString(COLUMN_ACCOUNT_NAME));
            groupValues.put(Groups.ACCOUNT_TYPE, c.getString(COLUMN_ACCOUNT_TYPE));
            groupValues.put(Groups._ID, groupId);
            groupValues.put(Groups.DIRTY, c.getLong(COLUMN_DIRTY));
            groupValues.put(Groups.VERSION, c.getLong(COLUMN_VERSION));
            groupValues.put(Groups.SOURCE_ID, c.getString(COLUMN_SOURCE_ID));
            groupValues.put(Groups.RES_PACKAGE, c.getString(COLUMN_RES_PACKAGE));
            groupValues.put(Groups.TITLE, c.getString(COLUMN_TITLE));
            groupValues.put(Groups.TITLE_RES, c.getString(COLUMN_TITLE_RES));
            groupValues.put(Groups.GROUP_VISIBLE, c.getLong(COLUMN_GROUP_VISIBLE));
            groupValues.put(Groups.SYNC1, c.getString(COLUMN_SYNC1));
            groupValues.put(Groups.SYNC2, c.getString(COLUMN_SYNC2));
            groupValues.put(Groups.SYNC3, c.getString(COLUMN_SYNC3));
            groupValues.put(Groups.SYNC4, c.getString(COLUMN_SYNC4));
            groupValues.put(Groups.SYSTEM_ID, c.getString(COLUMN_SYSTEM_ID));
            groupValues.put(Groups.DELETED, c.getLong(COLUMN_DELETED));
            groupValues.put(Groups.NOTES, c.getString(COLUMN_NOTES));
            Entity group = new Entity(groupValues);

            mEntityCursor.moveToNext();

            return group;
        }
    }

    @Override
    public EntityIterator queryEntities(Uri uri, String selection, String[] selectionArgs,
            String sortOrder) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case RAW_CONTACTS:
            case RAW_CONTACTS_ID:
                String contactsIdString = null;
                if (match == RAW_CONTACTS_ID) {
                    contactsIdString = uri.getPathSegments().get(1);
                }

                return new ContactsEntityIterator(this, contactsIdString,
                        uri, selection, selectionArgs, sortOrder);
            case GROUPS:
            case GROUPS_ID:
                String idString = null;
                if (match == GROUPS_ID) {
                    idString = uri.getPathSegments().get(1);
                }

                return new GroupsEntityIterator(this, idString,
                        uri, selection, selectionArgs, sortOrder);
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public String getType(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
            case RAW_CONTACTS: return RawContacts.CONTENT_TYPE;
            case RAW_CONTACTS_ID: return RawContacts.CONTENT_ITEM_TYPE;
            case DATA_ID:
                final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                long dataId = ContentUris.parseId(uri);
                return mOpenHelper.getDataMimeType(dataId);
            case AGGREGATION_EXCEPTIONS: return AggregationExceptions.CONTENT_TYPE;
            case AGGREGATION_EXCEPTION_ID: return AggregationExceptions.CONTENT_ITEM_TYPE;
            case AGGREGATION_SUGGESTIONS: return Contacts.CONTENT_TYPE;
            case SEARCH_SUGGESTIONS:
                return SearchManager.SUGGEST_MIME_TYPE;
            case SEARCH_SHORTCUT:
                return SearchManager.SHORTCUT_MIME_TYPE;
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            ContentProviderResult[] results = super.applyBatch(operations);
            db.setTransactionSuccessful();
            return results;
        } finally {
            db.endTransaction();
        }
    }

    private void setDisplayName(long rawContactId, String displayName) {
        if (displayName != null) {
            mContactDisplayNameUpdate.bindString(1, displayName);
        } else {
            mContactDisplayNameUpdate.bindNull(1);
        }
        mContactDisplayNameUpdate.bindLong(2, rawContactId);
        mContactDisplayNameUpdate.execute();
    }

    /**
     * Checks the {@link Data#MARK_AS_DIRTY} query parameter.
     *
     * Returns true if the parameter is missing or is either "true" or "1".
     */
    private boolean shouldMarkRawContactAsDirty(Uri uri) {
        if (mImportMode) {
            return false;
        }

        String param = uri.getQueryParameter(Data.MARK_AS_DIRTY);
        return param == null || (!param.equalsIgnoreCase("false") && !param.equals("0"));
    }

    /**
     * Sets the {@link RawContacts#DIRTY} for the specified raw contact.
     */
    private void setRawContactDirty(long rawContactId) {
        mRawContactDirtyUpdate.bindLong(1, rawContactId);
        mRawContactDirtyUpdate.execute();
    }

    /**
     * Checks the {@link Groups#MARK_AS_DIRTY} query parameter.
     *
     * Returns true if the parameter is missing or is either "true" or "1".
     */
    private boolean shouldMarkGroupAsDirty(Uri uri) {
        if (mImportMode) {
            return false;
        }

        String param = uri.getQueryParameter(Groups.MARK_AS_DIRTY);
        return param == null || (!param.equalsIgnoreCase("false") && !param.equals("0"));
    }

    /*
     * Sets the given dataId record in the "data" table to primary, and resets all data records of
     * the same mimetype and under the same contact to not be primary.
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsPrimary(long dataId) {
        mSetPrimaryStatement.bindLong(1, dataId);
        mSetPrimaryStatement.bindLong(2, dataId);
        mSetPrimaryStatement.bindLong(3, dataId);
        mSetPrimaryStatement.execute();
    }

    /*
     * Sets the given dataId record in the "data" table to "super primary", and resets all data
     * records of the same mimetype and under the same aggregate to not be "super primary".
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsSuperPrimary(long dataId) {
        mSetSuperPrimaryStatement.bindLong(1, dataId);
        mSetSuperPrimaryStatement.bindLong(2, dataId);
        mSetSuperPrimaryStatement.bindLong(3, dataId);
        mSetSuperPrimaryStatement.execute();

        // Find the parent aggregate and package for this new primary
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        long aggId = -1;
        boolean isRestricted = false;
        String mimeType = null;

        Cursor cursor = null;
        try {
            cursor = db.query(DataRawContactsQuery.TABLE, DataRawContactsQuery.PROJECTION,
                    DataColumns.CONCRETE_ID + "=" + dataId, null, null, null, null);
            if (cursor.moveToFirst()) {
                aggId = cursor.getLong(DataRawContactsQuery.CONTACT_ID);
                isRestricted = (cursor.getInt(DataRawContactsQuery.IS_RESTRICTED) == 1);
                mimeType = cursor.getString(DataRawContactsQuery.MIMETYPE);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // Bypass aggregate update if no parent found, or if we don't keep track
        // of super-primary for this mimetype.
        if (aggId == -1) {
            return;
        }

        boolean isPhone = CommonDataKinds.Phone.CONTENT_ITEM_TYPE.equals(mimeType);
        boolean isEmail = CommonDataKinds.Email.CONTENT_ITEM_TYPE.equals(mimeType);

        // Record this value as the new primary for the parent aggregate
        final ContentValues values = new ContentValues();
        if (isPhone) {
            values.put(ContactsColumns.OPTIMAL_PRIMARY_PHONE_ID, dataId);
            values.put(ContactsColumns.OPTIMAL_PRIMARY_PHONE_IS_RESTRICTED, isRestricted);
        } else if (isEmail) {
            values.put(ContactsColumns.OPTIMAL_PRIMARY_EMAIL_ID, dataId);
            values.put(ContactsColumns.OPTIMAL_PRIMARY_EMAIL_IS_RESTRICTED, isRestricted);
        }

        // If this data is unrestricted, then also set as fallback
        if (!isRestricted && isPhone) {
            values.put(ContactsColumns.FALLBACK_PRIMARY_PHONE_ID, dataId);
        } else if (!isRestricted && isEmail) {
            values.put(ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID, dataId);
        }

        // Push update into contacts table, if needed
        if (values.size() > 0) {
            db.update(Tables.CONTACTS, values, Contacts._ID + "=" + aggId, null);
        }
    }

    private String buildContactLookupWhereClause(String filterParam) {
        StringBuilder filter = new StringBuilder();
        filter.append(Tables.CONTACTS);
        filter.append(".");
        filter.append(Contacts._ID);
        filter.append(" IN (SELECT ");
        filter.append(RawContacts.CONTACT_ID);
        filter.append(" FROM ");
        filter.append(Tables.RAW_CONTACTS);
        filter.append(" WHERE ");
        filter.append(RawContacts._ID);
        filter.append(" IN ");
        appendRawContactsByFilterAsNestedQuery(filter, filterParam, null);
        filter.append(")");
        return filter.toString();
    }

    public String getRawContactsByFilterAsNestedQuery(String filterParam) {
        StringBuilder sb = new StringBuilder();
        appendRawContactsByFilterAsNestedQuery(sb, filterParam, null);
        return sb.toString();
    }

    public void appendRawContactsByFilterAsNestedQuery(StringBuilder sb, String filterParam,
            String limit) {
        sb.append("(SELECT DISTINCT raw_contact_id FROM name_lookup WHERE normalized_name GLOB '");
        sb.append(NameNormalizer.normalize(filterParam));
        sb.append("*'");
        if (limit != null) {
            sb.append(" LIMIT ").append(limit);
        }
        sb.append(")");
    }

    /**
     * Inserts an argument at the beginning of the selection arg list.
     */
    private String[] insertSelectionArg(String[] selectionArgs, String arg) {
        if (selectionArgs == null) {
            return new String[] {arg};
        } else {
            int newLength = selectionArgs.length + 1;
            String[] newSelectionArgs = new String[newLength];
            newSelectionArgs[0] = arg;
            System.arraycopy(selectionArgs, 0, newSelectionArgs, 1, selectionArgs.length);
            return newSelectionArgs;
        }
    }
}
