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

import com.android.providers.contacts.OpenHelper.AggregatesColumns;
import com.android.providers.contacts.OpenHelper.AggregationExceptionColumns;
import com.android.providers.contacts.OpenHelper.Clauses;
import com.android.providers.contacts.OpenHelper.ContactsColumns;
import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.GroupsColumns;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.PhoneColumns;
import com.android.providers.contacts.OpenHelper.PhoneLookupColumns;
import com.android.providers.contacts.OpenHelper.Tables;
import com.android.internal.content.SyncStateContentProviderHelper;
import com.google.android.collect.Lists;

import android.accounts.Account;
import android.content.pm.PackageManager;
import android.content.ContentProvider;
import android.content.UriMatcher;
import android.content.Context;
import android.content.ContentValues;
import android.content.ContentUris;
import android.content.EntityIterator;
import android.content.Entity;
import android.content.ContentProviderResult;
import android.content.OperationApplicationException;
import android.content.ContentProviderOperation;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.Binder;
import android.os.RemoteException;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.Contacts.ContactMethods;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RestrictionExceptions;
import android.provider.ContactsContract.Aggregates.AggregationSuggestions;
import android.provider.ContactsContract.CommonDataKinds.BaseTypes;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Postal;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
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

    // TODO: define broadcastreceiver to catch app uninstalls that should clear exceptions
    // TODO: carefully prevent all incoming nested queries; they can be gaping security holes
    // TODO: check for restricted flag during insert(), update(), and delete() calls

    /** Default for the maximum number of returned aggregation suggestions. */
    private static final int DEFAULT_MAX_SUGGESTIONS = 5;

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final String STREQUENT_ORDER_BY = Aggregates.STARRED + " DESC, "
            + Aggregates.TIMES_CONTACTED + " DESC, "
            + Aggregates.DISPLAY_NAME + " ASC";
    private static final String STREQUENT_LIMIT =
            "(SELECT COUNT(1) FROM " + Tables.AGGREGATES + " WHERE "
            + Aggregates.STARRED + "=1) + 25";

    private static final int AGGREGATES = 1000;
    private static final int AGGREGATES_ID = 1001;
    private static final int AGGREGATES_DATA = 1002;
    private static final int AGGREGATES_SUMMARY = 1003;
    private static final int AGGREGATES_SUMMARY_ID = 1004;
    private static final int AGGREGATES_SUMMARY_FILTER = 1005;
    private static final int AGGREGATES_SUMMARY_STREQUENT = 1006;
    private static final int AGGREGATES_SUMMARY_STREQUENT_FILTER = 1007;
    private static final int AGGREGATES_SUMMARY_GROUP = 1008;

    private static final int CONTACTS = 2002;
    private static final int CONTACTS_ID = 2003;
    private static final int CONTACTS_DATA = 2004;
    private static final int CONTACTS_FILTER_EMAIL = 2005;

    private static final int DATA = 3000;
    private static final int DATA_ID = 3001;
    private static final int PHONES = 3002;
    private static final int PHONES_FILTER = 3003;
    private static final int POSTALS = 3004;

    private static final int PHONE_LOOKUP = 4000;

    private static final int AGGREGATION_EXCEPTIONS = 6000;
    private static final int AGGREGATION_EXCEPTION_ID = 6001;

    private static final int PRESENCE = 7000;
    private static final int PRESENCE_ID = 7001;

    private static final int AGGREGATION_SUGGESTIONS = 8000;

    private static final int RESTRICTION_EXCEPTIONS = 9000;

    private static final int GROUPS = 10000;
    private static final int GROUPS_ID = 10001;
    private static final int GROUPS_SUMMARY = 10003;

    private static final int SYNCSTATE = 11000;

    private interface Projections {
        public static final String[] CONTACTS_ACCOUNT_AND_PACKAGE =
                new String[]{Contacts.ACCOUNT_NAME, Contacts.ACCOUNT_TYPE,
                        ContactsColumns.PACKAGE_ID};
        public static final int CONTACTS_ACCOUNT_AND_PACKAGE_COL_ACCOUNT_NAME = 0;
        public static final int CONTACTS_ACCOUNT_AND_PACKAGE_COL_ACCOUNT_TYPE = 1;
        public static final int CONTACTS_ACCOUNT_AND_PACKAGE_COL_PACKAGE_ID = 2;

        public static final String[] PROJ_CONTACTS = new String[] {
            ContactsColumns.CONCRETE_ID,
        };

        public static final String[] PROJ_DATA_CONTACTS = new String[] {
            ContactsColumns.CONCRETE_ID,
            DataColumns.CONCRETE_ID,
            Contacts.AGGREGATE_ID,
            ContactsColumns.PACKAGE_ID,
            Contacts.IS_RESTRICTED,
            Data.MIMETYPE,
        };

        public static final int COL_CONTACT_ID = 0;
        public static final int COL_DATA_ID = 1;
        public static final int COL_AGGREGATE_ID = 2;
        public static final int COL_PACKAGE_ID = 3;
        public static final int COL_IS_RESTRICTED = 4;
        public static final int COL_MIMETYPE = 5;

        public static final String[] PROJ_DATA_AGGREGATES = new String[] {
            ContactsColumns.CONCRETE_ID,
            DataColumns.CONCRETE_ID,
            AggregatesColumns.CONCRETE_ID,
            MimetypesColumns.CONCRETE_ID,
            Phone.NUMBER,
            Email.DATA,
            AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID,
            AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID,
            AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID,
            AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID,
        };

        public static final int COL_MIMETYPE_ID = 3;
        public static final int COL_PHONE_NUMBER = 4;
        public static final int COL_EMAIL_DATA = 5;
        public static final int COL_OPTIMAL_PHONE_ID = 6;
        public static final int COL_FALLBACK_PHONE_ID = 7;
        public static final int COL_OPTIMAL_EMAIL_ID = 8;
        public static final int COL_FALLBACK_EMAIL_ID = 9;

    }

    private interface DisplayNameQuery {
        public static final String TABLES = Tables.DATA_JOIN_MIMETYPES;

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
        public static final String TABLES = Tables.DATA_JOIN_MIMETYPES;

        public static final String[] COLUMNS = new String[] {
            DataColumns.CONCRETE_ID,
            MimetypesColumns.MIMETYPE,
            Data.CONTACT_ID,
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
        };

        public static final int ID = 0;
        public static final int MIMETYPE = 1;
        public static final int CONTACT_ID = 2;
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

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sAggregatesProjectionMap;
    /** Contains the aggregate columns along with primary phone */
    private static final HashMap<String, String> sAggregatesSummaryProjectionMap;
    /** Contains the data, contacts, and aggregate columns, for joined tables. */
    private static final HashMap<String, String> sDataContactsAggregateProjectionMap;
    /** Contains the data, contacts, group sourceid and aggregate columns, for joined tables. */
    private static final HashMap<String, String> sDataContactsGroupsAggregateProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the data columns */
    private static final HashMap<String, String> sDataGroupsProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsGroupsProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsProjectionMap;
    /** Contains the just the {@link Groups} columns */
    private static final HashMap<String, String> sGroupsProjectionMap;
    /** Contains {@link Groups} columns along with summary details */
    private static final HashMap<String, String> sGroupsSummaryProjectionMap;
    /** Contains the just the agg_exceptions columns */
    private static final HashMap<String, String> sAggregationExceptionsProjectionMap;
    /** Contains the just the {@link RestrictionExceptions} columns */
    private static final HashMap<String, String> sRestrictionExceptionsProjectionMap;

    /** Sql select statement that returns the contact id associated with a data record. */
    private static final String sNestedContactIdSelect;
    /** Sql select statement that returns the mimetype id associated with a data record. */
    private static final String sNestedMimetypeSelect;
    /** Sql select statement that returns the aggregate id associated with a contact record. */
    private static final String sNestedAggregateIdSelect;
    /** Sql select statement that returns a list of contact ids associated with an aggregate record. */
    private static final String sNestedContactIdListSelect;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "primary" is selected.*/
    private static final String sSetPrimaryWhere;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "super primary" is selected.*/
    private static final String sSetSuperPrimaryWhere;
    /** Sql where statement for filtering on groups. */
    private static final String sAggregatesInGroupSelect;
    /** Precompiled sql statement for setting a data record to the primary. */
    private SQLiteStatement mSetPrimaryStatement;
    /** Precompiled sql statement for setting a data record to the super primary. */
    private SQLiteStatement mSetSuperPrimaryStatement;
    /** Precompiled sql statement for incrementing times contacted for an aggregate */
    private SQLiteStatement mLastTimeContactedUpdate;
    /** Precompiled sql statement for updating a contact display name */
    private SQLiteStatement mContactDisplayNameUpdate;

    private static final String GTALK_PROTOCOL_STRING = ContactMethods
            .encodePredefinedImProtocol(ContactMethods.PROTOCOL_GOOGLE_TALK);

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates", AGGREGATES);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#", AGGREGATES_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#/data", AGGREGATES_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary", AGGREGATES_SUMMARY);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary/#", AGGREGATES_SUMMARY_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary/filter/*",
                AGGREGATES_SUMMARY_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary/strequent/",
                AGGREGATES_SUMMARY_STREQUENT);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary/strequent/filter/*",
                AGGREGATES_SUMMARY_STREQUENT_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_summary/group/*",
                AGGREGATES_SUMMARY_GROUP);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#/suggestions",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter_email/*",
                CONTACTS_FILTER_EMAIL);

        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones", PHONES);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones/filter/*", PHONES_FILTER);
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

        matcher.addURI(ContactsContract.AUTHORITY, "restriction_exceptions", RESTRICTION_EXCEPTIONS);

        HashMap<String, String> columns;

        // Aggregates projection map
        columns = new HashMap<String, String>();
        columns.put(Aggregates._ID, "aggregates._id AS _id");
        columns.put(Aggregates.DISPLAY_NAME, Aggregates.DISPLAY_NAME);
        columns.put(Aggregates.LAST_TIME_CONTACTED, Aggregates.LAST_TIME_CONTACTED);
        columns.put(Aggregates.TIMES_CONTACTED, Aggregates.TIMES_CONTACTED);
        columns.put(Aggregates.STARRED, Aggregates.STARRED);
        columns.put(Aggregates.IN_VISIBLE_GROUP, Aggregates.IN_VISIBLE_GROUP);
        columns.put(Aggregates.PHOTO_ID, Aggregates.PHOTO_ID);
        columns.put(Aggregates.PRIMARY_PHONE_ID, Aggregates.PRIMARY_PHONE_ID);
        columns.put(Aggregates.PRIMARY_EMAIL_ID, Aggregates.PRIMARY_EMAIL_ID);
        columns.put(Aggregates.CUSTOM_RINGTONE, Aggregates.CUSTOM_RINGTONE);
        columns.put(Aggregates.SEND_TO_VOICEMAIL, Aggregates.SEND_TO_VOICEMAIL);
        columns.put(AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID,
                AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID);
        columns.put(AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID,
                AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID);
        sAggregatesProjectionMap = columns;

        // Aggregates primaries projection map. The overall presence status is
        // the most-present value, as indicated by the largest value.
        columns = new HashMap<String, String>();
        columns.putAll(sAggregatesProjectionMap);
        columns.put(CommonDataKinds.Phone.TYPE, CommonDataKinds.Phone.TYPE);
        columns.put(CommonDataKinds.Phone.LABEL, CommonDataKinds.Phone.LABEL);
        columns.put(CommonDataKinds.Phone.NUMBER, CommonDataKinds.Phone.NUMBER);
        columns.put(Presence.PRESENCE_STATUS, "MAX(" + Presence.PRESENCE_STATUS + ")");
        sAggregatesSummaryProjectionMap = columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.PACKAGE, Contacts.PACKAGE);
        columns.put(Contacts.AGGREGATE_ID, Contacts.AGGREGATE_ID);
        columns.put(Contacts.ACCOUNT_NAME,
                OpenHelper.ContactsColumns.CONCRETE_ACCOUNT_NAME + " as " + Contacts.ACCOUNT_NAME);
        columns.put(Contacts.ACCOUNT_TYPE,
                OpenHelper.ContactsColumns.CONCRETE_ACCOUNT_TYPE + " as " + Contacts.ACCOUNT_TYPE);
        columns.put(Contacts.SOURCE_ID,
                OpenHelper.ContactsColumns.CONCRETE_SOURCE_ID + " as " + Contacts.SOURCE_ID);
        columns.put(Contacts.VERSION,
                OpenHelper.ContactsColumns.CONCRETE_VERSION + " as " + Contacts.VERSION);
        columns.put(Contacts.DIRTY,
                OpenHelper.ContactsColumns.CONCRETE_DIRTY + " as " + Contacts.DIRTY);
        sContactsProjectionMap = columns;

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, "data._id AS _id");
        columns.put(Data.CONTACT_ID, Data.CONTACT_ID);
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
        columns.put(GroupMembership.GROUP_SOURCE_ID, "groups.sourceid as " + GroupMembership.GROUP_SOURCE_ID);
        // Mappings used for backwards compatibility.
        columns.put("number", Phone.NUMBER);
        sDataGroupsProjectionMap = columns;

        // Data, groups and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sContactsProjectionMap);
        columns.putAll(sDataGroupsProjectionMap); // _id will be replaced with the one from data
        columns.put(Data.CONTACT_ID, DataColumns.CONCRETE_CONTACT_ID);
        sDataContactsGroupsProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sDataContactsGroupsProjectionMap);
        columns.remove(GroupMembership.GROUP_SOURCE_ID);
        sDataContactsProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sAggregatesProjectionMap);
        columns.putAll(sContactsProjectionMap); //
        columns.putAll(sDataGroupsProjectionMap); // _id will be replaced with the one from data
        columns.put(Data.CONTACT_ID, DataColumns.CONCRETE_CONTACT_ID);
        sDataContactsGroupsAggregateProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sDataContactsGroupsAggregateProjectionMap);
        columns.remove(GroupMembership.GROUP_SOURCE_ID);
        sDataContactsAggregateProjectionMap = columns;

        // Groups projection map
        columns = new HashMap<String, String>();
        columns.put(Groups._ID, "groups._id AS _id");
        columns.put(Groups.ACCOUNT_NAME, Groups.ACCOUNT_NAME);
        columns.put(Groups.ACCOUNT_TYPE, Groups.ACCOUNT_TYPE);
        columns.put(Groups.SOURCE_ID, Groups.SOURCE_ID);
        columns.put(Groups.DIRTY, Groups.DIRTY);
        columns.put(Groups.VERSION, Groups.VERSION);
        columns.put(Groups.PACKAGE, Groups.PACKAGE);
        columns.put(Groups.PACKAGE_ID, GroupsColumns.CONCRETE_PACKAGE_ID);
        columns.put(Groups.TITLE, Groups.TITLE);
        columns.put(Groups.TITLE_RESOURCE, Groups.TITLE_RESOURCE);
        columns.put(Groups.GROUP_VISIBLE, Groups.GROUP_VISIBLE);
        sGroupsProjectionMap = columns;

        // Contacts and groups projection map
        columns = new HashMap<String, String>();
        columns.putAll(sGroupsProjectionMap);

        columns.put(Groups.SUMMARY_COUNT, "(SELECT COUNT(DISTINCT " + AggregatesColumns.CONCRETE_ID
                + ") FROM " + Tables.DATA_JOIN_MIMETYPES_CONTACTS_AGGREGATES + " WHERE "
                + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP + " AND " + Clauses.BELONGS_TO_GROUP
                + ") AS " + Groups.SUMMARY_COUNT);

        columns.put(Groups.SUMMARY_WITH_PHONES, "(SELECT COUNT(DISTINCT "
                + AggregatesColumns.CONCRETE_ID + ") FROM "
                + Tables.DATA_JOIN_MIMETYPES_CONTACTS_AGGREGATES + " WHERE "
                + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP + " AND " + Clauses.BELONGS_TO_GROUP
                + " AND " + Clauses.HAS_PRIMARY_PHONE + ") AS " + Groups.SUMMARY_WITH_PHONES);

        sGroupsSummaryProjectionMap = columns;

        // Aggregate exception projection map
        columns = new HashMap<String, String>();
        columns.put(AggregationExceptionColumns._ID, Tables.AGGREGATION_EXCEPTIONS + "._id AS _id");
        columns.put(AggregationExceptions.TYPE, AggregationExceptions.TYPE);
        columns.put(AggregationExceptions.AGGREGATE_ID,
                "contacts1." + Contacts.AGGREGATE_ID + " AS " + AggregationExceptions.AGGREGATE_ID);
        columns.put(AggregationExceptions.CONTACT_ID, AggregationExceptionColumns.CONTACT_ID2);
        sAggregationExceptionsProjectionMap = columns;

        // Restriction exception projection map
        columns = new HashMap<String, String>();
        columns.put(RestrictionExceptions.PACKAGE_PROVIDER, RestrictionExceptions.PACKAGE_PROVIDER);
        columns.put(RestrictionExceptions.PACKAGE_CLIENT, RestrictionExceptions.PACKAGE_CLIENT);
        columns.put(RestrictionExceptions.ALLOW_ACCESS, "1"); // Access granted if row returned
        sRestrictionExceptionsProjectionMap = columns;

        sNestedContactIdSelect = "SELECT " + Data.CONTACT_ID + " FROM " + Tables.DATA + " WHERE "
                + Data._ID + "=?";
        sNestedMimetypeSelect = "SELECT " + DataColumns.MIMETYPE_ID + " FROM " + Tables.DATA
                + " WHERE " + Data._ID + "=?";
        sNestedAggregateIdSelect = "SELECT " + Contacts.AGGREGATE_ID + " FROM " + Tables.CONTACTS
                + " WHERE " + Contacts._ID + "=(" + sNestedContactIdSelect + ")";
        sNestedContactIdListSelect = "SELECT " + Contacts._ID + " FROM " + Tables.CONTACTS
                + " WHERE " + Contacts.AGGREGATE_ID + "=(" + sNestedAggregateIdSelect + ")";
        sSetPrimaryWhere = Data.CONTACT_ID + "=(" + sNestedContactIdSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
        sSetSuperPrimaryWhere = Data.CONTACT_ID + " IN (" + sNestedContactIdListSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
        sAggregatesInGroupSelect = AggregatesColumns.CONCRETE_ID + " IN (SELECT "
                + Contacts.AGGREGATE_ID + " FROM " + Tables.CONTACTS + " WHERE ("
                + ContactsColumns.CONCRETE_ID + " IN (SELECT " + Tables.DATA + "."
                + Data.CONTACT_ID + " FROM " + Tables.DATA_JOIN_MIMETYPES + " WHERE ("
                + Data.MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE + "' AND "
                + GroupMembership.GROUP_ROW_ID + "=(SELECT " + Tables.GROUPS + "."
                + Groups._ID + " FROM " + Tables.GROUPS + " WHERE " + Groups.TITLE + "=?)))))";
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
            final long dataId = db.insert(Tables.DATA, null, values);

            Integer primary = values.getAsInteger(Data.IS_PRIMARY);
            if (primary != null && primary != 0) {
                setIsPrimary(dataId);
            }

            fixContactDisplayName(db, contactId);
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
            long contactId = c.getLong(DataQuery.CONTACT_ID);
            boolean primary = c.getInt(DataQuery.IS_PRIMARY) != 0;
            int count = db.delete(Tables.DATA, Data._ID + "=" + dataId, null);
            if (count != 0 && primary) {
                fixPrimary(db, contactId);
                fixContactDisplayName(db, contactId);
            }
            return count;
        }

        private void fixPrimary(SQLiteDatabase db, long contactId) {
            long newPrimaryId = findNewPrimaryDataId(db, contactId);
            if (newPrimaryId != -1) {
                ContactsProvider2.this.setIsPrimary(newPrimaryId);
            }
        }

        protected long findNewPrimaryDataId(SQLiteDatabase db, long contactId) {
            long primaryId = -1;
            int primaryType = -1;
            Cursor c = queryData(db, contactId);
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

        protected Cursor queryData(SQLiteDatabase db, long contactId) {
            // TODO Lookup integer mimetype IDs' instead of joining for speed
            return db.query(DataQuery.TABLES, DataQuery.COLUMNS, Data.CONTACT_ID + "="
                    + contactId + " AND " + MimetypesColumns.MIMETYPE + "='" + mMimetype + "'",
                    null, null, null, null);
        }

        protected void fixContactDisplayName(SQLiteDatabase db, long contactId) {
            if (!sDisplayNamePriorities.containsKey(mMimetype)) {
                return;
            }

            String bestDisplayName = null;
            Cursor c = db.query(DisplayNameQuery.TABLES, DisplayNameQuery.COLUMNS,
                    Data.CONTACT_ID + "=" + contactId, null, null, null, null);
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

            ContactsProvider2.this.setDisplayName(contactId, bestDisplayName);
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
            fixStructuredNameComponents(values);
            return super.insert(db, contactId, values);
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
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
                throw new RuntimeException(mLabelColumn + " value can only be specified with "
                        + mTypeColumn + "=" + BaseTypes.TYPE_CUSTOM + "(custom)");
            }

            if (type == BaseTypes.TYPE_CUSTOM && label == null) {
                throw new RuntimeException(mLabelColumn + " value must be specified when "
                        + mTypeColumn + "=" + BaseTypes.TYPE_CUSTOM + "(custom)");
            }

            return super.insert(db, contactId, values);
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
            long id = super.insert(db, contactId, values);
            fixContactDisplayName(db, contactId);
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
            long id = super.insert(db, contactId, values);
            fixContactDisplayName(db, contactId);
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
        public long insert(SQLiteDatabase db, long contactId, ContentValues values) {
            ContentValues phoneValues = new ContentValues();
            String number = values.getAsString(Phone.NUMBER);
            String normalizedNumber = null;
            if (number != null) {
                normalizedNumber = PhoneNumberUtils.getStrippedReversed(number);
                values.put(PhoneColumns.NORMALIZED_NUMBER, normalizedNumber);
            }

            long id = super.insert(db, contactId, values);

            if (number != null) {
                phoneValues.put(PhoneLookupColumns.CONTACT_ID, contactId);
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
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        mContactAggregator = new ContactAggregator(context, mOpenHelper, mAggregationScheduler);

        mSetPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_PRIMARY
                + "=(_id=?) WHERE " + sSetPrimaryWhere);
        mSetSuperPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_SUPER_PRIMARY
                + "=(_id=?) WHERE " + sSetSuperPrimaryWhere);
        mLastTimeContactedUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.TIMES_CONTACTED + "=" + Contacts.TIMES_CONTACTED + "+1,"
                + Contacts.LAST_TIME_CONTACTED + "=? WHERE " + Contacts.AGGREGATE_ID + "=?");

        mContactDisplayNameUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + ContactsColumns.DISPLAY_NAME + "=? WHERE " + Contacts._ID + "=?");

        mNameSplitter = new NameSplitter(
                context.getString(com.android.internal.R.string.common_name_prefixes),
                context.getString(com.android.internal.R.string.common_last_name_prefixes),
                context.getString(com.android.internal.R.string.common_name_suffixes),
                context.getString(com.android.internal.R.string.common_name_conjunctions));

        mLegacyApiSupport = new LegacyApiSupport(context, mOpenHelper, this);

        mDataRowHandlers = new HashMap<String, DataRowHandler>();

        mDataRowHandlers.put(Email.CONTENT_ITEM_TYPE, new EmailDataRowHandler());
        mDataRowHandlers.put(Im.CONTENT_ITEM_TYPE,
                new CommonDataRowHandler(Im.CONTENT_ITEM_TYPE, Im.TYPE, Im.LABEL));
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE,
                new CommonDataRowHandler(Postal.CONTENT_ITEM_TYPE, Postal.TYPE, Postal.LABEL));
        mDataRowHandlers.put(Organization.CONTENT_ITEM_TYPE, new OrganizationDataRowHandler());
        mDataRowHandlers.put(Phone.CONTENT_ITEM_TYPE, new PhoneDataRowHandler());
        mDataRowHandlers.put(Postal.CONTENT_ITEM_TYPE,
                new CommonDataRowHandler(Nickname.CONTENT_ITEM_TYPE, Nickname.TYPE, Nickname.LABEL));
        mDataRowHandlers.put(StructuredName.CONTENT_ITEM_TYPE,
                new StructuredNameRowHandler(mNameSplitter));

        return (db != null);
    }

    /* Visible for testing */
    protected OpenHelper getOpenHelper(final Context context) {
        return OpenHelper.getInstance(context);
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

            case AGGREGATES: {
                insertAggregate(values);
                break;
            }

            case CONTACTS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertContact(values, account);
                break;
            }

            case CONTACTS_DATA: {
                values.put(Data.CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values);
                break;
            }

            case DATA: {
                id = insertData(values);
                break;
            }

            case GROUPS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertGroup(values, account);
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
        final String accountName = values.getAsString(Contacts.ACCOUNT_NAME);
        final String accountType = values.getAsString(Contacts.ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName) || !TextUtils.isEmpty(accountType)) {
            final Account valuesAccount = new Account(accountName, accountType);
            if (account != null && !valuesAccount.equals(account)) {
                return false;
            }
            account = valuesAccount;
        }
        if (account != null) {
            values.put(Contacts.ACCOUNT_NAME, account.mName);
            values.put(Contacts.ACCOUNT_TYPE, account.mType);
        }
        return true;
    }

    /**
     * Inserts an item in the aggregates table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertAggregate(ContentValues values) {
        throw new UnsupportedOperationException("Aggregates are created automatically");
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @param account the account this contact should be associated with. may be null.
     * @return the row ID of the newly created row
     */
    private long insertContact(ContentValues values, Account account) {
        /*
         * The contact record is inserted in the contacts table, but it needs to
         * be processed by the aggregator before it will be returned by the
         * "aggregates" queries.
         */
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues overriddenValues = new ContentValues(values);
        overriddenValues.putNull(Contacts.AGGREGATE_ID);
        if (!resolveAccount(overriddenValues, account)) {
            return -1;
        }

        // Replace package with internal mapping
        final String packageName = overriddenValues.getAsString(Contacts.PACKAGE);
        overriddenValues.put(ContactsColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
        overriddenValues.remove(Contacts.PACKAGE);

        long contactId = db.insert(Tables.CONTACTS, Contacts.AGGREGATE_ID, overriddenValues);

        int aggregationMode = Contacts.AGGREGATION_MODE_DEFAULT;
        if (values.containsKey(Contacts.AGGREGATION_MODE)) {
            aggregationMode = values.getAsInteger(Contacts.AGGREGATION_MODE);
        }

        triggerAggregation(contactId, aggregationMode);

        return contactId;
    }

    /**
     * Inserts an item in the data table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertData(ContentValues values) {
        int aggregationMode = Contacts.AGGREGATION_MODE_DISABLED;

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            long contactId = values.getAsLong(Data.CONTACT_ID);

            // Replace mimetype with internal mapping
            final String mimeType = values.getAsString(Data.MIMETYPE);
            values.put(DataColumns.MIMETYPE_ID, mOpenHelper.getMimeTypeId(mimeType));
            values.remove(Data.MIMETYPE);

            if (GroupMembership.CONTENT_ITEM_TYPE.equals(mimeType)) {
                boolean containsGroupSourceId = values.containsKey(GroupMembership.GROUP_SOURCE_ID);
                boolean containsGroupId = values.containsKey(GroupMembership.GROUP_ROW_ID);
                if (containsGroupSourceId && containsGroupId) {
                    throw new IllegalArgumentException(
                            "you are not allowed to set both the GroupMembership.GROUP_SOURCE_ID "
                                    + "and GroupMembership.GROUP_ROW_ID");
                }

                if (!containsGroupSourceId && !containsGroupId) {
                    throw new IllegalArgumentException(
                            "you must set exactly one of GroupMembership.GROUP_SOURCE_ID "
                                    + "and GroupMembership.GROUP_ROW_ID");
                }

                if (containsGroupSourceId) {
                    final String sourceId = values.getAsString(GroupMembership.GROUP_SOURCE_ID);
                    final long groupId = getOrMakeGroup(db, contactId, sourceId);
                    values.remove(GroupMembership.GROUP_SOURCE_ID);
                    values.put(GroupMembership.GROUP_ROW_ID, groupId);
                }
            }

            id = getDataRowHandler(mimeType).insert(db, contactId, values);

            aggregationMode = mContactAggregator.markContactForAggregation(contactId);

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        triggerAggregation(id, aggregationMode);
        return id;
    }

    private void triggerAggregation(long contactId, int aggregationMode) {
        switch (aggregationMode) {
            case Contacts.AGGREGATION_MODE_DEFAULT:
                mContactAggregator.schedule();
                break;

            case Contacts.AGGREGATION_MODE_IMMEDITATE:
                mContactAggregator.aggregateContact(contactId);
                break;

            case Contacts.AGGREGATION_MODE_DISABLED:
                // Do nothing
                break;
        }
    }

    public int deleteData(long dataId, String[] allowedMimeTypes) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        Cursor c = db.query(DataQuery.TABLES, DataQuery.COLUMNS,
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
                throw new RuntimeException("Data type mismatch: expected "
                        + Lists.newArrayList(allowedMimeTypes));
            }

            return getDataRowHandler(mimeType).delete(db, c);
        } finally {
            c.close();
        }
    }

    /**
     * Returns the group id of the group with sourceId and the same account as contactId.
     * If the group doesn't already exist then it is first created,
     * @param db SQLiteDatabase to use for this operation
     * @param contactId the contact this group is associated with
     * @param sourceId the sourceIf of the group to query or create
     * @return the group id of the existing or created group
     * @throws IllegalArgumentException if the contact is not associated with an account
     * @throws IllegalStateException if a group needs to be created but the creation failed
     */
    private long getOrMakeGroup(SQLiteDatabase db, long contactId, String sourceId) {
        Account account = null;
        long contactPackage = -1;
        Cursor c = db.query(Tables.CONTACTS,
                Projections.CONTACTS_ACCOUNT_AND_PACKAGE,
                Contacts._ID + "=" + contactId, null, null, null, null);
        try {
            if (c.moveToNext()) {
                final String accountName =
                        c.getString(Projections.CONTACTS_ACCOUNT_AND_PACKAGE_COL_ACCOUNT_NAME);
                final String accountType =
                        c.getString(Projections.CONTACTS_ACCOUNT_AND_PACKAGE_COL_ACCOUNT_TYPE);
                if (!TextUtils.isEmpty(accountName) && !TextUtils.isEmpty(accountType)) {
                    account = new Account(accountName, accountType);
                }
                contactPackage = c.getLong(Projections.CONTACTS_ACCOUNT_AND_PACKAGE_COL_PACKAGE_ID);
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
        // as the contact refered to by contactId
        c = db.query(Tables.GROUPS, new String[]{Contacts._ID},
                Clauses.GROUP_HAS_ACCOUNT_AND_SOURCE_ID,
                new String[]{sourceId, account.mName, account.mType}, null, null, null);
        try {
            if (c.moveToNext()) {
                return c.getLong(0);
            } else {
                ContentValues groupValues = new ContentValues();
                groupValues.put(Groups.PACKAGE_ID, contactPackage);
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
     * Delete the given {@link Data} row, fixing up any {@link Aggregates}
     * primaries that reference it.
     */
    private int deleteData(long dataId) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final long mimePhone = mOpenHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);
        final long mimeEmail = mOpenHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);

        // Check to see if the data about to be deleted was a super-primary on
        // the parent aggregate, and set flags to fix-up once deleted.
        long aggId = -1;
        long mimeId = -1;
        String dataRaw = null;
        boolean fixOptimal = false;
        boolean fixFallback = false;

        Cursor cursor = null;
        try {
            cursor = db.query(Tables.DATA_JOIN_MIMETYPES_CONTACTS_AGGREGATES,
                    Projections.PROJ_DATA_AGGREGATES, DataColumns.CONCRETE_ID + "=" + dataId, null,
                    null, null, null);
            if (cursor.moveToFirst()) {
                aggId = cursor.getLong(Projections.COL_AGGREGATE_ID);
                mimeId = cursor.getLong(Projections.COL_MIMETYPE_ID);
                if (mimeId == mimePhone) {
                    dataRaw = cursor.getString(Projections.COL_PHONE_NUMBER);
                    fixOptimal = (cursor.getLong(Projections.COL_OPTIMAL_PHONE_ID) == dataId);
                    fixFallback = (cursor.getLong(Projections.COL_FALLBACK_PHONE_ID) == dataId);
                } else if (mimeId == mimeEmail) {
                    dataRaw = cursor.getString(Projections.COL_EMAIL_DATA);
                    fixOptimal = (cursor.getLong(Projections.COL_OPTIMAL_EMAIL_ID) == dataId);
                    fixFallback = (cursor.getLong(Projections.COL_FALLBACK_EMAIL_ID) == dataId);
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }

        // Delete the requested data item.
        int dataDeleted = db.delete(Tables.DATA, Data._ID + "=" + dataId, null);

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
                    Contacts.IS_RESTRICTED,
                    ContactsColumns.PACKAGE_ID,
                    scoreClause.toString(),
            };

            final int COL_DATA_ID = 0;
            final int COL_IS_RESTRICTED = 1;
            final int COL_PACKAGE_ID = 2;
            final int COL_SCORE = 3;

            cursor = db.query(Tables.DATA_JOIN_MIMETYPES_CONTACTS_AGGREGATES, PROJ_PRIMARY,
                    AggregatesColumns.CONCRETE_ID + "=" + aggId + " AND " + DataColumns.MIMETYPE_ID
                            + "=" + mimeId, null, null, null, SCORE);

            if (fixOptimal) {
                String colId = null;
                String colPackageId = null;
                if (mimeId == mimePhone) {
                    colId = AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID;
                    colPackageId = AggregatesColumns.OPTIMAL_PRIMARY_PHONE_PACKAGE_ID;
                } else if (mimeId == mimeEmail) {
                    colId = AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID;
                    colPackageId = AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID;
                }

                // Start by replacing with null, since fixOptimal told us that
                // the previous aggregate values are bad.
                values.putNull(colId);
                values.putNull(colPackageId);

                // When finding a new optimal primary, we only care about the
                // highest scoring value, regardless of source.
                if (cursor.moveToFirst()) {
                    final long newOptimal = cursor.getLong(COL_DATA_ID);
                    final long newOptimalPackage = cursor.getLong(COL_PACKAGE_ID);

                    if (newOptimal != 0) {
                        values.put(colId, newOptimal);
                    }
                    if (newOptimalPackage != 0) {
                        values.put(colPackageId, newOptimalPackage);
                    }
                }
            }

            if (fixFallback) {
                String colId = null;
                if (mimeId == mimePhone) {
                    colId = AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID;
                } else if (mimeId == mimeEmail) {
                    colId = AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID;
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

            // Push through any aggregate updates we have
            if (values.size() > 0) {
                db.update(Tables.AGGREGATES, values, AggregatesColumns.CONCRETE_ID + "=" + aggId,
                        null);
            }
        }

        return dataDeleted;
    }

    /**
     * Inserts an item in the groups table
     */
    private long insertGroup(ContentValues values, Account account) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues overriddenValues = new ContentValues(values);
        if (!resolveAccount(overriddenValues, account)) {
            return -1;
        }

        // Replace package with internal mapping
        final String packageName = overriddenValues.getAsString(Groups.PACKAGE);
        overriddenValues.put(Groups.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
        overriddenValues.remove(Groups.PACKAGE);

        return db.insert(Tables.GROUPS, Groups.TITLE, overriddenValues);
    }

    /**
     * Inserts a presence update.
     */
    private long insertPresence(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final String handle = values.getAsString(Presence.IM_HANDLE);
        final String protocol = values.getAsString(Presence.IM_PROTOCOL);
        if (TextUtils.isEmpty(handle) || TextUtils.isEmpty(protocol)) {
            throw new IllegalArgumentException("IM_PROTOCOL and IM_HANDLE are required");
        }

        // TODO: generalize to allow other providers to match against email
        boolean matchEmail = GTALK_PROTOCOL_STRING.equals(protocol);

        String selection;
        String[] selectionArgs;
        if (matchEmail) {
            selection = "(" + Clauses.WHERE_IM_MATCHES + ") OR (" + Clauses.WHERE_EMAIL_MATCHES + ")";
            selectionArgs = new String[] { protocol, handle, handle };
        } else {
            selection = Clauses.WHERE_IM_MATCHES;
            selectionArgs = new String[] { protocol, handle };
        }

        long dataId = -1;
        long aggId = -1;
        Cursor cursor = null;
        try {
            cursor = db.query(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES,
                    Projections.PROJ_DATA_CONTACTS, selection, selectionArgs, null, null, null);
            if (cursor.moveToFirst()) {
                dataId = cursor.getLong(Projections.COL_DATA_ID);
                aggId = cursor.getLong(Projections.COL_AGGREGATE_ID);
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
        values.put(Presence.AGGREGATE_ID, aggId);

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

            case AGGREGATES_ID: {
                long aggregateId = ContentUris.parseId(uri);

                // Remove references to the aggregate first
                ContentValues values = new ContentValues();
                values.putNull(Contacts.AGGREGATE_ID);
                db.update(Tables.CONTACTS, values, Contacts.AGGREGATE_ID + "=" + aggregateId, null);

                return db.delete(Tables.AGGREGATES, BaseColumns._ID + "=" + aggregateId, null);
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                int contactsDeleted = db.delete(Tables.CONTACTS, Contacts._ID + "=" + contactId, null);
                int dataDeleted = db.delete(Tables.DATA, Data.CONTACT_ID + "=" + contactId, null);
                return contactsDeleted + dataDeleted;
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                return deleteData(dataId);
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                final long groupMembershipMimetypeId = mOpenHelper
                        .getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
                int groupsDeleted = db.delete(Tables.GROUPS, Groups._ID + "=" + groupId, null);
                int dataDeleted = db.delete(Tables.DATA, DataColumns.MIMETYPE_ID + "="
                        + groupMembershipMimetypeId + " AND " + GroupMembership.GROUP_ROW_ID + "="
                        + groupId, null);
                mOpenHelper.updateAllVisible();
                return groupsDeleted + dataDeleted;
            }

            case PRESENCE: {
                return db.delete(Tables.PRESENCE, null, null);
            }

            default:
                return mLegacyApiSupport.delete(uri, selection, selectionArgs);
        }
    }

    private static Account readAccountFromQueryParams(Uri uri) {
        final String name = uri.getQueryParameter(Contacts.ACCOUNT_NAME);
        final String type = uri.getQueryParameter(Contacts.ACCOUNT_TYPE);
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

            // TODO(emillar): We will want to disallow editing the aggregates table at some point.
            case AGGREGATES: {
                count = db.update(Tables.AGGREGATES, values, selection, selectionArgs);
                break;
            }

            case AGGREGATES_ID: {
                count = updateAggregateData(db, ContentUris.parseId(uri), values);
                break;
            }

            case DATA_ID: {
                boolean containsIsSuperPrimary = values.containsKey(Data.IS_SUPER_PRIMARY);
                boolean containsIsPrimary = values.containsKey(Data.IS_PRIMARY);
                final long id = ContentUris.parseId(uri);

                // Remove primary or super primary values being set to 0. This is disallowed by the
                // content provider.
                if (containsIsSuperPrimary && values.getAsInteger(Data.IS_SUPER_PRIMARY) == 0) {
                    containsIsSuperPrimary = false;
                    values.remove(Data.IS_SUPER_PRIMARY);
                }
                if (containsIsPrimary && values.getAsInteger(Data.IS_PRIMARY) == 0) {
                    containsIsPrimary = false;
                    values.remove(Data.IS_PRIMARY);
                }

                if (containsIsSuperPrimary) {
                    setIsSuperPrimary(id);
                    setIsPrimary(id);

                    // Now that we've taken care of setting these, remove them from "values".
                    values.remove(Data.IS_SUPER_PRIMARY);
                    if (containsIsPrimary) {
                        values.remove(Data.IS_PRIMARY);
                    }
                } else if (containsIsPrimary) {
                    setIsPrimary(id);

                    // Now that we've taken care of setting this, remove it from "values".
                    values.remove(Data.IS_PRIMARY);
                }

                if (values.size() > 0) {
                    String selectionWithId = (Data._ID + " = " + ContentUris.parseId(uri) + " ")
                            + (selection == null ? "" : " AND " + selection);
                    count = db.update(Tables.DATA, values, selectionWithId, selectionArgs);
                }
                break;
            }

            case CONTACTS: {
                count = db.update(Tables.CONTACTS, values, selection, selectionArgs);
                break;
            }

            case CONTACTS_ID: {
                String selectionWithId = (Contacts._ID + " = " + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.CONTACTS, values, selectionWithId, selectionArgs);
                Log.i(TAG, "Selection is: " + selectionWithId);
                break;
            }

            case DATA: {
                count = db.update(Tables.DATA, values, selection, selectionArgs);
                break;
            }

            case GROUPS: {
                count = db.update(Tables.GROUPS, values, selection, selectionArgs);
                mOpenHelper.updateAllVisible();
                break;
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                String selectionWithId = (Groups._ID + "=" + groupId + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.GROUPS, values, selectionWithId, selectionArgs);

                // If changing visibility, then update aggregates
                if (values.containsKey(Groups.GROUP_VISIBLE)) {
                    mOpenHelper.updateAllVisible();
                }

                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                count = updateAggregationException(db, values);
                break;
            }

            case RESTRICTION_EXCEPTIONS: {
                // Enforce required fields
                boolean hasFields = values.containsKey(RestrictionExceptions.PACKAGE_PROVIDER)
                        && values.containsKey(RestrictionExceptions.PACKAGE_CLIENT)
                        && values.containsKey(RestrictionExceptions.ALLOW_ACCESS);
                if (!hasFields) {
                    throw new IllegalArgumentException("PACKAGE_PROVIDER, PACKAGE_CLIENT, and"
                            + "ALLOW_ACCESS are all required fields");
                }

                final String packageProvider = values
                        .getAsString(RestrictionExceptions.PACKAGE_PROVIDER);
                final boolean allowAccess = (values
                        .getAsInteger(RestrictionExceptions.ALLOW_ACCESS) == 1);

                final Context context = getContext();
                final PackageManager pm = context.getPackageManager();

                // Enforce that caller has authority over the requested package
                // TODO: move back to Binder.getCallingUid() when we can stub-out test suite
                final int callingUid = OpenHelper
                        .getUidForPackageName(pm, context.getPackageName());
                final String[] ownedPackages = pm.getPackagesForUid(callingUid);
                if (!isContained(ownedPackages, packageProvider)) {
                    throw new RuntimeException(
                            "Requested PACKAGE_PROVIDER doesn't belong to calling UID.");
                }

                // Add or remove exception using exception helper
                if (allowAccess) {
                    mOpenHelper.addRestrictionException(context, values);
                } else {
                    mOpenHelper.removeRestrictionException(context, values);
                }

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

    private int updateAggregateData(SQLiteDatabase db, long aggregateId, ContentValues values) {

        // First update all constituent contacts
        ContentValues optionValues = new ContentValues(5);
        OpenHelper.copyStringValue(optionValues, Contacts.CUSTOM_RINGTONE,
                values, Aggregates.CUSTOM_RINGTONE);
        OpenHelper.copyLongValue(optionValues, Contacts.SEND_TO_VOICEMAIL,
                values, Aggregates.SEND_TO_VOICEMAIL);
        OpenHelper.copyLongValue(optionValues, Contacts.LAST_TIME_CONTACTED,
                values, Aggregates.LAST_TIME_CONTACTED);
        OpenHelper.copyLongValue(optionValues, Contacts.TIMES_CONTACTED,
                values, Aggregates.TIMES_CONTACTED);
        OpenHelper.copyLongValue(optionValues, Contacts.STARRED,
                values, Aggregates.STARRED);

        // Nothing to update - just return
        if (optionValues.size() == 0) {
            return 0;
        }

        db.update(Tables.CONTACTS, optionValues, Contacts.AGGREGATE_ID + "=" + aggregateId, null);
        return db.update(Tables.AGGREGATES, values, Aggregates._ID + "=" + aggregateId, null);
    }

    public void updateContactTime(long aggregateId, long lastTimeContacted) {
        mLastTimeContactedUpdate.bindLong(1, lastTimeContacted);
        mLastTimeContactedUpdate.bindLong(2, aggregateId);
        mLastTimeContactedUpdate.execute();
    }

    private static class ContactPair {
        final long contactId1;
        final long contactId2;

        /**
         * Constructor that ensures that this.contactId1 &lt; this.contactId2
         */
        public ContactPair(long contactId1, long contactId2) {
            if (contactId1 < contactId2) {
                this.contactId1 = contactId1;
                this.contactId2 = contactId2;
            } else {
                this.contactId2 = contactId1;
                this.contactId1 = contactId2;
            }
        }
    }

    private int updateAggregationException(SQLiteDatabase db, ContentValues values) {
        int exceptionType = values.getAsInteger(AggregationExceptions.TYPE);
        long aggregateId = values.getAsInteger(AggregationExceptions.AGGREGATE_ID);
        long contactId = values.getAsInteger(AggregationExceptions.CONTACT_ID);

        // First, we build a list of contactID-contactID pairs for the given aggregate and contact.
        ArrayList<ContactPair> pairs = new ArrayList<ContactPair>();
        Cursor c = db.query(Tables.CONTACTS, Projections.PROJ_CONTACTS,
                Contacts.AGGREGATE_ID + "=" + aggregateId,
                null, null, null, null);
        try {
            while (c.moveToNext()) {
                long aggregatedContactId = c.getLong(Projections.COL_CONTACT_ID);
                if (aggregatedContactId != contactId) {
                    pairs.add(new ContactPair(aggregatedContactId, contactId));
                }
            }
        } finally {
            c.close();
        }

        // Now we iterate through all contact pairs to see if we need to insert/delete/update
        // the corresponding exception
        ContentValues exceptionValues = new ContentValues(3);
        exceptionValues.put(AggregationExceptions.TYPE, exceptionType);
        for (ContactPair pair : pairs) {
            final String whereClause =
                    AggregationExceptionColumns.CONTACT_ID1 + "=" + pair.contactId1 + " AND "
                    + AggregationExceptionColumns.CONTACT_ID2 + "=" + pair.contactId2;
            if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC) {
                db.delete(Tables.AGGREGATION_EXCEPTIONS, whereClause, null);
            } else {
                exceptionValues.put(AggregationExceptionColumns.CONTACT_ID1, pair.contactId1);
                exceptionValues.put(AggregationExceptionColumns.CONTACT_ID2, pair.contactId2);
                db.replace(Tables.AGGREGATION_EXCEPTIONS, AggregationExceptions._ID,
                        exceptionValues);
            }
        }

        int aggregationMode = mContactAggregator.markContactForAggregation(contactId);
        if (aggregationMode != Contacts.AGGREGATION_MODE_DISABLED) {
            mContactAggregator.aggregateContact(db, contactId);
            if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC
                    || exceptionType == AggregationExceptions.TYPE_KEEP_OUT) {
                mContactAggregator.updateAggregateData(aggregateId);
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
        if (array == null) {
            array = new String[] {value};
        } else if (!isContained(array, value)) {
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
        String limit = null;
        String aggregateIdColName = Tables.AGGREGATES + "." + Aggregates._ID;

        // TODO: Consider writing a test case for RestrictionExceptions when you
        // write a new query() block to make sure it protects restricted data.
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mOpenHelper.getSyncState().query(db, projection, selection,  selectionArgs,
                        sortOrder);

            case AGGREGATES: {
                qb.setTables(Tables.AGGREGATES);
                applyAggregateRestrictionExceptions(qb);
                applyAggregatePrimaryRestrictionExceptions(sAggregatesProjectionMap);
                qb.setProjectionMap(sAggregatesProjectionMap);
                break;
            }

            case AGGREGATES_ID: {
                long aggId = ContentUris.parseId(uri);
                qb.setTables(Tables.AGGREGATES);
                qb.appendWhere(AggregatesColumns.CONCRETE_ID + "=" + aggId + " AND ");
                applyAggregateRestrictionExceptions(qb);
                applyAggregatePrimaryRestrictionExceptions(sAggregatesProjectionMap);
                qb.setProjectionMap(sAggregatesProjectionMap);
                break;
            }

            case AGGREGATES_SUMMARY: {
                // TODO: join into social status tables
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                applyAggregateRestrictionExceptions(qb);
                applyAggregatePrimaryRestrictionExceptions(sAggregatesSummaryProjectionMap);
                projection = assertContained(projection, Aggregates.PRIMARY_PHONE_ID);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                groupBy = aggregateIdColName;
                break;
            }

            case AGGREGATES_SUMMARY_ID: {
                // TODO: join into social status tables
                long aggId = ContentUris.parseId(uri);
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                qb.appendWhere(AggregatesColumns.CONCRETE_ID + "=" + aggId + " AND ");
                applyAggregateRestrictionExceptions(qb);
                applyAggregatePrimaryRestrictionExceptions(sAggregatesSummaryProjectionMap);
                projection = assertContained(projection, Aggregates.PRIMARY_PHONE_ID);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                groupBy = aggregateIdColName;
                break;
            }

            case AGGREGATES_SUMMARY_FILTER: {
                // TODO: filter query based on callingUid
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(buildAggregateLookupWhereClause(uri.getLastPathSegment()));
                }
                groupBy = aggregateIdColName;
                break;
            }

            case AGGREGATES_SUMMARY_STREQUENT_FILTER:
            case AGGREGATES_SUMMARY_STREQUENT: {
                // Build the first query for starred
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                if (match == AGGREGATES_SUMMARY_STREQUENT_FILTER
                        && uri.getPathSegments().size() > 3) {
                    qb.appendWhere(buildAggregateLookupWhereClause(uri.getLastPathSegment()));
                }
                final String starredQuery = qb.buildQuery(projection, Aggregates.STARRED + "=1",
                        null, aggregateIdColName, null, null,
                        null /* limit */);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                if (match == AGGREGATES_SUMMARY_STREQUENT_FILTER
                        && uri.getPathSegments().size() > 3) {
                    qb.appendWhere(buildAggregateLookupWhereClause(uri.getLastPathSegment()));
                }
                final String frequentQuery = qb.buildQuery(projection,
                        Aggregates.TIMES_CONTACTED + " > 0 AND (" + Aggregates.STARRED
                        + " = 0 OR " + Aggregates.STARRED + " IS NULL)",
                        null, aggregateIdColName, null, null, null);

                // Put them together
                final String query = qb.buildUnionQuery(new String[] {starredQuery, frequentQuery},
                        STREQUENT_ORDER_BY, STREQUENT_LIMIT);
                Cursor c = db.rawQueryWithFactory(null, query, null,
                        Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);

                if ((c != null) && !isTemporary()) {
                    c.setNotificationUri(getContext().getContentResolver(),
                            ContactsContract.AUTHORITY_URI);
                }
                return c;
            }

            case AGGREGATES_SUMMARY_GROUP: {
                qb.setTables(Tables.AGGREGATES_JOIN_PRESENCE_PRIMARY_PHONE);
                applyAggregateRestrictionExceptions(qb);
                applyAggregatePrimaryRestrictionExceptions(sAggregatesSummaryProjectionMap);
                projection = assertContained(projection, Aggregates.PRIMARY_PHONE_ID);
                qb.setProjectionMap(sAggregatesSummaryProjectionMap);
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(" AND " + sAggregatesInGroupSelect);
                    selectionArgs = appendGroupArg(selectionArgs, uri.getLastPathSegment());
                }
                groupBy = aggregateIdColName;
                break;
            }

            case AGGREGATES_DATA: {
                long aggId = Long.parseLong(uri.getPathSegments().get(1));
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES_GROUPS);
                qb.setProjectionMap(sDataContactsGroupsAggregateProjectionMap);
                qb.appendWhere(Contacts.AGGREGATE_ID + "=" + aggId + " AND ");
                applyDataRestrictionExceptions(qb);
                break;
            }

            case PHONES_FILTER: {
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES);
                qb.setProjectionMap(sDataContactsAggregateProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(" AND " + buildAggregateLookupWhereClause(
                            uri.getLastPathSegment()));
                }
                break;
            }

            case PHONES: {
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES);
                qb.setProjectionMap(sDataContactsAggregateProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = \"" + Phone.CONTENT_ITEM_TYPE + "\"");
                break;
            }

            case POSTALS: {
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES);
                qb.setProjectionMap(sDataContactsAggregateProjectionMap);
                qb.appendWhere(Data.MIMETYPE + " = \"" + Postal.CONTENT_ITEM_TYPE + "\"");
                break;
            }

            case CONTACTS: {
                qb.setTables(Tables.CONTACTS_JOIN_PACKAGES);
                qb.setProjectionMap(sContactsProjectionMap);
                applyContactsRestrictionExceptions(qb);
                break;
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                qb.setTables(Tables.CONTACTS_JOIN_PACKAGES);
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere(ContactsColumns.CONCRETE_ID + "=" + contactId + " AND ");
                applyContactsRestrictionExceptions(qb);
                break;
            }

            case CONTACTS_DATA: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_GROUPS);
                qb.setProjectionMap(sDataContactsGroupsProjectionMap);
                qb.appendWhere(Data.CONTACT_ID + "=" + contactId + " AND ");
                applyDataRestrictionExceptions(qb);
                break;
            }

            case CONTACTS_FILTER_EMAIL: {
                // TODO: filter query based on callingUid
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_AGGREGATES);
                qb.setProjectionMap(sDataContactsProjectionMap);
                qb.appendWhere(Data.MIMETYPE + "='" + CommonDataKinds.Email.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + CommonDataKinds.Email.DATA + "=");
                qb.appendWhereEscapeString(uri.getPathSegments().get(2));
                break;
            }

            case DATA: {
                final String accountName = uri.getQueryParameter(Contacts.ACCOUNT_NAME);
                final String accountType = uri.getQueryParameter(Contacts.ACCOUNT_TYPE);
                if (!TextUtils.isEmpty(accountName)) {
                    qb.appendWhere(Contacts.ACCOUNT_NAME + "="
                            + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                            + Contacts.ACCOUNT_TYPE + "="
                            + DatabaseUtils.sqlEscapeString(accountType) + " AND ");
                }
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_GROUPS);
                qb.setProjectionMap(sDataGroupsProjectionMap);
                applyDataRestrictionExceptions(qb);
                break;
            }

            case DATA_ID: {
                qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_GROUPS);
                qb.setProjectionMap(sDataGroupsProjectionMap);
                qb.appendWhere(DataColumns.CONCRETE_ID + "=" + ContentUris.parseId(uri) + " AND ");
                applyDataRestrictionExceptions(qb);
                break;
            }

            case PHONE_LOOKUP: {
                // TODO: filter query based on callingUid
                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = Data.CONTACT_ID;
                }

                final String number = uri.getLastPathSegment();
                OpenHelper.buildPhoneLookupQuery(qb, number);
                qb.setProjectionMap(sDataContactsProjectionMap);
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
                qb.setTables(Tables.GROUPS_JOIN_PACKAGES_DATA_CONTACTS_AGGREGATES);
                qb.setProjectionMap(sGroupsSummaryProjectionMap);
                groupBy = GroupsColumns.CONCRETE_ID;
                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                qb.setTables(Tables.AGGREGATION_EXCEPTIONS_JOIN_CONTACTS);
                qb.setProjectionMap(sAggregationExceptionsProjectionMap);
                break;
            }

            case AGGREGATION_SUGGESTIONS: {
                long aggregateId = Long.parseLong(uri.getPathSegments().get(1));
                final String maxSuggestionsParam =
                        uri.getQueryParameter(AggregationSuggestions.MAX_SUGGESTIONS);

                final int maxSuggestions;
                if (maxSuggestionsParam != null) {
                    maxSuggestions = Integer.parseInt(maxSuggestionsParam);
                } else {
                    maxSuggestions = DEFAULT_MAX_SUGGESTIONS;
                }

                return mContactAggregator.queryAggregationSuggestions(aggregateId, projection,
                        sAggregatesProjectionMap, maxSuggestions);
            }

            case RESTRICTION_EXCEPTIONS: {
                qb.setTables(Tables.RESTRICTION_EXCEPTIONS);
                qb.setProjectionMap(sRestrictionExceptionsProjectionMap);
                break;
            }

            default:
                return mLegacyApiSupport.query(uri, projection, selection, selectionArgs,
                        sortOrder);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs,
                groupBy, null, sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    /**
     * Restrict selection of {@link Aggregates} to only public ones, or those
     * the caller has been granted a {@link RestrictionExceptions} to.
     */
    private void applyAggregateRestrictionExceptions(SQLiteQueryBuilder qb) {
        final int clientUid = OpenHelper.getUidForPackageName(getContext().getPackageManager(),
                getContext().getPackageName());

        qb.appendWhere("(" + AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID + " IS NULL");
        final String exceptionClause = mOpenHelper.getRestrictionExceptionClause(clientUid,
                AggregatesColumns.SINGLE_RESTRICTED_PACKAGE_ID);
        if (exceptionClause != null) {
            qb.appendWhere(" OR (" + exceptionClause + ")");
        }
        qb.appendWhere(")");
    }

    /**
     * Find any exceptions that have been granted to the calling process, and
     * add projections to correctly select {@link Aggregates#PRIMARY_PHONE_ID}
     * and {@link Aggregates#PRIMARY_EMAIL_ID}.
     */
    private void applyAggregatePrimaryRestrictionExceptions(HashMap<String, String> projection) {
        // TODO: move back to Binder.getCallingUid() when we can stub-out test suite
        final int clientUid = OpenHelper.getUidForPackageName(getContext().getPackageManager(),
                getContext().getPackageName());

        final String projectionPhone = "(CASE WHEN "
                + mOpenHelper.getRestrictionExceptionClause(clientUid,
                        AggregatesColumns.OPTIMAL_PRIMARY_PHONE_PACKAGE_ID) + " THEN "
                + AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID + " ELSE "
                + AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID + " END) AS "
                + Aggregates.PRIMARY_PHONE_ID;
        projection.remove(Aggregates.PRIMARY_PHONE_ID);
        projection.put(Aggregates.PRIMARY_PHONE_ID, projectionPhone);

        final String projectionEmail = "(CASE WHEN "
            + mOpenHelper.getRestrictionExceptionClause(clientUid,
                    AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID) + " THEN "
            + AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID + " ELSE "
            + AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID + " END) AS "
            + Aggregates.PRIMARY_EMAIL_ID;
        projection.remove(Aggregates.PRIMARY_EMAIL_ID);
        projection.put(Aggregates.PRIMARY_EMAIL_ID, projectionEmail);
    }

    /**
     * Find any exceptions that have been granted to the
     * {@link Binder#getCallingUid()}, and add a limiting clause to the given
     * {@link SQLiteQueryBuilder} to hide restricted data.
     */
    private void applyContactsRestrictionExceptions(SQLiteQueryBuilder qb) {
        // TODO: move back to Binder.getCallingUid() when we can stub-out test suite
        final int clientUid = OpenHelper.getUidForPackageName(getContext().getPackageManager(),
                getContext().getPackageName());

        qb.appendWhere("(" + Contacts.IS_RESTRICTED + "=0");
        final String exceptionClause = mOpenHelper.getRestrictionExceptionClause(clientUid,
                OpenHelper.ContactsColumns.CONCRETE_PACKAGE_ID);
        if (exceptionClause != null) {
            qb.appendWhere(" OR (" + exceptionClause + ")");
        }
        qb.appendWhere(")");
    }

    /**
     * Find any exceptions that have been granted to the
     * {@link Binder#getCallingUid()}, and add a limiting clause to the given
     * {@link SQLiteQueryBuilder} to hide restricted data.
     */
    void applyDataRestrictionExceptions(SQLiteQueryBuilder qb) {
        applyContactsRestrictionExceptions(qb);
    }

    /**
     * An implementation of EntityIterator that joins the contacts and data tables
     * and consumes all the data rows for a contact in order to build the Entity for a contact.
     */
    private static class ContactsEntityIterator implements EntityIterator {
        private final Cursor mEntityCursor;
        private volatile boolean mIsClosed;

        private static final String[] DATA_KEYS = new String[]{
                "data1",
                "data2",
                "data3",
                "data4",
                "data5",
                "data6",
                "data7",
                "data8",
                "data9",
                "data10"};

        private static final String[] PROJECTION = new String[]{
                Contacts.ACCOUNT_NAME,
                Contacts.ACCOUNT_TYPE,
                Contacts.SOURCE_ID,
                Contacts.VERSION,
                Contacts.DIRTY,
                Contacts.Data._ID,
                Contacts.Data.MIMETYPE,
                Contacts.Data.DATA1,
                Contacts.Data.DATA2,
                Contacts.Data.DATA3,
                Contacts.Data.DATA4,
                Contacts.Data.DATA5,
                Contacts.Data.DATA6,
                Contacts.Data.DATA7,
                Contacts.Data.DATA8,
                Contacts.Data.DATA9,
                Contacts.Data.DATA10,
                Contacts.Data.CONTACT_ID,
                Contacts.Data.IS_PRIMARY,
                Contacts.Data.DATA_VERSION,
                GroupMembership.GROUP_SOURCE_ID};

        private static final int COLUMN_ACCOUNT_NAME = 0;
        private static final int COLUMN_ACCOUNT_TYPE = 1;
        private static final int COLUMN_SOURCE_ID = 2;
        private static final int COLUMN_VERSION = 3;
        private static final int COLUMN_DIRTY = 4;
        private static final int COLUMN_DATA_ID = 5;
        private static final int COLUMN_MIMETYPE = 6;
        private static final int COLUMN_DATA1 = 7;
        private static final int COLUMN_CONTACT_ID = 17;
        private static final int COLUMN_IS_PRIMARY = 18;
        private static final int COLUMN_DATA_VERSION = 19;
        private static final int COLUMN_GROUP_SOURCE_ID = 20;

        public ContactsEntityIterator(ContactsProvider2 provider, String contactsIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;

            final String updatedSortOrder = (sortOrder == null)
                    ? Contacts.Data.CONTACT_ID
                    : (Contacts.Data.CONTACT_ID + "," + sortOrder);

            final SQLiteDatabase db = provider.mOpenHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES_GROUPS);
            qb.setProjectionMap(sDataContactsGroupsProjectionMap);
            if (contactsIdString != null) {
                qb.appendWhere(Data.CONTACT_ID + "=" + contactsIdString);
            }
            final String accountName = uri.getQueryParameter(Contacts.ACCOUNT_NAME);
            final String accountType = uri.getQueryParameter(Contacts.ACCOUNT_TYPE);
            if (!TextUtils.isEmpty(accountName)) {
                qb.appendWhere(Contacts.ACCOUNT_NAME + "="
                        + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                        + Contacts.ACCOUNT_TYPE + "="
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

            final long contactId = c.getLong(COLUMN_CONTACT_ID);

            // we expect the cursor is already at the row we need to read from
            ContentValues contactValues = new ContentValues();
            contactValues.put(Contacts.ACCOUNT_NAME, c.getString(COLUMN_ACCOUNT_NAME));
            contactValues.put(Contacts.ACCOUNT_TYPE, c.getString(COLUMN_ACCOUNT_TYPE));
            contactValues.put(Contacts._ID, contactId);
            contactValues.put(Contacts.DIRTY, c.getLong(COLUMN_DIRTY));
            contactValues.put(Contacts.VERSION, c.getLong(COLUMN_VERSION));
            contactValues.put(Contacts.SOURCE_ID, c.getString(COLUMN_SOURCE_ID));
            Entity contact = new Entity(contactValues);

            // read data rows until the contact id changes
            do {
                if (contactId != c.getLong(COLUMN_CONTACT_ID)) {
                    break;
                }
                // add the data to to the contact
                ContentValues dataValues = new ContentValues();
                dataValues.put(Contacts.Data._ID, c.getString(COLUMN_DATA_ID));
                dataValues.put(Contacts.Data.MIMETYPE, c.getString(COLUMN_MIMETYPE));
                dataValues.put(Contacts.Data.IS_PRIMARY, c.getString(COLUMN_IS_PRIMARY));
                dataValues.put(Contacts.Data.DATA_VERSION, c.getLong(COLUMN_DATA_VERSION));
                if (!c.isNull(COLUMN_GROUP_SOURCE_ID)) {
                    dataValues.put(GroupMembership.GROUP_SOURCE_ID,
                            c.getString(COLUMN_GROUP_SOURCE_ID));
                }
                dataValues.put(Contacts.Data.DATA_VERSION, c.getLong(COLUMN_DATA_VERSION));
                for (int i = 0; i < 10; i++) {
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

    @Override
    public EntityIterator queryEntities(Uri uri, String selection, String[] selectionArgs,
            String sortOrder) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS:
            case CONTACTS_ID:
                String contactsIdString = null;
                if (match == CONTACTS_ID) {
                    contactsIdString = uri.getPathSegments().get(1);
                }

                return new ContactsEntityIterator(this, contactsIdString,
                        uri, selection, selectionArgs, sortOrder);
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public String getType(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case AGGREGATES: return Aggregates.CONTENT_TYPE;
            case AGGREGATES_ID: return Aggregates.CONTENT_ITEM_TYPE;
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
            case DATA_ID:
                final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                long dataId = ContentUris.parseId(uri);
                return mOpenHelper.getDataMimeType(dataId);
            case AGGREGATION_EXCEPTIONS: return AggregationExceptions.CONTENT_TYPE;
            case AGGREGATION_EXCEPTION_ID: return AggregationExceptions.CONTENT_ITEM_TYPE;
            case AGGREGATION_SUGGESTIONS: return Aggregates.CONTENT_TYPE;
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

    private void setDisplayName(long contactId, String displayName) {
        if (displayName != null) {
            mContactDisplayNameUpdate.bindString(1, displayName);
        } else {
            mContactDisplayNameUpdate.bindNull(1);
        }
        mContactDisplayNameUpdate.bindLong(2, contactId);
        mContactDisplayNameUpdate.execute();
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
        long packageId = -1;
        boolean isRestricted = false;
        String mimeType = null;

        Cursor cursor = null;
        try {
            cursor = db.query(Tables.DATA_JOIN_MIMETYPES_CONTACTS_PACKAGES,
                    Projections.PROJ_DATA_CONTACTS, DataColumns.CONCRETE_ID + "=" + dataId, null,
                    null, null, null);
            if (cursor.moveToFirst()) {
                aggId = cursor.getLong(Projections.COL_AGGREGATE_ID);
                packageId = cursor.getLong(Projections.COL_PACKAGE_ID);
                isRestricted = (cursor.getInt(Projections.COL_IS_RESTRICTED) == 1);
                mimeType = cursor.getString(Projections.COL_MIMETYPE);
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
            values.put(AggregatesColumns.OPTIMAL_PRIMARY_PHONE_ID, dataId);
            values.put(AggregatesColumns.OPTIMAL_PRIMARY_PHONE_PACKAGE_ID, packageId);
        } else if (isEmail) {
            values.put(AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_ID, dataId);
            values.put(AggregatesColumns.OPTIMAL_PRIMARY_EMAIL_PACKAGE_ID, packageId);
        }

        // If this data is unrestricted, then also set as fallback
        if (!isRestricted && isPhone) {
            values.put(AggregatesColumns.FALLBACK_PRIMARY_PHONE_ID, dataId);
        } else if (!isRestricted && isEmail) {
            values.put(AggregatesColumns.FALLBACK_PRIMARY_EMAIL_ID, dataId);
        }

        // Push update into aggregates table, if needed
        if (values.size() > 0) {
            db.update(Tables.AGGREGATES, values, Aggregates._ID + "=" + aggId, null);
        }

    }

    private String buildAggregateLookupWhereClause(String filterParam) {
        StringBuilder filter = new StringBuilder();
        filter.append(Tables.AGGREGATES);
        filter.append(".");
        filter.append(Aggregates._ID);
        filter.append(" IN (SELECT ");
        filter.append(Contacts.AGGREGATE_ID);
        filter.append(" FROM ");
        filter.append(Tables.CONTACTS);
        filter.append(" WHERE ");
        filter.append(Contacts._ID);
        filter.append(" IN (SELECT  contact_id FROM name_lookup WHERE normalized_name GLOB '");
        // NOTE: Query parameters won't work here since the SQL compiler
        // needs to parse the actual string to know that it can use the
        // index to do a prefix scan.
        filter.append(NameNormalizer.normalize(filterParam) + "*");
        filter.append("'))");
        return filter.toString();
    }

    private String[] appendGroupArg(String[] selectionArgs, String arg) {
        if (selectionArgs == null) {
            return new String[] {arg};
        } else {
            int newLength = selectionArgs.length + 1;
            String[] newSelectionArgs = new String[newLength];
            System.arraycopy(selectionArgs, 0, newSelectionArgs, 0, selectionArgs.length);
            newSelectionArgs[newLength - 1] = arg;
            return newSelectionArgs;
        }
    }
}
