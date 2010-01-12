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
import com.android.providers.contacts.ContactLookupKey.LookupKeySegment;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregationExceptionColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Clauses;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsStatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DisplayNameSources;
import com.android.providers.contacts.ContactsDatabaseHelper.GroupsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MimetypesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.ContactsDatabaseHelper.NicknameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.SettingsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.StatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.google.android.collect.Lists;
import com.google.android.collect.Maps;
import com.google.android.collect.Sets;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.OnAccountsUpdateListener;
import android.app.SearchManager;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.IContentService;
import android.content.OperationApplicationException;
import android.content.SharedPreferences;
import android.content.SyncAdapterType;
import android.content.UriMatcher;
import android.content.SharedPreferences.Editor;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteContentHelper;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.Bundle;
import android.os.MemoryFile;
import android.os.RemoteException;
import android.os.SystemProperties;
import android.pim.vcard.VCardComposer;
import android.preference.PreferenceManager;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.LiveFolders;
import android.provider.OpenableColumns;
import android.provider.SyncStateContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.PhoneLookup;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.BaseTypes;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.text.util.Rfc822Tokenizer;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends SQLiteContentProvider implements OnAccountsUpdateListener {

    private static final String TAG = "ContactsProvider";

    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

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

    private static final String AGGREGATE_CONTACTS = "sync.contacts.aggregate";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final String TIMES_CONTACED_SORT_COLUMN = "times_contacted_sort";

    private static final String STREQUENT_ORDER_BY = Contacts.STARRED + " DESC, "
            + TIMES_CONTACED_SORT_COLUMN + " DESC, "
            + Contacts.DISPLAY_NAME + " COLLATE LOCALIZED ASC";
    private static final String STREQUENT_LIMIT =
            "(SELECT COUNT(1) FROM " + Tables.CONTACTS + " WHERE "
            + Contacts.STARRED + "=1) + 25";

    private static final int CONTACTS = 1000;
    private static final int CONTACTS_ID = 1001;
    private static final int CONTACTS_LOOKUP = 1002;
    private static final int CONTACTS_LOOKUP_ID = 1003;
    private static final int CONTACTS_DATA = 1004;
    private static final int CONTACTS_FILTER = 1005;
    private static final int CONTACTS_STREQUENT = 1006;
    private static final int CONTACTS_STREQUENT_FILTER = 1007;
    private static final int CONTACTS_GROUP = 1008;
    private static final int CONTACTS_PHOTO = 1009;
    private static final int CONTACTS_AS_VCARD = 1010;

    private static final int RAW_CONTACTS = 2002;
    private static final int RAW_CONTACTS_ID = 2003;
    private static final int RAW_CONTACTS_DATA = 2004;
    private static final int RAW_CONTACT_ENTITY_ID = 2005;

    private static final int DATA = 3000;
    private static final int DATA_ID = 3001;
    private static final int PHONES = 3002;
    private static final int PHONES_ID = 3003;
    private static final int PHONES_FILTER = 3004;
    private static final int EMAILS = 3005;
    private static final int EMAILS_ID = 3006;
    private static final int EMAILS_LOOKUP = 3007;
    private static final int EMAILS_FILTER = 3008;
    private static final int POSTALS = 3009;
    private static final int POSTALS_ID = 3010;

    private static final int PHONE_LOOKUP = 4000;

    private static final int AGGREGATION_EXCEPTIONS = 6000;
    private static final int AGGREGATION_EXCEPTION_ID = 6001;

    private static final int STATUS_UPDATES = 7000;
    private static final int STATUS_UPDATES_ID = 7001;

    private static final int AGGREGATION_SUGGESTIONS = 8000;

    private static final int SETTINGS = 9000;

    private static final int GROUPS = 10000;
    private static final int GROUPS_ID = 10001;
    private static final int GROUPS_SUMMARY = 10003;

    private static final int SYNCSTATE = 11000;
    private static final int SYNCSTATE_ID = 11001;

    private static final int SEARCH_SUGGESTIONS = 12001;
    private static final int SEARCH_SHORTCUT = 12002;

    private static final int LIVE_FOLDERS_CONTACTS = 14000;
    private static final int LIVE_FOLDERS_CONTACTS_WITH_PHONES = 14001;
    private static final int LIVE_FOLDERS_CONTACTS_FAVORITES = 14002;
    private static final int LIVE_FOLDERS_CONTACTS_GROUP_NAME = 14003;

    private static final int RAW_CONTACT_ENTITIES = 15001;

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

    private interface DataContactsQuery {
        public static final String TABLE = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String[] PROJECTION = new String[] {
            RawContactsColumns.CONCRETE_ID,
            DataColumns.CONCRETE_ID,
            ContactsColumns.CONCRETE_ID
        };

        public static final int RAW_CONTACT_ID = 0;
        public static final int DATA_ID = 1;
        public static final int CONTACT_ID = 2;
    }

    private interface DisplayNameQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPES;

        public static final String[] COLUMNS = new String[] {
            MimetypesColumns.MIMETYPE,
            Data.IS_PRIMARY,
            Data.DATA1,
            Organization.TITLE,
        };

        public static final int MIMETYPE = 0;
        public static final int IS_PRIMARY = 1;
        public static final int DATA = 2;
        public static final int TITLE = 3;
    }

    private interface DataDeleteQuery {
        public static final String TABLE = Tables.DATA_JOIN_MIMETYPES;

        public static final String[] CONCRETE_COLUMNS = new String[] {
            DataColumns.CONCRETE_ID,
            MimetypesColumns.MIMETYPE,
            Data.RAW_CONTACT_ID,
            Data.IS_PRIMARY,
            Data.DATA1,
        };

        public static final String[] COLUMNS = new String[] {
            Data._ID,
            MimetypesColumns.MIMETYPE,
            Data.RAW_CONTACT_ID,
            Data.IS_PRIMARY,
            Data.DATA1,
        };

        public static final int _ID = 0;
        public static final int MIMETYPE = 1;
        public static final int RAW_CONTACT_ID = 2;
        public static final int IS_PRIMARY = 3;
        public static final int DATA1 = 4;
    }

    private interface DataUpdateQuery {
        String[] COLUMNS = { Data._ID, Data.RAW_CONTACT_ID, Data.MIMETYPE };

        int _ID = 0;
        int RAW_CONTACT_ID = 1;
        int MIMETYPE = 2;
    }


    private interface NicknameLookupQuery {
        String TABLE = Tables.NICKNAME_LOOKUP;

        String[] COLUMNS = new String[] {
            NicknameLookupColumns.CLUSTER
        };

        int CLUSTER = 0;
    }

    private interface RawContactsQuery {
        String TABLE = Tables.RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                ContactsContract.RawContacts.DELETED
        };

        int DELETED = 0;
    }

    private static final HashMap<String, Integer> sDisplayNameSources;
    static {
        sDisplayNameSources = new HashMap<String, Integer>();
        sDisplayNameSources.put(StructuredName.CONTENT_ITEM_TYPE,
                DisplayNameSources.STRUCTURED_NAME);
        sDisplayNameSources.put(Nickname.CONTENT_ITEM_TYPE,
                DisplayNameSources.NICKNAME);
        sDisplayNameSources.put(Organization.CONTENT_ITEM_TYPE,
                DisplayNameSources.ORGANIZATION);
        sDisplayNameSources.put(Phone.CONTENT_ITEM_TYPE,
                DisplayNameSources.PHONE);
        sDisplayNameSources.put(Email.CONTENT_ITEM_TYPE,
                DisplayNameSources.EMAIL);
    }

    public static final String DEFAULT_ACCOUNT_TYPE = "com.google";
    public static final String FEATURE_LEGACY_HOSTED_OR_GOOGLE = "legacy_hosted_or_google";

    /** Sql where statement for filtering on groups. */
    private static final String CONTACTS_IN_GROUP_SELECT =
            Contacts._ID + " IN "
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

    /** Contains just BaseColumns._COUNT */
    private static final HashMap<String, String> sCountProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Used for pushing starred contacts to the top of a times contacted list **/
    private static final HashMap<String, String> sStrequentStarredProjectionMap;
    private static final HashMap<String, String> sStrequentFrequentProjectionMap;
    /** Contains just the contacts vCard columns */
    private static final HashMap<String, String> sContactsVCardProjectionMap;
    /** Contains just the raw contacts columns */
    private static final HashMap<String, String> sRawContactsProjectionMap;
    /** Contains the columns from the raw contacts entity view*/
    private static final HashMap<String, String> sRawContactsEntityProjectionMap;
    /** Contains columns from the data view */
    private static final HashMap<String, String> sDataProjectionMap;
    /** Contains columns from the data view */
    private static final HashMap<String, String> sDistinctDataProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sPhoneLookupProjectionMap;
    /** Contains the just the {@link Groups} columns */
    private static final HashMap<String, String> sGroupsProjectionMap;
    /** Contains {@link Groups} columns along with summary details */
    private static final HashMap<String, String> sGroupsSummaryProjectionMap;
    /** Contains the agg_exceptions columns */
    private static final HashMap<String, String> sAggregationExceptionsProjectionMap;
    /** Contains the agg_exceptions columns */
    private static final HashMap<String, String> sSettingsProjectionMap;
    /** Contains StatusUpdates columns */
    private static final HashMap<String, String> sStatusUpdatesProjectionMap;
    /** Contains Live Folders columns */
    private static final HashMap<String, String> sLiveFoldersProjectionMap;

    /** Precompiled sql statement for setting a data record to the primary. */
    private SQLiteStatement mSetPrimaryStatement;
    /** Precompiled sql statement for setting a data record to the super primary. */
    private SQLiteStatement mSetSuperPrimaryStatement;
    /** Precompiled sql statement for incrementing times contacted for a contact */
    private SQLiteStatement mContactsLastTimeContactedUpdate;
    /** Precompiled sql statement for updating a contact display name */
    private SQLiteStatement mRawContactDisplayNameUpdate;
    /** Precompiled sql statement for marking a raw contact as dirty */
    private SQLiteStatement mRawContactDirtyUpdate;
    /** Precompiled sql statement for updating an aggregated status update */
    private SQLiteStatement mLastStatusUpdate;
    private SQLiteStatement mNameLookupInsert;
    private SQLiteStatement mNameLookupDelete;
    private SQLiteStatement mStatusUpdateAutoTimestamp;
    private SQLiteStatement mStatusUpdateInsert;
    private SQLiteStatement mStatusUpdateReplace;
    private SQLiteStatement mStatusAttributionUpdate;
    private SQLiteStatement mStatusUpdateDelete;

    private long mMimeTypeIdEmail;
    private long mMimeTypeIdIm;
    private StringBuilder mSb = new StringBuilder();

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/suggestions",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/suggestions/*",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/photo", CONTACTS_PHOTO);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter/*", CONTACTS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*", CONTACTS_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/#", CONTACTS_LOOKUP_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/as_vcard/*", CONTACTS_AS_VCARD);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/strequent/", CONTACTS_STREQUENT);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/strequent/filter/*",
                CONTACTS_STREQUENT_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/group/*", CONTACTS_GROUP);

        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts", RAW_CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#", RAW_CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#/data", RAW_CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "raw_contacts/#/entity", RAW_CONTACT_ENTITY_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "raw_contact_entities", RAW_CONTACT_ENTITIES);

        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones", PHONES);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones/#", PHONES_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones/filter", PHONES_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/phones/filter/*", PHONES_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails", EMAILS);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/#", EMAILS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/lookup/*", EMAILS_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/filter", EMAILS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/filter/*", EMAILS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/postals", POSTALS);
        matcher.addURI(ContactsContract.AUTHORITY, "data/postals/#", POSTALS_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "groups", GROUPS);
        matcher.addURI(ContactsContract.AUTHORITY, "groups/#", GROUPS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "groups_summary", GROUPS_SUMMARY);

        matcher.addURI(ContactsContract.AUTHORITY, SyncStateContentProviderHelper.PATH, SYNCSTATE);
        matcher.addURI(ContactsContract.AUTHORITY, SyncStateContentProviderHelper.PATH + "/#",
                SYNCSTATE_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "phone_lookup/*", PHONE_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregation_exceptions",
                AGGREGATION_EXCEPTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregation_exceptions/*",
                AGGREGATION_EXCEPTION_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "settings", SETTINGS);

        matcher.addURI(ContactsContract.AUTHORITY, "status_updates", STATUS_UPDATES);
        matcher.addURI(ContactsContract.AUTHORITY, "status_updates/#", STATUS_UPDATES_ID);

        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY,
                SEARCH_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY + "/*",
                SEARCH_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/#",
                SEARCH_SHORTCUT);

        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts",
                LIVE_FOLDERS_CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts/*",
                LIVE_FOLDERS_CONTACTS_GROUP_NAME);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts_with_phones",
                LIVE_FOLDERS_CONTACTS_WITH_PHONES);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/favorites",
                LIVE_FOLDERS_CONTACTS_FAVORITES);
    }

    static {
        sCountProjectionMap = new HashMap<String, String>();
        sCountProjectionMap.put(BaseColumns._COUNT, "COUNT(*)");

        sContactsProjectionMap = new HashMap<String, String>();
        sContactsProjectionMap.put(Contacts._ID, Contacts._ID);
        sContactsProjectionMap.put(Contacts.DISPLAY_NAME, Contacts.DISPLAY_NAME);
        sContactsProjectionMap.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sContactsProjectionMap.put(Contacts.TIMES_CONTACTED, Contacts.TIMES_CONTACTED);
        sContactsProjectionMap.put(Contacts.STARRED, Contacts.STARRED);
        sContactsProjectionMap.put(Contacts.IN_VISIBLE_GROUP, Contacts.IN_VISIBLE_GROUP);
        sContactsProjectionMap.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        sContactsProjectionMap.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        sContactsProjectionMap.put(Contacts.HAS_PHONE_NUMBER, Contacts.HAS_PHONE_NUMBER);
        sContactsProjectionMap.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
        sContactsProjectionMap.put(Contacts.LOOKUP_KEY, Contacts.LOOKUP_KEY);

        // Handle projections for Contacts-level statuses
        addProjection(sContactsProjectionMap, Contacts.CONTACT_PRESENCE,
                Tables.AGGREGATED_PRESENCE + "." + StatusUpdates.PRESENCE);
        addProjection(sContactsProjectionMap, Contacts.CONTACT_STATUS,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS);
        addProjection(sContactsProjectionMap, Contacts.CONTACT_STATUS_TIMESTAMP,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP);
        addProjection(sContactsProjectionMap, Contacts.CONTACT_STATUS_RES_PACKAGE,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE);
        addProjection(sContactsProjectionMap, Contacts.CONTACT_STATUS_LABEL,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_LABEL);
        addProjection(sContactsProjectionMap, Contacts.CONTACT_STATUS_ICON,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_ICON);

        sStrequentStarredProjectionMap = new HashMap<String, String>(sContactsProjectionMap);
        sStrequentStarredProjectionMap.put(TIMES_CONTACED_SORT_COLUMN,
                  Long.MAX_VALUE + " AS " + TIMES_CONTACED_SORT_COLUMN);

        sStrequentFrequentProjectionMap = new HashMap<String, String>(sContactsProjectionMap);
        sStrequentFrequentProjectionMap.put(TIMES_CONTACED_SORT_COLUMN,
                  Contacts.TIMES_CONTACTED + " AS " + TIMES_CONTACED_SORT_COLUMN);

        sContactsVCardProjectionMap = Maps.newHashMap();
        sContactsVCardProjectionMap.put(OpenableColumns.DISPLAY_NAME, Contacts.DISPLAY_NAME
                + " || '.vcf' AS " + OpenableColumns.DISPLAY_NAME);
        sContactsVCardProjectionMap.put(OpenableColumns.SIZE, "0 AS " + OpenableColumns.SIZE);

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
        sDataProjectionMap.put(Data.CONTACT_ID, Data.CONTACT_ID);
        sDataProjectionMap.put(RawContacts.ACCOUNT_NAME, RawContacts.ACCOUNT_NAME);
        sDataProjectionMap.put(RawContacts.ACCOUNT_TYPE, RawContacts.ACCOUNT_TYPE);
        sDataProjectionMap.put(RawContacts.SOURCE_ID, RawContacts.SOURCE_ID);
        sDataProjectionMap.put(RawContacts.VERSION, RawContacts.VERSION);
        sDataProjectionMap.put(RawContacts.DIRTY, RawContacts.DIRTY);
        sDataProjectionMap.put(Contacts.LOOKUP_KEY, Contacts.LOOKUP_KEY);
        sDataProjectionMap.put(Contacts.DISPLAY_NAME, Contacts.DISPLAY_NAME);
        sDataProjectionMap.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        sDataProjectionMap.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
        sDataProjectionMap.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sDataProjectionMap.put(Contacts.TIMES_CONTACTED, Contacts.TIMES_CONTACTED);
        sDataProjectionMap.put(Contacts.STARRED, Contacts.STARRED);
        sDataProjectionMap.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        sDataProjectionMap.put(Contacts.IN_VISIBLE_GROUP, Contacts.IN_VISIBLE_GROUP);
        sDataProjectionMap.put(GroupMembership.GROUP_SOURCE_ID, GroupMembership.GROUP_SOURCE_ID);

        HashMap<String, String> columns;
        columns = new HashMap<String, String>();
        columns.put(RawContacts._ID, RawContacts._ID);
        columns.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        columns.put(RawContacts.ACCOUNT_NAME, RawContacts.ACCOUNT_NAME);
        columns.put(RawContacts.ACCOUNT_TYPE, RawContacts.ACCOUNT_TYPE);
        columns.put(RawContacts.SOURCE_ID, RawContacts.SOURCE_ID);
        columns.put(RawContacts.VERSION, RawContacts.VERSION);
        columns.put(RawContacts.DIRTY, RawContacts.DIRTY);
        columns.put(RawContacts.DELETED, RawContacts.DELETED);
        columns.put(RawContacts.IS_RESTRICTED, RawContacts.IS_RESTRICTED);
        columns.put(RawContacts.SYNC1, RawContacts.SYNC1);
        columns.put(RawContacts.SYNC2, RawContacts.SYNC2);
        columns.put(RawContacts.SYNC3, RawContacts.SYNC3);
        columns.put(RawContacts.SYNC4, RawContacts.SYNC4);
        columns.put(Data.RES_PACKAGE, Data.RES_PACKAGE);
        columns.put(Data.MIMETYPE, Data.MIMETYPE);
        columns.put(Data.DATA1, Data.DATA1);
        columns.put(Data.DATA2, Data.DATA2);
        columns.put(Data.DATA3, Data.DATA3);
        columns.put(Data.DATA4, Data.DATA4);
        columns.put(Data.DATA5, Data.DATA5);
        columns.put(Data.DATA6, Data.DATA6);
        columns.put(Data.DATA7, Data.DATA7);
        columns.put(Data.DATA8, Data.DATA8);
        columns.put(Data.DATA9, Data.DATA9);
        columns.put(Data.DATA10, Data.DATA10);
        columns.put(Data.DATA11, Data.DATA11);
        columns.put(Data.DATA12, Data.DATA12);
        columns.put(Data.DATA13, Data.DATA13);
        columns.put(Data.DATA14, Data.DATA14);
        columns.put(Data.DATA15, Data.DATA15);
        columns.put(Data.SYNC1, Data.SYNC1);
        columns.put(Data.SYNC2, Data.SYNC2);
        columns.put(Data.SYNC3, Data.SYNC3);
        columns.put(Data.SYNC4, Data.SYNC4);
        columns.put(RawContacts.Entity.DATA_ID, RawContacts.Entity.DATA_ID);
        columns.put(Data.STARRED, Data.STARRED);
        columns.put(Data.DATA_VERSION, Data.DATA_VERSION);
        columns.put(Data.IS_PRIMARY, Data.IS_PRIMARY);
        columns.put(Data.IS_SUPER_PRIMARY, Data.IS_SUPER_PRIMARY);
        columns.put(GroupMembership.GROUP_SOURCE_ID, GroupMembership.GROUP_SOURCE_ID);
        sRawContactsEntityProjectionMap = columns;

        // Handle projections for Contacts-level statuses
        addProjection(sDataProjectionMap, Contacts.CONTACT_PRESENCE,
                Tables.AGGREGATED_PRESENCE + "." + StatusUpdates.PRESENCE);
        addProjection(sDataProjectionMap, Contacts.CONTACT_STATUS,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS);
        addProjection(sDataProjectionMap, Contacts.CONTACT_STATUS_TIMESTAMP,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP);
        addProjection(sDataProjectionMap, Contacts.CONTACT_STATUS_RES_PACKAGE,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE);
        addProjection(sDataProjectionMap, Contacts.CONTACT_STATUS_LABEL,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_LABEL);
        addProjection(sDataProjectionMap, Contacts.CONTACT_STATUS_ICON,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_ICON);

        // Handle projections for Data-level statuses
        addProjection(sDataProjectionMap, Data.PRESENCE,
                Tables.PRESENCE + "." + StatusUpdates.PRESENCE);
        addProjection(sDataProjectionMap, Data.STATUS,
                StatusUpdatesColumns.CONCRETE_STATUS);
        addProjection(sDataProjectionMap, Data.STATUS_TIMESTAMP,
                StatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP);
        addProjection(sDataProjectionMap, Data.STATUS_RES_PACKAGE,
                StatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE);
        addProjection(sDataProjectionMap, Data.STATUS_LABEL,
                StatusUpdatesColumns.CONCRETE_STATUS_LABEL);
        addProjection(sDataProjectionMap, Data.STATUS_ICON,
                StatusUpdatesColumns.CONCRETE_STATUS_ICON);

        // Projection map for data grouped by contact (not raw contact) and some data field(s)
        sDistinctDataProjectionMap = new HashMap<String, String>();
        sDistinctDataProjectionMap.put(Data._ID,
                "MIN(" + Data._ID + ") AS " + Data._ID);
        sDistinctDataProjectionMap.put(Data.DATA_VERSION, Data.DATA_VERSION);
        sDistinctDataProjectionMap.put(Data.IS_PRIMARY, Data.IS_PRIMARY);
        sDistinctDataProjectionMap.put(Data.IS_SUPER_PRIMARY, Data.IS_SUPER_PRIMARY);
        sDistinctDataProjectionMap.put(Data.RES_PACKAGE, Data.RES_PACKAGE);
        sDistinctDataProjectionMap.put(Data.MIMETYPE, Data.MIMETYPE);
        sDistinctDataProjectionMap.put(Data.DATA1, Data.DATA1);
        sDistinctDataProjectionMap.put(Data.DATA2, Data.DATA2);
        sDistinctDataProjectionMap.put(Data.DATA3, Data.DATA3);
        sDistinctDataProjectionMap.put(Data.DATA4, Data.DATA4);
        sDistinctDataProjectionMap.put(Data.DATA5, Data.DATA5);
        sDistinctDataProjectionMap.put(Data.DATA6, Data.DATA6);
        sDistinctDataProjectionMap.put(Data.DATA7, Data.DATA7);
        sDistinctDataProjectionMap.put(Data.DATA8, Data.DATA8);
        sDistinctDataProjectionMap.put(Data.DATA9, Data.DATA9);
        sDistinctDataProjectionMap.put(Data.DATA10, Data.DATA10);
        sDistinctDataProjectionMap.put(Data.DATA11, Data.DATA11);
        sDistinctDataProjectionMap.put(Data.DATA12, Data.DATA12);
        sDistinctDataProjectionMap.put(Data.DATA13, Data.DATA13);
        sDistinctDataProjectionMap.put(Data.DATA14, Data.DATA14);
        sDistinctDataProjectionMap.put(Data.DATA15, Data.DATA15);
        sDistinctDataProjectionMap.put(Data.SYNC1, Data.SYNC1);
        sDistinctDataProjectionMap.put(Data.SYNC2, Data.SYNC2);
        sDistinctDataProjectionMap.put(Data.SYNC3, Data.SYNC3);
        sDistinctDataProjectionMap.put(Data.SYNC4, Data.SYNC4);
        sDistinctDataProjectionMap.put(RawContacts.CONTACT_ID, RawContacts.CONTACT_ID);
        sDistinctDataProjectionMap.put(Contacts.LOOKUP_KEY, Contacts.LOOKUP_KEY);
        sDistinctDataProjectionMap.put(Contacts.DISPLAY_NAME, Contacts.DISPLAY_NAME);
        sDistinctDataProjectionMap.put(Contacts.CUSTOM_RINGTONE, Contacts.CUSTOM_RINGTONE);
        sDistinctDataProjectionMap.put(Contacts.SEND_TO_VOICEMAIL, Contacts.SEND_TO_VOICEMAIL);
        sDistinctDataProjectionMap.put(Contacts.LAST_TIME_CONTACTED, Contacts.LAST_TIME_CONTACTED);
        sDistinctDataProjectionMap.put(Contacts.TIMES_CONTACTED, Contacts.TIMES_CONTACTED);
        sDistinctDataProjectionMap.put(Contacts.STARRED, Contacts.STARRED);
        sDistinctDataProjectionMap.put(Contacts.PHOTO_ID, Contacts.PHOTO_ID);
        sDistinctDataProjectionMap.put(Contacts.IN_VISIBLE_GROUP, Contacts.IN_VISIBLE_GROUP);
        sDistinctDataProjectionMap.put(GroupMembership.GROUP_SOURCE_ID,
                GroupMembership.GROUP_SOURCE_ID);

        // Handle projections for Contacts-level statuses
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_PRESENCE,
                Tables.AGGREGATED_PRESENCE + "." + StatusUpdates.PRESENCE);
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_STATUS,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS);
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_STATUS_TIMESTAMP,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP);
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_STATUS_RES_PACKAGE,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE);
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_STATUS_LABEL,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_LABEL);
        addProjection(sDistinctDataProjectionMap, Contacts.CONTACT_STATUS_ICON,
                ContactsStatusUpdatesColumns.CONCRETE_STATUS_ICON);

        // Handle projections for Data-level statuses
        addProjection(sDistinctDataProjectionMap, Data.PRESENCE,
                Tables.PRESENCE + "." + StatusUpdates.PRESENCE);
        addProjection(sDistinctDataProjectionMap, Data.STATUS,
                StatusUpdatesColumns.CONCRETE_STATUS);
        addProjection(sDistinctDataProjectionMap, Data.STATUS_TIMESTAMP,
                StatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP);
        addProjection(sDistinctDataProjectionMap, Data.STATUS_RES_PACKAGE,
                StatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE);
        addProjection(sDistinctDataProjectionMap, Data.STATUS_LABEL,
                StatusUpdatesColumns.CONCRETE_STATUS_LABEL);
        addProjection(sDistinctDataProjectionMap, Data.STATUS_ICON,
                StatusUpdatesColumns.CONCRETE_STATUS_ICON);

        sPhoneLookupProjectionMap = new HashMap<String, String>();
        sPhoneLookupProjectionMap.put(PhoneLookup._ID,
                ContactsColumns.CONCRETE_ID + " AS " + PhoneLookup._ID);
        sPhoneLookupProjectionMap.put(PhoneLookup.LOOKUP_KEY,
                Contacts.LOOKUP_KEY + " AS " + PhoneLookup.LOOKUP_KEY);
        sPhoneLookupProjectionMap.put(PhoneLookup.DISPLAY_NAME,
                ContactsColumns.CONCRETE_DISPLAY_NAME + " AS " + PhoneLookup.DISPLAY_NAME);
        sPhoneLookupProjectionMap.put(PhoneLookup.LAST_TIME_CONTACTED,
                ContactsColumns.CONCRETE_LAST_TIME_CONTACTED
                        + " AS " + PhoneLookup.LAST_TIME_CONTACTED);
        sPhoneLookupProjectionMap.put(PhoneLookup.TIMES_CONTACTED,
                ContactsColumns.CONCRETE_TIMES_CONTACTED + " AS " + PhoneLookup.TIMES_CONTACTED);
        sPhoneLookupProjectionMap.put(PhoneLookup.STARRED,
                ContactsColumns.CONCRETE_STARRED + " AS " + PhoneLookup.STARRED);
        sPhoneLookupProjectionMap.put(PhoneLookup.IN_VISIBLE_GROUP,
                Contacts.IN_VISIBLE_GROUP + " AS " + PhoneLookup.IN_VISIBLE_GROUP);
        sPhoneLookupProjectionMap.put(PhoneLookup.PHOTO_ID,
                Contacts.PHOTO_ID + " AS " + PhoneLookup.PHOTO_ID);
        sPhoneLookupProjectionMap.put(PhoneLookup.CUSTOM_RINGTONE,
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE + " AS " + PhoneLookup.CUSTOM_RINGTONE);
        sPhoneLookupProjectionMap.put(PhoneLookup.HAS_PHONE_NUMBER,
                Contacts.HAS_PHONE_NUMBER + " AS " + PhoneLookup.HAS_PHONE_NUMBER);
        sPhoneLookupProjectionMap.put(PhoneLookup.SEND_TO_VOICEMAIL,
                ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                        + " AS " + PhoneLookup.SEND_TO_VOICEMAIL);
        sPhoneLookupProjectionMap.put(PhoneLookup.NUMBER,
                Phone.NUMBER + " AS " + PhoneLookup.NUMBER);
        sPhoneLookupProjectionMap.put(PhoneLookup.TYPE,
                Phone.TYPE + " AS " + PhoneLookup.TYPE);
        sPhoneLookupProjectionMap.put(PhoneLookup.LABEL,
                Phone.LABEL + " AS " + PhoneLookup.LABEL);

        // Groups projection map
        columns = new HashMap<String, String>();
        columns.put(Groups._ID, Groups._ID);
        columns.put(Groups.ACCOUNT_NAME, Groups.ACCOUNT_NAME);
        columns.put(Groups.ACCOUNT_TYPE, Groups.ACCOUNT_TYPE);
        columns.put(Groups.SOURCE_ID, Groups.SOURCE_ID);
        columns.put(Groups.DIRTY, Groups.DIRTY);
        columns.put(Groups.VERSION, Groups.VERSION);
        columns.put(Groups.RES_PACKAGE, Groups.RES_PACKAGE);
        columns.put(Groups.TITLE, Groups.TITLE);
        columns.put(Groups.TITLE_RES, Groups.TITLE_RES);
        columns.put(Groups.GROUP_VISIBLE, Groups.GROUP_VISIBLE);
        columns.put(Groups.SYSTEM_ID, Groups.SYSTEM_ID);
        columns.put(Groups.DELETED, Groups.DELETED);
        columns.put(Groups.NOTES, Groups.NOTES);
        columns.put(Groups.SHOULD_SYNC, Groups.SHOULD_SYNC);
        columns.put(Groups.SYNC1, Groups.SYNC1);
        columns.put(Groups.SYNC2, Groups.SYNC2);
        columns.put(Groups.SYNC3, Groups.SYNC3);
        columns.put(Groups.SYNC4, Groups.SYNC4);
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
                + " AND " + Contacts.HAS_PHONE_NUMBER + ") AS " + Groups.SUMMARY_WITH_PHONES);
        sGroupsSummaryProjectionMap = columns;

        // Aggregate exception projection map
        columns = new HashMap<String, String>();
        columns.put(AggregationExceptionColumns._ID, Tables.AGGREGATION_EXCEPTIONS + "._id AS _id");
        columns.put(AggregationExceptions.TYPE, AggregationExceptions.TYPE);
        columns.put(AggregationExceptions.RAW_CONTACT_ID1, AggregationExceptions.RAW_CONTACT_ID1);
        columns.put(AggregationExceptions.RAW_CONTACT_ID2, AggregationExceptions.RAW_CONTACT_ID2);
        sAggregationExceptionsProjectionMap = columns;

        // Settings projection map
        columns = new HashMap<String, String>();
        columns.put(Settings.ACCOUNT_NAME, Settings.ACCOUNT_NAME);
        columns.put(Settings.ACCOUNT_TYPE, Settings.ACCOUNT_TYPE);
        columns.put(Settings.UNGROUPED_VISIBLE, Settings.UNGROUPED_VISIBLE);
        columns.put(Settings.SHOULD_SYNC, Settings.SHOULD_SYNC);
        columns.put(Settings.ANY_UNSYNCED, "(CASE WHEN MIN(" + Settings.SHOULD_SYNC
                + ",(SELECT (CASE WHEN MIN(" + Groups.SHOULD_SYNC + ") IS NULL THEN 1 ELSE MIN("
                + Groups.SHOULD_SYNC + ") END) FROM " + Tables.GROUPS + " WHERE "
                + GroupsColumns.CONCRETE_ACCOUNT_NAME + "=" + SettingsColumns.CONCRETE_ACCOUNT_NAME
                + " AND " + GroupsColumns.CONCRETE_ACCOUNT_TYPE + "="
                + SettingsColumns.CONCRETE_ACCOUNT_TYPE + "))=0 THEN 1 ELSE 0 END) AS "
                + Settings.ANY_UNSYNCED);
        columns.put(Settings.UNGROUPED_COUNT, "(SELECT COUNT(*) FROM (SELECT 1 FROM "
                + Tables.SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS + " GROUP BY "
                + Clauses.GROUP_BY_ACCOUNT_CONTACT_ID + " HAVING " + Clauses.HAVING_NO_GROUPS
                + ")) AS " + Settings.UNGROUPED_COUNT);
        columns.put(Settings.UNGROUPED_WITH_PHONES, "(SELECT COUNT(*) FROM (SELECT 1 FROM "
                + Tables.SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS + " WHERE "
                + Contacts.HAS_PHONE_NUMBER + " GROUP BY " + Clauses.GROUP_BY_ACCOUNT_CONTACT_ID
                + " HAVING " + Clauses.HAVING_NO_GROUPS + ")) AS "
                + Settings.UNGROUPED_WITH_PHONES);
        sSettingsProjectionMap = columns;

        columns = new HashMap<String, String>();
        columns.put(PresenceColumns.RAW_CONTACT_ID, PresenceColumns.RAW_CONTACT_ID);
        columns.put(StatusUpdates.DATA_ID,
                DataColumns.CONCRETE_ID + " AS " + StatusUpdates.DATA_ID);
        columns.put(StatusUpdates.IM_ACCOUNT, StatusUpdates.IM_ACCOUNT);
        columns.put(StatusUpdates.IM_HANDLE, StatusUpdates.IM_HANDLE);
        columns.put(StatusUpdates.PROTOCOL, StatusUpdates.PROTOCOL);
        // We cannot allow a null in the custom protocol field, because SQLite3 does not
        // properly enforce uniqueness of null values
        columns.put(StatusUpdates.CUSTOM_PROTOCOL, "(CASE WHEN " + StatusUpdates.CUSTOM_PROTOCOL
                + "='' THEN NULL ELSE " + StatusUpdates.CUSTOM_PROTOCOL + " END) AS "
                + StatusUpdates.CUSTOM_PROTOCOL);
        columns.put(StatusUpdates.PRESENCE, StatusUpdates.PRESENCE);
        columns.put(StatusUpdates.STATUS, StatusUpdates.STATUS);
        columns.put(StatusUpdates.STATUS_TIMESTAMP, StatusUpdates.STATUS_TIMESTAMP);
        columns.put(StatusUpdates.STATUS_RES_PACKAGE, StatusUpdates.STATUS_RES_PACKAGE);
        columns.put(StatusUpdates.STATUS_ICON, StatusUpdates.STATUS_ICON);
        columns.put(StatusUpdates.STATUS_LABEL, StatusUpdates.STATUS_LABEL);
        sStatusUpdatesProjectionMap = columns;

        // Live folder projection
        sLiveFoldersProjectionMap = new HashMap<String, String>();
        sLiveFoldersProjectionMap.put(LiveFolders._ID,
                Contacts._ID + " AS " + LiveFolders._ID);
        sLiveFoldersProjectionMap.put(LiveFolders.NAME,
                Contacts.DISPLAY_NAME + " AS " + LiveFolders.NAME);

        // TODO: Put contact photo back when we have a way to display a default icon
        // for contacts without a photo
        // sLiveFoldersProjectionMap.put(LiveFolders.ICON_BITMAP,
        //      Photos.DATA + " AS " + LiveFolders.ICON_BITMAP);
    }

    private static void addProjection(HashMap<String, String> map, String toField, String fromField) {
        map.put(toField, fromField + " AS " + toField);
    }

    /**
     * Handles inserts and update for a specific Data type.
     */
    private abstract class DataRowHandler {

        protected final String mMimetype;
        protected long mMimetypeId;

        public DataRowHandler(String mimetype) {
            mMimetype = mimetype;

            // To ensure the data column position. This is dead code if properly configured.
            if (StructuredName.DISPLAY_NAME != Data.DATA1 || Nickname.NAME != Data.DATA1
                    || Organization.COMPANY != Data.DATA1 || Phone.NUMBER != Data.DATA1
                    || Email.DATA != Data.DATA1) {
                throw new AssertionError("Some of ContactsContract.CommonDataKinds class primary"
                        + " data is not in DATA1 column");
            }
        }

        protected long getMimeTypeId() {
            if (mMimetypeId == 0) {
                mMimetypeId = mDbHelper.getMimeTypeId(mMimetype);
            }
            return mMimetypeId;
        }

        /**
         * Inserts a row into the {@link Data} table.
         */
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            final long dataId = db.insert(Tables.DATA, null, values);

            Integer primary = values.getAsInteger(Data.IS_PRIMARY);
            if (primary != null && primary != 0) {
                setIsPrimary(rawContactId, dataId, getMimeTypeId());
            }

            return dataId;
        }

        /**
         * Validates data and updates a {@link Data} row using the cursor, which contains
         * the current data.
         */
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);

            if (values.containsKey(Data.IS_SUPER_PRIMARY)) {
                long mimeTypeId = getMimeTypeId();
                setIsSuperPrimary(rawContactId, dataId, mimeTypeId);
                setIsPrimary(rawContactId, dataId, mimeTypeId);

                // Now that we've taken care of setting these, remove them from "values".
                values.remove(Data.IS_SUPER_PRIMARY);
                values.remove(Data.IS_PRIMARY);
            } else if (values.containsKey(Data.IS_PRIMARY)) {
                setIsPrimary(rawContactId, dataId, getMimeTypeId());

                // Now that we've taken care of setting this, remove it from "values".
                values.remove(Data.IS_PRIMARY);
            }

            if (values.size() > 0) {
                mDb.update(Tables.DATA, values, Data._ID + " = " + dataId, null);
            }

            if (!callerIsSyncAdapter) {
                setRawContactDirty(rawContactId);
            }
        }

        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataDeleteQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
            boolean primary = c.getInt(DataDeleteQuery.IS_PRIMARY) != 0;
            int count = db.delete(Tables.DATA, Data._ID + "=" + dataId, null);
            db.delete(Tables.PRESENCE, PresenceColumns.RAW_CONTACT_ID + "=" + rawContactId, null);
            if (count != 0 && primary) {
                fixPrimary(db, rawContactId);
            }
            return count;
        }

        private void fixPrimary(SQLiteDatabase db, long rawContactId) {
            long newPrimaryId = findNewPrimaryDataId(db, rawContactId);
            if (newPrimaryId != -1) {
                setIsPrimary(rawContactId, newPrimaryId, getMimeTypeId());
            }
        }

        protected long findNewPrimaryDataId(SQLiteDatabase db, long rawContactId) {
            long primaryId = -1;
            int primaryType = -1;
            Cursor c = queryData(db, rawContactId);
            try {
                while (c.moveToNext()) {
                    long dataId = c.getLong(DataDeleteQuery._ID);
                    int type = c.getInt(DataDeleteQuery.DATA1);
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
            return db.query(DataDeleteQuery.TABLE, DataDeleteQuery.CONCRETE_COLUMNS,
                    Data.RAW_CONTACT_ID + "=" + rawContactId +
                    " AND " + MimetypesColumns.MIMETYPE + "='" + mMimetype + "'",
                    null, null, null, null);
        }

        protected void fixRawContactDisplayName(SQLiteDatabase db, long rawContactId) {
            String bestDisplayName = null;
            int bestDisplayNameSource = DisplayNameSources.UNDEFINED;

            Cursor c = db.query(DisplayNameQuery.TABLE, DisplayNameQuery.COLUMNS,
                    Data.RAW_CONTACT_ID + "=" + rawContactId, null, null, null, null);
            try {
                while (c.moveToNext()) {
                    String mimeType = c.getString(DisplayNameQuery.MIMETYPE);

                    // Display name is at DATA1 in all type.  This is ensured in the constructor.
                    String name = c.getString(DisplayNameQuery.DATA);
                    if (TextUtils.isEmpty(name)
                            && Organization.CONTENT_ITEM_TYPE.equals(mimeType)) {
                        name = c.getString(DisplayNameQuery.TITLE);
                    }
                    boolean primary = StructuredName.CONTENT_ITEM_TYPE.equals(mimeType)
                        || (c.getInt(DisplayNameQuery.IS_PRIMARY) != 0);

                    if (name != null) {
                        Integer source = sDisplayNameSources.get(mimeType);
                        if (source != null
                                && (source > bestDisplayNameSource
                                        || (source == bestDisplayNameSource && primary))) {
                            bestDisplayNameSource = source;
                            bestDisplayName = name;
                        }
                    }
                }

            } finally {
                c.close();
            }

            setDisplayName(rawContactId, bestDisplayName, bestDisplayNameSource);
            if (!isNewRawContact(rawContactId)) {
                mContactAggregator.updateDisplayName(db, rawContactId);
            }
        }

        public boolean isAggregationRequired() {
            return true;
        }

        /**
         * Return set of values, using current values at given {@link Data#_ID}
         * as baseline, but augmented with any updates.
         */
        public ContentValues getAugmentedValues(SQLiteDatabase db, long dataId,
                ContentValues update) {
            final ContentValues values = new ContentValues();
            final Cursor cursor = db.query(Tables.DATA, null, Data._ID + "=" + dataId,
                    null, null, null, null);
            try {
                if (cursor.moveToFirst()) {
                    for (int i = 0; i < cursor.getColumnCount(); i++) {
                        final String key = cursor.getColumnName(i);
                        values.put(key, cursor.getString(i));
                    }
                }
            } finally {
                cursor.close();
            }
            values.putAll(update);
            return values;
        }
    }

    public class CustomDataRowHandler extends DataRowHandler {

        public CustomDataRowHandler(String mimetype) {
            super(mimetype);
        }
    }

    public class StructuredNameRowHandler extends DataRowHandler {
        private final NameSplitter mSplitter;

        public StructuredNameRowHandler(NameSplitter splitter) {
            super(StructuredName.CONTENT_ITEM_TYPE);
            mSplitter = splitter;
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            fixStructuredNameComponents(values, values);

            long dataId = super.insert(db, rawContactId, values);

            String name = values.getAsString(StructuredName.DISPLAY_NAME);
            insertNameLookupForStructuredName(rawContactId, dataId, name);
            fixRawContactDisplayName(db, rawContactId);
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            final long dataId = c.getLong(DataUpdateQuery._ID);
            final long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);

            final ContentValues augmented = getAugmentedValues(db, dataId, values);
            fixStructuredNameComponents(augmented, values);

            super.update(db, values, c, callerIsSyncAdapter);

            if (values.containsKey(StructuredName.DISPLAY_NAME)) {
                String name = values.getAsString(StructuredName.DISPLAY_NAME);
                deleteNameLookup(dataId);
                insertNameLookupForStructuredName(rawContactId, dataId, name);
            }
            fixRawContactDisplayName(db, rawContactId);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataDeleteQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

            int count = super.delete(db, c);

            deleteNameLookup(dataId);
            fixRawContactDisplayName(db, rawContactId);
            return count;
        }

        /**
         * Specific list of structured fields.
         */
        private final String[] STRUCTURED_FIELDS = new String[] {
                StructuredName.PREFIX, StructuredName.GIVEN_NAME, StructuredName.MIDDLE_NAME,
                StructuredName.FAMILY_NAME, StructuredName.SUFFIX
        };

        /**
         * Parses the supplied display name, but only if the incoming values do
         * not already contain structured name parts. Also, if the display name
         * is not provided, generate one by concatenating first name and last
         * name.
         */
        private void fixStructuredNameComponents(ContentValues augmented, ContentValues update) {
            final String unstruct = update.getAsString(StructuredName.DISPLAY_NAME);

            final boolean touchedUnstruct = !TextUtils.isEmpty(unstruct);
            final boolean touchedStruct = !areAllEmpty(update, STRUCTURED_FIELDS);

            if (touchedUnstruct && !touchedStruct) {
                NameSplitter.Name name = new NameSplitter.Name();
                mSplitter.split(name, unstruct);
                name.toValues(update);
            } else if (!touchedUnstruct
                    && (touchedStruct || areAnySpecified(update, STRUCTURED_FIELDS))) {
                // We need to update the display name when any structured components
                // are specified, even when they are null, which is why we are checking
                // areAnySpecified.  The touchedStruct in the condition is an optimization:
                // if there are non-null values, we know for a fact that some values are present.
                NameSplitter.Name name = new NameSplitter.Name();
                name.fromValues(augmented);
                final String joined = mSplitter.join(name);
                update.put(StructuredName.DISPLAY_NAME, joined);
            }
        }
    }

    public class StructuredPostalRowHandler extends DataRowHandler {
        private PostalSplitter mSplitter;

        public StructuredPostalRowHandler(PostalSplitter splitter) {
            super(StructuredPostal.CONTENT_ITEM_TYPE);
            mSplitter = splitter;
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            fixStructuredPostalComponents(values, values);
            return super.insert(db, rawContactId, values);
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            final long dataId = c.getLong(DataUpdateQuery._ID);
            final ContentValues augmented = getAugmentedValues(db, dataId, values);
            fixStructuredPostalComponents(augmented, values);
            super.update(db, values, c, callerIsSyncAdapter);
        }

        /**
         * Specific list of structured fields.
         */
        private final String[] STRUCTURED_FIELDS = new String[] {
                StructuredPostal.STREET, StructuredPostal.POBOX, StructuredPostal.NEIGHBORHOOD,
                StructuredPostal.CITY, StructuredPostal.REGION, StructuredPostal.POSTCODE,
                StructuredPostal.COUNTRY,
        };

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
                // See comment in
                postal.fromValues(augmented);
                final String joined = mSplitter.join(postal);
                update.put(StructuredPostal.FORMATTED_ADDRESS, joined);
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
            enforceTypeAndLabel(values, values);
            return super.insert(db, rawContactId, values);
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            final long dataId = c.getLong(DataUpdateQuery._ID);
            final ContentValues augmented = getAugmentedValues(db, dataId, values);
            enforceTypeAndLabel(augmented, values);
            super.update(db, values, c, callerIsSyncAdapter);
        }

        /**
         * If the given {@link ContentValues} defines {@link #mTypeColumn},
         * enforce that {@link #mLabelColumn} only appears when type is
         * {@link BaseTypes#TYPE_CUSTOM}. Exception is thrown otherwise.
         */
        private void enforceTypeAndLabel(ContentValues augmented, ContentValues update) {
            final boolean hasType = !TextUtils.isEmpty(augmented.getAsString(mTypeColumn));
            final boolean hasLabel = !TextUtils.isEmpty(augmented.getAsString(mLabelColumn));

            if (hasLabel && !hasType) {
                // When label exists, assert that some type is defined
                throw new IllegalArgumentException(mTypeColumn + " must be specified when "
                        + mLabelColumn + " is defined.");
            }
        }
    }

    public class OrganizationDataRowHandler extends CommonDataRowHandler {

        public OrganizationDataRowHandler() {
            super(Organization.CONTENT_ITEM_TYPE, Organization.TYPE, Organization.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            String company = values.getAsString(Organization.COMPANY);
            String title = values.getAsString(Organization.TITLE);

            long dataId = super.insert(db, rawContactId, values);

            fixRawContactDisplayName(db, rawContactId);
            insertNameLookupForOrganization(rawContactId, dataId, company, title);
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            String company = values.getAsString(Organization.COMPANY);
            String title = values.getAsString(Organization.TITLE);
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);

            super.update(db, values, c, callerIsSyncAdapter);

            fixRawContactDisplayName(db, rawContactId);
            deleteNameLookup(dataId);
            insertNameLookupForOrganization(rawContactId, dataId, company, title);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

            int count = super.delete(db, c);
            fixRawContactDisplayName(db, rawContactId);
            deleteNameLookup(dataId);
            return count;
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

        @Override
        public boolean isAggregationRequired() {
            return false;
        }
    }

    public class EmailDataRowHandler extends CommonDataRowHandler {

        public EmailDataRowHandler() {
            super(Email.CONTENT_ITEM_TYPE, Email.TYPE, Email.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            String address = values.getAsString(Email.DATA);

            long dataId = super.insert(db, rawContactId, values);

            fixRawContactDisplayName(db, rawContactId);
            insertNameLookupForEmail(rawContactId, dataId, address);
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            String address = values.getAsString(Email.DATA);

            super.update(db, values, c, callerIsSyncAdapter);

            deleteNameLookup(dataId);
            insertNameLookupForEmail(rawContactId, dataId, address);
            fixRawContactDisplayName(db, rawContactId);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataDeleteQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

            int count = super.delete(db, c);

            deleteNameLookup(dataId);
            fixRawContactDisplayName(db, rawContactId);
            return count;
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

    public class NicknameDataRowHandler extends CommonDataRowHandler {

        public NicknameDataRowHandler() {
            super(Nickname.CONTENT_ITEM_TYPE, Nickname.TYPE, Nickname.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            String nickname = values.getAsString(Nickname.NAME);

            long dataId = super.insert(db, rawContactId, values);

            fixRawContactDisplayName(db, rawContactId);
            insertNameLookupForNickname(rawContactId, dataId, nickname);
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            String nickname = values.getAsString(Nickname.NAME);

            super.update(db, values, c, callerIsSyncAdapter);

            deleteNameLookup(dataId);
            insertNameLookupForNickname(rawContactId, dataId, nickname);
            fixRawContactDisplayName(db, rawContactId);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataDeleteQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

            int count = super.delete(db, c);

            deleteNameLookup(dataId);
            fixRawContactDisplayName(db, rawContactId);
            return count;
        }
    }

    public class PhoneDataRowHandler extends CommonDataRowHandler {

        public PhoneDataRowHandler() {
            super(Phone.CONTENT_ITEM_TYPE, Phone.TYPE, Phone.LABEL);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            long dataId;
            if (values.containsKey(Phone.NUMBER)) {
                String number = values.getAsString(Phone.NUMBER);
                String normalizedNumber = computeNormalizedNumber(number, values);

                dataId = super.insert(db, rawContactId, values);

                updatePhoneLookup(db, rawContactId, dataId, number, normalizedNumber);
                mContactAggregator.updateHasPhoneNumber(db, rawContactId);
                fixRawContactDisplayName(db, rawContactId);
            } else {
                dataId = super.insert(db, rawContactId, values);
            }
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long dataId = c.getLong(DataUpdateQuery._ID);
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            if (values.containsKey(Phone.NUMBER)) {
                String number = values.getAsString(Phone.NUMBER);
                String normalizedNumber = computeNormalizedNumber(number, values);

                super.update(db, values, c, callerIsSyncAdapter);

                updatePhoneLookup(db, rawContactId, dataId, number, normalizedNumber);
                mContactAggregator.updateHasPhoneNumber(db, rawContactId);
                fixRawContactDisplayName(db, rawContactId);
            } else {
                super.update(db, values, c, callerIsSyncAdapter);
            }
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long dataId = c.getLong(DataDeleteQuery._ID);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);

            int count = super.delete(db, c);

            updatePhoneLookup(db, rawContactId, dataId, null, null);
            mContactAggregator.updateHasPhoneNumber(db, rawContactId);
            fixRawContactDisplayName(db, rawContactId);
            return count;
        }

        private String computeNormalizedNumber(String number, ContentValues values) {
            String normalizedNumber = null;
            if (number != null) {
                normalizedNumber = PhoneNumberUtils.getStrippedReversed(number);
            }
            values.put(PhoneColumns.NORMALIZED_NUMBER, normalizedNumber);
            return normalizedNumber;
        }

        private void updatePhoneLookup(SQLiteDatabase db, long rawContactId, long dataId,
                String number, String normalizedNumber) {
            if (number != null) {
                ContentValues phoneValues = new ContentValues();
                phoneValues.put(PhoneLookupColumns.RAW_CONTACT_ID, rawContactId);
                phoneValues.put(PhoneLookupColumns.DATA_ID, dataId);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER, normalizedNumber);
                db.replace(Tables.PHONE_LOOKUP, null, phoneValues);
            } else {
                db.delete(Tables.PHONE_LOOKUP, PhoneLookupColumns.DATA_ID + "=" + dataId, null);
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
    }

    public class GroupMembershipRowHandler extends DataRowHandler {

        public GroupMembershipRowHandler() {
            super(GroupMembership.CONTENT_ITEM_TYPE);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            resolveGroupSourceIdInValues(rawContactId, db, values, true);
            long dataId = super.insert(db, rawContactId, values);
            updateVisibility(rawContactId);
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            resolveGroupSourceIdInValues(rawContactId, db, values, false);
            super.update(db, values, c, callerIsSyncAdapter);
            updateVisibility(rawContactId);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
            int count = super.delete(db, c);
            updateVisibility(rawContactId);
            return count;
        }

        private void updateVisibility(long rawContactId) {
            long contactId = mDbHelper.getContactId(rawContactId);
            if (contactId != 0) {
                mDbHelper.updateContactVisible(contactId);
            }
        }

        private void resolveGroupSourceIdInValues(long rawContactId, SQLiteDatabase db,
                ContentValues values, boolean isInsert) {
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

        @Override
        public boolean isAggregationRequired() {
            return false;
        }
    }

    public class PhotoDataRowHandler extends DataRowHandler {

        public PhotoDataRowHandler() {
            super(Photo.CONTENT_ITEM_TYPE);
        }

        @Override
        public long insert(SQLiteDatabase db, long rawContactId, ContentValues values) {
            long dataId = super.insert(db, rawContactId, values);
            if (!isNewRawContact(rawContactId)) {
                mContactAggregator.updatePhotoId(db, rawContactId);
            }
            return dataId;
        }

        @Override
        public void update(SQLiteDatabase db, ContentValues values, Cursor c,
                boolean callerIsSyncAdapter) {
            long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
            super.update(db, values, c, callerIsSyncAdapter);
            mContactAggregator.updatePhotoId(db, rawContactId);
        }

        @Override
        public int delete(SQLiteDatabase db, Cursor c) {
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
            int count = super.delete(db, c);
            mContactAggregator.updatePhotoId(db, rawContactId);
            return count;
        }

        @Override
        public boolean isAggregationRequired() {
            return false;
        }
    }


    private HashMap<String, DataRowHandler> mDataRowHandlers;
    private final ContactAggregationScheduler mAggregationScheduler;
    private ContactsDatabaseHelper mDbHelper;

    private NameSplitter mNameSplitter;
    private NameLookupBuilder mNameLookupBuilder;
    private HashMap<String, SoftReference<String[]>> mNicknameClusterCache =
            new HashMap<String, SoftReference<String[]>>();
    private PostalSplitter mPostalSplitter;

    private ContactAggregator mContactAggregator;
    private LegacyApiSupport mLegacyApiSupport;
    private GlobalSearchSupport mGlobalSearchSupport;

    private ContentValues mValues = new ContentValues();

    private volatile CountDownLatch mAccessLatch;
    private boolean mImportMode;

    private HashSet<Long> mInsertedRawContacts = Sets.newHashSet();
    private HashSet<Long> mUpdatedRawContacts = Sets.newHashSet();
    private HashMap<Long, Object> mUpdatedSyncStates = Maps.newHashMap();

    private boolean mVisibleTouched = false;

    private boolean mSyncToNetwork;

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
        super.onCreate();
        try {
            return initialize();
        } catch (RuntimeException e) {
            Log.e(TAG, "Cannot start provider", e);
            return false;
        }
    }

    private boolean initialize() {
        final Context context = getContext();
        mDbHelper = (ContactsDatabaseHelper)getDatabaseHelper();
        mGlobalSearchSupport = new GlobalSearchSupport(this);
        mLegacyApiSupport = new LegacyApiSupport(context, mDbHelper, this, mGlobalSearchSupport);
        mContactAggregator = new ContactAggregator(this, mDbHelper, mAggregationScheduler);
        mContactAggregator.setEnabled(SystemProperties.getBoolean(AGGREGATE_CONTACTS, true));

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();

        mSetPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA +
                " SET " + Data.IS_PRIMARY + "=(_id=?)" +
                " WHERE " + DataColumns.MIMETYPE_ID + "=?" +
                "   AND " + Data.RAW_CONTACT_ID + "=?");

        mSetSuperPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA +
                " SET " + Data.IS_SUPER_PRIMARY + "=(" + Data._ID + "=?)" +
                " WHERE " + DataColumns.MIMETYPE_ID + "=?" +
                "   AND " + Data.RAW_CONTACT_ID + " IN (" +
                        "SELECT " + RawContacts._ID +
                        " FROM " + Tables.RAW_CONTACTS +
                        " WHERE " + RawContacts.CONTACT_ID + " =(" +
                                "SELECT " + RawContacts.CONTACT_ID +
                                " FROM " + Tables.RAW_CONTACTS +
                                " WHERE " + RawContacts._ID + "=?))");

        mContactsLastTimeContactedUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.LAST_TIME_CONTACTED + "=? " +
                "WHERE " + Contacts._ID + "=?");

        mRawContactDisplayNameUpdate = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.DISPLAY_NAME + "=?,"
                        + RawContactsColumns.DISPLAY_NAME_SOURCE + "=?" +
                " WHERE " + RawContacts._ID + "=?");

        mRawContactDirtyUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContacts.DIRTY + "=1 WHERE " + RawContacts._ID + "=?");

        mLastStatusUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS
                + " SET " + ContactsColumns.LAST_STATUS_UPDATE_ID + "=" +
                        "(SELECT " + DataColumns.CONCRETE_ID +
                        " FROM " + Tables.STATUS_UPDATES +
                        " JOIN " + Tables.DATA +
                        "   ON (" + StatusUpdatesColumns.DATA_ID + "="
                                + DataColumns.CONCRETE_ID + ")" +
                        " JOIN " + Tables.RAW_CONTACTS +
                        "   ON (" + DataColumns.CONCRETE_RAW_CONTACT_ID + "="
                                + RawContactsColumns.CONCRETE_ID + ")" +
                        " WHERE " + RawContacts.CONTACT_ID + "=?" +
                        " ORDER BY " + StatusUpdates.STATUS_TIMESTAMP + " DESC,"
                                + StatusUpdates.STATUS +
                        " LIMIT 1)"
                + " WHERE " + ContactsColumns.CONCRETE_ID + "=?");

        final Locale locale = Locale.getDefault();
        mNameSplitter = new NameSplitter(
                context.getString(com.android.internal.R.string.common_name_prefixes),
                context.getString(com.android.internal.R.string.common_last_name_prefixes),
                context.getString(com.android.internal.R.string.common_name_suffixes),
                context.getString(com.android.internal.R.string.common_name_conjunctions),
                locale);
        mNameLookupBuilder = new StructuredNameLookupBuilder(mNameSplitter);
        mPostalSplitter = new PostalSplitter(locale);

        mNameLookupInsert = db.compileStatement("INSERT OR IGNORE INTO " + Tables.NAME_LOOKUP + "("
                + NameLookupColumns.RAW_CONTACT_ID + "," + NameLookupColumns.DATA_ID + ","
                + NameLookupColumns.NAME_TYPE + "," + NameLookupColumns.NORMALIZED_NAME
                + ") VALUES (?,?,?,?)");
        mNameLookupDelete = db.compileStatement("DELETE FROM " + Tables.NAME_LOOKUP + " WHERE "
                + NameLookupColumns.DATA_ID + "=?");

        mStatusUpdateInsert = db.compileStatement(
                "INSERT INTO " + Tables.STATUS_UPDATES + "("
                        + StatusUpdatesColumns.DATA_ID + ", "
                        + StatusUpdates.STATUS + ","
                        + StatusUpdates.STATUS_RES_PACKAGE + ","
                        + StatusUpdates.STATUS_ICON + ","
                        + StatusUpdates.STATUS_LABEL + ")" +
                " VALUES (?,?,?,?,?)");

        mStatusUpdateReplace = db.compileStatement(
                "INSERT OR REPLACE INTO " + Tables.STATUS_UPDATES + "("
                        + StatusUpdatesColumns.DATA_ID + ", "
                        + StatusUpdates.STATUS_TIMESTAMP + ","
                        + StatusUpdates.STATUS + ","
                        + StatusUpdates.STATUS_RES_PACKAGE + ","
                        + StatusUpdates.STATUS_ICON + ","
                        + StatusUpdates.STATUS_LABEL + ")" +
                " VALUES (?,?,?,?,?,?)");

        mStatusUpdateAutoTimestamp = db.compileStatement(
                "UPDATE " + Tables.STATUS_UPDATES +
                " SET " + StatusUpdates.STATUS_TIMESTAMP + "=?,"
                        + StatusUpdates.STATUS + "=?" +
                " WHERE " + StatusUpdatesColumns.DATA_ID + "=?"
                        + " AND " + StatusUpdates.STATUS + "!=?");

        mStatusAttributionUpdate = db.compileStatement(
                "UPDATE " + Tables.STATUS_UPDATES +
                " SET " + StatusUpdates.STATUS_RES_PACKAGE + "=?,"
                        + StatusUpdates.STATUS_ICON + "=?,"
                        + StatusUpdates.STATUS_LABEL + "=?" +
                " WHERE " + StatusUpdatesColumns.DATA_ID + "=?");

        mStatusUpdateDelete = db.compileStatement(
                "DELETE FROM " + Tables.STATUS_UPDATES +
                " WHERE " + StatusUpdatesColumns.DATA_ID + "=?");

        mDataRowHandlers = new HashMap<String, DataRowHandler>();

        mDataRowHandlers.put(Email.CONTENT_ITEM_TYPE, new EmailDataRowHandler());
        mDataRowHandlers.put(Im.CONTENT_ITEM_TYPE,
                new CommonDataRowHandler(Im.CONTENT_ITEM_TYPE, Im.TYPE, Im.LABEL));
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE, new CommonDataRowHandler(
                StructuredPostal.CONTENT_ITEM_TYPE, StructuredPostal.TYPE, StructuredPostal.LABEL));
        mDataRowHandlers.put(Organization.CONTENT_ITEM_TYPE, new OrganizationDataRowHandler());
        mDataRowHandlers.put(Phone.CONTENT_ITEM_TYPE, new PhoneDataRowHandler());
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE, new NicknameDataRowHandler());
        mDataRowHandlers.put(StructuredName.CONTENT_ITEM_TYPE,
                new StructuredNameRowHandler(mNameSplitter));
        mDataRowHandlers.put(StructuredPostal.CONTENT_ITEM_TYPE,
                new StructuredPostalRowHandler(mPostalSplitter));
        mDataRowHandlers.put(GroupMembership.CONTENT_ITEM_TYPE, new GroupMembershipRowHandler());
        mDataRowHandlers.put(Photo.CONTENT_ITEM_TYPE, new PhotoDataRowHandler());

        if (isLegacyContactImportNeeded()) {
            importLegacyContactsAsync();
        }

        verifyAccounts();

        mMimeTypeIdEmail = mDbHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);
        mMimeTypeIdIm = mDbHelper.getMimeTypeId(Im.CONTENT_ITEM_TYPE);
        return (db != null);
    }

    protected void verifyAccounts() {
        AccountManager.get(getContext()).addOnAccountsUpdatedListener(this, null, false);
        onAccountsUpdated(AccountManager.get(getContext()).getAccounts());
    }

    /* Visible for testing */
    @Override
    protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
        return ContactsDatabaseHelper.getInstance(context);
    }

    /* package */ ContactAggregationScheduler getContactAggregationScheduler() {
        return mAggregationScheduler;
    }

    /* package */ NameSplitter getNameSplitter() {
        return mNameSplitter;
    }

    protected boolean isLegacyContactImportNeeded() {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
        return prefs.getInt(PREF_CONTACTS_IMPORTED, 0) < PREF_CONTACTS_IMPORT_VERSION;
    }

    protected LegacyContactImporter getLegacyContactImporter() {
        return new LegacyContactImporter(getContext(), this);
    }

    /**
     * Imports legacy contacts in a separate thread.  As long as the import process is running
     * all other access to the contacts is blocked.
     */
    private void importLegacyContactsAsync() {
        mAccessLatch = new CountDownLatch(1);

        Thread importThread = new Thread("LegacyContactImport") {
            @Override
            public void run() {
                if (importLegacyContacts()) {

                    /*
                     * When the import process is done, we can unlock the provider and
                     * start aggregating the imported contacts asynchronously.
                     */
                    mAccessLatch.countDown();
                    mAccessLatch = null;
                    scheduleContactAggregation();
                }
            }
        };

        importThread.start();
    }

    private boolean importLegacyContacts() {
        LegacyContactImporter importer = getLegacyContactImporter();
        if (importLegacyContacts(importer)) {
            SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
            Editor editor = prefs.edit();
            editor.putInt(PREF_CONTACTS_IMPORTED, PREF_CONTACTS_IMPORT_VERSION);
            editor.commit();
            return true;
        } else {
            return false;
        }
    }

    /* Visible for testing */
    /* package */ boolean importLegacyContacts(LegacyContactImporter importer) {
        boolean aggregatorEnabled = mContactAggregator.isEnabled();
        mContactAggregator.setEnabled(false);
        mImportMode = true;
        try {
            importer.importContacts();
            mContactAggregator.setEnabled(aggregatorEnabled);
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
        mDbHelper.wipeData();
    }

    /**
     * While importing and aggregating contacts, this content provider will
     * block all attempts to change contacts data. In particular, it will hold
     * up all contact syncs. As soon as the import process is complete, all
     * processes waiting to write to the provider are unblocked and can proceed
     * to compete for the database transaction monitor.
     */
    private void waitForAccess() {
        CountDownLatch latch = mAccessLatch;
        if (latch != null) {
            while (true) {
                try {
                    latch.await();
                    mAccessLatch = null;
                    return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        waitForAccess();
        return super.insert(uri, values);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        waitForAccess();
        return super.update(uri, values, selection, selectionArgs);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        waitForAccess();
        return super.delete(uri, selection, selectionArgs);
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        waitForAccess();
        return super.applyBatch(operations);
    }

    @Override
    protected void onBeginTransaction() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "onBeginTransaction");
        }
        super.onBeginTransaction();
        mContactAggregator.clearPendingAggregations();
        clearTransactionalChanges();
    }

    private void clearTransactionalChanges() {
        mInsertedRawContacts.clear();
        mUpdatedRawContacts.clear();
        mUpdatedSyncStates.clear();
    }

    @Override
    protected void beforeTransactionCommit() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "beforeTransactionCommit");
        }
        super.beforeTransactionCommit();
        flushTransactionalChanges();
        mContactAggregator.aggregateInTransaction(mDb);
        if (mVisibleTouched) {
            mVisibleTouched = false;
            mDbHelper.updateAllVisible();
        }
    }

    private void flushTransactionalChanges() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "flushTransactionChanges");
        }
        for (long rawContactId : mInsertedRawContacts) {
            mContactAggregator.insertContact(mDb, rawContactId);
        }

        String ids;
        if (!mUpdatedRawContacts.isEmpty()) {
            ids = buildIdsString(mUpdatedRawContacts);
            mDb.execSQL("UPDATE raw_contacts SET version = version + 1 WHERE _id in " + ids,
                    new Object[]{});
        }

        for (Map.Entry<Long, Object> entry : mUpdatedSyncStates.entrySet()) {
            long id = entry.getKey();
            mDbHelper.getSyncState().update(mDb, id, entry.getValue());
        }

        clearTransactionalChanges();
    }

    private String buildIdsString(HashSet<Long> ids) {
        StringBuilder idsBuilder = null;
        for (long id : ids) {
            if (idsBuilder == null) {
                idsBuilder = new StringBuilder();
                idsBuilder.append("(");
            } else {
                idsBuilder.append(",");
            }
            idsBuilder.append(id);
        }
        idsBuilder.append(")");
        return idsBuilder.toString();
    }

    @Override
    protected void notifyChange() {
        notifyChange(mSyncToNetwork);
        mSyncToNetwork = false;
    }

    protected void notifyChange(boolean syncToNetwork) {
        getContext().getContentResolver().notifyChange(ContactsContract.AUTHORITY_URI, null,
                syncToNetwork);
    }

    protected void scheduleContactAggregation() {
        mContactAggregator.schedule();
    }

    private boolean isNewRawContact(long rawContactId) {
        return mInsertedRawContacts.contains(rawContactId);
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
    protected Uri insertInTransaction(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insertInTransaction: " + uri);
        }

        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, ContactsContract.CALLER_IS_SYNCADAPTER, false);

        final int match = sUriMatcher.match(uri);
        long id = 0;

        switch (match) {
            case SYNCSTATE:
                id = mDbHelper.getSyncState().insert(mDb, values);
                break;

            case CONTACTS: {
                insertContact(values);
                break;
            }

            case RAW_CONTACTS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertRawContact(values, account);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case RAW_CONTACTS_DATA: {
                values.put(Data.RAW_CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values, callerIsSyncAdapter);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case DATA: {
                id = insertData(values, callerIsSyncAdapter);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case GROUPS: {
                final Account account = readAccountFromQueryParams(uri);
                id = insertGroup(uri, values, account, callerIsSyncAdapter);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case SETTINGS: {
                id = insertSettings(uri, values);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case STATUS_UPDATES: {
                id = insertStatusUpdate(values);
                break;
            }

            default:
                mSyncToNetwork = true;
                return mLegacyApiSupport.insert(uri, values);
        }

        if (id < 0) {
            return null;
        }

        return ContentUris.withAppendedId(uri, id);
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
            values.put(RawContacts.ACCOUNT_NAME, account.name);
            values.put(RawContacts.ACCOUNT_TYPE, account.type);
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
        throw new UnsupportedOperationException("Aggregate contacts are created automatically");
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @param account the account this contact should be associated with. may be null.
     * @return the row ID of the newly created row
     */
    private long insertRawContact(ContentValues values, Account account) {
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

        long rawContactId =
                mDb.insert(Tables.RAW_CONTACTS, RawContacts.CONTACT_ID, overriddenValues);
        mContactAggregator.markNewForAggregation(rawContactId);

        // Trigger creation of a Contact based on this RawContact at the end of transaction
        mInsertedRawContacts.add(rawContactId);
        return rawContactId;
    }

    /**
     * Inserts an item in the data table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertData(ContentValues values, boolean callerIsSyncAdapter) {
        long id = 0;
        mValues.clear();
        mValues.putAll(values);

        long rawContactId = mValues.getAsLong(Data.RAW_CONTACT_ID);

        // Replace package with internal mapping
        final String packageName = mValues.getAsString(Data.RES_PACKAGE);
        if (packageName != null) {
            mValues.put(DataColumns.PACKAGE_ID, mDbHelper.getPackageId(packageName));
        }
        mValues.remove(Data.RES_PACKAGE);

        // Replace mimetype with internal mapping
        final String mimeType = mValues.getAsString(Data.MIMETYPE);
        if (TextUtils.isEmpty(mimeType)) {
            throw new IllegalArgumentException(Data.MIMETYPE + " is required");
        }

        mValues.put(DataColumns.MIMETYPE_ID, mDbHelper.getMimeTypeId(mimeType));
        mValues.remove(Data.MIMETYPE);

        DataRowHandler rowHandler = getDataRowHandler(mimeType);
        id = rowHandler.insert(mDb, rawContactId, mValues);
        if (!callerIsSyncAdapter) {
            setRawContactDirty(rawContactId);
        }
        mUpdatedRawContacts.add(rawContactId);

        if (rowHandler.isAggregationRequired()) {
            triggerAggregation(rawContactId);
        }
        return id;
    }

    private void triggerAggregation(long rawContactId) {
        if (!mContactAggregator.isEnabled()) {
            return;
        }

        int aggregationMode = mDbHelper.getAggregationMode(rawContactId);
        switch (aggregationMode) {
            case RawContacts.AGGREGATION_MODE_DISABLED:
                break;

            case RawContacts.AGGREGATION_MODE_DEFAULT: {
                mContactAggregator.markForAggregation(rawContactId);
                break;
            }

            case RawContacts.AGGREGATION_MODE_SUSPENDED: {
                long contactId = mDbHelper.getContactId(rawContactId);

                if (contactId != 0) {
                    mContactAggregator.updateAggregateData(contactId);
                }
                break;
            }

            case RawContacts.AGGREGATION_MODE_IMMEDIATE: {
                long contactId = mDbHelper.getContactId(rawContactId);
                mContactAggregator.aggregateContact(mDb, rawContactId, contactId);
                break;
            }
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
                new String[]{sourceId, account.name, account.type}, null, null, null);
        try {
            if (c.moveToNext()) {
                return c.getLong(0);
            } else {
                ContentValues groupValues = new ContentValues();
                groupValues.put(Groups.ACCOUNT_NAME, account.name);
                groupValues.put(Groups.ACCOUNT_TYPE, account.type);
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
    private int deleteData(String selection, String[] selectionArgs, boolean callerIsSyncAdapter) {
        int count = 0;

        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about deleting data we don't have permission to read.
        Cursor c = query(Data.CONTENT_URI, DataDeleteQuery.COLUMNS, selection, selectionArgs, null);
        try {
            while(c.moveToNext()) {
                long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
                String mimeType = c.getString(DataDeleteQuery.MIMETYPE);
                DataRowHandler rowHandler = getDataRowHandler(mimeType);
                count += rowHandler.delete(mDb, c);
                if (!callerIsSyncAdapter) {
                    setRawContactDirty(rawContactId);
                    if (rowHandler.isAggregationRequired()) {
                        triggerAggregation(rawContactId);
                    }
                }
            }
        } finally {
            c.close();
        }

        return count;
    }

    /**
     * Delete a data row provided that it is one of the allowed mime types.
     */
    public int deleteData(long dataId, String[] allowedMimeTypes) {

        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about deleting data we don't have permission to read.
        Cursor c = query(Data.CONTENT_URI, DataDeleteQuery.COLUMNS, Data._ID + "=" + dataId, null,
                null);

        try {
            if (!c.moveToFirst()) {
                return 0;
            }

            String mimeType = c.getString(DataDeleteQuery.MIMETYPE);
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

            DataRowHandler rowHandler = getDataRowHandler(mimeType);
            int count = rowHandler.delete(mDb, c);
            long rawContactId = c.getLong(DataDeleteQuery.RAW_CONTACT_ID);
            if (rowHandler.isAggregationRequired()) {
                triggerAggregation(rawContactId);
            }
            return count;
        } finally {
            c.close();
        }
    }

    /**
     * Inserts an item in the groups table
     */
    private long insertGroup(Uri uri, ContentValues values, Account account,
            boolean callerIsSyncAdapter) {
        ContentValues overriddenValues = new ContentValues(values);
        if (!resolveAccount(overriddenValues, account)) {
            return -1;
        }

        // Replace package with internal mapping
        final String packageName = overriddenValues.getAsString(Groups.RES_PACKAGE);
        if (packageName != null) {
            overriddenValues.put(GroupsColumns.PACKAGE_ID, mDbHelper.getPackageId(packageName));
        }
        overriddenValues.remove(Groups.RES_PACKAGE);

        if (!callerIsSyncAdapter) {
            overriddenValues.put(Groups.DIRTY, 1);
        }

        long result = mDb.insert(Tables.GROUPS, Groups.TITLE, overriddenValues);

        if (overriddenValues.containsKey(Groups.GROUP_VISIBLE)) {
            mVisibleTouched = true;
        }

        return result;
    }

    private long insertSettings(Uri uri, ContentValues values) {
        final long id = mDb.insert(Tables.SETTINGS, null, values);

        if (values.containsKey(Settings.UNGROUPED_VISIBLE)) {
            mVisibleTouched = true;
        }

        return id;
    }

    /**
     * Inserts a status update.
     */
    public long insertStatusUpdate(ContentValues values) {
        final String handle = values.getAsString(StatusUpdates.IM_HANDLE);
        final Integer protocol = values.getAsInteger(StatusUpdates.PROTOCOL);
        String customProtocol = null;

        if (protocol != null && protocol == Im.PROTOCOL_CUSTOM) {
            customProtocol = values.getAsString(StatusUpdates.CUSTOM_PROTOCOL);
            if (TextUtils.isEmpty(customProtocol)) {
                throw new IllegalArgumentException(
                        "CUSTOM_PROTOCOL is required when PROTOCOL=PROTOCOL_CUSTOM");
            }
        }

        long rawContactId = -1;
        long contactId = -1;
        Long dataId = values.getAsLong(StatusUpdates.DATA_ID);
        mSb.setLength(0);
        if (dataId != null) {
            // Lookup the contact info for the given data row.

            mSb.append(Tables.DATA + "." + Data._ID + "=");
            mSb.append(dataId);
        } else {
            // Lookup the data row to attach this presence update to

            if (TextUtils.isEmpty(handle) || protocol == null) {
                throw new IllegalArgumentException("PROTOCOL and IM_HANDLE are required");
            }

            // TODO: generalize to allow other providers to match against email
            boolean matchEmail = Im.PROTOCOL_GOOGLE_TALK == protocol;

            if (matchEmail) {

                // The following hack forces SQLite to use the (mimetype_id,data1) index, otherwise
                // the "OR" conjunction confuses it and it switches to a full scan of
                // the raw_contacts table.

                // This code relies on the fact that Im.DATA and Email.DATA are in fact the same
                // column - Data.DATA1
                mSb.append(DataColumns.MIMETYPE_ID + " IN (")
                        .append(mMimeTypeIdEmail)
                        .append(",")
                        .append(mMimeTypeIdIm)
                        .append(")" + " AND " + Data.DATA1 + "=");
                DatabaseUtils.appendEscapedSQLString(mSb, handle);
                mSb.append(" AND ((" + DataColumns.MIMETYPE_ID + "=")
                        .append(mMimeTypeIdIm)
                        .append(" AND " + Im.PROTOCOL + "=")
                        .append(protocol);
                if (customProtocol != null) {
                    mSb.append(" AND " + Im.CUSTOM_PROTOCOL + "=");
                    DatabaseUtils.appendEscapedSQLString(mSb, customProtocol);
                }
                mSb.append(") OR (" + DataColumns.MIMETYPE_ID + "=")
                        .append(mMimeTypeIdEmail)
                        .append("))");
            } else {
                mSb.append(DataColumns.MIMETYPE_ID + "=")
                        .append(mMimeTypeIdIm)
                        .append(" AND " + Im.PROTOCOL + "=")
                        .append(protocol)
                        .append(" AND " + Im.DATA + "=");
                DatabaseUtils.appendEscapedSQLString(mSb, handle);
                if (customProtocol != null) {
                    mSb.append(" AND " + Im.CUSTOM_PROTOCOL + "=");
                    DatabaseUtils.appendEscapedSQLString(mSb, customProtocol);
                }
            }

            if (values.containsKey(StatusUpdates.DATA_ID)) {
                mSb.append(" AND " + DataColumns.CONCRETE_ID + "=")
                        .append(values.getAsLong(StatusUpdates.DATA_ID));
            }
        }
        mSb.append(" AND ").append(getContactsRestrictions());

        Cursor cursor = null;
        try {
            cursor = mDb.query(DataContactsQuery.TABLE, DataContactsQuery.PROJECTION,
                    mSb.toString(), null, null, null,
                    Contacts.IN_VISIBLE_GROUP + " DESC, " + Data.RAW_CONTACT_ID);
            if (cursor.moveToFirst()) {
                dataId = cursor.getLong(DataContactsQuery.DATA_ID);
                rawContactId = cursor.getLong(DataContactsQuery.RAW_CONTACT_ID);
                contactId = cursor.getLong(DataContactsQuery.CONTACT_ID);
            } else {
                // No contact found, return a null URI
                return -1;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (values.containsKey(StatusUpdates.PRESENCE)) {
            if (customProtocol == null) {
                // We cannot allow a null in the custom protocol field, because SQLite3 does not
                // properly enforce uniqueness of null values
                customProtocol = "";
            }

            mValues.clear();
            mValues.put(StatusUpdates.DATA_ID, dataId);
            mValues.put(PresenceColumns.RAW_CONTACT_ID, rawContactId);
            mValues.put(PresenceColumns.CONTACT_ID, contactId);
            mValues.put(StatusUpdates.PROTOCOL, protocol);
            mValues.put(StatusUpdates.CUSTOM_PROTOCOL, customProtocol);
            mValues.put(StatusUpdates.IM_HANDLE, handle);
            if (values.containsKey(StatusUpdates.IM_ACCOUNT)) {
                mValues.put(StatusUpdates.IM_ACCOUNT, values.getAsString(StatusUpdates.IM_ACCOUNT));
            }
            mValues.put(StatusUpdates.PRESENCE,
                    values.getAsString(StatusUpdates.PRESENCE));

            // Insert the presence update
            mDb.replace(Tables.PRESENCE, null, mValues);
        }


        if (values.containsKey(StatusUpdates.STATUS)) {
            String status = values.getAsString(StatusUpdates.STATUS);
            String resPackage = values.getAsString(StatusUpdates.STATUS_RES_PACKAGE);
            Integer labelResource = values.getAsInteger(StatusUpdates.STATUS_LABEL);

            if (TextUtils.isEmpty(resPackage)
                    && (labelResource == null || labelResource == 0)
                    && protocol != null) {
                labelResource = Im.getProtocolLabelResource(protocol);
            }

            Long iconResource = values.getAsLong(StatusUpdates.STATUS_ICON);
            // TODO compute the default icon based on the protocol

            if (TextUtils.isEmpty(status)) {
                mStatusUpdateDelete.bindLong(1, dataId);
                mStatusUpdateDelete.execute();
            } else if (values.containsKey(StatusUpdates.STATUS_TIMESTAMP)) {
                long timestamp = values.getAsLong(StatusUpdates.STATUS_TIMESTAMP);
                mStatusUpdateReplace.bindLong(1, dataId);
                mStatusUpdateReplace.bindLong(2, timestamp);
                DatabaseUtils.bindObjectToProgram(mStatusUpdateReplace, 3, status);
                DatabaseUtils.bindObjectToProgram(mStatusUpdateReplace, 4, resPackage);
                DatabaseUtils.bindObjectToProgram(mStatusUpdateReplace, 5, iconResource);
                DatabaseUtils.bindObjectToProgram(mStatusUpdateReplace, 6, labelResource);
                mStatusUpdateReplace.execute();
            } else {

                try {
                    mStatusUpdateInsert.bindLong(1, dataId);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateInsert, 2, status);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateInsert, 3, resPackage);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateInsert, 4, iconResource);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateInsert, 5, labelResource);
                    mStatusUpdateInsert.executeInsert();
                } catch (SQLiteConstraintException e) {
                    // The row already exists - update it
                    long timestamp = System.currentTimeMillis();
                    mStatusUpdateAutoTimestamp.bindLong(1, timestamp);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateAutoTimestamp, 2, status);
                    mStatusUpdateAutoTimestamp.bindLong(3, dataId);
                    DatabaseUtils.bindObjectToProgram(mStatusUpdateAutoTimestamp, 4, status);
                    mStatusUpdateAutoTimestamp.execute();

                    DatabaseUtils.bindObjectToProgram(mStatusAttributionUpdate, 1, resPackage);
                    DatabaseUtils.bindObjectToProgram(mStatusAttributionUpdate, 2, iconResource);
                    DatabaseUtils.bindObjectToProgram(mStatusAttributionUpdate, 3, labelResource);
                    mStatusAttributionUpdate.bindLong(4, dataId);
                    mStatusAttributionUpdate.execute();
                }
            }
        }

        if (contactId != -1) {
            mLastStatusUpdate.bindLong(1, contactId);
            mLastStatusUpdate.bindLong(2, contactId);
            mLastStatusUpdate.execute();
        }

        return dataId;
    }

    @Override
    protected int deleteInTransaction(Uri uri, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "deleteInTransaction: " + uri);
        }
        flushTransactionalChanges();
        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, ContactsContract.CALLER_IS_SYNCADAPTER, false);
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().delete(mDb, selection, selectionArgs);

            case SYNCSTATE_ID:
                String selectionWithId =
                        (SyncStateContract.Columns._ID + "=" + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND (" + selection + ")");
                return mDbHelper.getSyncState().delete(mDb, selectionWithId, selectionArgs);

            case CONTACTS: {
                // TODO
                return 0;
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                return deleteContact(contactId);
            }

            case CONTACTS_LOOKUP:
            case CONTACTS_LOOKUP_ID: {
                final List<String> pathSegments = uri.getPathSegments();
                final int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException("URI " + uri + " is missing a lookup key");
                }
                final String lookupKey = pathSegments.get(2);
                final long contactId = lookupContactIdByLookupKey(mDb, lookupKey);
                return deleteContact(contactId);
            }

            case RAW_CONTACTS: {
                int numDeletes = 0;
                Cursor c = mDb.query(Tables.RAW_CONTACTS, new String[]{RawContacts._ID},
                        appendAccountToSelection(uri, selection), selectionArgs, null, null, null);
                try {
                    while (c.moveToNext()) {
                        final long rawContactId = c.getLong(0);
                        numDeletes += deleteRawContact(rawContactId, callerIsSyncAdapter);
                    }
                } finally {
                    c.close();
                }
                return numDeletes;
            }

            case RAW_CONTACTS_ID: {
                final long rawContactId = ContentUris.parseId(uri);
                return deleteRawContact(rawContactId, callerIsSyncAdapter);
            }

            case DATA: {
                mSyncToNetwork |= !callerIsSyncAdapter;
                return deleteData(appendAccountToSelection(uri, selection), selectionArgs,
                        callerIsSyncAdapter);
            }

            case DATA_ID:
            case PHONES_ID:
            case EMAILS_ID:
            case POSTALS_ID: {
                long dataId = ContentUris.parseId(uri);
                mSyncToNetwork |= !callerIsSyncAdapter;
                return deleteData(Data._ID + "=" + dataId, null, callerIsSyncAdapter);
            }

            case GROUPS_ID: {
                mSyncToNetwork |= !callerIsSyncAdapter;
                return deleteGroup(uri, ContentUris.parseId(uri), callerIsSyncAdapter);
            }

            case GROUPS: {
                int numDeletes = 0;
                Cursor c = mDb.query(Tables.GROUPS, new String[]{Groups._ID},
                        appendAccountToSelection(uri, selection), selectionArgs, null, null, null);
                try {
                    while (c.moveToNext()) {
                        numDeletes += deleteGroup(uri, c.getLong(0), callerIsSyncAdapter);
                    }
                } finally {
                    c.close();
                }
                if (numDeletes > 0) {
                    mSyncToNetwork |= !callerIsSyncAdapter;
                }
                return numDeletes;
            }

            case SETTINGS: {
                mSyncToNetwork |= !callerIsSyncAdapter;
                return deleteSettings(uri, selection, selectionArgs);
            }

            case STATUS_UPDATES: {
                return deleteStatusUpdates(selection, selectionArgs);
            }

            default: {
                mSyncToNetwork = true;
                return mLegacyApiSupport.delete(uri, selection, selectionArgs);
            }
        }
    }

    private static boolean readBooleanQueryParameter(Uri uri, String name, boolean defaultValue) {
        final String flag = uri.getQueryParameter(name);
        return flag == null
                ? defaultValue
                : (!"false".equals(flag.toLowerCase()) && !"0".equals(flag.toLowerCase()));
    }

    public int deleteGroup(Uri uri, long groupId, boolean callerIsSyncAdapter) {
        final long groupMembershipMimetypeId = mDbHelper
                .getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        mDb.delete(Tables.DATA, DataColumns.MIMETYPE_ID + "="
                + groupMembershipMimetypeId + " AND " + GroupMembership.GROUP_ROW_ID + "="
                + groupId, null);

        try {
            if (callerIsSyncAdapter) {
                return mDb.delete(Tables.GROUPS, Groups._ID + "=" + groupId, null);
            } else {
                mValues.clear();
                mValues.put(Groups.DELETED, 1);
                mValues.put(Groups.DIRTY, 1);
                return mDb.update(Tables.GROUPS, mValues, Groups._ID + "=" + groupId, null);
            }
        } finally {
            mVisibleTouched = true;
        }
    }

    private int deleteSettings(Uri uri, String selection, String[] selectionArgs) {
        final int count = mDb.delete(Tables.SETTINGS, selection, selectionArgs);
        mVisibleTouched = true;
        return count;
    }

    private int deleteContact(long contactId) {
        Cursor c = mDb.query(Tables.RAW_CONTACTS, new String[]{RawContacts._ID},
                RawContacts.CONTACT_ID + "=" + contactId, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(0);
                markRawContactAsDeleted(rawContactId);
            }
        } finally {
            c.close();
        }

        return mDb.delete(Tables.CONTACTS, Contacts._ID + "=" + contactId, null);
    }

    public int deleteRawContact(long rawContactId, boolean callerIsSyncAdapter) {
        if (callerIsSyncAdapter) {
            mDb.delete(Tables.PRESENCE, PresenceColumns.RAW_CONTACT_ID + "=" + rawContactId, null);
            return mDb.delete(Tables.RAW_CONTACTS, RawContacts._ID + "=" + rawContactId, null);
        } else {
            mDbHelper.removeContactIfSingleton(rawContactId);
            return markRawContactAsDeleted(rawContactId);
        }
    }

    private int deleteStatusUpdates(String selection, String[] selectionArgs) {
        // TODO delete from both tables: presence and status_updates
        return mDb.delete(Tables.PRESENCE, selection, selectionArgs);
    }

    private int markRawContactAsDeleted(long rawContactId) {
        mSyncToNetwork = true;

        mValues.clear();
        mValues.put(RawContacts.DELETED, 1);
        mValues.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DISABLED);
        mValues.put(RawContactsColumns.AGGREGATION_NEEDED, 1);
        mValues.putNull(RawContacts.CONTACT_ID);
        mValues.put(RawContacts.DIRTY, 1);
        return updateRawContact(rawContactId, mValues);
    }

    @Override
    protected int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "updateInTransaction: " + uri);
        }

        int count = 0;

        final int match = sUriMatcher.match(uri);
        if (match == SYNCSTATE_ID && selection == null) {
            long rowId = ContentUris.parseId(uri);
            Object data = values.get(ContactsContract.SyncStateColumns.DATA);
            mUpdatedSyncStates.put(rowId, data);
            return 1;
        }
        flushTransactionalChanges();
        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, ContactsContract.CALLER_IS_SYNCADAPTER, false);
        switch(match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().update(mDb, values,
                        appendAccountToSelection(uri, selection), selectionArgs);

            case SYNCSTATE_ID: {
                selection = appendAccountToSelection(uri, selection);
                String selectionWithId =
                        (SyncStateContract.Columns._ID + "=" + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND (" + selection + ")");
                return mDbHelper.getSyncState().update(mDb, values,
                        selectionWithId, selectionArgs);
            }

            case CONTACTS: {
                count = updateContactOptions(values, selection, selectionArgs);
                break;
            }

            case CONTACTS_ID: {
                count = updateContactOptions(ContentUris.parseId(uri), values);
                break;
            }

            case CONTACTS_LOOKUP:
            case CONTACTS_LOOKUP_ID: {
                final List<String> pathSegments = uri.getPathSegments();
                final int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException("URI " + uri + " is missing a lookup key");
                }
                final String lookupKey = pathSegments.get(2);
                final long contactId = lookupContactIdByLookupKey(mDb, lookupKey);
                count = updateContactOptions(contactId, values);
                break;
            }

            case RAW_CONTACTS_DATA: {
                final String rawContactId = uri.getPathSegments().get(1);
                String selectionWithId = (Data.RAW_CONTACT_ID + "=" + rawContactId + " ")
                    + (selection == null ? "" : " AND " + selection);

                count = updateData(uri, values, selectionWithId, selectionArgs, callerIsSyncAdapter);

                break;
            }

            case DATA: {
                count = updateData(uri, values, appendAccountToSelection(uri, selection),
                        selectionArgs, callerIsSyncAdapter);
                if (count > 0) {
                    mSyncToNetwork |= !callerIsSyncAdapter;
                }
                break;
            }

            case DATA_ID:
            case PHONES_ID:
            case EMAILS_ID:
            case POSTALS_ID: {
                count = updateData(uri, values, selection, selectionArgs, callerIsSyncAdapter);
                if (count > 0) {
                    mSyncToNetwork |= !callerIsSyncAdapter;
                }
                break;
            }

            case RAW_CONTACTS: {
                selection = appendAccountToSelection(uri, selection);
                count = updateRawContacts(values, selection, selectionArgs);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                if (selection != null) {
                    count = updateRawContacts(values, RawContacts._ID + "=" + rawContactId
                                    + " AND(" + selection + ")", selectionArgs);
                } else {
                    count = updateRawContacts(values, RawContacts._ID + "=" + rawContactId, null);
                }
                break;
            }

            case GROUPS: {
                count = updateGroups(uri, values, appendAccountToSelection(uri, selection),
                        selectionArgs, callerIsSyncAdapter);
                if (count > 0) {
                    mSyncToNetwork |= !callerIsSyncAdapter;
                }
                break;
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                String selectionWithId = (Groups._ID + "=" + groupId + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = updateGroups(uri, values, selectionWithId, selectionArgs,
                        callerIsSyncAdapter);
                if (count > 0) {
                    mSyncToNetwork |= !callerIsSyncAdapter;
                }
                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                count = updateAggregationException(mDb, values);
                break;
            }

            case SETTINGS: {
                count = updateSettings(uri, values, selection, selectionArgs);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            default: {
                mSyncToNetwork = true;
                return mLegacyApiSupport.update(uri, values, selection, selectionArgs);
            }
        }

        return count;
    }

    private int updateGroups(Uri uri, ContentValues values, String selectionWithId,
            String[] selectionArgs, boolean callerIsSyncAdapter) {

        ContentValues updatedValues;
        if (!callerIsSyncAdapter && !values.containsKey(Groups.DIRTY)) {
            updatedValues = mValues;
            updatedValues.clear();
            updatedValues.putAll(values);
            updatedValues.put(Groups.DIRTY, 1);
        } else {
            updatedValues = values;
        }

        int count = mDb.update(Tables.GROUPS, updatedValues, selectionWithId, selectionArgs);
        if (updatedValues.containsKey(Groups.GROUP_VISIBLE)) {
            mVisibleTouched = true;
        }
        if (updatedValues.containsKey(Groups.SHOULD_SYNC)
                && updatedValues.getAsInteger(Groups.SHOULD_SYNC) != 0) {
            final long groupId = ContentUris.parseId(uri);
            Cursor c = mDb.query(Tables.GROUPS, new String[]{Groups.ACCOUNT_NAME,
                    Groups.ACCOUNT_TYPE}, Groups._ID + "=" + groupId, null, null,
                    null, null);
            String accountName;
            String accountType;
            try {
                while (c.moveToNext()) {
                    accountName = c.getString(0);
                    accountType = c.getString(1);
                    if(!TextUtils.isEmpty(accountName) && !TextUtils.isEmpty(accountType)) {
                        Account account = new Account(accountName, accountType);
                    ContentResolver.requestSync(account, ContactsContract.AUTHORITY,
                                new Bundle());
                        break;
                    }
                }
            } finally {
                c.close();
            }
        }
        return count;
    }

    private int updateSettings(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        final int count = mDb.update(Tables.SETTINGS, values, selection, selectionArgs);
        if (values.containsKey(Settings.UNGROUPED_VISIBLE)) {
            mVisibleTouched = true;
        }
        return count;
    }

    private int updateRawContacts(ContentValues values, String selection, String[] selectionArgs) {
        if (values.containsKey(RawContacts.CONTACT_ID)) {
            throw new IllegalArgumentException(RawContacts.CONTACT_ID + " should not be included " +
                    "in content values. Contact IDs are assigned automatically");
        }

        int count = 0;
        Cursor cursor = mDb.query(mDbHelper.getRawContactView(),
                new String[] { RawContacts._ID }, selection,
                selectionArgs, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long rawContactId = cursor.getLong(0);
                updateRawContact(rawContactId, values);
                count++;
            }
        } finally {
            cursor.close();
        }

        return count;
    }

    private int updateRawContact(long rawContactId, ContentValues values) {
        final String selection = RawContacts._ID + " = " + rawContactId;
        final boolean requestUndoDelete = (values.containsKey(RawContacts.DELETED)
                && values.getAsInteger(RawContacts.DELETED) == 0);
        int previousDeleted = 0;
        if (requestUndoDelete) {
            Cursor cursor = mDb.query(RawContactsQuery.TABLE, RawContactsQuery.COLUMNS, selection,
                    null, null, null, null);
            try {
                if (cursor.moveToFirst()) {
                    previousDeleted = cursor.getInt(RawContactsQuery.DELETED);
                }
            } finally {
                cursor.close();
            }
            values.put(ContactsContract.RawContacts.AGGREGATION_MODE,
                    ContactsContract.RawContacts.AGGREGATION_MODE_DEFAULT);
        }
        int count = mDb.update(Tables.RAW_CONTACTS, values, selection, null);
        if (count != 0) {
            if (values.containsKey(RawContacts.STARRED)) {
                mContactAggregator.updateStarred(rawContactId);
            }
            if (values.containsKey(RawContacts.SOURCE_ID)) {
                mContactAggregator.updateLookupKey(mDb, rawContactId);
            }
            if (requestUndoDelete && previousDeleted == 1) {
                // undo delete, needs aggregation again.
                mInsertedRawContacts.add(rawContactId);
            }
        }
        return count;
    }

    private int updateData(Uri uri, ContentValues values, String selection,
            String[] selectionArgs, boolean callerIsSyncAdapter) {
        mValues.clear();
        mValues.putAll(values);
        mValues.remove(Data._ID);
        mValues.remove(Data.RAW_CONTACT_ID);
        mValues.remove(Data.MIMETYPE);

        String packageName = values.getAsString(Data.RES_PACKAGE);
        if (packageName != null) {
            mValues.remove(Data.RES_PACKAGE);
            mValues.put(DataColumns.PACKAGE_ID, mDbHelper.getPackageId(packageName));
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

        int count = 0;

        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about updating data we don't have permission to read.
        Cursor c = query(uri, DataUpdateQuery.COLUMNS, selection, selectionArgs, null);
        try {
            while(c.moveToNext()) {
                count += updateData(mValues, c, callerIsSyncAdapter);
            }
        } finally {
            c.close();
        }

        return count;
    }

    private int updateData(ContentValues values, Cursor c, boolean callerIsSyncAdapter) {
        if (values.size() == 0) {
            return 0;
        }

        final String mimeType = c.getString(DataUpdateQuery.MIMETYPE);
        DataRowHandler rowHandler = getDataRowHandler(mimeType);
        rowHandler.update(mDb, values, c, callerIsSyncAdapter);
        long rawContactId = c.getLong(DataUpdateQuery.RAW_CONTACT_ID);
        if (rowHandler.isAggregationRequired()) {
            triggerAggregation(rawContactId);
        }

        return 1;
    }

    private int updateContactOptions(ContentValues values, String selection,
            String[] selectionArgs) {
        int count = 0;
        Cursor cursor = mDb.query(mDbHelper.getContactView(),
                new String[] { Contacts._ID }, selection,
                selectionArgs, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long contactId = cursor.getLong(0);
                updateContactOptions(contactId, values);
                count++;
            }
        } finally {
            cursor.close();
        }

        return count;
    }

    private int updateContactOptions(long contactId, ContentValues values) {

        mValues.clear();
        ContactsDatabaseHelper.copyStringValue(mValues, RawContacts.CUSTOM_RINGTONE,
                values, Contacts.CUSTOM_RINGTONE);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.SEND_TO_VOICEMAIL,
                values, Contacts.SEND_TO_VOICEMAIL);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.LAST_TIME_CONTACTED,
                values, Contacts.LAST_TIME_CONTACTED);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.TIMES_CONTACTED,
                values, Contacts.TIMES_CONTACTED);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.STARRED,
                values, Contacts.STARRED);

        // Nothing to update - just return
        if (mValues.size() == 0) {
            return 0;
        }

        if (mValues.containsKey(RawContacts.STARRED)) {
            // Mark dirty when changing starred to trigger sync
            mValues.put(RawContacts.DIRTY, 1);
        }

        mDb.update(Tables.RAW_CONTACTS, mValues, RawContacts.CONTACT_ID + "=" + contactId, null);

        // Copy changeable values to prevent automatically managed fields from
        // being explicitly updated by clients.
        mValues.clear();
        ContactsDatabaseHelper.copyStringValue(mValues, RawContacts.CUSTOM_RINGTONE,
                values, Contacts.CUSTOM_RINGTONE);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.SEND_TO_VOICEMAIL,
                values, Contacts.SEND_TO_VOICEMAIL);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.LAST_TIME_CONTACTED,
                values, Contacts.LAST_TIME_CONTACTED);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.TIMES_CONTACTED,
                values, Contacts.TIMES_CONTACTED);
        ContactsDatabaseHelper.copyLongValue(mValues, RawContacts.STARRED,
                values, Contacts.STARRED);

        return mDb.update(Tables.CONTACTS, mValues, Contacts._ID + "=" + contactId, null);
    }

    public void updateContactLastContactedTime(long contactId, long lastTimeContacted) {
        mContactsLastTimeContactedUpdate.bindLong(1, lastTimeContacted);
        mContactsLastTimeContactedUpdate.bindLong(2, contactId);
        mContactsLastTimeContactedUpdate.execute();
    }

    private int updateAggregationException(SQLiteDatabase db, ContentValues values) {
        int exceptionType = values.getAsInteger(AggregationExceptions.TYPE);
        long rcId1 = values.getAsInteger(AggregationExceptions.RAW_CONTACT_ID1);
        long rcId2 = values.getAsInteger(AggregationExceptions.RAW_CONTACT_ID2);

        long rawContactId1, rawContactId2;
        if (rcId1 < rcId2) {
            rawContactId1 = rcId1;
            rawContactId2 = rcId2;
        } else {
            rawContactId2 = rcId1;
            rawContactId1 = rcId2;
        }

        if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC) {
            db.delete(Tables.AGGREGATION_EXCEPTIONS,
                    AggregationExceptions.RAW_CONTACT_ID1 + "=" + rawContactId1 + " AND "
                    + AggregationExceptions.RAW_CONTACT_ID2 + "=" + rawContactId2, null);
        } else {
            ContentValues exceptionValues = new ContentValues(3);
            exceptionValues.put(AggregationExceptions.TYPE, exceptionType);
            exceptionValues.put(AggregationExceptions.RAW_CONTACT_ID1, rawContactId1);
            exceptionValues.put(AggregationExceptions.RAW_CONTACT_ID2, rawContactId2);
            db.replace(Tables.AGGREGATION_EXCEPTIONS, AggregationExceptions._ID,
                    exceptionValues);
        }

        mContactAggregator.markForAggregation(rawContactId1);
        mContactAggregator.markForAggregation(rawContactId2);

        long contactId1 = mDbHelper.getContactId(rawContactId1);
        mContactAggregator.aggregateContact(db, rawContactId1, contactId1);

        long contactId2 = mDbHelper.getContactId(rawContactId2);
        mContactAggregator.aggregateContact(db, rawContactId2, contactId2);

        // The return value is fake - we just confirm that we made a change, not count actual
        // rows changed.
        return 1;
    }

    public void onAccountsUpdated(Account[] accounts) {
        mDb = mDbHelper.getWritableDatabase();
        if (mDb == null) return;

        HashSet<Account> existingAccounts = new HashSet<Account>();
        boolean hasUnassignedContacts[] = new boolean[]{false};
        mDb.beginTransaction();
        try {
            findValidAccounts(existingAccounts, hasUnassignedContacts,
                    Tables.RAW_CONTACTS, RawContacts.ACCOUNT_NAME, RawContacts.ACCOUNT_TYPE);
            findValidAccounts(existingAccounts, hasUnassignedContacts,
                    Tables.GROUPS, Groups.ACCOUNT_NAME, Groups.ACCOUNT_TYPE);
            findValidAccounts(existingAccounts, hasUnassignedContacts,
                    Tables.SETTINGS, Settings.ACCOUNT_NAME, Settings.ACCOUNT_TYPE);

            // Remove all valid accounts from the existing account set. What is left
            // in the existingAccounts set will be extra accounts whose data must be deleted.
            HashSet<Account> accountsToDelete = new HashSet<Account>(existingAccounts);
            for (Account account : accounts) {
                accountsToDelete.remove(account);
            }

            for (Account account : accountsToDelete) {
                Log.d(TAG, "removing data for removed account " + account);
                String[] params = new String[] {account.name, account.type};
                mDb.execSQL(
                        "DELETE FROM " + Tables.GROUPS +
                        " WHERE " + Groups.ACCOUNT_NAME + " = ?" +
                                " AND " + Groups.ACCOUNT_TYPE + " = ?", params);
                mDb.execSQL(
                        "DELETE FROM " + Tables.PRESENCE +
                        " WHERE " + PresenceColumns.RAW_CONTACT_ID + " IN (" +
                                "SELECT " + RawContacts._ID +
                                " FROM " + Tables.RAW_CONTACTS +
                                " WHERE " + RawContacts.ACCOUNT_NAME + " = ?" +
                                " AND " + RawContacts.ACCOUNT_TYPE + " = ?)", params);
                mDb.execSQL(
                        "DELETE FROM " + Tables.RAW_CONTACTS +
                        " WHERE " + RawContacts.ACCOUNT_NAME + " = ?" +
                        " AND " + RawContacts.ACCOUNT_TYPE + " = ?", params);
                mDb.execSQL(
                        "DELETE FROM " + Tables.SETTINGS +
                        " WHERE " + Settings.ACCOUNT_NAME + " = ?" +
                        " AND " + Settings.ACCOUNT_TYPE + " = ?", params);
            }

            if (hasUnassignedContacts[0]) {

                Account primaryAccount = null;
                for (Account account : accounts) {
                    if (isWritableAccount(account)) {
                        primaryAccount = account;
                        break;
                    }
                }

                if (primaryAccount != null) {
                    String[] params = new String[] {primaryAccount.name, primaryAccount.type};

                    mDb.execSQL(
                            "UPDATE " + Tables.RAW_CONTACTS +
                            " SET " + RawContacts.ACCOUNT_NAME + "=?,"
                                    + RawContacts.ACCOUNT_TYPE + "=?" +
                            " WHERE " + RawContacts.ACCOUNT_NAME + " IS NULL" +
                            " AND " + RawContacts.ACCOUNT_TYPE + " IS NULL", params);

                    // We don't currently support groups for unsynced accounts, so this is for
                    // the future
                    mDb.execSQL(
                            "UPDATE " + Tables.GROUPS +
                            " SET " + Groups.ACCOUNT_NAME + "=?,"
                                    + Groups.ACCOUNT_TYPE + "=?" +
                            " WHERE " + Groups.ACCOUNT_NAME + " IS NULL" +
                            " AND " + Groups.ACCOUNT_TYPE + " IS NULL", params);
                }
            }

            mDbHelper.getSyncState().onAccountsChanged(mDb, accounts);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
    }

    /**
     * Finds all distinct accounts present in the specified table.
     */
    private void findValidAccounts(Set<Account> validAccounts, boolean[] hasUnassignedContacts,
            String table, String accountNameColumn, String accountTypeColumn) {
        Cursor c = mDb.rawQuery("SELECT DISTINCT " + accountNameColumn + "," + accountTypeColumn
                + " FROM " + table, null);
        try {
            while (c.moveToNext()) {
                if (c.isNull(0) && c.isNull(1)) {
                    hasUnassignedContacts[0] = true;
                } else {
                    validAccounts.add(new Account(c.getString(0), c.getString(1)));
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Test all against {@link TextUtils#isEmpty(CharSequence)}.
     */
    private static boolean areAllEmpty(ContentValues values, String[] keys) {
        for (String key : keys) {
            if (!TextUtils.isEmpty(values.getAsString(key))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if a value (possibly null) is specified for at least one of the supplied keys.
     */
    private static boolean areAnySpecified(ContentValues values, String[] keys) {
        for (String key : keys) {
            if (values.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: " + uri);
        }

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;
        String limit = getLimit(uri);

        // TODO: Consider writing a test case for RestrictionExceptions when you
        // write a new query() block to make sure it protects restricted data.
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().query(db, projection, selection,  selectionArgs,
                        sortOrder);

            case CONTACTS: {
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                break;
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                qb.appendWhere(Contacts._ID + "=" + contactId);
                break;
            }

            case CONTACTS_LOOKUP:
            case CONTACTS_LOOKUP_ID: {
                List<String> pathSegments = uri.getPathSegments();
                int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException("URI " + uri + " is missing a lookup key");
                }
                String lookupKey = pathSegments.get(2);
                if (segmentCount == 4) {
                    long contactId = Long.parseLong(pathSegments.get(3));
                    SQLiteQueryBuilder lookupQb = new SQLiteQueryBuilder();
                    setTablesAndProjectionMapForContacts(lookupQb, uri, projection);
                    lookupQb.appendWhere(Contacts._ID + "=" + contactId + " AND " +
                            Contacts.LOOKUP_KEY + "=");
                    lookupQb.appendWhereEscapeString(lookupKey);
                    Cursor c = query(db, lookupQb, projection, selection, selectionArgs, sortOrder,
                            groupBy, limit);
                    if (c.getCount() != 0) {
                        return c;
                    }

                    c.close();
                }

                setTablesAndProjectionMapForContacts(qb, uri, projection);
                qb.appendWhere(Contacts._ID + "=" + lookupContactIdByLookupKey(db, lookupKey));
                break;
            }

            case CONTACTS_AS_VCARD: {
                // When reading as vCard always use restricted view
                final String lookupKey = uri.getPathSegments().get(2);
                qb.setTables(mDbHelper.getContactView(true /* require restricted */));
                qb.setProjectionMap(sContactsVCardProjectionMap);
                qb.appendWhere(Contacts._ID + "=" + lookupContactIdByLookupKey(db, lookupKey));
                break;
            }

            case CONTACTS_FILTER: {
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append(Contacts._ID + " IN ");
                    appendContactFilterAsNestedQuery(sb, filterParam);
                    qb.appendWhere(sb.toString());
                }
                break;
            }

            case CONTACTS_STREQUENT_FILTER:
            case CONTACTS_STREQUENT: {
                String filterSql = null;
                if (match == CONTACTS_STREQUENT_FILTER
                        && uri.getPathSegments().size() > 3) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append(Contacts._ID + " IN ");
                    appendContactFilterAsNestedQuery(sb, filterParam);
                    filterSql = sb.toString();
                }

                setTablesAndProjectionMapForContacts(qb, uri, projection);

                String[] starredProjection = null;
                String[] frequentProjection = null;
                if (projection != null) {
                    starredProjection = appendProjectionArg(projection, TIMES_CONTACED_SORT_COLUMN);
                    frequentProjection = appendProjectionArg(projection, TIMES_CONTACED_SORT_COLUMN);
                }

                // Build the first query for starred
                if (filterSql != null) {
                    qb.appendWhere(filterSql);
                }
                qb.setProjectionMap(sStrequentStarredProjectionMap);
                final String starredQuery = qb.buildQuery(starredProjection, Contacts.STARRED + "=1",
                        null, Contacts._ID, null, null, null);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                if (filterSql != null) {
                    qb.appendWhere(filterSql);
                }
                qb.setProjectionMap(sStrequentFrequentProjectionMap);
                final String frequentQuery = qb.buildQuery(frequentProjection,
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

            case CONTACTS_GROUP: {
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(CONTACTS_IN_GROUP_SELECT);
                    selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                }
                break;
            }

            case CONTACTS_DATA: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=" + contactId);
                break;
            }

            case CONTACTS_PHOTO: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=" + contactId);
                qb.appendWhere(" AND " + Data._ID + "=" + Contacts.PHOTO_ID);
                break;
            }

            case PHONES: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case PHONES_ID: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + Data._ID + "=" + uri.getLastPathSegment());
                break;
            }

            case PHONES_FILTER: {
                setTablesAndProjectionMapForData(qb, uri, projection, true);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append("(");

                    boolean orNeeded = false;
                    String normalizedName = NameNormalizer.normalize(filterParam);
                    if (normalizedName.length() > 0) {
                        sb.append(Data.RAW_CONTACT_ID + " IN ");
                        appendRawContactsByNormalizedNameFilter(sb, normalizedName, null, false);
                        orNeeded = true;
                    }

                    if (isPhoneNumber(filterParam)) {
                        if (orNeeded) {
                            sb.append(" OR ");
                        }
                        String number = PhoneNumberUtils.convertKeypadLettersToDigits(filterParam);
                        String reversed = PhoneNumberUtils.getStrippedReversed(number);
                        sb.append(Data._ID +
                                " IN (SELECT " + PhoneLookupColumns.DATA_ID
                                  + " FROM " + Tables.PHONE_LOOKUP
                                  + " WHERE " + PhoneLookupColumns.NORMALIZED_NUMBER + " LIKE '%");
                        sb.append(reversed);
                        sb.append("')");
                    }
                    sb.append(")");
                    qb.appendWhere(" AND " + sb);
                }
                groupBy = PhoneColumns.NORMALIZED_NUMBER + "," + RawContacts.CONTACT_ID;
                if (sortOrder == null) {
                    sortOrder = Contacts.IN_VISIBLE_GROUP + " DESC, " + RawContacts.CONTACT_ID;
                }
                break;
            }

            case EMAILS: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case EMAILS_ID: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + Data._ID + "=" + uri.getLastPathSegment());
                break;
            }

            case EMAILS_LOOKUP: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    qb.appendWhere(" AND " + Email.DATA + "=");
                    qb.appendWhereEscapeString(uri.getLastPathSegment());
                }
                break;
            }

            case EMAILS_FILTER: {
                setTablesAndProjectionMapForData(qb, uri, projection, true);
                String filterParam = null;
                if (uri.getPathSegments().size() > 3) {
                    filterParam = uri.getLastPathSegment();
                    if (TextUtils.isEmpty(filterParam)) {
                        filterParam = null;
                    }
                }

                if (filterParam == null) {
                    // If the filter is unspecified, return nothing
                    qb.appendWhere(" AND 0");
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append(" AND " + Data._ID + " IN (");
                    sb.append(
                            "SELECT " + Data._ID +
                            " FROM " + Tables.DATA +
                            " WHERE " + DataColumns.MIMETYPE_ID + "=" + mMimeTypeIdEmail +
                            " AND " + Data.DATA1 + " LIKE ");
                    DatabaseUtils.appendEscapedSQLString(sb, filterParam + '%');
                    if (!filterParam.contains("@")) {
                        String normalizedName = NameNormalizer.normalize(filterParam);
                        if (normalizedName.length() > 0) {

                            /*
                             * Using a UNION instead of an "OR" to make SQLite use the right
                             * indexes. We need it to use the (mimetype,data1) index for the
                             * email lookup (see above), but not for the name lookup.
                             * SQLite is not smart enough to use the index on one side of an OR
                             * but not on the other. Using two separate nested queries
                             * and a UNION between them does the job.
                             */
                            sb.append(
                                    " UNION SELECT " + Data._ID +
                                    " FROM " + Tables.DATA +
                                    " WHERE +" + DataColumns.MIMETYPE_ID + "=" + mMimeTypeIdEmail +
                                    " AND " + Data.RAW_CONTACT_ID + " IN ");
                            appendRawContactsByNormalizedNameFilter(sb, normalizedName, null, false);
                        }
                    }
                    sb.append(")");
                    qb.appendWhere(sb);
                }
                groupBy = Email.DATA + "," + RawContacts.CONTACT_ID;
                if (sortOrder == null) {
                    sortOrder = Contacts.IN_VISIBLE_GROUP + " DESC, " + RawContacts.CONTACT_ID;
                }
                break;
            }

            case POSTALS: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '"
                        + StructuredPostal.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case POSTALS_ID: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '"
                        + StructuredPostal.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + Data._ID + "=" + uri.getLastPathSegment());
                break;
            }

            case RAW_CONTACTS: {
                setTablesAndProjectionMapForRawContacts(qb, uri);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                setTablesAndProjectionMapForRawContacts(qb, uri);
                qb.appendWhere(" AND " + RawContacts._ID + "=" + rawContactId);
                break;
            }

            case RAW_CONTACTS_DATA: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.RAW_CONTACT_ID + "=" + rawContactId);
                break;
            }

            case DATA: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                break;
            }

            case DATA_ID: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data._ID + "=" + ContentUris.parseId(uri));
                break;
            }

            case PHONE_LOOKUP: {

                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = RawContactsColumns.CONCRETE_ID;
                }

                String number = uri.getPathSegments().size() > 1 ? uri.getLastPathSegment() : "";
                mDbHelper.buildPhoneLookupAndContactQuery(qb, number);
                qb.setProjectionMap(sPhoneLookupProjectionMap);

                // Phone lookup cannot be combined with a selection
                selection = null;
                selectionArgs = null;
                break;
            }

            case GROUPS: {
                qb.setTables(mDbHelper.getGroupView());
                qb.setProjectionMap(sGroupsProjectionMap);
                appendAccountFromParameter(qb, uri);
                break;
            }

            case GROUPS_ID: {
                long groupId = ContentUris.parseId(uri);
                qb.setTables(mDbHelper.getGroupView());
                qb.setProjectionMap(sGroupsProjectionMap);
                qb.appendWhere(Groups._ID + "=" + groupId);
                break;
            }

            case GROUPS_SUMMARY: {
                qb.setTables(mDbHelper.getGroupView() + " AS groups");
                qb.setProjectionMap(sGroupsSummaryProjectionMap);
                appendAccountFromParameter(qb, uri);
                groupBy = Groups._ID;
                break;
            }

            case AGGREGATION_EXCEPTIONS: {
                qb.setTables(Tables.AGGREGATION_EXCEPTIONS);
                qb.setProjectionMap(sAggregationExceptionsProjectionMap);
                break;
            }

            case AGGREGATION_SUGGESTIONS: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                String filter = null;
                if (uri.getPathSegments().size() > 3) {
                    filter = uri.getPathSegments().get(3);
                }
                final int maxSuggestions;
                if (limit != null) {
                    maxSuggestions = Integer.parseInt(limit);
                } else {
                    maxSuggestions = DEFAULT_MAX_SUGGESTIONS;
                }

                setTablesAndProjectionMapForContacts(qb, uri, projection);

                return mContactAggregator.queryAggregationSuggestions(qb, projection, contactId,
                        maxSuggestions, filter);
            }

            case SETTINGS: {
                qb.setTables(Tables.SETTINGS);
                qb.setProjectionMap(sSettingsProjectionMap);
                appendAccountFromParameter(qb, uri);

                // When requesting specific columns, this query requires
                // late-binding of the GroupMembership MIME-type.
                final String groupMembershipMimetypeId = Long.toString(mDbHelper
                        .getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE));
                if (projection != null && projection.length != 0 &&
                        mDbHelper.isInProjection(projection, Settings.UNGROUPED_COUNT)) {
                    selectionArgs = insertSelectionArg(selectionArgs, groupMembershipMimetypeId);
                }
                if (projection != null && projection.length != 0 &&
                        mDbHelper.isInProjection(projection, Settings.UNGROUPED_WITH_PHONES)) {
                    selectionArgs = insertSelectionArg(selectionArgs, groupMembershipMimetypeId);
                }

                break;
            }

            case STATUS_UPDATES: {
                setTableAndProjectionMapForStatusUpdates(qb, projection);
                break;
            }

            case STATUS_UPDATES_ID: {
                setTableAndProjectionMapForStatusUpdates(qb, projection);
                qb.appendWhere(DataColumns.CONCRETE_ID + "=" + ContentUris.parseId(uri));
                break;
            }

            case SEARCH_SUGGESTIONS: {
                return mGlobalSearchSupport.handleSearchSuggestionsQuery(db, uri, limit);
            }

            case SEARCH_SHORTCUT: {
                long contactId = ContentUris.parseId(uri);
                return mGlobalSearchSupport.handleSearchShortcutRefresh(db, contactId, projection);
            }

            case LIVE_FOLDERS_CONTACTS:
                qb.setTables(mDbHelper.getContactView());
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                break;

            case LIVE_FOLDERS_CONTACTS_WITH_PHONES:
                qb.setTables(mDbHelper.getContactView());
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(Contacts.HAS_PHONE_NUMBER + "=1");
                break;

            case LIVE_FOLDERS_CONTACTS_FAVORITES:
                qb.setTables(mDbHelper.getContactView());
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(Contacts.STARRED + "=1");
                break;

            case LIVE_FOLDERS_CONTACTS_GROUP_NAME:
                qb.setTables(mDbHelper.getContactView());
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(CONTACTS_IN_GROUP_SELECT);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                break;

            case RAW_CONTACT_ENTITIES: {
                setTablesAndProjectionMapForRawContactsEntities(qb, uri);
                break;
            }

            case RAW_CONTACT_ENTITY_ID: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForRawContactsEntities(qb, uri);
                qb.appendWhere(" AND " + RawContacts._ID + "=" + rawContactId);
                break;
            }

            default:
                return mLegacyApiSupport.query(uri, projection, selection, selectionArgs,
                        sortOrder, limit);
        }

        return query(db, qb, projection, selection, selectionArgs, sortOrder, groupBy, limit);
    }

    private Cursor query(final SQLiteDatabase db, SQLiteQueryBuilder qb, String[] projection,
            String selection, String[] selectionArgs, String sortOrder, String groupBy,
            String limit) {
        if (projection != null && projection.length == 1
                && BaseColumns._COUNT.equals(projection[0])) {
            qb.setProjectionMap(sCountProjectionMap);
        }
        final Cursor c = qb.query(db, projection, selection, selectionArgs, groupBy, null,
                sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    private long lookupContactIdByLookupKey(SQLiteDatabase db, String lookupKey) {
        ContactLookupKey key = new ContactLookupKey();
        ArrayList<LookupKeySegment> segments = key.parse(lookupKey);

        long contactId = lookupContactIdBySourceIds(db, segments);
        if (contactId == -1) {
            contactId = lookupContactIdByDisplayNames(db, segments);
        }

        return contactId;
    }

    private interface LookupBySourceIdQuery {
        String TABLE = Tables.RAW_CONTACTS;

        String COLUMNS[] = {
                RawContacts.CONTACT_ID,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.ACCOUNT_NAME,
                RawContacts.SOURCE_ID
        };

        int CONTACT_ID = 0;
        int ACCOUNT_TYPE = 1;
        int ACCOUNT_NAME = 2;
        int SOURCE_ID = 3;
    }

    private long lookupContactIdBySourceIds(SQLiteDatabase db,
                ArrayList<LookupKeySegment> segments) {
        int sourceIdCount = 0;
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.sourceIdLookup) {
                sourceIdCount++;
            }
        }

        if (sourceIdCount == 0) {
            return -1;
        }

        // First try sync ids
        StringBuilder sb = new StringBuilder();
        sb.append(RawContacts.SOURCE_ID + " IN (");
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.sourceIdLookup) {
                DatabaseUtils.appendEscapedSQLString(sb, segment.key);
                sb.append(",");
            }
        }
        sb.setLength(sb.length() - 1);      // Last comma
        sb.append(") AND " + RawContacts.CONTACT_ID + " NOT NULL");

        Cursor c = db.query(LookupBySourceIdQuery.TABLE, LookupBySourceIdQuery.COLUMNS,
                 sb.toString(), null, null, null, null);
        try {
            while (c.moveToNext()) {
                String accountType = c.getString(LookupBySourceIdQuery.ACCOUNT_TYPE);
                String accountName = c.getString(LookupBySourceIdQuery.ACCOUNT_NAME);
                int accountHashCode =
                        ContactLookupKey.getAccountHashCode(accountType, accountName);
                String sourceId = c.getString(LookupBySourceIdQuery.SOURCE_ID);
                for (int i = 0; i < segments.size(); i++) {
                    LookupKeySegment segment = segments.get(i);
                    if (segment.sourceIdLookup && accountHashCode == segment.accountHashCode
                            && segment.key.equals(sourceId)) {
                        segment.contactId = c.getLong(LookupBySourceIdQuery.CONTACT_ID);
                        break;
                    }
                }
            }
        } finally {
            c.close();
        }

        return getMostReferencedContactId(segments);
    }

    private interface LookupByDisplayNameQuery {
        String TABLE = Tables.NAME_LOOKUP_JOIN_RAW_CONTACTS;

        String COLUMNS[] = {
                RawContacts.CONTACT_ID,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.ACCOUNT_NAME,
                NameLookupColumns.NORMALIZED_NAME
        };

        int CONTACT_ID = 0;
        int ACCOUNT_TYPE = 1;
        int ACCOUNT_NAME = 2;
        int NORMALIZED_NAME = 3;
    }

    private long lookupContactIdByDisplayNames(SQLiteDatabase db,
                ArrayList<LookupKeySegment> segments) {
        int displayNameCount = 0;
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (!segment.sourceIdLookup) {
                displayNameCount++;
            }
        }

        if (displayNameCount == 0) {
            return -1;
        }

        // First try sync ids
        StringBuilder sb = new StringBuilder();
        sb.append(NameLookupColumns.NORMALIZED_NAME + " IN (");
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (!segment.sourceIdLookup) {
                DatabaseUtils.appendEscapedSQLString(sb, segment.key);
                sb.append(",");
            }
        }
        sb.setLength(sb.length() - 1);      // Last comma
        sb.append(") AND " + NameLookupColumns.NAME_TYPE + "=" + NameLookupType.NAME_COLLATION_KEY
                + " AND " + RawContacts.CONTACT_ID + " NOT NULL");

        Cursor c = db.query(LookupByDisplayNameQuery.TABLE, LookupByDisplayNameQuery.COLUMNS,
                 sb.toString(), null, null, null, null);
        try {
            while (c.moveToNext()) {
                String accountType = c.getString(LookupByDisplayNameQuery.ACCOUNT_TYPE);
                String accountName = c.getString(LookupByDisplayNameQuery.ACCOUNT_NAME);
                int accountHashCode =
                        ContactLookupKey.getAccountHashCode(accountType, accountName);
                String name = c.getString(LookupByDisplayNameQuery.NORMALIZED_NAME);
                for (int i = 0; i < segments.size(); i++) {
                    LookupKeySegment segment = segments.get(i);
                    if (!segment.sourceIdLookup && accountHashCode == segment.accountHashCode
                            && segment.key.equals(name)) {
                        segment.contactId = c.getLong(LookupByDisplayNameQuery.CONTACT_ID);
                        break;
                    }
                }
            }
        } finally {
            c.close();
        }

        return getMostReferencedContactId(segments);
    }

    /**
     * Returns the contact ID that is mentioned the highest number of times.
     */
    private long getMostReferencedContactId(ArrayList<LookupKeySegment> segments) {
        Collections.sort(segments);

        long bestContactId = -1;
        int bestRefCount = 0;

        long contactId = -1;
        int count = 0;

        int segmentCount = segments.size();
        for (int i = 0; i < segmentCount; i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.contactId != -1) {
                if (segment.contactId == contactId) {
                    count++;
                } else {
                    if (count > bestRefCount) {
                        bestContactId = contactId;
                        bestRefCount = count;
                    }
                    contactId = segment.contactId;
                    count = 1;
                }
            }
        }
        if (count > bestRefCount) {
            return contactId;
        } else {
            return bestContactId;
        }
    }

    private void setTablesAndProjectionMapForContacts(SQLiteQueryBuilder qb, Uri uri,
            String[] projection) {
        StringBuilder sb = new StringBuilder();
        boolean excludeRestrictedData = false;
        String requestingPackage = uri.getQueryParameter(
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY);
        if (requestingPackage != null) {
            excludeRestrictedData = !mDbHelper.hasAccessToRestrictedData(requestingPackage);
        }
        sb.append(mDbHelper.getContactView(excludeRestrictedData));
        if (mDbHelper.isInProjection(projection,
                Contacts.CONTACT_PRESENCE)) {
            sb.append(" LEFT OUTER JOIN " + Tables.AGGREGATED_PRESENCE +
                    " ON (" + Contacts._ID + " = " + AggregatedPresenceColumns.CONTACT_ID + ")");
        }
        if (mDbHelper.isInProjection(projection,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES + " "
                    + ContactsStatusUpdatesColumns.ALIAS +
                    " ON (" + ContactsColumns.LAST_STATUS_UPDATE_ID + "="
                            + ContactsStatusUpdatesColumns.CONCRETE_DATA_ID + ")");
        }
        qb.setTables(sb.toString());
        qb.setProjectionMap(sContactsProjectionMap);
    }

    private void setTablesAndProjectionMapForRawContacts(SQLiteQueryBuilder qb, Uri uri) {
        StringBuilder sb = new StringBuilder();
        boolean excludeRestrictedData = false;
        String requestingPackage = uri.getQueryParameter(
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY);
        if (requestingPackage != null) {
            excludeRestrictedData = !mDbHelper.hasAccessToRestrictedData(requestingPackage);
        }
        sb.append(mDbHelper.getRawContactView(excludeRestrictedData));
        qb.setTables(sb.toString());
        qb.setProjectionMap(sRawContactsProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTablesAndProjectionMapForRawContactsEntities(SQLiteQueryBuilder qb, Uri uri) {
        // Note: currently, "export only" equals to "restricted", but may not in the future.
        boolean excludeRestrictedData = readBooleanQueryParameter(uri,
                Data.FOR_EXPORT_ONLY, false);

        String requestingPackage = uri.getQueryParameter(
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY);
        if (requestingPackage != null) {
            excludeRestrictedData = excludeRestrictedData
                    || !mDbHelper.hasAccessToRestrictedData(requestingPackage);
        }
        qb.setTables(mDbHelper.getContactEntitiesView(excludeRestrictedData));
        qb.setProjectionMap(sRawContactsEntityProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTablesAndProjectionMapForData(SQLiteQueryBuilder qb, Uri uri,
            String[] projection, boolean distinct) {
        StringBuilder sb = new StringBuilder();
        // Note: currently, "export only" equals to "restricted", but may not in the future.
        boolean excludeRestrictedData = readBooleanQueryParameter(uri,
                Data.FOR_EXPORT_ONLY, false);

        String requestingPackage = uri.getQueryParameter(
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY);
        if (requestingPackage != null) {
            excludeRestrictedData = excludeRestrictedData
                    || !mDbHelper.hasAccessToRestrictedData(requestingPackage);
        }

        sb.append(mDbHelper.getDataView(excludeRestrictedData));
        sb.append(" data");

        // Include aggregated presence when requested
        if (mDbHelper.isInProjection(projection, Data.CONTACT_PRESENCE)) {
            sb.append(" LEFT OUTER JOIN " + Tables.AGGREGATED_PRESENCE +
                    " ON (" + AggregatedPresenceColumns.CONCRETE_CONTACT_ID + "="
                    + RawContacts.CONTACT_ID + ")");
        }

        // Include aggregated status updates when requested
        if (mDbHelper.isInProjection(projection,
                Data.CONTACT_STATUS,
                Data.CONTACT_STATUS_RES_PACKAGE,
                Data.CONTACT_STATUS_ICON,
                Data.CONTACT_STATUS_LABEL,
                Data.CONTACT_STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES + " "
                    + ContactsStatusUpdatesColumns.ALIAS +
                    " ON (" + ContactsColumns.LAST_STATUS_UPDATE_ID + "="
                            + ContactsStatusUpdatesColumns.CONCRETE_DATA_ID + ")");
        }

        // Include individual presence when requested
        if (mDbHelper.isInProjection(projection, Data.PRESENCE)) {
            sb.append(" LEFT OUTER JOIN " + Tables.PRESENCE +
                    " ON (" + StatusUpdates.DATA_ID + "="
                    + DataColumns.CONCRETE_ID + ")");
        }

        // Include individual status updates when requested
        if (mDbHelper.isInProjection(projection,
                Data.STATUS,
                Data.STATUS_RES_PACKAGE,
                Data.STATUS_ICON,
                Data.STATUS_LABEL,
                Data.STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES +
                    " ON (" + StatusUpdatesColumns.CONCRETE_DATA_ID + "="
                            + DataColumns.CONCRETE_ID + ")");
        }

        qb.setTables(sb.toString());
        qb.setProjectionMap(distinct ? sDistinctDataProjectionMap : sDataProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTableAndProjectionMapForStatusUpdates(SQLiteQueryBuilder qb,
            String[] projection) {
        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getDataView());
        sb.append(" data");

        if (mDbHelper.isInProjection(projection, StatusUpdates.PRESENCE)) {
            sb.append(" LEFT OUTER JOIN " + Tables.PRESENCE +
                    " ON(" + Tables.PRESENCE + "." + StatusUpdates.DATA_ID
                    + "=" + DataColumns.CONCRETE_ID + ")");
        }

        if (mDbHelper.isInProjection(projection,
                StatusUpdates.STATUS,
                StatusUpdates.STATUS_RES_PACKAGE,
                StatusUpdates.STATUS_ICON,
                StatusUpdates.STATUS_LABEL,
                StatusUpdates.STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES +
                    " ON(" + Tables.STATUS_UPDATES + "." + StatusUpdatesColumns.DATA_ID
                    + "=" + DataColumns.CONCRETE_ID + ")");
        }
        qb.setTables(sb.toString());
        qb.setProjectionMap(sStatusUpdatesProjectionMap);
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

    private String appendAccountToSelection(Uri uri, String selection) {
        final String accountName = uri.getQueryParameter(RawContacts.ACCOUNT_NAME);
        final String accountType = uri.getQueryParameter(RawContacts.ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName)) {
            StringBuilder selectionSb = new StringBuilder(RawContacts.ACCOUNT_NAME + "="
                    + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                    + RawContacts.ACCOUNT_TYPE + "="
                    + DatabaseUtils.sqlEscapeString(accountType));
            if (!TextUtils.isEmpty(selection)) {
                selectionSb.append(" AND (");
                selectionSb.append(selection);
                selectionSb.append(')');
            }
            return selectionSb.toString();
        } else {
            return selection;
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

    /**
     * Returns true if all the characters are meaningful as digits
     * in a phone number -- letters, digits, and a few punctuation marks.
     */
    private boolean isPhoneNumber(CharSequence cons) {
        int len = cons.length();

        for (int i = 0; i < len; i++) {
            char c = cons.charAt(i);

            if ((c >= '0') && (c <= '9')) {
                continue;
            }
            if ((c == ' ') || (c == '-') || (c == '(') || (c == ')') || (c == '.') || (c == '+')
                    || (c == '#') || (c == '*')) {
                continue;
            }
            if ((c >= 'A') && (c <= 'Z')) {
                continue;
            }
            if ((c >= 'a') && (c <= 'z')) {
                continue;
            }

            return false;
        }

        return true;
    }

    String getContactsRestrictions() {
        if (mDbHelper.hasAccessToRestrictedData()) {
            return "1";
        } else {
            return RawContacts.IS_RESTRICTED + "=0";
        }
    }

    public String getContactsRestrictionExceptionAsNestedQuery(String contactIdColumn) {
        if (mDbHelper.hasAccessToRestrictedData()) {
            return "1";
        } else {
            return "(SELECT " + RawContacts.IS_RESTRICTED + " FROM " + Tables.RAW_CONTACTS
                    + " WHERE " + RawContactsColumns.CONCRETE_ID + "=" + contactIdColumn + ")=0";
        }
    }

    @Override
    public AssetFileDescriptor openAssetFile(Uri uri, String mode) throws FileNotFoundException {
        int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS_PHOTO: {
                if (!"r".equals(mode)) {
                    throw new FileNotFoundException("Mode " + mode + " not supported.");
                }

                long contactId = Long.parseLong(uri.getPathSegments().get(1));

                String sql =
                        "SELECT " + Photo.PHOTO + " FROM " + mDbHelper.getDataView() +
                        " WHERE " + Data._ID + "=" + Contacts.PHOTO_ID
                                + " AND " + RawContacts.CONTACT_ID + "=" + contactId;
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                return SQLiteContentHelper.getBlobColumnAsAssetFile(db, sql, null);
            }

            case CONTACTS_AS_VCARD: {
                final String lookupKey = uri.getPathSegments().get(2);
                final long contactId = lookupContactIdByLookupKey(mDb, lookupKey);
                final String selection = Contacts._ID + "=" + contactId;

                // When opening a contact as file, we pass back contents as a
                // vCard-encoded stream. We build into a local buffer first,
                // then pipe into MemoryFile once the exact size is known.
                final ByteArrayOutputStream localStream = new ByteArrayOutputStream();
                outputRawContactsAsVCard(localStream, selection, null);
                return buildAssetFileDescriptor(localStream);
            }

            default:
                throw new FileNotFoundException("No file at: " + uri);
        }
    }

    private static final String CONTACT_MEMORY_FILE_NAME = "contactAssetFile";
    private static final String VCARD_TYPE_DEFAULT = "default";

    /**
     * Build a {@link AssetFileDescriptor} through a {@link MemoryFile} with the
     * contents of the given {@link ByteArrayOutputStream}.
     */
    private AssetFileDescriptor buildAssetFileDescriptor(ByteArrayOutputStream stream) {
        AssetFileDescriptor fd = null;
        try {
            stream.flush();

            final byte[] byteData = stream.toByteArray();
            final int size = byteData.length;

            final MemoryFile memoryFile = new MemoryFile(CONTACT_MEMORY_FILE_NAME, size);
            memoryFile.writeBytes(byteData, 0, 0, size);
            memoryFile.deactivate();

            fd = AssetFileDescriptor.fromMemoryFile(memoryFile);
        } catch (IOException e) {
            Log.w(TAG, "Problem writing stream into an AssetFileDescriptor: " + e.toString());
        }
        return fd;
    }

    /**
     * Output {@link RawContacts} matching the requested selection in the vCard
     * format to the given {@link OutputStream}. This method returns silently if
     * any errors encountered.
     */
    private void outputRawContactsAsVCard(OutputStream stream, String selection,
            String[] selectionArgs) {
        final Context context = this.getContext();
        final VCardComposer composer = new VCardComposer(context, VCARD_TYPE_DEFAULT, false);
        composer.addHandler(composer.new HandlerForOutputStream(stream));

        // No extra checks since composer always uses restricted views
        if (!composer.init(selection, selectionArgs))
            return;

        while (!composer.isAfterLast()) {
            if (!composer.createOneEntry()) {
                Log.w(TAG, "Failed to output a contact.");
            }
        }
        composer.terminate();
    }


    private static Account readAccountFromQueryParams(Uri uri) {
        final String name = uri.getQueryParameter(RawContacts.ACCOUNT_NAME);
        final String type = uri.getQueryParameter(RawContacts.ACCOUNT_TYPE);
        if (TextUtils.isEmpty(name) || TextUtils.isEmpty(type)) {
            return null;
        }
        return new Account(name, type);
    }


    /**
     * An implementation of EntityIterator that joins the contacts and data tables
     * and consumes all the data rows for a contact in order to build the Entity for a contact.
     */
    private static class RawContactsEntityIterator implements EntityIterator {
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

        public static final String[] PROJECTION = new String[]{
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.SOURCE_ID,
                RawContacts.VERSION,
                RawContacts.DIRTY,
                RawContacts.Entity.DATA_ID,
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
                RawContacts._ID,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.DATA_VERSION,
                GroupMembership.GROUP_SOURCE_ID,
                RawContacts.SYNC1,
                RawContacts.SYNC2,
                RawContacts.SYNC3,
                RawContacts.SYNC4,
                RawContacts.DELETED,
                RawContacts.CONTACT_ID,
                RawContacts.STARRED,
                RawContacts.IS_RESTRICTED};

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
        private static final int COLUMN_IS_SUPER_PRIMARY = 29;
        private static final int COLUMN_DATA_VERSION = 30;
        private static final int COLUMN_GROUP_SOURCE_ID = 31;
        private static final int COLUMN_SYNC1 = 32;
        private static final int COLUMN_SYNC2 = 33;
        private static final int COLUMN_SYNC3 = 34;
        private static final int COLUMN_SYNC4 = 35;
        private static final int COLUMN_DELETED = 36;
        private static final int COLUMN_CONTACT_ID = 37;
        private static final int COLUMN_STARRED = 38;
        private static final int COLUMN_IS_RESTRICTED = 39;

        public RawContactsEntityIterator(ContactsProvider2 provider, Uri entityUri,
                String contactsIdString,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;
            Uri uri;
            if (contactsIdString != null) {
                uri = Uri.withAppendedPath(RawContacts.CONTENT_URI, contactsIdString);
                uri = Uri.withAppendedPath(uri, RawContacts.Entity.CONTENT_DIRECTORY);
            } else {
                uri = ContactsContract.RawContactsEntity.CONTENT_URI;
            }
            final Uri.Builder builder = uri.buildUpon();
            String query = entityUri.getQuery();
            builder.encodedQuery(query);
            mEntityCursor = provider.query(builder.build(),
                    PROJECTION, selection, selectionArgs, sortOrder);
            mEntityCursor.moveToFirst();
        }

        public void reset() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling reset() when the iterator is closed");
            }
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
            contactValues.put(RawContacts.CONTACT_ID, c.getLong(COLUMN_CONTACT_ID));
            contactValues.put(RawContacts.STARRED, c.getLong(COLUMN_STARRED));
            contactValues.put(RawContacts.IS_RESTRICTED, c.getInt(COLUMN_IS_RESTRICTED));
            Entity contact = new Entity(contactValues);

            // read data rows until the contact id changes
            do {
                if (rawContactId != c.getLong(COLUMN_RAW_CONTACT_ID)) {
                    break;
                }
//                if (c.isNull(COLUMN_CONTACT_ID)) {
//                    continue;
//                }
                // add the data to to the contact
                ContentValues dataValues = new ContentValues();
                dataValues.put(Data._ID, c.getLong(COLUMN_DATA_ID));
                dataValues.put(Data.RES_PACKAGE, c.getString(COLUMN_RES_PACKAGE));
                dataValues.put(Data.MIMETYPE, c.getString(COLUMN_MIMETYPE));
                dataValues.put(Data.IS_PRIMARY, c.getLong(COLUMN_IS_PRIMARY));
                dataValues.put(Data.IS_SUPER_PRIMARY, c.getLong(COLUMN_IS_SUPER_PRIMARY));
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
                Groups.DELETED,
                Groups.SHOULD_SYNC};

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
        private static final int COLUMN_SHOULD_SYNC = 17;

        public GroupsEntityIterator(ContactsProvider2 provider, String groupIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;

            final String updatedSortOrder = (sortOrder == null)
                    ? Groups._ID
                    : (Groups._ID + "," + sortOrder);

            final SQLiteDatabase db = provider.mDbHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(provider.mDbHelper.getGroupView());
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

        public void reset() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling reset() when the iterator is closed");
            }
            mEntityCursor.moveToFirst();
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
            groupValues.put(Groups.SHOULD_SYNC, c.getString(COLUMN_SHOULD_SYNC));
            Entity group = new Entity(groupValues);

            mEntityCursor.moveToNext();

            return group;
        }
    }

    @Override
    public EntityIterator queryEntities(Uri uri, String selection, String[] selectionArgs,
            String sortOrder) {
        waitForAccess();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case RAW_CONTACTS:
            case RAW_CONTACTS_ID:
                String contactsIdString = null;
                if (match == RAW_CONTACTS_ID) {
                    contactsIdString = uri.getPathSegments().get(1);
                }

                return new RawContactsEntityIterator(this, uri, contactsIdString,
                        selection, selectionArgs, sortOrder);
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
            case CONTACTS:
            case CONTACTS_LOOKUP:
                return Contacts.CONTENT_TYPE;
            case CONTACTS_ID:
            case CONTACTS_LOOKUP_ID:
                return Contacts.CONTENT_ITEM_TYPE;
            case CONTACTS_AS_VCARD:
                return Contacts.CONTENT_VCARD_TYPE;
            case RAW_CONTACTS:
                return RawContacts.CONTENT_TYPE;
            case RAW_CONTACTS_ID:
                return RawContacts.CONTENT_ITEM_TYPE;
            case DATA_ID:
                return mDbHelper.getDataMimeType(ContentUris.parseId(uri));
            case PHONES:
                return Phone.CONTENT_TYPE;
            case PHONES_ID:
                return Phone.CONTENT_ITEM_TYPE;
            case EMAILS:
                return Email.CONTENT_TYPE;
            case EMAILS_ID:
                return Email.CONTENT_ITEM_TYPE;
            case POSTALS:
                return StructuredPostal.CONTENT_TYPE;
            case POSTALS_ID:
                return StructuredPostal.CONTENT_ITEM_TYPE;
            case AGGREGATION_EXCEPTIONS:
                return AggregationExceptions.CONTENT_TYPE;
            case AGGREGATION_EXCEPTION_ID:
                return AggregationExceptions.CONTENT_ITEM_TYPE;
            case SETTINGS:
                return Settings.CONTENT_TYPE;
            case AGGREGATION_SUGGESTIONS:
                return Contacts.CONTENT_TYPE;
            case SEARCH_SUGGESTIONS:
                return SearchManager.SUGGEST_MIME_TYPE;
            case SEARCH_SHORTCUT:
                return SearchManager.SHORTCUT_MIME_TYPE;
            default:
                return mLegacyApiSupport.getType(uri);
        }
    }

    private void setDisplayName(long rawContactId, String displayName, int bestDisplayNameSource) {
        if (displayName != null) {
            mRawContactDisplayNameUpdate.bindString(1, displayName);
        } else {
            mRawContactDisplayNameUpdate.bindNull(1);
        }
        mRawContactDisplayNameUpdate.bindLong(2, bestDisplayNameSource);
        mRawContactDisplayNameUpdate.bindLong(3, rawContactId);
        mRawContactDisplayNameUpdate.execute();
    }

    /**
     * Sets the {@link RawContacts#DIRTY} for the specified raw contact.
     */
    private void setRawContactDirty(long rawContactId) {
        mRawContactDirtyUpdate.bindLong(1, rawContactId);
        mRawContactDirtyUpdate.execute();
    }

    /*
     * Sets the given dataId record in the "data" table to primary, and resets all data records of
     * the same mimetype and under the same contact to not be primary.
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsPrimary(long rawContactId, long dataId, long mimeTypeId) {
        mSetPrimaryStatement.bindLong(1, dataId);
        mSetPrimaryStatement.bindLong(2, mimeTypeId);
        mSetPrimaryStatement.bindLong(3, rawContactId);
        mSetPrimaryStatement.execute();
    }

    /*
     * Sets the given dataId record in the "data" table to "super primary", and resets all data
     * records of the same mimetype and under the same aggregate to not be "super primary".
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsSuperPrimary(long rawContactId, long dataId, long mimeTypeId) {
        mSetSuperPrimaryStatement.bindLong(1, dataId);
        mSetSuperPrimaryStatement.bindLong(2, mimeTypeId);
        mSetSuperPrimaryStatement.bindLong(3, rawContactId);
        mSetSuperPrimaryStatement.execute();
    }

    public void insertNameLookupForEmail(long rawContactId, long dataId, String email) {
        if (TextUtils.isEmpty(email)) {
            return;
        }

        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return;
        }

        String address = tokens[0].getAddress();
        int at = address.indexOf('@');
        if (at != -1) {
            address = address.substring(0, at);
        }

        insertNameLookup(rawContactId, dataId,
                NameLookupType.EMAIL_BASED_NICKNAME, NameNormalizer.normalize(address));
    }

    /**
     * Normalizes the nickname and inserts it in the name lookup table.
     */
    public void insertNameLookupForNickname(long rawContactId, long dataId, String nickname) {
        if (TextUtils.isEmpty(nickname)) {
            return;
        }

        insertNameLookup(rawContactId, dataId,
                NameLookupType.NICKNAME, NameNormalizer.normalize(nickname));
    }

    public void insertNameLookupForOrganization(long rawContactId, long dataId, String company,
            String title) {
        if (!TextUtils.isEmpty(company)) {
            insertNameLookup(rawContactId, dataId,
                    NameLookupType.ORGANIZATION, NameNormalizer.normalize(company));
        }
        if (!TextUtils.isEmpty(title)) {
            insertNameLookup(rawContactId, dataId,
                    NameLookupType.ORGANIZATION, NameNormalizer.normalize(title));
        }
    }

    public void insertNameLookupForStructuredName(long rawContactId, long dataId, String name) {
        mNameLookupBuilder.insertNameLookup(rawContactId, dataId, name);
    }

    /**
     * Returns nickname cluster IDs or null. Maintains cache.
     */
    protected String[] getCommonNicknameClusters(String normalizedName) {
        SoftReference<String[]> ref;
        String[] clusters = null;
        synchronized (mNicknameClusterCache) {
            if (mNicknameClusterCache.containsKey(normalizedName)) {
                ref = mNicknameClusterCache.get(normalizedName);
                if (ref == null) {
                    return null;
                }
                clusters = ref.get();
            }
        }

        if (clusters == null) {
            clusters = loadNicknameClusters(normalizedName);
            ref = clusters == null ? null : new SoftReference<String[]>(clusters);
            synchronized (mNicknameClusterCache) {
                mNicknameClusterCache.put(normalizedName, ref);
            }
        }
        return clusters;
    }

    protected String[] loadNicknameClusters(String normalizedName) {
        SQLiteDatabase db = mDbHelper.getReadableDatabase();
        String[] clusters = null;
        Cursor cursor = db.query(NicknameLookupQuery.TABLE, NicknameLookupQuery.COLUMNS,
                NicknameLookupColumns.NAME + "=?", new String[] { normalizedName },
                null, null, null);
        try {
            int count = cursor.getCount();
            if (count > 0) {
                clusters = new String[count];
                for (int i = 0; i < count; i++) {
                    cursor.moveToNext();
                    clusters[i] = cursor.getString(NicknameLookupQuery.CLUSTER);
                }
            }
        } finally {
            cursor.close();
        }
        return clusters;
    }

    private class StructuredNameLookupBuilder extends NameLookupBuilder {

        public StructuredNameLookupBuilder(NameSplitter splitter) {
            super(splitter);
        }

        @Override
        protected void insertNameLookup(long rawContactId, long dataId, int lookupType,
                String name) {
            ContactsProvider2.this.insertNameLookup(rawContactId, dataId, lookupType, name);
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return ContactsProvider2.this.getCommonNicknameClusters(normalizedName);
        }
    }

    /**
     * Inserts a record in the {@link Tables#NAME_LOOKUP} table.
     */
    public void insertNameLookup(long rawContactId, long dataId, int lookupType, String name) {
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 1, rawContactId);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 2, dataId);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 3, lookupType);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 4, name);
        mNameLookupInsert.executeInsert();
    }

    /**
     * Deletes all {@link Tables#NAME_LOOKUP} table rows associated with the specified data element.
     */
    public void deleteNameLookup(long dataId) {
        DatabaseUtils.bindObjectToProgram(mNameLookupDelete, 1, dataId);
        mNameLookupDelete.execute();
    }

    public void appendContactFilterAsNestedQuery(StringBuilder sb, String filterParam) {
        sb.append("(" +
                "SELECT DISTINCT " + RawContacts.CONTACT_ID +
                " FROM " + Tables.RAW_CONTACTS +
                " JOIN " + Tables.NAME_LOOKUP +
                " ON(" + RawContactsColumns.CONCRETE_ID + "="
                        + NameLookupColumns.RAW_CONTACT_ID + ")" +
                " WHERE normalized_name GLOB '");
        sb.append(NameNormalizer.normalize(filterParam));
        sb.append("*' AND " + NameLookupColumns.NAME_TYPE + " IN("
                + NameLookupType.NAME_COLLATION_KEY + ","
                + NameLookupType.EMAIL_BASED_NICKNAME + ","
                + NameLookupType.NICKNAME + ","
                + NameLookupType.ORGANIZATION + "))");
    }

    public String getRawContactsByFilterAsNestedQuery(String filterParam) {
        StringBuilder sb = new StringBuilder();
        appendRawContactsByFilterAsNestedQuery(sb, filterParam, null);
        return sb.toString();
    }

    public void appendRawContactsByFilterAsNestedQuery(StringBuilder sb, String filterParam,
            String limit) {
        appendRawContactsByNormalizedNameFilter(sb, NameNormalizer.normalize(filterParam), limit,
                true);
    }

    private void appendRawContactsByNormalizedNameFilter(StringBuilder sb, String normalizedName,
            String limit, boolean allowEmailMatch) {
        sb.append("(" +
                "SELECT DISTINCT " + NameLookupColumns.RAW_CONTACT_ID +
                " FROM " + Tables.NAME_LOOKUP +
                " WHERE " + NameLookupColumns.NORMALIZED_NAME +
                " GLOB '");
        sb.append(normalizedName);
        sb.append("*' AND " + NameLookupColumns.NAME_TYPE + " IN ("
                + NameLookupType.NAME_COLLATION_KEY + ","
                + NameLookupType.NICKNAME + ","
                + NameLookupType.ORGANIZATION);
        if (allowEmailMatch) {
            sb.append("," + NameLookupType.EMAIL_BASED_NICKNAME);
        }
        sb.append(")");

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

    private String[] appendProjectionArg(String[] projection, String arg) {
        if (projection == null) {
            return null;
        }
        final int length = projection.length;
        String[] newProjection = new String[length + 1];
        System.arraycopy(projection, 0, newProjection, 0, length);
        newProjection[length] = arg;
        return newProjection;
    }

    protected Account getDefaultAccount() {
        AccountManager accountManager = AccountManager.get(getContext());
        try {
            Account[] accounts = accountManager.getAccountsByTypeAndFeatures(DEFAULT_ACCOUNT_TYPE,
                    new String[] {FEATURE_LEGACY_HOSTED_OR_GOOGLE}, null, null).getResult();
            if (accounts != null && accounts.length > 0) {
                return accounts[0];
            }
        } catch (Throwable e) {
            Log.e(TAG, "Cannot determine the default account for contacts compatibility", e);
        }
        return null;
    }

    protected boolean isWritableAccount(Account account) {
        IContentService contentService = ContentResolver.getContentService();
        try {
            for (SyncAdapterType sync : contentService.getSyncAdapterTypes()) {
                if (ContactsContract.AUTHORITY.equals(sync.authority) &&
                        account.type.equals(sync.accountType)) {
                    return sync.supportsUploading();
                }
            }
        } catch (RemoteException e) {
            Log.e(TAG, "Could not acquire sync adapter types");
        }

        return false;
    }
}
