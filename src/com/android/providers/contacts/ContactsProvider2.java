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

import com.android.common.content.SQLiteContentProvider;
import com.android.common.content.SyncStateContentProviderHelper;
import com.android.providers.contacts.ContactAggregator.AggregationSuggestionParameter;
import com.android.providers.contacts.ContactLookupKey.LookupKeySegment;
import com.android.providers.contacts.ContactsDatabaseHelper.AccountsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregationExceptionColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Clauses;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsStatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataUsageStatColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.GroupsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupType;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.SearchIndexColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.SettingsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.StatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.vcard.VCardComposer;
import com.android.vcard.VCardConfig;
import com.google.android.collect.Lists;
import com.google.android.collect.Maps;
import com.google.android.collect.Sets;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.OnAccountsUpdateListener;
import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.SearchManager;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.IContentService;
import android.content.Intent;
import android.content.OperationApplicationException;
import android.content.SharedPreferences;
import android.content.SyncAdapterType;
import android.content.UriMatcher;
import android.content.res.AssetFileDescriptor;
import android.database.CrossProcessCursor;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.CursorWrapper;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.MatrixCursor.RowBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.net.Uri.Builder;
import android.os.Binder;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Message;
import android.os.ParcelFileDescriptor;
import android.os.Process;
import android.os.RemoteException;
import android.os.StrictMode;
import android.os.SystemClock;
import android.os.SystemProperties;
import android.preference.PreferenceManager;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.ContactCounts;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Contacts.AggregationSuggestions;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.DataUsageFeedback;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Intents;
import android.provider.ContactsContract.PhoneLookup;
import android.provider.ContactsContract.ProviderStatus;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.SearchSnippetColumns;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.LiveFolders;
import android.provider.OpenableColumns;
import android.provider.SyncStateContract;
import android.telephony.PhoneNumberUtils;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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

    private static final int BACKGROUND_TASK_INITIALIZE = 0;
    private static final int BACKGROUND_TASK_OPEN_WRITE_ACCESS = 1;
    private static final int BACKGROUND_TASK_IMPORT_LEGACY_CONTACTS = 2;
    private static final int BACKGROUND_TASK_UPDATE_ACCOUNTS = 3;
    private static final int BACKGROUND_TASK_UPDATE_LOCALE = 4;
    private static final int BACKGROUND_TASK_UPGRADE_AGGREGATION_ALGORITHM = 5;
    private static final int BACKGROUND_TASK_UPDATE_SEARCH_INDEX = 6;
    private static final int BACKGROUND_TASK_UPDATE_PROVIDER_STATUS = 7;
    private static final int BACKGROUND_TASK_UPDATE_DIRECTORIES = 8;
    private static final int BACKGROUND_TASK_CHANGE_LOCALE = 9;

    /** Default for the maximum number of returned aggregation suggestions. */
    private static final int DEFAULT_MAX_SUGGESTIONS = 5;

    /**
     * Property key for the legacy contact import version. The need for a version
     * as opposed to a boolean flag is that if we discover bugs in the contact import process,
     * we can trigger re-import by incrementing the import version.
     */
    private static final String PROPERTY_CONTACTS_IMPORTED = "contacts_imported_v1";
    private static final int PROPERTY_CONTACTS_IMPORT_VERSION = 1;
    private static final String PREF_LOCALE = "locale";

    private static final String PROPERTY_AGGREGATION_ALGORITHM = "aggregation_v2";
    private static final int PROPERTY_AGGREGATION_ALGORITHM_VERSION = 2;

    private static final String AGGREGATE_CONTACTS = "sync.contacts.aggregate";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final String TIMES_CONTACTED_SORT_COLUMN = "times_contacted_sort";

    private static final String STREQUENT_ORDER_BY = Contacts.STARRED + " DESC, "
            + TIMES_CONTACTED_SORT_COLUMN + " DESC, "
            + Contacts.DISPLAY_NAME + " COLLATE LOCALIZED ASC";
    private static final String STREQUENT_LIMIT =
            "(SELECT COUNT(1) FROM " + Tables.CONTACTS + " WHERE "
            + Contacts.STARRED + "=1) + 25";

    /* package */ static final String UPDATE_TIMES_CONTACTED_CONTACTS_TABLE =
            "UPDATE " + Tables.CONTACTS + " SET " + Contacts.TIMES_CONTACTED + "=" +
            " CASE WHEN " + Contacts.TIMES_CONTACTED + " IS NULL THEN 1 ELSE " +
            " (" + Contacts.TIMES_CONTACTED + " + 1) END WHERE " + Contacts._ID + "=?";

    /* package */ static final String UPDATE_TIMES_CONTACTED_RAWCONTACTS_TABLE =
            "UPDATE " + Tables.RAW_CONTACTS + " SET " + RawContacts.TIMES_CONTACTED + "=" +
            " CASE WHEN " + RawContacts.TIMES_CONTACTED + " IS NULL THEN 1 ELSE " +
            " (" + RawContacts.TIMES_CONTACTED + " + 1) END WHERE " + RawContacts.CONTACT_ID + "=?";

    /* package */ static final String PHONEBOOK_COLLATOR_NAME = "PHONEBOOK";

    // Regex for splitting query strings - we split on any group of non-alphanumeric characters,
    // excluding the @ symbol.
    /* package */ static final String QUERY_TOKENIZER_REGEX = "[^\\w@]+";

    private static final int CONTACTS = 1000;
    private static final int CONTACTS_ID = 1001;
    private static final int CONTACTS_LOOKUP = 1002;
    private static final int CONTACTS_LOOKUP_ID = 1003;
    private static final int CONTACTS_ID_DATA = 1004;
    private static final int CONTACTS_FILTER = 1005;
    private static final int CONTACTS_STREQUENT = 1006;
    private static final int CONTACTS_STREQUENT_FILTER = 1007;
    private static final int CONTACTS_GROUP = 1008;
    private static final int CONTACTS_ID_PHOTO = 1009;
    private static final int CONTACTS_AS_VCARD = 1010;
    private static final int CONTACTS_AS_MULTI_VCARD = 1011;
    private static final int CONTACTS_LOOKUP_DATA = 1012;
    private static final int CONTACTS_LOOKUP_ID_DATA = 1013;
    private static final int CONTACTS_ID_ENTITIES = 1014;
    private static final int CONTACTS_LOOKUP_ENTITIES = 1015;
    private static final int CONTACTS_LOOKUP_ID_ENTITIES = 1016;

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

    private static final int PROVIDER_STATUS = 16001;

    private static final int DIRECTORIES = 17001;
    private static final int DIRECTORIES_ID = 17002;

    private static final int COMPLETE_NAME = 18000;

    private static final int PROFILE = 19000;
    private static final int PROFILE_ENTITIES = 19001;
    private static final int PROFILE_DATA = 19002;
    private static final int PROFILE_DATA_ID = 19003;
    private static final int PROFILE_AS_VCARD = 19004;
    private static final int PROFILE_RAW_CONTACTS = 19005;
    private static final int PROFILE_RAW_CONTACTS_ID = 19006;
    private static final int PROFILE_RAW_CONTACTS_ID_DATA = 19007;
    private static final int PROFILE_RAW_CONTACTS_ID_ENTITIES = 19008;

    private static final int DATA_USAGE_FEEDBACK_ID = 20001;

    private static final String SELECTION_FAVORITES_GROUPS_BY_RAW_CONTACT_ID =
            RawContactsColumns.CONCRETE_ID + "=? AND "
                    + GroupsColumns.CONCRETE_ACCOUNT_NAME
                    + "=" + RawContactsColumns.CONCRETE_ACCOUNT_NAME + " AND "
                    + GroupsColumns.CONCRETE_ACCOUNT_TYPE
                    + "=" + RawContactsColumns.CONCRETE_ACCOUNT_TYPE
                    + " AND " + Groups.FAVORITES + " != 0";

    private static final String SELECTION_AUTO_ADD_GROUPS_BY_RAW_CONTACT_ID =
            RawContactsColumns.CONCRETE_ID + "=? AND "
                    + GroupsColumns.CONCRETE_ACCOUNT_NAME + "="
                    + RawContactsColumns.CONCRETE_ACCOUNT_NAME + " AND "
                    + GroupsColumns.CONCRETE_ACCOUNT_TYPE + "="
                    + RawContactsColumns.CONCRETE_ACCOUNT_TYPE + " AND "
                    + Groups.AUTO_ADD + " != 0";

    private static final String[] PROJECTION_GROUP_ID
            = new String[]{Tables.GROUPS + "." + Groups._ID};

    private static final String SELECTION_GROUPMEMBERSHIP_DATA = DataColumns.MIMETYPE_ID + "=? "
            + "AND " + GroupMembership.GROUP_ROW_ID + "=? "
            + "AND " + GroupMembership.RAW_CONTACT_ID + "=?";

    private static final String SELECTION_STARRED_FROM_RAW_CONTACTS =
            "SELECT " + RawContacts.STARRED
                    + " FROM " + Tables.RAW_CONTACTS + " WHERE " + RawContacts._ID + "=?";

    public class AddressBookCursor extends CursorWrapper implements CrossProcessCursor {
        private final CrossProcessCursor mCursor;
        private final Bundle mBundle;

        public AddressBookCursor(CrossProcessCursor cursor, String[] titles, int[] counts) {
            super(cursor);
            mCursor = cursor;
            mBundle = new Bundle();
            mBundle.putStringArray(ContactCounts.EXTRA_ADDRESS_BOOK_INDEX_TITLES, titles);
            mBundle.putIntArray(ContactCounts.EXTRA_ADDRESS_BOOK_INDEX_COUNTS, counts);
        }

        @Override
        public Bundle getExtras() {
            return mBundle;
        }

        @Override
        public void fillWindow(int pos, CursorWindow window) {
            mCursor.fillWindow(pos, window);
        }

        @Override
        public CursorWindow getWindow() {
            return mCursor.getWindow();
        }

        @Override
        public boolean onMove(int oldPosition, int newPosition) {
            return mCursor.onMove(oldPosition, newPosition);
        }
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

    interface RawContactsQuery {
        String TABLE = Tables.RAW_CONTACTS;

        String[] COLUMNS = new String[] {
                RawContacts.DELETED,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.ACCOUNT_NAME,
        };

        int DELETED = 0;
        int ACCOUNT_TYPE = 1;
        int ACCOUNT_NAME = 2;
    }

    public static final String DEFAULT_ACCOUNT_TYPE = "com.google";

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

    /** Sql for updating DIRTY flag on multiple raw contacts */
    private static final String UPDATE_RAW_CONTACT_SET_DIRTY_SQL =
            "UPDATE " + Tables.RAW_CONTACTS +
            " SET " + RawContacts.DIRTY + "=1" +
            " WHERE " + RawContacts._ID + " IN (";

    /** Sql for updating VERSION on multiple raw contacts */
    private static final String UPDATE_RAW_CONTACT_SET_VERSION_SQL =
            "UPDATE " + Tables.RAW_CONTACTS +
            " SET " + RawContacts.VERSION + " = " + RawContacts.VERSION + " + 1" +
            " WHERE " + RawContacts._ID + " IN (";

    // Current contacts - those contacted within the last 3 days (in seconds)
    private static final long EMAIL_FILTER_CURRENT = 3 * 24 * 60 * 60;

    // Recent contacts - those contacted within the last 30 days (in seconds)
    private static final long EMAIL_FILTER_RECENT = 30 * 24 * 60 * 60;

    /*
     * Sorting order for email address suggestions: first starred, then the rest.
     * second in_visible_group, then the rest.
     * Within the four (starred/unstarred, in_visible_group/not-in_visible_group) groups
     * - three buckets: very recently contacted, then fairly
     * recently contacted, then the rest.  Within each of the bucket - descending count
     * of times contacted (both for data row and for contact row). If all else fails, alphabetical.
     * (Super)primary email address is returned before other addresses for the same contact.
     */
    private static final String EMAIL_FILTER_SORT_ORDER =
        Contacts.STARRED + " DESC, "
        + Contacts.IN_VISIBLE_GROUP + " DESC, "
        + "(CASE WHEN " + DataUsageStatColumns.LAST_TIME_USED + " < " + EMAIL_FILTER_CURRENT
        + " THEN 0 "
                + " WHEN " + DataUsageStatColumns.LAST_TIME_USED + " < " + EMAIL_FILTER_RECENT
        + " THEN 1 "
        + " ELSE 2 END), "
        + DataUsageStatColumns.TIMES_USED + " DESC, "
        + Contacts.DISPLAY_NAME + ", "
        + Data.CONTACT_ID + ", "
        + Data.IS_SUPER_PRIMARY + " DESC, "
        + Data.IS_PRIMARY + " DESC";

    /** Currently same as {@link #EMAIL_FILTER_SORT_ORDER} */
    private static final String PHONE_FILTER_SORT_ORDER = EMAIL_FILTER_SORT_ORDER;

    /** Name lookup types used for contact filtering */
    private static final String CONTACT_LOOKUP_NAME_TYPES =
            NameLookupType.NAME_COLLATION_KEY + "," +
            NameLookupType.EMAIL_BASED_NICKNAME + "," +
            NameLookupType.NICKNAME;

    /**
     * If any of these columns are used in a Data projection, there is no point in
     * using the DISTINCT keyword, which can negatively affect performance.
     */
    private static final String[] DISTINCT_DATA_PROHIBITING_COLUMNS = {
            Data._ID,
            Data.RAW_CONTACT_ID,
            Data.NAME_RAW_CONTACT_ID,
            RawContacts.ACCOUNT_NAME,
            RawContacts.ACCOUNT_TYPE,
            RawContacts.DIRTY,
            RawContacts.NAME_VERIFIED,
            RawContacts.SOURCE_ID,
            RawContacts.VERSION,
    };

    private static final ProjectionMap sContactsColumns = ProjectionMap.builder()
            .add(Contacts.CUSTOM_RINGTONE)
            .add(Contacts.DISPLAY_NAME)
            .add(Contacts.DISPLAY_NAME_ALTERNATIVE)
            .add(Contacts.DISPLAY_NAME_SOURCE)
            .add(Contacts.IN_VISIBLE_GROUP)
            .add(Contacts.LAST_TIME_CONTACTED)
            .add(Contacts.LOOKUP_KEY)
            .add(Contacts.PHONETIC_NAME)
            .add(Contacts.PHONETIC_NAME_STYLE)
            .add(Contacts.PHOTO_ID)
            .add(Contacts.PHOTO_URI)
            .add(Contacts.PHOTO_THUMBNAIL_URI)
            .add(Contacts.SEND_TO_VOICEMAIL)
            .add(Contacts.SORT_KEY_ALTERNATIVE)
            .add(Contacts.SORT_KEY_PRIMARY)
            .add(Contacts.STARRED)
            .add(Contacts.TIMES_CONTACTED)
            .add(Contacts.HAS_PHONE_NUMBER)
            .build();

    private static final ProjectionMap sContactsPresenceColumns = ProjectionMap.builder()
            .add(Contacts.CONTACT_PRESENCE,
                    Tables.AGGREGATED_PRESENCE + "." + StatusUpdates.PRESENCE)
            .add(Contacts.CONTACT_CHAT_CAPABILITY,
                    Tables.AGGREGATED_PRESENCE + "." + StatusUpdates.CHAT_CAPABILITY)
            .add(Contacts.CONTACT_STATUS,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS)
            .add(Contacts.CONTACT_STATUS_TIMESTAMP,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP)
            .add(Contacts.CONTACT_STATUS_RES_PACKAGE,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE)
            .add(Contacts.CONTACT_STATUS_LABEL,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_LABEL)
            .add(Contacts.CONTACT_STATUS_ICON,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_ICON)
            .build();

    private static final ProjectionMap sSnippetColumns = ProjectionMap.builder()
            .add(SearchSnippetColumns.SNIPPET)
            .build();

    private static final ProjectionMap sRawContactColumns = ProjectionMap.builder()
            .add(RawContacts.ACCOUNT_NAME)
            .add(RawContacts.ACCOUNT_TYPE)
            .add(RawContacts.DIRTY)
            .add(RawContacts.NAME_VERIFIED)
            .add(RawContacts.SOURCE_ID)
            .add(RawContacts.VERSION)
            .build();

    private static final ProjectionMap sRawContactSyncColumns = ProjectionMap.builder()
            .add(RawContacts.SYNC1)
            .add(RawContacts.SYNC2)
            .add(RawContacts.SYNC3)
            .add(RawContacts.SYNC4)
            .build();

    private static final ProjectionMap sDataColumns = ProjectionMap.builder()
            .add(Data.DATA1)
            .add(Data.DATA2)
            .add(Data.DATA3)
            .add(Data.DATA4)
            .add(Data.DATA5)
            .add(Data.DATA6)
            .add(Data.DATA7)
            .add(Data.DATA8)
            .add(Data.DATA9)
            .add(Data.DATA10)
            .add(Data.DATA11)
            .add(Data.DATA12)
            .add(Data.DATA13)
            .add(Data.DATA14)
            .add(Data.DATA15)
            .add(Data.DATA_VERSION)
            .add(Data.IS_PRIMARY)
            .add(Data.IS_SUPER_PRIMARY)
            .add(Data.MIMETYPE)
            .add(Data.RES_PACKAGE)
            .add(Data.SYNC1)
            .add(Data.SYNC2)
            .add(Data.SYNC3)
            .add(Data.SYNC4)
            .add(GroupMembership.GROUP_SOURCE_ID)
            .build();

    private static final ProjectionMap sContactPresenceColumns = ProjectionMap.builder()
            .add(Contacts.CONTACT_PRESENCE,
                    Tables.AGGREGATED_PRESENCE + '.' + StatusUpdates.PRESENCE)
            .add(Contacts.CONTACT_CHAT_CAPABILITY,
                    Tables.AGGREGATED_PRESENCE + '.' + StatusUpdates.CHAT_CAPABILITY)
            .add(Contacts.CONTACT_STATUS,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS)
            .add(Contacts.CONTACT_STATUS_TIMESTAMP,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP)
            .add(Contacts.CONTACT_STATUS_RES_PACKAGE,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE)
            .add(Contacts.CONTACT_STATUS_LABEL,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_LABEL)
            .add(Contacts.CONTACT_STATUS_ICON,
                    ContactsStatusUpdatesColumns.CONCRETE_STATUS_ICON)
            .build();

    private static final ProjectionMap sDataPresenceColumns = ProjectionMap.builder()
            .add(Data.PRESENCE, Tables.PRESENCE + "." + StatusUpdates.PRESENCE)
            .add(Data.CHAT_CAPABILITY, Tables.PRESENCE + "." + StatusUpdates.CHAT_CAPABILITY)
            .add(Data.STATUS, StatusUpdatesColumns.CONCRETE_STATUS)
            .add(Data.STATUS_TIMESTAMP, StatusUpdatesColumns.CONCRETE_STATUS_TIMESTAMP)
            .add(Data.STATUS_RES_PACKAGE, StatusUpdatesColumns.CONCRETE_STATUS_RES_PACKAGE)
            .add(Data.STATUS_LABEL, StatusUpdatesColumns.CONCRETE_STATUS_LABEL)
            .add(Data.STATUS_ICON, StatusUpdatesColumns.CONCRETE_STATUS_ICON)
            .build();

    /** Contains just BaseColumns._COUNT */
    private static final ProjectionMap sCountProjectionMap = ProjectionMap.builder()
            .add(BaseColumns._COUNT, "COUNT(*)")
            .build();

    /** Contains just the contacts columns */
    private static final ProjectionMap sContactsProjectionMap = ProjectionMap.builder()
            .add(Contacts._ID)
            .add(Contacts.HAS_PHONE_NUMBER)
            .add(Contacts.NAME_RAW_CONTACT_ID)
            .add(Contacts.IS_USER_PROFILE)
            .addAll(sContactsColumns)
            .addAll(sContactsPresenceColumns)
            .build();

    /** Contains just the contacts columns */
    private static final ProjectionMap sContactsProjectionWithSnippetMap = ProjectionMap.builder()
            .addAll(sContactsProjectionMap)
            .addAll(sSnippetColumns)
            .build();

    /** Used for pushing starred contacts to the top of a times contacted list **/
    private static final ProjectionMap sStrequentStarredProjectionMap = ProjectionMap.builder()
            .addAll(sContactsProjectionMap)
            .add(TIMES_CONTACTED_SORT_COLUMN, String.valueOf(Long.MAX_VALUE))
            .build();

    private static final ProjectionMap sStrequentFrequentProjectionMap = ProjectionMap.builder()
            .addAll(sContactsProjectionMap)
            .add(TIMES_CONTACTED_SORT_COLUMN, Contacts.TIMES_CONTACTED)
            .build();

    /** Contains just the contacts vCard columns */
    private static final ProjectionMap sContactsVCardProjectionMap = ProjectionMap.builder()
            .add(Contacts._ID)
            .add(OpenableColumns.DISPLAY_NAME, Contacts.DISPLAY_NAME + " || '.vcf'")
            .add(OpenableColumns.SIZE, "NULL")
            .build();

    /** Contains just the raw contacts columns */
    private static final ProjectionMap sRawContactsProjectionMap = ProjectionMap.builder()
            .add(RawContacts._ID)
            .add(RawContacts.CONTACT_ID)
            .add(RawContacts.DELETED)
            .add(RawContacts.DISPLAY_NAME_PRIMARY)
            .add(RawContacts.DISPLAY_NAME_ALTERNATIVE)
            .add(RawContacts.DISPLAY_NAME_SOURCE)
            .add(RawContacts.PHONETIC_NAME)
            .add(RawContacts.PHONETIC_NAME_STYLE)
            .add(RawContacts.SORT_KEY_PRIMARY)
            .add(RawContacts.SORT_KEY_ALTERNATIVE)
            .add(RawContacts.TIMES_CONTACTED)
            .add(RawContacts.LAST_TIME_CONTACTED)
            .add(RawContacts.CUSTOM_RINGTONE)
            .add(RawContacts.SEND_TO_VOICEMAIL)
            .add(RawContacts.STARRED)
            .add(RawContacts.AGGREGATION_MODE)
            .add(RawContacts.RAW_CONTACT_IS_USER_PROFILE)
            .addAll(sRawContactColumns)
            .addAll(sRawContactSyncColumns)
            .build();

    /** Contains the columns from the raw entity view*/
    private static final ProjectionMap sRawEntityProjectionMap = ProjectionMap.builder()
            .add(RawContacts._ID)
            .add(RawContacts.CONTACT_ID)
            .add(RawContacts.Entity.DATA_ID)
            .add(RawContacts.IS_RESTRICTED)
            .add(RawContacts.DELETED)
            .add(RawContacts.STARRED)
            .add(RawContacts.RAW_CONTACT_IS_USER_PROFILE)
            .addAll(sRawContactColumns)
            .addAll(sRawContactSyncColumns)
            .addAll(sDataColumns)
            .build();

    /** Contains the columns from the contact entity view*/
    private static final ProjectionMap sEntityProjectionMap = ProjectionMap.builder()
            .add(Contacts.Entity._ID)
            .add(Contacts.Entity.CONTACT_ID)
            .add(Contacts.Entity.RAW_CONTACT_ID)
            .add(Contacts.Entity.DATA_ID)
            .add(Contacts.Entity.NAME_RAW_CONTACT_ID)
            .add(Contacts.Entity.DELETED)
            .add(Contacts.Entity.IS_RESTRICTED)
            .add(Contacts.IS_USER_PROFILE)
            .addAll(sContactsColumns)
            .addAll(sContactPresenceColumns)
            .addAll(sRawContactColumns)
            .addAll(sRawContactSyncColumns)
            .addAll(sDataColumns)
            .addAll(sDataPresenceColumns)
            .build();

    /** Contains columns from the data view */
    private static final ProjectionMap sDataProjectionMap = ProjectionMap.builder()
            .add(Data._ID)
            .add(Data.RAW_CONTACT_ID)
            .add(Data.CONTACT_ID)
            .add(Data.NAME_RAW_CONTACT_ID)
            .add(RawContacts.RAW_CONTACT_IS_USER_PROFILE)
            .addAll(sDataColumns)
            .addAll(sDataPresenceColumns)
            .addAll(sRawContactColumns)
            .addAll(sContactsColumns)
            .addAll(sContactPresenceColumns)
            .build();

    /** Contains columns from the data view */
    private static final ProjectionMap sDistinctDataProjectionMap = ProjectionMap.builder()
            .add(Data._ID, "MIN(" + Data._ID + ")")
            .add(RawContacts.CONTACT_ID)
            .add(RawContacts.RAW_CONTACT_IS_USER_PROFILE)
            .addAll(sDataColumns)
            .addAll(sDataPresenceColumns)
            .addAll(sContactsColumns)
            .addAll(sContactPresenceColumns)
            .build();

    /** Contains the data and contacts columns, for joined tables */
    private static final ProjectionMap sPhoneLookupProjectionMap = ProjectionMap.builder()
            .add(PhoneLookup._ID, "contacts_view." + Contacts._ID)
            .add(PhoneLookup.LOOKUP_KEY, "contacts_view." + Contacts.LOOKUP_KEY)
            .add(PhoneLookup.DISPLAY_NAME, "contacts_view." + Contacts.DISPLAY_NAME)
            .add(PhoneLookup.LAST_TIME_CONTACTED, "contacts_view." + Contacts.LAST_TIME_CONTACTED)
            .add(PhoneLookup.TIMES_CONTACTED, "contacts_view." + Contacts.TIMES_CONTACTED)
            .add(PhoneLookup.STARRED, "contacts_view." + Contacts.STARRED)
            .add(PhoneLookup.IN_VISIBLE_GROUP, "contacts_view." + Contacts.IN_VISIBLE_GROUP)
            .add(PhoneLookup.PHOTO_ID, "contacts_view." + Contacts.PHOTO_ID)
            .add(PhoneLookup.PHOTO_URI, "contacts_view." + Contacts.PHOTO_URI)
            .add(PhoneLookup.PHOTO_THUMBNAIL_URI, "contacts_view." + Contacts.PHOTO_THUMBNAIL_URI)
            .add(PhoneLookup.CUSTOM_RINGTONE, "contacts_view." + Contacts.CUSTOM_RINGTONE)
            .add(PhoneLookup.HAS_PHONE_NUMBER, "contacts_view." + Contacts.HAS_PHONE_NUMBER)
            .add(PhoneLookup.SEND_TO_VOICEMAIL, "contacts_view." + Contacts.SEND_TO_VOICEMAIL)
            .add(PhoneLookup.NUMBER, Phone.NUMBER)
            .add(PhoneLookup.TYPE, Phone.TYPE)
            .add(PhoneLookup.LABEL, Phone.LABEL)
            .add(PhoneLookup.NORMALIZED_NUMBER, Phone.NORMALIZED_NUMBER)
            .build();

    /** Contains the just the {@link Groups} columns */
    private static final ProjectionMap sGroupsProjectionMap = ProjectionMap.builder()
            .add(Groups._ID)
            .add(Groups.ACCOUNT_NAME)
            .add(Groups.ACCOUNT_TYPE)
            .add(Groups.SOURCE_ID)
            .add(Groups.DIRTY)
            .add(Groups.VERSION)
            .add(Groups.RES_PACKAGE)
            .add(Groups.TITLE)
            .add(Groups.TITLE_RES)
            .add(Groups.GROUP_VISIBLE)
            .add(Groups.SYSTEM_ID)
            .add(Groups.DELETED)
            .add(Groups.NOTES)
            .add(Groups.SHOULD_SYNC)
            .add(Groups.FAVORITES)
            .add(Groups.AUTO_ADD)
            .add(Groups.GROUP_IS_READ_ONLY)
            .add(Groups.SYNC1)
            .add(Groups.SYNC2)
            .add(Groups.SYNC3)
            .add(Groups.SYNC4)
            .build();

    /** Contains {@link Groups} columns along with summary details */
    private static final ProjectionMap sGroupsSummaryProjectionMap = ProjectionMap.builder()
            .addAll(sGroupsProjectionMap)
            .add(Groups.SUMMARY_COUNT,
                    "(SELECT COUNT(DISTINCT " + ContactsColumns.CONCRETE_ID
                    + ") FROM " + Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS
                    + " WHERE " + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP
                    + " AND " + Clauses.BELONGS_TO_GROUP
                    + ")")
            .add(Groups.SUMMARY_WITH_PHONES,
                    "(SELECT COUNT(DISTINCT " + ContactsColumns.CONCRETE_ID
                    + ") FROM " + Tables.DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS
                    + " WHERE " + Clauses.MIMETYPE_IS_GROUP_MEMBERSHIP
                    + " AND " + Clauses.BELONGS_TO_GROUP
                    + " AND " + Contacts.HAS_PHONE_NUMBER + ")")
            .build();

    /** Contains the agg_exceptions columns */
    private static final ProjectionMap sAggregationExceptionsProjectionMap = ProjectionMap.builder()
            .add(AggregationExceptionColumns._ID, Tables.AGGREGATION_EXCEPTIONS + "._id")
            .add(AggregationExceptions.TYPE)
            .add(AggregationExceptions.RAW_CONTACT_ID1)
            .add(AggregationExceptions.RAW_CONTACT_ID2)
            .build();

    /** Contains the agg_exceptions columns */
    private static final ProjectionMap sSettingsProjectionMap = ProjectionMap.builder()
            .add(Settings.ACCOUNT_NAME)
            .add(Settings.ACCOUNT_TYPE)
            .add(Settings.UNGROUPED_VISIBLE)
            .add(Settings.SHOULD_SYNC)
            .add(Settings.ANY_UNSYNCED,
                    "(CASE WHEN MIN(" + Settings.SHOULD_SYNC
                        + ",(SELECT "
                                + "(CASE WHEN MIN(" + Groups.SHOULD_SYNC + ") IS NULL"
                                + " THEN 1"
                                + " ELSE MIN(" + Groups.SHOULD_SYNC + ")"
                                + " END)"
                            + " FROM " + Tables.GROUPS
                            + " WHERE " + GroupsColumns.CONCRETE_ACCOUNT_NAME + "="
                                    + SettingsColumns.CONCRETE_ACCOUNT_NAME
                                + " AND " + GroupsColumns.CONCRETE_ACCOUNT_TYPE + "="
                                    + SettingsColumns.CONCRETE_ACCOUNT_TYPE + "))=0"
                    + " THEN 1"
                    + " ELSE 0"
                    + " END)")
            .add(Settings.UNGROUPED_COUNT,
                    "(SELECT COUNT(*)"
                    + " FROM (SELECT 1"
                            + " FROM " + Tables.SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS
                            + " GROUP BY " + Clauses.GROUP_BY_ACCOUNT_CONTACT_ID
                            + " HAVING " + Clauses.HAVING_NO_GROUPS
                    + "))")
            .add(Settings.UNGROUPED_WITH_PHONES,
                    "(SELECT COUNT(*)"
                    + " FROM (SELECT 1"
                            + " FROM " + Tables.SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS
                            + " WHERE " + Contacts.HAS_PHONE_NUMBER
                            + " GROUP BY " + Clauses.GROUP_BY_ACCOUNT_CONTACT_ID
                            + " HAVING " + Clauses.HAVING_NO_GROUPS
                    + "))")
            .build();

    /** Contains StatusUpdates columns */
    private static final ProjectionMap sStatusUpdatesProjectionMap = ProjectionMap.builder()
            .add(PresenceColumns.RAW_CONTACT_ID)
            .add(StatusUpdates.DATA_ID, DataColumns.CONCRETE_ID)
            .add(StatusUpdates.IM_ACCOUNT)
            .add(StatusUpdates.IM_HANDLE)
            .add(StatusUpdates.PROTOCOL)
            // We cannot allow a null in the custom protocol field, because SQLite3 does not
            // properly enforce uniqueness of null values
            .add(StatusUpdates.CUSTOM_PROTOCOL,
                    "(CASE WHEN " + StatusUpdates.CUSTOM_PROTOCOL + "=''"
                    + " THEN NULL"
                    + " ELSE " + StatusUpdates.CUSTOM_PROTOCOL + " END)")
            .add(StatusUpdates.PRESENCE)
            .add(StatusUpdates.CHAT_CAPABILITY)
            .add(StatusUpdates.STATUS)
            .add(StatusUpdates.STATUS_TIMESTAMP)
            .add(StatusUpdates.STATUS_RES_PACKAGE)
            .add(StatusUpdates.STATUS_ICON)
            .add(StatusUpdates.STATUS_LABEL)
            .build();

    /** Contains Live Folders columns */
    private static final ProjectionMap sLiveFoldersProjectionMap = ProjectionMap.builder()
            .add(LiveFolders._ID, Contacts._ID)
            .add(LiveFolders.NAME, Contacts.DISPLAY_NAME)
            // TODO: Put contact photo back when we have a way to display a default icon
            // for contacts without a photo
            // .add(LiveFolders.ICON_BITMAP, Photos.DATA)
            .build();

    /** Contains {@link Directory} columns */
    private static final ProjectionMap sDirectoryProjectionMap = ProjectionMap.builder()
            .add(Directory._ID)
            .add(Directory.PACKAGE_NAME)
            .add(Directory.TYPE_RESOURCE_ID)
            .add(Directory.DISPLAY_NAME)
            .add(Directory.DIRECTORY_AUTHORITY)
            .add(Directory.ACCOUNT_TYPE)
            .add(Directory.ACCOUNT_NAME)
            .add(Directory.EXPORT_SUPPORT)
            .add(Directory.SHORTCUT_SUPPORT)
            .add(Directory.PHOTO_SUPPORT)
            .build();

    // where clause to update the status_updates table
    private static final String WHERE_CLAUSE_FOR_STATUS_UPDATES_TABLE =
            StatusUpdatesColumns.DATA_ID + " IN (SELECT Distinct " + StatusUpdates.DATA_ID +
            " FROM " + Tables.STATUS_UPDATES + " LEFT OUTER JOIN " + Tables.PRESENCE +
            " ON " + StatusUpdatesColumns.DATA_ID + " = " + StatusUpdates.DATA_ID + " WHERE ";

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * Notification ID for failure to import contacts.
     */
    private static final int LEGACY_IMPORT_FAILED_NOTIFICATION = 1;

    private static final String DEFAULT_SNIPPET_ARG_START_MATCH = "[";
    private static final String DEFAULT_SNIPPET_ARG_END_MATCH = "]";
    private static final String DEFAULT_SNIPPET_ARG_ELLIPSIS = "...";
    private static final int DEFAULT_SNIPPET_ARG_MAX_TOKENS = -10;

    private boolean sIsPhoneInitialized;
    private boolean sIsPhone;

    private StringBuilder mSb = new StringBuilder();
    private String[] mSelectionArgs1 = new String[1];
    private String[] mSelectionArgs2 = new String[2];
    private ArrayList<String> mSelectionArgs = Lists.newArrayList();

    private Account mAccount;

    /**
     * Stores mapping from type Strings exposed via {@link DataUsageFeedback} to
     * type integers in {@link DataUsageStatColumns}.
     */
    private static final Map<String, Integer> sDataUsageTypeMap;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_ID_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/entities", CONTACTS_ID_ENTITIES);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/suggestions",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/suggestions/*",
                AGGREGATION_SUGGESTIONS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/photo", CONTACTS_ID_PHOTO);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter", CONTACTS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter/*", CONTACTS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*", CONTACTS_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/data", CONTACTS_LOOKUP_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/#", CONTACTS_LOOKUP_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/#/data",
                CONTACTS_LOOKUP_ID_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/entities",
                CONTACTS_LOOKUP_ENTITIES);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/lookup/*/#/entities",
                CONTACTS_LOOKUP_ID_ENTITIES);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/as_vcard/*", CONTACTS_AS_VCARD);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/as_multi_vcard/*",
                CONTACTS_AS_MULTI_VCARD);
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
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/lookup", EMAILS_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/lookup/*", EMAILS_LOOKUP);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/filter", EMAILS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/emails/filter/*", EMAILS_FILTER);
        matcher.addURI(ContactsContract.AUTHORITY, "data/postals", POSTALS);
        matcher.addURI(ContactsContract.AUTHORITY, "data/postals/#", POSTALS_ID);
        /** "*" is in CSV form with data ids ("123,456,789") */
        matcher.addURI(ContactsContract.AUTHORITY, "data/usagefeedback/*", DATA_USAGE_FEEDBACK_ID);

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
        matcher.addURI(ContactsContract.AUTHORITY, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/*",
                SEARCH_SHORTCUT);

        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts",
                LIVE_FOLDERS_CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts/*",
                LIVE_FOLDERS_CONTACTS_GROUP_NAME);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/contacts_with_phones",
                LIVE_FOLDERS_CONTACTS_WITH_PHONES);
        matcher.addURI(ContactsContract.AUTHORITY, "live_folders/favorites",
                LIVE_FOLDERS_CONTACTS_FAVORITES);

        matcher.addURI(ContactsContract.AUTHORITY, "provider_status", PROVIDER_STATUS);

        matcher.addURI(ContactsContract.AUTHORITY, "directories", DIRECTORIES);
        matcher.addURI(ContactsContract.AUTHORITY, "directories/#", DIRECTORIES_ID);

        matcher.addURI(ContactsContract.AUTHORITY, "complete_name", COMPLETE_NAME);

        matcher.addURI(ContactsContract.AUTHORITY, "profile", PROFILE);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/entities", PROFILE_ENTITIES);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/data", PROFILE_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/data/#", PROFILE_DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/as_vcard", PROFILE_AS_VCARD);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/raw_contacts", PROFILE_RAW_CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/raw_contacts/#",
                PROFILE_RAW_CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/raw_contacts/#/data",
                PROFILE_RAW_CONTACTS_ID_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "profile/raw_contacts/#/entity",
                PROFILE_RAW_CONTACTS_ID_ENTITIES);

        HashMap<String, Integer> tmpTypeMap = new HashMap<String, Integer>();
        tmpTypeMap.put(DataUsageFeedback.USAGE_TYPE_CALL, DataUsageStatColumns.USAGE_TYPE_INT_CALL);
        tmpTypeMap.put(DataUsageFeedback.USAGE_TYPE_LONG_TEXT,
                DataUsageStatColumns.USAGE_TYPE_INT_LONG_TEXT);
        tmpTypeMap.put(DataUsageFeedback.USAGE_TYPE_SHORT_TEXT,
                DataUsageStatColumns.USAGE_TYPE_INT_SHORT_TEXT);
        sDataUsageTypeMap = Collections.unmodifiableMap(tmpTypeMap);
    }

    private static class DirectoryInfo {
        String authority;
        String accountName;
        String accountType;
    }

    /**
     * Cached information about contact directories.
     */
    private HashMap<String, DirectoryInfo> mDirectoryCache = new HashMap<String, DirectoryInfo>();
    private boolean mDirectoryCacheValid = false;

    /**
     * An entry in group id cache. It maps the combination of (account type, account name
     * and source id) to group row id.
     */
    public static class GroupIdCacheEntry {
        String accountType;
        String accountName;
        String sourceId;
        long groupId;
    }

    // We don't need a soft cache for groups - the assumption is that there will only
    // be a small number of contact groups. The cache is keyed off source id.  The value
    // is a list of groups with this group id.
    private HashMap<String, ArrayList<GroupIdCacheEntry>> mGroupIdCache = Maps.newHashMap();

    /**
     * Cached information about the contact ID and raw contact IDs that make up the user's
     * profile entry.
     */
    private static class ProfileIdCache {
        boolean inited;
        long profileContactId;
        Set<Long> profileRawContactIds = Sets.newHashSet();
        Set<Long> profileDataIds = Sets.newHashSet();

        /**
         * Initializes the cache of profile contact and raw contact IDs.  Does nothing if
         * the cache is already initialized (unless forceRefresh is set to true).
         * @param db The contacts database.
         * @param forceRefresh Whether to force re-initialization of the cache.
         */
        private void init(SQLiteDatabase db, boolean forceRefresh) {
            if (!inited || forceRefresh) {
                profileContactId = 0;
                profileRawContactIds.clear();
                profileDataIds.clear();
                Cursor c = db.rawQuery("SELECT " +
                        RawContactsColumns.CONCRETE_CONTACT_ID + "," +
                        RawContactsColumns.CONCRETE_ID + "," +
                        DataColumns.CONCRETE_ID +
                        " FROM " + Tables.RAW_CONTACTS + " JOIN " + Tables.ACCOUNTS + " ON " +
                        RawContactsColumns.CONCRETE_ID + "=" +
                        AccountsColumns.PROFILE_RAW_CONTACT_ID +
                        " JOIN " + Tables.DATA + " ON " +
                        RawContactsColumns.CONCRETE_ID + "=" + DataColumns.CONCRETE_RAW_CONTACT_ID,
                        null);
                try {
                    while (c.moveToNext()) {
                        if (profileContactId == 0) {
                            profileContactId = c.getLong(0);
                        }
                        profileRawContactIds.add(c.getLong(1));
                        profileDataIds.add(c.getLong(2));
                    }
                } finally {
                    c.close();
                }
            }
        }
    }

    private ProfileIdCache mProfileIdCache;

    private HashMap<String, DataRowHandler> mDataRowHandlers;
    private ContactsDatabaseHelper mDbHelper;

    private NameSplitter mNameSplitter;
    private NameLookupBuilder mNameLookupBuilder;

    private PostalSplitter mPostalSplitter;

    private ContactDirectoryManager mContactDirectoryManager;
    private ContactAggregator mContactAggregator;
    private LegacyApiSupport mLegacyApiSupport;
    private GlobalSearchSupport mGlobalSearchSupport;
    private CommonNicknameCache mCommonNicknameCache;
    private SearchIndexManager mSearchIndexManager;

    private ContentValues mValues = new ContentValues();
    private HashMap<String, Boolean> mAccountWritability = Maps.newHashMap();

    private int mProviderStatus = ProviderStatus.STATUS_NORMAL;
    private boolean mProviderStatusUpdateNeeded;
    private long mEstimatedStorageRequirement = 0;
    private volatile CountDownLatch mReadAccessLatch;
    private volatile CountDownLatch mWriteAccessLatch;
    private boolean mAccountUpdateListenerRegistered;
    private boolean mOkToOpenAccess = true;

    private TransactionContext mTransactionContext = new TransactionContext();

    private boolean mVisibleTouched = false;

    private boolean mSyncToNetwork;

    private Locale mCurrentLocale;
    private int mContactsAccountCount;

    private HandlerThread mBackgroundThread;
    private Handler mBackgroundHandler;

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
        StrictMode.setThreadPolicy(
                new StrictMode.ThreadPolicy.Builder().detectAll().penaltyLog().build());

        mProfileIdCache = new ProfileIdCache();
        mDbHelper = (ContactsDatabaseHelper)getDatabaseHelper();
        mContactDirectoryManager = new ContactDirectoryManager(this);
        mGlobalSearchSupport = new GlobalSearchSupport(this);

        // The provider is closed for business until fully initialized
        mReadAccessLatch = new CountDownLatch(1);
        mWriteAccessLatch = new CountDownLatch(1);

        mBackgroundThread = new HandlerThread("ContactsProviderWorker",
                Process.THREAD_PRIORITY_BACKGROUND);
        mBackgroundThread.start();
        mBackgroundHandler = new Handler(mBackgroundThread.getLooper()) {
            @Override
            public void handleMessage(Message msg) {
                performBackgroundTask(msg.what, msg.obj);
            }
        };

        scheduleBackgroundTask(BACKGROUND_TASK_INITIALIZE);
        scheduleBackgroundTask(BACKGROUND_TASK_IMPORT_LEGACY_CONTACTS);
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_ACCOUNTS);
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_LOCALE);
        scheduleBackgroundTask(BACKGROUND_TASK_UPGRADE_AGGREGATION_ALGORITHM);
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_SEARCH_INDEX);
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_PROVIDER_STATUS);
        scheduleBackgroundTask(BACKGROUND_TASK_OPEN_WRITE_ACCESS);

        return true;
    }

    /**
     * (Re)allocates all locale-sensitive structures.
     */
    private void initForDefaultLocale() {
        Context context = getContext();
        mLegacyApiSupport = new LegacyApiSupport(context, mDbHelper, this, mGlobalSearchSupport);
        mCurrentLocale = getLocale();
        mNameSplitter = mDbHelper.createNameSplitter();
        mNameLookupBuilder = new StructuredNameLookupBuilder(mNameSplitter);
        mPostalSplitter = new PostalSplitter(mCurrentLocale);
        mCommonNicknameCache = new CommonNicknameCache(mDbHelper.getReadableDatabase());
        ContactLocaleUtils.getIntance().setLocale(mCurrentLocale);
        mContactAggregator = new ContactAggregator(this, mDbHelper,
                createPhotoPriorityResolver(context), mNameSplitter, mCommonNicknameCache);
        mContactAggregator.setEnabled(SystemProperties.getBoolean(AGGREGATE_CONTACTS, true));
        mSearchIndexManager = new SearchIndexManager(this);

        mDataRowHandlers = new HashMap<String, DataRowHandler>();

        mDataRowHandlers.put(Email.CONTENT_ITEM_TYPE,
                new DataRowHandlerForEmail(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(Im.CONTENT_ITEM_TYPE,
                new DataRowHandlerForIm(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(Organization.CONTENT_ITEM_TYPE,
                new DataRowHandlerForOrganization(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(Phone.CONTENT_ITEM_TYPE,
                new DataRowHandlerForPhoneNumber(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(Nickname.CONTENT_ITEM_TYPE,
                new DataRowHandlerForNickname(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(StructuredName.CONTENT_ITEM_TYPE,
                new DataRowHandlerForStructuredName(context, mDbHelper, mContactAggregator,
                        mNameSplitter, mNameLookupBuilder));
        mDataRowHandlers.put(StructuredPostal.CONTENT_ITEM_TYPE,
                new DataRowHandlerForStructuredPostal(context, mDbHelper, mContactAggregator,
                        mPostalSplitter));
        mDataRowHandlers.put(GroupMembership.CONTENT_ITEM_TYPE,
                new DataRowHandlerForGroupMembership(context, mDbHelper, mContactAggregator,
                        mGroupIdCache));
        mDataRowHandlers.put(Photo.CONTENT_ITEM_TYPE,
                new DataRowHandlerForPhoto(context, mDbHelper, mContactAggregator));
        mDataRowHandlers.put(Note.CONTENT_ITEM_TYPE,
                new DataRowHandlerForNote(context, mDbHelper, mContactAggregator));
    }

    /**
     * Visible for testing.
     */
    /* package */ PhotoPriorityResolver createPhotoPriorityResolver(Context context) {
        return new PhotoPriorityResolver(context);
    }

    protected void scheduleBackgroundTask(int task) {
        mBackgroundHandler.sendEmptyMessage(task);
    }

    protected void scheduleBackgroundTask(int task, Object arg) {
        mBackgroundHandler.sendMessage(mBackgroundHandler.obtainMessage(task, arg));
    }

    protected void performBackgroundTask(int task, Object arg) {
        switch (task) {
            case BACKGROUND_TASK_INITIALIZE: {
                initForDefaultLocale();
                mReadAccessLatch.countDown();
                mReadAccessLatch = null;
                break;
            }

            case BACKGROUND_TASK_OPEN_WRITE_ACCESS: {
                if (mOkToOpenAccess) {
                    mWriteAccessLatch.countDown();
                    mWriteAccessLatch = null;
                }
                break;
            }

            case BACKGROUND_TASK_IMPORT_LEGACY_CONTACTS: {
                if (isLegacyContactImportNeeded()) {
                    importLegacyContactsInBackground();
                }
                break;
            }

            case BACKGROUND_TASK_UPDATE_ACCOUNTS: {
                Context context = getContext();
                if (!mAccountUpdateListenerRegistered) {
                    AccountManager.get(context).addOnAccountsUpdatedListener(this, null, false);
                    mAccountUpdateListenerRegistered = true;
                }

                Account[] accounts = AccountManager.get(context).getAccounts();
                boolean accountsChanged = updateAccountsInBackground(accounts);
                updateContactsAccountCount(accounts);
                updateDirectoriesInBackground(accountsChanged);
                break;
            }

            case BACKGROUND_TASK_UPDATE_LOCALE: {
                updateLocaleInBackground();
                break;
            }

            case BACKGROUND_TASK_CHANGE_LOCALE: {
                changeLocaleInBackground();
                break;
            }

            case BACKGROUND_TASK_UPGRADE_AGGREGATION_ALGORITHM: {
                if (isAggregationUpgradeNeeded()) {
                    upgradeAggregationAlgorithmInBackground();
                }
                break;
            }

            case BACKGROUND_TASK_UPDATE_SEARCH_INDEX: {
                updateSearchIndexInBackground();
                break;
            }

            case BACKGROUND_TASK_UPDATE_PROVIDER_STATUS: {
                updateProviderStatus();
                break;
            }

            case BACKGROUND_TASK_UPDATE_DIRECTORIES: {
                if (arg != null) {
                    mContactDirectoryManager.onPackageChanged((String) arg);
                }
                break;
            }
        }
    }

    public void onLocaleChanged() {
        if (mProviderStatus != ProviderStatus.STATUS_NORMAL
                && mProviderStatus != ProviderStatus.STATUS_NO_ACCOUNTS_NO_CONTACTS) {
            return;
        }

        scheduleBackgroundTask(BACKGROUND_TASK_CHANGE_LOCALE);
    }

    /**
     * Verifies that the contacts database is properly configured for the current locale.
     * If not, changes the database locale to the current locale using an asynchronous task.
     * This needs to be done asynchronously because the process involves rebuilding
     * large data structures (name lookup, sort keys), which can take minutes on
     * a large set of contacts.
     */
    protected void updateLocaleInBackground() {

        // The process is already running - postpone the change
        if (mProviderStatus == ProviderStatus.STATUS_CHANGING_LOCALE) {
            return;
        }

        final SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
        final String providerLocale = prefs.getString(PREF_LOCALE, null);
        final Locale currentLocale = mCurrentLocale;
        if (currentLocale.toString().equals(providerLocale)) {
            return;
        }

        int providerStatus = mProviderStatus;
        setProviderStatus(ProviderStatus.STATUS_CHANGING_LOCALE);
        mDbHelper.setLocale(this, currentLocale);
        prefs.edit().putString(PREF_LOCALE, currentLocale.toString()).apply();
        setProviderStatus(providerStatus);
    }

    /**
     * Reinitializes the provider for a new locale.
     */
    private void changeLocaleInBackground() {
        // Re-initializing the provider without stopping it.
        // Locking the database will prevent inserts/updates/deletes from
        // running at the same time, but queries may still be running
        // on other threads. Those queries may return inconsistent results.
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            initForDefaultLocale();
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        updateLocaleInBackground();
    }

    protected void updateSearchIndexInBackground() {
        mSearchIndexManager.updateIndex();
    }

    protected void updateDirectoriesInBackground(boolean rescan) {
        mContactDirectoryManager.scanAllPackages(rescan);
    }

    private void updateProviderStatus() {
        if (mProviderStatus != ProviderStatus.STATUS_NORMAL
                && mProviderStatus != ProviderStatus.STATUS_NO_ACCOUNTS_NO_CONTACTS) {
            return;
        }

        if (mContactsAccountCount == 0
                && DatabaseUtils.queryNumEntries(mDbHelper.getReadableDatabase(),
                        Tables.CONTACTS, null) == 0) {
            setProviderStatus(ProviderStatus.STATUS_NO_ACCOUNTS_NO_CONTACTS);
        } else {
            setProviderStatus(ProviderStatus.STATUS_NORMAL);
        }
    }

    /* Visible for testing */
    @Override
    protected ContactsDatabaseHelper getDatabaseHelper(final Context context) {
        return ContactsDatabaseHelper.getInstance(context);
    }

    /* package */ NameSplitter getNameSplitter() {
        return mNameSplitter;
    }

    /* package */ NameLookupBuilder getNameLookupBuilder() {
        return mNameLookupBuilder;
    }

    /* Visible for testing */
    public ContactDirectoryManager getContactDirectoryManagerForTest() {
        return mContactDirectoryManager;
    }

    /* Visible for testing */
    protected Locale getLocale() {
        return Locale.getDefault();
    }

    protected boolean isLegacyContactImportNeeded() {
        int version = Integer.parseInt(mDbHelper.getProperty(PROPERTY_CONTACTS_IMPORTED, "0"));
        return version < PROPERTY_CONTACTS_IMPORT_VERSION;
    }

    protected LegacyContactImporter getLegacyContactImporter() {
        return new LegacyContactImporter(getContext(), this);
    }

    /**
     * Imports legacy contacts as a background task.
     */
    private void importLegacyContactsInBackground() {
        Log.v(TAG, "Importing legacy contacts");
        setProviderStatus(ProviderStatus.STATUS_UPGRADING);

        final SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getContext());
        mDbHelper.setLocale(this, mCurrentLocale);
        prefs.edit().putString(PREF_LOCALE, mCurrentLocale.toString()).commit();

        LegacyContactImporter importer = getLegacyContactImporter();
        if (importLegacyContacts(importer)) {
            onLegacyContactImportSuccess();
        } else {
            onLegacyContactImportFailure();
        }
    }

    /**
     * Unlocks the provider and declares that the import process is complete.
     */
    private void onLegacyContactImportSuccess() {
        NotificationManager nm =
            (NotificationManager)getContext().getSystemService(Context.NOTIFICATION_SERVICE);
        nm.cancel(LEGACY_IMPORT_FAILED_NOTIFICATION);

        // Store a property in the database indicating that the conversion process succeeded
        mDbHelper.setProperty(PROPERTY_CONTACTS_IMPORTED,
                String.valueOf(PROPERTY_CONTACTS_IMPORT_VERSION));
        setProviderStatus(ProviderStatus.STATUS_NORMAL);
        Log.v(TAG, "Completed import of legacy contacts");
    }

    /**
     * Announces the provider status and keeps the provider locked.
     */
    private void onLegacyContactImportFailure() {
        Context context = getContext();
        NotificationManager nm =
            (NotificationManager)context.getSystemService(Context.NOTIFICATION_SERVICE);

        // Show a notification
        Notification n = new Notification(android.R.drawable.stat_notify_error,
                context.getString(R.string.upgrade_out_of_memory_notification_ticker),
                System.currentTimeMillis());
        n.setLatestEventInfo(context,
                context.getString(R.string.upgrade_out_of_memory_notification_title),
                context.getString(R.string.upgrade_out_of_memory_notification_text),
                PendingIntent.getActivity(context, 0, new Intent(Intents.UI.LIST_DEFAULT), 0));
        n.flags |= Notification.FLAG_NO_CLEAR | Notification.FLAG_ONGOING_EVENT;

        nm.notify(LEGACY_IMPORT_FAILED_NOTIFICATION, n);

        setProviderStatus(ProviderStatus.STATUS_UPGRADE_OUT_OF_MEMORY);
        Log.v(TAG, "Failed to import legacy contacts");

        // Do not let any database changes until this issue is resolved.
        mOkToOpenAccess = false;
    }

    /* Visible for testing */
    /* package */ boolean importLegacyContacts(LegacyContactImporter importer) {
        boolean aggregatorEnabled = mContactAggregator.isEnabled();
        mContactAggregator.setEnabled(false);
        try {
            if (importer.importContacts()) {

                // TODO aggregate all newly added raw contacts
                mContactAggregator.setEnabled(aggregatorEnabled);
                return true;
            }
        } catch (Throwable e) {
           Log.e(TAG, "Legacy contact import failed", e);
        }
        mEstimatedStorageRequirement = importer.getEstimatedStorageRequirement();
        return false;
    }

    /**
     * Wipes all data from the contacts database.
     */
    /* package */ void wipeData() {
        mDbHelper.wipeData();
        mProviderStatus = ProviderStatus.STATUS_NO_ACCOUNTS_NO_CONTACTS;
    }

    /**
     * During intialization, this content provider will
     * block all attempts to change contacts data. In particular, it will hold
     * up all contact syncs. As soon as the import process is complete, all
     * processes waiting to write to the provider are unblocked and can proceed
     * to compete for the database transaction monitor.
     */
    private void waitForAccess(CountDownLatch latch) {
        if (latch == null) {
            return;
        }

        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        waitForAccess(mWriteAccessLatch);
        return super.insert(uri, values);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        if (mWriteAccessLatch != null) {
            // We are stuck trying to upgrade contacts db.  The only update request
            // allowed in this case is an update of provider status, which will trigger
            // an attempt to upgrade contacts again.
            int match = sUriMatcher.match(uri);
            if (match == PROVIDER_STATUS) {
                Integer newStatus = values.getAsInteger(ProviderStatus.STATUS);
                if (newStatus != null && newStatus == ProviderStatus.STATUS_UPGRADING) {
                    scheduleBackgroundTask(BACKGROUND_TASK_IMPORT_LEGACY_CONTACTS);
                    return 1;
                } else {
                    return 0;
                }
            }
        }
        waitForAccess(mWriteAccessLatch);
        return super.update(uri, values, selection, selectionArgs);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        waitForAccess(mWriteAccessLatch);
        return super.delete(uri, selection, selectionArgs);
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        waitForAccess(mWriteAccessLatch);
        return super.applyBatch(operations);
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values) {
        waitForAccess(mWriteAccessLatch);
        return super.bulkInsert(uri, values);
    }

    @Override
    protected void onBeginTransaction() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "onBeginTransaction");
        }
        super.onBeginTransaction();
        mContactAggregator.clearPendingAggregations();
        mTransactionContext.clear();
    }


    @Override
    protected void beforeTransactionCommit() {

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "beforeTransactionCommit");
        }
        super.beforeTransactionCommit();
        flushTransactionalChanges();
        mContactAggregator.aggregateInTransaction(mTransactionContext, mDb);
        if (mVisibleTouched) {
            mVisibleTouched = false;
            mDbHelper.updateAllVisible();
        }

        updateSearchIndexInTransaction();

        if (mProviderStatusUpdateNeeded) {
            updateProviderStatus();
            mProviderStatusUpdateNeeded = false;
        }
    }

    private void updateSearchIndexInTransaction() {
        Set<Long> staleContacts = mTransactionContext.getStaleSearchIndexContactIds();
        Set<Long> staleRawContacts = mTransactionContext.getStaleSearchIndexRawContactIds();
        if (!staleContacts.isEmpty() || !staleRawContacts.isEmpty()) {
            mSearchIndexManager.updateIndexForRawContacts(staleContacts, staleRawContacts);
            mTransactionContext.clearSearchIndexUpdates();
        }
    }

    private void flushTransactionalChanges() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "flushTransactionChanges");
        }

        // Determine whether we need to refresh the profile ID cache.
        boolean profileCacheRefreshNeeded = false;

        for (long rawContactId : mTransactionContext.getInsertedRawContactIds()) {
            mDbHelper.updateRawContactDisplayName(mDb, rawContactId);
            mContactAggregator.onRawContactInsert(mTransactionContext, mDb, rawContactId);
        }

        Map<Long, Account> insertedProfileRawContactAccountMap =
                mTransactionContext.getInsertedProfileRawContactIds();
        if (!insertedProfileRawContactAccountMap.isEmpty()) {
            for (long profileRawContactId : insertedProfileRawContactAccountMap.keySet()) {
                mDbHelper.updateRawContactDisplayName(mDb, profileRawContactId);
                mContactAggregator.onProfileRawContactInsert(mTransactionContext, mDb,
                        profileRawContactId,
                        insertedProfileRawContactAccountMap.get(profileRawContactId));
            }
            profileCacheRefreshNeeded = true;
        }

        Set<Long> dirtyRawContacts = mTransactionContext.getDirtyRawContactIds();
        if (!dirtyRawContacts.isEmpty()) {
            mSb.setLength(0);
            mSb.append(UPDATE_RAW_CONTACT_SET_DIRTY_SQL);
            appendIds(mSb, dirtyRawContacts);
            mSb.append(")");
            mDb.execSQL(mSb.toString());

            profileCacheRefreshNeeded = profileCacheRefreshNeeded ||
                    !Collections.disjoint(mProfileIdCache.profileRawContactIds, dirtyRawContacts);
        }

        Set<Long> updatedRawContacts = mTransactionContext.getUpdatedRawContactIds();
        if (!updatedRawContacts.isEmpty()) {
            mSb.setLength(0);
            mSb.append(UPDATE_RAW_CONTACT_SET_VERSION_SQL);
            appendIds(mSb, updatedRawContacts);
            mSb.append(")");
            mDb.execSQL(mSb.toString());

            profileCacheRefreshNeeded = profileCacheRefreshNeeded ||
                    !Collections.disjoint(mProfileIdCache.profileRawContactIds, updatedRawContacts);
        }

        for (Map.Entry<Long, Object> entry : mTransactionContext.getUpdatedSyncStates()) {
            long id = entry.getKey();
            if (mDbHelper.getSyncState().update(mDb, id, entry.getValue()) <= 0) {
                throw new IllegalStateException(
                        "unable to update sync state, does it still exist?");
            }
        }

        if (profileCacheRefreshNeeded) {
            // Force the profile ID cache to refresh.
            mProfileIdCache.init(mDb, true);
        }

        mTransactionContext.clear();
    }

    /**
     * Appends comma separated ids.
     * @param ids Should not be empty
     */
    private void appendIds(StringBuilder sb, Set<Long> ids) {
        for (long id : ids) {
            sb.append(id).append(',');
        }

        sb.setLength(sb.length() - 1); // Yank the last comma
    }

    /**
     * Checks whether the given contact ID represents the user's personal profile - if it is, calls
     * a permission check (for writing the profile if forWrite is true, for reading the profile
     * otherwise).  If the contact ID is not the user's profile, no check is executed.
     * @param contactId The contact ID to be checked.
     * @param forWrite Whether the caller is attempting to do a write (vs. read) operation.
     */
    private void enforceProfilePermissionForContact(long contactId, boolean forWrite) {
        mProfileIdCache.init(mDb, false);
        if (mProfileIdCache.profileContactId == contactId) {
            enforceProfilePermission(forWrite);
        }
    }

    /**
     * Checks whether the given raw contact ID is a member of the user's personal profile - if it
     * is, calls a permission check (for writing the profile if forWrite is true, for reading the
     * profile otherwise).  If the raw contact ID is not in the user's profile, no check is
     * executed.
     * @param rawContactId The raw contact ID to be checked.
     * @param forWrite Whether the caller is attempting to do a write (vs. read) operation.
     */
    private void enforceProfilePermissionForRawContact(long rawContactId, boolean forWrite) {
        mProfileIdCache.init(mDb, false);
        if (mProfileIdCache.profileRawContactIds.contains(rawContactId)) {
            enforceProfilePermission(forWrite);
        }
    }

    /**
     * Checks whether the given data ID is a member of the user's personal profile - if it is,
     * calls a permission check (for writing the profile if forWrite is true, for reading the
     * profile otherwise).  If the data ID is not in the user's profile, no check is executed.
     * @param dataId The data ID to be checked.
     * @param forWrite Whether the caller is attempting to do a write (vs. read) operation.
     */
    private void enforceProfilePermissionForData(long dataId, boolean forWrite) {
        mProfileIdCache.init(mDb, false);
        if (mProfileIdCache.profileDataIds.contains(dataId)) {
            enforceProfilePermission(forWrite);
        }
    }

    /**
     * Performs a permission check for WRITE_PROFILE or READ_PROFILE (depending on the parameter).
     * If the permission check fails, this will throw a SecurityException.
     * @param forWrite Whether the caller is attempting to do a write (vs. read) operation.
     */
    private void enforceProfilePermission(boolean forWrite) {
        String profilePermission = forWrite
                ? "android.permission.WRITE_PROFILE"
                : "android.permission.READ_PROFILE";
        getContext().enforceCallingOrSelfPermission(profilePermission, null);
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

    protected void setProviderStatus(int status) {
        if (mProviderStatus != status) {
            mProviderStatus = status;
            getContext().getContentResolver().notifyChange(ProviderStatus.CONTENT_URI, null, false);
        }
    }

    public DataRowHandler getDataRowHandler(final String mimeType) {
        DataRowHandler handler = mDataRowHandlers.get(mimeType);
        if (handler == null) {
            handler = new DataRowHandlerForCustomMimetype(
                    getContext(), mDbHelper, mContactAggregator, mimeType);
            mDataRowHandlers.put(mimeType, handler);
        }
        return handler;
    }

    @Override
    protected Uri insertInTransaction(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insertInTransaction: " + uri + " " + values);
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

            case PROFILE: {
                throw new UnsupportedOperationException(
                        "The profile contact is created automatically");
            }

            case RAW_CONTACTS: {
                id = insertRawContact(uri, values, callerIsSyncAdapter, false);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case RAW_CONTACTS_DATA: {
                values.put(Data.RAW_CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values, callerIsSyncAdapter);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case PROFILE_RAW_CONTACTS: {
                enforceProfilePermission(true);
                id = insertRawContact(uri, values, callerIsSyncAdapter, true);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case DATA: {
                id = insertData(values, callerIsSyncAdapter);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case GROUPS: {
                id = insertGroup(uri, values, callerIsSyncAdapter);
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
     * If account is non-null then store it in the values. If the account is
     * already specified in the values then it must be consistent with the
     * account, if it is non-null.
     *
     * @param uri Current {@link Uri} being operated on.
     * @param values {@link ContentValues} to read and possibly update.
     * @throws IllegalArgumentException when only one of
     *             {@link RawContacts#ACCOUNT_NAME} or
     *             {@link RawContacts#ACCOUNT_TYPE} is specified, leaving the
     *             other undefined.
     * @throws IllegalArgumentException when {@link RawContacts#ACCOUNT_NAME}
     *             and {@link RawContacts#ACCOUNT_TYPE} are inconsistent between
     *             the given {@link Uri} and {@link ContentValues}.
     */
    private Account resolveAccount(Uri uri, ContentValues values) throws IllegalArgumentException {
        String accountName = getQueryParameter(uri, RawContacts.ACCOUNT_NAME);
        String accountType = getQueryParameter(uri, RawContacts.ACCOUNT_TYPE);
        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);

        String valueAccountName = values.getAsString(RawContacts.ACCOUNT_NAME);
        String valueAccountType = values.getAsString(RawContacts.ACCOUNT_TYPE);
        final boolean partialValues = TextUtils.isEmpty(valueAccountName)
                ^ TextUtils.isEmpty(valueAccountType);

        if (partialUri || partialValues) {
            // Throw when either account is incomplete
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri));
        }

        // Accounts are valid by only checking one parameter, since we've
        // already ruled out partial accounts.
        final boolean validUri = !TextUtils.isEmpty(accountName);
        final boolean validValues = !TextUtils.isEmpty(valueAccountName);

        if (validValues && validUri) {
            // Check that accounts match when both present
            final boolean accountMatch = TextUtils.equals(accountName, valueAccountName)
                    && TextUtils.equals(accountType, valueAccountType);
            if (!accountMatch) {
                throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                        "When both specified, ACCOUNT_NAME and ACCOUNT_TYPE must match", uri));
            }
        } else if (validUri) {
            // Fill values from Uri when not present
            values.put(RawContacts.ACCOUNT_NAME, accountName);
            values.put(RawContacts.ACCOUNT_TYPE, accountType);
        } else if (validValues) {
            accountName = valueAccountName;
            accountType = valueAccountType;
        } else {
            return null;
        }

        // Use cached Account object when matches, otherwise create
        if (mAccount == null
                || !mAccount.name.equals(accountName)
                || !mAccount.type.equals(accountType)) {
            mAccount = new Account(accountName, accountType);
        }

        return mAccount;
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
     * Inserts an item in the raw contacts table
     *
     * @param uri the values for the new row
     * @param values the account this contact should be associated with. may be null.
     * @param callerIsSyncAdapter
     * @param forProfile Whether this raw contact is being inserted into the user's profile.
     * @return the row ID of the newly created row
     */
    private long insertRawContact(Uri uri, ContentValues values, boolean callerIsSyncAdapter,
            boolean forProfile) {
        mValues.clear();
        mValues.putAll(values);
        mValues.putNull(RawContacts.CONTACT_ID);

        final Account account = resolveAccount(uri, mValues);

        if (values.containsKey(RawContacts.DELETED)
                && values.getAsInteger(RawContacts.DELETED) != 0) {
            mValues.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DISABLED);
        }

        long rawContactId = mDb.insert(Tables.RAW_CONTACTS, RawContacts.CONTACT_ID, mValues);
        int aggregationMode = RawContacts.AGGREGATION_MODE_DEFAULT;
        if (forProfile) {
            // Profile raw contacts should never be aggregated by the aggregator; they are always
            // aggregated under a single profile contact.
            aggregationMode = RawContacts.AGGREGATION_MODE_DISABLED;
        } else if (mValues.containsKey(RawContacts.AGGREGATION_MODE)) {
            aggregationMode = mValues.getAsInteger(RawContacts.AGGREGATION_MODE);
        }
        mContactAggregator.markNewForAggregation(rawContactId, aggregationMode);

        if (forProfile) {
            // Trigger creation of the user profile Contact (or association with the existing one)
            // at the end of the transaction.
            mTransactionContext.profileRawContactInserted(rawContactId, account);
        } else {
            // Trigger creation of a Contact based on this RawContact at the end of transaction
            mTransactionContext.rawContactInserted(rawContactId, account);
        }

        if (!callerIsSyncAdapter) {
            addAutoAddMembership(rawContactId);
            final Long starred = values.getAsLong(RawContacts.STARRED);
            if (starred != null && starred != 0) {
                updateFavoritesMembership(rawContactId, starred != 0);
            }
        }

        mProviderStatusUpdateNeeded = true;
        return rawContactId;
    }

    private void addAutoAddMembership(long rawContactId) {
        final Long groupId = findGroupByRawContactId(SELECTION_AUTO_ADD_GROUPS_BY_RAW_CONTACT_ID,
                rawContactId);
        if (groupId != null) {
            insertDataGroupMembership(rawContactId, groupId);
        }
    }

    private Long findGroupByRawContactId(String selection, long rawContactId) {
        Cursor c = mDb.query(Tables.GROUPS + "," + Tables.RAW_CONTACTS, PROJECTION_GROUP_ID,
                selection,
                new String[]{Long.toString(rawContactId)},
                null /* groupBy */, null /* having */, null /* orderBy */);
        try {
            while (c.moveToNext()) {
                return c.getLong(0);
            }
            return null;
        } finally {
            c.close();
        }
    }

    private void updateFavoritesMembership(long rawContactId, boolean isStarred) {
        final Long groupId = findGroupByRawContactId(SELECTION_FAVORITES_GROUPS_BY_RAW_CONTACT_ID,
                rawContactId);
        if (groupId != null) {
            if (isStarred) {
                insertDataGroupMembership(rawContactId, groupId);
            } else {
                deleteDataGroupMembership(rawContactId, groupId);
            }
        }
    }

    private void insertDataGroupMembership(long rawContactId, long groupId) {
        ContentValues groupMembershipValues = new ContentValues();
        groupMembershipValues.put(GroupMembership.GROUP_ROW_ID, groupId);
        groupMembershipValues.put(GroupMembership.RAW_CONTACT_ID, rawContactId);
        groupMembershipValues.put(DataColumns.MIMETYPE_ID,
                mDbHelper.getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE));
        mDb.insert(Tables.DATA, null, groupMembershipValues);
    }

    private void deleteDataGroupMembership(long rawContactId, long groupId) {
        final String[] selectionArgs = {
                Long.toString(mDbHelper.getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE)),
                Long.toString(groupId),
                Long.toString(rawContactId)};
        mDb.delete(Tables.DATA, SELECTION_GROUPMEMBERSHIP_DATA, selectionArgs);
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

        // If the data being inserted belongs to the user's profile entry, check for the
        // WRITE_PROFILE permission before proceeding.
        enforceProfilePermissionForRawContact(rawContactId, true);

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
        id = rowHandler.insert(mDb, mTransactionContext, rawContactId, mValues);
        if (!callerIsSyncAdapter) {
            mTransactionContext.markRawContactDirty(rawContactId);
        }
        mTransactionContext.rawContactUpdated(rawContactId);
        return id;
    }

    public void updateRawContactDisplayName(SQLiteDatabase db, long rawContactId) {
        mDbHelper.updateRawContactDisplayName(db, rawContactId);
    }

    /**
     * Delete data row by row so that fixing of primaries etc work correctly.
     */
    private int deleteData(String selection, String[] selectionArgs, boolean callerIsSyncAdapter) {
        int count = 0;

        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about deleting data we don't have permission to read.
        Cursor c = query(Data.CONTENT_URI, DataRowHandler.DataDeleteQuery.COLUMNS,
                selection, selectionArgs, null);
        try {
            while(c.moveToNext()) {
                long rawContactId = c.getLong(DataRowHandler.DataDeleteQuery.RAW_CONTACT_ID);

                // Check for write profile permission if the data belongs to the profile.
                enforceProfilePermissionForRawContact(rawContactId, true);

                String mimeType = c.getString(DataRowHandler.DataDeleteQuery.MIMETYPE);
                DataRowHandler rowHandler = getDataRowHandler(mimeType);
                count += rowHandler.delete(mDb, mTransactionContext, c);
                if (!callerIsSyncAdapter) {
                    mTransactionContext.markRawContactDirty(rawContactId);
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
        mSelectionArgs1[0] = String.valueOf(dataId);
        Cursor c = query(Data.CONTENT_URI, DataRowHandler.DataDeleteQuery.COLUMNS, Data._ID + "=?",
                mSelectionArgs1, null);

        try {
            if (!c.moveToFirst()) {
                return 0;
            }

            String mimeType = c.getString(DataRowHandler.DataDeleteQuery.MIMETYPE);
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

            // Check for write profile permission if the data belongs to the profile.
            long rawContactId = c.getLong(DataRowHandler.DataDeleteQuery.RAW_CONTACT_ID);
            enforceProfilePermissionForRawContact(rawContactId, true);

            DataRowHandler rowHandler = getDataRowHandler(mimeType);
            return rowHandler.delete(mDb, mTransactionContext, c);
        } finally {
            c.close();
        }
    }

    /**
     * Inserts an item in the groups table
     */
    private long insertGroup(Uri uri, ContentValues values, boolean callerIsSyncAdapter) {
        mValues.clear();
        mValues.putAll(values);

        final Account account = resolveAccount(uri, mValues);

        // Replace package with internal mapping
        final String packageName = mValues.getAsString(Groups.RES_PACKAGE);
        if (packageName != null) {
            mValues.put(GroupsColumns.PACKAGE_ID, mDbHelper.getPackageId(packageName));
        }
        mValues.remove(Groups.RES_PACKAGE);

        final boolean isFavoritesGroup = mValues.getAsLong(Groups.FAVORITES) != null
                ? mValues.getAsLong(Groups.FAVORITES) != 0
                : false;

        if (!callerIsSyncAdapter) {
            mValues.put(Groups.DIRTY, 1);
        }

        long result = mDb.insert(Tables.GROUPS, Groups.TITLE, mValues);

        if (!callerIsSyncAdapter && isFavoritesGroup) {
            // add all starred raw contacts to this group
            String selection;
            String[] selectionArgs;
            if (account == null) {
                selection = RawContacts.ACCOUNT_NAME + " IS NULL AND "
                        + RawContacts.ACCOUNT_TYPE + " IS NULL";
                selectionArgs = null;
            } else {
                selection = RawContacts.ACCOUNT_NAME + "=? AND "
                        + RawContacts.ACCOUNT_TYPE + "=?";
                selectionArgs = new String[]{account.name, account.type};
            }
            Cursor c = mDb.query(Tables.RAW_CONTACTS,
                    new String[]{RawContacts._ID, RawContacts.STARRED},
                    selection, selectionArgs, null, null, null);
            try {
                while (c.moveToNext()) {
                    if (c.getLong(1) != 0) {
                        final long rawContactId = c.getLong(0);
                        insertDataGroupMembership(rawContactId, result);
                        mTransactionContext.markRawContactDirty(rawContactId);
                    }
                }
            } finally {
                c.close();
            }
        }

        if (mValues.containsKey(Groups.GROUP_VISIBLE)) {
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
        mSelectionArgs.clear();
        if (dataId != null) {
            // Lookup the contact info for the given data row.

            mSb.append(Tables.DATA + "." + Data._ID + "=?");
            mSelectionArgs.add(String.valueOf(dataId));
        } else {
            // Lookup the data row to attach this presence update to

            if (TextUtils.isEmpty(handle) || protocol == null) {
                throw new IllegalArgumentException("PROTOCOL and IM_HANDLE are required");
            }

            // TODO: generalize to allow other providers to match against email
            boolean matchEmail = Im.PROTOCOL_GOOGLE_TALK == protocol;

            String mimeTypeIdIm = String.valueOf(mDbHelper.getMimeTypeIdForIm());
            if (matchEmail) {
                String mimeTypeIdEmail = String.valueOf(mDbHelper.getMimeTypeIdForEmail());

                // The following hack forces SQLite to use the (mimetype_id,data1) index, otherwise
                // the "OR" conjunction confuses it and it switches to a full scan of
                // the raw_contacts table.

                // This code relies on the fact that Im.DATA and Email.DATA are in fact the same
                // column - Data.DATA1
                mSb.append(DataColumns.MIMETYPE_ID + " IN (?,?)" +
                        " AND " + Data.DATA1 + "=?" +
                        " AND ((" + DataColumns.MIMETYPE_ID + "=? AND " + Im.PROTOCOL + "=?");
                mSelectionArgs.add(mimeTypeIdEmail);
                mSelectionArgs.add(mimeTypeIdIm);
                mSelectionArgs.add(handle);
                mSelectionArgs.add(mimeTypeIdIm);
                mSelectionArgs.add(String.valueOf(protocol));
                if (customProtocol != null) {
                    mSb.append(" AND " + Im.CUSTOM_PROTOCOL + "=?");
                    mSelectionArgs.add(customProtocol);
                }
                mSb.append(") OR (" + DataColumns.MIMETYPE_ID + "=?))");
                mSelectionArgs.add(mimeTypeIdEmail);
            } else {
                mSb.append(DataColumns.MIMETYPE_ID + "=?" +
                        " AND " + Im.PROTOCOL + "=?" +
                        " AND " + Im.DATA + "=?");
                mSelectionArgs.add(mimeTypeIdIm);
                mSelectionArgs.add(String.valueOf(protocol));
                mSelectionArgs.add(handle);
                if (customProtocol != null) {
                    mSb.append(" AND " + Im.CUSTOM_PROTOCOL + "=?");
                    mSelectionArgs.add(customProtocol);
                }
            }

            if (values.containsKey(StatusUpdates.DATA_ID)) {
                mSb.append(" AND " + DataColumns.CONCRETE_ID + "=?");
                mSelectionArgs.add(values.getAsString(StatusUpdates.DATA_ID));
            }
        }
        mSb.append(" AND ").append(getContactsRestrictions());

        Cursor cursor = null;
        try {
            cursor = mDb.query(DataContactsQuery.TABLE, DataContactsQuery.PROJECTION,
                    mSb.toString(), mSelectionArgs.toArray(EMPTY_STRING_ARRAY), null, null,
                    Clauses.CONTACT_VISIBLE + " DESC, " + Data.RAW_CONTACT_ID);
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
            mValues.put(StatusUpdates.CHAT_CAPABILITY,
                    values.getAsString(StatusUpdates.CHAT_CAPABILITY));

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
                mDbHelper.deleteStatusUpdate(dataId);
            } else if (values.containsKey(StatusUpdates.STATUS_TIMESTAMP)) {
                long timestamp = values.getAsLong(StatusUpdates.STATUS_TIMESTAMP);
                mDbHelper.replaceStatusUpdate(dataId, timestamp, status, resPackage, iconResource,
                        labelResource);
            } else {
                mDbHelper.insertStatusUpdate(dataId, status, resPackage, iconResource,
                        labelResource);
            }
        }

        if (contactId != -1) {
            mContactAggregator.updateLastStatusUpdateId(contactId);
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
                return deleteContact(contactId, callerIsSyncAdapter);
            }

            case CONTACTS_LOOKUP: {
                final List<String> pathSegments = uri.getPathSegments();
                final int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Missing a lookup key", uri));
                }
                final String lookupKey = pathSegments.get(2);
                final long contactId = lookupContactIdByLookupKey(mDb, lookupKey);
                return deleteContact(contactId, callerIsSyncAdapter);
            }

            case CONTACTS_LOOKUP_ID: {
                // lookup contact by id and lookup key to see if they still match the actual record
                final List<String> pathSegments = uri.getPathSegments();
                final String lookupKey = pathSegments.get(2);
                SQLiteQueryBuilder lookupQb = new SQLiteQueryBuilder();
                setTablesAndProjectionMapForContacts(lookupQb, uri, null);
                long contactId = ContentUris.parseId(uri);
                String[] args;
                if (selectionArgs == null) {
                    args = new String[2];
                } else {
                    args = new String[selectionArgs.length + 2];
                    System.arraycopy(selectionArgs, 0, args, 2, selectionArgs.length);
                }
                args[0] = String.valueOf(contactId);
                args[1] = Uri.encode(lookupKey);
                lookupQb.appendWhere(Contacts._ID + "=? AND " + Contacts.LOOKUP_KEY + "=?");
                final SQLiteDatabase db = mDbHelper.getReadableDatabase();
                Cursor c = query(db, lookupQb, null, selection, args, null, null, null);
                try {
                    if (c.getCount() == 1) {
                        // contact was unmodified so go ahead and delete it
                        return deleteContact(contactId, callerIsSyncAdapter);
                    } else {
                        // row was changed (e.g. the merging might have changed), we got multiple
                        // rows or the supplied selection filtered the record out
                        return 0;
                    }
                } finally {
                    c.close();
                }
            }

            case RAW_CONTACTS: {
                int numDeletes = 0;
                Cursor c = mDb.query(Tables.RAW_CONTACTS,
                        new String[]{RawContacts._ID, RawContacts.CONTACT_ID},
                        appendAccountToSelection(uri, selection), selectionArgs, null, null, null);
                try {
                    while (c.moveToNext()) {
                        final long rawContactId = c.getLong(0);
                        long contactId = c.getLong(1);
                        numDeletes += deleteRawContact(rawContactId, contactId,
                                callerIsSyncAdapter);
                    }
                } finally {
                    c.close();
                }
                return numDeletes;
            }

            case RAW_CONTACTS_ID: {
                final long rawContactId = ContentUris.parseId(uri);
                return deleteRawContact(rawContactId, mDbHelper.getContactId(rawContactId),
                        callerIsSyncAdapter);
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
                mSelectionArgs1[0] = String.valueOf(dataId);
                return deleteData(Data._ID + "=?", mSelectionArgs1, callerIsSyncAdapter);
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
                return deleteSettings(uri, appendAccountToSelection(uri, selection), selectionArgs);
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

    public int deleteGroup(Uri uri, long groupId, boolean callerIsSyncAdapter) {
        mGroupIdCache.clear();
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

    private int deleteContact(long contactId, boolean callerIsSyncAdapter) {
        enforceProfilePermissionForContact(contactId, true);
        mSelectionArgs1[0] = Long.toString(contactId);
        Cursor c = mDb.query(Tables.RAW_CONTACTS, new String[]{RawContacts._ID},
                RawContacts.CONTACT_ID + "=?", mSelectionArgs1,
                null, null, null);
        try {
            while (c.moveToNext()) {
                long rawContactId = c.getLong(0);
                markRawContactAsDeleted(rawContactId, callerIsSyncAdapter);
            }
        } finally {
            c.close();
        }

        mProviderStatusUpdateNeeded = true;

        return mDb.delete(Tables.CONTACTS, Contacts._ID + "=" + contactId, null);
    }

    public int deleteRawContact(long rawContactId, long contactId, boolean callerIsSyncAdapter) {
        enforceProfilePermissionForRawContact(rawContactId, true);
        mContactAggregator.invalidateAggregationExceptionCache();
        mProviderStatusUpdateNeeded = true;

        if (callerIsSyncAdapter) {
            mDb.delete(Tables.PRESENCE, PresenceColumns.RAW_CONTACT_ID + "=" + rawContactId, null);
            int count = mDb.delete(Tables.RAW_CONTACTS, RawContacts._ID + "=" + rawContactId, null);
            mContactAggregator.updateDisplayNameForContact(mDb, contactId);
            return count;
        } else {
            mDbHelper.removeContactIfSingleton(rawContactId);
            return markRawContactAsDeleted(rawContactId, callerIsSyncAdapter);
        }
    }

    private int deleteStatusUpdates(String selection, String[] selectionArgs) {
      // delete from both tables: presence and status_updates
      // TODO should account type/name be appended to the where clause?
      if (VERBOSE_LOGGING) {
          Log.v(TAG, "deleting data from status_updates for " + selection);
      }
      mDb.delete(Tables.STATUS_UPDATES, getWhereClauseForStatusUpdatesTable(selection),
          selectionArgs);
      return mDb.delete(Tables.PRESENCE, selection, selectionArgs);
    }

    private int markRawContactAsDeleted(long rawContactId, boolean callerIsSyncAdapter) {
        mSyncToNetwork = true;

        mValues.clear();
        mValues.put(RawContacts.DELETED, 1);
        mValues.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DISABLED);
        mValues.put(RawContactsColumns.AGGREGATION_NEEDED, 1);
        mValues.putNull(RawContacts.CONTACT_ID);
        mValues.put(RawContacts.DIRTY, 1);
        return updateRawContact(rawContactId, mValues, callerIsSyncAdapter);
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
            Object data = values.get(ContactsContract.SyncState.DATA);
            mTransactionContext.syncStateUpdated(rowId, data);
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
                count = updateContactOptions(values, selection, selectionArgs, callerIsSyncAdapter);
                break;
            }

            case CONTACTS_ID: {
                count = updateContactOptions(ContentUris.parseId(uri), values, callerIsSyncAdapter);
                break;
            }

            case PROFILE: {
                // Restrict update to the user's profile.
                StringBuilder profileSelection = new StringBuilder();
                profileSelection.append(Contacts.IS_USER_PROFILE + "=1");
                if (!TextUtils.isEmpty(selection)) {
                    profileSelection.append(" AND (").append(selection).append(")");
                }
                count = updateContactOptions(values, profileSelection.toString(), selectionArgs,
                        callerIsSyncAdapter);
                break;
            }

            case CONTACTS_LOOKUP:
            case CONTACTS_LOOKUP_ID: {
                final List<String> pathSegments = uri.getPathSegments();
                final int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Missing a lookup key", uri));
                }
                final String lookupKey = pathSegments.get(2);
                final long contactId = lookupContactIdByLookupKey(mDb, lookupKey);
                count = updateContactOptions(contactId, values, callerIsSyncAdapter);
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
                count = updateRawContacts(values, selection, selectionArgs, callerIsSyncAdapter);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                if (selection != null) {
                    selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                    count = updateRawContacts(values, RawContacts._ID + "=?"
                                    + " AND(" + selection + ")", selectionArgs,
                            callerIsSyncAdapter);
                } else {
                    mSelectionArgs1[0] = String.valueOf(rawContactId);
                    count = updateRawContacts(values, RawContacts._ID + "=?", mSelectionArgs1,
                            callerIsSyncAdapter);
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
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(groupId));
                String selectionWithId = Groups._ID + "=? "
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
                count = updateSettings(uri, values, appendAccountToSelection(uri, selection),
                        selectionArgs);
                mSyncToNetwork |= !callerIsSyncAdapter;
                break;
            }

            case STATUS_UPDATES: {
                count = updateStatusUpdate(uri, values, selection, selectionArgs);
                break;
            }

            case DIRECTORIES: {
                mContactDirectoryManager.scanPackagesByUid(Binder.getCallingUid());
                count = 1;
                break;
            }

            case DATA_USAGE_FEEDBACK_ID: {
                if (handleDataUsageFeedback(uri)) {
                    count = 1;
                } else {
                    count = 0;
                }
                break;
            }

            default: {
                mSyncToNetwork = true;
                return mLegacyApiSupport.update(uri, values, selection, selectionArgs);
            }
        }

        return count;
    }

    private int updateStatusUpdate(Uri uri, ContentValues values, String selection,
        String[] selectionArgs) {
        // update status_updates table, if status is provided
        // TODO should account type/name be appended to the where clause?
        int updateCount = 0;
        ContentValues settableValues = getSettableColumnsForStatusUpdatesTable(values);
        if (settableValues.size() > 0) {
          updateCount = mDb.update(Tables.STATUS_UPDATES,
                    settableValues,
                    getWhereClauseForStatusUpdatesTable(selection),
                    selectionArgs);
        }

        // now update the Presence table
        settableValues = getSettableColumnsForPresenceTable(values);
        if (settableValues.size() > 0) {
          updateCount = mDb.update(Tables.PRESENCE, settableValues,
                    selection, selectionArgs);
        }
        // TODO updateCount is not entirely a valid count of updated rows because 2 tables could
        // potentially get updated in this method.
        return updateCount;
    }

    /**
     * Build a where clause to select the rows to be updated in status_updates table.
     */
    private String getWhereClauseForStatusUpdatesTable(String selection) {
        mSb.setLength(0);
        mSb.append(WHERE_CLAUSE_FOR_STATUS_UPDATES_TABLE);
        mSb.append(selection);
        mSb.append(")");
        return mSb.toString();
    }

    private ContentValues getSettableColumnsForStatusUpdatesTable(ContentValues values) {
        mValues.clear();
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.STATUS, values,
            StatusUpdates.STATUS);
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.STATUS_TIMESTAMP, values,
            StatusUpdates.STATUS_TIMESTAMP);
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.STATUS_RES_PACKAGE, values,
            StatusUpdates.STATUS_RES_PACKAGE);
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.STATUS_LABEL, values,
            StatusUpdates.STATUS_LABEL);
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.STATUS_ICON, values,
            StatusUpdates.STATUS_ICON);
        return mValues;
    }

    private ContentValues getSettableColumnsForPresenceTable(ContentValues values) {
        mValues.clear();
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.PRESENCE, values,
            StatusUpdates.PRESENCE);
        ContactsDatabaseHelper.copyStringValue(mValues, StatusUpdates.CHAT_CAPABILITY, values,
                StatusUpdates.CHAT_CAPABILITY);
        return mValues;
    }

    private int updateGroups(Uri uri, ContentValues values, String selectionWithId,
            String[] selectionArgs, boolean callerIsSyncAdapter) {

        mGroupIdCache.clear();

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
            Cursor c = mDb.query(Tables.GROUPS, new String[]{Groups.ACCOUNT_NAME,
                    Groups.ACCOUNT_TYPE}, selectionWithId, selectionArgs, null,
                    null, null);
            String accountName;
            String accountType;
            try {
                while (c.moveToNext()) {
                    accountName = c.getString(0);
                    accountType = c.getString(1);
                    if (!TextUtils.isEmpty(accountName) && !TextUtils.isEmpty(accountType)) {
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

    private int updateRawContacts(ContentValues values, String selection, String[] selectionArgs,
            boolean callerIsSyncAdapter) {
        if (values.containsKey(RawContacts.CONTACT_ID)) {
            throw new IllegalArgumentException(RawContacts.CONTACT_ID + " should not be included " +
                    "in content values. Contact IDs are assigned automatically");
        }

        if (!callerIsSyncAdapter) {
            selection = DatabaseUtils.concatenateWhere(selection,
                    RawContacts.RAW_CONTACT_IS_READ_ONLY + "=0");
        }

        int count = 0;
        Cursor cursor = mDb.query(mDbHelper.getRawContactView(),
                new String[] { RawContacts._ID }, selection,
                selectionArgs, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long rawContactId = cursor.getLong(0);
                updateRawContact(rawContactId, values, callerIsSyncAdapter);
                count++;
            }
        } finally {
            cursor.close();
        }

        return count;
    }

    private int updateRawContact(long rawContactId, ContentValues values,
            boolean callerIsSyncAdapter) {

        // Enforce profile permissions if the raw contact is in the user's profile.
        enforceProfilePermissionForRawContact(rawContactId, true);

        final String selection = RawContacts._ID + " = ?";
        mSelectionArgs1[0] = Long.toString(rawContactId);
        final boolean requestUndoDelete = (values.containsKey(RawContacts.DELETED)
                && values.getAsInteger(RawContacts.DELETED) == 0);
        int previousDeleted = 0;
        String accountType = null;
        String accountName = null;
        if (requestUndoDelete) {
            Cursor cursor = mDb.query(RawContactsQuery.TABLE, RawContactsQuery.COLUMNS, selection,
                    mSelectionArgs1, null, null, null);
            try {
                if (cursor.moveToFirst()) {
                    previousDeleted = cursor.getInt(RawContactsQuery.DELETED);
                    accountType = cursor.getString(RawContactsQuery.ACCOUNT_TYPE);
                    accountName = cursor.getString(RawContactsQuery.ACCOUNT_NAME);
                }
            } finally {
                cursor.close();
            }
            values.put(ContactsContract.RawContacts.AGGREGATION_MODE,
                    ContactsContract.RawContacts.AGGREGATION_MODE_DEFAULT);
        }

        int count = mDb.update(Tables.RAW_CONTACTS, values, selection, mSelectionArgs1);
        if (count != 0) {
            if (values.containsKey(RawContacts.AGGREGATION_MODE)) {
                int aggregationMode = values.getAsInteger(RawContacts.AGGREGATION_MODE);

                // As per ContactsContract documentation, changing aggregation mode
                // to DEFAULT should not trigger aggregation
                if (aggregationMode != RawContacts.AGGREGATION_MODE_DEFAULT) {
                    mContactAggregator.markForAggregation(rawContactId, aggregationMode, false);
                }
            }
            if (values.containsKey(RawContacts.STARRED)) {
                if (!callerIsSyncAdapter) {
                    updateFavoritesMembership(rawContactId,
                            values.getAsLong(RawContacts.STARRED) != 0);
                }
                mContactAggregator.updateStarred(rawContactId);
            } else {
                // if this raw contact is being associated with an account, then update the
                // favorites group membership based on whether or not this contact is starred.
                // If it is starred, add a group membership, if one doesn't already exist
                // otherwise delete any matching group memberships.
                if (!callerIsSyncAdapter && values.containsKey(RawContacts.ACCOUNT_NAME)) {
                    boolean starred = 0 != DatabaseUtils.longForQuery(mDb,
                            SELECTION_STARRED_FROM_RAW_CONTACTS,
                            new String[]{Long.toString(rawContactId)});
                    updateFavoritesMembership(rawContactId, starred);
                }
            }

            // if this raw contact is being associated with an account, then add a
            // group membership to the group marked as AutoAdd, if any.
            if (!callerIsSyncAdapter && values.containsKey(RawContacts.ACCOUNT_NAME)) {
                addAutoAddMembership(rawContactId);
            }

            if (values.containsKey(RawContacts.SOURCE_ID)) {
                mContactAggregator.updateLookupKeyForRawContact(mDb, rawContactId);
            }
            if (values.containsKey(RawContacts.NAME_VERIFIED)) {

                // If setting NAME_VERIFIED for this raw contact, reset it for all
                // other raw contacts in the same aggregate
                if (values.getAsInteger(RawContacts.NAME_VERIFIED) != 0) {
                    mDbHelper.resetNameVerifiedForOtherRawContacts(rawContactId);
                }
                mContactAggregator.updateDisplayNameForRawContact(mDb, rawContactId);
            }
            if (requestUndoDelete && previousDeleted == 1) {
                mTransactionContext.rawContactInserted(rawContactId,
                        new Account(accountName, accountType));
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

        if (!callerIsSyncAdapter) {
            selection = DatabaseUtils.concatenateWhere(selection,
                    Data.IS_READ_ONLY + "=0");
        }

        int count = 0;

        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about updating data we don't have permission to read.
        Cursor c = query(uri, DataRowHandler.DataUpdateQuery.COLUMNS,
                selection, selectionArgs, null);
        try {
            while(c.moveToNext()) {
                // Check profile permission for the raw contact that owns each data record.
                long rawContactId = c.getLong(DataRowHandler.DataUpdateQuery.RAW_CONTACT_ID);
                enforceProfilePermissionForRawContact(rawContactId, true);

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

        final String mimeType = c.getString(DataRowHandler.DataUpdateQuery.MIMETYPE);
        DataRowHandler rowHandler = getDataRowHandler(mimeType);
        if (rowHandler.update(mDb, mTransactionContext, values, c, callerIsSyncAdapter)) {
            return 1;
        } else {
            return 0;
        }
    }

    private int updateContactOptions(ContentValues values, String selection,
            String[] selectionArgs, boolean callerIsSyncAdapter) {
        int count = 0;
        Cursor cursor = mDb.query(mDbHelper.getContactView(),
                new String[] { Contacts._ID, Contacts.IS_USER_PROFILE }, selection,
                selectionArgs, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long contactId = cursor.getLong(0);

                // Check for profile write permission before updating a user's profile contact.
                boolean isProfile = cursor.getInt(1) == 1;
                if (isProfile) {
                    enforceProfilePermission(true);
                }

                updateContactOptions(contactId, values, callerIsSyncAdapter);
                count++;
            }
        } finally {
            cursor.close();
        }

        return count;
    }

    private int updateContactOptions(long contactId, ContentValues values,
            boolean callerIsSyncAdapter) {

        // Check write permission if the contact is the user's profile.
        enforceProfilePermissionForContact(contactId, true);

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

        mSelectionArgs1[0] = String.valueOf(contactId);
        mDb.update(Tables.RAW_CONTACTS, mValues, RawContacts.CONTACT_ID + "=?"
                + " AND " + RawContacts.RAW_CONTACT_IS_READ_ONLY + "=0", mSelectionArgs1);

        if (mValues.containsKey(RawContacts.STARRED) && !callerIsSyncAdapter) {
            Cursor cursor = mDb.query(mDbHelper.getRawContactView(),
                    new String[] { RawContacts._ID }, RawContacts.CONTACT_ID + "=?",
                    mSelectionArgs1, null, null, null);
            try {
                while (cursor.moveToNext()) {
                    long rawContactId = cursor.getLong(0);
                    updateFavoritesMembership(rawContactId,
                            mValues.getAsLong(RawContacts.STARRED) != 0);
                }
            } finally {
                cursor.close();
            }
        }

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

        int rslt = mDb.update(Tables.CONTACTS, mValues, Contacts._ID + "=?", mSelectionArgs1);

        if (values.containsKey(Contacts.LAST_TIME_CONTACTED) &&
                !values.containsKey(Contacts.TIMES_CONTACTED)) {
            mDb.execSQL(UPDATE_TIMES_CONTACTED_CONTACTS_TABLE, mSelectionArgs1);
            mDb.execSQL(UPDATE_TIMES_CONTACTED_RAWCONTACTS_TABLE, mSelectionArgs1);
        }
        return rslt;
    }

    private int updateAggregationException(SQLiteDatabase db, ContentValues values) {
        int exceptionType = values.getAsInteger(AggregationExceptions.TYPE);
        long rcId1 = values.getAsInteger(AggregationExceptions.RAW_CONTACT_ID1);
        long rcId2 = values.getAsInteger(AggregationExceptions.RAW_CONTACT_ID2);

        long rawContactId1;
        long rawContactId2;
        if (rcId1 < rcId2) {
            rawContactId1 = rcId1;
            rawContactId2 = rcId2;
        } else {
            rawContactId2 = rcId1;
            rawContactId1 = rcId2;
        }

        if (exceptionType == AggregationExceptions.TYPE_AUTOMATIC) {
            mSelectionArgs2[0] = String.valueOf(rawContactId1);
            mSelectionArgs2[1] = String.valueOf(rawContactId2);
            db.delete(Tables.AGGREGATION_EXCEPTIONS,
                    AggregationExceptions.RAW_CONTACT_ID1 + "=? AND "
                    + AggregationExceptions.RAW_CONTACT_ID2 + "=?", mSelectionArgs2);
        } else {
            ContentValues exceptionValues = new ContentValues(3);
            exceptionValues.put(AggregationExceptions.TYPE, exceptionType);
            exceptionValues.put(AggregationExceptions.RAW_CONTACT_ID1, rawContactId1);
            exceptionValues.put(AggregationExceptions.RAW_CONTACT_ID2, rawContactId2);
            db.replace(Tables.AGGREGATION_EXCEPTIONS, AggregationExceptions._ID,
                    exceptionValues);
        }

        mContactAggregator.invalidateAggregationExceptionCache();
        mContactAggregator.markForAggregation(rawContactId1,
                RawContacts.AGGREGATION_MODE_DEFAULT, true);
        mContactAggregator.markForAggregation(rawContactId2,
                RawContacts.AGGREGATION_MODE_DEFAULT, true);

        mContactAggregator.aggregateContact(mTransactionContext, db, rawContactId1);
        mContactAggregator.aggregateContact(mTransactionContext, db, rawContactId2);

        // The return value is fake - we just confirm that we made a change, not count actual
        // rows changed.
        return 1;
    }

    public void onAccountsUpdated(Account[] accounts) {
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_ACCOUNTS);
    }

    protected boolean updateAccountsInBackground(Account[] accounts) {
        // TODO : Check the unit test.
        boolean accountsChanged = false;
        HashSet<Account> existingAccounts = new HashSet<Account>();
        mDb = mDbHelper.getWritableDatabase();
        mDb.beginTransaction();
        try {
            findValidAccounts(existingAccounts);

            // Add a row to the ACCOUNTS table for each new account
            for (Account account : accounts) {
                if (!existingAccounts.contains(account)) {
                    accountsChanged = true;
                    mDb.execSQL("INSERT INTO " + Tables.ACCOUNTS + " (" + RawContacts.ACCOUNT_NAME
                            + ", " + RawContacts.ACCOUNT_TYPE + ") VALUES (?, ?)",
                            new String[] {account.name, account.type});
                }
            }

            // Remove all valid accounts from the existing account set. What is left
            // in the accountsToDelete set will be extra accounts whose data must be deleted.
            HashSet<Account> accountsToDelete = new HashSet<Account>(existingAccounts);
            for (Account account : accounts) {
                accountsToDelete.remove(account);
            }

            if (!accountsToDelete.isEmpty()) {
                accountsChanged = true;
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
                    mDb.execSQL(
                            "DELETE FROM " + Tables.ACCOUNTS +
                            " WHERE " + RawContacts.ACCOUNT_NAME + "=?" +
                            " AND " + RawContacts.ACCOUNT_TYPE + "=?", params);
                    mDb.execSQL(
                            "DELETE FROM " + Tables.DIRECTORIES +
                            " WHERE " + Directory.ACCOUNT_NAME + "=?" +
                            " AND " + Directory.ACCOUNT_TYPE + "=?", params);
                    resetDirectoryCache();
                }

                // Find all aggregated contacts that used to contain the raw contacts
                // we have just deleted and see if they are still referencing the deleted
                // names or photos.  If so, fix up those contacts.
                HashSet<Long> orphanContactIds = Sets.newHashSet();
                Cursor cursor = mDb.rawQuery("SELECT " + Contacts._ID +
                        " FROM " + Tables.CONTACTS +
                        " WHERE (" + Contacts.NAME_RAW_CONTACT_ID + " NOT NULL AND " +
                                Contacts.NAME_RAW_CONTACT_ID + " NOT IN " +
                                        "(SELECT " + RawContacts._ID +
                                        " FROM " + Tables.RAW_CONTACTS + "))" +
                        " OR (" + Contacts.PHOTO_ID + " NOT NULL AND " +
                                Contacts.PHOTO_ID + " NOT IN " +
                                        "(SELECT " + Data._ID +
                                        " FROM " + Tables.DATA + "))", null);
                try {
                    while (cursor.moveToNext()) {
                        orphanContactIds.add(cursor.getLong(0));
                    }
                } finally {
                    cursor.close();
                }

                for (Long contactId : orphanContactIds) {
                    mContactAggregator.updateAggregateData(mTransactionContext, contactId);
                }
                mDbHelper.updateAllVisible();
                updateSearchIndexInTransaction();
            }

            if (accountsChanged) {
                mDbHelper.getSyncState().onAccountsChanged(mDb, accounts);
            }
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
        mAccountWritability.clear();

        if (accountsChanged) {
            updateContactsAccountCount(accounts);
            updateProviderStatus();
        }

        return accountsChanged;
    }

    private void updateContactsAccountCount(Account[] accounts) {
        int count = 0;
        for (Account account : accounts) {
            if (isContactsAccount(account)) {
                count++;
            }
        }
        mContactsAccountCount = count;
    }

    protected boolean isContactsAccount(Account account) {
        final IContentService cs = ContentResolver.getContentService();
        try {
            return cs.getIsSyncable(account, ContactsContract.AUTHORITY) > 0;
        } catch (RemoteException e) {
            Log.e(TAG, "Cannot obtain sync flag for account: " + account, e);
            return false;
        }
    }

    public void onPackageChanged(String packageName) {
        scheduleBackgroundTask(BACKGROUND_TASK_UPDATE_DIRECTORIES, packageName);
    }

    /**
     * Finds all distinct accounts present in the specified table.
     */
    private void findValidAccounts(Set<Account> validAccounts) {
        Cursor c = mDb.rawQuery(
                "SELECT " + RawContacts.ACCOUNT_NAME + "," + RawContacts.ACCOUNT_TYPE +
                " FROM " + Tables.ACCOUNTS, null);
        try {
            while (c.moveToNext()) {
                if (!c.isNull(0) || !c.isNull(1)) {
                    validAccounts.add(new Account(c.getString(0), c.getString(1)));
                }
            }
        } finally {
            c.close();
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        waitForAccess(mReadAccessLatch);

        String directory = getQueryParameter(uri, ContactsContract.DIRECTORY_PARAM_KEY);
        if (directory == null) {
            return wrapCursor(uri,
                    queryLocal(uri, projection, selection, selectionArgs, sortOrder, -1));
        } else if (directory.equals("0")) {
            return wrapCursor(uri,
                    queryLocal(uri, projection, selection, selectionArgs, sortOrder,
                            Directory.DEFAULT));
        } else if (directory.equals("1")) {
            return wrapCursor(uri,
                    queryLocal(uri, projection, selection, selectionArgs, sortOrder,
                            Directory.LOCAL_INVISIBLE));
        }

        DirectoryInfo directoryInfo = getDirectoryAuthority(directory);
        if (directoryInfo == null) {
            Log.e(TAG, "Invalid directory ID: " + uri);
            return null;
        }

        Builder builder = new Uri.Builder();
        builder.scheme(ContentResolver.SCHEME_CONTENT);
        builder.authority(directoryInfo.authority);
        builder.encodedPath(uri.getEncodedPath());
        if (directoryInfo.accountName != null) {
            builder.appendQueryParameter(RawContacts.ACCOUNT_NAME, directoryInfo.accountName);
        }
        if (directoryInfo.accountType != null) {
            builder.appendQueryParameter(RawContacts.ACCOUNT_TYPE, directoryInfo.accountType);
        }

        String limit = getLimit(uri);
        if (limit != null) {
            builder.appendQueryParameter(ContactsContract.LIMIT_PARAM_KEY, limit);
        }

        Uri directoryUri = builder.build();

        if (projection == null) {
            projection = getDefaultProjection(uri);
        }

        Cursor cursor = getContext().getContentResolver().query(directoryUri, projection, selection,
                selectionArgs, sortOrder);

        if (cursor == null) {
            return null;
        }

        CrossProcessCursor crossProcessCursor = getCrossProcessCursor(cursor);
        if (crossProcessCursor != null) {
            return wrapCursor(uri, cursor);
        } else {
            return matrixCursorFromCursor(wrapCursor(uri, cursor));
        }
    }

    private Cursor wrapCursor(Uri uri, Cursor cursor) {

        // If the cursor doesn't contain a snippet column, don't bother wrapping it.
        if (cursor.getColumnIndex(SearchSnippetColumns.SNIPPET) < 0) {
            return cursor;
        }

        // Parse out snippet arguments for use when snippets are retrieved from the cursor.
        String[] args = null;
        String snippetArgs =
                getQueryParameter(uri, SearchSnippetColumns.SNIPPET_ARGS_PARAM_KEY);
        if (snippetArgs != null) {
            args = snippetArgs.split(",");
        }

        String query = uri.getLastPathSegment();
        String startMatch = args != null && args.length > 0 ? args[0]
                : DEFAULT_SNIPPET_ARG_START_MATCH;
        String endMatch = args != null && args.length > 1 ? args[1]
                : DEFAULT_SNIPPET_ARG_END_MATCH;
        String ellipsis = args != null && args.length > 2 ? args[2]
                : DEFAULT_SNIPPET_ARG_ELLIPSIS;
        int maxTokens = args != null && args.length > 3 ? Integer.parseInt(args[3])
                : DEFAULT_SNIPPET_ARG_MAX_TOKENS;

        return new SnippetizingCursorWrapper(cursor, query, startMatch, endMatch, ellipsis,
                maxTokens);
    }

    private CrossProcessCursor getCrossProcessCursor(Cursor cursor) {
        Cursor c = cursor;
        if (c instanceof CrossProcessCursor) {
            return (CrossProcessCursor) c;
        } else if (c instanceof CursorWindow) {
            return getCrossProcessCursor(((CursorWrapper) c).getWrappedCursor());
        } else {
            return null;
        }
    }

    public MatrixCursor matrixCursorFromCursor(Cursor cursor) {
        MatrixCursor newCursor = new MatrixCursor(cursor.getColumnNames());
        int numColumns = cursor.getColumnCount();
        String data[] = new String[numColumns];
        cursor.moveToPosition(-1);
        while (cursor.moveToNext()) {
            for (int i = 0; i < numColumns; i++) {
                data[i] = cursor.getString(i);
            }
            newCursor.addRow(data);
        }
        return newCursor;
    }

    private static final class DirectoryQuery {
        public static final String[] COLUMNS = new String[] {
                Directory._ID,
                Directory.DIRECTORY_AUTHORITY,
                Directory.ACCOUNT_NAME,
                Directory.ACCOUNT_TYPE
        };

        public static final int DIRECTORY_ID = 0;
        public static final int AUTHORITY = 1;
        public static final int ACCOUNT_NAME = 2;
        public static final int ACCOUNT_TYPE = 3;
    }

    /**
     * Reads and caches directory information for the database.
     */
    private DirectoryInfo getDirectoryAuthority(String directoryId) {
        synchronized (mDirectoryCache) {
            if (!mDirectoryCacheValid) {
                mDirectoryCache.clear();
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                Cursor cursor = db.query(Tables.DIRECTORIES,
                        DirectoryQuery.COLUMNS,
                        null, null, null, null, null);
                try {
                    while (cursor.moveToNext()) {
                        DirectoryInfo info = new DirectoryInfo();
                        String id = cursor.getString(DirectoryQuery.DIRECTORY_ID);
                        info.authority = cursor.getString(DirectoryQuery.AUTHORITY);
                        info.accountName = cursor.getString(DirectoryQuery.ACCOUNT_NAME);
                        info.accountType = cursor.getString(DirectoryQuery.ACCOUNT_TYPE);
                        mDirectoryCache.put(id, info);
                    }
                } finally {
                    cursor.close();
                }
                mDirectoryCacheValid = true;
            }

            return mDirectoryCache.get(directoryId);
        }
    }

    public void resetDirectoryCache() {
        synchronized(mDirectoryCache) {
            mDirectoryCacheValid = false;
        }
    }

    public Cursor queryLocal(Uri uri, String[] projection, String selection, String[] selectionArgs,
                String sortOrder, long directoryId) {
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
                boolean existingWhere = appendLocalDirectorySelectionIfNeeded(qb, directoryId);
                appendProfileRestriction(qb, uri, Contacts.IS_USER_PROFILE, existingWhere);
                sortOrder = prependProfileSortIfNeeded(uri, sortOrder);
                break;
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                enforceProfilePermissionForContact(contactId, false);
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(contactId));
                qb.appendWhere(Contacts._ID + "=?");
                break;
            }

            case CONTACTS_LOOKUP:
            case CONTACTS_LOOKUP_ID: {
                List<String> pathSegments = uri.getPathSegments();
                int segmentCount = pathSegments.size();
                if (segmentCount < 3) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Missing a lookup key", uri));
                }

                String lookupKey = pathSegments.get(2);
                if (segmentCount == 4) {
                    long contactId = Long.parseLong(pathSegments.get(3));
                    enforceProfilePermissionForContact(contactId, false);
                    SQLiteQueryBuilder lookupQb = new SQLiteQueryBuilder();
                    setTablesAndProjectionMapForContacts(lookupQb, uri, projection);

                    Cursor c = queryWithContactIdAndLookupKey(lookupQb, db, uri,
                            projection, selection, selectionArgs, sortOrder, groupBy, limit,
                            Contacts._ID, contactId, Contacts.LOOKUP_KEY, lookupKey);
                    if (c != null) {
                        return c;
                    }
                }

                setTablesAndProjectionMapForContacts(qb, uri, projection);
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(lookupContactIdByLookupKey(db, lookupKey)));
                qb.appendWhere(Contacts._ID + "=?");
                break;
            }

            case CONTACTS_LOOKUP_DATA:
            case CONTACTS_LOOKUP_ID_DATA: {
                List<String> pathSegments = uri.getPathSegments();
                int segmentCount = pathSegments.size();
                if (segmentCount < 4) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Missing a lookup key", uri));
                }
                String lookupKey = pathSegments.get(2);
                if (segmentCount == 5) {
                    long contactId = Long.parseLong(pathSegments.get(3));
                    enforceProfilePermissionForContact(contactId, false);
                    SQLiteQueryBuilder lookupQb = new SQLiteQueryBuilder();
                    setTablesAndProjectionMapForData(lookupQb, uri, projection, false);
                    lookupQb.appendWhere(" AND ");
                    Cursor c = queryWithContactIdAndLookupKey(lookupQb, db, uri,
                            projection, selection, selectionArgs, sortOrder, groupBy, limit,
                            Data.CONTACT_ID, contactId, Data.LOOKUP_KEY, lookupKey);
                    if (c != null) {
                        return c;
                    }

                    // TODO see if the contact exists but has no data rows (rare)
                }

                setTablesAndProjectionMapForData(qb, uri, projection, false);
                long contactId = lookupContactIdByLookupKey(db, lookupKey);
                enforceProfilePermissionForContact(contactId, false);
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(contactId));
                qb.appendWhere(" AND " + Data.CONTACT_ID + "=?");
                break;
            }

            case CONTACTS_AS_VCARD: {
                // When reading as vCard always use restricted view
                final String lookupKey = Uri.encode(uri.getPathSegments().get(2));
                long contactId = lookupContactIdByLookupKey(db, lookupKey);
                enforceProfilePermissionForContact(contactId, false);
                qb.setTables(mDbHelper.getContactView(true /* require restricted */));
                qb.setProjectionMap(sContactsVCardProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(contactId));
                qb.appendWhere(Contacts._ID + "=?");
                break;
            }

            case CONTACTS_AS_MULTI_VCARD: {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                String currentDateString = dateFormat.format(new Date()).toString();
                return db.rawQuery(
                    "SELECT" +
                    " 'vcards_' || ? || '.vcf' AS " + OpenableColumns.DISPLAY_NAME + "," +
                    " NULL AS " + OpenableColumns.SIZE,
                    new String[] { currentDateString });
            }

            case CONTACTS_FILTER: {
                String filterParam = "";
                if (uri.getPathSegments().size() > 2) {
                    filterParam = uri.getLastPathSegment();
                }
                setTablesAndProjectionMapForContactsWithSnippet(
                        qb, uri, projection, filterParam, directoryId);
                appendProfileRestriction(qb, uri, Contacts.IS_USER_PROFILE, false);
                sortOrder = prependProfileSortIfNeeded(uri, sortOrder);
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
                    starredProjection =
                            appendProjectionArg(projection, TIMES_CONTACTED_SORT_COLUMN);
                    frequentProjection =
                            appendProjectionArg(projection, TIMES_CONTACTED_SORT_COLUMN);
                }
                qb.setProjectionMap(sStrequentStarredProjectionMap);

                // Build the first query for starred
                if (filterSql != null) {
                    qb.appendWhere(filterSql + " AND ");
                }
                qb.appendWhere(Contacts.IS_USER_PROFILE + "=0");
                final String starredQuery = qb.buildQuery(starredProjection,
                        Contacts.STARRED + "=1", Contacts._ID, null, null, null);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                qb.setProjectionMap(sStrequentFrequentProjectionMap);
                if (filterSql != null) {
                    qb.appendWhere(filterSql + " AND ");
                }
                qb.appendWhere(Contacts.IS_USER_PROFILE + "=0");
                final String frequentQuery = qb.buildQuery(frequentProjection,
                        Contacts.TIMES_CONTACTED + " > 0 AND (" + Contacts.STARRED
                        + " = 0 OR " + Contacts.STARRED + " IS NULL)",
                        Contacts._ID, null, null, null);

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

            case PROFILE: {
                enforceProfilePermission(false);
                setTablesAndProjectionMapForContacts(qb, uri, projection);
                qb.appendWhere(Contacts.IS_USER_PROFILE + "=1");
                break;
            }

            case PROFILE_ENTITIES: {
                enforceProfilePermission(false);
                setTablesAndProjectionMapForEntities(qb, uri, projection);
                qb.appendWhere(" AND " + Contacts.IS_USER_PROFILE + "=1");
                break;
            }

            case PROFILE_DATA: {
                enforceProfilePermission(false);
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1");
                break;
            }

            case PROFILE_DATA_ID: {
                enforceProfilePermission(false);
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(" AND " + Data._ID + "=? AND "
                        + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1");
                break;
            }

            case PROFILE_AS_VCARD: {
                enforceProfilePermission(false);
                // When reading as vCard always use restricted view
                qb.setTables(mDbHelper.getContactView(true /* require restricted */));
                qb.setProjectionMap(sContactsVCardProjectionMap);
                qb.appendWhere(Contacts.IS_USER_PROFILE + "=1");
                break;
            }

            case CONTACTS_ID_DATA: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(contactId));
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=?");
                break;
            }

            case CONTACTS_ID_PHOTO: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(contactId));
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=?");
                qb.appendWhere(" AND " + Data._ID + "=" + Contacts.PHOTO_ID);
                break;
            }

            case CONTACTS_ID_ENTITIES: {
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForEntities(qb, uri, projection);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(contactId));
                qb.appendWhere(" AND " + RawContacts.CONTACT_ID + "=?");
                break;
            }

            case CONTACTS_LOOKUP_ENTITIES:
            case CONTACTS_LOOKUP_ID_ENTITIES: {
                List<String> pathSegments = uri.getPathSegments();
                int segmentCount = pathSegments.size();
                if (segmentCount < 4) {
                    throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                            "Missing a lookup key", uri));
                }
                String lookupKey = pathSegments.get(2);
                if (segmentCount == 5) {
                    long contactId = Long.parseLong(pathSegments.get(3));
                    SQLiteQueryBuilder lookupQb = new SQLiteQueryBuilder();
                    setTablesAndProjectionMapForEntities(lookupQb, uri, projection);
                    lookupQb.appendWhere(" AND ");

                    Cursor c = queryWithContactIdAndLookupKey(lookupQb, db, uri,
                            projection, selection, selectionArgs, sortOrder, groupBy, limit,
                            Contacts.Entity.CONTACT_ID, contactId,
                            Contacts.Entity.LOOKUP_KEY, lookupKey);
                    if (c != null) {
                        return c;
                    }
                }

                setTablesAndProjectionMapForEntities(qb, uri, projection);
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(lookupContactIdByLookupKey(db, lookupKey)));
                qb.appendWhere(" AND " + Contacts.Entity.CONTACT_ID + "=?");
                break;
            }

            case PHONES: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                break;
            }

            case PHONES_ID: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + Data._ID + "=?");
                break;
            }

            case PHONES_FILTER: {
                String typeParam = uri.getQueryParameter(DataUsageFeedback.USAGE_TYPE);
                Integer typeInt = sDataUsageTypeMap.get(typeParam);
                if (typeInt == null) {
                    typeInt = DataUsageStatColumns.USAGE_TYPE_INT_CALL;
                }
                setTablesAndProjectionMapForData(qb, uri, projection, true, typeInt);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Phone.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    StringBuilder sb = new StringBuilder();
                    sb.append(" AND (");

                    boolean hasCondition = false;
                    boolean orNeeded = false;
                    String normalizedName = NameNormalizer.normalize(filterParam);
                    if (normalizedName.length() > 0) {
                        sb.append(Data.RAW_CONTACT_ID + " IN " +
                                "(SELECT " + RawContactsColumns.CONCRETE_ID +
                                " FROM " + Tables.SEARCH_INDEX +
                                " JOIN " + Tables.RAW_CONTACTS +
                                " ON (" + Tables.SEARCH_INDEX + "." + SearchIndexColumns.CONTACT_ID
                                        + "=" + RawContactsColumns.CONCRETE_CONTACT_ID + ")" +
                                " WHERE " + SearchIndexColumns.NAME + " MATCH ");
                        DatabaseUtils.appendEscapedSQLString(sb, sanitizeMatch(filterParam) + "*");
                        sb.append(")");
                        orNeeded = true;
                        hasCondition = true;
                    }

                    String number = PhoneNumberUtils.normalizeNumber(filterParam);
                    if (!TextUtils.isEmpty(number)) {
                        if (orNeeded) {
                            sb.append(" OR ");
                        }
                        sb.append(Data._ID +
                                " IN (SELECT DISTINCT " + PhoneLookupColumns.DATA_ID
                                + " FROM " + Tables.PHONE_LOOKUP
                                + " WHERE " + PhoneLookupColumns.NORMALIZED_NUMBER + " LIKE '");
                        sb.append(number);
                        sb.append("%')");
                        hasCondition = true;
                    }

                    if (!hasCondition) {
                        // If it is neither a phone number nor a name, the query should return
                        // an empty cursor.  Let's ensure that.
                        sb.append("0");
                    }
                    sb.append(")");
                    qb.appendWhere(sb);
                }
                groupBy = PhoneColumns.NORMALIZED_NUMBER + "," + RawContacts.CONTACT_ID;
                if (sortOrder == null) {
                    final String accountPromotionSortOrder = getAccountPromotionSortOrder(uri);
                    if (!TextUtils.isEmpty(accountPromotionSortOrder)) {
                        sortOrder = accountPromotionSortOrder + ", " + PHONE_FILTER_SORT_ORDER;
                    } else {
                        sortOrder = PHONE_FILTER_SORT_ORDER;
                    }
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
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Data._ID + "=?");
                break;
            }

            case EMAILS_LOOKUP: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '" + Email.CONTENT_ITEM_TYPE + "'");
                if (uri.getPathSegments().size() > 2) {
                    String email = uri.getLastPathSegment();
                    String address = mDbHelper.extractAddressFromEmailAddress(email);
                    selectionArgs = insertSelectionArg(selectionArgs, address);
                    qb.appendWhere(" AND UPPER(" + Email.DATA + ")=UPPER(?)");
                }
                break;
            }

            case EMAILS_FILTER: {
                String typeParam = uri.getQueryParameter(DataUsageFeedback.USAGE_TYPE);
                Integer typeInt = sDataUsageTypeMap.get(typeParam);
                if (typeInt == null) {
                    typeInt = DataUsageStatColumns.USAGE_TYPE_INT_LONG_TEXT;
                }
                setTablesAndProjectionMapForData(qb, uri, projection, true, typeInt);
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
                            " WHERE " + DataColumns.MIMETYPE_ID + "=");
                    sb.append(mDbHelper.getMimeTypeIdForEmail());
                    sb.append(" AND " + Data.DATA1 + " LIKE ");
                    DatabaseUtils.appendEscapedSQLString(sb, filterParam + '%');
                    if (!filterParam.contains("@")) {
                        sb.append(
                                " UNION SELECT " + Data._ID +
                                " FROM " + Tables.DATA +
                                " WHERE +" + DataColumns.MIMETYPE_ID + "=");
                        sb.append(mDbHelper.getMimeTypeIdForEmail());
                        sb.append(" AND " + Data.RAW_CONTACT_ID + " IN " +
                                "(SELECT " + RawContactsColumns.CONCRETE_ID +
                                " FROM " + Tables.SEARCH_INDEX +
                                " JOIN " + Tables.RAW_CONTACTS +
                                " ON (" + Tables.SEARCH_INDEX + "." + SearchIndexColumns.CONTACT_ID
                                        + "=" + RawContactsColumns.CONCRETE_CONTACT_ID + ")" +
                                " WHERE " + SearchIndexColumns.NAME + " MATCH ");
                        DatabaseUtils.appendEscapedSQLString(sb, sanitizeMatch(filterParam) + "*");
                        sb.append(")");
                    }
                    sb.append(")");
                    qb.appendWhere(sb);
                }
                groupBy = Email.DATA + "," + RawContacts.CONTACT_ID;
                if (sortOrder == null) {
                    final String accountPromotionSortOrder = getAccountPromotionSortOrder(uri);
                    if (!TextUtils.isEmpty(accountPromotionSortOrder)) {
                        sortOrder = accountPromotionSortOrder + ", " + EMAIL_FILTER_SORT_ORDER;
                    } else {
                        sortOrder = EMAIL_FILTER_SORT_ORDER;
                    }
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
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(" AND " + Data.MIMETYPE + " = '"
                        + StructuredPostal.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + Data._ID + "=?");
                break;
            }

            case RAW_CONTACTS: {
                setTablesAndProjectionMapForRawContacts(qb, uri);
                appendProfileRestriction(qb, uri, RawContacts.RAW_CONTACT_IS_USER_PROFILE, true);
                break;
            }

            case RAW_CONTACTS_ID: {
                long rawContactId = ContentUris.parseId(uri);
                enforceProfilePermissionForRawContact(rawContactId, false);
                setTablesAndProjectionMapForRawContacts(qb, uri);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                qb.appendWhere(" AND " + RawContacts._ID + "=?");
                break;
            }

            case RAW_CONTACTS_DATA: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                qb.appendWhere(" AND " + Data.RAW_CONTACT_ID + "=?");
                appendProfileRestriction(qb, uri, RawContacts.RAW_CONTACT_IS_USER_PROFILE, true);
                break;
            }


            case PROFILE_RAW_CONTACTS: {
                enforceProfilePermission(false);
                setTablesAndProjectionMapForRawContacts(qb, uri);
                qb.appendWhere(" AND " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1");
                break;
            }

            case PROFILE_RAW_CONTACTS_ID: {
                enforceProfilePermission(false);
                long rawContactId = ContentUris.parseId(uri);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                setTablesAndProjectionMapForRawContacts(qb, uri);
                qb.appendWhere(" AND " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1 AND "
                        + RawContacts._ID + "=?");
                break;
            }

            case PROFILE_RAW_CONTACTS_ID_DATA: {
                enforceProfilePermission(false);
                long rawContactId = Long.parseLong(uri.getPathSegments().get(2));
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                qb.appendWhere(" AND " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1 AND "
                        + Data.RAW_CONTACT_ID + "=?");
                break;
            }

            case PROFILE_RAW_CONTACTS_ID_ENTITIES: {
                enforceProfilePermission(false);
                long rawContactId = Long.parseLong(uri.getPathSegments().get(2));
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                setTablesAndProjectionMapForRawEntities(qb, uri);
                qb.appendWhere(" AND " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + "=1 AND "
                        + RawContacts._ID + "=?");
                break;
            }

            case DATA: {
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                appendProfileRestriction(qb, uri, RawContacts.RAW_CONTACT_IS_USER_PROFILE, true);
                break;
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                enforceProfilePermissionForData(dataId, false);
                setTablesAndProjectionMapForData(qb, uri, projection, false);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(" AND " + Data._ID + "=?");
                break;
            }

            case PHONE_LOOKUP: {

                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = " length(lookup.normalized_number) DESC";
                }

                String number = uri.getPathSegments().size() > 1 ? uri.getLastPathSegment() : "";
                String numberE164 = PhoneNumberUtils.formatNumberToE164(number,
                        mDbHelper.getCurrentCountryIso());
                String normalizedNumber =
                        PhoneNumberUtils.normalizeNumber(number);
                mDbHelper.buildPhoneLookupAndContactQuery(qb, normalizedNumber, numberE164);
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
                qb.setTables(mDbHelper.getGroupView());
                qb.setProjectionMap(sGroupsProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(Groups._ID + "=?");
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

                ArrayList<AggregationSuggestionParameter> parameters = null;
                List<String> query = uri.getQueryParameters("query");
                if (query != null && !query.isEmpty()) {
                    parameters = new ArrayList<AggregationSuggestionParameter>(query.size());
                    for (String parameter : query) {
                        int offset = parameter.indexOf(':');
                        parameters.add(offset == -1
                                ? new AggregationSuggestionParameter(
                                        AggregationSuggestions.PARAMETER_MATCH_NAME,
                                        parameter)
                                : new AggregationSuggestionParameter(
                                        parameter.substring(0, offset),
                                        parameter.substring(offset + 1)));
                    }
                }

                setTablesAndProjectionMapForContacts(qb, uri, projection);

                return mContactAggregator.queryAggregationSuggestions(qb, projection, contactId,
                        maxSuggestions, filter, parameters);
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
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(DataColumns.CONCRETE_ID + "=?");
                break;
            }

            case SEARCH_SUGGESTIONS: {
                return mGlobalSearchSupport.handleSearchSuggestionsQuery(
                        db, uri, projection, limit);
            }

            case SEARCH_SHORTCUT: {
                String lookupKey = uri.getLastPathSegment();
                String filter = getQueryParameter(
                        uri, SearchManager.SUGGEST_COLUMN_INTENT_EXTRA_DATA);
                return mGlobalSearchSupport.handleSearchShortcutRefresh(
                        db, projection, lookupKey, filter);
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
                setTablesAndProjectionMapForRawEntities(qb, uri);
                break;
            }

            case RAW_CONTACT_ENTITY_ID: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                setTablesAndProjectionMapForRawEntities(qb, uri);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(rawContactId));
                qb.appendWhere(" AND " + RawContacts._ID + "=?");
                break;
            }

            case PROVIDER_STATUS: {
                return queryProviderStatus(uri, projection);
            }

            case DIRECTORIES : {
                qb.setTables(Tables.DIRECTORIES);
                qb.setProjectionMap(sDirectoryProjectionMap);
                break;
            }

            case DIRECTORIES_ID : {
                long id = ContentUris.parseId(uri);
                qb.setTables(Tables.DIRECTORIES);
                qb.setProjectionMap(sDirectoryProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, String.valueOf(id));
                qb.appendWhere(Directory._ID + "=?");
                break;
            }

            case COMPLETE_NAME: {
                return completeName(uri, projection);
            }

            default:
                return mLegacyApiSupport.query(uri, projection, selection, selectionArgs,
                        sortOrder, limit);
        }

        qb.setStrict(true);

        Cursor cursor =
                query(db, qb, projection, selection, selectionArgs, sortOrder, groupBy, limit);
        if (readBooleanQueryParameter(uri, ContactCounts.ADDRESS_BOOK_INDEX_EXTRAS, false)) {
            cursor = bundleLetterCountExtras(cursor, db, qb, selection, selectionArgs, sortOrder);
        }
        return cursor;
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

    /**
     * Creates a single-row cursor containing the current status of the provider.
     */
    private Cursor queryProviderStatus(Uri uri, String[] projection) {
        MatrixCursor cursor = new MatrixCursor(projection);
        RowBuilder row = cursor.newRow();
        for (int i = 0; i < projection.length; i++) {
            if (ProviderStatus.STATUS.equals(projection[i])) {
                row.add(mProviderStatus);
            } else if (ProviderStatus.DATA1.equals(projection[i])) {
                row.add(mEstimatedStorageRequirement);
            }
        }
        return cursor;
    }

    /**
     * Runs the query with the supplied contact ID and lookup ID.  If the query succeeds,
     * it returns the resulting cursor, otherwise it returns null and the calling
     * method needs to resolve the lookup key and rerun the query.
     */
    private Cursor queryWithContactIdAndLookupKey(SQLiteQueryBuilder lookupQb,
            SQLiteDatabase db, Uri uri,
            String[] projection, String selection, String[] selectionArgs,
            String sortOrder, String groupBy, String limit,
            String contactIdColumn, long contactId, String lookupKeyColumn, String lookupKey) {
        String[] args;
        if (selectionArgs == null) {
            args = new String[2];
        } else {
            args = new String[selectionArgs.length + 2];
            System.arraycopy(selectionArgs, 0, args, 2, selectionArgs.length);
        }
        args[0] = String.valueOf(contactId);
        args[1] = Uri.encode(lookupKey);
        lookupQb.appendWhere(contactIdColumn + "=? AND " + lookupKeyColumn + "=?");
        Cursor c = query(db, lookupQb, projection, selection, args, sortOrder,
                groupBy, limit);
        if (c.getCount() != 0) {
            return c;
        }

        c.close();
        return null;
    }

    private static final class AddressBookIndexQuery {
        public static final String LETTER = "letter";
        public static final String TITLE = "title";
        public static final String COUNT = "count";

        public static final String[] COLUMNS = new String[] {
                LETTER, TITLE, COUNT
        };

        public static final int COLUMN_LETTER = 0;
        public static final int COLUMN_TITLE = 1;
        public static final int COLUMN_COUNT = 2;

        // The first letter of the sort key column is what is used for the index headings, except
        // in the case of the user's profile, in which case it is empty.
        public static final String SECTION_HEADING_TEMPLATE =
                "(CASE WHEN %1$s=1 THEN '' ELSE SUBSTR(%2$s,1,1) END)";

        public static final String ORDER_BY = LETTER + " COLLATE " + PHONEBOOK_COLLATOR_NAME;
    }

    /**
     * Computes counts by the address book index titles and adds the resulting tally
     * to the returned cursor as a bundle of extras.
     */
    private Cursor bundleLetterCountExtras(Cursor cursor, final SQLiteDatabase db,
            SQLiteQueryBuilder qb, String selection, String[] selectionArgs, String sortOrder) {
        String sortKey;

        // The sort order suffix could be something like "DESC".
        // We want to preserve it in the query even though we will change
        // the sort column itself.
        String sortOrderSuffix = "";
        if (sortOrder != null) {

            // If the sort order contains one of the "is_profile" columns, we need to strip it out
            // first.
            if (sortOrder.contains(Contacts.IS_USER_PROFILE)
                    || sortOrder.contains(RawContacts.RAW_CONTACT_IS_USER_PROFILE)) {
                String[] splitOrderClauses = sortOrder.split(",");
                StringBuilder rejoinedClause = new StringBuilder();
                for (String orderClause : splitOrderClauses) {
                    if (!orderClause.contains(Contacts.IS_USER_PROFILE)
                            && !orderClause.contains(RawContacts.RAW_CONTACT_IS_USER_PROFILE)) {
                        if (rejoinedClause.length() > 0) {
                            rejoinedClause.append(", ");
                        }
                        rejoinedClause.append(orderClause.trim());
                    }
                }
                sortOrder = rejoinedClause.toString();
            }

            int spaceIndex = sortOrder.indexOf(' ');
            if (spaceIndex != -1) {
                sortKey = sortOrder.substring(0, spaceIndex);
                sortOrderSuffix = sortOrder.substring(spaceIndex);
            } else {
                sortKey = sortOrder;
            }
        } else {
            sortKey = Contacts.SORT_KEY_PRIMARY;
        }

        String locale = getLocale().toString();
        HashMap<String, String> projectionMap = Maps.newHashMap();

        // The user profile column varies depending on the view.
        String profileColumn = qb.getTables().contains(mDbHelper.getContactView())
                ? Contacts.IS_USER_PROFILE
                : RawContacts.RAW_CONTACT_IS_USER_PROFILE;
        String sectionHeading = String.format(
                AddressBookIndexQuery.SECTION_HEADING_TEMPLATE, profileColumn, sortKey);
        projectionMap.put(AddressBookIndexQuery.LETTER,
                sectionHeading + " AS " + AddressBookIndexQuery.LETTER);

        /**
         * Use the GET_PHONEBOOK_INDEX function, which is an android extension for SQLite3,
         * to map the first letter of the sort key to a character that is traditionally
         * used in phonebooks to represent that letter.  For example, in Korean it will
         * be the first consonant in the letter; for Japanese it will be Hiragana rather
         * than Katakana.
         */
        projectionMap.put(AddressBookIndexQuery.TITLE,
                "GET_PHONEBOOK_INDEX(" + sectionHeading + ",'" + locale + "')"
                        + " AS " + AddressBookIndexQuery.TITLE);
        projectionMap.put(AddressBookIndexQuery.COUNT,
                "COUNT(" + Contacts._ID + ") AS " + AddressBookIndexQuery.COUNT);
        qb.setProjectionMap(projectionMap);

        Cursor indexCursor = qb.query(db, AddressBookIndexQuery.COLUMNS, selection, selectionArgs,
                AddressBookIndexQuery.ORDER_BY, null /* having */,
                AddressBookIndexQuery.ORDER_BY + sortOrderSuffix);

        try {
            int groupCount = indexCursor.getCount();
            String titles[] = new String[groupCount];
            int counts[] = new int[groupCount];
            int indexCount = 0;
            String currentTitle = null;

            // Since GET_PHONEBOOK_INDEX is a many-to-1 function, we may end up
            // with multiple entries for the same title.  The following code
            // collapses those duplicates.
            for (int i = 0; i < groupCount; i++) {
                indexCursor.moveToNext();
                String title = indexCursor.getString(AddressBookIndexQuery.COLUMN_TITLE);
                int count = indexCursor.getInt(AddressBookIndexQuery.COLUMN_COUNT);
                if (indexCount == 0 || !TextUtils.equals(title, currentTitle)) {
                    titles[indexCount] = currentTitle = title;
                    counts[indexCount] = count;
                    indexCount++;
                } else {
                    counts[indexCount - 1] += count;
                }
            }

            if (indexCount < groupCount) {
                String[] newTitles = new String[indexCount];
                System.arraycopy(titles, 0, newTitles, 0, indexCount);
                titles = newTitles;

                int[] newCounts = new int[indexCount];
                System.arraycopy(counts, 0, newCounts, 0, indexCount);
                counts = newCounts;
            }

            return new AddressBookCursor((CrossProcessCursor) cursor, titles, counts);
        } finally {
            indexCursor.close();
        }
    }

    /**
     * Returns the contact Id for the contact identified by the lookupKey.
     * Robust against changes in the lookup key: if the key has changed, will
     * look up the contact by the raw contact IDs or name encoded in the lookup
     * key.
     */
    public long lookupContactIdByLookupKey(SQLiteDatabase db, String lookupKey) {
        ContactLookupKey key = new ContactLookupKey();
        ArrayList<LookupKeySegment> segments = key.parse(lookupKey);

        long contactId = -1;
        if (lookupKeyContainsType(segments, ContactLookupKey.LOOKUP_TYPE_SOURCE_ID)) {
            contactId = lookupContactIdBySourceIds(db, segments);
            if (contactId != -1) {
                return contactId;
            }
        }

        boolean hasRawContactIds =
                lookupKeyContainsType(segments, ContactLookupKey.LOOKUP_TYPE_RAW_CONTACT_ID);
        if (hasRawContactIds) {
            contactId = lookupContactIdByRawContactIds(db, segments);
            if (contactId != -1) {
                return contactId;
            }
        }

        if (hasRawContactIds
                || lookupKeyContainsType(segments, ContactLookupKey.LOOKUP_TYPE_DISPLAY_NAME)) {
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
        StringBuilder sb = new StringBuilder();
        sb.append(RawContacts.SOURCE_ID + " IN (");
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.lookupType == ContactLookupKey.LOOKUP_TYPE_SOURCE_ID) {
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
                    if (segment.lookupType == ContactLookupKey.LOOKUP_TYPE_SOURCE_ID
                            && accountHashCode == segment.accountHashCode
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

    private interface LookupByRawContactIdQuery {
        String TABLE = Tables.RAW_CONTACTS;

        String COLUMNS[] = {
                RawContacts.CONTACT_ID,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.ACCOUNT_NAME,
                RawContacts._ID,
        };

        int CONTACT_ID = 0;
        int ACCOUNT_TYPE = 1;
        int ACCOUNT_NAME = 2;
        int ID = 3;
    }

    private long lookupContactIdByRawContactIds(SQLiteDatabase db,
            ArrayList<LookupKeySegment> segments) {
        StringBuilder sb = new StringBuilder();
        sb.append(RawContacts._ID + " IN (");
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.lookupType == ContactLookupKey.LOOKUP_TYPE_RAW_CONTACT_ID) {
                sb.append(segment.rawContactId);
                sb.append(",");
            }
        }
        sb.setLength(sb.length() - 1);      // Last comma
        sb.append(") AND " + RawContacts.CONTACT_ID + " NOT NULL");

        Cursor c = db.query(LookupByRawContactIdQuery.TABLE, LookupByRawContactIdQuery.COLUMNS,
                 sb.toString(), null, null, null, null);
        try {
            while (c.moveToNext()) {
                String accountType = c.getString(LookupByRawContactIdQuery.ACCOUNT_TYPE);
                String accountName = c.getString(LookupByRawContactIdQuery.ACCOUNT_NAME);
                int accountHashCode =
                        ContactLookupKey.getAccountHashCode(accountType, accountName);
                String rawContactId = c.getString(LookupByRawContactIdQuery.ID);
                for (int i = 0; i < segments.size(); i++) {
                    LookupKeySegment segment = segments.get(i);
                    if (segment.lookupType == ContactLookupKey.LOOKUP_TYPE_RAW_CONTACT_ID
                            && accountHashCode == segment.accountHashCode
                            && segment.rawContactId.equals(rawContactId)) {
                        segment.contactId = c.getLong(LookupByRawContactIdQuery.CONTACT_ID);
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
        StringBuilder sb = new StringBuilder();
        sb.append(NameLookupColumns.NORMALIZED_NAME + " IN (");
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.lookupType == ContactLookupKey.LOOKUP_TYPE_DISPLAY_NAME
                    || segment.lookupType == ContactLookupKey.LOOKUP_TYPE_RAW_CONTACT_ID) {
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
                    if ((segment.lookupType == ContactLookupKey.LOOKUP_TYPE_DISPLAY_NAME
                            || segment.lookupType == ContactLookupKey.LOOKUP_TYPE_RAW_CONTACT_ID)
                            && accountHashCode == segment.accountHashCode
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

    private boolean lookupKeyContainsType(ArrayList<LookupKeySegment> segments, int lookupType) {
        for (int i = 0; i < segments.size(); i++) {
            LookupKeySegment segment = segments.get(i);
            if (segment.lookupType == lookupType) {
                return true;
            }
        }

        return false;
    }

    public void updateLookupKeyForRawContact(SQLiteDatabase db, long rawContactId) {
        mContactAggregator.updateLookupKeyForRawContact(db, rawContactId);
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
        sb.append(mDbHelper.getContactView(shouldExcludeRestrictedData(uri)));
        appendContactPresenceJoin(sb, projection, Contacts._ID);
        appendContactStatusUpdateJoin(sb, projection, ContactsColumns.LAST_STATUS_UPDATE_ID);
        qb.setTables(sb.toString());
        qb.setProjectionMap(sContactsProjectionMap);
    }

    /**
     * Finds name lookup records matching the supplied filter, picks one arbitrary match per
     * contact and joins that with other contacts tables.
     */
    private void setTablesAndProjectionMapForContactsWithSnippet(SQLiteQueryBuilder qb, Uri uri,
            String[] projection, String filter, long directoryId) {

        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getContactView(shouldExcludeRestrictedData(uri)));

        if (filter != null) {
            filter = filter.trim();
        }

        if (TextUtils.isEmpty(filter) || (directoryId != -1 && directoryId != Directory.DEFAULT)) {
            sb.append(" JOIN (SELECT NULL AS " + SearchSnippetColumns.SNIPPET + " WHERE 0)");
        } else {
            appendSearchIndexJoin(sb, uri, projection, filter);
        }
        appendContactPresenceJoin(sb, projection, Contacts._ID);
        appendContactStatusUpdateJoin(sb, projection, ContactsColumns.LAST_STATUS_UPDATE_ID);
        qb.setTables(sb.toString());
        qb.setProjectionMap(sContactsProjectionWithSnippetMap);
    }

    private void appendSearchIndexJoin(
            StringBuilder sb, Uri uri, String[] projection, String filter) {

        if (mDbHelper.isInProjection(projection, SearchSnippetColumns.SNIPPET)) {
            String[] args = null;
            String snippetArgs =
                    getQueryParameter(uri, SearchSnippetColumns.SNIPPET_ARGS_PARAM_KEY);
            if (snippetArgs != null) {
                args = snippetArgs.split(",");
            }

            String startMatch = args != null && args.length > 0 ? args[0]
                    : DEFAULT_SNIPPET_ARG_START_MATCH;
            String endMatch = args != null && args.length > 1 ? args[1]
                    : DEFAULT_SNIPPET_ARG_END_MATCH;
            String ellipsis = args != null && args.length > 2 ? args[2]
                    : DEFAULT_SNIPPET_ARG_ELLIPSIS;
            int maxTokens = args != null && args.length > 3 ? Integer.parseInt(args[3])
                    : DEFAULT_SNIPPET_ARG_MAX_TOKENS;

            appendSearchIndexJoin(
                    sb, filter, true, startMatch, endMatch, ellipsis, maxTokens);
        } else {
            appendSearchIndexJoin(sb, filter, false, null, null, null, 0);
        }
    }

    public void appendSearchIndexJoin(StringBuilder sb, String filter,
            boolean snippetNeeded, String startMatch, String endMatch, String ellipsis,
            int maxTokens) {
        boolean isEmailAddress = false;
        String emailAddress = null;
        boolean isPhoneNumber = false;
        String phoneNumber = null;
        String numberE164 = null;

        // If the query consists of a single word, we can do snippetizing after-the-fact for a
        // performance boost.
        boolean singleTokenSearch = filter.split(QUERY_TOKENIZER_REGEX).length == 1;

        if (filter.indexOf('@') != -1) {
            emailAddress = mDbHelper.extractAddressFromEmailAddress(filter);
            isEmailAddress = !TextUtils.isEmpty(emailAddress);
        } else {
            isPhoneNumber = isPhoneNumber(filter);
            if (isPhoneNumber) {
                phoneNumber = PhoneNumberUtils.normalizeNumber(filter);
                numberE164 = PhoneNumberUtils.formatNumberToE164(phoneNumber,
                        mDbHelper.getCountryIso());
            }
        }

        sb.append(" JOIN (SELECT " + SearchIndexColumns.CONTACT_ID + " AS snippet_contact_id");
        if (snippetNeeded) {
            sb.append(", ");
            if (isEmailAddress) {
                sb.append("ifnull(");
                DatabaseUtils.appendEscapedSQLString(sb, startMatch);
                sb.append("||(SELECT MIN(" + Email.ADDRESS + ")");
                sb.append(" FROM " + Tables.DATA_JOIN_RAW_CONTACTS);
                sb.append(" WHERE  " + Tables.SEARCH_INDEX + "." + SearchIndexColumns.CONTACT_ID);
                sb.append("=" + RawContacts.CONTACT_ID + " AND " + Email.ADDRESS + " LIKE ");
                DatabaseUtils.appendEscapedSQLString(sb, filter + "%");
                sb.append(")||");
                DatabaseUtils.appendEscapedSQLString(sb, endMatch);
                sb.append(",");

                // Optimization for single-token search.
                if (singleTokenSearch) {
                    sb.append(SearchIndexColumns.CONTENT);
                } else {
                    appendSnippetFunction(sb, startMatch, endMatch, ellipsis, maxTokens);
                }
                sb.append(")");
            } else if (isPhoneNumber) {
                sb.append("ifnull(");
                DatabaseUtils.appendEscapedSQLString(sb, startMatch);
                sb.append("||(SELECT MIN(" + Phone.NUMBER + ")");
                sb.append(" FROM " +
                        Tables.DATA_JOIN_RAW_CONTACTS + " JOIN " + Tables.PHONE_LOOKUP);
                sb.append(" ON " + DataColumns.CONCRETE_ID);
                sb.append("=" + Tables.PHONE_LOOKUP + "." + PhoneLookupColumns.DATA_ID);
                sb.append(" WHERE  " + Tables.SEARCH_INDEX + "." + SearchIndexColumns.CONTACT_ID);
                sb.append("=" + RawContacts.CONTACT_ID);
                sb.append(" AND " + PhoneLookupColumns.NORMALIZED_NUMBER + " LIKE '");
                sb.append(phoneNumber);
                sb.append("%'");
                if (!TextUtils.isEmpty(numberE164)) {
                    sb.append(" OR " + PhoneLookupColumns.NORMALIZED_NUMBER + " LIKE '");
                    sb.append(numberE164);
                    sb.append("%'");
                }
                sb.append(")||");
                DatabaseUtils.appendEscapedSQLString(sb, endMatch);
                sb.append(",");

                // Optimization for single-token search.
                if (singleTokenSearch) {
                    sb.append(SearchIndexColumns.CONTENT);
                } else {
                    appendSnippetFunction(sb, startMatch, endMatch, ellipsis, maxTokens);
                }
                sb.append(")");
            } else {
                final String normalizedFilter = NameNormalizer.normalize(filter);
                if (!TextUtils.isEmpty(normalizedFilter)) {
                    // Optimization for single-token search.
                    if (singleTokenSearch) {
                        sb.append(SearchIndexColumns.CONTENT);
                    } else {
                        sb.append("(CASE WHEN EXISTS (SELECT 1 FROM ");
                        sb.append(Tables.RAW_CONTACTS + " AS rc INNER JOIN ");
                        sb.append(Tables.NAME_LOOKUP + " AS nl ON (rc." + RawContacts._ID);
                        sb.append("=nl." + NameLookupColumns.RAW_CONTACT_ID);
                        sb.append(") WHERE nl." + NameLookupColumns.NORMALIZED_NAME);
                        sb.append(" GLOB '" + normalizedFilter + "*' AND ");
                        sb.append("nl." + NameLookupColumns.NAME_TYPE + "=");
                        sb.append(NameLookupType.NAME_COLLATION_KEY + " AND ");
                        sb.append(Tables.SEARCH_INDEX + "." + SearchIndexColumns.CONTACT_ID);
                        sb.append("=rc." + RawContacts.CONTACT_ID);
                        sb.append(") THEN NULL ELSE ");
                        appendSnippetFunction(sb, startMatch, endMatch, ellipsis, maxTokens);
                        sb.append(" END)");
                    }
                } else {
                    sb.append("NULL");
                }
            }
            sb.append(" AS " + SearchSnippetColumns.SNIPPET);
        }

        sb.append(" FROM " + Tables.SEARCH_INDEX);
        sb.append(" WHERE ");
        sb.append(Tables.SEARCH_INDEX + " MATCH ");
        if (isEmailAddress) {
            DatabaseUtils.appendEscapedSQLString(sb, "\"" + sanitizeMatch(filter) + "*\"");
        } else if (isPhoneNumber) {
            DatabaseUtils.appendEscapedSQLString(sb,
                    "\"" + sanitizeMatch(filter) + "*\" OR \"" + phoneNumber + "*\""
                            + (numberE164 != null ? " OR \"" + numberE164 + "\"" : ""));
        } else {
            DatabaseUtils.appendEscapedSQLString(sb, sanitizeMatch(filter) + "*");
        }
        sb.append(") ON (" + Contacts._ID + "=snippet_contact_id)");
    }

    private String sanitizeMatch(String filter) {
        // TODO more robust preprocessing of match expressions
        return filter.replace('-', ' ').replace('\"', ' ');
    }

    private void appendSnippetFunction(
            StringBuilder sb, String startMatch, String endMatch, String ellipsis, int maxTokens) {
        sb.append("snippet(" + Tables.SEARCH_INDEX + ",");
        DatabaseUtils.appendEscapedSQLString(sb, startMatch);
        sb.append(",");
        DatabaseUtils.appendEscapedSQLString(sb, endMatch);
        sb.append(",");
        DatabaseUtils.appendEscapedSQLString(sb, ellipsis);

        // The index of the column used for the snippet, "content"
        sb.append(",1,");
        sb.append(maxTokens);
        sb.append(")");
    }

    private void setTablesAndProjectionMapForRawContacts(SQLiteQueryBuilder qb, Uri uri) {
        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getRawContactView(shouldExcludeRestrictedData(uri)));
        qb.setTables(sb.toString());
        qb.setProjectionMap(sRawContactsProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTablesAndProjectionMapForRawEntities(SQLiteQueryBuilder qb, Uri uri) {
        qb.setTables(mDbHelper.getRawEntitiesView(shouldExcludeRestrictedData(uri)));
        qb.setProjectionMap(sRawEntityProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTablesAndProjectionMapForData(SQLiteQueryBuilder qb, Uri uri,
            String[] projection, boolean distinct) {
        setTablesAndProjectionMapForData(qb, uri, projection, distinct, null);
    }

    /**
     * @param usageType when non-null {@link Tables#DATA_USAGE_STAT} is joined with the specified
     * type.
     */
    private void setTablesAndProjectionMapForData(SQLiteQueryBuilder qb, Uri uri,
            String[] projection, boolean distinct, Integer usageType) {
        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getDataView(shouldExcludeRestrictedData(uri)));
        sb.append(" data");

        appendContactPresenceJoin(sb, projection, RawContacts.CONTACT_ID);
        appendContactStatusUpdateJoin(sb, projection, ContactsColumns.LAST_STATUS_UPDATE_ID);
        appendDataPresenceJoin(sb, projection, DataColumns.CONCRETE_ID);
        appendDataStatusUpdateJoin(sb, projection, DataColumns.CONCRETE_ID);

        if (usageType != null) {
            appendDataUsageStatJoin(sb, usageType, DataColumns.CONCRETE_ID);
        }

        qb.setTables(sb.toString());

        boolean useDistinct = distinct
                || !mDbHelper.isInProjection(projection, DISTINCT_DATA_PROHIBITING_COLUMNS);
        qb.setDistinct(useDistinct);
        qb.setProjectionMap(useDistinct ? sDistinctDataProjectionMap : sDataProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void setTableAndProjectionMapForStatusUpdates(SQLiteQueryBuilder qb,
            String[] projection) {
        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getDataView());
        sb.append(" data");
        appendDataPresenceJoin(sb, projection, DataColumns.CONCRETE_ID);
        appendDataStatusUpdateJoin(sb, projection, DataColumns.CONCRETE_ID);

        qb.setTables(sb.toString());
        qb.setProjectionMap(sStatusUpdatesProjectionMap);
    }

    private void setTablesAndProjectionMapForEntities(SQLiteQueryBuilder qb, Uri uri,
            String[] projection) {
        StringBuilder sb = new StringBuilder();
        sb.append(mDbHelper.getEntitiesView(shouldExcludeRestrictedData(uri)));
        sb.append(" data");

        appendContactPresenceJoin(sb, projection, Contacts.Entity.CONTACT_ID);
        appendContactStatusUpdateJoin(sb, projection, ContactsColumns.LAST_STATUS_UPDATE_ID);
        appendDataPresenceJoin(sb, projection, Contacts.Entity.DATA_ID);
        appendDataStatusUpdateJoin(sb, projection, Contacts.Entity.DATA_ID);

        qb.setTables(sb.toString());
        qb.setProjectionMap(sEntityProjectionMap);
        appendAccountFromParameter(qb, uri);
    }

    private void appendContactStatusUpdateJoin(StringBuilder sb, String[] projection,
            String lastStatusUpdateIdColumn) {
        if (mDbHelper.isInProjection(projection,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES + " "
                    + ContactsStatusUpdatesColumns.ALIAS +
                    " ON (" + lastStatusUpdateIdColumn + "="
                            + ContactsStatusUpdatesColumns.CONCRETE_DATA_ID + ")");
        }
    }

    private void appendDataStatusUpdateJoin(StringBuilder sb, String[] projection,
            String dataIdColumn) {
        if (mDbHelper.isInProjection(projection,
                StatusUpdates.STATUS,
                StatusUpdates.STATUS_RES_PACKAGE,
                StatusUpdates.STATUS_ICON,
                StatusUpdates.STATUS_LABEL,
                StatusUpdates.STATUS_TIMESTAMP)) {
            sb.append(" LEFT OUTER JOIN " + Tables.STATUS_UPDATES +
                    " ON (" + StatusUpdatesColumns.CONCRETE_DATA_ID + "="
                            + dataIdColumn + ")");
        }
    }

    private void appendDataUsageStatJoin(StringBuilder sb, int usageType, String dataIdColumn) {
        sb.append(" LEFT OUTER JOIN " + Tables.DATA_USAGE_STAT +
                " ON (" + DataUsageStatColumns.CONCRETE_DATA_ID + "=" + dataIdColumn +
                " AND " + DataUsageStatColumns.CONCRETE_USAGE_TYPE + "=" + usageType + ")");
    }

    private void appendContactPresenceJoin(StringBuilder sb, String[] projection,
            String contactIdColumn) {
        if (mDbHelper.isInProjection(projection,
                Contacts.CONTACT_PRESENCE, Contacts.CONTACT_CHAT_CAPABILITY)) {
            sb.append(" LEFT OUTER JOIN " + Tables.AGGREGATED_PRESENCE +
                    " ON (" + contactIdColumn + " = "
                            + AggregatedPresenceColumns.CONCRETE_CONTACT_ID + ")");
        }
    }

    private void appendDataPresenceJoin(StringBuilder sb, String[] projection,
            String dataIdColumn) {
        if (mDbHelper.isInProjection(projection, Data.PRESENCE, Data.CHAT_CAPABILITY)) {
            sb.append(" LEFT OUTER JOIN " + Tables.PRESENCE +
                    " ON (" + StatusUpdates.DATA_ID + "=" + dataIdColumn + ")");
        }
    }

    private boolean appendLocalDirectorySelectionIfNeeded(SQLiteQueryBuilder qb, long directoryId) {
        if (directoryId == Directory.DEFAULT) {
            qb.appendWhere(Contacts._ID + " IN " + Tables.DEFAULT_DIRECTORY);
            return true;
        } else if (directoryId == Directory.LOCAL_INVISIBLE){
            qb.appendWhere(Contacts._ID + " NOT IN " + Tables.DEFAULT_DIRECTORY);
            return true;
        }
        return false;
    }

    private void appendProfileRestriction(SQLiteQueryBuilder qb, Uri uri, String profileColumn,
            boolean andRequired) {
        if (!shouldIncludeProfile(uri)) {
            qb.appendWhere((andRequired ? " AND (" : "")
                    + profileColumn + " IS NULL OR "
                    + profileColumn + "=0"
                    + (andRequired ? ")" : ""));
        }
    }

    private String prependProfileSortIfNeeded(Uri uri, String sortOrder) {
        if (shouldIncludeProfile(uri)) {
            if (TextUtils.isEmpty(sortOrder)) {
                return Contacts.IS_USER_PROFILE + " DESC";
            } else {
                return Contacts.IS_USER_PROFILE + " DESC, " + sortOrder;
            }
        }
        return sortOrder;
    }

    private boolean shouldIncludeProfile(Uri uri) {
        // The user's profile may be returned alongside other contacts if it was requested and
        // the calling application has permission to read profile data.
        boolean profileRequested = readBooleanQueryParameter(uri, ContactsContract.ALLOW_PROFILE,
                false);
        if (profileRequested) {
            enforceProfilePermission(false);
        }
        return profileRequested;
    }

    private boolean shouldExcludeRestrictedData(Uri uri) {
        // Note: currently, "export only" equals to "restricted", but may not in the future.
        boolean excludeRestrictedData = readBooleanQueryParameter(uri,
                Data.FOR_EXPORT_ONLY, false);
        if (excludeRestrictedData) {
            return true;
        }

        String requestingPackage = getQueryParameter(uri,
                ContactsContract.REQUESTING_PACKAGE_PARAM_KEY);
        if (requestingPackage != null) {
            return !mDbHelper.hasAccessToRestrictedData(requestingPackage);
        }

        return false;
    }

    private void appendAccountFromParameter(SQLiteQueryBuilder qb, Uri uri) {
        final String accountName = getQueryParameter(uri, RawContacts.ACCOUNT_NAME);
        final String accountType = getQueryParameter(uri, RawContacts.ACCOUNT_TYPE);

        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);
        if (partialUri) {
            // Throw when either account is incomplete
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri));
        }

        // Accounts are valid by only checking one parameter, since we've
        // already ruled out partial accounts.
        final boolean validAccount = !TextUtils.isEmpty(accountName);
        if (validAccount) {
            qb.appendWhere(RawContacts.ACCOUNT_NAME + "="
                    + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                    + RawContacts.ACCOUNT_TYPE + "="
                    + DatabaseUtils.sqlEscapeString(accountType));
        } else {
            qb.appendWhere("1");
        }
    }

    private String appendAccountToSelection(Uri uri, String selection) {
        final String accountName = getQueryParameter(uri, RawContacts.ACCOUNT_NAME);
        final String accountType = getQueryParameter(uri, RawContacts.ACCOUNT_TYPE);

        final boolean partialUri = TextUtils.isEmpty(accountName) ^ TextUtils.isEmpty(accountType);
        if (partialUri) {
            // Throw when either account is incomplete
            throw new IllegalArgumentException(mDbHelper.exceptionMessage(
                    "Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE", uri));
        }

        // Accounts are valid by only checking one parameter, since we've
        // already ruled out partial accounts.
        final boolean validAccount = !TextUtils.isEmpty(accountName);
        if (validAccount) {
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
    private String getLimit(Uri uri) {
        String limitParam = getQueryParameter(uri, ContactsContract.LIMIT_PARAM_KEY);
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
        if (mDbHelper.hasAccessToRestrictedData()) {
            return "1";
        } else {
            return RawContactsColumns.CONCRETE_IS_RESTRICTED + "=0";
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

        waitForAccess(mReadAccessLatch);

        int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS_ID_PHOTO: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                enforceProfilePermissionForRawContact(rawContactId, false);
                return openPhotoAssetFile(uri, mode,
                        Data._ID + "=" + Contacts.PHOTO_ID + " AND " +
                                RawContacts.CONTACT_ID + "=?",
                        new String[]{String.valueOf(rawContactId)});
            }

            case DATA_ID: {
                long dataId = Long.parseLong(uri.getPathSegments().get(1));
                enforceProfilePermissionForData(dataId, false);
                return openPhotoAssetFile(uri, mode,
                        Data._ID + "=? AND " + Data.MIMETYPE + "='" + Photo.CONTENT_ITEM_TYPE + "'",
                        new String[]{String.valueOf(dataId)});
            }

            case PROFILE_AS_VCARD: {
                // When opening a contact as file, we pass back contents as a
                // vCard-encoded stream. We build into a local buffer first,
                // then pipe into MemoryFile once the exact size is known.
                final ByteArrayOutputStream localStream = new ByteArrayOutputStream();
                outputRawContactsAsVCard(uri, localStream, null, null);
                return buildAssetFileDescriptor(localStream);
            }

            case CONTACTS_AS_VCARD: {
                // When opening a contact as file, we pass back contents as a
                // vCard-encoded stream. We build into a local buffer first,
                // then pipe into MemoryFile once the exact size is known.
                final ByteArrayOutputStream localStream = new ByteArrayOutputStream();
                outputRawContactsAsVCard(uri, localStream, null, null);
                return buildAssetFileDescriptor(localStream);
            }

            case CONTACTS_AS_MULTI_VCARD: {
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                final String lookupKeys = uri.getPathSegments().get(2);
                final String[] loopupKeyList = lookupKeys.split(":");
                final StringBuilder inBuilder = new StringBuilder();
                Uri queryUri = Contacts.CONTENT_URI;
                int index = 0;

                mProfileIdCache.init(mDb, false);

                // SQLite has limits on how many parameters can be used
                // so the IDs are concatenated to a query string here instead
                for (String lookupKey : loopupKeyList) {
                    if (index == 0) {
                        inBuilder.append("(");
                    } else {
                        inBuilder.append(",");
                    }
                    long contactId = lookupContactIdByLookupKey(db, lookupKey);
                    enforceProfilePermissionForContact(contactId, false);
                    inBuilder.append(contactId);
                    if (mProfileIdCache.profileContactId == contactId) {
                        queryUri = queryUri.buildUpon().appendQueryParameter(
                                ContactsContract.ALLOW_PROFILE, "true").build();
                    }
                    index++;
                }
                inBuilder.append(')');
                final String selection = Contacts._ID + " IN " + inBuilder.toString();

                // When opening a contact as file, we pass back contents as a
                // vCard-encoded stream. We build into a local buffer first,
                // then pipe into MemoryFile once the exact size is known.
                final ByteArrayOutputStream localStream = new ByteArrayOutputStream();
                outputRawContactsAsVCard(queryUri, localStream, selection, null);
                return buildAssetFileDescriptor(localStream);
            }

            default:
                throw new FileNotFoundException(mDbHelper.exceptionMessage("File does not exist",
                        uri));
        }
    }

    private AssetFileDescriptor openPhotoAssetFile(Uri uri, String mode, String selection,
            String[] selectionArgs)
            throws FileNotFoundException {
        if (!"r".equals(mode)) {
            throw new FileNotFoundException(mDbHelper.exceptionMessage("Mode " + mode
                    + " not supported.", uri));
        }

        String sql =
                "SELECT " + Photo.PHOTO + " FROM " + mDbHelper.getDataView() +
                " WHERE " + selection;
        SQLiteDatabase db = mDbHelper.getReadableDatabase();
        try {
            return makeAssetFileDescriptor(
                    DatabaseUtils.blobFileDescriptorForQuery(db, sql, selectionArgs));
        } catch (SQLiteDoneException e) {
            // this will happen if the DB query returns no rows (i.e. contact does not exist)
            throw new FileNotFoundException(uri.toString());
        }
    }

    private static final String CONTACT_MEMORY_FILE_NAME = "contactAssetFile";

    /**
     * Returns an {@link AssetFileDescriptor} backed by the
     * contents of the given {@link ByteArrayOutputStream}.
     */
    private AssetFileDescriptor buildAssetFileDescriptor(ByteArrayOutputStream stream) {
        try {
            stream.flush();

            final byte[] byteData = stream.toByteArray();

            return makeAssetFileDescriptor(
                    ParcelFileDescriptor.fromData(byteData, CONTACT_MEMORY_FILE_NAME),
                    byteData.length);
        } catch (IOException e) {
            Log.w(TAG, "Problem writing stream into an ParcelFileDescriptor: " + e.toString());
            return null;
        }
    }

    private AssetFileDescriptor makeAssetFileDescriptor(ParcelFileDescriptor fd) {
        return makeAssetFileDescriptor(fd, AssetFileDescriptor.UNKNOWN_LENGTH);
    }

    private AssetFileDescriptor makeAssetFileDescriptor(ParcelFileDescriptor fd, long length) {
        return fd != null ? new AssetFileDescriptor(fd, 0, length) : null;
    }

    /**
     * Output {@link RawContacts} matching the requested selection in the vCard
     * format to the given {@link OutputStream}. This method returns silently if
     * any errors encountered.
     */
    private void outputRawContactsAsVCard(Uri uri, OutputStream stream,
            String selection, String[] selectionArgs) {
        final Context context = this.getContext();
        int vcardconfig = VCardConfig.VCARD_TYPE_DEFAULT;
        if(uri.getBooleanQueryParameter(
                Contacts.QUERY_PARAMETER_VCARD_NO_PHOTO, false)) {
            vcardconfig |= VCardConfig.FLAG_REFRAIN_IMAGE_EXPORT;
        }
        final VCardComposer composer =
                new VCardComposer(context, vcardconfig, false);
        Writer writer = null;
        try {
            // No extra checks since composer always uses restricted views.
            writer = new BufferedWriter(new OutputStreamWriter(stream));
            if (!composer.init(uri, selection, selectionArgs, null)) {
                Log.w(TAG, "Failed to init VCardComposer");
                return;
            }

            while (!composer.isAfterLast()) {
                writer.write(composer.createOneEntry());
            }
        } catch (IOException e) {
            Log.e(TAG, "IOException: " + e);
        } finally {
            composer.terminate();
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    Log.w(TAG, "IOException during closing output stream: " + e);
                }
            }
        }
    }

    @Override
    public String getType(Uri uri) {

        waitForAccess(mReadAccessLatch);

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS:
                return Contacts.CONTENT_TYPE;
            case CONTACTS_LOOKUP:
            case CONTACTS_ID:
            case CONTACTS_LOOKUP_ID:
            case PROFILE:
                return Contacts.CONTENT_ITEM_TYPE;
            case CONTACTS_AS_VCARD:
            case CONTACTS_AS_MULTI_VCARD:
            case PROFILE_AS_VCARD:
                return Contacts.CONTENT_VCARD_TYPE;
            case CONTACTS_ID_PHOTO:
                return "image/png";
            case RAW_CONTACTS:
            case PROFILE_RAW_CONTACTS:
                return RawContacts.CONTENT_TYPE;
            case RAW_CONTACTS_ID:
            case PROFILE_RAW_CONTACTS_ID:
                return RawContacts.CONTENT_ITEM_TYPE;
            case DATA:
            case PROFILE_DATA:
                return Data.CONTENT_TYPE;
            case DATA_ID:
                return mDbHelper.getDataMimeType(ContentUris.parseId(uri));
            case PHONES:
                return Phone.CONTENT_TYPE;
            case PHONES_ID:
                return Phone.CONTENT_ITEM_TYPE;
            case PHONE_LOOKUP:
                return PhoneLookup.CONTENT_TYPE;
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
            case DIRECTORIES:
                return Directory.CONTENT_TYPE;
            case DIRECTORIES_ID:
                return Directory.CONTENT_ITEM_TYPE;
            default:
                return mLegacyApiSupport.getType(uri);
        }
    }

    public String[] getDefaultProjection(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS:
            case CONTACTS_LOOKUP:
            case CONTACTS_ID:
            case CONTACTS_LOOKUP_ID:
            case AGGREGATION_SUGGESTIONS:
            case PROFILE:
                return sContactsProjectionMap.getColumnNames();

            case CONTACTS_ID_ENTITIES:
            case PROFILE_ENTITIES:
                return sEntityProjectionMap.getColumnNames();

            case CONTACTS_AS_VCARD:
            case CONTACTS_AS_MULTI_VCARD:
            case PROFILE_AS_VCARD:
                return sContactsVCardProjectionMap.getColumnNames();

            case RAW_CONTACTS:
            case RAW_CONTACTS_ID:
            case PROFILE_RAW_CONTACTS:
            case PROFILE_RAW_CONTACTS_ID:
                return sRawContactsProjectionMap.getColumnNames();

            case DATA_ID:
            case PHONES:
            case PHONES_ID:
            case EMAILS:
            case EMAILS_ID:
            case POSTALS:
            case POSTALS_ID:
            case PROFILE_DATA:
                return sDataProjectionMap.getColumnNames();

            case PHONE_LOOKUP:
                return sPhoneLookupProjectionMap.getColumnNames();

            case AGGREGATION_EXCEPTIONS:
            case AGGREGATION_EXCEPTION_ID:
                return sAggregationExceptionsProjectionMap.getColumnNames();

            case SETTINGS:
                return sSettingsProjectionMap.getColumnNames();

            case DIRECTORIES:
            case DIRECTORIES_ID:
                return sDirectoryProjectionMap.getColumnNames();

            default:
                return null;
        }
    }

    private class StructuredNameLookupBuilder extends NameLookupBuilder {

        public StructuredNameLookupBuilder(NameSplitter splitter) {
            super(splitter);
        }

        @Override
        protected void insertNameLookup(long rawContactId, long dataId, int lookupType,
                String name) {
            mDbHelper.insertNameLookup(rawContactId, dataId, lookupType, name);
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return mCommonNicknameCache.getCommonNicknameClusters(normalizedName);
        }
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
        sb.append("*' AND " + NameLookupColumns.NAME_TYPE +
                    " IN(" + CONTACT_LOOKUP_NAME_TYPES + "))");
    }

    public boolean isPhoneNumber(String filter) {
        boolean atLeastOneDigit = false;
        int len = filter.length();
        for (int i = 0; i < len; i++) {
            char c = filter.charAt(i);
            if (c >= '0' && c <= '9') {
                atLeastOneDigit = true;
            } else if (c != '*' && c != '#' && c != '+' && c != 'N' && c != '.' && c != ';'
                    && c != '-' && c != '(' && c != ')' && c != ' ') {
                return false;
            }
        }
        return atLeastOneDigit;
    }

    /**
     * Takes components of a name from the query parameters and returns a cursor with those
     * components as well as all missing components.  There is no database activity involved
     * in this so the call can be made on the UI thread.
     */
    private Cursor completeName(Uri uri, String[] projection) {
        if (projection == null) {
            projection = sDataProjectionMap.getColumnNames();
        }

        ContentValues values = new ContentValues();
        DataRowHandlerForStructuredName handler = (DataRowHandlerForStructuredName)
                getDataRowHandler(StructuredName.CONTENT_ITEM_TYPE);

        copyQueryParamsToContentValues(values, uri,
                StructuredName.DISPLAY_NAME,
                StructuredName.PREFIX,
                StructuredName.GIVEN_NAME,
                StructuredName.MIDDLE_NAME,
                StructuredName.FAMILY_NAME,
                StructuredName.SUFFIX,
                StructuredName.PHONETIC_NAME,
                StructuredName.PHONETIC_FAMILY_NAME,
                StructuredName.PHONETIC_MIDDLE_NAME,
                StructuredName.PHONETIC_GIVEN_NAME
        );

        handler.fixStructuredNameComponents(values, values);

        MatrixCursor cursor = new MatrixCursor(projection);
        Object[] row = new Object[projection.length];
        for (int i = 0; i < projection.length; i++) {
            row[i] = values.get(projection[i]);
        }
        cursor.addRow(row);
        return cursor;
    }

    private void copyQueryParamsToContentValues(ContentValues values, Uri uri, String... columns) {
        for (String column : columns) {
            String param = uri.getQueryParameter(column);
            if (param != null) {
                values.put(column, param);
            }
        }
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
            Account[] accounts = accountManager.getAccountsByType(DEFAULT_ACCOUNT_TYPE);
            if (accounts != null && accounts.length > 0) {
                return accounts[0];
            }
        } catch (Throwable e) {
            Log.e(TAG, "Cannot determine the default account for contacts compatibility", e);
        }
        return null;
    }

    /**
     * Returns true if the specified account type is writable.
     */
    protected boolean isWritableAccount(String accountType) {
        if (accountType == null) {
            return true;
        }

        Boolean writable = mAccountWritability.get(accountType);
        if (writable != null) {
            return writable;
        }

        IContentService contentService = ContentResolver.getContentService();
        try {
            for (SyncAdapterType sync : contentService.getSyncAdapterTypes()) {
                if (ContactsContract.AUTHORITY.equals(sync.authority) &&
                        accountType.equals(sync.accountType)) {
                    writable = sync.supportsUploading();
                    break;
                }
            }
        } catch (RemoteException e) {
            Log.e(TAG, "Could not acquire sync adapter types");
        }

        if (writable == null) {
            writable = false;
        }

        mAccountWritability.put(accountType, writable);
        return writable;
    }


    /* package */ static boolean readBooleanQueryParameter(Uri uri, String parameter,
            boolean defaultValue) {

        // Manually parse the query, which is much faster than calling uri.getQueryParameter
        String query = uri.getEncodedQuery();
        if (query == null) {
            return defaultValue;
        }

        int index = query.indexOf(parameter);
        if (index == -1) {
            return defaultValue;
        }

        index += parameter.length();

        return !matchQueryParameter(query, index, "=0", false)
                && !matchQueryParameter(query, index, "=false", true);
    }

    private static boolean matchQueryParameter(String query, int index, String value,
            boolean ignoreCase) {
        int length = value.length();
        return query.regionMatches(ignoreCase, index, value, 0, length)
                && (query.length() == index + length || query.charAt(index + length) == '&');
    }

    /**
     * A fast re-implementation of {@link Uri#getQueryParameter}
     */
    /* package */ static String getQueryParameter(Uri uri, String parameter) {
        String query = uri.getEncodedQuery();
        if (query == null) {
            return null;
        }

        int queryLength = query.length();
        int parameterLength = parameter.length();

        String value;
        int index = 0;
        while (true) {
            index = query.indexOf(parameter, index);
            if (index == -1) {
                return null;
            }

            // Should match against the whole parameter instead of its suffix.
            // e.g. The parameter "param" must not be found in "some_param=val".
            if (index > 0) {
                char prevChar = query.charAt(index - 1);
                if (prevChar != '?' && prevChar != '&') {
                    // With "some_param=val1&param=val2", we should find second "param" occurrence.
                    index += parameterLength;
                    continue;
                }
            }

            index += parameterLength;

            if (queryLength == index) {
                return null;
            }

            if (query.charAt(index) == '=') {
                index++;
                break;
            }
        }

        int ampIndex = query.indexOf('&', index);
        if (ampIndex == -1) {
            value = query.substring(index);
        } else {
            value = query.substring(index, ampIndex);
        }

        return Uri.decode(value);
    }

    protected boolean isAggregationUpgradeNeeded() {
        if (!mContactAggregator.isEnabled()) {
            return false;
        }

        int version = Integer.parseInt(mDbHelper.getProperty(PROPERTY_AGGREGATION_ALGORITHM, "1"));
        return version < PROPERTY_AGGREGATION_ALGORITHM_VERSION;
    }

    protected void upgradeAggregationAlgorithmInBackground() {
        // This upgrade will affect very few contacts, so it can be performed on the
        // main thread during the initial boot after an OTA

        Log.i(TAG, "Upgrading aggregation algorithm");
        int count = 0;
        long start = SystemClock.currentThreadTimeMillis();
        try {
            mDb = mDbHelper.getWritableDatabase();
            mDb.beginTransaction();
            Cursor cursor = mDb.query(true,
                    Tables.RAW_CONTACTS + " r1 JOIN " + Tables.RAW_CONTACTS + " r2",
                    new String[]{"r1." + RawContacts._ID},
                    "r1." + RawContacts._ID + "!=r2." + RawContacts._ID +
                    " AND r1." + RawContacts.CONTACT_ID + "=r2." + RawContacts.CONTACT_ID +
                    " AND r1." + RawContacts.ACCOUNT_NAME + "=r2." + RawContacts.ACCOUNT_NAME +
                    " AND r1." + RawContacts.ACCOUNT_TYPE + "=r2." + RawContacts.ACCOUNT_TYPE,
                    null, null, null, null, null);
            try {
                while (cursor.moveToNext()) {
                    long rawContactId = cursor.getLong(0);
                    mContactAggregator.markForAggregation(rawContactId,
                            RawContacts.AGGREGATION_MODE_DEFAULT, true);
                    count++;
                }
            } finally {
                cursor.close();
            }
            mContactAggregator.aggregateInTransaction(mTransactionContext, mDb);
            updateSearchIndexInTransaction();
            mDb.setTransactionSuccessful();
            mDbHelper.setProperty(PROPERTY_AGGREGATION_ALGORITHM,
                    String.valueOf(PROPERTY_AGGREGATION_ALGORITHM_VERSION));
        } finally {
            mDb.endTransaction();
            long end = SystemClock.currentThreadTimeMillis();
            Log.i(TAG, "Aggregation algorithm upgraded for " + count
                    + " contacts, in " + (end - start) + "ms");
        }
    }

    /* Visible for testing */
    boolean isPhone() {
        if (!sIsPhoneInitialized) {
            sIsPhone = new TelephonyManager(getContext()).isVoiceCapable();
            sIsPhoneInitialized = true;
        }
        return sIsPhone;
    }

    private boolean handleDataUsageFeedback(Uri uri) {
        final long currentTimeMillis = System.currentTimeMillis();
        final String usageType = uri.getQueryParameter(DataUsageFeedback.USAGE_TYPE);
        final String[] ids = uri.getLastPathSegment().trim().split(",");
        final ArrayList<Long> dataIds = new ArrayList<Long>();

        for (String id : ids) {
            dataIds.add(Long.valueOf(id));
        }
        final boolean successful;
        if (TextUtils.isEmpty(usageType)) {
            Log.w(TAG, "Method for data usage feedback isn't specified. Ignoring.");
            successful = false;
        } else {
            successful = updateDataUsageStat(dataIds, usageType, currentTimeMillis) > 0;
        }

        // Handle old API. This doesn't affect the result of this entire method.
        final String[] questionMarks = new String[ids.length];
        Arrays.fill(questionMarks, "?");
        final String where = Data._ID + " IN (" + TextUtils.join(",", questionMarks) + ")";
        final Cursor cursor = mDb.query(
                mDbHelper.getDataView(shouldExcludeRestrictedData(uri)),
                new String[] { Data.CONTACT_ID },
                where, ids, null, null, null);
        try {
            while (cursor.moveToNext()) {
                mSelectionArgs1[0] = cursor.getString(0);
                ContentValues values2 = new ContentValues();
                values2.put(Contacts.LAST_TIME_CONTACTED, currentTimeMillis);
                mDb.update(Tables.CONTACTS, values2, Contacts._ID + "=?", mSelectionArgs1);
                mDb.execSQL(UPDATE_TIMES_CONTACTED_CONTACTS_TABLE, mSelectionArgs1);
                mDb.execSQL(UPDATE_TIMES_CONTACTED_RAWCONTACTS_TABLE, mSelectionArgs1);
            }
        } finally {
            cursor.close();
        }

        return successful;
    }

    /**
     * Update {@link Tables#DATA_USAGE_STAT}.
     *
     * @return the number of rows affected.
     */
    private int updateDataUsageStat(
            ArrayList<Long> dataIds, String type, long currentTimeMillis) {
        final int typeInt = sDataUsageTypeMap.get(type);
        final String where = DataUsageStatColumns.DATA_ID + " =? AND "
                + DataUsageStatColumns.USAGE_TYPE_INT + " =?";
        final String[] columns =
                new String[] { DataUsageStatColumns._ID, DataUsageStatColumns.TIMES_USED };
        final ContentValues values = new ContentValues();
        for (Long dataId : dataIds) {
            final String[] args = new String[] { dataId.toString(), String.valueOf(typeInt) };
            mDb.beginTransaction();
            try {
                final Cursor cursor = mDb.query(Tables.DATA_USAGE_STAT, columns, where, args,
                        null, null, null);
                try {
                    if (cursor.getCount() > 0) {
                        if (!cursor.moveToFirst()) {
                            Log.e(TAG,
                                    "moveToFirst() failed while getAccount() returned non-zero.");
                        } else {
                            values.clear();
                            values.put(DataUsageStatColumns.TIMES_USED, cursor.getInt(1) + 1);
                            values.put(DataUsageStatColumns.LAST_TIME_USED, currentTimeMillis);
                            mDb.update(Tables.DATA_USAGE_STAT, values,
                                    DataUsageStatColumns._ID + " =?",
                                    new String[] { cursor.getString(0) });
                        }
                    } else {
                        values.clear();
                        values.put(DataUsageStatColumns.DATA_ID, dataId);
                        values.put(DataUsageStatColumns.USAGE_TYPE_INT, typeInt);
                        values.put(DataUsageStatColumns.TIMES_USED, 1);
                        values.put(DataUsageStatColumns.LAST_TIME_USED, currentTimeMillis);
                        mDb.insert(Tables.DATA_USAGE_STAT, null, values);
                    }
                    mDb.setTransactionSuccessful();
                } finally {
                    cursor.close();
                }
            } finally {
                mDb.endTransaction();
            }
        }

        return dataIds.size();
    }

    /**
     * Returns a sort order String for promoting data rows (email addresses, phone numbers, etc.)
     * associated with a primary account. The primary account should be supplied from applications
     * with {@link ContactsContract#PRIMARY_ACCOUNT_NAME} and
     * {@link ContactsContract#PRIMARY_ACCOUNT_TYPE}. Null will be returned when the primary
     * account isn't available.
     */
    private String getAccountPromotionSortOrder(Uri uri) {
        final String primaryAccountName =
                uri.getQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME);
        final String primaryAccountType =
                uri.getQueryParameter(ContactsContract.PRIMARY_ACCOUNT_TYPE);

        // Data rows associated with primary account should be promoted.
        if (!TextUtils.isEmpty(primaryAccountName)) {
            StringBuilder sb = new StringBuilder();
            sb.append("(CASE WHEN " + RawContacts.ACCOUNT_NAME + "=");
            DatabaseUtils.appendEscapedSQLString(sb, primaryAccountName);
            if (!TextUtils.isEmpty(primaryAccountType)) {
                sb.append(" AND " + RawContacts.ACCOUNT_TYPE + "=");
                DatabaseUtils.appendEscapedSQLString(sb, primaryAccountType);
            }
            sb.append(" THEN 0 ELSE 1 END)");
            return sb.toString();
        } else {
            return null;
        }
    }
}
