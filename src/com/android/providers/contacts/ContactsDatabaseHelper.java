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

import com.android.internal.R.bool;
import com.android.providers.contacts.sqlite.DatabaseAnalyzer;
import com.android.providers.contacts.sqlite.SqlChecker;
import com.android.providers.contacts.sqlite.SqlChecker.InvalidSqlException;
import com.android.providers.contacts.util.PropertyUtils;

import android.app.ActivityManager;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.pm.UserInfo;
import android.database.CharArrayBuffer;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.icu.util.VersionInfo;
import android.net.Uri;
import android.os.Binder;
import android.os.Bundle;
import android.os.SystemClock;
import android.os.UserManager;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Event;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Identity;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Relation;
import android.provider.ContactsContract.CommonDataKinds.SipAddress;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.CommonDataKinds.Website;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Contacts.Photo;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.DisplayNameSources;
import android.provider.ContactsContract.DisplayPhoto;
import android.provider.ContactsContract.FullNameStyle;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.MetadataSync;
import android.provider.ContactsContract.MetadataSyncState;
import android.provider.ContactsContract.PhoneticNameStyle;
import android.provider.ContactsContract.PhotoFiles;
import android.provider.ContactsContract.PinnedPositions;
import android.provider.ContactsContract.ProviderStatus;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.StreamItemPhotos;
import android.provider.ContactsContract.StreamItems;
import android.provider.DeviceConfig;
import android.telephony.PhoneNumberUtils;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.text.util.Rfc822Tokenizer;
import android.util.ArrayMap;
import android.util.ArraySet;
import android.util.Base64;
import android.util.Log;
import android.util.Slog;

import com.android.common.content.SyncStateContentProviderHelper;
import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.aggregation.util.CommonNicknameCache;
import com.android.providers.contacts.database.ContactsTableUtil;
import com.android.providers.contacts.database.DeletedContactsTableUtil;
import com.android.providers.contacts.database.MoreDatabaseUtils;
import com.android.providers.contacts.util.NeededForTesting;

import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Database helper for contacts. Designed as a singleton to make sure that all
 * {@link android.content.ContentProvider} users get the same reference.
 * Provides handy methods for maintaining package and mime-type lookup tables.
 */
public class ContactsDatabaseHelper extends SQLiteOpenHelper {

    /**
     * Contacts DB version ranges:
     * <pre>
     *   0-98    Cupcake/Donut
     *   100-199 Eclair
     *   200-299 Eclair-MR1
     *   300-349 Froyo
     *   350-399 Gingerbread
     *   400-499 Honeycomb
     *   500-549 Honeycomb-MR1
     *   550-599 Honeycomb-MR2
     *   600-699 Ice Cream Sandwich
     *   700-799 Jelly Bean
     *   800-899 Kitkat
     *   900-999 Lollipop
     *   1000-1099 M
     *   1100-1199 N
     *   1200-1299 O
     *   1300-1399 P
     *   1400-1499 Q
     * </pre>
     */
    static final int DATABASE_VERSION = 1400;
    private static final int MINIMUM_SUPPORTED_VERSION = 700;

    @VisibleForTesting
    static final boolean DISALLOW_SUB_QUERIES = false;

    private static final int IDLE_CONNECTION_TIMEOUT_MS = 30000;

    private static final String USE_STRICT_PHONE_NUMBER_COMPARISON_KEY
            = "use_strict_phone_number_comparison";

    private static final String USE_STRICT_PHONE_NUMBER_COMPARISON_FOR_RUSSIA_KEY
            = "use_strict_phone_number_comparison_for_russia";

    private static final String USE_STRICT_PHONE_NUMBER_COMPARISON_FOR_KAZAKHSTAN_KEY
            = "use_strict_phone_number_comparison_for_kazakhstan";

    private static final String RUSSIA_COUNTRY_CODE = "RU";
    private static final String KAZAKHSTAN_COUNTRY_CODE = "KZ";

    public interface Tables {
        public static final String CONTACTS = "contacts";
        public static final String DELETED_CONTACTS = "deleted_contacts";
        public static final String RAW_CONTACTS = "raw_contacts";
        public static final String STREAM_ITEMS = "stream_items";
        public static final String STREAM_ITEM_PHOTOS = "stream_item_photos";
        public static final String PHOTO_FILES = "photo_files";
        public static final String PACKAGES = "packages";
        public static final String MIMETYPES = "mimetypes";
        public static final String PHONE_LOOKUP = "phone_lookup";
        public static final String NAME_LOOKUP = "name_lookup";
        public static final String AGGREGATION_EXCEPTIONS = "agg_exceptions";
        public static final String SETTINGS = "settings";
        public static final String DATA = "data";
        public static final String GROUPS = "groups";
        public static final String PRESENCE = "presence";
        public static final String AGGREGATED_PRESENCE = "agg_presence";
        public static final String NICKNAME_LOOKUP = "nickname_lookup";
        public static final String STATUS_UPDATES = "status_updates";
        public static final String ACCOUNTS = "accounts";
        public static final String VISIBLE_CONTACTS = "visible_contacts";
        public static final String DIRECTORIES = "directories";
        public static final String DEFAULT_DIRECTORY = "default_directory";
        public static final String SEARCH_INDEX = "search_index";
        public static final String METADATA_SYNC = "metadata_sync";
        public static final String METADATA_SYNC_STATE = "metadata_sync_state";
        public static final String PRE_AUTHORIZED_URIS = "pre_authorized_uris";

        // This list of tables contains auto-incremented sequences.
        public static final String[] SEQUENCE_TABLES = new String[] {
                CONTACTS,
                RAW_CONTACTS,
                STREAM_ITEMS,
                STREAM_ITEM_PHOTOS,
                PHOTO_FILES,
                DATA,
                GROUPS,
                DIRECTORIES};

        /**
         * For {@link android.provider.ContactsContract.DataUsageFeedback}. The table structure
         * itself is not exposed outside.
         */
        public static final String DATA_USAGE_STAT = "data_usage_stat";

        public static final String DATA_JOIN_MIMETYPES = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id)";

        public static final String DATA_JOIN_RAW_CONTACTS = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        // NOTE: If you want to refer to account name/type/data_set, AccountsColumns.CONCRETE_XXX
        // MUST be used, as upgraded raw_contacts may have the account info columns too.
        public static final String DATA_JOIN_MIMETYPE_RAW_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)"
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                    + RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")";

        // NOTE: This requires late binding of GroupMembership MIME-type
        // TODO Consolidate settings and accounts
        public static final String RAW_CONTACTS_JOIN_SETTINGS_DATA_GROUPS = Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + "LEFT OUTER JOIN " + Tables.SETTINGS + " ON ("
                    + AccountsColumns.CONCRETE_ACCOUNT_NAME + "="
                        + SettingsColumns.CONCRETE_ACCOUNT_NAME + " AND "
                    + AccountsColumns.CONCRETE_ACCOUNT_TYPE + "="
                        + SettingsColumns.CONCRETE_ACCOUNT_TYPE + " AND "
                    + "((" + AccountsColumns.CONCRETE_DATA_SET + " IS NULL AND "
                            + SettingsColumns.CONCRETE_DATA_SET + " IS NULL) OR ("
                        + AccountsColumns.CONCRETE_DATA_SET + "="
                            + SettingsColumns.CONCRETE_DATA_SET + "))) "
                + "LEFT OUTER JOIN data ON (data.mimetype_id=? AND "
                    + "data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN groups ON (groups._id = data." + GroupMembership.GROUP_ROW_ID
                + ")";

        // NOTE: This requires late binding of GroupMembership MIME-type
        // TODO Add missing DATA_SET join -- or just consolidate settings and accounts
        public static final String SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS = "settings "
                + "LEFT OUTER JOIN raw_contacts ON ("
                    + RawContactsColumns.CONCRETE_ACCOUNT_ID + "=(SELECT "
                        + AccountsColumns.CONCRETE_ID
                        + " FROM " + Tables.ACCOUNTS
                        + " WHERE "
                            + "(" + AccountsColumns.CONCRETE_ACCOUNT_NAME
                                + "=" + SettingsColumns.CONCRETE_ACCOUNT_NAME + ") AND "
                            + "(" + AccountsColumns.CONCRETE_ACCOUNT_TYPE
                                + "=" + SettingsColumns.CONCRETE_ACCOUNT_TYPE + ")))"
                + "LEFT OUTER JOIN data ON (data.mimetype_id=? AND "
                    + "data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String CONTACTS_JOIN_RAW_CONTACTS_DATA_FILTERED_BY_GROUPMEMBERSHIP =
                Tables.CONTACTS
                    + " INNER JOIN " + Tables.RAW_CONTACTS
                        + " ON (" + RawContactsColumns.CONCRETE_CONTACT_ID + "="
                            + ContactsColumns.CONCRETE_ID
                        + ")"
                    + " INNER JOIN " + Tables.DATA
                        + " ON (" + DataColumns.CONCRETE_DATA1 + "=" + GroupsColumns.CONCRETE_ID
                        + " AND "
                        + DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                        + " AND "
                        + DataColumns.CONCRETE_MIMETYPE_ID + "="
                            + "(SELECT " + MimetypesColumns._ID
                            + " FROM " + Tables.MIMETYPES
                            + " WHERE "
                            + MimetypesColumns.CONCRETE_MIMETYPE + "="
                                + "'" + GroupMembership.CONTENT_ITEM_TYPE + "'"
                            + ")"
                        + ")";

        // NOTE: If you want to refer to account name/type/data_set, AccountsColumns.CONCRETE_XXX
        // MUST be used, as upgraded raw_contacts may have the account info columns too.
        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_GROUPS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                    + RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN groups "
                + "  ON (mimetypes.mimetype='" + GroupMembership.CONTENT_ITEM_TYPE + "' "
                + "      AND groups._id = data." + GroupMembership.GROUP_ROW_ID + ") ";

        public static final String ACTIVITIES_JOIN_MIMETYPES = "activities "
                + "LEFT OUTER JOIN mimetypes ON (activities.mimetype_id = mimetypes._id)";

        public static final String ACTIVITIES_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS =
                "activities "
                + "LEFT OUTER JOIN packages ON (activities.package_id = packages._id) "
                + "LEFT OUTER JOIN mimetypes ON (activities.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (activities.author_contact_id = " +
                        "raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String NAME_LOOKUP_JOIN_RAW_CONTACTS = "name_lookup "
                + "INNER JOIN view_raw_contacts ON (name_lookup.raw_contact_id = "
                + "view_raw_contacts._id)";

        public static final String RAW_CONTACTS_JOIN_ACCOUNTS = Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                + AccountsColumns.CONCRETE_ID + "=" + RawContactsColumns.CONCRETE_ACCOUNT_ID
                + ")";

        public static final String RAW_CONTACTS_JOIN_METADATA_SYNC = Tables.RAW_CONTACTS
                + " JOIN " + Tables.METADATA_SYNC + " ON ("
                + RawContactsColumns.CONCRETE_BACKUP_ID + "="
                + MetadataSyncColumns.CONCRETE_BACKUP_ID
                + " AND "
                + RawContactsColumns.CONCRETE_ACCOUNT_ID + "="
                + MetadataSyncColumns.CONCRETE_ACCOUNT_ID
                + ")";
    }

    public interface Joins {
        /**
         * Join string intended to be used with the GROUPS table/view.  The main table must be named
         * as "groups".
         *
         * Adds the "group_member_count column" to the query, which will be null if a group has
         * no members.  Use ifnull(group_member_count, 0) if 0 is needed instead.
         */
        public static final String GROUP_MEMBER_COUNT =
                " LEFT OUTER JOIN (SELECT "
                        + "data.data1 AS member_count_group_id, "
                        + "COUNT(data.raw_contact_id) AS group_member_count "
                    + "FROM data "
                    + "WHERE "
                        + "data.mimetype_id = (SELECT _id FROM mimetypes WHERE "
                            + "mimetypes.mimetype = '" + GroupMembership.CONTENT_ITEM_TYPE + "')"
                    + "GROUP BY member_count_group_id) AS member_count_table" // End of inner query
                + " ON (groups._id = member_count_table.member_count_group_id)";
    }

    public interface Views {
        public static final String DATA = "view_data";
        public static final String RAW_CONTACTS = "view_raw_contacts";
        public static final String CONTACTS = "view_contacts";
        public static final String ENTITIES = "view_entities";
        public static final String RAW_ENTITIES = "view_raw_entities";
        public static final String GROUPS = "view_groups";

        /** The data_usage_stat table with the low-res columns. */
        public static final String DATA_USAGE_LR = "view_data_usage";
        public static final String STREAM_ITEMS = "view_stream_items";
        public static final String METADATA_SYNC = "view_metadata_sync";
        public static final String METADATA_SYNC_STATE = "view_metadata_sync_state";
    }

    public interface Projections {
        String[] ID = new String[] {BaseColumns._ID};
        String[] LITERAL_ONE = new String[] {"1"};
    }

    /**
     * Property names for {@link ContactsDatabaseHelper#getProperty} and
     * {@link ContactsDatabaseHelper#setProperty}.
     */
    public interface DbProperties {
        String DIRECTORY_SCAN_COMPLETE = "directoryScanComplete";
        String AGGREGATION_ALGORITHM = "aggregation_v2";
        String KNOWN_ACCOUNTS = "known_accounts";
        String ICU_VERSION = "icu_version";
        String LOCALE = "locale";
        String DATABASE_TIME_CREATED = "database_time_created";
        String KNOWN_DIRECTORY_PACKAGES = "knownDirectoryPackages";
    }

    public interface Clauses {
        final String HAVING_NO_GROUPS = "COUNT(" + DataColumns.CONCRETE_GROUP_ID + ") == 0";

        final String GROUP_BY_ACCOUNT_CONTACT_ID = SettingsColumns.CONCRETE_ACCOUNT_NAME + ","
                + SettingsColumns.CONCRETE_ACCOUNT_TYPE + "," + RawContacts.CONTACT_ID;

        String LOCAL_ACCOUNT_ID =
                "(SELECT " + AccountsColumns._ID +
                " FROM " + Tables.ACCOUNTS +
                " WHERE " +
                    AccountsColumns.ACCOUNT_NAME + " IS NULL AND " +
                    AccountsColumns.ACCOUNT_TYPE + " IS NULL AND " +
                    AccountsColumns.DATA_SET + " IS NULL)";

        final String RAW_CONTACT_IS_LOCAL = RawContactsColumns.CONCRETE_ACCOUNT_ID
                + "=" + LOCAL_ACCOUNT_ID;

        final String ZERO_GROUP_MEMBERSHIPS = "COUNT(" + GroupsColumns.CONCRETE_ID + ")=0";

        final String OUTER_RAW_CONTACTS = "outer_raw_contacts";
        final String OUTER_RAW_CONTACTS_ID = OUTER_RAW_CONTACTS + "." + RawContacts._ID;

        final String CONTACT_IS_VISIBLE =
                "SELECT " +
                    "MAX((SELECT (CASE WHEN " +
                        "(CASE" +
                            " WHEN " + RAW_CONTACT_IS_LOCAL +
                            " THEN 1 " +
                            " WHEN " + ZERO_GROUP_MEMBERSHIPS +
                            " THEN " + Settings.UNGROUPED_VISIBLE +
                            " ELSE MAX(" + Groups.GROUP_VISIBLE + ")" +
                         "END)=1 THEN 1 ELSE 0 END)" +
                " FROM " + Tables.RAW_CONTACTS_JOIN_SETTINGS_DATA_GROUPS +
                " WHERE " + RawContactsColumns.CONCRETE_ID + "=" + OUTER_RAW_CONTACTS_ID + "))" +
                " FROM " + Tables.RAW_CONTACTS + " AS " + OUTER_RAW_CONTACTS +
                " WHERE " + RawContacts.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID +
                " GROUP BY " + RawContacts.CONTACT_ID;

        final String GROUP_HAS_ACCOUNT_AND_SOURCE_ID = Groups.SOURCE_ID + "=? AND "
                + GroupsColumns.ACCOUNT_ID + "=?";

        public static final String CONTACT_VISIBLE =
            "EXISTS (SELECT _id FROM " + Tables.VISIBLE_CONTACTS
                + " WHERE " + Tables.CONTACTS +"." + Contacts._ID
                        + "=" + Tables.VISIBLE_CONTACTS +"." + Contacts._ID + ")";

        public static final String CONTACT_IN_DEFAULT_DIRECTORY =
                "EXISTS (SELECT _id FROM " + Tables.DEFAULT_DIRECTORY
                        + " WHERE " + Tables.CONTACTS +"." + Contacts._ID
                        + "=" + Tables.DEFAULT_DIRECTORY +"." + Contacts._ID + ")";
    }

    public interface ContactsColumns {
        public static final String LAST_STATUS_UPDATE_ID = "status_update_id";

        public static final String CONCRETE_ID = Tables.CONTACTS + "." + BaseColumns._ID;

        public static final String CONCRETE_PHOTO_FILE_ID = Tables.CONTACTS + "."
                + Contacts.PHOTO_FILE_ID;

        public static final String CONCRETE_STARRED = Tables.CONTACTS + "." + Contacts.STARRED;
        public static final String CONCRETE_PINNED = Tables.CONTACTS + "." + Contacts.PINNED;
        public static final String CONCRETE_CUSTOM_RINGTONE = Tables.CONTACTS + "."
                + Contacts.CUSTOM_RINGTONE;
        public static final String CONCRETE_SEND_TO_VOICEMAIL = Tables.CONTACTS + "."
                + Contacts.SEND_TO_VOICEMAIL;
        public static final String CONCRETE_LOOKUP_KEY = Tables.CONTACTS + "."
                + Contacts.LOOKUP_KEY;
        public static final String CONCRETE_CONTACT_LAST_UPDATED_TIMESTAMP = Tables.CONTACTS + "."
                + Contacts.CONTACT_LAST_UPDATED_TIMESTAMP;
        public static final String PHONEBOOK_LABEL_PRIMARY = "phonebook_label";
        public static final String PHONEBOOK_BUCKET_PRIMARY = "phonebook_bucket";
        public static final String PHONEBOOK_LABEL_ALTERNATIVE = "phonebook_label_alt";
        public static final String PHONEBOOK_BUCKET_ALTERNATIVE = "phonebook_bucket_alt";
    }

    public interface RawContactsColumns {
        public static final String CONCRETE_ID =
                Tables.RAW_CONTACTS + "." + BaseColumns._ID;

        public static final String ACCOUNT_ID = "account_id";
        public static final String CONCRETE_ACCOUNT_ID = Tables.RAW_CONTACTS + "." + ACCOUNT_ID;
        public static final String CONCRETE_SOURCE_ID =
                Tables.RAW_CONTACTS + "." + RawContacts.SOURCE_ID;
        public static final String CONCRETE_BACKUP_ID =
                Tables.RAW_CONTACTS + "." + RawContacts.BACKUP_ID;
        public static final String CONCRETE_VERSION =
                Tables.RAW_CONTACTS + "." + RawContacts.VERSION;
        public static final String CONCRETE_DIRTY =
                Tables.RAW_CONTACTS + "." + RawContacts.DIRTY;
        public static final String CONCRETE_DELETED =
                Tables.RAW_CONTACTS + "." + RawContacts.DELETED;
        public static final String CONCRETE_SYNC1 =
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC1;
        public static final String CONCRETE_SYNC2 =
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC2;
        public static final String CONCRETE_SYNC3 =
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC3;
        public static final String CONCRETE_SYNC4 =
                Tables.RAW_CONTACTS + "." + RawContacts.SYNC4;
        public static final String CONCRETE_CUSTOM_RINGTONE =
                Tables.RAW_CONTACTS + "." + RawContacts.CUSTOM_RINGTONE;
        public static final String CONCRETE_SEND_TO_VOICEMAIL =
                Tables.RAW_CONTACTS + "." + RawContacts.SEND_TO_VOICEMAIL;
        public static final String CONCRETE_STARRED =
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED;
        public static final String CONCRETE_PINNED =
                Tables.RAW_CONTACTS + "." + RawContacts.PINNED;

        public static final String CONCRETE_METADATA_DIRTY =
                Tables.RAW_CONTACTS + "." + RawContacts.METADATA_DIRTY;
        public static final String DISPLAY_NAME = RawContacts.DISPLAY_NAME_PRIMARY;
        public static final String DISPLAY_NAME_SOURCE = RawContacts.DISPLAY_NAME_SOURCE;
        public static final String AGGREGATION_NEEDED = "aggregation_needed";

        public static final String CONCRETE_DISPLAY_NAME =
                Tables.RAW_CONTACTS + "." + DISPLAY_NAME;
        public static final String CONCRETE_CONTACT_ID =
                Tables.RAW_CONTACTS + "." + RawContacts.CONTACT_ID;
        public static final String PHONEBOOK_LABEL_PRIMARY =
            ContactsColumns.PHONEBOOK_LABEL_PRIMARY;
        public static final String PHONEBOOK_BUCKET_PRIMARY =
            ContactsColumns.PHONEBOOK_BUCKET_PRIMARY;
        public static final String PHONEBOOK_LABEL_ALTERNATIVE =
            ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE;
        public static final String PHONEBOOK_BUCKET_ALTERNATIVE =
            ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE;

        /**
         * This column is no longer used, but we keep it in the table so an upgraded database
         * will look the same as a new database. This reduces the chance of OEMs adding a second
         * column with the same name.
         */
        public static final String NAME_VERIFIED_OBSOLETE = "name_verified";
    }

    public interface ViewRawContactsColumns {
        String CONCRETE_ACCOUNT_NAME = Views.RAW_CONTACTS + "." + RawContacts.ACCOUNT_NAME;
        String CONCRETE_ACCOUNT_TYPE = Views.RAW_CONTACTS + "." + RawContacts.ACCOUNT_TYPE;
        String CONCRETE_DATA_SET = Views.RAW_CONTACTS + "." + RawContacts.DATA_SET;
    }

    public interface DataColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String MIMETYPE_ID = "mimetype_id";

        public static final String CONCRETE_ID = Tables.DATA + "." + BaseColumns._ID;
        public static final String CONCRETE_MIMETYPE_ID = Tables.DATA + "." + MIMETYPE_ID;
        public static final String CONCRETE_RAW_CONTACT_ID = Tables.DATA + "."
                + Data.RAW_CONTACT_ID;
        public static final String CONCRETE_GROUP_ID = Tables.DATA + "."
                + GroupMembership.GROUP_ROW_ID;

        public static final String CONCRETE_DATA1 = Tables.DATA + "." + Data.DATA1;
        public static final String CONCRETE_DATA2 = Tables.DATA + "." + Data.DATA2;
        public static final String CONCRETE_DATA3 = Tables.DATA + "." + Data.DATA3;
        public static final String CONCRETE_DATA4 = Tables.DATA + "." + Data.DATA4;
        public static final String CONCRETE_DATA5 = Tables.DATA + "." + Data.DATA5;
        public static final String CONCRETE_DATA6 = Tables.DATA + "." + Data.DATA6;
        public static final String CONCRETE_DATA7 = Tables.DATA + "." + Data.DATA7;
        public static final String CONCRETE_DATA8 = Tables.DATA + "." + Data.DATA8;
        public static final String CONCRETE_DATA9 = Tables.DATA + "." + Data.DATA9;
        public static final String CONCRETE_DATA10 = Tables.DATA + "." + Data.DATA10;
        public static final String CONCRETE_DATA11 = Tables.DATA + "." + Data.DATA11;
        public static final String CONCRETE_DATA12 = Tables.DATA + "." + Data.DATA12;
        public static final String CONCRETE_DATA13 = Tables.DATA + "." + Data.DATA13;
        public static final String CONCRETE_DATA14 = Tables.DATA + "." + Data.DATA14;
        public static final String CONCRETE_DATA15 = Tables.DATA + "." + Data.DATA15;
        public static final String CONCRETE_IS_PRIMARY = Tables.DATA + "." + Data.IS_PRIMARY;
        public static final String CONCRETE_PACKAGE_ID = Tables.DATA + "." + PACKAGE_ID;
    }

    // Used only for legacy API support.
    public interface ExtensionsColumns {
        public static final String NAME = Data.DATA1;
        public static final String VALUE = Data.DATA2;
    }

    public interface GroupMembershipColumns {
        public static final String RAW_CONTACT_ID = Data.RAW_CONTACT_ID;
        public static final String GROUP_ROW_ID = GroupMembership.GROUP_ROW_ID;
    }

    public interface GroupsColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String CONCRETE_PACKAGE_ID = Tables.GROUPS + "." + PACKAGE_ID;

        public static final String CONCRETE_ID = Tables.GROUPS + "." + BaseColumns._ID;
        public static final String CONCRETE_SOURCE_ID = Tables.GROUPS + "." + Groups.SOURCE_ID;

        public static final String ACCOUNT_ID = "account_id";
        public static final String CONCRETE_ACCOUNT_ID = Tables.GROUPS + "." + ACCOUNT_ID;
    }

    public interface ViewGroupsColumns {
        String CONCRETE_ACCOUNT_NAME = Views.GROUPS + "." + Groups.ACCOUNT_NAME;
        String CONCRETE_ACCOUNT_TYPE = Views.GROUPS + "." + Groups.ACCOUNT_TYPE;
        String CONCRETE_DATA_SET = Views.GROUPS + "." + Groups.DATA_SET;
    }

    public interface ActivitiesColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String MIMETYPE_ID = "mimetype_id";
    }

    public interface PhoneLookupColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String DATA_ID = "data_id";
        public static final String RAW_CONTACT_ID = "raw_contact_id";
        public static final String NORMALIZED_NUMBER = "normalized_number";
        public static final String MIN_MATCH = "min_match";
    }

    public interface NameLookupColumns {
        public static final String RAW_CONTACT_ID = "raw_contact_id";
        public static final String DATA_ID = "data_id";
        public static final String NORMALIZED_NAME = "normalized_name";
        public static final String NAME_TYPE = "name_type";
    }

    public interface PackagesColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String PACKAGE = "package";

        public static final String CONCRETE_ID = Tables.PACKAGES + "." + _ID;
    }

    public interface MimetypesColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String MIMETYPE = "mimetype";

        public static final String CONCRETE_ID = Tables.MIMETYPES + "." + BaseColumns._ID;
        public static final String CONCRETE_MIMETYPE = Tables.MIMETYPES + "." + MIMETYPE;
    }

    public interface AggregationExceptionColumns {
        public static final String _ID = BaseColumns._ID;
    }

    public interface NicknameLookupColumns {
        public static final String NAME = "name";
        public static final String CLUSTER = "cluster";
    }

    public interface SettingsColumns {
        public static final String CONCRETE_ACCOUNT_NAME = Tables.SETTINGS + "."
                + Settings.ACCOUNT_NAME;
        public static final String CONCRETE_ACCOUNT_TYPE = Tables.SETTINGS + "."
                + Settings.ACCOUNT_TYPE;
        public static final String CONCRETE_DATA_SET = Tables.SETTINGS + "."
                + Settings.DATA_SET;
    }

    public interface PresenceColumns {
        String RAW_CONTACT_ID = "presence_raw_contact_id";
        String CONTACT_ID = "presence_contact_id";
    }

    public interface AggregatedPresenceColumns {
        String CONTACT_ID = "presence_contact_id";
        String CONCRETE_CONTACT_ID = Tables.AGGREGATED_PRESENCE + "." + CONTACT_ID;
    }

    public interface StatusUpdatesColumns {
        String DATA_ID = "status_update_data_id";

        String CONCRETE_DATA_ID = Tables.STATUS_UPDATES + "." + DATA_ID;

        String CONCRETE_PRESENCE = Tables.STATUS_UPDATES + "." + StatusUpdates.PRESENCE;
        String CONCRETE_STATUS = Tables.STATUS_UPDATES + "." + StatusUpdates.STATUS;
        String CONCRETE_STATUS_TIMESTAMP = Tables.STATUS_UPDATES + "."
                + StatusUpdates.STATUS_TIMESTAMP;
        String CONCRETE_STATUS_RES_PACKAGE = Tables.STATUS_UPDATES + "."
                + StatusUpdates.STATUS_RES_PACKAGE;
        String CONCRETE_STATUS_LABEL = Tables.STATUS_UPDATES + "." + StatusUpdates.STATUS_LABEL;
        String CONCRETE_STATUS_ICON = Tables.STATUS_UPDATES + "." + StatusUpdates.STATUS_ICON;
    }

    public interface ContactsStatusUpdatesColumns {
        String ALIAS = "contacts_" + Tables.STATUS_UPDATES;

        String CONCRETE_DATA_ID = ALIAS + "." + StatusUpdatesColumns.DATA_ID;

        String CONCRETE_PRESENCE = ALIAS + "." + StatusUpdates.PRESENCE;
        String CONCRETE_STATUS = ALIAS + "." + StatusUpdates.STATUS;
        String CONCRETE_STATUS_TIMESTAMP = ALIAS + "." + StatusUpdates.STATUS_TIMESTAMP;
        String CONCRETE_STATUS_RES_PACKAGE = ALIAS + "." + StatusUpdates.STATUS_RES_PACKAGE;
        String CONCRETE_STATUS_LABEL = ALIAS + "." + StatusUpdates.STATUS_LABEL;
        String CONCRETE_STATUS_ICON = ALIAS + "." + StatusUpdates.STATUS_ICON;
    }

    public interface StreamItemsColumns {
        final String CONCRETE_ID = Tables.STREAM_ITEMS + "." + BaseColumns._ID;
        final String CONCRETE_RAW_CONTACT_ID =
                Tables.STREAM_ITEMS + "." + StreamItems.RAW_CONTACT_ID;
        final String CONCRETE_PACKAGE = Tables.STREAM_ITEMS + "." + StreamItems.RES_PACKAGE;
        final String CONCRETE_ICON = Tables.STREAM_ITEMS + "." + StreamItems.RES_ICON;
        final String CONCRETE_LABEL = Tables.STREAM_ITEMS + "." + StreamItems.RES_LABEL;
        final String CONCRETE_TEXT = Tables.STREAM_ITEMS + "." + StreamItems.TEXT;
        final String CONCRETE_TIMESTAMP = Tables.STREAM_ITEMS + "." + StreamItems.TIMESTAMP;
        final String CONCRETE_COMMENTS = Tables.STREAM_ITEMS + "." + StreamItems.COMMENTS;
        final String CONCRETE_SYNC1 = Tables.STREAM_ITEMS + "." + StreamItems.SYNC1;
        final String CONCRETE_SYNC2 = Tables.STREAM_ITEMS + "." + StreamItems.SYNC2;
        final String CONCRETE_SYNC3 = Tables.STREAM_ITEMS + "." + StreamItems.SYNC3;
        final String CONCRETE_SYNC4 = Tables.STREAM_ITEMS + "." + StreamItems.SYNC4;
    }

    public interface StreamItemPhotosColumns {
        final String CONCRETE_ID = Tables.STREAM_ITEM_PHOTOS + "." + BaseColumns._ID;
        final String CONCRETE_STREAM_ITEM_ID = Tables.STREAM_ITEM_PHOTOS + "."
                + StreamItemPhotos.STREAM_ITEM_ID;
        final String CONCRETE_SORT_INDEX =
                Tables.STREAM_ITEM_PHOTOS + "." + StreamItemPhotos.SORT_INDEX;
        final String CONCRETE_PHOTO_FILE_ID = Tables.STREAM_ITEM_PHOTOS + "."
                + StreamItemPhotos.PHOTO_FILE_ID;
        final String CONCRETE_SYNC1 = Tables.STREAM_ITEM_PHOTOS + "." + StreamItemPhotos.SYNC1;
        final String CONCRETE_SYNC2 = Tables.STREAM_ITEM_PHOTOS + "." + StreamItemPhotos.SYNC2;
        final String CONCRETE_SYNC3 = Tables.STREAM_ITEM_PHOTOS + "." + StreamItemPhotos.SYNC3;
        final String CONCRETE_SYNC4 = Tables.STREAM_ITEM_PHOTOS + "." + StreamItemPhotos.SYNC4;
    }

    public interface PhotoFilesColumns {
        String CONCRETE_ID = Tables.PHOTO_FILES + "." + BaseColumns._ID;
        String CONCRETE_HEIGHT = Tables.PHOTO_FILES + "." + PhotoFiles.HEIGHT;
        String CONCRETE_WIDTH = Tables.PHOTO_FILES + "." + PhotoFiles.WIDTH;
        String CONCRETE_FILESIZE = Tables.PHOTO_FILES + "." + PhotoFiles.FILESIZE;
    }

    public interface AccountsColumns extends BaseColumns {
        String CONCRETE_ID = Tables.ACCOUNTS + "." + BaseColumns._ID;

        String ACCOUNT_NAME = RawContacts.ACCOUNT_NAME;
        String ACCOUNT_TYPE = RawContacts.ACCOUNT_TYPE;
        String DATA_SET = RawContacts.DATA_SET;

        String CONCRETE_ACCOUNT_NAME = Tables.ACCOUNTS + "." + ACCOUNT_NAME;
        String CONCRETE_ACCOUNT_TYPE = Tables.ACCOUNTS + "." + ACCOUNT_TYPE;
        String CONCRETE_DATA_SET = Tables.ACCOUNTS + "." + DATA_SET;
    }

    public interface DirectoryColumns {
        public static final String TYPE_RESOURCE_NAME = "typeResourceName";
    }

    public interface SearchIndexColumns {
        public static final String CONTACT_ID = "contact_id";
        public static final String CONTENT = "content";
        public static final String NAME = "name";
        public static final String TOKENS = "tokens";
    }

    public interface PreAuthorizedUris {
        public static final String _ID = BaseColumns._ID;
        public static final String URI = "uri";
        public static final String EXPIRATION = "expiration";
    }

    /**
     * Private table for calculating per-contact-method ranking.
     */
    public interface DataUsageStatColumns {
        /** type: INTEGER (long) */
        public static final String _ID = "stat_id";
        public static final String CONCRETE_ID = Tables.DATA_USAGE_STAT + "." + _ID;

        /** type: INTEGER (long) */
        public static final String DATA_ID = "data_id";
        public static final String CONCRETE_DATA_ID = Tables.DATA_USAGE_STAT + "." + DATA_ID;

        /** type: INTEGER (long) */
        public static final String RAW_LAST_TIME_USED = Data.RAW_LAST_TIME_USED;
        public static final String LR_LAST_TIME_USED = Data.LR_LAST_TIME_USED;

        /** type: INTEGER */
        public static final String RAW_TIMES_USED = Data.RAW_TIMES_USED;
        public static final String LR_TIMES_USED = Data.LR_TIMES_USED;

        /** type: INTEGER */
        public static final String USAGE_TYPE_INT = "usage_type";
        public static final String CONCRETE_USAGE_TYPE =
                Tables.DATA_USAGE_STAT + "." + USAGE_TYPE_INT;

        /**
         * Integer values for USAGE_TYPE.
         *
         * @see android.provider.ContactsContract.DataUsageFeedback#USAGE_TYPE
         */
        public static final int USAGE_TYPE_INT_CALL = 0;
        public static final int USAGE_TYPE_INT_LONG_TEXT = 1;
        public static final int USAGE_TYPE_INT_SHORT_TEXT = 2;
    }

    public interface MetadataSyncColumns {
        static final String CONCRETE_ID = Tables.METADATA_SYNC + "._id";
        static final String ACCOUNT_ID = "account_id";
        static final String CONCRETE_BACKUP_ID = Tables.METADATA_SYNC + "." +
                MetadataSync.RAW_CONTACT_BACKUP_ID;
        static final String CONCRETE_ACCOUNT_ID = Tables.METADATA_SYNC + "." + ACCOUNT_ID;
        static final String CONCRETE_DELETED = Tables.METADATA_SYNC + "." +
                MetadataSync.DELETED;
    }

    public interface MetadataSyncStateColumns {
        static final String CONCRETE_ID = Tables.METADATA_SYNC_STATE + "._id";
        static final String ACCOUNT_ID = "account_id";
        static final String CONCRETE_ACCOUNT_ID = Tables.METADATA_SYNC_STATE + "." + ACCOUNT_ID;
    }

    private  interface EmailQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Email._ID,
                Email.RAW_CONTACT_ID,
                Email.ADDRESS};

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int ADDRESS = 2;
    }

    private interface StructuredNameQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                StructuredName._ID,
                StructuredName.RAW_CONTACT_ID,
                StructuredName.DISPLAY_NAME,
        };

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int DISPLAY_NAME = 2;
    }

    private interface NicknameQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Nickname._ID,
                Nickname.RAW_CONTACT_ID,
                Nickname.NAME};

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int NAME = 2;
    }

    private interface RawContactNameQuery {
        public static final String RAW_SQL =
                "SELECT "
                        + DataColumns.MIMETYPE_ID + ","
                        + Data.IS_PRIMARY + ","
                        + Data.DATA1 + ","
                        + Data.DATA2 + ","
                        + Data.DATA3 + ","
                        + Data.DATA4 + ","
                        + Data.DATA5 + ","
                        + Data.DATA6 + ","
                        + Data.DATA7 + ","
                        + Data.DATA8 + ","
                        + Data.DATA9 + ","
                        + Data.DATA10 + ","
                        + Data.DATA11 +
                " FROM " + Tables.DATA +
                " WHERE " + Data.RAW_CONTACT_ID + "=?" +
                        " AND (" + Data.DATA1 + " NOT NULL OR " +
                                Data.DATA8 + " NOT NULL OR " +
                                Data.DATA9 + " NOT NULL OR " +
                                Data.DATA10 + " NOT NULL OR " +  // Phonetic name not empty
                                Organization.TITLE + " NOT NULL)";

        public static final int MIMETYPE = 0;
        public static final int IS_PRIMARY = 1;
        public static final int DATA1 = 2;
        public static final int GIVEN_NAME = 3;                         // data2
        public static final int FAMILY_NAME = 4;                        // data3
        public static final int PREFIX = 5;                             // data4
        public static final int TITLE = 5;                              // data4
        public static final int MIDDLE_NAME = 6;                        // data5
        public static final int SUFFIX = 7;                             // data6
        public static final int PHONETIC_GIVEN_NAME = 8;                // data7
        public static final int PHONETIC_MIDDLE_NAME = 9;               // data8
        public static final int ORGANIZATION_PHONETIC_NAME = 9;         // data8
        public static final int PHONETIC_FAMILY_NAME = 10;              // data9
        public static final int FULL_NAME_STYLE = 11;                   // data10
        public static final int ORGANIZATION_PHONETIC_NAME_STYLE = 11;  // data10
        public static final int PHONETIC_NAME_STYLE = 12;               // data11
    }

    public final static class NameLookupType {
        public static final int NAME_EXACT = 0;
        public static final int NAME_VARIANT = 1;
        public static final int NAME_COLLATION_KEY = 2;
        public static final int NICKNAME = 3;
        public static final int EMAIL_BASED_NICKNAME = 4;

        // The highest name-lookup type plus one.
        public static final int TYPE_COUNT = 5;

        public static boolean isBasedOnStructuredName(int nameLookupType) {
            return nameLookupType == NameLookupType.NAME_EXACT
                    || nameLookupType == NameLookupType.NAME_VARIANT
                    || nameLookupType == NameLookupType.NAME_COLLATION_KEY;
        }
    }

    private class StructuredNameLookupBuilder extends NameLookupBuilder {
        // NOTE(gilad): Is in intentional that we don't use the declaration on L960?
        private final SQLiteStatement mNameLookupInsert;
        private final CommonNicknameCache mCommonNicknameCache;

        public StructuredNameLookupBuilder(NameSplitter splitter,
                CommonNicknameCache commonNicknameCache, SQLiteStatement nameLookupInsert) {

            super(splitter);
            this.mCommonNicknameCache = commonNicknameCache;
            this.mNameLookupInsert = nameLookupInsert;
        }

        @Override
        protected void insertNameLookup(
                long rawContactId, long dataId, int lookupType, String name) {

            if (!TextUtils.isEmpty(name)) {
                ContactsDatabaseHelper.this.insertNormalizedNameLookup(
                        mNameLookupInsert, rawContactId, dataId, lookupType, name);
            }
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return mCommonNicknameCache.getCommonNicknameClusters(normalizedName);
        }
    }

    private static final String TAG = "ContactsDatabaseHelper";

    private static final String DATABASE_NAME = "contacts2.db";

    private static ContactsDatabaseHelper sSingleton = null;

    /** In-memory map of commonly found MIME-types to their ids in the MIMETYPES table */
    @VisibleForTesting
    final ArrayMap<String, Long> mCommonMimeTypeIdsCache = new ArrayMap<>();

    @VisibleForTesting
    static final String[] COMMON_MIME_TYPES = {
            Email.CONTENT_ITEM_TYPE,
            Im.CONTENT_ITEM_TYPE,
            Nickname.CONTENT_ITEM_TYPE,
            Organization.CONTENT_ITEM_TYPE,
            Phone.CONTENT_ITEM_TYPE,
            SipAddress.CONTENT_ITEM_TYPE,
            StructuredName.CONTENT_ITEM_TYPE,
            StructuredPostal.CONTENT_ITEM_TYPE,
            Identity.CONTENT_ITEM_TYPE,
            android.provider.ContactsContract.CommonDataKinds.Photo.CONTENT_ITEM_TYPE,
            GroupMembership.CONTENT_ITEM_TYPE,
            Note.CONTENT_ITEM_TYPE,
            Event.CONTENT_ITEM_TYPE,
            Website.CONTENT_ITEM_TYPE,
            Relation.CONTENT_ITEM_TYPE,
            "vnd.com.google.cursor.item/contact_misc"
    };

    private final Context mContext;
    private final boolean mDatabaseOptimizationEnabled;
    private final boolean mIsTestInstance;
    private final SyncStateContentProviderHelper mSyncState;
    private final CountryMonitor mCountryMonitor;

    /**
     * Time when the DB was created.  It's persisted in {@link DbProperties#DATABASE_TIME_CREATED},
     * but loaded into memory so it can be accessed even when the DB is busy.
     */
    private long mDatabaseCreationTime;

    private MessageDigest mMessageDigest;
    {
        try {
            mMessageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No such algorithm.", e);
        }
    }

    // We access it from multiple threads, so mark as volatile.
    private volatile boolean mUseStrictPhoneNumberComparison;

    // They're basically accessed only in one method, as well as in dump(), so technically
    // they should be volatile too, but it's not really needed in practice.
    private boolean mUseStrictPhoneNumberComparisonBase;
    private boolean mUseStrictPhoneNumberComparisonForRussia;
    private boolean mUseStrictPhoneNumberComparisonForKazakhstan;
    private int mMinMatch;

    private String[] mSelectionArgs1 = new String[1];
    private NameSplitter.Name mName = new NameSplitter.Name();
    private CharArrayBuffer mCharArrayBuffer = new CharArrayBuffer(128);
    private NameSplitter mNameSplitter;

    private final Executor mLazilyCreatedExecutor =
            new ThreadPoolExecutor(0, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static synchronized ContactsDatabaseHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new ContactsDatabaseHelper(context, DATABASE_NAME, true,
                    /* isTestInstance=*/ false);
        }
        return sSingleton;
    }

    /**
     * Returns a new instance for unit tests.
     */
    @NeededForTesting
    public static ContactsDatabaseHelper getNewInstanceForTest(Context context, String filename) {
        return new ContactsDatabaseHelper(context, filename, false, /* isTestInstance=*/ true);
    }

    protected ContactsDatabaseHelper(
            Context context, String databaseName, boolean optimizationEnabled,
            boolean isTestInstance) {
        super(context, databaseName, null, DATABASE_VERSION, MINIMUM_SUPPORTED_VERSION, null);
        boolean enableWal = android.provider.Settings.Global.getInt(context.getContentResolver(),
                android.provider.Settings.Global.CONTACTS_DATABASE_WAL_ENABLED, 1) == 1;
        if (dbForProfile() != 0 || ActivityManager.isLowRamDeviceStatic()) {
            enableWal = false;
        }
        setWriteAheadLoggingEnabled(enableWal);
        // Memory optimization - close idle connections after 30s of inactivity
        setIdleConnectionTimeout(IDLE_CONNECTION_TIMEOUT_MS);
        mDatabaseOptimizationEnabled = optimizationEnabled;
        mIsTestInstance = isTestInstance;
        mContext = context;
        mSyncState = new SyncStateContentProviderHelper();

        mCountryMonitor = new CountryMonitor(context, this::updateUseStrictPhoneNumberComparison);

        startListeningToDeviceConfigUpdates();

        updateUseStrictPhoneNumberComparison();
    }

    protected void startListeningToDeviceConfigUpdates() {
        // Note we override this method in the profile helper to skip it.

        DeviceConfig.addOnPropertiesChangedListener(DeviceConfig.NAMESPACE_CONTACTS_PROVIDER,
                mLazilyCreatedExecutor, (props) -> onDeviceConfigUpdated());
    }

    private void onDeviceConfigUpdated() {
        updateUseStrictPhoneNumberComparison();
    }

    protected void updateUseStrictPhoneNumberComparison() {
        // Note we override this method in the profile helper to skip it.

        final String country = getCurrentCountryIso();

        Log.i(TAG, "updateUseStrictPhoneNumberComparison: " + country);

        // Load all the configs so we can show them in dumpsys.
        mUseStrictPhoneNumberComparisonBase = getConfig(
                USE_STRICT_PHONE_NUMBER_COMPARISON_KEY,
                bool.config_use_strict_phone_number_comparation);

        mUseStrictPhoneNumberComparisonForRussia = getConfig(
                USE_STRICT_PHONE_NUMBER_COMPARISON_FOR_RUSSIA_KEY,
                bool.config_use_strict_phone_number_comparation_for_russia);

        mUseStrictPhoneNumberComparisonForKazakhstan = getConfig(
                USE_STRICT_PHONE_NUMBER_COMPARISON_FOR_KAZAKHSTAN_KEY,
                bool.config_use_strict_phone_number_comparation_for_kazakhstan);

        if (RUSSIA_COUNTRY_CODE.equals(country)) {
            mUseStrictPhoneNumberComparison = mUseStrictPhoneNumberComparisonForRussia;

        } else if (KAZAKHSTAN_COUNTRY_CODE.equals(country)) {
            mUseStrictPhoneNumberComparison = mUseStrictPhoneNumberComparisonForKazakhstan;

        } else {
            mUseStrictPhoneNumberComparison = mUseStrictPhoneNumberComparisonBase;
        }

        mMinMatch = mContext.getResources().getInteger(
                com.android.internal.R.integer.config_phonenumber_compare_min_match);
    }

    private boolean getConfig(String configKey, int defaultResId) {
        return DeviceConfig.getBoolean(
                DeviceConfig.NAMESPACE_CONTACTS_PROVIDER, configKey,
                mContext.getResources().getBoolean(defaultResId));
    }

    public SQLiteDatabase getDatabase(boolean writable) {
        return writable ? getWritableDatabase() : getReadableDatabase();
    }

    /**
     * Populate ids of known mimetypes into a map for easy access
     *
     * @param db target database
     */
    private void prepopulateCommonMimeTypes(SQLiteDatabase db) {
        mCommonMimeTypeIdsCache.clear();
        for(String commonMimeType: COMMON_MIME_TYPES) {
            mCommonMimeTypeIdsCache.put(commonMimeType, insertMimeType(db, commonMimeType));
        }
    }

    @Override
    public void onBeforeDelete(SQLiteDatabase db) {
        Log.w(TAG, "Database version " + db.getVersion() + " for " + DATABASE_NAME
                + " is no longer supported. Data will be lost on upgrading to " + DATABASE_VERSION);
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        Log.d(TAG, "WAL enabled for " + getDatabaseName() + ": " + db.isWriteAheadLoggingEnabled());
        prepopulateCommonMimeTypes(db);
        mSyncState.onDatabaseOpened(db);
        // Deleting any state from the presence tables to mimic their behavior from the time they
        // were in-memory tables
        db.execSQL("DELETE FROM " + Tables.PRESENCE + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATED_PRESENCE + ";");

        loadDatabaseCreationTime(db);
    }

    protected void setDatabaseCreationTime(SQLiteDatabase db) {
        // Note we don't do this in the profile DB helper.
        mDatabaseCreationTime = System.currentTimeMillis();
        PropertyUtils.setProperty(db, DbProperties.DATABASE_TIME_CREATED, String.valueOf(
                mDatabaseCreationTime));
    }

    protected void loadDatabaseCreationTime(SQLiteDatabase db) {
        // Note we don't do this in the profile DB helper.

        mDatabaseCreationTime = 0;
        final String timestamp = PropertyUtils.getProperty(db,
                DbProperties.DATABASE_TIME_CREATED, "");
        if (!TextUtils.isEmpty(timestamp)) {
            try {
                mDatabaseCreationTime = Long.parseLong(timestamp);
            } catch (NumberFormatException e) {
                Log.w(TAG, "Failed to parse timestamp: " + timestamp);
            }
        }
        if (AbstractContactsProvider.VERBOSE_LOGGING) {
            Log.v(TAG, "Open: creation time=" + mDatabaseCreationTime);
        }
        if (mDatabaseCreationTime == 0) {
            Log.w(TAG, "Unable to load creating time; resetting.");
            // Hmm, failed to load the timestamp.  Just set the current time then.
            mDatabaseCreationTime = System.currentTimeMillis();
            PropertyUtils.setProperty(db,
                    DbProperties.DATABASE_TIME_CREATED, Long.toString(mDatabaseCreationTime));
        }
    }

    private void createPresenceTables(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.PRESENCE + " ("+
                StatusUpdates.DATA_ID + " INTEGER PRIMARY KEY REFERENCES data(_id)," +
                StatusUpdates.PROTOCOL + " INTEGER NOT NULL," +
                StatusUpdates.CUSTOM_PROTOCOL + " TEXT," +
                StatusUpdates.IM_HANDLE + " TEXT," +
                StatusUpdates.IM_ACCOUNT + " TEXT," +
                PresenceColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id)," +
                PresenceColumns.RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)," +
                StatusUpdates.PRESENCE + " INTEGER," +
                StatusUpdates.CHAT_CAPABILITY + " INTEGER NOT NULL DEFAULT 0," +
                "UNIQUE(" + StatusUpdates.PROTOCOL + ", " + StatusUpdates.CUSTOM_PROTOCOL
                    + ", " + StatusUpdates.IM_HANDLE + ", " + StatusUpdates.IM_ACCOUNT + ")" +
        ");");

        db.execSQL("CREATE INDEX IF NOT EXISTS presenceIndex" + " ON "
                + Tables.PRESENCE + " (" + PresenceColumns.RAW_CONTACT_ID + ");");
        db.execSQL("CREATE INDEX IF NOT EXISTS presenceIndex2" + " ON "
                + Tables.PRESENCE + " (" + PresenceColumns.CONTACT_ID + ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS "
                + Tables.AGGREGATED_PRESENCE + " ("+
                AggregatedPresenceColumns.CONTACT_ID
                        + " INTEGER PRIMARY KEY REFERENCES contacts(_id)," +
                StatusUpdates.PRESENCE + " INTEGER," +
                StatusUpdates.CHAT_CAPABILITY + " INTEGER NOT NULL DEFAULT 0" +
        ");");

        db.execSQL("CREATE TRIGGER IF NOT EXISTS " + Tables.PRESENCE + "_deleted"
                + " BEFORE DELETE ON " + Tables.PRESENCE
                + " BEGIN "
                + "   DELETE FROM " + Tables.AGGREGATED_PRESENCE
                + "     WHERE " + AggregatedPresenceColumns.CONTACT_ID + " = " +
                        "(SELECT " + PresenceColumns.CONTACT_ID +
                        " FROM " + Tables.PRESENCE +
                        " WHERE " + PresenceColumns.RAW_CONTACT_ID
                                + "=OLD." + PresenceColumns.RAW_CONTACT_ID +
                        " AND NOT EXISTS" +
                                "(SELECT " + PresenceColumns.RAW_CONTACT_ID +
                                " FROM " + Tables.PRESENCE +
                                " WHERE " + PresenceColumns.CONTACT_ID
                                        + "=OLD." + PresenceColumns.CONTACT_ID +
                                " AND " + PresenceColumns.RAW_CONTACT_ID
                                        + "!=OLD." + PresenceColumns.RAW_CONTACT_ID + "));"
                + " END");

        final String replaceAggregatePresenceSql =
                "INSERT OR REPLACE INTO " + Tables.AGGREGATED_PRESENCE + "("
                        + AggregatedPresenceColumns.CONTACT_ID + ", "
                        + StatusUpdates.PRESENCE + ", "
                        + StatusUpdates.CHAT_CAPABILITY + ")"
                + " SELECT "
                        + PresenceColumns.CONTACT_ID + ","
                        + StatusUpdates.PRESENCE + ","
                        + StatusUpdates.CHAT_CAPABILITY
                + " FROM " + Tables.PRESENCE
                + " WHERE "
                    + " (ifnull(" + StatusUpdates.PRESENCE + ",0)  * 10 "
                            + "+ ifnull(" + StatusUpdates.CHAT_CAPABILITY + ", 0))"
                    + " = (SELECT "
                        + "MAX (ifnull(" + StatusUpdates.PRESENCE + ",0)  * 10 "
                                + "+ ifnull(" + StatusUpdates.CHAT_CAPABILITY + ", 0))"
                        + " FROM " + Tables.PRESENCE
                        + " WHERE " + PresenceColumns.CONTACT_ID
                            + "=NEW." + PresenceColumns.CONTACT_ID
                    + ")"
                + " AND " + PresenceColumns.CONTACT_ID + "=NEW." + PresenceColumns.CONTACT_ID + ";";

        db.execSQL("CREATE TRIGGER IF NOT EXISTS " + Tables.PRESENCE + "_inserted"
                + " AFTER INSERT ON " + Tables.PRESENCE
                + " BEGIN "
                + replaceAggregatePresenceSql
                + " END");

        db.execSQL("CREATE TRIGGER IF NOT EXISTS " + Tables.PRESENCE + "_updated"
                + " AFTER UPDATE ON " + Tables.PRESENCE
                + " BEGIN "
                + replaceAggregatePresenceSql
                + " END");
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        Log.i(TAG, "Bootstrapping database " + DATABASE_NAME + " version: " + DATABASE_VERSION);

        mSyncState.createDatabase(db);

        // Create the properties table first so the create time is available as soon as possible.
        // The create time is needed by BOOT_COMPLETE to send broadcasts.
        PropertyUtils.createPropertiesTable(db);

        setDatabaseCreationTime(db);

        db.execSQL("CREATE TABLE " + Tables.ACCOUNTS + " (" +
                AccountsColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                AccountsColumns.ACCOUNT_NAME + " TEXT, " +
                AccountsColumns.ACCOUNT_TYPE + " TEXT, " +
                AccountsColumns.DATA_SET + " TEXT" +
        ");");

        // Note, there are two sets of the usage stat columns: LR_* and RAW_*.
        // RAW_* contain the real values, which clients can't access.  The column names start
        // with a special prefix, which clients are prohibited from using in queries (including
        // "where" of deletes/updates.)
        // The LR_* columns have the original, public names.  The views have the LR columns too,
        // which contain the "low-res" numbers.  The tables, though, do *not* have to have these
        // columns, because we won't use them anyway.  However, because old versions of the tables
        // had those columns, and SQLite doesn't allow removing existing columns, meaning upgraded
        // tables will have these LR_* columns anyway.  So, in order to make a new database look
        // the same as an upgraded database, we create the LR columns in a new database too.
        // Otherwise, we would easily end up with writing SQLs that will run fine in a new DB
        // but not in an upgraded database, and because all unit tests will run with a new database,
        // we can't easily catch these sort of issues.

        // One row per group of contacts corresponding to the same person
        db.execSQL("CREATE TABLE " + Tables.CONTACTS + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Contacts.NAME_RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)," +
                Contacts.PHOTO_ID + " INTEGER REFERENCES data(_id)," +
                Contacts.PHOTO_FILE_ID + " INTEGER REFERENCES photo_files(_id)," +
                Contacts.CUSTOM_RINGTONE + " TEXT," +
                Contacts.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +

                Contacts.RAW_TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.RAW_LAST_TIME_CONTACTED + " INTEGER," +

                Contacts.LR_TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.LR_LAST_TIME_CONTACTED + " INTEGER," +

                Contacts.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.PINNED + " INTEGER NOT NULL DEFAULT " + PinnedPositions.UNPINNED + "," +
                Contacts.HAS_PHONE_NUMBER + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.LOOKUP_KEY + " TEXT," +
                ContactsColumns.LAST_STATUS_UPDATE_ID + " INTEGER REFERENCES data(_id)," +
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP + " INTEGER" +
        ");");

        ContactsTableUtil.createIndexes(db);

        // deleted_contacts table
        DeletedContactsTableUtil.create(db);

        // Raw_contacts table
        db.execSQL("CREATE TABLE " + Tables.RAW_CONTACTS + " (" +
                RawContacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                RawContactsColumns.ACCOUNT_ID + " INTEGER REFERENCES " +
                    Tables.ACCOUNTS + "(" + AccountsColumns._ID + ")," +
                RawContacts.SOURCE_ID + " TEXT," +
                RawContacts.BACKUP_ID + " TEXT," +
                RawContacts.RAW_CONTACT_IS_READ_ONLY + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.METADATA_DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.CONTACT_ID + " INTEGER REFERENCES contacts(_id)," +
                RawContacts.AGGREGATION_MODE + " INTEGER NOT NULL DEFAULT " +
                        RawContacts.AGGREGATION_MODE_DEFAULT + "," +
                RawContactsColumns.AGGREGATION_NEEDED + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.CUSTOM_RINGTONE + " TEXT," +
                RawContacts.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +

                RawContacts.RAW_TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.RAW_LAST_TIME_CONTACTED + " INTEGER," +

                RawContacts.LR_TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.LR_LAST_TIME_CONTACTED + " INTEGER," +

                RawContacts.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.PINNED + " INTEGER NOT NULL DEFAULT "  + PinnedPositions.UNPINNED +
                    "," + RawContacts.DISPLAY_NAME_PRIMARY + " TEXT," +
                RawContacts.DISPLAY_NAME_ALTERNATIVE + " TEXT," +
                RawContacts.DISPLAY_NAME_SOURCE + " INTEGER NOT NULL DEFAULT " +
                        DisplayNameSources.UNDEFINED + "," +
                RawContacts.PHONETIC_NAME + " TEXT," +
                // TODO: PHONETIC_NAME_STYLE should be INTEGER. There is a
                // mismatch between how the column is created here (TEXT) and
                // how it is created in upgradeToVersion205 (INTEGER).
                RawContacts.PHONETIC_NAME_STYLE + " TEXT," +
                RawContacts.SORT_KEY_PRIMARY + " TEXT COLLATE " +
                        ContactsProvider2.PHONEBOOK_COLLATOR_NAME + "," +
                RawContactsColumns.PHONEBOOK_LABEL_PRIMARY + " TEXT," +
                RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY + " INTEGER," +
                RawContacts.SORT_KEY_ALTERNATIVE + " TEXT COLLATE " +
                        ContactsProvider2.PHONEBOOK_COLLATOR_NAME + "," +
                RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE + " TEXT," +
                RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE + " INTEGER," +
                RawContactsColumns.NAME_VERIFIED_OBSOLETE + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.SYNC1 + " TEXT, " +
                RawContacts.SYNC2 + " TEXT, " +
                RawContacts.SYNC3 + " TEXT, " +
                RawContacts.SYNC4 + " TEXT " +
        ");");

        db.execSQL("CREATE INDEX raw_contacts_contact_id_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContacts.CONTACT_ID +
        ");");

        db.execSQL("CREATE INDEX raw_contacts_source_id_account_id_index ON " +
                Tables.RAW_CONTACTS + " (" +
                RawContacts.SOURCE_ID + ", " +
                RawContactsColumns.ACCOUNT_ID +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS raw_contacts_backup_id_account_id_index ON " +
                Tables.RAW_CONTACTS + " (" +
                RawContacts.BACKUP_ID + ", " +
                RawContactsColumns.ACCOUNT_ID +
        ");");

        db.execSQL("CREATE TABLE " + Tables.STREAM_ITEMS + " (" +
                StreamItems._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                StreamItems.RAW_CONTACT_ID + " INTEGER NOT NULL, " +
                StreamItems.RES_PACKAGE + " TEXT, " +
                StreamItems.RES_ICON + " TEXT, " +
                StreamItems.RES_LABEL + " TEXT, " +
                StreamItems.TEXT + " TEXT, " +
                StreamItems.TIMESTAMP + " INTEGER NOT NULL, " +
                StreamItems.COMMENTS + " TEXT, " +
                StreamItems.SYNC1 + " TEXT, " +
                StreamItems.SYNC2 + " TEXT, " +
                StreamItems.SYNC3 + " TEXT, " +
                StreamItems.SYNC4 + " TEXT, " +
                "FOREIGN KEY(" + StreamItems.RAW_CONTACT_ID + ") REFERENCES " +
                        Tables.RAW_CONTACTS + "(" + RawContacts._ID + "));");

        db.execSQL("CREATE TABLE " + Tables.STREAM_ITEM_PHOTOS + " (" +
                StreamItemPhotos._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                StreamItemPhotos.STREAM_ITEM_ID + " INTEGER NOT NULL, " +
                StreamItemPhotos.SORT_INDEX + " INTEGER, " +
                StreamItemPhotos.PHOTO_FILE_ID + " INTEGER NOT NULL, " +
                StreamItemPhotos.SYNC1 + " TEXT, " +
                StreamItemPhotos.SYNC2 + " TEXT, " +
                StreamItemPhotos.SYNC3 + " TEXT, " +
                StreamItemPhotos.SYNC4 + " TEXT, " +
                "FOREIGN KEY(" + StreamItemPhotos.STREAM_ITEM_ID + ") REFERENCES " +
                        Tables.STREAM_ITEMS + "(" + StreamItems._ID + "));");

        db.execSQL("CREATE TABLE " + Tables.PHOTO_FILES + " (" +
                PhotoFiles._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                PhotoFiles.HEIGHT + " INTEGER NOT NULL, " +
                PhotoFiles.WIDTH + " INTEGER NOT NULL, " +
                PhotoFiles.FILESIZE + " INTEGER NOT NULL);");

        // TODO readd the index and investigate a controlled use of it
//        db.execSQL("CREATE INDEX raw_contacts_agg_index ON " + Tables.RAW_CONTACTS + " (" +
//                RawContactsColumns.AGGREGATION_NEEDED +
//        ");");

        // Package name mapping table
        db.execSQL("CREATE TABLE " + Tables.PACKAGES + " (" +
                PackagesColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                PackagesColumns.PACKAGE + " TEXT NOT NULL" +
        ");");

        // Mimetype mapping table
        db.execSQL("CREATE TABLE " + Tables.MIMETYPES + " (" +
                MimetypesColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                MimetypesColumns.MIMETYPE + " TEXT NOT NULL" +
        ");");

        // Mimetype table requires an index on mime type
        db.execSQL("CREATE UNIQUE INDEX mime_type ON " + Tables.MIMETYPES + " (" +
                MimetypesColumns.MIMETYPE +
        ");");

        // Public generic data table
        db.execSQL("CREATE TABLE " + Tables.DATA + " (" +
                Data._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                DataColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                DataColumns.MIMETYPE_ID + " INTEGER REFERENCES mimetype(_id) NOT NULL," +
                Data.RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id) NOT NULL," +
                Data.HASH_ID + " TEXT," +
                Data.IS_READ_ONLY + " INTEGER NOT NULL DEFAULT 0," +
                Data.IS_PRIMARY + " INTEGER NOT NULL DEFAULT 0," +
                Data.IS_SUPER_PRIMARY + " INTEGER NOT NULL DEFAULT 0," +
                Data.DATA_VERSION + " INTEGER NOT NULL DEFAULT 0," +
                Data.DATA1 + " TEXT," +
                Data.DATA2 + " TEXT," +
                Data.DATA3 + " TEXT," +
                Data.DATA4 + " TEXT," +
                Data.DATA5 + " TEXT," +
                Data.DATA6 + " TEXT," +
                Data.DATA7 + " TEXT," +
                Data.DATA8 + " TEXT," +
                Data.DATA9 + " TEXT," +
                Data.DATA10 + " TEXT," +
                Data.DATA11 + " TEXT," +
                Data.DATA12 + " TEXT," +
                Data.DATA13 + " TEXT," +
                Data.DATA14 + " TEXT," +
                Data.DATA15 + " TEXT," +
                Data.SYNC1 + " TEXT, " +
                Data.SYNC2 + " TEXT, " +
                Data.SYNC3 + " TEXT, " +
                Data.SYNC4 + " TEXT, " +
                Data.CARRIER_PRESENCE + " INTEGER NOT NULL DEFAULT 0, " +
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME + " TEXT, " +
                Data.PREFERRED_PHONE_ACCOUNT_ID + " TEXT " +
        ");");

        db.execSQL("CREATE INDEX data_raw_contact_id ON " + Tables.DATA + " (" +
                Data.RAW_CONTACT_ID +
        ");");

        /**
         * For email lookup and similar queries.
         */
        db.execSQL("CREATE INDEX data_mimetype_data1_index ON " + Tables.DATA + " (" +
                DataColumns.MIMETYPE_ID + "," +
                Data.DATA1 +
        ");");

        /**
         * For contact backup restore queries.
         */
        db.execSQL("CREATE INDEX IF NOT EXISTS data_hash_id_index ON " + Tables.DATA + " (" +
                Data.HASH_ID +
        ");");


        // Private phone numbers table used for lookup
        db.execSQL("CREATE TABLE " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.DATA_ID
                        + " INTEGER REFERENCES data(_id) NOT NULL," +
                PhoneLookupColumns.RAW_CONTACT_ID
                        + " INTEGER REFERENCES raw_contacts(_id) NOT NULL," +
                PhoneLookupColumns.NORMALIZED_NUMBER + " TEXT NOT NULL," +
                PhoneLookupColumns.MIN_MATCH + " TEXT NOT NULL" +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.NORMALIZED_NUMBER + "," +
                PhoneLookupColumns.RAW_CONTACT_ID + "," +
                PhoneLookupColumns.DATA_ID +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_min_match_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.MIN_MATCH + "," +
                PhoneLookupColumns.RAW_CONTACT_ID + "," +
                PhoneLookupColumns.DATA_ID +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_data_id_min_match_index ON " + Tables.PHONE_LOOKUP +
                " (" + PhoneLookupColumns.DATA_ID + ", " + PhoneLookupColumns.MIN_MATCH + ");");

        // Private name/nickname table used for lookup.
        db.execSQL("CREATE TABLE " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.DATA_ID
                        + " INTEGER REFERENCES data(_id) NOT NULL," +
                NameLookupColumns.RAW_CONTACT_ID
                        + " INTEGER REFERENCES raw_contacts(_id) NOT NULL," +
                NameLookupColumns.NORMALIZED_NAME + " TEXT NOT NULL," +
                NameLookupColumns.NAME_TYPE + " INTEGER NOT NULL," +
                "PRIMARY KEY ("
                        + NameLookupColumns.DATA_ID + ", "
                        + NameLookupColumns.NORMALIZED_NAME + ", "
                        + NameLookupColumns.NAME_TYPE + ")" +
        ");");

        db.execSQL("CREATE INDEX name_lookup_raw_contact_id_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.RAW_CONTACT_ID +
        ");");

        db.execSQL("CREATE TABLE " + Tables.NICKNAME_LOOKUP + " (" +
                NicknameLookupColumns.NAME + " TEXT," +
                NicknameLookupColumns.CLUSTER + " TEXT" +
        ");");

        db.execSQL("CREATE UNIQUE INDEX nickname_lookup_index ON " + Tables.NICKNAME_LOOKUP + " (" +
                NicknameLookupColumns.NAME + ", " +
                NicknameLookupColumns.CLUSTER +
        ");");

        // Groups table.
        db.execSQL("CREATE TABLE " + Tables.GROUPS + " (" +
                Groups._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                GroupsColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                GroupsColumns.ACCOUNT_ID + " INTEGER REFERENCES " +
                    Tables.ACCOUNTS + "(" + AccountsColumns._ID + ")," +
                Groups.SOURCE_ID + " TEXT," +
                Groups.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                Groups.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                Groups.TITLE + " TEXT," +
                Groups.TITLE_RES + " INTEGER," +
                Groups.NOTES + " TEXT," +
                Groups.SYSTEM_ID + " TEXT," +
                Groups.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                Groups.GROUP_VISIBLE + " INTEGER NOT NULL DEFAULT 0," +
                Groups.SHOULD_SYNC + " INTEGER NOT NULL DEFAULT 1," +
                Groups.AUTO_ADD + " INTEGER NOT NULL DEFAULT 0," +
                Groups.FAVORITES + " INTEGER NOT NULL DEFAULT 0," +
                Groups.GROUP_IS_READ_ONLY + " INTEGER NOT NULL DEFAULT 0," +
                Groups.SYNC1 + " TEXT, " +
                Groups.SYNC2 + " TEXT, " +
                Groups.SYNC3 + " TEXT, " +
                Groups.SYNC4 + " TEXT " +
        ");");

        db.execSQL("CREATE INDEX groups_source_id_account_id_index ON " + Tables.GROUPS + " (" +
                Groups.SOURCE_ID + ", " +
                GroupsColumns.ACCOUNT_ID +
        ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                AggregationExceptions.TYPE + " INTEGER NOT NULL, " +
                AggregationExceptions.RAW_CONTACT_ID1
                        + " INTEGER REFERENCES raw_contacts(_id), " +
                AggregationExceptions.RAW_CONTACT_ID2
                        + " INTEGER REFERENCES raw_contacts(_id)" +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index1 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptions.RAW_CONTACT_ID1 + ", " +
                AggregationExceptions.RAW_CONTACT_ID2 +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index2 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptions.RAW_CONTACT_ID2 + ", " +
                AggregationExceptions.RAW_CONTACT_ID1 +
        ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.SETTINGS + " (" +
                Settings.ACCOUNT_NAME + " STRING NOT NULL," +
                Settings.ACCOUNT_TYPE + " STRING NOT NULL," +
                Settings.DATA_SET + " STRING," +
                Settings.UNGROUPED_VISIBLE + " INTEGER NOT NULL DEFAULT 0," +
                Settings.SHOULD_SYNC + " INTEGER NOT NULL DEFAULT 1" +
        ");");

        db.execSQL("CREATE TABLE " + Tables.VISIBLE_CONTACTS + " (" +
                Contacts._ID + " INTEGER PRIMARY KEY" +
        ");");

        db.execSQL("CREATE TABLE " + Tables.DEFAULT_DIRECTORY + " (" +
                Contacts._ID + " INTEGER PRIMARY KEY" +
        ");");

        db.execSQL("CREATE TABLE " + Tables.STATUS_UPDATES + " (" +
                StatusUpdatesColumns.DATA_ID + " INTEGER PRIMARY KEY REFERENCES data(_id)," +
                StatusUpdates.STATUS + " TEXT," +
                StatusUpdates.STATUS_TIMESTAMP + " INTEGER," +
                StatusUpdates.STATUS_RES_PACKAGE + " TEXT, " +
                StatusUpdates.STATUS_LABEL + " INTEGER, " +
                StatusUpdates.STATUS_ICON + " INTEGER" +
        ");");

        createDirectoriesTable(db);
        createSearchIndexTable(db, false /* we build stats table later */);

        db.execSQL("CREATE TABLE " + Tables.DATA_USAGE_STAT + "(" +
                DataUsageStatColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                DataUsageStatColumns.DATA_ID + " INTEGER NOT NULL, " +
                DataUsageStatColumns.USAGE_TYPE_INT + " INTEGER NOT NULL DEFAULT 0, " +

                DataUsageStatColumns.RAW_TIMES_USED + " INTEGER NOT NULL DEFAULT 0, " +
                DataUsageStatColumns.RAW_LAST_TIME_USED + " INTEGER NOT NULL DEFAULT 0, " +

                DataUsageStatColumns.LR_TIMES_USED + " INTEGER NOT NULL DEFAULT 0, " +
                DataUsageStatColumns.LR_LAST_TIME_USED + " INTEGER NOT NULL DEFAULT 0, " +

                "FOREIGN KEY(" + DataUsageStatColumns.DATA_ID + ") REFERENCES "
                        + Tables.DATA + "(" + Data._ID + ")" +
        ");");
        db.execSQL("CREATE UNIQUE INDEX data_usage_stat_index ON " +
                Tables.DATA_USAGE_STAT + " (" +
                DataUsageStatColumns.DATA_ID + ", " +
                DataUsageStatColumns.USAGE_TYPE_INT +
        ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS "
                + Tables.METADATA_SYNC + " (" +
                MetadataSync._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                MetadataSync.RAW_CONTACT_BACKUP_ID + " TEXT NOT NULL," +
                MetadataSyncColumns.ACCOUNT_ID + " INTEGER NOT NULL," +
                MetadataSync.DATA + " TEXT," +
                MetadataSync.DELETED + " INTEGER NOT NULL DEFAULT 0);");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS metadata_sync_index ON " +
                Tables.METADATA_SYNC + " (" +
                MetadataSync.RAW_CONTACT_BACKUP_ID + ", " +
                MetadataSyncColumns.ACCOUNT_ID +");");

        db.execSQL("CREATE TABLE " + Tables.PRE_AUTHORIZED_URIS + " ("+
                PreAuthorizedUris._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                PreAuthorizedUris.URI + " STRING NOT NULL, " +
                PreAuthorizedUris.EXPIRATION + " INTEGER NOT NULL DEFAULT 0);");

        db.execSQL("CREATE TABLE IF NOT EXISTS "
                + Tables.METADATA_SYNC_STATE + " (" +
                MetadataSyncState._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                MetadataSyncStateColumns.ACCOUNT_ID + " INTEGER NOT NULL," +
                MetadataSyncState.STATE + " BLOB);");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS metadata_sync_state_index ON " +
                Tables.METADATA_SYNC_STATE + " (" +
                MetadataSyncColumns.ACCOUNT_ID +");");

        // When adding new tables, be sure to also add size-estimates in updateSqliteStats
        createContactsViews(db);
        createGroupsView(db);
        createContactsTriggers(db);
        createContactsIndexes(db, false /* we build stats table later */);
        createPresenceTables(db);

        loadNicknameLookupTable(db);

        // Set sequence starts.
        initializeAutoIncrementSequences(db);

        // Add the legacy API support views, etc.
        LegacyApiSupport.createDatabase(db);

        if (mDatabaseOptimizationEnabled) {
            // This will create a sqlite_stat1 table that is used for query optimization
            db.execSQL("ANALYZE;");

            updateSqliteStats(db);
        }

        postOnCreate();
    }

    protected void postOnCreate() {
        // Only do this for the main DB, but not for the profile DB.

        notifyProviderStatusChange(mContext);

        // Trigger all sync adapters.
        ContentResolver.requestSync(null /* all accounts */,
                ContactsContract.AUTHORITY, new Bundle());

        // Send the broadcast.
        final Intent dbCreatedIntent = new Intent(
                ContactsContract.Intents.CONTACTS_DATABASE_CREATED);
        dbCreatedIntent.addFlags(Intent.FLAG_RECEIVER_REGISTERED_ONLY_BEFORE_BOOT);
        mContext.sendBroadcast(dbCreatedIntent, android.Manifest.permission.READ_CONTACTS);
    }

    protected void initializeAutoIncrementSequences(SQLiteDatabase db) {
        // Default implementation does nothing.
    }

    private void createDirectoriesTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + Tables.DIRECTORIES + "(" +
                Directory._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Directory.PACKAGE_NAME + " TEXT NOT NULL," +
                Directory.DIRECTORY_AUTHORITY + " TEXT NOT NULL," +
                Directory.TYPE_RESOURCE_ID + " INTEGER," +
                DirectoryColumns.TYPE_RESOURCE_NAME + " TEXT," +
                Directory.ACCOUNT_TYPE + " TEXT," +
                Directory.ACCOUNT_NAME + " TEXT," +
                Directory.DISPLAY_NAME + " TEXT, " +
                Directory.EXPORT_SUPPORT + " INTEGER NOT NULL" +
                        " DEFAULT " + Directory.EXPORT_SUPPORT_NONE + "," +
                Directory.SHORTCUT_SUPPORT + " INTEGER NOT NULL" +
                        " DEFAULT " + Directory.SHORTCUT_SUPPORT_NONE + "," +
                Directory.PHOTO_SUPPORT + " INTEGER NOT NULL" +
                        " DEFAULT " + Directory.PHOTO_SUPPORT_NONE +
        ");");

        // Trigger a full scan of directories in the system
        PropertyUtils.setProperty(db, DbProperties.DIRECTORY_SCAN_COMPLETE, "0");
    }

    public void createSearchIndexTable(SQLiteDatabase db, boolean rebuildSqliteStats) {
        db.beginTransactionNonExclusive();
        try {
            db.execSQL("DROP TABLE IF EXISTS " + Tables.SEARCH_INDEX);
            db.execSQL("CREATE VIRTUAL TABLE " + Tables.SEARCH_INDEX
                    + " USING FTS4 ("
                    + SearchIndexColumns.CONTACT_ID + " INTEGER REFERENCES contacts(_id) NOT NULL,"
                    + SearchIndexColumns.CONTENT + " TEXT, "
                    + SearchIndexColumns.NAME + " TEXT, "
                    + SearchIndexColumns.TOKENS + " TEXT"
                    + ")");
            if (rebuildSqliteStats) {
                updateSqliteStats(db);
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    private void createContactsTriggers(SQLiteDatabase db) {

        // Automatically delete Data rows when a raw contact is deleted.
        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.RAW_CONTACTS + "_deleted;");
        db.execSQL("CREATE TRIGGER " + Tables.RAW_CONTACTS + "_deleted "
                + "   BEFORE DELETE ON " + Tables.RAW_CONTACTS
                + " BEGIN "
                + "   DELETE FROM " + Tables.DATA
                + "     WHERE " + Data.RAW_CONTACT_ID
                                + "=OLD." + RawContacts._ID + ";"
                + "   DELETE FROM " + Tables.AGGREGATION_EXCEPTIONS
                + "     WHERE " + AggregationExceptions.RAW_CONTACT_ID1
                                + "=OLD." + RawContacts._ID
                + "        OR " + AggregationExceptions.RAW_CONTACT_ID2
                                + "=OLD." + RawContacts._ID + ";"
                + "   DELETE FROM " + Tables.VISIBLE_CONTACTS
                + "     WHERE " + Contacts._ID + "=OLD." + RawContacts.CONTACT_ID
                + "       AND (SELECT COUNT(*) FROM " + Tables.RAW_CONTACTS
                + "            WHERE " + RawContacts.CONTACT_ID + "=OLD." + RawContacts.CONTACT_ID
                + "           )=1;"
                + "   DELETE FROM " + Tables.DEFAULT_DIRECTORY
                + "     WHERE " + Contacts._ID + "=OLD." + RawContacts.CONTACT_ID
                + "       AND (SELECT COUNT(*) FROM " + Tables.RAW_CONTACTS
                + "            WHERE " + RawContacts.CONTACT_ID + "=OLD." + RawContacts.CONTACT_ID
                + "           )=1;"
                + "   DELETE FROM " + Tables.CONTACTS
                + "     WHERE " + Contacts._ID + "=OLD." + RawContacts.CONTACT_ID
                + "       AND (SELECT COUNT(*) FROM " + Tables.RAW_CONTACTS
                + "            WHERE " + RawContacts.CONTACT_ID + "=OLD." + RawContacts.CONTACT_ID
                + "           )=1;"
                + " END");


        db.execSQL("DROP TRIGGER IF EXISTS contacts_times_contacted;");
        db.execSQL("DROP TRIGGER IF EXISTS raw_contacts_times_contacted;");

        // Triggers that update {@link RawContacts#VERSION} when the contact is marked for deletion
        // or any time a data row is inserted, updated or deleted.
        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.RAW_CONTACTS + "_marked_deleted;");
        db.execSQL("CREATE TRIGGER " + Tables.RAW_CONTACTS + "_marked_deleted "
                + "   AFTER UPDATE ON " + Tables.RAW_CONTACTS
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET "
                +         RawContacts.VERSION + "=OLD." + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + RawContacts._ID
                + "       AND NEW." + RawContacts.DELETED + "!= OLD." + RawContacts.DELETED + ";"
                + " END");

        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.DATA + "_updated;");
        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_updated AFTER UPDATE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.DATA
                + "     SET " + Data.DATA_VERSION + "=OLD." + Data.DATA_VERSION + "+1 "
                + "     WHERE " + Data._ID + "=OLD." + Data._ID + ";"
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET " + RawContacts.VERSION + "=" + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + Data.RAW_CONTACT_ID + ";"
                + " END");

        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.DATA + "_deleted;");
        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_deleted BEFORE DELETE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET " + RawContacts.VERSION + "=" + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + Data.RAW_CONTACT_ID + ";"
                + "   DELETE FROM " + Tables.PHONE_LOOKUP
                + "     WHERE " + PhoneLookupColumns.DATA_ID + "=OLD." + Data._ID + ";"
                + "   DELETE FROM " + Tables.STATUS_UPDATES
                + "     WHERE " + StatusUpdatesColumns.DATA_ID + "=OLD." + Data._ID + ";"
                + "   DELETE FROM " + Tables.NAME_LOOKUP
                + "     WHERE " + NameLookupColumns.DATA_ID + "=OLD." + Data._ID + ";"
                + " END");


        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.GROUPS + "_updated1;");
        db.execSQL("CREATE TRIGGER " + Tables.GROUPS + "_updated1 "
                + "   AFTER UPDATE ON " + Tables.GROUPS
                + " BEGIN "
                + "   UPDATE " + Tables.GROUPS
                + "     SET "
                +         Groups.VERSION + "=OLD." + Groups.VERSION + "+1"
                + "     WHERE " + Groups._ID + "=OLD." + Groups._ID + ";"
                + " END");

        // Update DEFAULT_FILTER table per AUTO_ADD column update, see upgradeToVersion411.
        final String insertContactsWithoutAccount = (
                " INSERT OR IGNORE INTO " + Tables.DEFAULT_DIRECTORY +
                "     SELECT " + RawContacts.CONTACT_ID +
                "     FROM " + Tables.RAW_CONTACTS +
                "     WHERE " + RawContactsColumns.CONCRETE_ACCOUNT_ID +
                            "=" + Clauses.LOCAL_ACCOUNT_ID + ";");

        final String insertContactsWithAccountNoDefaultGroup = (
                " INSERT OR IGNORE INTO " + Tables.DEFAULT_DIRECTORY +
                "     SELECT " + RawContacts.CONTACT_ID +
                "         FROM " + Tables.RAW_CONTACTS +
                "     WHERE NOT EXISTS" +
                "         (SELECT " + Groups._ID +
                "             FROM " + Tables.GROUPS +
                "             WHERE " + RawContactsColumns.CONCRETE_ACCOUNT_ID + " = " +
                                    GroupsColumns.CONCRETE_ACCOUNT_ID +
                "             AND " + Groups.AUTO_ADD + " != 0" + ");");

        final String insertContactsWithAccountDefaultGroup = (
                " INSERT OR IGNORE INTO " + Tables.DEFAULT_DIRECTORY +
                "     SELECT " + RawContacts.CONTACT_ID +
                "         FROM " + Tables.RAW_CONTACTS +
                "     JOIN " + Tables.DATA +
                "           ON (" + RawContactsColumns.CONCRETE_ID + "=" +
                        Data.RAW_CONTACT_ID + ")" +
                "     WHERE " + DataColumns.MIMETYPE_ID + "=" +
                    "(SELECT " + MimetypesColumns._ID + " FROM " + Tables.MIMETYPES +
                        " WHERE " + MimetypesColumns.MIMETYPE +
                            "='" + GroupMembership.CONTENT_ITEM_TYPE + "')" +
                "     AND EXISTS" +
                "         (SELECT " + Groups._ID +
                "             FROM " + Tables.GROUPS +
                "                 WHERE " + RawContactsColumns.CONCRETE_ACCOUNT_ID + " = " +
                                        GroupsColumns.CONCRETE_ACCOUNT_ID +
                "                 AND " + Groups.AUTO_ADD + " != 0" + ");");

        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.GROUPS + "_auto_add_updated1;");
        db.execSQL("CREATE TRIGGER " + Tables.GROUPS + "_auto_add_updated1 "
                + "   AFTER UPDATE OF " + Groups.AUTO_ADD + " ON " + Tables.GROUPS
                + " BEGIN "
                + "   DELETE FROM " + Tables.DEFAULT_DIRECTORY + ";"
                    + insertContactsWithoutAccount
                    + insertContactsWithAccountNoDefaultGroup
                    + insertContactsWithAccountDefaultGroup
                + " END");
    }

    private void createContactsIndexes(SQLiteDatabase db, boolean rebuildSqliteStats) {
        db.execSQL("DROP INDEX IF EXISTS name_lookup_index");
        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + "," +
                NameLookupColumns.NAME_TYPE + ", " +
                NameLookupColumns.RAW_CONTACT_ID + ", " +
                NameLookupColumns.DATA_ID +
        ");");

        db.execSQL("DROP INDEX IF EXISTS raw_contact_sort_key1_index");
        db.execSQL("CREATE INDEX raw_contact_sort_key1_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContacts.SORT_KEY_PRIMARY +
        ");");

        db.execSQL("DROP INDEX IF EXISTS raw_contact_sort_key2_index");
        db.execSQL("CREATE INDEX raw_contact_sort_key2_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContacts.SORT_KEY_ALTERNATIVE +
        ");");

        if (rebuildSqliteStats) {
            updateSqliteStats(db);
        }
    }

    private void createContactsViews(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Views.CONTACTS + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.DATA + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.RAW_CONTACTS + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.RAW_ENTITIES + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.ENTITIES + ";");
        db.execSQL("DROP VIEW IF EXISTS view_data_usage_stat;");
        db.execSQL("DROP VIEW IF EXISTS " + Views.DATA_USAGE_LR + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.STREAM_ITEMS + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.METADATA_SYNC_STATE + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.METADATA_SYNC + ";");

        String dataColumns =
                Data.IS_PRIMARY + ", "
                + Data.IS_SUPER_PRIMARY + ", "
                + Data.DATA_VERSION + ", "
                + DataColumns.CONCRETE_PACKAGE_ID + ","
                + PackagesColumns.PACKAGE + " AS " + Data.RES_PACKAGE + ","
                + DataColumns.CONCRETE_MIMETYPE_ID + ","
                + MimetypesColumns.MIMETYPE + " AS " + Data.MIMETYPE + ", "
                + Data.IS_READ_ONLY + ", "
                + Data.DATA1 + ", "
                + Data.DATA2 + ", "
                + Data.DATA3 + ", "
                + Data.DATA4 + ", "
                + Data.DATA5 + ", "
                + Data.DATA6 + ", "
                + Data.DATA7 + ", "
                + Data.DATA8 + ", "
                + Data.DATA9 + ", "
                + Data.DATA10 + ", "
                + Data.DATA11 + ", "
                + Data.DATA12 + ", "
                + Data.DATA13 + ", "
                + Data.DATA14 + ", "
                + Data.DATA15 + ", "
                + Data.CARRIER_PRESENCE + ", "
                + Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME + ", "
                + Data.PREFERRED_PHONE_ACCOUNT_ID + ", "
                + Data.SYNC1 + ", "
                + Data.SYNC2 + ", "
                + Data.SYNC3 + ", "
                + Data.SYNC4;

        String syncColumns =
                RawContactsColumns.CONCRETE_ACCOUNT_ID + ","
                + AccountsColumns.CONCRETE_ACCOUNT_NAME + " AS " + RawContacts.ACCOUNT_NAME + ","
                + AccountsColumns.CONCRETE_ACCOUNT_TYPE + " AS " + RawContacts.ACCOUNT_TYPE + ","
                + AccountsColumns.CONCRETE_DATA_SET + " AS " + RawContacts.DATA_SET + ","
                + "(CASE WHEN " + AccountsColumns.CONCRETE_DATA_SET + " IS NULL THEN "
                            + AccountsColumns.CONCRETE_ACCOUNT_TYPE
                        + " ELSE " + AccountsColumns.CONCRETE_ACCOUNT_TYPE + "||'/'||"
                            + AccountsColumns.CONCRETE_DATA_SET + " END) AS "
                                + RawContacts.ACCOUNT_TYPE_AND_DATA_SET + ","
                + RawContactsColumns.CONCRETE_SOURCE_ID + " AS " + RawContacts.SOURCE_ID + ","
                + RawContactsColumns.CONCRETE_BACKUP_ID + " AS " + RawContacts.BACKUP_ID + ","
                + RawContactsColumns.CONCRETE_VERSION + " AS " + RawContacts.VERSION + ","
                + RawContactsColumns.CONCRETE_DIRTY + " AS " + RawContacts.DIRTY + ","
                + RawContactsColumns.CONCRETE_SYNC1 + " AS " + RawContacts.SYNC1 + ","
                + RawContactsColumns.CONCRETE_SYNC2 + " AS " + RawContacts.SYNC2 + ","
                + RawContactsColumns.CONCRETE_SYNC3 + " AS " + RawContacts.SYNC3 + ","
                + RawContactsColumns.CONCRETE_SYNC4 + " AS " + RawContacts.SYNC4;

        String baseContactColumns =
                Contacts.HAS_PHONE_NUMBER + ", "
                + Contacts.NAME_RAW_CONTACT_ID + ", "
                + Contacts.LOOKUP_KEY + ", "
                + Contacts.PHOTO_ID + ", "
                + Contacts.PHOTO_FILE_ID + ", "
                + "CAST(" + Clauses.CONTACT_VISIBLE + " AS INTEGER) AS "
                        + Contacts.IN_VISIBLE_GROUP + ", "
                + "CAST(" + Clauses.CONTACT_IN_DEFAULT_DIRECTORY + " AS INTEGER) AS "
                        + Contacts.IN_DEFAULT_DIRECTORY + ", "
                + ContactsColumns.LAST_STATUS_UPDATE_ID + ", "
                + ContactsColumns.CONCRETE_CONTACT_LAST_UPDATED_TIMESTAMP;

        String contactOptionColumns =
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE
                        + " AS " + Contacts.CUSTOM_RINGTONE + ","
                + ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                        + " AS " + Contacts.SEND_TO_VOICEMAIL + ","

                + "0 AS " + Contacts.RAW_LAST_TIME_CONTACTED + ","
                + "0 AS " + Contacts.RAW_TIMES_CONTACTED + ","

                + "0 AS " + Contacts.LR_LAST_TIME_CONTACTED + ","
                + "0 AS " + Contacts.LR_TIMES_CONTACTED + ","

                + ContactsColumns.CONCRETE_STARRED
                        + " AS " + Contacts.STARRED + ","
                + ContactsColumns.CONCRETE_PINNED
                        + " AS " + Contacts.PINNED;

        String contactNameColumns =
                "name_raw_contact." + RawContacts.DISPLAY_NAME_SOURCE
                        + " AS " + Contacts.DISPLAY_NAME_SOURCE + ", "
                + "name_raw_contact." + RawContacts.DISPLAY_NAME_PRIMARY
                        + " AS " + Contacts.DISPLAY_NAME_PRIMARY + ", "
                + "name_raw_contact." + RawContacts.DISPLAY_NAME_ALTERNATIVE
                        + " AS " + Contacts.DISPLAY_NAME_ALTERNATIVE + ", "
                + "name_raw_contact." + RawContacts.PHONETIC_NAME
                        + " AS " + Contacts.PHONETIC_NAME + ", "
                + "name_raw_contact." + RawContacts.PHONETIC_NAME_STYLE
                        + " AS " + Contacts.PHONETIC_NAME_STYLE + ", "
                + "name_raw_contact." + RawContacts.SORT_KEY_PRIMARY
                        + " AS " + Contacts.SORT_KEY_PRIMARY + ", "
                + "name_raw_contact." + RawContactsColumns.PHONEBOOK_LABEL_PRIMARY
                        + " AS " + ContactsColumns.PHONEBOOK_LABEL_PRIMARY + ", "
                + "name_raw_contact." + RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY
                        + " AS " + ContactsColumns.PHONEBOOK_BUCKET_PRIMARY + ", "
                + "name_raw_contact." + RawContacts.SORT_KEY_ALTERNATIVE
                        + " AS " + Contacts.SORT_KEY_ALTERNATIVE + ", "
                + "name_raw_contact." + RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE
                        + " AS " + ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE + ", "
                + "name_raw_contact." + RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE
                        + " AS " + ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE;

        String dataSelect = "SELECT "
                + DataColumns.CONCRETE_ID + " AS " + Data._ID + ","
                + Data.HASH_ID + ", "
                + Data.RAW_CONTACT_ID + ", "
                + RawContactsColumns.CONCRETE_CONTACT_ID + " AS " + RawContacts.CONTACT_ID + ", "
                + syncColumns + ", "
                + dataColumns + ", "
                + contactOptionColumns + ", "
                + contactNameColumns + ", "
                + baseContactColumns + ", "
                + buildDisplayPhotoUriAlias(RawContactsColumns.CONCRETE_CONTACT_ID,
                        Contacts.PHOTO_URI) + ", "
                + buildThumbnailPhotoUriAlias(RawContactsColumns.CONCRETE_CONTACT_ID,
                        Contacts.PHOTO_THUMBNAIL_URI) + ", "
                + dbForProfile() + " AS " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + ", "
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.DATA
                + " JOIN " + Tables.MIMETYPES + " ON ("
                +   DataColumns.CONCRETE_MIMETYPE_ID + "=" + MimetypesColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS + " ON ("
                +   DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + " JOIN " + Tables.CONTACTS + " ON ("
                +   RawContactsColumns.CONCRETE_CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS + " AS name_raw_contact ON("
                +   Contacts.NAME_RAW_CONTACT_ID + "=name_raw_contact." + RawContacts._ID + ")"
                + " LEFT OUTER JOIN " + Tables.PACKAGES + " ON ("
                +   DataColumns.CONCRETE_PACKAGE_ID + "=" + PackagesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.GROUPS + " ON ("
                +   MimetypesColumns.CONCRETE_MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE
                +   "' AND " + GroupsColumns.CONCRETE_ID + "="
                        + Tables.DATA + "." + GroupMembership.GROUP_ROW_ID + ")";

        db.execSQL("CREATE VIEW " + Views.DATA + " AS " + dataSelect);

        String rawContactOptionColumns =
                RawContacts.CUSTOM_RINGTONE + ","
                + RawContacts.SEND_TO_VOICEMAIL + ","
                + "0 AS " + RawContacts.RAW_LAST_TIME_CONTACTED + ","
                + "0 AS " + RawContacts.LR_LAST_TIME_CONTACTED + ","
                + "0 AS " + RawContacts.RAW_TIMES_CONTACTED + ","
                + "0 AS " + RawContacts.LR_TIMES_CONTACTED + ","
                + RawContacts.STARRED + ","
                + RawContacts.PINNED;

        String rawContactsSelect = "SELECT "
                + RawContactsColumns.CONCRETE_ID + " AS " + RawContacts._ID + ","
                + RawContacts.CONTACT_ID + ", "
                + RawContacts.AGGREGATION_MODE + ", "
                + RawContacts.RAW_CONTACT_IS_READ_ONLY + ", "
                + RawContacts.DELETED + ", "
                + RawContactsColumns.CONCRETE_METADATA_DIRTY + ", "
                + RawContacts.DISPLAY_NAME_SOURCE  + ", "
                + RawContacts.DISPLAY_NAME_PRIMARY  + ", "
                + RawContacts.DISPLAY_NAME_ALTERNATIVE  + ", "
                + RawContacts.PHONETIC_NAME  + ", "
                + RawContacts.PHONETIC_NAME_STYLE  + ", "
                + RawContacts.SORT_KEY_PRIMARY  + ", "
                + RawContactsColumns.PHONEBOOK_LABEL_PRIMARY  + ", "
                + RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY  + ", "
                + RawContacts.SORT_KEY_ALTERNATIVE + ", "
                + RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE  + ", "
                + RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE  + ", "
                + dbForProfile() + " AS " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + ", "
                + rawContactOptionColumns + ", "
                + syncColumns
                + " FROM " + Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")";

        db.execSQL("CREATE VIEW " + Views.RAW_CONTACTS + " AS " + rawContactsSelect);

        String contactsColumns =
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE
                        + " AS " + Contacts.CUSTOM_RINGTONE + ", "
                + contactNameColumns + ", "
                + baseContactColumns + ", "

                + "0 AS " + Contacts.RAW_LAST_TIME_CONTACTED + ", "
                + "0 AS " + Contacts.LR_LAST_TIME_CONTACTED + ", "

                + ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                        + " AS " + Contacts.SEND_TO_VOICEMAIL + ", "
                + ContactsColumns.CONCRETE_STARRED
                        + " AS " + Contacts.STARRED + ", "
                + ContactsColumns.CONCRETE_PINNED
                + " AS " + Contacts.PINNED + ", "

                + "0 AS " + Contacts.RAW_TIMES_CONTACTED + ", "
                + "0 AS " + Contacts.LR_TIMES_CONTACTED;

        String contactsSelect = "SELECT "
                + ContactsColumns.CONCRETE_ID + " AS " + Contacts._ID + ","
                + contactsColumns + ", "
                + buildDisplayPhotoUriAlias(ContactsColumns.CONCRETE_ID, Contacts.PHOTO_URI) + ", "
                + buildThumbnailPhotoUriAlias(ContactsColumns.CONCRETE_ID,
                        Contacts.PHOTO_THUMBNAIL_URI) + ", "
                + dbForProfile() + " AS " + Contacts.IS_USER_PROFILE
                + " FROM " + Tables.CONTACTS
                + " JOIN " + Tables.RAW_CONTACTS + " AS name_raw_contact ON("
                +   Contacts.NAME_RAW_CONTACT_ID + "=name_raw_contact." + RawContacts._ID + ")";

        db.execSQL("CREATE VIEW " + Views.CONTACTS + " AS " + contactsSelect);

        String rawEntitiesSelect = "SELECT "
                + RawContacts.CONTACT_ID + ", "
                + RawContactsColumns.CONCRETE_DELETED + " AS " + RawContacts.DELETED + ","
                + RawContactsColumns.CONCRETE_METADATA_DIRTY + ", "
                + dataColumns + ", "
                + syncColumns + ", "
                + Data.SYNC1 + ", "
                + Data.SYNC2 + ", "
                + Data.SYNC3 + ", "
                + Data.SYNC4 + ", "
                + RawContactsColumns.CONCRETE_ID + " AS " + RawContacts._ID + ", "
                + DataColumns.CONCRETE_ID + " AS " + RawContacts.Entity.DATA_ID + ","
                + RawContactsColumns.CONCRETE_STARRED + " AS " + RawContacts.STARRED + ","
                + dbForProfile() + " AS " + RawContacts.RAW_CONTACT_IS_USER_PROFILE + ","
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + " LEFT OUTER JOIN " + Tables.DATA + " ON ("
                +   DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.PACKAGES + " ON ("
                +   DataColumns.CONCRETE_PACKAGE_ID + "=" + PackagesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.MIMETYPES + " ON ("
                +   DataColumns.CONCRETE_MIMETYPE_ID + "=" + MimetypesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.GROUPS + " ON ("
                +   MimetypesColumns.CONCRETE_MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE
                +   "' AND " + GroupsColumns.CONCRETE_ID + "="
                + Tables.DATA + "." + GroupMembership.GROUP_ROW_ID + ")";

        db.execSQL("CREATE VIEW " + Views.RAW_ENTITIES + " AS "
                + rawEntitiesSelect);

        String entitiesSelect = "SELECT "
                + RawContactsColumns.CONCRETE_CONTACT_ID + " AS " + Contacts._ID + ", "
                + RawContactsColumns.CONCRETE_CONTACT_ID + " AS " + RawContacts.CONTACT_ID + ", "
                + RawContactsColumns.CONCRETE_DELETED + " AS " + RawContacts.DELETED + ","
                + RawContactsColumns.CONCRETE_METADATA_DIRTY + ", "
                + dataColumns + ", "
                + syncColumns + ", "
                + contactsColumns + ", "
                + buildDisplayPhotoUriAlias(RawContactsColumns.CONCRETE_CONTACT_ID,
                        Contacts.PHOTO_URI) + ", "
                + buildThumbnailPhotoUriAlias(RawContactsColumns.CONCRETE_CONTACT_ID,
                        Contacts.PHOTO_THUMBNAIL_URI) + ", "
                + dbForProfile() + " AS " + Contacts.IS_USER_PROFILE + ", "
                + Data.SYNC1 + ", "
                + Data.SYNC2 + ", "
                + Data.SYNC3 + ", "
                + Data.SYNC4 + ", "
                + RawContactsColumns.CONCRETE_ID + " AS " + Contacts.Entity.RAW_CONTACT_ID + ", "
                + DataColumns.CONCRETE_ID + " AS " + Contacts.Entity.DATA_ID + ","
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.RAW_CONTACTS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + " JOIN " + Tables.CONTACTS + " ON ("
                +   RawContactsColumns.CONCRETE_CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS + " AS name_raw_contact ON("
                +   Contacts.NAME_RAW_CONTACT_ID + "=name_raw_contact." + RawContacts._ID + ")"
                + " LEFT OUTER JOIN " + Tables.DATA + " ON ("
                +   DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.PACKAGES + " ON ("
                +   DataColumns.CONCRETE_PACKAGE_ID + "=" + PackagesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.MIMETYPES + " ON ("
                +   DataColumns.CONCRETE_MIMETYPE_ID + "=" + MimetypesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.GROUPS + " ON ("
                +   MimetypesColumns.CONCRETE_MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE
                +   "' AND " + GroupsColumns.CONCRETE_ID + "="
                + Tables.DATA + "." + GroupMembership.GROUP_ROW_ID + ")";

        db.execSQL("CREATE VIEW " + Views.ENTITIES + " AS "
                + entitiesSelect);

        // View on top of DATA_USAGE_STAT, which is always empty.
        final String dataUsageViewSelect = "SELECT "
                + DataUsageStatColumns._ID + ", "
                + DataUsageStatColumns.DATA_ID + ", "
                + DataUsageStatColumns.USAGE_TYPE_INT + ", "
                + "0 AS " + DataUsageStatColumns.RAW_TIMES_USED + ", "
                + "0 AS " + DataUsageStatColumns.RAW_LAST_TIME_USED + ","
                + "0 AS " + DataUsageStatColumns.LR_TIMES_USED + ","
                + "0 AS " + DataUsageStatColumns.LR_LAST_TIME_USED
                + " FROM " + Tables.DATA_USAGE_STAT
                + " WHERE 0";

        // When the data_usage_stat table is needed with the low-res columns, use this, which is
        // faster than the DATA_USAGE_STAT view since it doesn't involve joins.
        db.execSQL("CREATE VIEW " + Views.DATA_USAGE_LR + " AS " + dataUsageViewSelect);

        String streamItemSelect = "SELECT " +
                StreamItemsColumns.CONCRETE_ID + ", " +
                ContactsColumns.CONCRETE_ID + " AS " + StreamItems.CONTACT_ID + ", " +
                ContactsColumns.CONCRETE_LOOKUP_KEY +
                        " AS " + StreamItems.CONTACT_LOOKUP_KEY + ", " +
                AccountsColumns.CONCRETE_ACCOUNT_NAME + ", " +
                AccountsColumns.CONCRETE_ACCOUNT_TYPE + ", " +
                AccountsColumns.CONCRETE_DATA_SET + ", " +
                StreamItemsColumns.CONCRETE_RAW_CONTACT_ID +
                        " as " + StreamItems.RAW_CONTACT_ID + ", " +
                RawContactsColumns.CONCRETE_SOURCE_ID +
                        " as " + StreamItems.RAW_CONTACT_SOURCE_ID + ", " +
                StreamItemsColumns.CONCRETE_PACKAGE + ", " +
                StreamItemsColumns.CONCRETE_ICON + ", " +
                StreamItemsColumns.CONCRETE_LABEL + ", " +
                StreamItemsColumns.CONCRETE_TEXT + ", " +
                StreamItemsColumns.CONCRETE_TIMESTAMP + ", " +
                StreamItemsColumns.CONCRETE_COMMENTS + ", " +
                StreamItemsColumns.CONCRETE_SYNC1 + ", " +
                StreamItemsColumns.CONCRETE_SYNC2 + ", " +
                StreamItemsColumns.CONCRETE_SYNC3 + ", " +
                StreamItemsColumns.CONCRETE_SYNC4 +
                " FROM " + Tables.STREAM_ITEMS
                + " JOIN " + Tables.RAW_CONTACTS + " ON ("
                + StreamItemsColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID
                    + ")"
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                    + ")"
                + " JOIN " + Tables.CONTACTS + " ON ("
                + RawContactsColumns.CONCRETE_CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID + ")";

        db.execSQL("CREATE VIEW " + Views.STREAM_ITEMS + " AS " + streamItemSelect);

        String metadataSyncSelect = "SELECT " +
                MetadataSyncColumns.CONCRETE_ID + ", " +
                MetadataSync.RAW_CONTACT_BACKUP_ID + ", " +
                AccountsColumns.ACCOUNT_NAME + ", " +
                AccountsColumns.ACCOUNT_TYPE + ", " +
                AccountsColumns.DATA_SET + ", " +
                MetadataSync.DATA + ", " +
                MetadataSync.DELETED +
                " FROM " + Tables.METADATA_SYNC
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   MetadataSyncColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                + ")";

        db.execSQL("CREATE VIEW " + Views.METADATA_SYNC + " AS " + metadataSyncSelect);

        String metadataSyncStateSelect = "SELECT " +
                MetadataSyncStateColumns.CONCRETE_ID + ", " +
                AccountsColumns.ACCOUNT_NAME + ", " +
                AccountsColumns.ACCOUNT_TYPE + ", " +
                AccountsColumns.DATA_SET + ", " +
                MetadataSyncState.STATE +
                " FROM " + Tables.METADATA_SYNC_STATE
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                +   MetadataSyncStateColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID
                + ")";

        db.execSQL("CREATE VIEW " + Views.METADATA_SYNC_STATE + " AS " + metadataSyncStateSelect);
    }

    private static String buildDisplayPhotoUriAlias(String contactIdColumn, String alias) {
        return "(CASE WHEN " + Contacts.PHOTO_FILE_ID + " IS NULL THEN (CASE WHEN "
                + Contacts.PHOTO_ID + " IS NULL"
                + " OR " + Contacts.PHOTO_ID + "=0"
                + " THEN NULL"
                + " ELSE '" + Contacts.CONTENT_URI + "/'||"
                        + contactIdColumn + "|| '/" + Photo.CONTENT_DIRECTORY + "'"
                + " END) ELSE '" + DisplayPhoto.CONTENT_URI + "/'||"
                        + Contacts.PHOTO_FILE_ID + " END)"
                + " AS " + alias;
    }

    private static String buildThumbnailPhotoUriAlias(String contactIdColumn, String alias) {
        return "(CASE WHEN "
                + Contacts.PHOTO_ID + " IS NULL"
                + " OR " + Contacts.PHOTO_ID + "=0"
                + " THEN NULL"
                + " ELSE '" + Contacts.CONTENT_URI + "/'||"
                        + contactIdColumn + "|| '/" + Photo.CONTENT_DIRECTORY + "'"
                + " END)"
                + " AS " + alias;
    }

    /**
     * Returns the value to be returned when querying the column indicating that the contact
     * or raw contact belongs to the user's personal profile.  Overridden in the profile
     * DB helper subclass.
     */
    protected int dbForProfile() {
        return 0;
    }

    private void createGroupsView(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Views.GROUPS + ";");

        String groupsColumns =
                GroupsColumns.CONCRETE_ACCOUNT_ID + " AS " + GroupsColumns.ACCOUNT_ID + ","
                + AccountsColumns.CONCRETE_ACCOUNT_NAME + " AS " + Groups.ACCOUNT_NAME + ","
                + AccountsColumns.CONCRETE_ACCOUNT_TYPE + " AS " + Groups.ACCOUNT_TYPE + ","
                + AccountsColumns.CONCRETE_DATA_SET + " AS " + Groups.DATA_SET + ","
                + "(CASE WHEN " + AccountsColumns.CONCRETE_DATA_SET
                    + " IS NULL THEN " + AccountsColumns.CONCRETE_ACCOUNT_TYPE
                    + " ELSE " + AccountsColumns.CONCRETE_ACCOUNT_TYPE
                        + "||'/'||" + AccountsColumns.CONCRETE_DATA_SET + " END) AS "
                            + Groups.ACCOUNT_TYPE_AND_DATA_SET + ","
                + Groups.SOURCE_ID + ","
                + Groups.VERSION + ","
                + Groups.DIRTY + ","
                + Groups.TITLE + ","
                + Groups.TITLE_RES + ","
                + Groups.NOTES + ","
                + Groups.SYSTEM_ID + ","
                + Groups.DELETED + ","
                + Groups.GROUP_VISIBLE + ","
                + Groups.SHOULD_SYNC + ","
                + Groups.AUTO_ADD + ","
                + Groups.FAVORITES + ","
                + Groups.GROUP_IS_READ_ONLY + ","
                + Groups.SYNC1 + ","
                + Groups.SYNC2 + ","
                + Groups.SYNC3 + ","
                + Groups.SYNC4 + ","
                + PackagesColumns.PACKAGE + " AS " + Groups.RES_PACKAGE;

        String groupsSelect = "SELECT "
                + GroupsColumns.CONCRETE_ID + " AS " + Groups._ID + ","
                + groupsColumns
                + " FROM " + Tables.GROUPS
                + " JOIN " + Tables.ACCOUNTS + " ON ("
                    + GroupsColumns.CONCRETE_ACCOUNT_ID + "=" + AccountsColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.PACKAGES + " ON ("
                    + GroupsColumns.CONCRETE_PACKAGE_ID + "=" + PackagesColumns.CONCRETE_ID + ")";

        db.execSQL("CREATE VIEW " + Views.GROUPS + " AS " + groupsSelect);
    }

    @Override
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG, "ContactsProvider cannot proceed because downgrading your database is not " +
                "supported. To continue, please either re-upgrade to your previous Android " +
                "version, or clear all application data in Contacts Storage (this will result " +
                "in the loss of all local contacts that are not synced). To avoid data loss, " +
                "your contacts database will not be wiped automatically.");
        super.onDowngrade(db, oldVersion, newVersion);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG,
                "Upgrading " + DATABASE_NAME + " from version " + oldVersion + " to " + newVersion);

        prepopulateCommonMimeTypes(db);

        boolean upgradeViewsAndTriggers = false;
        boolean upgradeNameLookup = false;
        boolean upgradeLegacyApiSupport = false;
        boolean upgradeSearchIndex = false;
        boolean rescanDirectories = false;
        boolean rebuildSqliteStats = false;
        boolean upgradeLocaleSpecificData = false;

        if (oldVersion < 701) {
            upgradeToVersion701(db);
            oldVersion = 701;
        }

        if (oldVersion < 702) {
            upgradeToVersion702(db);
            oldVersion = 702;
        }

        if (oldVersion < 703) {
            // Now names like "L'Image" will be searchable.
            upgradeSearchIndex = true;
            oldVersion = 703;
        }

        if (oldVersion < 704) {
            db.execSQL("DROP TABLE IF EXISTS activities;");
            oldVersion = 704;
        }

        if (oldVersion < 705) {
            // Before this version, we didn't rebuild the search index on locale changes, so
            // if the locale has changed after sync, the index contains gets stale.
            // To correct the issue we have to rebuild the index here.
            upgradeSearchIndex = true;
            oldVersion = 705;
        }

        if (oldVersion < 706) {
            // Prior to this version, we didn't rebuild the stats table after drop operations,
            // which resulted in losing some of the rows from the stats table.
            rebuildSqliteStats = true;
            oldVersion = 706;
        }

        if (oldVersion < 707) {
            upgradeToVersion707(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 707;
        }

        if (oldVersion < 708) {
            // Sort keys, phonebook labels and buckets, and search keys have
            // changed so force a rebuild.
            upgradeLocaleSpecificData = true;
            oldVersion = 708;
        }
        if (oldVersion < 709) {
            // Added secondary locale phonebook labels; changed Japanese
            // and Chinese sort keys.
            upgradeLocaleSpecificData = true;
            oldVersion = 709;
        }

        if (oldVersion < 710) {
            upgradeToVersion710(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 710;
        }

        if (oldVersion < 800) {
            upgradeToVersion800(db);
            oldVersion = 800;
        }

        if (oldVersion < 801) {
            PropertyUtils.setProperty(db, DbProperties.DATABASE_TIME_CREATED, String.valueOf(
                    System.currentTimeMillis()));
            oldVersion = 801;
        }

        if (oldVersion < 802) {
            upgradeToVersion802(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 802;
        }

        if (oldVersion < 803) {
            // Rebuild the search index so that names, organizations and nicknames are
            // now indexed as names.
            upgradeSearchIndex = true;
            oldVersion = 803;
        }

        if (oldVersion < 804) {
            // Reserved.
            oldVersion = 804;
        }

        if (oldVersion < 900) {
            upgradeViewsAndTriggers = true;
            oldVersion = 900;
        }

        if (oldVersion < 901) {
            // Rebuild the search index to fix any search index that was previously in a
            // broken state due to b/11059351
            upgradeSearchIndex = true;
            oldVersion = 901;
        }

        if (oldVersion < 902) {
            upgradeToVersion902(db);
            oldVersion = 902;
        }

        if (oldVersion < 903) {
            upgradeToVersion903(db);
            oldVersion = 903;
        }

        if (oldVersion < 904) {
            upgradeToVersion904(db);
            oldVersion = 904;
        }

        if (oldVersion < 905) {
            upgradeToVersion905(db);
            oldVersion = 905;
        }

        if (oldVersion < 906) {
            upgradeToVersion906(db);
            oldVersion = 906;
        }

        if (oldVersion < 907) {
            // Rebuild NAME_LOOKUP.
            upgradeNameLookup = true;
            oldVersion = 907;
        }

        if (oldVersion < 908) {
            upgradeToVersion908(db);
            oldVersion = 908;
        }

        if (oldVersion < 909) {
            upgradeToVersion909(db);
            oldVersion = 909;
        }

        if (oldVersion < 910) {
            upgradeToVersion910(db);
            oldVersion = 910;
        }
        if (oldVersion < 1000) {
            upgradeToVersion1000(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1000;
        }

        if (oldVersion < 1002) {
            rebuildSqliteStats = true;
            upgradeToVersion1002(db);
            oldVersion = 1002;
        }

        if (oldVersion < 1003) {
            upgradeToVersion1003(db);
            oldVersion = 1003;
        }

        if (oldVersion < 1004) {
            upgradeToVersion1004(db);
            oldVersion = 1004;
        }

        if (oldVersion < 1005) {
            upgradeToVersion1005(db);
            oldVersion = 1005;
        }

        if (oldVersion < 1006) {
            upgradeViewsAndTriggers = true;
            oldVersion = 1006;
        }

        if (oldVersion < 1007) {
            upgradeToVersion1007(db);
            oldVersion = 1007;
        }

        if (oldVersion < 1009) {
            upgradeToVersion1009(db);
            oldVersion = 1009;
        }

        if (oldVersion < 1100) {
            upgradeToVersion1100(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1100;
        }

        if (oldVersion < 1101) {
            upgradeToVersion1101(db);
            oldVersion = 1101;
        }

        if (oldVersion < 1102) {
            // Version 1009 was added *after* 1100/1101.  For master devices
            // that have already been updated to 1101, we do it again.
            upgradeToVersion1009(db);
            oldVersion = 1102;
        }

        if (oldVersion < 1103) {
            upgradeViewsAndTriggers = true;
            oldVersion = 1103;
        }

        if (oldVersion < 1104) {
            upgradeToVersion1104(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1104;
        }

        if (oldVersion < 1105) {
            upgradeToVersion1105(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1105;
        }

        if (oldVersion < 1106) {
            upgradeToVersion1106(db);
            oldVersion = 1106;
        }

        if (oldVersion < 1107) {
            upgradeToVersion1107(db);
            oldVersion = 1107;
        }

        if (oldVersion < 1108) {
            upgradeToVersion1108(db);
            oldVersion = 1108;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1109)) {
            upgradeToVersion1109(db);
            oldVersion = 1109;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1110)) {
            upgradeToVersion1110(db);
            oldVersion = 1110;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1111)) {
            upgradeToVersion1111(db);
            oldVersion = 1111;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1200)) {
            createPresenceTables(db);
            oldVersion = 1200;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1201)) {
            upgradeToVersion1201(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1201;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1202)) {
            upgradeViewsAndTriggers = true;
            oldVersion = 1202;
        }

        if (isUpgradeRequired(oldVersion,newVersion, 1300)) {
            upgradeToVersion1300(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1300;
        }

        if (isUpgradeRequired(oldVersion, newVersion, 1400)) {
            ContactsProvider2.deleteDataUsage(db);
            upgradeViewsAndTriggers = true;
            oldVersion = 1400;
        }

        // We extracted "calls" and "voicemail_status" at this point, but we can't remove them here
        // yet, until CallLogDatabaseHelper moves the data.

        if (upgradeViewsAndTriggers) {
            createContactsViews(db);
            createGroupsView(db);
            createContactsTriggers(db);
            createContactsIndexes(db, false /* we build stats table later */);
            upgradeLegacyApiSupport = true;
            rebuildSqliteStats = true;
        }

        if (upgradeLegacyApiSupport) {
            LegacyApiSupport.createViews(db);
        }

        if (upgradeLocaleSpecificData) {
            upgradeLocaleData(db, false /* we build stats table later */);
            // Name lookups are rebuilt as part of the full locale rebuild
            upgradeNameLookup = false;
            upgradeSearchIndex = true;
            rebuildSqliteStats = true;
        }

        if (upgradeNameLookup) {
            rebuildNameLookup(db, false /* we build stats table later */);
            rebuildSqliteStats = true;
        }

        if (upgradeSearchIndex) {
            rebuildSearchIndex(db, false /* we build stats table later */);
            rebuildSqliteStats = true;
        }

        if (rescanDirectories) {
            // Force the next ContactDirectoryManager.scanAllPackages() to rescan all packages.
            // (It's called from the BACKGROUND_TASK_UPDATE_ACCOUNTS background task.)
            PropertyUtils.setProperty(db, DbProperties.DIRECTORY_SCAN_COMPLETE, "0");
        }

        if (rebuildSqliteStats) {
            updateSqliteStats(db);
        }

        if (oldVersion != newVersion) {
            throw new IllegalStateException(
                    "error upgrading the database to version " + newVersion);
        }
    }

    private static boolean isUpgradeRequired(int oldVersion, int newVersion, int version) {
        return oldVersion < version && newVersion >= version;
    }

    private void rebuildNameLookup(SQLiteDatabase db, boolean rebuildSqliteStats) {
        db.execSQL("DROP INDEX IF EXISTS name_lookup_index");
        insertNameLookup(db);
        createContactsIndexes(db, rebuildSqliteStats);
    }

    protected void rebuildSearchIndex() {
        rebuildSearchIndex(getWritableDatabase(), true);
    }

    private void rebuildSearchIndex(SQLiteDatabase db, boolean rebuildSqliteStats) {
        createSearchIndexTable(db, rebuildSqliteStats);
        PropertyUtils.setProperty(db, SearchIndexManager.PROPERTY_SEARCH_INDEX_VERSION, "0");
    }

    /**
     * Checks whether the current ICU code version matches that used to build
     * the locale specific data in the ContactsDB.
     */
    public boolean needsToUpdateLocaleData(LocaleSet locales) {
        final String dbLocale = getProperty(DbProperties.LOCALE, "");
        if (!dbLocale.equals(locales.toString())) {
            return true;
        }
        final String curICUVersion = getDeviceIcuVersion();
        final String dbICUVersion = getProperty(DbProperties.ICU_VERSION,
                "(unknown)");
        if (!curICUVersion.equals(dbICUVersion)) {
            Log.i(TAG, "ICU version has changed. Current version is "
                    + curICUVersion + "; DB was built with " + dbICUVersion);
            return true;
        }
        return false;
    }

    private static String getDeviceIcuVersion() {
        return VersionInfo.ICU_VERSION.toString();
    }

    private void upgradeLocaleData(SQLiteDatabase db, boolean rebuildSqliteStats) {
        final LocaleSet locales = LocaleSet.newDefault();
        Log.i(TAG, "Upgrading locale data for " + locales
                + " (ICU v" + getDeviceIcuVersion() + ")");
        final long start = SystemClock.elapsedRealtime();
        rebuildLocaleData(db, locales, rebuildSqliteStats);
        Log.i(TAG, "Locale update completed in " + (SystemClock.elapsedRealtime() - start) + "ms");
    }

    private void rebuildLocaleData(SQLiteDatabase db, LocaleSet locales, boolean rebuildSqliteStats) {
        db.execSQL("DROP INDEX raw_contact_sort_key1_index");
        db.execSQL("DROP INDEX raw_contact_sort_key2_index");
        db.execSQL("DROP INDEX IF EXISTS name_lookup_index");

        loadNicknameLookupTable(db);
        insertNameLookup(db);
        rebuildSortKeys(db);
        createContactsIndexes(db, rebuildSqliteStats);

        FastScrollingIndexCache.getInstance(mContext).invalidate();
        // Update the ICU version used to generate the locale derived data
        // so we can tell when we need to rebuild with new ICU versions.
        PropertyUtils.setProperty(db, DbProperties.ICU_VERSION, getDeviceIcuVersion());
        PropertyUtils.setProperty(db, DbProperties.LOCALE, locales.toString());
    }

    /**
     * Regenerates all locale-sensitive data if needed:
     * nickname_lookup, name_lookup and sort keys. Invalidates the fast
     * scrolling index cache.
     */
    public void setLocale(LocaleSet locales) {
        if (!needsToUpdateLocaleData(locales)) {
            return;
        }
        Log.i(TAG, "Switching to locale " + locales
                + " (ICU v" + getDeviceIcuVersion() + ")");

        final long start = SystemClock.elapsedRealtime();
        SQLiteDatabase db = getWritableDatabase();
        db.setLocale(locales.getPrimaryLocale());
        db.beginTransaction();
        try {
            rebuildLocaleData(db, locales, true);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        Log.i(TAG, "Locale change completed in " + (SystemClock.elapsedRealtime() - start) + "ms");
    }

    /**
     * Regenerates sort keys for all contacts.
     */
    private void rebuildSortKeys(SQLiteDatabase db) {
        Cursor cursor = db.query(Tables.RAW_CONTACTS, new String[] {RawContacts._ID},
                null, null, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long rawContactId = cursor.getLong(0);
                updateRawContactDisplayName(db, rawContactId);
            }
        } finally {
            cursor.close();
        }
    }

    private void insertNameLookup(SQLiteDatabase db) {
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP);

        final SQLiteStatement nameLookupInsert = db.compileStatement(
                "INSERT OR IGNORE INTO " + Tables.NAME_LOOKUP + "("
                        + NameLookupColumns.RAW_CONTACT_ID + ","
                        + NameLookupColumns.DATA_ID + ","
                        + NameLookupColumns.NAME_TYPE + ","
                        + NameLookupColumns.NORMALIZED_NAME +
                ") VALUES (?,?,?,?)");

        try {
            insertStructuredNameLookup(db, nameLookupInsert);
            insertEmailLookup(db, nameLookupInsert);
            insertNicknameLookup(db, nameLookupInsert);
        } finally {
            nameLookupInsert.close();
        }
    }

    /**
     * Inserts name lookup rows for all structured names in the database.
     */
    private void insertStructuredNameLookup(SQLiteDatabase db, SQLiteStatement nameLookupInsert) {
        NameSplitter nameSplitter = createNameSplitter();
        NameLookupBuilder nameLookupBuilder = new StructuredNameLookupBuilder(nameSplitter,
                new CommonNicknameCache(db), nameLookupInsert);
        final long mimeTypeId = lookupMimeTypeId(db, StructuredName.CONTENT_ITEM_TYPE);
        Cursor cursor = db.query(StructuredNameQuery.TABLE, StructuredNameQuery.COLUMNS,
                StructuredNameQuery.SELECTION, new String[] {String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(StructuredNameQuery.ID);
                long rawContactId = cursor.getLong(StructuredNameQuery.RAW_CONTACT_ID);
                String name = cursor.getString(StructuredNameQuery.DISPLAY_NAME);
                int fullNameStyle = nameSplitter.guessFullNameStyle(name);
                fullNameStyle = nameSplitter.getAdjustedFullNameStyle(fullNameStyle);
                nameLookupBuilder.insertNameLookup(rawContactId, dataId, name, fullNameStyle);
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts name lookup rows for all email addresses in the database.
     */
    private void insertEmailLookup(SQLiteDatabase db, SQLiteStatement nameLookupInsert) {
        final long mimeTypeId = lookupMimeTypeId(db, Email.CONTENT_ITEM_TYPE);
        Cursor cursor = db.query(EmailQuery.TABLE, EmailQuery.COLUMNS,
                EmailQuery.SELECTION, new String[] {String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(EmailQuery.ID);
                long rawContactId = cursor.getLong(EmailQuery.RAW_CONTACT_ID);
                String address = cursor.getString(EmailQuery.ADDRESS);
                address = extractHandleFromEmailAddress(address);
                insertNameLookup(nameLookupInsert, rawContactId, dataId,
                        NameLookupType.EMAIL_BASED_NICKNAME, address);
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts name lookup rows for all nicknames in the database.
     */
    private void insertNicknameLookup(SQLiteDatabase db, SQLiteStatement nameLookupInsert) {
        final long mimeTypeId = lookupMimeTypeId(db, Nickname.CONTENT_ITEM_TYPE);
        Cursor cursor = db.query(NicknameQuery.TABLE, NicknameQuery.COLUMNS,
                NicknameQuery.SELECTION, new String[]{String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(NicknameQuery.ID);
                long rawContactId = cursor.getLong(NicknameQuery.RAW_CONTACT_ID);
                String nickname = cursor.getString(NicknameQuery.NAME);
                insertNameLookup(nameLookupInsert, rawContactId, dataId,
                        NameLookupType.NICKNAME, nickname);
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts a record in the {@link Tables#NAME_LOOKUP} table.
     */
    public void insertNameLookup(SQLiteStatement stmt, long rawContactId, long dataId,
            int lookupType, String name) {
        if (TextUtils.isEmpty(name)) {
            return;
        }

        String normalized = NameNormalizer.normalize(name);
        if (TextUtils.isEmpty(normalized)) {
            return;
        }

        insertNormalizedNameLookup(stmt, rawContactId, dataId, lookupType, normalized);
    }

    private void insertNormalizedNameLookup(SQLiteStatement stmt, long rawContactId, long dataId,
            int lookupType, String normalizedName) {
        stmt.bindLong(1, rawContactId);
        stmt.bindLong(2, dataId);
        stmt.bindLong(3, lookupType);
        stmt.bindString(4, normalizedName);
        stmt.executeInsert();
    }

    private void upgradeToVersion701(SQLiteDatabase db) {
        db.execSQL("UPDATE raw_contacts SET last_time_contacted =" +
                " max(ifnull(last_time_contacted, 0), " +
                " ifnull((SELECT max(last_time_used) " +
                    " FROM data JOIN data_usage_stat ON (data._id = data_usage_stat.data_id)" +
                    " WHERE data.raw_contact_id = raw_contacts._id), 0))");
        // Replace 0 with null.  This isn't really necessary, but we do this anyway for consistency.
        db.execSQL("UPDATE raw_contacts SET last_time_contacted = null" +
                " where last_time_contacted = 0");
    }

    /**
     * Pre-HC devices don't have correct "NORMALIZED_NUMBERS".  Clear them up.
     */
    private void upgradeToVersion702(SQLiteDatabase db) {
        // All the "correct" Phone.NORMALIZED_NUMBERS should begin with "+".  The upgraded data
        // don't.  Find all Phone.NORMALIZED_NUMBERS that don't begin with "+".
        final int count;
        final long[] dataIds;
        final long[] rawContactIds;
        final String[] phoneNumbers;
        final StringBuilder sbDataIds;
        final Cursor c = db.rawQuery(
                "SELECT _id, raw_contact_id, data1 FROM data " +
                " WHERE mimetype_id=" +
                    "(SELECT _id FROM mimetypes" +
                    " WHERE mimetype='vnd.android.cursor.item/phone_v2')" +
                " AND data4 not like '+%'", // "Not like" will exclude nulls too.
                null);
        try {
            count = c.getCount();
            if (count == 0) {
                return;
            }
            dataIds = new long[count];
            rawContactIds = new long[count];
            phoneNumbers = new String[count];
            sbDataIds = new StringBuilder();

            c.moveToPosition(-1);
            while (c.moveToNext()) {
                final int i = c.getPosition();
                dataIds[i] = c.getLong(0);
                rawContactIds[i] = c.getLong(1);
                phoneNumbers[i] = c.getString(2);

                if (sbDataIds.length() > 0) {
                    sbDataIds.append(",");
                }
                sbDataIds.append(dataIds[i]);
            }
        } finally {
            c.close();
        }

        final String dataIdList = sbDataIds.toString();

        // Then, update the Data and PhoneLookup tables.

        // First, just null out all Phone.NORMALIZED_NUMBERS for those.
        db.execSQL("UPDATE data SET data4 = null" +
                " WHERE _id IN (" + dataIdList + ")");

        // Then, re-create phone_lookup for them.
        db.execSQL("DELETE FROM phone_lookup" +
                " WHERE data_id IN (" + dataIdList + ")");

        for (int i = 0; i < count; i++) {
            // Mimic how DataRowHandlerForPhoneNumber.insert() works when it can't normalize
            // numbers.
            final String phoneNumber = phoneNumbers[i];
            if (TextUtils.isEmpty(phoneNumber)) continue;

            final String normalized = PhoneNumberUtils.normalizeNumber(phoneNumber);
            if (!TextUtils.isEmpty(normalized)) {
                db.execSQL("INSERT INTO phone_lookup" +
                        "(data_id, raw_contact_id, normalized_number, min_match)" +
                        " VALUES(?,?,?,?)",
                        new String[] {
                            String.valueOf(dataIds[i]),
                            String.valueOf(rawContactIds[i]),
                            normalized,
                            PhoneNumberUtils.toCallerIDMinMatch(normalized)});
            }
        }
    }

    private void upgradeToVersion707(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE raw_contacts ADD phonebook_label TEXT;");
        db.execSQL("ALTER TABLE raw_contacts ADD phonebook_bucket INTEGER;");
        db.execSQL("ALTER TABLE raw_contacts ADD phonebook_label_alt TEXT;");
        db.execSQL("ALTER TABLE raw_contacts ADD phonebook_bucket_alt INTEGER;");
    }

    private void upgradeToVersion710(SQLiteDatabase db) {

        // Adding timestamp to contacts table.
        db.execSQL("ALTER TABLE contacts"
                + " ADD contact_last_updated_timestamp INTEGER;");

        db.execSQL("UPDATE contacts"
                + " SET contact_last_updated_timestamp"
                + " = " + System.currentTimeMillis());

        db.execSQL("CREATE INDEX contacts_contact_last_updated_timestamp_index "
                + "ON contacts(contact_last_updated_timestamp)");

        // New deleted contacts table.
        db.execSQL("CREATE TABLE deleted_contacts (" +
                "contact_id INTEGER PRIMARY KEY," +
                "contact_deleted_timestamp INTEGER NOT NULL default 0"
                + ");");

        db.execSQL("CREATE INDEX deleted_contacts_contact_deleted_timestamp_index "
                + "ON deleted_contacts(contact_deleted_timestamp)");
    }

    private void upgradeToVersion800(SQLiteDatabase db) {
        // Default Calls.PRESENTATION_ALLOWED=1
        db.execSQL("ALTER TABLE calls ADD presentation INTEGER NOT NULL DEFAULT 1;");

        // Re-map CallerInfo.{..}_NUMBER strings to Calls.PRESENTATION_{..} ints
        //  PRIVATE_NUMBER="-2" -> PRESENTATION_RESTRICTED=2
        //  UNKNOWN_NUMBER="-1" -> PRESENTATION_UNKNOWN   =3
        // PAYPHONE_NUMBER="-3" -> PRESENTATION_PAYPHONE  =4
        db.execSQL("UPDATE calls SET presentation=2, number='' WHERE number='-2';");
        db.execSQL("UPDATE calls SET presentation=3, number='' WHERE number='-1';");
        db.execSQL("UPDATE calls SET presentation=4, number='' WHERE number='-3';");
    }

    private void upgradeToVersion802(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE contacts ADD pinned INTEGER NOT NULL DEFAULT " +
                ContactsContract.PinnedPositions.UNPINNED + ";");
        db.execSQL("ALTER TABLE raw_contacts ADD pinned INTEGER NOT NULL DEFAULT  " +
                ContactsContract.PinnedPositions.UNPINNED + ";");
    }

    private void upgradeToVersion902(SQLiteDatabase db) {
        // adding account identifier to call log table
        db.execSQL("ALTER TABLE calls ADD subscription_component_name TEXT;");
        db.execSQL("ALTER TABLE calls ADD subscription_id TEXT;");
    }

    /**
     * Searches for any calls in the call log with no normalized phone number and attempts to add
     * one if the number can be normalized.
     *
     * @param db The database.
     */
    private void upgradeToVersion903(SQLiteDatabase db) {
        // Find the calls in the call log with no normalized phone number.
        final Cursor c = db.rawQuery(
                "SELECT _id, number, countryiso FROM calls " +
                        " WHERE (normalized_number is null OR normalized_number = '') " +
                        " AND countryiso != '' AND countryiso is not null " +
                        " AND number != '' AND number is not null;",
                null
        );

        try {
            if (c.getCount() == 0) {
                return;
            }

            db.beginTransaction();
            try {
                c.moveToPosition(-1);
                while (c.moveToNext()) {
                    final long callId = c.getLong(0);
                    final String unNormalizedNumber = c.getString(1);
                    final String countryIso = c.getString(2);

                    // Attempt to get normalized number.
                    String normalizedNumber = PhoneNumberUtils
                            .formatNumberToE164(unNormalizedNumber, countryIso);

                    if (!TextUtils.isEmpty(normalizedNumber)) {
                        db.execSQL("UPDATE calls set normalized_number = ? " +
                                        "where _id = ?;",
                                new String[]{
                                        normalizedNumber,
                                        String.valueOf(callId),
                                }
                        );
                    }
                }

                db.setTransactionSuccessful();
            } finally {
                db.endTransaction();
            }
        } finally {
            c.close();
        }
    }

    /**
     * Updates the calls table in the database to include the call_duration and features columns.
     * @param db The database to update.
     */
    private void upgradeToVersion904(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD features INTEGER NOT NULL DEFAULT 0;");
        db.execSQL("ALTER TABLE calls ADD data_usage INTEGER;");
    }

    /**
     * Adds the voicemail transcription to the Table.Calls
     */
    private void upgradeToVersion905(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD transcription TEXT;");
    }

    /**
     * Upgrades the database with the new value for {@link PinnedPositions#UNPINNED}. In this
     * database version upgrade, the value is changed from 2147483647 (Integer.MAX_VALUE) to 0.
     *
     * The first pinned contact now starts from position 1.
     */
    @VisibleForTesting
    public void upgradeToVersion906(SQLiteDatabase db) {
        db.execSQL("UPDATE contacts SET pinned = pinned + 1"
                + " WHERE pinned >= 0 AND pinned < 2147483647;");
        db.execSQL("UPDATE raw_contacts SET pinned = pinned + 1"
                + " WHERE pinned >= 0 AND pinned < 2147483647;");

        db.execSQL("UPDATE contacts SET pinned = 0"
                + " WHERE pinned = 2147483647;");
        db.execSQL("UPDATE raw_contacts SET pinned = 0"
                + " WHERE pinned = 2147483647;");
    }

    private void upgradeToVersion908(SQLiteDatabase db) {
        db.execSQL("UPDATE contacts SET pinned = 0 WHERE pinned = 2147483647;");
        db.execSQL("UPDATE raw_contacts SET pinned = 0 WHERE pinned = 2147483647;");
    }

    private void upgradeToVersion909(SQLiteDatabase db) {
        try {
            db.execSQL("ALTER TABLE calls ADD sub_id INTEGER DEFAULT -1;");
        } catch (SQLiteException e) {
            // The column already exists--copy over data
            db.execSQL("UPDATE calls SET subscription_component_name='com.android.phone/"
                    + "com.android.services.telephony.TelephonyConnectionService';");
            db.execSQL("UPDATE calls SET subscription_id=sub_id;");
        }
    }

    /**
     * Delete any remaining rows in the calls table if the user is a profile of another user.
     * b/17096027
     */
    private void upgradeToVersion910(SQLiteDatabase db) {
        final UserManager userManager = (UserManager) mContext.getSystemService(
                Context.USER_SERVICE);
        final UserInfo user = userManager.getUserInfo(userManager.getUserHandle());
        if (user.isManagedProfile()) {
            db.execSQL("DELETE FROM calls;");
        }
    }

    /**
     * Add backup_id column to raw_contacts table and hash_id column to data table.
     */
    private void upgradeToVersion1000(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE raw_contacts ADD backup_id TEXT;");
        db.execSQL("ALTER TABLE data ADD hash_id TEXT;");
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS raw_contacts_backup_id_account_id_index ON " +
                "raw_contacts (backup_id, account_id);");
        db.execSQL("CREATE INDEX IF NOT EXISTS data_hash_id_index ON data (hash_id);");
    }

    @VisibleForTesting
    public void upgradeToVersion1002(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS pre_authorized_uris;");
        db.execSQL("CREATE TABLE pre_authorized_uris ("+
                "_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "uri STRING NOT NULL, " +
                "expiration INTEGER NOT NULL DEFAULT 0);");
    }

    public void upgradeToVersion1003(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD phone_account_address TEXT;");

        // After version 1003, we are using the ICC ID as the phone-account ID. This code updates
        // any existing telephony connection-service calllog entries to the ICC ID from the
        // previously used subscription ID.
        // TODO: This is inconsistent, depending on the initialization state of SubscriptionManager.
        //       Sometimes it returns zero subscriptions. May want to move this upgrade to run after
        //       ON_BOOT_COMPLETE instead of PRE_BOOT_COMPLETE.
        SubscriptionManager sm = SubscriptionManager.from(mContext);
        if (sm != null) {
            for (SubscriptionInfo info : sm.getActiveSubscriptionInfoList()) {
                String iccId = info.getIccId();
                int subId = info.getSubscriptionId();
                if (!TextUtils.isEmpty(iccId) &&
                        subId != SubscriptionManager.INVALID_SUBSCRIPTION_ID) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("UPDATE calls SET subscription_id=");
                    DatabaseUtils.appendEscapedSQLString(sb, iccId);
                    sb.append(" WHERE subscription_id=");
                    sb.append(subId);
                    sb.append(" AND subscription_component_name='com.android.phone/"
                            + "com.android.services.telephony.TelephonyConnectionService';");

                    db.execSQL(sb.toString());
                }
            }
        }
    }

    /**
     * Add a "hidden" column for call log entries we want to hide after an upgrade until the user
     * adds the right phone account to the device.
     */
    public void upgradeToVersion1004(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD phone_account_hidden INTEGER NOT NULL DEFAULT 0;");
    }

    public void upgradeToVersion1005(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD photo_uri TEXT;");
    }

    /**
     * The try/catch pattern exists because some devices have the upgrade and some do not. This is
     * because the below updates were merged into version 1005 after some devices had already
     * upgraded to version 1005 and hence did not receive the below upgrades.
     */
    public void upgradeToVersion1007(SQLiteDatabase db) {
        try {
            // Add multi-sim fields
            db.execSQL("ALTER TABLE voicemail_status ADD phone_account_component_name TEXT;");
            db.execSQL("ALTER TABLE voicemail_status ADD phone_account_id TEXT;");

            // For use by the sync adapter
            db.execSQL("ALTER TABLE calls ADD dirty INTEGER NOT NULL DEFAULT 0;");
            db.execSQL("ALTER TABLE calls ADD deleted INTEGER NOT NULL DEFAULT 0;");
        } catch (SQLiteException e) {
            // These columns already exist. Do nothing.
            // Log verbose because this should be the majority case.
            Log.v(TAG, "Version 1007: Columns already exist, skipping upgrade steps.");
        }
  }


    public void upgradeToVersion1009(SQLiteDatabase db) {
        try {
            db.execSQL("ALTER TABLE data ADD carrier_presence INTEGER NOT NULL DEFAULT 0");
        } catch (SQLiteException ignore) {
        }
    }

    private void upgradeToVersion1100(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE raw_contacts ADD metadata_dirty INTEGER NOT NULL DEFAULT 0;");
    }

    // Data.hash_id column is used for metadata backup, and this upgrade is to generate
    // hash_id column. Usually data1 and data2 are two main columns to store data info.
    // But for photo, we don't use data1 and data2, instead, use data15 to store photo blob.
    // So this upgrade generates hash_id from (data1 + data2) or (data15) using sha-1.
    public void upgradeToVersion1101(SQLiteDatabase db) {
        final SQLiteStatement update = db.compileStatement(
                "UPDATE " + Tables.DATA +
                " SET " + Data.HASH_ID + "=?" +
                " WHERE " + Data._ID + "=?"
        );
        final Cursor c = db.query(Tables.DATA,
                new String[] {Data._ID, Data.DATA1, Data.DATA2, Data.DATA15},
                null, null, null, null, Data._ID);
        try {
            while (c.moveToNext()) {
                final long dataId = c.getLong(0);
                final String data1 = c.getString(1);
                final String data2 = c.getString(2);
                final byte[] data15 = c.getBlob(3);
                final String hashId = legacyGenerateHashId(data1, data2, data15);
                if (!TextUtils.isEmpty(hashId)) {
                    update.bindString(1, hashId);
                    update.bindLong(2, dataId);
                    update.execute();
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Add new metadata_sync table to cache the meta data on raw contacts level from server before
     * they are merged into other CP2 tables. The data column is the blob column containing all
     * the backed up metadata for this raw_contact. This table should only be used by metadata
     * sync adapter.
     */
    public void upgradeToVersion1104(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS metadata_sync;");
        db.execSQL("CREATE TABLE metadata_sync (" +
                "_id INTEGER PRIMARY KEY AUTOINCREMENT, raw_contact_backup_id TEXT NOT NULL, " +
                "account_id INTEGER NOT NULL, data TEXT, deleted INTEGER NOT NULL DEFAULT 0);");
        db.execSQL("CREATE UNIQUE INDEX metadata_sync_index ON metadata_sync (" +
                "raw_contact_backup_id, account_id);");
    }

    /**
     * Add new metadata_sync_state table to store the metadata sync state for a set of accounts.
     */
    public void upgradeToVersion1105(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS metadata_sync_state;");
        db.execSQL("CREATE TABLE metadata_sync_state (" +
                "_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "account_id INTEGER NOT NULL, state BLOB);");
        db.execSQL("CREATE UNIQUE INDEX metadata_sync_state_index ON metadata_sync_state (" +
                "account_id);");
    }

    public void upgradeToVersion1106(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE calls ADD post_dial_digits TEXT NOT NULL DEFAULT ''");
    }

    public void upgradeToVersion1107(SQLiteDatabase db) {
        try {
            db.execSQL("ALTER TABLE calls ADD post_dial_digits TEXT NOT NULL DEFAULT ''");
        } catch (SQLiteException ignore) {
            // This is for devices which got initialized without a post_dial_digits
            // column from version 1106. The exception indicates that the column is
            // already present, so nothing needs to be done.
        }
    }

    public void upgradeToVersion1108(SQLiteDatabase db) {
        db.execSQL(
                "ALTER TABLE calls ADD add_for_all_users INTEGER NOT NULL DEFAULT 1");
    }

    public void upgradeToVersion1109(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE voicemail_status ADD quota_occupied INTEGER DEFAULT -1;");
        db.execSQL("ALTER TABLE voicemail_status ADD quota_total INTEGER DEFAULT -1;");
        db.execSQL("ALTER TABLE calls ADD last_modified INTEGER DEFAULT 0;");
    }

    /**
     * Update hash_id for photo data. Generates the same value for all photo mimetype data, since
     * there's only one photo for each raw_contact.
     */
    public void upgradeToVersion1110(SQLiteDatabase db) {
        final long mimeTypeId = lookupMimeTypeId(db,
                ContactsContract.CommonDataKinds.Photo.CONTENT_ITEM_TYPE);
        final ContentValues values = new ContentValues();
        values.put(Data.HASH_ID, getPhotoHashId());
        db.update(Tables.DATA, values, DataColumns.MIMETYPE_ID + " = " + mimeTypeId, null);
    }

    public String getPhotoHashId() {
        return generateHashId(ContactsContract.CommonDataKinds.Photo.CONTENT_ITEM_TYPE, null);
    }

    @VisibleForTesting
    public void upgradeToVersion1111(SQLiteDatabase db) {
        // Re-order contacts with no display name to the phone number bucket and give
        // them the phone number label. See b/21736630.
        final ContactLocaleUtils localeUtils = ContactLocaleUtils.getInstance();
        final int index = localeUtils.getNumberBucketIndex();
        final String label = localeUtils.getBucketLabel(index);
        // Note, sort_key = null is equivalent to display_name = null
        db.execSQL("UPDATE raw_contacts SET phonebook_bucket = " + index +
                ", phonebook_label='" + label + "' WHERE sort_key IS NULL AND phonebook_bucket=0;");
        db.execSQL("UPDATE raw_contacts SET phonebook_bucket_alt = " + index +
                ", phonebook_label_alt='" + label +
                "' WHERE sort_key_alt IS NULL AND phonebook_bucket_alt=0;");

        FastScrollingIndexCache.getInstance(mContext).invalidate();
    }

    private void upgradeToVersion1201(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE contacts ADD x_times_contacted INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE contacts ADD x_last_time_contacted INTEGER");

        db.execSQL("ALTER TABLE raw_contacts ADD x_times_contacted INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE raw_contacts ADD x_last_time_contacted INTEGER");

        db.execSQL("ALTER TABLE data_usage_stat ADD x_times_used INTEGER NOT NULL DEFAULT 0");
        db.execSQL("ALTER TABLE data_usage_stat ADD x_last_time_used INTEGER NOT NULL DEFAULT 0");

        db.execSQL("UPDATE contacts SET "
                + "x_times_contacted = ifnull(times_contacted,0),"
                + "x_last_time_contacted = ifnull(last_time_contacted,0),"
                + "times_contacted = 0,"
                + "last_time_contacted = 0");

        db.execSQL("UPDATE raw_contacts SET "
                + "x_times_contacted = ifnull(times_contacted,0),"
                + "x_last_time_contacted = ifnull(last_time_contacted,0),"
                + "times_contacted = 0,"
                + "last_time_contacted = 0");

        db.execSQL("UPDATE data_usage_stat SET "
                + "x_times_used = ifnull(times_used,0),"
                + "x_last_time_used = ifnull(last_time_used,0),"
                + "times_used = 0,"
                + "last_time_used = 0");
    }

    public void upgradeToVersion1300(SQLiteDatabase db) {
        try {
            db.execSQL("ALTER TABLE data ADD preferred_phone_account_component_name "
                    + "TEXT;");
            db.execSQL("ALTER TABLE data ADD preferred_phone_account_id TEXT;");
        } catch (SQLiteException ignore) {
        }
    }

    /**
     * This method is only used in upgradeToVersion1101 method, and should not be used in other
     * places now. Because data15 is not used to generate hash_id for photo, and the new generating
     * method for photos should be getPhotoHashId().
     *
     * Generate hash_id from data1, data2 and data15 columns.
     * If one of data1 and data2 is not null, using data1 and data2 to get hash_id,
     * otherwise, using data15 to generate.
     */
    public String legacyGenerateHashId(String data1, String data2, byte[] data15) {
        final StringBuilder sb = new StringBuilder();
        byte[] hashInput = null;
        if (!TextUtils.isEmpty(data1) || !TextUtils.isEmpty(data2)) {
            sb.append(data1);
            sb.append(data2);
            hashInput = sb.toString().getBytes();
        } else if (data15 != null) {
            hashInput = data15;
        }
        if (hashInput != null) {
            final String hashId = generateHashIdForData(hashInput);
            return hashId;
        } else {
            return null;
        }
    }

    public String generateHashId(String data1, String data2) {
        final StringBuilder sb = new StringBuilder();
        byte[] hashInput = null;
        if (!TextUtils.isEmpty(data1) || !TextUtils.isEmpty(data2)) {
            sb.append(data1);
            sb.append(data2);
            hashInput = sb.toString().getBytes();
        }
        if (hashInput != null) {
            final String hashId = generateHashIdForData(hashInput);
            return hashId;
        } else {
            return null;
        }
    }

    // Use SHA-1 hash method to generate hash string for the input.
    @VisibleForTesting
    String generateHashIdForData(byte[] input) {
        synchronized (mMessageDigest) {
            final byte[] hashResult = mMessageDigest.digest(input);
            return Base64.encodeToString(hashResult, Base64.DEFAULT);
        }
    }

    public String extractHandleFromEmailAddress(String email) {
        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return null;
        }

        String address = tokens[0].getAddress();
        int index = address.indexOf('@');
        if (index != -1) {
            return address.substring(0, index);
        }
        return null;
    }

    public String extractAddressFromEmailAddress(String email) {
        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return null;
        }
        return tokens[0].getAddress().trim();
    }

    /**
     * Inserts a new mimetype into the table Tables.MIMETYPES and returns its id. Use
     * {@link #lookupMimeTypeId(SQLiteDatabase, String)} to lookup id of a mimetype that is
     * guaranteed to be in the database
     *
     * @param db the SQLiteDatabase object returned by {@link #getWritableDatabase()}
     * @param mimeType The mimetype to insert
     * @return the id of the newly inserted row
     */
    private long insertMimeType(SQLiteDatabase db, String mimeType) {
        final String insert = "INSERT INTO " + Tables.MIMETYPES + "("
                + MimetypesColumns.MIMETYPE +
                ") VALUES (?)";
        long id = insertWithOneArgAndReturnId(db, insert, mimeType);
        if (id >= 0) {
            return id;
        }
        return lookupMimeTypeId(db, mimeType);
    }

    /**
     * Looks up Tables.MIMETYPES for the mime type and returns its id. Returns -1 if the mime type
     * is not found. Use {@link #insertMimeType(SQLiteDatabase, String)} when it is doubtful whether
     * the mimetype already exists in the table or not.
     *
     * @param db
     * @param mimeType
     * @return the id of the row containing the mime type or -1 if the mime type was not found.
     */
    private long lookupMimeTypeId(SQLiteDatabase db, String mimeType) {
        Long id = mCommonMimeTypeIdsCache.get(mimeType);
        if (id != null) {
            return id;
        }
        final String query = "SELECT " +
                MimetypesColumns._ID + " FROM " + Tables.MIMETYPES + " WHERE "
                + MimetypesColumns.MIMETYPE +
                "=?";
        id = queryIdWithOneArg(db, query, mimeType);
        if (id < 0) {
            Log.e(TAG, "Mimetype " + mimeType + " not found in the MIMETYPES table");
        }
        return id;
    }

    private static void bindString(SQLiteStatement stmt, int index, String value) {
        if (value == null) {
            stmt.bindNull(index);
        } else {
            stmt.bindString(index, value);
        }
    }

    private void bindLong(SQLiteStatement stmt, int index, Number value) {
        if (value == null) {
            stmt.bindNull(index);
        } else {
            stmt.bindLong(index, value.longValue());
        }
    }

    /**
     * Adds index stats into the SQLite database to force it to always use the lookup indexes.
     *
     * Note if you drop a table or an index, the corresponding row will be removed from this table.
     * Make sure to call this method after such operations.
     */
    private void updateSqliteStats(SQLiteDatabase db) {
        if (!mDatabaseOptimizationEnabled) {
            return;  // We don't use sqlite_stat1 during tests.
        }

        // Specific stats strings are based on an actual large database after running ANALYZE
        // Important here are relative sizes. Raw-Contacts is slightly bigger than Contacts
        // Warning: Missing tables in here will make SQLite assume to contain 1000000 rows,
        // which can lead to catastrophic query plans for small tables

        // What these numbers mean is described in this file.
        // http://www.sqlite.org/cgi/src/finfo?name=src/analyze.c

        // Excerpt:
        /*
        ** Format of sqlite_stat1:
        **
        ** There is normally one row per index, with the index identified by the
        ** name in the idx column.  The tbl column is the name of the table to
        ** which the index belongs.  In each such row, the stat column will be
        ** a string consisting of a list of integers.  The first integer in this
        ** list is the number of rows in the index and in the table.  The second
        ** integer is the average number of rows in the index that have the same
        ** value in the first column of the index.  The third integer is the average
        ** number of rows in the index that have the same value for the first two
        ** columns.  The N-th integer (for N>1) is the average number of rows in
        ** the index which have the same value for the first N-1 columns.  For
        ** a K-column index, there will be K+1 integers in the stat column.  If
        ** the index is unique, then the last integer will be 1.
        **
        ** The list of integers in the stat column can optionally be followed
        ** by the keyword "unordered".  The "unordered" keyword, if it is present,
        ** must be separated from the last integer by a single space.  If the
        ** "unordered" keyword is present, then the query planner assumes that
        ** the index is unordered and will not use the index for a range query.
        **
        ** If the sqlite_stat1.idx column is NULL, then the sqlite_stat1.stat
        ** column contains a single integer which is the (estimated) number of
        ** rows in the table identified by sqlite_stat1.tbl.
        */

        try {
            db.execSQL("DELETE FROM sqlite_stat1");
            updateIndexStats(db, Tables.CONTACTS,
                    "contacts_has_phone_index", "9000 500");
            updateIndexStats(db, Tables.CONTACTS,
                    "contacts_name_raw_contact_id_index", "9000 1");
            updateIndexStats(db, Tables.CONTACTS, MoreDatabaseUtils.buildIndexName(Tables.CONTACTS,
                    Contacts.CONTACT_LAST_UPDATED_TIMESTAMP), "9000 10");

            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contacts_contact_id_index", "10000 2");
            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contact_sort_key2_index", "10000 2");
            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contact_sort_key1_index", "10000 2");
            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contacts_source_id_account_id_index", "10000 1 1");

            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "name_lookup_raw_contact_id_index", "35000 4");
            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "name_lookup_index", "35000 2 2 2 1");
            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "sqlite_autoindex_name_lookup_1", "35000 3 2 1");

            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_index", "3500 3 2 1");
            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_min_match_index", "3500 3 2 2");
            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_data_id_min_match_index", "3500 2 2");

            updateIndexStats(db, Tables.DATA,
                    "data_mimetype_data1_index", "60000 5000 2");
            updateIndexStats(db, Tables.DATA,
                    "data_raw_contact_id", "60000 10");

            updateIndexStats(db, Tables.GROUPS,
                    "groups_source_id_account_id_index", "50 2 2 1 1");

            updateIndexStats(db, Tables.NICKNAME_LOOKUP,
                    "nickname_lookup_index", "500 2 1");

            updateIndexStats(db, Tables.STATUS_UPDATES,
                    null, "100");

            updateIndexStats(db, Tables.STREAM_ITEMS,
                    null, "500");
            updateIndexStats(db, Tables.STREAM_ITEM_PHOTOS,
                    null, "50");

            updateIndexStats(db, Tables.ACCOUNTS,
                    null, "3");

            updateIndexStats(db, Tables.PRE_AUTHORIZED_URIS,
                    null, "1");

            updateIndexStats(db, Tables.VISIBLE_CONTACTS,
                    null, "2000");

            updateIndexStats(db, Tables.PHOTO_FILES,
                    null, "50");

            updateIndexStats(db, Tables.DEFAULT_DIRECTORY,
                    null, "1500");

            updateIndexStats(db, Tables.MIMETYPES,
                    "mime_type", "18 1");

            updateIndexStats(db, Tables.DATA_USAGE_STAT,
                    "data_usage_stat_index", "20 2 1");

            updateIndexStats(db, Tables.METADATA_SYNC,
                    "metadata_sync_index", "10000 1 1");

            // Tiny tables
            updateIndexStats(db, Tables.AGGREGATION_EXCEPTIONS,
                    null, "10");
            updateIndexStats(db, Tables.SETTINGS,
                    null, "10");
            updateIndexStats(db, Tables.PACKAGES,
                    null, "0");
            updateIndexStats(db, Tables.DIRECTORIES,
                    null, "3");
            updateIndexStats(db, LegacyApiSupport.LegacyTables.SETTINGS,
                    null, "0");
            updateIndexStats(db, "android_metadata",
                    null, "1");
            updateIndexStats(db, "_sync_state",
                    "sqlite_autoindex__sync_state_1", "2 1 1");
            updateIndexStats(db, "_sync_state_metadata",
                    null, "1");
            updateIndexStats(db, "properties",
                    "sqlite_autoindex_properties_1", "4 1");

            updateIndexStats(db, Tables.METADATA_SYNC_STATE,
                    "metadata_sync_state_index", "2 1 1");

            // Search index
            updateIndexStats(db, "search_index_docsize",
                    null, "9000");
            updateIndexStats(db, "search_index_content",
                    null, "9000");
            updateIndexStats(db, "search_index_stat",
                    null, "1");
            updateIndexStats(db, "search_index_segments",
                    null, "450");
            updateIndexStats(db, "search_index_segdir",
                    "sqlite_autoindex_search_index_segdir_1", "9 5 1");

            updateIndexStats(db, Tables.PRESENCE, "presenceIndex", "1 1");
            updateIndexStats(db, Tables.PRESENCE, "presenceIndex2", "1 1");
            updateIndexStats(db, Tables.AGGREGATED_PRESENCE, null, "1");

            // Force SQLite to reload sqlite_stat1.
            db.execSQL("ANALYZE sqlite_master;");
        } catch (SQLException e) {
            Log.e(TAG, "Could not update index stats", e);
        }
    }

    /**
     * Stores statistics for a given index.
     *
     * @param stats has the following structure: the first index is the expected size of
     * the table.  The following integer(s) are the expected number of records selected with the
     * index.  There should be one integer per indexed column.
     */
    private void updateIndexStats(SQLiteDatabase db, String table, String index, String stats) {
        if (index == null) {
            db.execSQL("DELETE FROM sqlite_stat1 WHERE tbl=? AND idx IS NULL",
                    new String[] {table});
        } else {
            db.execSQL("DELETE FROM sqlite_stat1 WHERE tbl=? AND idx=?",
                    new String[] {table, index});
        }
        db.execSQL("INSERT INTO sqlite_stat1 (tbl,idx,stat) VALUES (?,?,?)",
                new String[] {table, index, stats});
    }

    /**
     * Wipes all data except mime type and package lookup tables.
     */
    public void wipeData() {
        SQLiteDatabase db = getWritableDatabase();

        db.execSQL("DELETE FROM " + Tables.ACCOUNTS + ";");
        db.execSQL("DELETE FROM " + Tables.CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.RAW_CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.STREAM_ITEMS + ";");
        db.execSQL("DELETE FROM " + Tables.STREAM_ITEM_PHOTOS + ";");
        db.execSQL("DELETE FROM " + Tables.PHOTO_FILES + ";");
        db.execSQL("DELETE FROM " + Tables.DATA + ";");
        db.execSQL("DELETE FROM " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.GROUPS + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATION_EXCEPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.SETTINGS + ";");
        db.execSQL("DELETE FROM " + Tables.DIRECTORIES + ";");
        db.execSQL("DELETE FROM " + Tables.SEARCH_INDEX + ";");
        db.execSQL("DELETE FROM " + Tables.DELETED_CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.MIMETYPES + ";");
        db.execSQL("DELETE FROM " + Tables.PACKAGES + ";");
        db.execSQL("DELETE FROM " + Tables.PRESENCE + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATED_PRESENCE + ";");

        prepopulateCommonMimeTypes(db);
        // Note: we are not removing reference data from Tables.NICKNAME_LOOKUP
    }

    public NameSplitter createNameSplitter() {
        return createNameSplitter(Locale.getDefault());
    }

    public NameSplitter createNameSplitter(Locale locale) {
        mNameSplitter = new NameSplitter(
                mContext.getString(com.android.internal.R.string.common_name_prefixes),
                mContext.getString(com.android.internal.R.string.common_last_name_prefixes),
                mContext.getString(com.android.internal.R.string.common_name_suffixes),
                mContext.getString(com.android.internal.R.string.common_name_conjunctions),
                locale);
        return mNameSplitter;
    }

    /**
     * Return the {@link ApplicationInfo#uid} for the given package name.
     */
    public static int getUidForPackageName(PackageManager pm, String packageName) {
        try {
            ApplicationInfo clientInfo = pm.getApplicationInfo(packageName, 0 /* no flags */);
            return clientInfo.uid;
        } catch (NameNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static long queryIdWithOneArg(SQLiteDatabase db, String sql, String sqlArgument) {
        final SQLiteStatement query = db.compileStatement(sql);
        try {
            bindString(query, 1, sqlArgument);
            try {
                return query.simpleQueryForLong();
            } catch (SQLiteDoneException notFound) {
                return -1;
            }
        } finally {
            query.close();
        }
    }

    @VisibleForTesting
    static long insertWithOneArgAndReturnId(SQLiteDatabase db, String sql, String sqlArgument) {
        final SQLiteStatement insert = db.compileStatement(sql);
        try {
            bindString(insert, 1, sqlArgument);
            try {
                return insert.executeInsert();
            } catch (SQLiteConstraintException conflict) {
                return -1;
            }
        } finally {
            insert.close();
        }
    }

    /**
     * Convert a package name into an integer, using {@link Tables#PACKAGES} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getPackageId(String packageName) {
        final String query =
                "SELECT " + PackagesColumns._ID +
                " FROM " + Tables.PACKAGES +
                " WHERE " + PackagesColumns.PACKAGE + "=?";

        final String insert =
                "INSERT INTO " + Tables.PACKAGES + "("
                        + PackagesColumns.PACKAGE +
                ") VALUES (?)";

        SQLiteDatabase db = getWritableDatabase();
        long id = queryIdWithOneArg(db, query, packageName);
        if (id >= 0) {
            return id;
        }
        id = insertWithOneArgAndReturnId(db, insert, packageName);
        if (id >= 0) {
            return id;
        }
        // just in case there was a race while doing insert above
        return queryIdWithOneArg(db, query, packageName);
    }

    /**
     * Convert a mimetype into an integer, using {@link Tables#MIMETYPES} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getMimeTypeId(String mimeType) {
        SQLiteDatabase db = getWritableDatabase();
        long id = lookupMimeTypeId(db, mimeType);
        if (id < 0) {
            return insertMimeType(db, mimeType);
        }
        return id;
    }

    public long getMimeTypeIdForStructuredName() {
        return lookupMimeTypeId(getWritableDatabase(), StructuredName.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForStructuredPostal() {
        return lookupMimeTypeId(getWritableDatabase(), StructuredPostal.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForOrganization() {
        return lookupMimeTypeId(getWritableDatabase(), Organization.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForIm() {
        return lookupMimeTypeId(getWritableDatabase(), Im.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForEmail() {
        return lookupMimeTypeId(getWritableDatabase(), Email.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForPhone() {
        return lookupMimeTypeId(getWritableDatabase(), Phone.CONTENT_ITEM_TYPE);
    }

    public long getMimeTypeIdForSip() {
        return lookupMimeTypeId(getWritableDatabase(), SipAddress.CONTENT_ITEM_TYPE);
    }

    /**
     * Returns a {@link ContactsContract.DisplayNameSources} value based on {@param mimeTypeId}.
     * This does not return {@link ContactsContract.DisplayNameSources#STRUCTURED_PHONETIC_NAME}.
     * The calling client needs to inspect the structured name itself to distinguish between
     * {@link ContactsContract.DisplayNameSources#STRUCTURED_NAME} and
     * {@code STRUCTURED_PHONETIC_NAME}.
     */
    private int getDisplayNameSourceForMimeTypeId(int mimeTypeId) {
        if (mimeTypeId == mCommonMimeTypeIdsCache.get(StructuredName.CONTENT_ITEM_TYPE)) {
            return DisplayNameSources.STRUCTURED_NAME;
        }
        if (mimeTypeId == mCommonMimeTypeIdsCache.get(Email.CONTENT_ITEM_TYPE)) {
            return DisplayNameSources.EMAIL;
        }
        if (mimeTypeId == mCommonMimeTypeIdsCache.get(Phone.CONTENT_ITEM_TYPE)) {
            return DisplayNameSources.PHONE;
        }
        if (mimeTypeId == mCommonMimeTypeIdsCache.get(Organization.CONTENT_ITEM_TYPE)) {
            return DisplayNameSources.ORGANIZATION;
        }
        if (mimeTypeId == mCommonMimeTypeIdsCache.get(Nickname.CONTENT_ITEM_TYPE)) {
            return DisplayNameSources.NICKNAME;
        }
        return DisplayNameSources.UNDEFINED;
    }

    /**
     * Find the mimetype for the given {@link Data#_ID}.
     */
    public String getDataMimeType(long dataId) {
        final SQLiteStatement dataMimetypeQuery = getWritableDatabase().compileStatement(
                    "SELECT " + MimetypesColumns.MIMETYPE +
                    " FROM " + Tables.DATA_JOIN_MIMETYPES +
                    " WHERE " + Tables.DATA + "." + Data._ID + "=?");
        try {
            // Try database query to find mimetype
            dataMimetypeQuery.bindLong(1, dataId);
            return dataMimetypeQuery.simpleQueryForString();
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    /**
     * Gets all accounts in the accounts table.
     */
    public Set<AccountWithDataSet> getAllAccountsWithDataSets() {
        final ArraySet<AccountWithDataSet> result = new ArraySet<>();
        Cursor c = getReadableDatabase().rawQuery(
                "SELECT DISTINCT " +  AccountsColumns._ID + "," + AccountsColumns.ACCOUNT_NAME +
                "," + AccountsColumns.ACCOUNT_TYPE + "," + AccountsColumns.DATA_SET +
                " FROM " + Tables.ACCOUNTS, null);
        try {
            while (c.moveToNext()) {
                result.add(AccountWithDataSet.get(c.getString(1), c.getString(2), c.getString(3)));
            }
        } finally {
            c.close();
        }
        return result;
    }

    /**
     * @return ID of the specified account, or null if the account doesn't exist.
     */
    public Long getAccountIdOrNull(AccountWithDataSet accountWithDataSet) {
        if (accountWithDataSet == null) {
            accountWithDataSet = AccountWithDataSet.LOCAL;
        }
        final SQLiteStatement select = getWritableDatabase().compileStatement(
                "SELECT " + AccountsColumns._ID +
                        " FROM " + Tables.ACCOUNTS +
                        " WHERE " +
                        "((?1 IS NULL AND " + AccountsColumns.ACCOUNT_NAME + " IS NULL) OR " +
                        "(" + AccountsColumns.ACCOUNT_NAME + "=?1)) AND " +
                        "((?2 IS NULL AND " + AccountsColumns.ACCOUNT_TYPE + " IS NULL) OR " +
                        "(" + AccountsColumns.ACCOUNT_TYPE + "=?2)) AND " +
                        "((?3 IS NULL AND " + AccountsColumns.DATA_SET + " IS NULL) OR " +
                        "(" + AccountsColumns.DATA_SET + "=?3))");
        try {
            DatabaseUtils.bindObjectToProgram(select, 1, accountWithDataSet.getAccountName());
            DatabaseUtils.bindObjectToProgram(select, 2, accountWithDataSet.getAccountType());
            DatabaseUtils.bindObjectToProgram(select, 3, accountWithDataSet.getDataSet());
            try {
                return select.simpleQueryForLong();
            } catch (SQLiteDoneException notFound) {
                return null;
            }
        } finally {
            select.close();
        }
    }

    /**
     * @return ID of the specified account.  This method will create a record in the accounts table
     *     if the account doesn't exist in the accounts table.
     *
     * This must be used in a transaction, so there's no need for synchronization.
     */
    public long getOrCreateAccountIdInTransaction(AccountWithDataSet accountWithDataSet) {
        if (accountWithDataSet == null) {
            accountWithDataSet = AccountWithDataSet.LOCAL;
        }
        Long id = getAccountIdOrNull(accountWithDataSet);
        if (id != null) {
            return id;
        }
        final SQLiteStatement insert = getWritableDatabase().compileStatement(
                "INSERT INTO " + Tables.ACCOUNTS +
                " (" + AccountsColumns.ACCOUNT_NAME + ", " +
                AccountsColumns.ACCOUNT_TYPE + ", " +
                AccountsColumns.DATA_SET + ") VALUES (?, ?, ?)");
        try {
            DatabaseUtils.bindObjectToProgram(insert, 1, accountWithDataSet.getAccountName());
            DatabaseUtils.bindObjectToProgram(insert, 2, accountWithDataSet.getAccountType());
            DatabaseUtils.bindObjectToProgram(insert, 3, accountWithDataSet.getDataSet());
            id = insert.executeInsert();
        } finally {
            insert.close();
        }

        return id;
    }

    /**
     * Update {@link Contacts#IN_VISIBLE_GROUP} for all contacts.
     */
    public void updateAllVisible() {
        updateCustomContactVisibility(getWritableDatabase(), -1);
    }

    /**
     * Updates contact visibility and return true iff the visibility was actually changed.
     */
    public boolean updateContactVisibleOnlyIfChanged(TransactionContext txContext, long contactId) {
        return updateContactVisible(txContext, contactId, true);
    }

    /**
     * Update {@link Contacts#IN_VISIBLE_GROUP} and
     * {@link Tables#DEFAULT_DIRECTORY} for a specific contact.
     */
    public void updateContactVisible(TransactionContext txContext, long contactId) {
        updateContactVisible(txContext, contactId, false);
    }

    public boolean updateContactVisible(
            TransactionContext txContext, long contactId, boolean onlyIfChanged) {
        SQLiteDatabase db = getWritableDatabase();
        updateCustomContactVisibility(db, contactId);

        String contactIdAsString = String.valueOf(contactId);
        long mimetype = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);

        // The contact will be included in the default directory if contains a raw contact that is
        // in any group or in an account that does not have any AUTO_ADD groups.
        boolean newVisibility = DatabaseUtils.longForQuery(db,
                "SELECT EXISTS (" +
                    "SELECT " + RawContacts.CONTACT_ID +
                    " FROM " + Tables.RAW_CONTACTS +
                    " JOIN " + Tables.DATA +
                    "   ON (" + RawContactsColumns.CONCRETE_ID + "="
                            + Data.RAW_CONTACT_ID + ")" +
                    " WHERE " + RawContacts.CONTACT_ID + "=?1" +
                    "   AND " + DataColumns.MIMETYPE_ID + "=?2" +
                ") OR EXISTS (" +
                    "SELECT " + RawContacts._ID +
                    " FROM " + Tables.RAW_CONTACTS +
                    " WHERE " + RawContacts.CONTACT_ID + "=?1" +
                    "   AND NOT EXISTS" +
                        " (SELECT " + Groups._ID +
                        "  FROM " + Tables.GROUPS +
                        "  WHERE " + RawContactsColumns.CONCRETE_ACCOUNT_ID + " = "
                                + GroupsColumns.CONCRETE_ACCOUNT_ID +
                        "  AND " + Groups.AUTO_ADD + " != 0" +
                        ")" +
                ") OR EXISTS (" +
                    "SELECT " + RawContacts._ID +
                    " FROM " + Tables.RAW_CONTACTS +
                    " WHERE " + RawContacts.CONTACT_ID + "=?1" +
                    "   AND " + RawContactsColumns.CONCRETE_ACCOUNT_ID + "=" +
                        Clauses.LOCAL_ACCOUNT_ID +
                ")",
                new String[] {
                    contactIdAsString,
                    String.valueOf(mimetype)
                }) != 0;

        if (onlyIfChanged) {
            boolean oldVisibility = isContactInDefaultDirectory(db, contactId);
            if (oldVisibility == newVisibility) {
                return false;
            }
        }

        if (newVisibility) {
            db.execSQL("INSERT OR IGNORE INTO " + Tables.DEFAULT_DIRECTORY + " VALUES(?)",
                    new String[] {contactIdAsString});
            txContext.invalidateSearchIndexForContact(contactId);
        } else {
            db.execSQL("DELETE FROM " + Tables.DEFAULT_DIRECTORY +
                        " WHERE " + Contacts._ID + "=?",
                    new String[] {contactIdAsString});
            db.execSQL("DELETE FROM " + Tables.SEARCH_INDEX +
                        " WHERE " + SearchIndexColumns.CONTACT_ID + "=CAST(? AS int)",
                    new String[] {contactIdAsString});
        }
        return true;
    }

    public boolean isContactInDefaultDirectory(SQLiteDatabase db, long contactId) {
        final SQLiteStatement contactInDefaultDirectoryQuery = db.compileStatement(
                    "SELECT EXISTS (" +
                            "SELECT 1 FROM " + Tables.DEFAULT_DIRECTORY +
                            " WHERE " + Contacts._ID + "=?)");
        contactInDefaultDirectoryQuery.bindLong(1, contactId);
        return contactInDefaultDirectoryQuery.simpleQueryForLong() != 0;
    }

    /**
     * Update the visible_contacts table according to the current visibility of contacts, which
     * is defined by {@link Clauses#CONTACT_IS_VISIBLE}.
     *
     * If {@code optionalContactId} is non-negative, it'll update only for the specified contact.
     */
    private void updateCustomContactVisibility(SQLiteDatabase db, long optionalContactId) {
        final long groupMembershipMimetypeId = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        String[] selectionArgs = new String[] {String.valueOf(groupMembershipMimetypeId)};

        final String contactIdSelect = (optionalContactId < 0) ? "" :
                (Contacts._ID + "=" + optionalContactId + " AND ");

        // First delete what needs to be deleted, then insert what needs to be added.
        // Since flash writes are very expensive, this approach is much better than
        // delete-all-insert-all.
        db.execSQL(
                "DELETE FROM " + Tables.VISIBLE_CONTACTS +
                " WHERE " + Contacts._ID + " IN" +
                    "(SELECT " + Contacts._ID +
                    " FROM " + Tables.CONTACTS +
                    " WHERE " + contactIdSelect + "(" + Clauses.CONTACT_IS_VISIBLE + ")=0) ",
                selectionArgs);

        db.execSQL(
                "INSERT INTO " + Tables.VISIBLE_CONTACTS +
                " SELECT " + Contacts._ID +
                " FROM " + Tables.CONTACTS +
                " WHERE " +
                    contactIdSelect +
                    Contacts._ID + " NOT IN " + Tables.VISIBLE_CONTACTS +
                    " AND (" + Clauses.CONTACT_IS_VISIBLE + ")=1 ",
                selectionArgs);
    }

    /**
     * Returns contact ID for the given contact or zero if it is NULL.
     */
    public long getContactId(long rawContactId) {
        final SQLiteStatement contactIdQuery = getWritableDatabase().compileStatement(
                    "SELECT " + RawContacts.CONTACT_ID +
                    " FROM " + Tables.RAW_CONTACTS +
                    " WHERE " + RawContacts._ID + "=?");
        try {
            contactIdQuery.bindLong(1, rawContactId);
            return contactIdQuery.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            return 0;  // No valid mapping found.
        }
    }

    public int getAggregationMode(long rawContactId) {
        final SQLiteStatement aggregationModeQuery = getWritableDatabase().compileStatement(
                    "SELECT " + RawContacts.AGGREGATION_MODE +
                    " FROM " + Tables.RAW_CONTACTS +
                    " WHERE " + RawContacts._ID + "=?");
        try {
            aggregationModeQuery.bindLong(1, rawContactId);
            return (int) aggregationModeQuery.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            return RawContacts.AGGREGATION_MODE_DISABLED;  // No valid row found.
        }
    }

    public void buildPhoneLookupAndContactQuery(
            SQLiteQueryBuilder qb, String normalizedNumber, String numberE164) {

        String minMatch = PhoneNumberUtils.toCallerIDMinMatch(normalizedNumber);
        StringBuilder sb = new StringBuilder();
        appendPhoneLookupTables(sb, minMatch, true);
        qb.setTables(sb.toString());

        sb = new StringBuilder();
        appendPhoneLookupSelection(sb, normalizedNumber, numberE164);
        qb.appendWhere(sb.toString());
    }

    /**
     * Phone lookup method that uses the custom SQLite function phone_number_compare_loose
     * that serves as a fallback in case the regular lookup does not return any results.
     * @param qb The query builder.
     * @param number The phone number to search for.
     */
    public void buildFallbackPhoneLookupAndContactQuery(SQLiteQueryBuilder qb, String number) {
        final String minMatch = PhoneNumberUtils.toCallerIDMinMatch(number);
        final StringBuilder sb = new StringBuilder();
        // Append lookup tables.
        sb.append(Tables.RAW_CONTACTS);
        sb.append(" JOIN " + Views.CONTACTS + " as contacts_view"
                + " ON (contacts_view._id = " + Tables.RAW_CONTACTS
                + "." + RawContacts.CONTACT_ID + ")" +
                " JOIN (SELECT " + PhoneLookupColumns.DATA_ID + "," +
                PhoneLookupColumns.NORMALIZED_NUMBER + " FROM "+ Tables.PHONE_LOOKUP + " "
                + "WHERE (" + Tables.PHONE_LOOKUP + "." + PhoneLookupColumns.MIN_MATCH + " = '");
        sb.append(minMatch);
        sb.append("')) AS lookup " +
                "ON lookup." + PhoneLookupColumns.DATA_ID + "=" + Tables.DATA + "." + Data._ID
                + " JOIN " + Tables.DATA + " "
                + "ON " + Tables.DATA + "." + Data.RAW_CONTACT_ID + "=" + Tables.RAW_CONTACTS + "."
                + RawContacts._ID);

        qb.setTables(sb.toString());

        sb.setLength(0);
        sb.append("PHONE_NUMBERS_EQUAL(" + Tables.DATA + "." + Phone.NUMBER + ", ");
        DatabaseUtils.appendEscapedSQLString(sb, number);
        sb.append(mUseStrictPhoneNumberComparison ? ", 1)" : ", 0, " + mMinMatch + ")");
        qb.appendWhere(sb.toString());
    }

    /**
     * Adds query for selecting the contact with the given {@code sipAddress} to the given
     * {@link StringBuilder}.
     *
     * @return the query arguments to be passed in with the query
     */
    public String[] buildSipContactQuery(StringBuilder sb, String sipAddress) {
        sb.append("upper(");
        sb.append(Data.DATA1);
        sb.append(")=upper(?) AND ");
        sb.append(DataColumns.MIMETYPE_ID);
        sb.append("=");
        sb.append(Long.toString(getMimeTypeIdForSip()));
        // Return the arguments to be passed to the query.
        return new String[] {sipAddress};
    }

    public String buildPhoneLookupAsNestedQuery(String number) {
        StringBuilder sb = new StringBuilder();
        final String minMatch = PhoneNumberUtils.toCallerIDMinMatch(number);
        sb.append("(SELECT DISTINCT raw_contact_id" + " FROM ");
        appendPhoneLookupTables(sb, minMatch, false);
        sb.append(" WHERE ");
        appendPhoneLookupSelection(sb, number, null);
        sb.append(")");
        return sb.toString();
    }

    private void appendPhoneLookupTables(
            StringBuilder sb, final String minMatch, boolean joinContacts) {

        sb.append(Tables.RAW_CONTACTS);
        if (joinContacts) {
            sb.append(" JOIN " + Views.CONTACTS + " contacts_view"
                    + " ON (contacts_view._id = raw_contacts.contact_id)");
        }
        sb.append(", (SELECT data_id, normalized_number, length(normalized_number) as len "
                + " FROM phone_lookup " + " WHERE (" + Tables.PHONE_LOOKUP + "."
                + PhoneLookupColumns.MIN_MATCH + " = '");
        sb.append(minMatch);
        sb.append("')) AS lookup, " + Tables.DATA);
    }

    private void appendPhoneLookupSelection(StringBuilder sb, String number, String numberE164) {
        sb.append("lookup.data_id=data._id AND data.raw_contact_id=raw_contacts._id");
        boolean hasNumberE164 = !TextUtils.isEmpty(numberE164);
        boolean hasNumber = !TextUtils.isEmpty(number);
        if (hasNumberE164 || hasNumber) {
            sb.append(" AND ( ");
            if (hasNumberE164) {
                sb.append(" lookup.normalized_number = ");
                DatabaseUtils.appendEscapedSQLString(sb, numberE164);
            }
            if (hasNumberE164 && hasNumber) {
                sb.append(" OR ");
            }
            if (hasNumber) {
                // Skip the suffix match entirely if we are using strict number comparison.
                if (!mUseStrictPhoneNumberComparison) {
                    int numberLen = number.length();
                    sb.append(" lookup.len <= ");
                    sb.append(numberLen);
                    sb.append(" AND substr(");
                    DatabaseUtils.appendEscapedSQLString(sb, number);
                    sb.append(',');
                    sb.append(numberLen);
                    sb.append(" - lookup.len + 1) = lookup.normalized_number");

                    // Some countries (e.g. Brazil) can have incoming calls which contain only
                    // the local number (no country calling code and no area code).  This case
                    // is handled below, see b/5197612.
                    // This also handles a Gingerbread -> ICS upgrade issue; see b/5638376.
                    sb.append(" OR (");
                    sb.append(" lookup.len > ");
                    sb.append(numberLen);
                    sb.append(" AND substr(lookup.normalized_number,");
                    sb.append("lookup.len + 1 - ");
                    sb.append(numberLen);
                    sb.append(") = ");
                    DatabaseUtils.appendEscapedSQLString(sb, number);
                    sb.append(")");
                } else {
                    sb.append("0");
                }
            }
            sb.append(')');
        }
    }

    public String getUseStrictPhoneNumberComparisonParameter() {
        return mUseStrictPhoneNumberComparison ? "1" : "0";
    }

    public String getMinMatchParameter() {
        return String.valueOf(mMinMatch);
    }

    /**
     * Loads common nickname mappings into the database.
     */
    private void loadNicknameLookupTable(SQLiteDatabase db) {
        db.execSQL("DELETE FROM " + Tables.NICKNAME_LOOKUP);

        String[] strings = mContext.getResources().getStringArray(
                com.android.internal.R.array.common_nicknames);
        if (strings == null || strings.length == 0) {
            return;
        }

        final SQLiteStatement nicknameLookupInsert = db.compileStatement("INSERT INTO "
                + Tables.NICKNAME_LOOKUP + "(" + NicknameLookupColumns.NAME + ","
                + NicknameLookupColumns.CLUSTER + ") VALUES (?,?)");

        try {
            for (int clusterId = 0; clusterId < strings.length; clusterId++) {
                String[] names = strings[clusterId].split(",");
                for (String name : names) {
                    String normalizedName = NameNormalizer.normalize(name);
                    try {
                        nicknameLookupInsert.bindString(1, normalizedName);
                        nicknameLookupInsert.bindString(2, String.valueOf(clusterId));
                        nicknameLookupInsert.executeInsert();
                    } catch (SQLiteException e) {
                        // Print the exception and keep going (this is not a fatal error).
                        Log.e(TAG, "Cannot insert nickname: " + name, e);
                    }
                }
            }
        } finally {
            nicknameLookupInsert.close();
        }
    }

    public static void copyStringValue(
            ContentValues toValues, String toKey, ContentValues fromValues, String fromKey) {

        if (fromValues.containsKey(fromKey)) {
            toValues.put(toKey, fromValues.getAsString(fromKey));
        }
    }

    public static void copyLongValue(
            ContentValues toValues, String toKey, ContentValues fromValues, String fromKey) {

        if (fromValues.containsKey(fromKey)) {
            long longValue;
            Object value = fromValues.get(fromKey);
            if (value instanceof Boolean) {
                longValue = (Boolean) value ? 1 : 0;
            } else if (value instanceof String) {
                longValue = Long.parseLong((String)value);
            } else {
                longValue = ((Number)value).longValue();
            }
            toValues.put(toKey, longValue);
        }
    }

    public SyncStateContentProviderHelper getSyncState() {
        return mSyncState;
    }

    /**
     * Returns the value from the {@link PropertyUtils.Tables#PROPERTIES} table.
     */
    public String getProperty(String key, String defaultValue) {
        return PropertyUtils.getProperty(getReadableDatabase(), key, defaultValue);
    }

    /**
     * Stores a key-value pair in the {@link PropertyUtils.Tables#PROPERTIES} table.
     */
    public void setProperty(String key, String value) {
        PropertyUtils.setProperty(getWritableDatabase(), key, value);
    }

    public void forceDirectoryRescan() {
        setProperty(DbProperties.DIRECTORY_SCAN_COMPLETE, "0");
    }

    /**
     * Test if the given column appears in the given projection.
     */
    public static boolean isInProjection(String[] projection, String column) {
        if (projection == null) {
            return true;  // Null means "all columns".  We can't really tell if it's in there.
        }
        for (String test : projection) {
            if (column.equals(test)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tests if any of the columns appear in the given projection.
     */
    public static boolean isInProjection(String[] projection, String... columns) {
        if (projection == null) {
            return true;
        }

        // Optimized for a single-column test
        if (columns.length == 1) {
            return isInProjection(projection, columns[0]);
        }
        for (String test : projection) {
            for (String column : columns) {
                if (column.equals(test)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a detailed exception message for the supplied URI.  It includes the calling
     * user and calling package(s).
     */
    public String exceptionMessage(Uri uri) {
        return exceptionMessage(null, uri);
    }

    /**
     * Returns a detailed exception message for the supplied URI.  It includes the calling
     * user and calling package(s).
     */
    public String exceptionMessage(String message, Uri uri) {
        StringBuilder sb = new StringBuilder();
        if (message != null) {
            sb.append(message).append("; ");
        }
        sb.append("URI: ").append(uri);
        final PackageManager pm = mContext.getPackageManager();
        int callingUid = Binder.getCallingUid();
        sb.append(", calling user: ");
        String userName = pm.getNameForUid(callingUid);
        sb.append(userName == null ? callingUid : userName);

        final String[] callerPackages = pm.getPackagesForUid(callingUid);
        if (callerPackages != null && callerPackages.length > 0) {
            if (callerPackages.length == 1) {
                sb.append(", calling package:");
                sb.append(callerPackages[0]);
            } else {
                sb.append(", calling package is one of: [");
                for (int i = 0; i < callerPackages.length; i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(callerPackages[i]);
                }
                sb.append("]");
            }
        }
        return sb.toString();
    }

    public void deleteStatusUpdate(long dataId) {
        final SQLiteStatement statusUpdateDelete = getWritableDatabase().compileStatement(
                    "DELETE FROM " + Tables.STATUS_UPDATES +
                    " WHERE " + StatusUpdatesColumns.DATA_ID + "=?");
        statusUpdateDelete.bindLong(1, dataId);
        statusUpdateDelete.execute();
    }

    public void replaceStatusUpdate(Long dataId, long timestamp, String status, String resPackage,
            Integer iconResource, Integer labelResource) {
        final SQLiteStatement statusUpdateReplace = getWritableDatabase().compileStatement(
                    "INSERT OR REPLACE INTO " + Tables.STATUS_UPDATES + "("
                            + StatusUpdatesColumns.DATA_ID + ", "
                            + StatusUpdates.STATUS_TIMESTAMP + ","
                            + StatusUpdates.STATUS + ","
                            + StatusUpdates.STATUS_RES_PACKAGE + ","
                            + StatusUpdates.STATUS_ICON + ","
                            + StatusUpdates.STATUS_LABEL + ")" +
                    " VALUES (?,?,?,?,?,?)");
        statusUpdateReplace.bindLong(1, dataId);
        statusUpdateReplace.bindLong(2, timestamp);
        bindString(statusUpdateReplace, 3, status);
        bindString(statusUpdateReplace, 4, resPackage);
        bindLong(statusUpdateReplace, 5, iconResource);
        bindLong(statusUpdateReplace, 6, labelResource);
        statusUpdateReplace.execute();
    }

    public void insertStatusUpdate(Long dataId, String status, String resPackage,
            Integer iconResource, Integer labelResource) {
        final SQLiteStatement statusUpdateInsert = getWritableDatabase().compileStatement(
                    "INSERT INTO " + Tables.STATUS_UPDATES + "("
                            + StatusUpdatesColumns.DATA_ID + ", "
                            + StatusUpdates.STATUS + ","
                            + StatusUpdates.STATUS_RES_PACKAGE + ","
                            + StatusUpdates.STATUS_ICON + ","
                            + StatusUpdates.STATUS_LABEL + ")" +
                    " VALUES (?,?,?,?,?)");
        try {
            statusUpdateInsert.bindLong(1, dataId);
            bindString(statusUpdateInsert, 2, status);
            bindString(statusUpdateInsert, 3, resPackage);
            bindLong(statusUpdateInsert, 4, iconResource);
            bindLong(statusUpdateInsert, 5, labelResource);
            statusUpdateInsert.executeInsert();
        } catch (SQLiteConstraintException e) {
            // The row already exists - update it
            final SQLiteStatement statusUpdateAutoTimestamp = getWritableDatabase()
                    .compileStatement(
                        "UPDATE " + Tables.STATUS_UPDATES +
                        " SET " + StatusUpdates.STATUS_TIMESTAMP + "=?,"
                                + StatusUpdates.STATUS + "=?" +
                        " WHERE " + StatusUpdatesColumns.DATA_ID + "=?"
                                + " AND " + StatusUpdates.STATUS + "!=?");

            long timestamp = System.currentTimeMillis();
            statusUpdateAutoTimestamp.bindLong(1, timestamp);
            bindString(statusUpdateAutoTimestamp, 2, status);
            statusUpdateAutoTimestamp.bindLong(3, dataId);
            bindString(statusUpdateAutoTimestamp, 4, status);
            statusUpdateAutoTimestamp.execute();

            final SQLiteStatement statusAttributionUpdate = getWritableDatabase().compileStatement(
                        "UPDATE " + Tables.STATUS_UPDATES +
                        " SET " + StatusUpdates.STATUS_RES_PACKAGE + "=?,"
                                + StatusUpdates.STATUS_ICON + "=?,"
                                + StatusUpdates.STATUS_LABEL + "=?" +
                        " WHERE " + StatusUpdatesColumns.DATA_ID + "=?");
            bindString(statusAttributionUpdate, 1, resPackage);
            bindLong(statusAttributionUpdate, 2, iconResource);
            bindLong(statusAttributionUpdate, 3, labelResource);
            statusAttributionUpdate.bindLong(4, dataId);
            statusAttributionUpdate.execute();
        }
    }

    /**
     * Updates a raw contact display name based on data rows, e.g. structured name,
     * organization, email etc.
     */
    public void updateRawContactDisplayName(SQLiteDatabase db, long rawContactId) {
        if (mNameSplitter == null) {
            createNameSplitter();
        }

        int bestDisplayNameSource = DisplayNameSources.UNDEFINED;
        NameSplitter.Name bestName = null;
        String bestDisplayName = null;
        String bestPhoneticName = null;
        int bestPhoneticNameStyle = PhoneticNameStyle.UNDEFINED;

        mSelectionArgs1[0] = String.valueOf(rawContactId);
        Cursor c = db.rawQuery(RawContactNameQuery.RAW_SQL, mSelectionArgs1);
        try {
            while (c.moveToNext()) {
                int mimeType = c.getInt(RawContactNameQuery.MIMETYPE);
                int source = getDisplayNameSourceForMimeTypeId(mimeType);

                if (source == DisplayNameSources.STRUCTURED_NAME) {
                    final String given = c.getString(RawContactNameQuery.GIVEN_NAME);
                    final String middle = c.getString(RawContactNameQuery.MIDDLE_NAME);
                    final String family = c.getString(RawContactNameQuery.FAMILY_NAME);
                    final String suffix = c.getString(RawContactNameQuery.SUFFIX);
                    final String prefix = c.getString(RawContactNameQuery.PREFIX);
                    if (TextUtils.isEmpty(given) && TextUtils.isEmpty(middle)
                            && TextUtils.isEmpty(family) && TextUtils.isEmpty(suffix)
                            && TextUtils.isEmpty(prefix)) {
                        // Every non-phonetic name component is empty. Therefore, lets lower the
                        // source score to STRUCTURED_PHONETIC_NAME.
                        source = DisplayNameSources.STRUCTURED_PHONETIC_NAME;
                    }
                }

                if (source < bestDisplayNameSource || source == DisplayNameSources.UNDEFINED) {
                    continue;
                }

                if (source == bestDisplayNameSource
                        && c.getInt(RawContactNameQuery.IS_PRIMARY) == 0) {
                    continue;
                }

                if (mimeType == getMimeTypeIdForStructuredName()) {
                    NameSplitter.Name name;
                    if (bestName != null) {
                        name = new NameSplitter.Name();
                    } else {
                        name = mName;
                        name.clear();
                    }
                    name.prefix = c.getString(RawContactNameQuery.PREFIX);
                    name.givenNames = c.getString(RawContactNameQuery.GIVEN_NAME);
                    name.middleName = c.getString(RawContactNameQuery.MIDDLE_NAME);
                    name.familyName = c.getString(RawContactNameQuery.FAMILY_NAME);
                    name.suffix = c.getString(RawContactNameQuery.SUFFIX);
                    name.fullNameStyle = c.isNull(RawContactNameQuery.FULL_NAME_STYLE)
                            ? FullNameStyle.UNDEFINED
                            : c.getInt(RawContactNameQuery.FULL_NAME_STYLE);
                    name.phoneticFamilyName = c.getString(RawContactNameQuery.PHONETIC_FAMILY_NAME);
                    name.phoneticMiddleName = c.getString(RawContactNameQuery.PHONETIC_MIDDLE_NAME);
                    name.phoneticGivenName = c.getString(RawContactNameQuery.PHONETIC_GIVEN_NAME);
                    name.phoneticNameStyle = c.isNull(RawContactNameQuery.PHONETIC_NAME_STYLE)
                            ? PhoneticNameStyle.UNDEFINED
                            : c.getInt(RawContactNameQuery.PHONETIC_NAME_STYLE);
                    if (!name.isEmpty()) {
                        bestDisplayNameSource = source;
                        bestName = name;
                    }
                } else if (mimeType == getMimeTypeIdForOrganization()) {
                    mCharArrayBuffer.sizeCopied = 0;
                    c.copyStringToBuffer(RawContactNameQuery.DATA1, mCharArrayBuffer);
                    if (mCharArrayBuffer.sizeCopied != 0) {
                        bestDisplayNameSource = source;
                        bestDisplayName = new String(mCharArrayBuffer.data, 0,
                                mCharArrayBuffer.sizeCopied);
                        bestPhoneticName = c.getString(
                                RawContactNameQuery.ORGANIZATION_PHONETIC_NAME);
                        bestPhoneticNameStyle =
                                c.isNull(RawContactNameQuery.ORGANIZATION_PHONETIC_NAME_STYLE)
                                   ? PhoneticNameStyle.UNDEFINED
                                   : c.getInt(RawContactNameQuery.ORGANIZATION_PHONETIC_NAME_STYLE);
                    } else {
                        c.copyStringToBuffer(RawContactNameQuery.TITLE, mCharArrayBuffer);
                        if (mCharArrayBuffer.sizeCopied != 0) {
                            bestDisplayNameSource = source;
                            bestDisplayName = new String(mCharArrayBuffer.data, 0,
                                    mCharArrayBuffer.sizeCopied);
                            bestPhoneticName = null;
                            bestPhoneticNameStyle = PhoneticNameStyle.UNDEFINED;
                        }
                    }
                } else {
                    // Display name is at DATA1 in all other types.
                    // This is ensured in the constructor.

                    mCharArrayBuffer.sizeCopied = 0;
                    c.copyStringToBuffer(RawContactNameQuery.DATA1, mCharArrayBuffer);
                    if (mCharArrayBuffer.sizeCopied != 0) {
                        bestDisplayNameSource = source;
                        bestDisplayName = new String(mCharArrayBuffer.data, 0,
                                mCharArrayBuffer.sizeCopied);
                        bestPhoneticName = null;
                        bestPhoneticNameStyle = PhoneticNameStyle.UNDEFINED;
                    }
                }
            }

        } finally {
            c.close();
        }

        String displayNamePrimary;
        String displayNameAlternative;
        String sortNamePrimary;
        String sortNameAlternative;
        String sortKeyPrimary = null;
        String sortKeyAlternative = null;
        int displayNameStyle = FullNameStyle.UNDEFINED;

        if (bestDisplayNameSource == DisplayNameSources.STRUCTURED_NAME
                || bestDisplayNameSource == DisplayNameSources.STRUCTURED_PHONETIC_NAME) {
            displayNameStyle = bestName.fullNameStyle;
            if (displayNameStyle == FullNameStyle.CJK
                    || displayNameStyle == FullNameStyle.UNDEFINED) {
                displayNameStyle = mNameSplitter.getAdjustedFullNameStyle(displayNameStyle);
                bestName.fullNameStyle = displayNameStyle;
            }

            displayNamePrimary = mNameSplitter.join(bestName, true, true);
            displayNameAlternative = mNameSplitter.join(bestName, false, true);

            if (TextUtils.isEmpty(bestName.prefix)) {
                sortNamePrimary = displayNamePrimary;
                sortNameAlternative = displayNameAlternative;
            } else {
                sortNamePrimary = mNameSplitter.join(bestName, true, false);
                sortNameAlternative = mNameSplitter.join(bestName, false, false);
            }

            bestPhoneticName = mNameSplitter.joinPhoneticName(bestName);
            bestPhoneticNameStyle = bestName.phoneticNameStyle;
        } else {
            displayNamePrimary = displayNameAlternative = bestDisplayName;
            sortNamePrimary = sortNameAlternative = bestDisplayName;
        }

        if (bestPhoneticName != null) {
            if (displayNamePrimary == null) {
                displayNamePrimary = bestPhoneticName;
            }
            if (displayNameAlternative == null) {
                displayNameAlternative = bestPhoneticName;
            }
            // Phonetic names disregard name order so displayNamePrimary and displayNameAlternative
            // are the same.
            sortKeyPrimary = sortKeyAlternative = bestPhoneticName;
            if (bestPhoneticNameStyle == PhoneticNameStyle.UNDEFINED) {
                bestPhoneticNameStyle = mNameSplitter.guessPhoneticNameStyle(bestPhoneticName);
            }
        } else {
            bestPhoneticNameStyle = PhoneticNameStyle.UNDEFINED;
            if (displayNameStyle == FullNameStyle.UNDEFINED) {
                displayNameStyle = mNameSplitter.guessFullNameStyle(bestDisplayName);
                if (displayNameStyle == FullNameStyle.UNDEFINED
                        || displayNameStyle == FullNameStyle.CJK) {
                    displayNameStyle = mNameSplitter.getAdjustedNameStyleBasedOnPhoneticNameStyle(
                            displayNameStyle, bestPhoneticNameStyle);
                }
                displayNameStyle = mNameSplitter.getAdjustedFullNameStyle(displayNameStyle);
            }
            if (displayNameStyle == FullNameStyle.CHINESE ||
                    displayNameStyle == FullNameStyle.CJK) {
                sortKeyPrimary = sortKeyAlternative = sortNamePrimary;
            }
        }

        if (sortKeyPrimary == null) {
            sortKeyPrimary = sortNamePrimary;
            sortKeyAlternative = sortNameAlternative;
        }

        final ContactLocaleUtils localeUtils = ContactLocaleUtils.getInstance();
        int phonebookBucketPrimary = TextUtils.isEmpty(sortKeyPrimary)
                ? localeUtils.getNumberBucketIndex()
                : localeUtils.getBucketIndex(sortKeyPrimary);
        String phonebookLabelPrimary = localeUtils.getBucketLabel(phonebookBucketPrimary);

        int phonebookBucketAlternative = TextUtils.isEmpty(sortKeyAlternative)
                ? localeUtils.getNumberBucketIndex()
                : localeUtils.getBucketIndex(sortKeyAlternative);
        String phonebookLabelAlternative = localeUtils.getBucketLabel(phonebookBucketAlternative);

        final SQLiteStatement rawContactDisplayNameUpdate = db.compileStatement(
                    "UPDATE " + Tables.RAW_CONTACTS +
                    " SET " +
                            RawContacts.DISPLAY_NAME_SOURCE + "=?," +
                            RawContacts.DISPLAY_NAME_PRIMARY + "=?," +
                            RawContacts.DISPLAY_NAME_ALTERNATIVE + "=?," +
                            RawContacts.PHONETIC_NAME + "=?," +
                            RawContacts.PHONETIC_NAME_STYLE + "=?," +
                            RawContacts.SORT_KEY_PRIMARY + "=?," +
                            RawContactsColumns.PHONEBOOK_LABEL_PRIMARY + "=?," +
                            RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY + "=?," +
                            RawContacts.SORT_KEY_ALTERNATIVE + "=?," +
                            RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE + "=?," +
                            RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE + "=?" +
                    " WHERE " + RawContacts._ID + "=?");

        rawContactDisplayNameUpdate.bindLong(1, bestDisplayNameSource);
        bindString(rawContactDisplayNameUpdate, 2, displayNamePrimary);
        bindString(rawContactDisplayNameUpdate, 3, displayNameAlternative);
        bindString(rawContactDisplayNameUpdate, 4, bestPhoneticName);
        rawContactDisplayNameUpdate.bindLong(5, bestPhoneticNameStyle);
        bindString(rawContactDisplayNameUpdate, 6, sortKeyPrimary);
        bindString(rawContactDisplayNameUpdate, 7, phonebookLabelPrimary);
        rawContactDisplayNameUpdate.bindLong(8, phonebookBucketPrimary);
        bindString(rawContactDisplayNameUpdate, 9, sortKeyAlternative);
        bindString(rawContactDisplayNameUpdate, 10, phonebookLabelAlternative);
        rawContactDisplayNameUpdate.bindLong(11, phonebookBucketAlternative);
        rawContactDisplayNameUpdate.bindLong(12, rawContactId);
        rawContactDisplayNameUpdate.execute();
    }

    /**
     * Sets the given dataId record in the "data" table to primary, and resets all data records of
     * the same mimetype and under the same contact to not be primary.
     *
     * @param dataId the id of the data record to be set to primary. Pass -1 to clear the primary
     * flag of all data items of this raw contacts
     */
    public void setIsPrimary(long rawContactId, long dataId, long mimeTypeId) {
        final SQLiteStatement setPrimaryStatement = getWritableDatabase().compileStatement(
                    "UPDATE " + Tables.DATA +
                    " SET " + Data.IS_PRIMARY + "=(_id=?)" +
                    " WHERE " + DataColumns.MIMETYPE_ID + "=?" +
                    "   AND " + Data.RAW_CONTACT_ID + "=?");
        setPrimaryStatement.bindLong(1, dataId);
        setPrimaryStatement.bindLong(2, mimeTypeId);
        setPrimaryStatement.bindLong(3, rawContactId);
        setPrimaryStatement.execute();
    }

    /**
     * Clears the super primary of all data items of the given raw contact. does not touch
     * other raw contacts of the same joined aggregate
     */
    public void clearSuperPrimary(long rawContactId, long mimeTypeId) {
        final SQLiteStatement clearSuperPrimaryStatement = getWritableDatabase().compileStatement(
                    "UPDATE " + Tables.DATA +
                    " SET " + Data.IS_SUPER_PRIMARY + "=0" +
                    " WHERE " + DataColumns.MIMETYPE_ID + "=?" +
                    "   AND " + Data.RAW_CONTACT_ID + "=?");
        clearSuperPrimaryStatement.bindLong(1, mimeTypeId);
        clearSuperPrimaryStatement.bindLong(2, rawContactId);
        clearSuperPrimaryStatement.execute();
    }

    /**
     * Sets the given dataId record in the "data" table to "super primary", and resets all data
     * records of the same mimetype and under the same aggregate to not be "super primary".
     *
     * @param dataId the id of the data record to be set to primary.
     */
    public void setIsSuperPrimary(long rawContactId, long dataId, long mimeTypeId) {
        final SQLiteStatement setSuperPrimaryStatement = getWritableDatabase().compileStatement(
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
        setSuperPrimaryStatement.bindLong(1, dataId);
        setSuperPrimaryStatement.bindLong(2, mimeTypeId);
        setSuperPrimaryStatement.bindLong(3, rawContactId);
        setSuperPrimaryStatement.execute();
    }

    /**
     * Inserts a record in the {@link Tables#NAME_LOOKUP} table.
     */
    public void insertNameLookup(long rawContactId, long dataId, int lookupType, String name) {
        if (TextUtils.isEmpty(name)) {
            return;
        }

        final SQLiteStatement nameLookupInsert = getWritableDatabase().compileStatement(
                    "INSERT OR IGNORE INTO " + Tables.NAME_LOOKUP + "("
                            + NameLookupColumns.RAW_CONTACT_ID + ","
                            + NameLookupColumns.DATA_ID + ","
                            + NameLookupColumns.NAME_TYPE + ","
                            + NameLookupColumns.NORMALIZED_NAME
                    + ") VALUES (?,?,?,?)");
        nameLookupInsert.bindLong(1, rawContactId);
        nameLookupInsert.bindLong(2, dataId);
        nameLookupInsert.bindLong(3, lookupType);
        bindString(nameLookupInsert, 4, name);
        nameLookupInsert.executeInsert();
    }

    /**
     * Deletes all {@link Tables#NAME_LOOKUP} table rows associated with the specified data element.
     */
    public void deleteNameLookup(long dataId) {
        final SQLiteStatement nameLookupDelete = getWritableDatabase().compileStatement(
                    "DELETE FROM " + Tables.NAME_LOOKUP +
                    " WHERE " + NameLookupColumns.DATA_ID + "=?");
        nameLookupDelete.bindLong(1, dataId);
        nameLookupDelete.execute();
    }

    public String insertNameLookupForEmail(long rawContactId, long dataId, String email) {
        if (TextUtils.isEmpty(email)) {
            return null;
        }

        String address = extractHandleFromEmailAddress(email);
        if (address == null) {
            return null;
        }

        insertNameLookup(rawContactId, dataId,
                NameLookupType.EMAIL_BASED_NICKNAME, NameNormalizer.normalize(address));
        return address;
    }

    /**
     * Normalizes the nickname and inserts it in the name lookup table.
     */
    public void insertNameLookupForNickname(long rawContactId, long dataId, String nickname) {
        if (!TextUtils.isEmpty(nickname)) {
            insertNameLookup(rawContactId, dataId,
                    NameLookupType.NICKNAME, NameNormalizer.normalize(nickname));
        }
    }

    /**
     * Performs a query and returns true if any Data item of the raw contact with the given
     * id and mimetype is marked as super-primary
     */
    public boolean rawContactHasSuperPrimary(long rawContactId, long mimeTypeId) {
        final Cursor existsCursor = getReadableDatabase().rawQuery(
                "SELECT EXISTS(SELECT 1 FROM " + Tables.DATA +
                " WHERE " + Data.RAW_CONTACT_ID + "=?" +
                " AND " + DataColumns.MIMETYPE_ID + "=?" +
                " AND " + Data.IS_SUPER_PRIMARY + "<>0)",
                new String[] {String.valueOf(rawContactId), String.valueOf(mimeTypeId)});
        try {
            if (!existsCursor.moveToFirst()) throw new IllegalStateException();
            return existsCursor.getInt(0) != 0;
        } finally {
            existsCursor.close();
        }
    }

    public String getCurrentCountryIso() {
        // For debugging.
        // String injected = android.os.SystemProperties.get("debug.cp2.injectedCountryIso");
        // if (!TextUtils.isEmpty(injected)) return injected;
        return mCountryMonitor.getCountryIso();
    }

    @NeededForTesting
    /* package */ void setUseStrictPhoneNumberComparisonForTest(boolean useStrict) {
        mUseStrictPhoneNumberComparison = useStrict;
    }

    @NeededForTesting
    /* package */ boolean getUseStrictPhoneNumberComparisonForTest() {
        return mUseStrictPhoneNumberComparison;
    }

    @VisibleForTesting
    public void setMinMatchForTest(int minMatch) {
        mMinMatch = minMatch;
    }

    @VisibleForTesting
    public int getMinMatchForTest() {
        return mMinMatch;
    }

    @NeededForTesting
    /* package */ String querySearchIndexContentForTest(long contactId) {
        return DatabaseUtils.stringForQuery(getReadableDatabase(),
                "SELECT " + SearchIndexColumns.CONTENT +
                " FROM " + Tables.SEARCH_INDEX +
                " WHERE " + SearchIndexColumns.CONTACT_ID + "=CAST(? AS int)",
                new String[] {String.valueOf(contactId)});
    }

    @NeededForTesting
    /* package */ String querySearchIndexTokensForTest(long contactId) {
        return DatabaseUtils.stringForQuery(getReadableDatabase(),
                "SELECT " + SearchIndexColumns.TOKENS +
                " FROM " + Tables.SEARCH_INDEX +
                " WHERE " + SearchIndexColumns.CONTACT_ID + "=CAST(? AS int)",
                new String[] {String.valueOf(contactId)});
    }

    public long upsertMetadataSync(String backupId, Long accountId, String data, Integer deleted) {
        final SQLiteStatement metadataSyncInsert = getWritableDatabase().compileStatement(
                    "INSERT OR REPLACE INTO " + Tables.METADATA_SYNC + "("
                            + MetadataSync.RAW_CONTACT_BACKUP_ID + ", "
                            + MetadataSyncColumns.ACCOUNT_ID + ", "
                            + MetadataSync.DATA + ","
                            + MetadataSync.DELETED + ")" +
                            " VALUES (?,?,?,?)");
        metadataSyncInsert.bindString(1, backupId);
        metadataSyncInsert.bindLong(2, accountId);
        data = (data == null) ? "" : data;
        metadataSyncInsert.bindString(3, data);
        metadataSyncInsert.bindLong(4, deleted);
        return metadataSyncInsert.executeInsert();
    }

    public static void notifyProviderStatusChange(Context context) {
        context.getContentResolver().notifyChange(ProviderStatus.CONTENT_URI,
                /* observer= */ null, /* syncToNetwork= */ false);
    }

    public long getDatabaseCreationTime() {
        return mDatabaseCreationTime;
    }

    private SqlChecker mCachedSqlChecker;

    private SqlChecker getSqlChecker() {
        // No need for synchronization on mCachedSqlChecker, because worst-case we'll just
        // initialize it twice.
        if (mCachedSqlChecker != null) {
            return mCachedSqlChecker;
        }
        final ArrayList<String> invalidTokens = new ArrayList<>();

        if (DISALLOW_SUB_QUERIES) {
            // Disallow referring to tables and views.  However, we exempt tables whose names are
            // also used as column names of any tables.  (Right now it's only 'data'.)
            invalidTokens.addAll(
                    DatabaseAnalyzer.findTableViewsAllowingColumns(getReadableDatabase()));

            // Disallow token "select" to disallow subqueries.
            invalidTokens.add("select");

            // Allow the use of "default_directory" for now, as it used to be sort of commonly used...
            invalidTokens.remove(Tables.DEFAULT_DIRECTORY.toLowerCase());
        }

        mCachedSqlChecker = new SqlChecker(invalidTokens);

        return mCachedSqlChecker;
    }

    /**
     * Ensure (a piece of) SQL is valid and doesn't contain disallowed tokens.
     */
    public void validateSql(String callerPackage, String sqlPiece) {
        // TODO Replace the Runnable with a lambda -- which would crash right now due to an art bug?
        runSqlValidation(callerPackage, new Runnable() {
            @Override
            public void run() {
                ContactsDatabaseHelper.this.getSqlChecker().ensureNoInvalidTokens(sqlPiece);
            }
        });
    }

    /**
     * Ensure all keys in {@code values} are valid. (i.e. they're all single token.)
     */
    public void validateContentValues(String callerPackage, ContentValues values) {
        // TODO Replace the Runnable with a lambda -- which would crash right now due to an art bug?
        runSqlValidation(callerPackage, new Runnable() {
            @Override
            public void run() {
                for (String key : values.keySet()) {
                    ContactsDatabaseHelper.this.getSqlChecker().ensureSingleTokenOnly(key);
                }
            }
        });
   }

    /**
     * Ensure all column names in {@code projection} are valid. (i.e. they're all single token.)
     */
    public void validateProjection(String callerPackage, String[] projection) {
        // TODO Replace the Runnable with a lambda -- which would crash right now due to an art bug?
        if (projection != null) {
            runSqlValidation(callerPackage, new Runnable() {
                @Override
                public void run() {
                    for (String column : projection) {
                        ContactsDatabaseHelper.this.getSqlChecker().ensureSingleTokenOnly(column);
                    }
                }
            });
        }
    }

    private void runSqlValidation(String callerPackage, Runnable r) {
        try {
            r.run();
        } catch (InvalidSqlException e) {
            reportInvalidSql(callerPackage, e);
        }
    }

    private void reportInvalidSql(String callerPackage, InvalidSqlException e) {
        Log.e(TAG, String.format("%s caller=%s", e.getMessage(), callerPackage));
        throw e;
    }

    /**
     * Calls WTF without crashing, so we can collect errors in the wild.  During unit tests, it'll
     * log only.
     */
    public void logWtf(String message) {
        if (mIsTestInstance) {
            Slog.w(TAG, "[Test mode, warning only] " + message);
        } else {
            Slog.wtfStack(TAG, message);
        }
    }

    public void dump(PrintWriter pw) {
        pw.print("CountryISO: ");
        pw.println(getCurrentCountryIso());

        pw.print("UseStrictPhoneNumberComparison: ");
        pw.println(mUseStrictPhoneNumberComparison);

        pw.print("UseStrictPhoneNumberComparisonBase: ");
        pw.println(mUseStrictPhoneNumberComparisonBase);

        pw.print("UseStrictPhoneNumberComparisonRU: ");
        pw.println(mUseStrictPhoneNumberComparisonForRussia);

        pw.print("UseStrictPhoneNumberComparisonKZ: ");
        pw.println(mUseStrictPhoneNumberComparisonForKazakhstan);

        pw.println();
    }
}
