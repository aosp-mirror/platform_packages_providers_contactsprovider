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

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.Binder;
import android.os.Bundle;
import android.os.SystemClock;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.DisplayNameSources;
import android.provider.ContactsContract.FullNameStyle;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.SocialContract.Activities;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.text.util.Rfc822Tokenizer;
import android.util.Log;

import java.util.HashMap;
import java.util.Locale;

/**
 * Database helper for contacts. Designed as a singleton to make sure that all
 * {@link android.content.ContentProvider} users get the same reference.
 * Provides handy methods for maintaining package and mime-type lookup tables.
 */
/* package */ class ContactsDatabaseHelper extends SQLiteOpenHelper {
    private static final String TAG = "ContactsDatabaseHelper";

    /**
     * Contacts DB version ranges:
     * <pre>
     *   0-98    Cupcake/Donut
     *   100-199 Eclair
     *   200-299 Eclair-MR1
     *   300-349 Froyo
     *   350-399 Gingerbread
     *   400-499 Honeycomb
     * </pre>
     */
    static final int DATABASE_VERSION = 353;

    private static final String DATABASE_NAME = "contacts2.db";
    private static final String DATABASE_PRESENCE = "presence_db";

    public interface Tables {
        public static final String CONTACTS = "contacts";
        public static final String RAW_CONTACTS = "raw_contacts";
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
        public static final String CALLS = "calls";
        public static final String CONTACT_ENTITIES = "contact_entities_view";
        public static final String CONTACT_ENTITIES_RESTRICTED = "contact_entities_view_restricted";
        public static final String STATUS_UPDATES = "status_updates";
        public static final String PROPERTIES = "properties";
        public static final String ACCOUNTS = "accounts";

        public static final String DATA_JOIN_MIMETYPES = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id)";

        public static final String DATA_JOIN_RAW_CONTACTS = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        public static final String DATA_JOIN_MIMETYPE_RAW_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        // NOTE: This requires late binding of GroupMembership MIME-type
        public static final String RAW_CONTACTS_JOIN_SETTINGS_DATA_GROUPS = "raw_contacts "
                + "LEFT OUTER JOIN settings ON ("
                    + "raw_contacts.account_name = settings.account_name AND "
                    + "raw_contacts.account_type = settings.account_type) "
                + "LEFT OUTER JOIN data ON (data.mimetype_id=? AND "
                    + "data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN groups ON (groups._id = data." + GroupMembership.GROUP_ROW_ID
                + ")";

        // NOTE: This requires late binding of GroupMembership MIME-type
        public static final String SETTINGS_JOIN_RAW_CONTACTS_DATA_MIMETYPES_CONTACTS = "settings "
                + "LEFT OUTER JOIN raw_contacts ON ("
                    + "raw_contacts.account_name = settings.account_name AND "
                    + "raw_contacts.account_type = settings.account_type) "
                + "LEFT OUTER JOIN data ON (data.mimetype_id=? AND "
                    + "data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_GROUPS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN groups "
                + "  ON (mimetypes.mimetype='" + GroupMembership.CONTENT_ITEM_TYPE + "' "
                + "      AND groups._id = data." + GroupMembership.GROUP_ROW_ID + ") ";

        public static final String GROUPS_JOIN_PACKAGES = "groups "
                + "LEFT OUTER JOIN packages ON (groups.package_id = packages._id)";


        public static final String ACTIVITIES = "activities";

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
                + "INNER JOIN raw_contacts ON (name_lookup.raw_contact_id = raw_contacts._id)";
    }

    public interface Views {
        public static final String DATA_ALL = "view_data";
        public static final String DATA_RESTRICTED = "view_data_restricted";

        public static final String RAW_CONTACTS_ALL = "view_raw_contacts";
        public static final String RAW_CONTACTS_RESTRICTED = "view_raw_contacts_restricted";

        public static final String CONTACTS_ALL = "view_contacts";
        public static final String CONTACTS_RESTRICTED = "view_contacts_restricted";

        public static final String GROUPS_ALL = "view_groups";
    }

    public interface Clauses {
        final String MIMETYPE_IS_GROUP_MEMBERSHIP = MimetypesColumns.CONCRETE_MIMETYPE + "='"
                + GroupMembership.CONTENT_ITEM_TYPE + "'";

        final String BELONGS_TO_GROUP = DataColumns.CONCRETE_GROUP_ID + "="
                + GroupsColumns.CONCRETE_ID;

        final String HAVING_NO_GROUPS = "COUNT(" + DataColumns.CONCRETE_GROUP_ID + ") == 0";

        final String GROUP_BY_ACCOUNT_CONTACT_ID = SettingsColumns.CONCRETE_ACCOUNT_NAME + ","
                + SettingsColumns.CONCRETE_ACCOUNT_TYPE + "," + RawContacts.CONTACT_ID;

        final String RAW_CONTACT_IS_LOCAL = RawContactsColumns.CONCRETE_ACCOUNT_NAME
                + " IS NULL AND " + RawContactsColumns.CONCRETE_ACCOUNT_TYPE + " IS NULL";

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
                + Groups.ACCOUNT_NAME + "=? AND " + Groups.ACCOUNT_TYPE + "=?";
    }

    public interface ContactsColumns {
        /**
         * This flag is set for a contact if it has only one constituent raw contact and
         * it is restricted.
         */
        public static final String SINGLE_IS_RESTRICTED = "single_is_restricted";

        public static final String LAST_STATUS_UPDATE_ID = "status_update_id";

        public static final String CONCRETE_ID = Tables.CONTACTS + "." + BaseColumns._ID;

        public static final String CONCRETE_TIMES_CONTACTED = Tables.CONTACTS + "."
                + Contacts.TIMES_CONTACTED;
        public static final String CONCRETE_LAST_TIME_CONTACTED = Tables.CONTACTS + "."
                + Contacts.LAST_TIME_CONTACTED;
        public static final String CONCRETE_STARRED = Tables.CONTACTS + "." + Contacts.STARRED;
        public static final String CONCRETE_CUSTOM_RINGTONE = Tables.CONTACTS + "."
                + Contacts.CUSTOM_RINGTONE;
        public static final String CONCRETE_SEND_TO_VOICEMAIL = Tables.CONTACTS + "."
                + Contacts.SEND_TO_VOICEMAIL;
        public static final String CONCRETE_LOOKUP_KEY = Tables.CONTACTS + "."
                + Contacts.LOOKUP_KEY;
    }

    public interface RawContactsColumns {
        public static final String CONCRETE_ID =
                Tables.RAW_CONTACTS + "." + BaseColumns._ID;
        public static final String CONCRETE_ACCOUNT_NAME =
                Tables.RAW_CONTACTS + "." + RawContacts.ACCOUNT_NAME;
        public static final String CONCRETE_ACCOUNT_TYPE =
                Tables.RAW_CONTACTS + "." + RawContacts.ACCOUNT_TYPE;
        public static final String CONCRETE_SOURCE_ID =
                Tables.RAW_CONTACTS + "." + RawContacts.SOURCE_ID;
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
        public static final String CONCRETE_STARRED =
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED;
        public static final String CONCRETE_IS_RESTRICTED =
                Tables.RAW_CONTACTS + "." + RawContacts.IS_RESTRICTED;

        public static final String DISPLAY_NAME = RawContacts.DISPLAY_NAME_PRIMARY;
        public static final String DISPLAY_NAME_SOURCE = RawContacts.DISPLAY_NAME_SOURCE;
        public static final String AGGREGATION_NEEDED = "aggregation_needed";
        public static final String CONTACT_IN_VISIBLE_GROUP = "contact_in_visible_group";

        public static final String CONCRETE_DISPLAY_NAME =
                Tables.RAW_CONTACTS + "." + DISPLAY_NAME;
        public static final String CONCRETE_CONTACT_ID =
                Tables.RAW_CONTACTS + "." + RawContacts.CONTACT_ID;
        public static final String CONCRETE_NAME_VERIFIED =
                Tables.RAW_CONTACTS + "." + RawContacts.NAME_VERIFIED;
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

    // Used only for legacy API support
    public interface ExtensionsColumns {
        public static final String NAME = Data.DATA1;
        public static final String VALUE = Data.DATA2;
    }

    public interface GroupMembershipColumns {
        public static final String RAW_CONTACT_ID = Data.RAW_CONTACT_ID;
        public static final String GROUP_ROW_ID = GroupMembership.GROUP_ROW_ID;
    }

    public interface PhoneColumns {
        public static final String NORMALIZED_NUMBER = Data.DATA4;
        public static final String CONCRETE_NORMALIZED_NUMBER = DataColumns.CONCRETE_DATA4;
    }

    public interface GroupsColumns {
        public static final String PACKAGE_ID = "package_id";

        public static final String CONCRETE_ID = Tables.GROUPS + "." + BaseColumns._ID;
        public static final String CONCRETE_SOURCE_ID = Tables.GROUPS + "." + Groups.SOURCE_ID;
        public static final String CONCRETE_ACCOUNT_NAME = Tables.GROUPS + "." + Groups.ACCOUNT_NAME;
        public static final String CONCRETE_ACCOUNT_TYPE = Tables.GROUPS + "." + Groups.ACCOUNT_TYPE;
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

    public final static class NameLookupType {
        public static final int NAME_EXACT = 0;
        public static final int NAME_VARIANT = 1;
        public static final int NAME_COLLATION_KEY = 2;
        public static final int NICKNAME = 3;
        public static final int EMAIL_BASED_NICKNAME = 4;
        public static final int ORGANIZATION = 5;
        public static final int NAME_SHORTHAND = 6;
        public static final int NAME_CONSONANTS = 7;

        // This is the highest name lookup type code plus one
        public static final int TYPE_COUNT = 8;

        public static boolean isBasedOnStructuredName(int nameLookupType) {
            return nameLookupType == NameLookupType.NAME_EXACT
                    || nameLookupType == NameLookupType.NAME_VARIANT
                    || nameLookupType == NameLookupType.NAME_COLLATION_KEY;
        }
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

    public interface PropertiesColumns {
        String PROPERTY_KEY = "property_key";
        String PROPERTY_VALUE = "property_value";
    }

    /** In-memory cache of previously found MIME-type mappings */
    private final HashMap<String, Long> mMimetypeCache = new HashMap<String, Long>();
    /** In-memory cache of previously found package name mappings */
    private final HashMap<String, Long> mPackageCache = new HashMap<String, Long>();


    /** Compiled statements for querying and inserting mappings */
    private SQLiteStatement mMimetypeQuery;
    private SQLiteStatement mPackageQuery;
    private SQLiteStatement mContactIdQuery;
    private SQLiteStatement mAggregationModeQuery;
    private SQLiteStatement mMimetypeInsert;
    private SQLiteStatement mPackageInsert;
    private SQLiteStatement mDataMimetypeQuery;
    private SQLiteStatement mActivitiesMimetypeQuery;

    private final Context mContext;
    private final SyncStateContentProviderHelper mSyncState;


    /** Compiled statements for updating {@link Contacts#IN_VISIBLE_GROUP}. */
    private SQLiteStatement mVisibleSpecificUpdate;
    private SQLiteStatement mVisibleUpdateRawContacts;
    private SQLiteStatement mVisibleSpecificUpdateRawContacts;

    private boolean mReopenDatabase = false;

    private static ContactsDatabaseHelper sSingleton = null;

    private boolean mUseStrictPhoneNumberComparison;

    /**
     * List of package names with access to {@link RawContacts#IS_RESTRICTED} data.
     */
    private String[] mUnrestrictedPackages;

    public static synchronized ContactsDatabaseHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new ContactsDatabaseHelper(context);
        }
        return sSingleton;
    }

    /**
     * Private constructor, callers except unit tests should obtain an instance through
     * {@link #getInstance(android.content.Context)} instead.
     */
    ContactsDatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        if (false) Log.i(TAG, "Creating OpenHelper");
        Resources resources = context.getResources();

        mContext = context;
        mSyncState = new SyncStateContentProviderHelper();
        mUseStrictPhoneNumberComparison =
                resources.getBoolean(
                        com.android.internal.R.bool.config_use_strict_phone_number_comparation);
        int resourceId = resources.getIdentifier("unrestricted_packages", "array",
                context.getPackageName());
        if (resourceId != 0) {
            mUnrestrictedPackages = resources.getStringArray(resourceId);
        } else {
            mUnrestrictedPackages = new String[0];
        }
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        mSyncState.onDatabaseOpened(db);

        // Create compiled statements for package and mimetype lookups
        mMimetypeQuery = db.compileStatement("SELECT " + MimetypesColumns._ID + " FROM "
                + Tables.MIMETYPES + " WHERE " + MimetypesColumns.MIMETYPE + "=?");
        mPackageQuery = db.compileStatement("SELECT " + PackagesColumns._ID + " FROM "
                + Tables.PACKAGES + " WHERE " + PackagesColumns.PACKAGE + "=?");
        mContactIdQuery = db.compileStatement("SELECT " + RawContacts.CONTACT_ID + " FROM "
                + Tables.RAW_CONTACTS + " WHERE " + RawContacts._ID + "=?");
        mAggregationModeQuery = db.compileStatement("SELECT " + RawContacts.AGGREGATION_MODE
                + " FROM " + Tables.RAW_CONTACTS + " WHERE " + RawContacts._ID + "=?");
        mMimetypeInsert = db.compileStatement("INSERT INTO " + Tables.MIMETYPES + "("
                + MimetypesColumns.MIMETYPE + ") VALUES (?)");
        mPackageInsert = db.compileStatement("INSERT INTO " + Tables.PACKAGES + "("
                + PackagesColumns.PACKAGE + ") VALUES (?)");

        mDataMimetypeQuery = db.compileStatement("SELECT " + MimetypesColumns.MIMETYPE + " FROM "
                + Tables.DATA_JOIN_MIMETYPES + " WHERE " + Tables.DATA + "." + Data._ID + "=?");
        mActivitiesMimetypeQuery = db.compileStatement("SELECT " + MimetypesColumns.MIMETYPE
                + " FROM " + Tables.ACTIVITIES_JOIN_MIMETYPES + " WHERE " + Tables.ACTIVITIES + "."
                + Activities._ID + "=?");

        // Change visibility of a specific contact
        mVisibleSpecificUpdate = db.compileStatement(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.IN_VISIBLE_GROUP + "=(" + Clauses.CONTACT_IS_VISIBLE + ")" +
                " WHERE " + ContactsColumns.CONCRETE_ID + "=?");

        // Return visibility of the aggregate contact joined with the raw contact
        String contactVisibility =
                "SELECT " + Contacts.IN_VISIBLE_GROUP +
                " FROM " + Tables.CONTACTS +
                " WHERE " + Contacts._ID + "=" + RawContacts.CONTACT_ID;

        // Set visibility of raw contacts to the visibility of corresponding aggregate contacts
        mVisibleUpdateRawContacts = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "=(CASE WHEN ("
                        + contactVisibility + ")=1 THEN 1 ELSE 0 END)" +
                " WHERE " + RawContacts.DELETED + "=0" +
                " AND " + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "!=("
                        + contactVisibility + ")=1");

        // Set visibility of a raw contact to the visibility of corresponding aggregate contact
        mVisibleSpecificUpdateRawContacts = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "=("
                        + contactVisibility + ")" +
                " WHERE " + RawContacts.DELETED + "=0 AND " + RawContacts.CONTACT_ID + "=?");

        db.execSQL("ATTACH DATABASE ':memory:' AS " + DATABASE_PRESENCE + ";");
        db.execSQL("CREATE TABLE IF NOT EXISTS " + DATABASE_PRESENCE + "." + Tables.PRESENCE + " ("+
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

        db.execSQL("CREATE INDEX IF NOT EXISTS " + DATABASE_PRESENCE + ".presenceIndex" + " ON "
                + Tables.PRESENCE + " (" + PresenceColumns.RAW_CONTACT_ID + ");");
        db.execSQL("CREATE INDEX IF NOT EXISTS " + DATABASE_PRESENCE + ".presenceIndex2" + " ON "
                + Tables.PRESENCE + " (" + PresenceColumns.CONTACT_ID + ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS "
                + DATABASE_PRESENCE + "." + Tables.AGGREGATED_PRESENCE + " ("+
                AggregatedPresenceColumns.CONTACT_ID
                        + " INTEGER PRIMARY KEY REFERENCES contacts(_id)," +
                StatusUpdates.PRESENCE_STATUS + " INTEGER," +
                StatusUpdates.CHAT_CAPABILITY + " INTEGER NOT NULL DEFAULT 0" +
        ");");


        db.execSQL("CREATE TRIGGER " + DATABASE_PRESENCE + "." + Tables.PRESENCE + "_deleted"
                + " BEFORE DELETE ON " + DATABASE_PRESENCE + "." + Tables.PRESENCE
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

        db.execSQL("CREATE TRIGGER " + DATABASE_PRESENCE + "." + Tables.PRESENCE + "_inserted"
                + " AFTER INSERT ON " + DATABASE_PRESENCE + "." + Tables.PRESENCE
                + " BEGIN "
                + replaceAggregatePresenceSql
                + " END");

        db.execSQL("CREATE TRIGGER " + DATABASE_PRESENCE + "." + Tables.PRESENCE + "_updated"
                + " AFTER UPDATE ON " + DATABASE_PRESENCE + "." + Tables.PRESENCE
                + " BEGIN "
                + replaceAggregatePresenceSql
                + " END");
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        Log.i(TAG, "Bootstrapping database");

        mSyncState.createDatabase(db);

        // One row per group of contacts corresponding to the same person
        db.execSQL("CREATE TABLE " + Tables.CONTACTS + " (" +
                BaseColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Contacts.NAME_RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)," +
                Contacts.PHOTO_ID + " INTEGER REFERENCES data(_id)," +
                Contacts.CUSTOM_RINGTONE + " TEXT," +
                Contacts.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.LAST_TIME_CONTACTED + " INTEGER," +
                Contacts.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.IN_VISIBLE_GROUP + " INTEGER NOT NULL DEFAULT 1," +
                Contacts.HAS_PHONE_NUMBER + " INTEGER NOT NULL DEFAULT 0," +
                Contacts.LOOKUP_KEY + " TEXT," +
                ContactsColumns.LAST_STATUS_UPDATE_ID + " INTEGER REFERENCES data(_id)," +
                ContactsColumns.SINGLE_IS_RESTRICTED + " INTEGER NOT NULL DEFAULT 0" +
        ");");

        db.execSQL("CREATE INDEX contacts_visible_index ON " + Tables.CONTACTS + " (" +
                Contacts.IN_VISIBLE_GROUP +
        ");");

        db.execSQL("CREATE INDEX contacts_has_phone_index ON " + Tables.CONTACTS + " (" +
                Contacts.HAS_PHONE_NUMBER +
        ");");

        db.execSQL("CREATE INDEX contacts_restricted_index ON " + Tables.CONTACTS + " (" +
                ContactsColumns.SINGLE_IS_RESTRICTED +
        ");");

        db.execSQL("CREATE INDEX contacts_name_raw_contact_id_index ON " + Tables.CONTACTS + " (" +
                Contacts.NAME_RAW_CONTACT_ID +
        ");");

        // Contacts table
        db.execSQL("CREATE TABLE " + Tables.RAW_CONTACTS + " (" +
                RawContacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                RawContacts.IS_RESTRICTED + " INTEGER DEFAULT 0," +
                RawContacts.ACCOUNT_NAME + " STRING DEFAULT NULL, " +
                RawContacts.ACCOUNT_TYPE + " STRING DEFAULT NULL, " +
                RawContacts.SOURCE_ID + " TEXT," +
                RawContacts.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.CONTACT_ID + " INTEGER REFERENCES contacts(_id)," +
                RawContacts.AGGREGATION_MODE + " INTEGER NOT NULL DEFAULT " +
                        RawContacts.AGGREGATION_MODE_DEFAULT + "," +
                RawContactsColumns.AGGREGATION_NEEDED + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.CUSTOM_RINGTONE + " TEXT," +
                RawContacts.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.LAST_TIME_CONTACTED + " INTEGER," +
                RawContacts.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.DISPLAY_NAME_PRIMARY + " TEXT," +
                RawContacts.DISPLAY_NAME_ALTERNATIVE + " TEXT," +
                RawContacts.DISPLAY_NAME_SOURCE + " INTEGER NOT NULL DEFAULT " +
                        DisplayNameSources.UNDEFINED + "," +
                RawContacts.PHONETIC_NAME + " TEXT," +
                RawContacts.PHONETIC_NAME_STYLE + " TEXT," +
                RawContacts.SORT_KEY_PRIMARY + " TEXT COLLATE " +
                        ContactsProvider2.PHONEBOOK_COLLATOR_NAME + "," +
                RawContacts.SORT_KEY_ALTERNATIVE + " TEXT COLLATE " +
                        ContactsProvider2.PHONEBOOK_COLLATOR_NAME + "," +
                RawContacts.NAME_VERIFIED + " INTEGER NOT NULL DEFAULT 0," +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.SYNC1 + " TEXT, " +
                RawContacts.SYNC2 + " TEXT, " +
                RawContacts.SYNC3 + " TEXT, " +
                RawContacts.SYNC4 + " TEXT " +
        ");");

        db.execSQL("CREATE INDEX raw_contacts_contact_id_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContacts.CONTACT_ID +
        ");");

        db.execSQL("CREATE INDEX raw_contacts_source_id_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContacts.SOURCE_ID + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                RawContacts.ACCOUNT_NAME +
        ");");

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
                Data.SYNC4 + " TEXT " +
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

        // Private phone numbers table used for lookup
        db.execSQL("CREATE TABLE " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.DATA_ID
                        + " INTEGER PRIMARY KEY REFERENCES data(_id) NOT NULL," +
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

        // Private name/nickname table used for lookup
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

        // Groups table
        db.execSQL("CREATE TABLE " + Tables.GROUPS + " (" +
                Groups._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                GroupsColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                Groups.ACCOUNT_NAME + " STRING DEFAULT NULL, " +
                Groups.ACCOUNT_TYPE + " STRING DEFAULT NULL, " +
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
                Groups.SYNC1 + " TEXT, " +
                Groups.SYNC2 + " TEXT, " +
                Groups.SYNC3 + " TEXT, " +
                Groups.SYNC4 + " TEXT " +
        ");");

        db.execSQL("CREATE INDEX groups_source_id_index ON " + Tables.GROUPS + " (" +
                Groups.SOURCE_ID + ", " +
                Groups.ACCOUNT_TYPE + ", " +
                Groups.ACCOUNT_NAME +
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
                Settings.UNGROUPED_VISIBLE + " INTEGER NOT NULL DEFAULT 0," +
                Settings.SHOULD_SYNC + " INTEGER NOT NULL DEFAULT 1, " +
                "PRIMARY KEY (" + Settings.ACCOUNT_NAME + ", " +
                    Settings.ACCOUNT_TYPE + ") ON CONFLICT REPLACE" +
        ");");

        // The table for recent calls is here so we can do table joins
        // on people, phones, and calls all in one place.
        db.execSQL("CREATE TABLE " + Tables.CALLS + " (" +
                Calls._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Calls.NUMBER + " TEXT," +
                Calls.DATE + " INTEGER," +
                Calls.DURATION + " INTEGER," +
                Calls.TYPE + " INTEGER," +
                Calls.NEW + " INTEGER," +
                Calls.CACHED_NAME + " TEXT," +
                Calls.CACHED_NUMBER_TYPE + " INTEGER," +
                Calls.CACHED_NUMBER_LABEL + " TEXT" +
        ");");

        // Activities table
        db.execSQL("CREATE TABLE " + Tables.ACTIVITIES + " (" +
                Activities._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                ActivitiesColumns.PACKAGE_ID + " INTEGER REFERENCES package(_id)," +
                ActivitiesColumns.MIMETYPE_ID + " INTEGER REFERENCES mimetype(_id) NOT NULL," +
                Activities.RAW_ID + " TEXT," +
                Activities.IN_REPLY_TO + " TEXT," +
                Activities.AUTHOR_CONTACT_ID +  " INTEGER REFERENCES raw_contacts(_id)," +
                Activities.TARGET_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)," +
                Activities.PUBLISHED + " INTEGER NOT NULL," +
                Activities.THREAD_PUBLISHED + " INTEGER NOT NULL," +
                Activities.TITLE + " TEXT NOT NULL," +
                Activities.SUMMARY + " TEXT," +
                Activities.LINK + " TEXT, " +
                Activities.THUMBNAIL + " BLOB" +
        ");");

        db.execSQL("CREATE TABLE " + Tables.STATUS_UPDATES + " (" +
                StatusUpdatesColumns.DATA_ID + " INTEGER PRIMARY KEY REFERENCES data(_id)," +
                StatusUpdates.STATUS + " TEXT," +
                StatusUpdates.STATUS_TIMESTAMP + " INTEGER," +
                StatusUpdates.STATUS_RES_PACKAGE + " TEXT, " +
                StatusUpdates.STATUS_LABEL + " INTEGER, " +
                StatusUpdates.STATUS_ICON + " INTEGER" +
        ");");

        db.execSQL("CREATE TABLE " + Tables.PROPERTIES + " (" +
                PropertiesColumns.PROPERTY_KEY + " TEXT PRIMARY KEY, " +
                PropertiesColumns.PROPERTY_VALUE + " TEXT " +
        ");");

        db.execSQL("CREATE TABLE " + Tables.ACCOUNTS + " (" +
                RawContacts.ACCOUNT_NAME + " TEXT, " +
                RawContacts.ACCOUNT_TYPE + " TEXT " +
        ");");

        // Allow contacts without any account to be created for now.  Achieve that
        // by inserting a fake account with both type and name as NULL.
        // This "account" should be eliminated as soon as the first real writable account
        // is added to the phone.
        db.execSQL("INSERT INTO accounts VALUES(NULL, NULL)");

        createContactsViews(db);
        createGroupsView(db);
        createContactEntitiesView(db);
        createContactsTriggers(db);
        createContactsIndexes(db);

        loadNicknameLookupTable(db);

        // Add the legacy API support views, etc
        LegacyApiSupport.createDatabase(db);

        // This will create a sqlite_stat1 table that is used for query optimization
        db.execSQL("ANALYZE;");

        updateSqliteStats(db);

        // We need to close and reopen the database connection so that the stats are
        // taken into account. Make a note of it and do the actual reopening in the
        // getWritableDatabase method.
        mReopenDatabase = true;

        ContentResolver.requestSync(null /* all accounts */,
                ContactsContract.AUTHORITY, new Bundle());
    }

    private static void createContactsTriggers(SQLiteDatabase db) {

        /*
         * Automatically delete Data rows when a raw contact is deleted.
         */
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
                + "   DELETE FROM " + Tables.CONTACTS
                + "     WHERE " + Contacts._ID + "=OLD." + RawContacts.CONTACT_ID
                + "       AND (SELECT COUNT(*) FROM " + Tables.RAW_CONTACTS
                + "            WHERE " + RawContacts.CONTACT_ID + "=OLD." + RawContacts.CONTACT_ID
                + "           )=1;"
                + " END");


        db.execSQL("DROP TRIGGER IF EXISTS contacts_times_contacted;");
        db.execSQL("DROP TRIGGER IF EXISTS raw_contacts_times_contacted;");

        /*
         * Triggers that update {@link RawContacts#VERSION} when the contact is
         * marked for deletion or any time a data row is inserted, updated or
         * deleted.
         */
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
    }

    private static void createContactsIndexes(SQLiteDatabase db) {
        db.execSQL("DROP INDEX IF EXISTS name_lookup_index");
        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + "," +
                NameLookupColumns.NAME_TYPE + ", " +
                NameLookupColumns.RAW_CONTACT_ID + ", " +
                NameLookupColumns.DATA_ID +
        ");");

        db.execSQL("DROP INDEX IF EXISTS raw_contact_sort_key1_index");
        db.execSQL("CREATE INDEX raw_contact_sort_key1_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "," +
                RawContacts.SORT_KEY_PRIMARY +
        ");");

        db.execSQL("DROP INDEX IF EXISTS raw_contact_sort_key2_index");
        db.execSQL("CREATE INDEX raw_contact_sort_key2_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "," +
                RawContacts.SORT_KEY_ALTERNATIVE +
        ");");
    }

    private static void createContactsViews(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Views.CONTACTS_ALL + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.CONTACTS_RESTRICTED + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.DATA_ALL + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.DATA_RESTRICTED + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.RAW_CONTACTS_ALL + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Views.RAW_CONTACTS_RESTRICTED + ";");

        String dataColumns =
                Data.IS_PRIMARY + ", "
                + Data.IS_SUPER_PRIMARY + ", "
                + Data.DATA_VERSION + ", "
                + PackagesColumns.PACKAGE + " AS " + Data.RES_PACKAGE + ","
                + MimetypesColumns.MIMETYPE + " AS " + Data.MIMETYPE + ", "
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
                + Data.SYNC1 + ", "
                + Data.SYNC2 + ", "
                + Data.SYNC3 + ", "
                + Data.SYNC4;

        String syncColumns =
                RawContactsColumns.CONCRETE_ACCOUNT_NAME + " AS " + RawContacts.ACCOUNT_NAME + ","
                + RawContactsColumns.CONCRETE_ACCOUNT_TYPE + " AS " + RawContacts.ACCOUNT_TYPE + ","
                + RawContactsColumns.CONCRETE_SOURCE_ID + " AS " + RawContacts.SOURCE_ID + ","
                + RawContactsColumns.CONCRETE_NAME_VERIFIED + " AS " + RawContacts.NAME_VERIFIED + ","
                + RawContactsColumns.CONCRETE_VERSION + " AS " + RawContacts.VERSION + ","
                + RawContactsColumns.CONCRETE_DIRTY + " AS " + RawContacts.DIRTY + ","
                + RawContactsColumns.CONCRETE_SYNC1 + " AS " + RawContacts.SYNC1 + ","
                + RawContactsColumns.CONCRETE_SYNC2 + " AS " + RawContacts.SYNC2 + ","
                + RawContactsColumns.CONCRETE_SYNC3 + " AS " + RawContacts.SYNC3 + ","
                + RawContactsColumns.CONCRETE_SYNC4 + " AS " + RawContacts.SYNC4;

        String contactOptionColumns =
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE
                        + " AS " + RawContacts.CUSTOM_RINGTONE + ","
                + ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                        + " AS " + RawContacts.SEND_TO_VOICEMAIL + ","
                + ContactsColumns.CONCRETE_LAST_TIME_CONTACTED
                        + " AS " + RawContacts.LAST_TIME_CONTACTED + ","
                + ContactsColumns.CONCRETE_TIMES_CONTACTED
                        + " AS " + RawContacts.TIMES_CONTACTED + ","
                + ContactsColumns.CONCRETE_STARRED
                        + " AS " + RawContacts.STARRED;

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
                + "name_raw_contact." + RawContacts.SORT_KEY_ALTERNATIVE
                        + " AS " + Contacts.SORT_KEY_ALTERNATIVE + ", "
                + "name_raw_contact." + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP
                        + " AS " + Contacts.IN_VISIBLE_GROUP;

        String dataSelect = "SELECT "
                + DataColumns.CONCRETE_ID + " AS " + Data._ID + ","
                + Data.RAW_CONTACT_ID + ", "
                + RawContactsColumns.CONCRETE_CONTACT_ID + " AS " + RawContacts.CONTACT_ID + ", "
                + syncColumns + ", "
                + dataColumns + ", "
                + contactOptionColumns + ", "
                + contactNameColumns + ", "
                + Contacts.LOOKUP_KEY + ", "
                + Contacts.PHOTO_ID + ", "
                + Contacts.NAME_RAW_CONTACT_ID + ","
                + ContactsColumns.LAST_STATUS_UPDATE_ID + ", "
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.DATA
                + " JOIN " + Tables.MIMETYPES + " ON ("
                +   DataColumns.CONCRETE_MIMETYPE_ID + "=" + MimetypesColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS + " ON ("
                +   DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID + ")"
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

        db.execSQL("CREATE VIEW " + Views.DATA_ALL + " AS " + dataSelect);
        db.execSQL("CREATE VIEW " + Views.DATA_RESTRICTED + " AS " + dataSelect + " WHERE "
                + RawContactsColumns.CONCRETE_IS_RESTRICTED + "=0");

        String rawContactOptionColumns =
                RawContacts.CUSTOM_RINGTONE + ","
                + RawContacts.SEND_TO_VOICEMAIL + ","
                + RawContacts.LAST_TIME_CONTACTED + ","
                + RawContacts.TIMES_CONTACTED + ","
                + RawContacts.STARRED;

        String rawContactsSelect = "SELECT "
                + RawContactsColumns.CONCRETE_ID + " AS " + RawContacts._ID + ","
                + RawContacts.CONTACT_ID + ", "
                + RawContacts.AGGREGATION_MODE + ", "
                + RawContacts.DELETED + ", "
                + RawContacts.DISPLAY_NAME_SOURCE  + ", "
                + RawContacts.DISPLAY_NAME_PRIMARY  + ", "
                + RawContacts.DISPLAY_NAME_ALTERNATIVE  + ", "
                + RawContacts.PHONETIC_NAME  + ", "
                + RawContacts.PHONETIC_NAME_STYLE  + ", "
                + RawContacts.SORT_KEY_PRIMARY  + ", "
                + RawContacts.SORT_KEY_ALTERNATIVE + ", "
                + rawContactOptionColumns + ", "
                + syncColumns
                + " FROM " + Tables.RAW_CONTACTS;

        db.execSQL("CREATE VIEW " + Views.RAW_CONTACTS_ALL + " AS " + rawContactsSelect);
        db.execSQL("CREATE VIEW " + Views.RAW_CONTACTS_RESTRICTED + " AS " + rawContactsSelect
                + " WHERE " + RawContacts.IS_RESTRICTED + "=0");

        String contactsColumns =
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE
                        + " AS " + Contacts.CUSTOM_RINGTONE + ", "
                + contactNameColumns + ", "
                + Contacts.HAS_PHONE_NUMBER + ", "
                + Contacts.LOOKUP_KEY + ", "
                + Contacts.PHOTO_ID + ", "
                + ContactsColumns.CONCRETE_LAST_TIME_CONTACTED
                        + " AS " + Contacts.LAST_TIME_CONTACTED + ", "
                + ContactsColumns.CONCRETE_SEND_TO_VOICEMAIL
                        + " AS " + Contacts.SEND_TO_VOICEMAIL + ", "
                + ContactsColumns.CONCRETE_STARRED
                        + " AS " + Contacts.STARRED + ", "
                + ContactsColumns.CONCRETE_TIMES_CONTACTED
                        + " AS " + Contacts.TIMES_CONTACTED + ", "
                + ContactsColumns.LAST_STATUS_UPDATE_ID;

        String contactsSelect = "SELECT "
                + ContactsColumns.CONCRETE_ID + " AS " + Contacts._ID + ","
                + contactsColumns
                + " FROM " + Tables.CONTACTS
                + " JOIN " + Tables.RAW_CONTACTS + " AS name_raw_contact ON("
                +   Contacts.NAME_RAW_CONTACT_ID + "=name_raw_contact." + RawContacts._ID + ")";

        db.execSQL("CREATE VIEW " + Views.CONTACTS_ALL + " AS " + contactsSelect);
        db.execSQL("CREATE VIEW " + Views.CONTACTS_RESTRICTED + " AS " + contactsSelect
                + " WHERE " + ContactsColumns.SINGLE_IS_RESTRICTED + "=0");
    }

    private static void createGroupsView(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Views.GROUPS_ALL + ";");
        String groupsColumns =
                Groups.ACCOUNT_NAME + ","
                + Groups.ACCOUNT_TYPE + ","
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
                + Groups.SYNC1 + ","
                + Groups.SYNC2 + ","
                + Groups.SYNC3 + ","
                + Groups.SYNC4 + ","
                + PackagesColumns.PACKAGE + " AS " + Groups.RES_PACKAGE;

        String groupsSelect = "SELECT "
                + GroupsColumns.CONCRETE_ID + " AS " + Groups._ID + ","
                + groupsColumns
                + " FROM " + Tables.GROUPS_JOIN_PACKAGES;

        db.execSQL("CREATE VIEW " + Views.GROUPS_ALL + " AS " + groupsSelect);
    }

    private static void createContactEntitiesView(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Tables.CONTACT_ENTITIES + ";");
        db.execSQL("DROP VIEW IF EXISTS " + Tables.CONTACT_ENTITIES_RESTRICTED + ";");

        String contactEntitiesSelect = "SELECT "
                + RawContactsColumns.CONCRETE_ACCOUNT_NAME + " AS " + RawContacts.ACCOUNT_NAME + ","
                + RawContactsColumns.CONCRETE_ACCOUNT_TYPE + " AS " + RawContacts.ACCOUNT_TYPE + ","
                + RawContactsColumns.CONCRETE_SOURCE_ID + " AS " + RawContacts.SOURCE_ID + ","
                + RawContactsColumns.CONCRETE_VERSION + " AS " + RawContacts.VERSION + ","
                + RawContactsColumns.CONCRETE_DIRTY + " AS " + RawContacts.DIRTY + ","
                + RawContactsColumns.CONCRETE_DELETED + " AS " + RawContacts.DELETED + ","
                + RawContactsColumns.CONCRETE_NAME_VERIFIED + " AS " + RawContacts.NAME_VERIFIED + ","
                + PackagesColumns.PACKAGE + " AS " + Data.RES_PACKAGE + ","
                + RawContacts.CONTACT_ID + ", "
                + RawContactsColumns.CONCRETE_SYNC1 + " AS " + RawContacts.SYNC1 + ", "
                + RawContactsColumns.CONCRETE_SYNC2 + " AS " + RawContacts.SYNC2 + ", "
                + RawContactsColumns.CONCRETE_SYNC3 + " AS " + RawContacts.SYNC3 + ", "
                + RawContactsColumns.CONCRETE_SYNC4 + " AS " + RawContacts.SYNC4 + ", "
                + Data.MIMETYPE + ", "
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
                + Data.SYNC1 + ", "
                + Data.SYNC2 + ", "
                + Data.SYNC3 + ", "
                + Data.SYNC4 + ", "
                + RawContactsColumns.CONCRETE_ID + " AS " + RawContacts._ID + ", "
                + Data.IS_PRIMARY + ", "
                + Data.IS_SUPER_PRIMARY + ", "
                + Data.DATA_VERSION + ", "
                + DataColumns.CONCRETE_ID + " AS " + RawContacts.Entity.DATA_ID + ","
                + RawContactsColumns.CONCRETE_STARRED + " AS " + RawContacts.STARRED + ","
                + RawContactsColumns.CONCRETE_IS_RESTRICTED + " AS "
                        + RawContacts.IS_RESTRICTED + ","
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.RAW_CONTACTS
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

        db.execSQL("CREATE VIEW " + Tables.CONTACT_ENTITIES + " AS "
                + contactEntitiesSelect);
        db.execSQL("CREATE VIEW " + Tables.CONTACT_ENTITIES_RESTRICTED + " AS "
                + contactEntitiesSelect + " WHERE " + RawContacts.IS_RESTRICTED + "=0");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        if (oldVersion < 99) {
            Log.i(TAG, "Upgrading from version " + oldVersion + " to " + newVersion
                    + ", data will be lost!");

            db.execSQL("DROP TABLE IF EXISTS " + Tables.CONTACTS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.RAW_CONTACTS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.PACKAGES + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.MIMETYPES + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.DATA + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.PHONE_LOOKUP + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.NAME_LOOKUP + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.NICKNAME_LOOKUP + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.GROUPS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.ACTIVITIES + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.CALLS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.SETTINGS + ";");
            db.execSQL("DROP TABLE IF EXISTS " + Tables.STATUS_UPDATES + ";");

            // TODO: we should not be dropping agg_exceptions and contact_options. In case that
            // table's schema changes, we should try to preserve the data, because it was entered
            // by the user and has never been synched to the server.
            db.execSQL("DROP TABLE IF EXISTS " + Tables.AGGREGATION_EXCEPTIONS + ";");

            onCreate(db);
            return;
        }

        Log.i(TAG, "Upgrading from version " + oldVersion + " to " + newVersion);

        boolean upgradeViewsAndTriggers = false;
        boolean upgradeNameLookup = false;

        if (oldVersion == 99) {
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 100) {
            db.execSQL("CREATE INDEX IF NOT EXISTS mimetypes_mimetype_index ON "
                    + Tables.MIMETYPES + " ("
                            + MimetypesColumns.MIMETYPE + ","
                            + MimetypesColumns._ID + ");");
            updateIndexStats(db, Tables.MIMETYPES,
                    "mimetypes_mimetype_index", "50 1 1");

            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 101) {
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 102) {
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 103) {
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 104 || oldVersion == 201) {
            LegacyApiSupport.createSettingsTable(db);
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 105) {
            upgradeToVersion202(db);
            upgradeNameLookup = true;
            oldVersion = 202;
        }

        if (oldVersion == 202) {
            upgradeToVersion203(db);
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 203) {
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 204) {
            upgradeToVersion205(db);
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 205) {
            upgrateToVersion206(db);
            upgradeViewsAndTriggers = true;
            oldVersion++;
        }

        if (oldVersion == 206) {
            upgradeToVersion300(db);
            oldVersion = 300;
        }

        if (oldVersion == 300) {
            upgradeViewsAndTriggers = true;
            oldVersion = 301;
        }

        if (oldVersion == 301) {
            upgradeViewsAndTriggers = true;
            oldVersion = 302;
        }

        if (oldVersion == 302) {
            upgradeEmailToVersion303(db);
            upgradeNicknameToVersion303(db);
            oldVersion = 303;
        }

        if (oldVersion == 303) {
            upgradeToVersion304(db);
            oldVersion = 304;
        }

        if (oldVersion == 304) {
            upgradeNameLookup = true;
            oldVersion = 305;
        }

        if (oldVersion == 305) {
            upgradeToVersion306(db);
            oldVersion = 306;
        }

        if (oldVersion == 306) {
            upgradeToVersion307(db);
            oldVersion = 307;
        }

        if (oldVersion == 307) {
            upgradeToVersion308(db);
            oldVersion = 308;
        }

        // Gingerbread upgrades
        if (oldVersion < 350) {
            upgradeViewsAndTriggers = true;
            oldVersion = 351;
        }

        if (oldVersion == 351) {
            upgradeNameLookup = true;
            oldVersion = 352;
        }

        if (oldVersion == 352) {
            upgradeToVersion353(db);
            oldVersion = 353;
        }

        if (upgradeViewsAndTriggers) {
            createContactsViews(db);
            createGroupsView(db);
            createContactEntitiesView(db);
            createContactsTriggers(db);
            createContactsIndexes(db);
            LegacyApiSupport.createViews(db);
            updateSqliteStats(db);
            mReopenDatabase = true;
        }

        if (upgradeNameLookup) {
            rebuildNameLookup(db);
        }

        if (oldVersion != newVersion) {
            throw new IllegalStateException(
                    "error upgrading the database to version " + newVersion);
        }
    }

    private void upgradeToVersion202(SQLiteDatabase db) {
        db.execSQL(
                "ALTER TABLE " + Tables.PHONE_LOOKUP +
                " ADD " + PhoneLookupColumns.MIN_MATCH + " TEXT;");

        db.execSQL("CREATE INDEX phone_lookup_min_match_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.MIN_MATCH + "," +
                PhoneLookupColumns.RAW_CONTACT_ID + "," +
                PhoneLookupColumns.DATA_ID +
        ");");

        updateIndexStats(db, Tables.PHONE_LOOKUP,
                "phone_lookup_min_match_index", "10000 2 2 1");

        SQLiteStatement update = db.compileStatement(
                "UPDATE " + Tables.PHONE_LOOKUP +
                " SET " + PhoneLookupColumns.MIN_MATCH + "=?" +
                " WHERE " + PhoneLookupColumns.DATA_ID + "=?");

        // Populate the new column
        Cursor c = db.query(Tables.PHONE_LOOKUP + " JOIN " + Tables.DATA +
                " ON (" + PhoneLookupColumns.DATA_ID + "=" + DataColumns.CONCRETE_ID + ")",
                new String[]{Data._ID, Phone.NUMBER}, null, null, null, null, null);
        try {
            while (c.moveToNext()) {
                long dataId = c.getLong(0);
                String number = c.getString(1);
                if (!TextUtils.isEmpty(number)) {
                    update.bindString(1, PhoneNumberUtils.toCallerIDMinMatch(number));
                    update.bindLong(2, dataId);
                    update.execute();
                }
            }
        } finally {
            c.close();
        }
    }

    private void upgradeToVersion203(SQLiteDatabase db) {
        // Garbage-collect first. A bug in Eclair was sometimes leaving
        // raw_contacts in the database that no longer had contacts associated
        // with them.  To avoid failures during this database upgrade, drop
        // the orphaned raw_contacts.
        db.execSQL(
                "DELETE FROM raw_contacts" +
                " WHERE contact_id NOT NULL" +
                " AND contact_id NOT IN (SELECT _id FROM contacts)");

        db.execSQL(
                "ALTER TABLE " + Tables.CONTACTS +
                " ADD " + Contacts.NAME_RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)");
        db.execSQL(
                "ALTER TABLE " + Tables.RAW_CONTACTS +
                " ADD " + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP
                        + " INTEGER NOT NULL DEFAULT 0");

        // For each Contact, find the RawContact that contributed the display name
        db.execSQL(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.NAME_RAW_CONTACT_ID + "=(" +
                        " SELECT " + RawContacts._ID +
                        " FROM " + Tables.RAW_CONTACTS +
                        " WHERE " + RawContacts.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID +
                        " AND " + RawContactsColumns.CONCRETE_DISPLAY_NAME + "=" +
                                Tables.CONTACTS + "." + Contacts.DISPLAY_NAME +
                        " ORDER BY " + RawContacts._ID +
                        " LIMIT 1)"
        );

        db.execSQL("CREATE INDEX contacts_name_raw_contact_id_index ON " + Tables.CONTACTS + " (" +
                Contacts.NAME_RAW_CONTACT_ID +
        ");");

        // If for some unknown reason we missed some names, let's make sure there are
        // no contacts without a name, picking a raw contact "at random".
        db.execSQL(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.NAME_RAW_CONTACT_ID + "=(" +
                        " SELECT " + RawContacts._ID +
                        " FROM " + Tables.RAW_CONTACTS +
                        " WHERE " + RawContacts.CONTACT_ID + "=" + ContactsColumns.CONCRETE_ID +
                        " ORDER BY " + RawContacts._ID +
                        " LIMIT 1)" +
                " WHERE " + Contacts.NAME_RAW_CONTACT_ID + " IS NULL"
        );

        // Wipe out DISPLAY_NAME on the Contacts table as it is no longer in use.
        db.execSQL(
                "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.DISPLAY_NAME + "=NULL"
        );

        // Copy the IN_VISIBLE_GROUP flag down to all raw contacts to allow
        // indexing on (display_name, in_visible_group)
        db.execSQL(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " + RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "=(" +
                        "SELECT " + Contacts.IN_VISIBLE_GROUP +
                        " FROM " + Tables.CONTACTS +
                        " WHERE " + Contacts._ID + "=" + RawContacts.CONTACT_ID + ")" +
                " WHERE " + RawContacts.CONTACT_ID + " NOT NULL"
        );

        db.execSQL("CREATE INDEX raw_contact_sort_key1_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "," +
                RawContactsColumns.DISPLAY_NAME + " COLLATE LOCALIZED ASC" +
        ");");

        db.execSQL("DROP INDEX contacts_visible_index");
        db.execSQL("CREATE INDEX contacts_visible_index ON " + Tables.CONTACTS + " (" +
                Contacts.IN_VISIBLE_GROUP +
        ");");
    }

    private void upgradeToVersion205(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.DISPLAY_NAME_ALTERNATIVE + " TEXT;");
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.PHONETIC_NAME + " TEXT;");
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.PHONETIC_NAME_STYLE + " INTEGER;");
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.SORT_KEY_PRIMARY
                + " TEXT COLLATE " + ContactsProvider2.PHONEBOOK_COLLATOR_NAME + ";");
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.SORT_KEY_ALTERNATIVE
                + " TEXT COLLATE " + ContactsProvider2.PHONEBOOK_COLLATOR_NAME + ";");

        final Locale locale = Locale.getDefault();

        NameSplitter splitter = createNameSplitter();

        SQLiteStatement rawContactUpdate = db.compileStatement(
                "UPDATE " + Tables.RAW_CONTACTS +
                " SET " +
                        RawContacts.DISPLAY_NAME_PRIMARY + "=?," +
                        RawContacts.DISPLAY_NAME_ALTERNATIVE + "=?," +
                        RawContacts.PHONETIC_NAME + "=?," +
                        RawContacts.PHONETIC_NAME_STYLE + "=?," +
                        RawContacts.SORT_KEY_PRIMARY + "=?," +
                        RawContacts.SORT_KEY_ALTERNATIVE + "=?" +
                " WHERE " + RawContacts._ID + "=?");

        upgradeStructuredNamesToVersion205(db, rawContactUpdate, splitter);
        upgradeOrganizationsToVersion205(db, rawContactUpdate, splitter);

        db.execSQL("DROP INDEX raw_contact_sort_key1_index");
        db.execSQL("CREATE INDEX raw_contact_sort_key1_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "," +
                RawContacts.SORT_KEY_PRIMARY +
        ");");

        db.execSQL("CREATE INDEX raw_contact_sort_key2_index ON " + Tables.RAW_CONTACTS + " (" +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP + "," +
                RawContacts.SORT_KEY_ALTERNATIVE +
        ");");
    }

    private interface StructName205Query {
        String TABLE = Tables.DATA_JOIN_RAW_CONTACTS;

        String COLUMNS[] = {
                DataColumns.CONCRETE_ID,
                Data.RAW_CONTACT_ID,
                RawContacts.DISPLAY_NAME_SOURCE,
                RawContacts.DISPLAY_NAME_PRIMARY,
                StructuredName.PREFIX,
                StructuredName.GIVEN_NAME,
                StructuredName.MIDDLE_NAME,
                StructuredName.FAMILY_NAME,
                StructuredName.SUFFIX,
                StructuredName.PHONETIC_FAMILY_NAME,
                StructuredName.PHONETIC_MIDDLE_NAME,
                StructuredName.PHONETIC_GIVEN_NAME,
        };

        int ID = 0;
        int RAW_CONTACT_ID = 1;
        int DISPLAY_NAME_SOURCE = 2;
        int DISPLAY_NAME = 3;
        int PREFIX = 4;
        int GIVEN_NAME = 5;
        int MIDDLE_NAME = 6;
        int FAMILY_NAME = 7;
        int SUFFIX = 8;
        int PHONETIC_FAMILY_NAME = 9;
        int PHONETIC_MIDDLE_NAME = 10;
        int PHONETIC_GIVEN_NAME = 11;
    }

    private void upgradeStructuredNamesToVersion205(SQLiteDatabase db,
            SQLiteStatement rawContactUpdate, NameSplitter splitter) {

        // Process structured names to detect the style of the full name and phonetic name

        long mMimeType;
        try {
            mMimeType = DatabaseUtils.longForQuery(db,
                    "SELECT " + MimetypesColumns._ID +
                    " FROM " + Tables.MIMETYPES +
                    " WHERE " + MimetypesColumns.MIMETYPE
                            + "='" + StructuredName.CONTENT_ITEM_TYPE + "'", null);
        } catch (SQLiteDoneException e) {
            // No structured names in the database
            return;
        }

        SQLiteStatement structuredNameUpdate = db.compileStatement(
                "UPDATE " + Tables.DATA +
                " SET " +
                        StructuredName.FULL_NAME_STYLE + "=?," +
                        StructuredName.DISPLAY_NAME + "=?," +
                        StructuredName.PHONETIC_NAME_STYLE + "=?" +
                " WHERE " + Data._ID + "=?");

        NameSplitter.Name name = new NameSplitter.Name();
        StringBuilder sb = new StringBuilder();
        Cursor cursor = db.query(StructName205Query.TABLE,
                StructName205Query.COLUMNS,
                DataColumns.MIMETYPE_ID + "=" + mMimeType, null, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(StructName205Query.ID);
                long rawContactId = cursor.getLong(StructName205Query.RAW_CONTACT_ID);
                int displayNameSource = cursor.getInt(StructName205Query.DISPLAY_NAME_SOURCE);
                String displayName = cursor.getString(StructName205Query.DISPLAY_NAME);

                name.clear();
                name.prefix = cursor.getString(StructName205Query.PREFIX);
                name.givenNames = cursor.getString(StructName205Query.GIVEN_NAME);
                name.middleName = cursor.getString(StructName205Query.MIDDLE_NAME);
                name.familyName = cursor.getString(StructName205Query.FAMILY_NAME);
                name.suffix = cursor.getString(StructName205Query.SUFFIX);
                name.phoneticFamilyName = cursor.getString(StructName205Query.PHONETIC_FAMILY_NAME);
                name.phoneticMiddleName = cursor.getString(StructName205Query.PHONETIC_MIDDLE_NAME);
                name.phoneticGivenName = cursor.getString(StructName205Query.PHONETIC_GIVEN_NAME);

                upgradeNameToVersion205(dataId, rawContactId, displayNameSource, displayName, name,
                        structuredNameUpdate, rawContactUpdate, splitter, sb);
            }
        } finally {
            cursor.close();
        }
    }

    private void upgradeNameToVersion205(long dataId, long rawContactId, int displayNameSource,
            String currentDisplayName, NameSplitter.Name name,
            SQLiteStatement structuredNameUpdate, SQLiteStatement rawContactUpdate,
            NameSplitter splitter, StringBuilder sb) {

        splitter.guessNameStyle(name);
        int unadjustedFullNameStyle = name.fullNameStyle;
        name.fullNameStyle = splitter.getAdjustedFullNameStyle(name.fullNameStyle);
        String displayName = splitter.join(name, true);

        // Don't update database with the adjusted fullNameStyle as it is locale
        // related
        structuredNameUpdate.bindLong(1, unadjustedFullNameStyle);
        DatabaseUtils.bindObjectToProgram(structuredNameUpdate, 2, displayName);
        structuredNameUpdate.bindLong(3, name.phoneticNameStyle);
        structuredNameUpdate.bindLong(4, dataId);
        structuredNameUpdate.execute();

        if (displayNameSource == DisplayNameSources.STRUCTURED_NAME) {
            String displayNameAlternative = splitter.join(name, false);
            String phoneticName = splitter.joinPhoneticName(name);
            String sortKey = null;
            String sortKeyAlternative = null;

            if (phoneticName != null) {
                sortKey = sortKeyAlternative = phoneticName;
            } else if (name.fullNameStyle == FullNameStyle.CHINESE ||
                    name.fullNameStyle == FullNameStyle.CJK) {
                sortKey = sortKeyAlternative = ContactLocaleUtils.getIntance()
                        .getSortKey(displayName, name.fullNameStyle);
            }

            if (sortKey == null) {
                sortKey = displayName;
                sortKeyAlternative = displayNameAlternative;
            }

            updateRawContact205(rawContactUpdate, rawContactId, displayName,
                    displayNameAlternative, name.phoneticNameStyle, phoneticName, sortKey,
                    sortKeyAlternative);
        }
    }

    private interface Organization205Query {
        String TABLE = Tables.DATA_JOIN_RAW_CONTACTS;

        String COLUMNS[] = {
                DataColumns.CONCRETE_ID,
                Data.RAW_CONTACT_ID,
                Organization.COMPANY,
                Organization.PHONETIC_NAME,
        };

        int ID = 0;
        int RAW_CONTACT_ID = 1;
        int COMPANY = 2;
        int PHONETIC_NAME = 3;
    }

    private void upgradeOrganizationsToVersion205(SQLiteDatabase db,
            SQLiteStatement rawContactUpdate, NameSplitter splitter) {
        final long mimeType = lookupMimeTypeId(db, Organization.CONTENT_ITEM_TYPE);

        SQLiteStatement organizationUpdate = db.compileStatement(
                "UPDATE " + Tables.DATA +
                " SET " +
                        Organization.PHONETIC_NAME_STYLE + "=?" +
                " WHERE " + Data._ID + "=?");

        Cursor cursor = db.query(Organization205Query.TABLE, Organization205Query.COLUMNS,
                DataColumns.MIMETYPE_ID + "=" + mimeType + " AND "
                        + RawContacts.DISPLAY_NAME_SOURCE + "=" + DisplayNameSources.ORGANIZATION,
                null, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(Organization205Query.ID);
                long rawContactId = cursor.getLong(Organization205Query.RAW_CONTACT_ID);
                String company = cursor.getString(Organization205Query.COMPANY);
                String phoneticName = cursor.getString(Organization205Query.PHONETIC_NAME);

                int phoneticNameStyle = splitter.guessPhoneticNameStyle(phoneticName);

                organizationUpdate.bindLong(1, phoneticNameStyle);
                organizationUpdate.bindLong(2, dataId);
                organizationUpdate.execute();

                String sortKey = null;
                if (phoneticName == null && company != null) {
                    int nameStyle = splitter.guessFullNameStyle(company);
                    nameStyle = splitter.getAdjustedFullNameStyle(nameStyle);
                    if (nameStyle == FullNameStyle.CHINESE ||
                            nameStyle == FullNameStyle.CJK ) {
                        sortKey = ContactLocaleUtils.getIntance()
                                .getSortKey(company, nameStyle);
                    }
                }

                if (sortKey == null) {
                    sortKey = company;
                }

                updateRawContact205(rawContactUpdate, rawContactId, company,
                        company, phoneticNameStyle, phoneticName, sortKey, sortKey);
            }
        } finally {
            cursor.close();
        }
    }

    private void updateRawContact205(SQLiteStatement rawContactUpdate, long rawContactId,
            String displayName, String displayNameAlternative, int phoneticNameStyle,
            String phoneticName, String sortKeyPrimary, String sortKeyAlternative) {
        bindString(rawContactUpdate, 1, displayName);
        bindString(rawContactUpdate, 2, displayNameAlternative);
        bindString(rawContactUpdate, 3, phoneticName);
        rawContactUpdate.bindLong(4, phoneticNameStyle);
        bindString(rawContactUpdate, 5, sortKeyPrimary);
        bindString(rawContactUpdate, 6, sortKeyAlternative);
        rawContactUpdate.bindLong(7, rawContactId);
        rawContactUpdate.execute();
    }

    private void upgrateToVersion206(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.RAW_CONTACTS
                + " ADD " + RawContacts.NAME_VERIFIED + " INTEGER NOT NULL DEFAULT 0;");
    }

    private interface Organization300Query {
        String TABLE = Tables.DATA;

        String SELECTION = DataColumns.MIMETYPE_ID + "=?";

        String COLUMNS[] = {
                Organization._ID,
                Organization.RAW_CONTACT_ID,
                Organization.COMPANY,
                Organization.TITLE
        };

        int ID = 0;
        int RAW_CONTACT_ID = 1;
        int COMPANY = 2;
        int TITLE = 3;
    }

    /**
     * Fix for the bug where name lookup records for organizations would get removed by
     * unrelated updates of the data rows.
     */
    private void upgradeToVersion300(SQLiteDatabase db) {
        final long mimeType = lookupMimeTypeId(db, Organization.CONTENT_ITEM_TYPE);
        if (mimeType == -1) {
            return;
        }

        ContentValues values = new ContentValues();

        // Find all data rows with the mime type "organization"
        Cursor cursor = db.query(Organization300Query.TABLE, Organization300Query.COLUMNS,
                Organization300Query.SELECTION, new String[] {String.valueOf(mimeType)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(Organization300Query.ID);
                long rawContactId = cursor.getLong(Organization300Query.RAW_CONTACT_ID);
                String company = cursor.getString(Organization300Query.COMPANY);
                String title = cursor.getString(Organization300Query.TITLE);

                // First delete name lookup if there is any (chances are there won't be)
                db.delete(Tables.NAME_LOOKUP, NameLookupColumns.DATA_ID + "=?",
                        new String[]{String.valueOf(dataId)});

                // Now insert two name lookup records: one for company name, one for title
                values.put(NameLookupColumns.DATA_ID, dataId);
                values.put(NameLookupColumns.RAW_CONTACT_ID, rawContactId);
                values.put(NameLookupColumns.NAME_TYPE, NameLookupType.ORGANIZATION);

                if (!TextUtils.isEmpty(company)) {
                    values.put(NameLookupColumns.NORMALIZED_NAME,
                            NameNormalizer.normalize(company));
                    db.insert(Tables.NAME_LOOKUP, null, values);
                }

                if (!TextUtils.isEmpty(title)) {
                    values.put(NameLookupColumns.NORMALIZED_NAME,
                            NameNormalizer.normalize(title));
                    db.insert(Tables.NAME_LOOKUP, null, values);
                }
            }
        } finally {
            cursor.close();
        }
    }

    private static final class Upgrade303Query {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=?" +
                    " AND " + Data._ID + " NOT IN " +
                    "(SELECT " + NameLookupColumns.DATA_ID + " FROM " + Tables.NAME_LOOKUP + ")" +
                    " AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Data._ID,
                Data.RAW_CONTACT_ID,
                Data.DATA1,
        };

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int DATA1 = 2;
    }

    /**
     * The {@link ContactsProvider2#update} method was deleting name lookup for new
     * emails during the sync.  We need to restore the lost name lookup rows.
     */
    private void upgradeEmailToVersion303(SQLiteDatabase db) {
        final long mimeTypeId = lookupMimeTypeId(db, Email.CONTENT_ITEM_TYPE);
        if (mimeTypeId == -1) {
            return;
        }

        ContentValues values = new ContentValues();

        // Find all data rows with the mime type "email" that are missing name lookup
        Cursor cursor = db.query(Upgrade303Query.TABLE, Upgrade303Query.COLUMNS,
                Upgrade303Query.SELECTION, new String[] {String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(Upgrade303Query.ID);
                long rawContactId = cursor.getLong(Upgrade303Query.RAW_CONTACT_ID);
                String value = cursor.getString(Upgrade303Query.DATA1);
                value = extractHandleFromEmailAddress(value);

                if (value != null) {
                    values.put(NameLookupColumns.DATA_ID, dataId);
                    values.put(NameLookupColumns.RAW_CONTACT_ID, rawContactId);
                    values.put(NameLookupColumns.NAME_TYPE, NameLookupType.EMAIL_BASED_NICKNAME);
                    values.put(NameLookupColumns.NORMALIZED_NAME, NameNormalizer.normalize(value));
                    db.insert(Tables.NAME_LOOKUP, null, values);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * The {@link ContactsProvider2#update} method was deleting name lookup for new
     * nicknames during the sync.  We need to restore the lost name lookup rows.
     */
    private void upgradeNicknameToVersion303(SQLiteDatabase db) {
        final long mimeTypeId = lookupMimeTypeId(db, Nickname.CONTENT_ITEM_TYPE);
        if (mimeTypeId == -1) {
            return;
        }

        ContentValues values = new ContentValues();

        // Find all data rows with the mime type "nickname" that are missing name lookup
        Cursor cursor = db.query(Upgrade303Query.TABLE, Upgrade303Query.COLUMNS,
                Upgrade303Query.SELECTION, new String[] {String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(Upgrade303Query.ID);
                long rawContactId = cursor.getLong(Upgrade303Query.RAW_CONTACT_ID);
                String value = cursor.getString(Upgrade303Query.DATA1);

                values.put(NameLookupColumns.DATA_ID, dataId);
                values.put(NameLookupColumns.RAW_CONTACT_ID, rawContactId);
                values.put(NameLookupColumns.NAME_TYPE, NameLookupType.NICKNAME);
                values.put(NameLookupColumns.NORMALIZED_NAME, NameNormalizer.normalize(value));
                db.insert(Tables.NAME_LOOKUP, null, values);
            }
        } finally {
            cursor.close();
        }
    }

    private void upgradeToVersion304(SQLiteDatabase db) {
        // Mimetype table requires an index on mime type
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS mime_type ON " + Tables.MIMETYPES + " (" +
                MimetypesColumns.MIMETYPE +
        ");");
    }

    private void upgradeToVersion306(SQLiteDatabase db) {
        // Fix invalid lookup that was used for Exchange contacts (it was not escaped)
        // It happened when a new contact was created AND synchronized
        final StringBuilder lookupKeyBuilder = new StringBuilder();
        final SQLiteStatement updateStatement = db.compileStatement(
                "UPDATE contacts " +
                "SET lookup=? " +
                "WHERE _id=?");
        final Cursor contactIdCursor = db.rawQuery(
                "SELECT DISTINCT contact_id " +
                "FROM raw_contacts " +
                "WHERE deleted=0 AND account_type='com.android.exchange'",
                null);
        try {
            while (contactIdCursor.moveToNext()) {
                final long contactId = contactIdCursor.getLong(0);
                lookupKeyBuilder.setLength(0);
                final Cursor c = db.rawQuery(
                        "SELECT account_type, account_name, _id, sourceid, display_name " +
                        "FROM raw_contacts " +
                        "WHERE contact_id=? " +
                        "ORDER BY _id",
                        new String[] { String.valueOf(contactId) });
                try {
                    while (c.moveToNext()) {
                        ContactLookupKey.appendToLookupKey(lookupKeyBuilder,
                                c.getString(0),
                                c.getString(1),
                                c.getLong(2),
                                c.getString(3),
                                c.getString(4));
                    }
                } finally {
                    c.close();
                }

                if (lookupKeyBuilder.length() == 0) {
                    updateStatement.bindNull(1);
                } else {
                    updateStatement.bindString(1, Uri.encode(lookupKeyBuilder.toString()));
                }
                updateStatement.bindLong(2, contactId);

                updateStatement.execute();
            }
        } finally {
            updateStatement.close();
            contactIdCursor.close();
        }
    }

    private void upgradeToVersion307(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE properties (" +
                "property_key TEXT PRIMARY_KEY, " +
                "property_value TEXT" +
        ");");
    }

    private void upgradeToVersion308(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE accounts (" +
                    "account_name TEXT, " +
                    "account_type TEXT " +
            ");");

            db.execSQL("INSERT INTO accounts " +
                    "SELECT DISTINCT account_name, account_type FROM raw_contacts");
    }

    private void upgradeToVersion353(SQLiteDatabase db) {
        db.execSQL("DELETE FROM contacts " +
                "WHERE NOT EXISTS (SELECT 1 FROM raw_contacts WHERE contact_id=contacts._id)");
    }

    private void rebuildNameLookup(SQLiteDatabase db) {
        db.execSQL("DROP INDEX IF EXISTS name_lookup_index");
        insertNameLookup(db);
        createContactsIndexes(db);
    }

    /**
     * Regenerates all locale-sensitive data: nickname_lookup, name_lookup and sort keys.
     */
    public void setLocale(ContactsProvider2 provider, Locale locale) {
        Log.i(TAG, "Switching to locale " + locale);

        long start = SystemClock.uptimeMillis();
        SQLiteDatabase db = getWritableDatabase();
        db.setLocale(locale);
        db.beginTransaction();
        try {
            db.execSQL("DROP INDEX raw_contact_sort_key1_index");
            db.execSQL("DROP INDEX raw_contact_sort_key2_index");
            db.execSQL("DROP INDEX IF EXISTS name_lookup_index");

            loadNicknameLookupTable(db);
            insertNameLookup(db);
            rebuildSortKeys(db, provider);
            createContactsIndexes(db);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        Log.i(TAG, "Locale change completed in " + (SystemClock.uptimeMillis() - start) + "ms");
    }

    /**
     * Regenerates sort keys for all contacts.
     */
    private void rebuildSortKeys(SQLiteDatabase db, ContactsProvider2 provider) {
        Cursor cursor = db.query(Tables.RAW_CONTACTS, new String[]{RawContacts._ID},
                null, null, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long rawContactId = cursor.getLong(0);
                provider.updateRawContactDisplayName(db, rawContactId);
            }
        } finally {
            cursor.close();
        }
    }

    private void insertNameLookup(SQLiteDatabase db) {
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP);

        SQLiteStatement nameLookupInsert = db.compileStatement(
                "INSERT OR IGNORE INTO " + Tables.NAME_LOOKUP + "("
                        + NameLookupColumns.RAW_CONTACT_ID + ","
                        + NameLookupColumns.DATA_ID + ","
                        + NameLookupColumns.NAME_TYPE + ","
                        + NameLookupColumns.NORMALIZED_NAME +
                ") VALUES (?,?,?,?)");

        try {
            insertStructuredNameLookup(db, nameLookupInsert);
            insertOrganizationLookup(db, nameLookupInsert);
            insertEmailLookup(db, nameLookupInsert);
            insertNicknameLookup(db, nameLookupInsert);
        } finally {
            nameLookupInsert.close();
        }
    }

    private static final class StructuredNameQuery {
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

    private class StructuredNameLookupBuilder extends NameLookupBuilder {

        private final SQLiteStatement mNameLookupInsert;
        private final CommonNicknameCache mCommonNicknameCache;

        public StructuredNameLookupBuilder(NameSplitter splitter,
                CommonNicknameCache commonNicknameCache, SQLiteStatement nameLookupInsert) {
            super(splitter);
            this.mCommonNicknameCache = commonNicknameCache;
            this.mNameLookupInsert = nameLookupInsert;
        }

        @Override
        protected void insertNameLookup(long rawContactId, long dataId, int lookupType,
                String name) {
            if (!TextUtils.isEmpty(name)) {
                ContactsDatabaseHelper.this.insertNormalizedNameLookup(mNameLookupInsert,
                        rawContactId, dataId, lookupType, name);
            }
        }

        @Override
        protected String[] getCommonNicknameClusters(String normalizedName) {
            return mCommonNicknameCache.getCommonNicknameClusters(normalizedName);
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

    private static final class OrganizationQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Organization._ID,
                Organization.RAW_CONTACT_ID,
                Organization.COMPANY,
                Organization.TITLE,
        };

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int COMPANY = 2;
        public static final int TITLE = 3;
    }

    /**
     * Inserts name lookup rows for all organizations in the database.
     */
    private void insertOrganizationLookup(SQLiteDatabase db, SQLiteStatement nameLookupInsert) {
        final long mimeTypeId = lookupMimeTypeId(db, Organization.CONTENT_ITEM_TYPE);
        Cursor cursor = db.query(OrganizationQuery.TABLE, OrganizationQuery.COLUMNS,
                OrganizationQuery.SELECTION, new String[] {String.valueOf(mimeTypeId)},
                null, null, null);
        try {
            while (cursor.moveToNext()) {
                long dataId = cursor.getLong(OrganizationQuery.ID);
                long rawContactId = cursor.getLong(OrganizationQuery.RAW_CONTACT_ID);
                String organization = cursor.getString(OrganizationQuery.COMPANY);
                String title = cursor.getString(OrganizationQuery.TITLE);
                insertNameLookup(nameLookupInsert, rawContactId, dataId,
                        NameLookupType.ORGANIZATION, organization);
                insertNameLookup(nameLookupInsert, rawContactId, dataId,
                        NameLookupType.ORGANIZATION, title);
            }
        } finally {
            cursor.close();
        }
    }

    private static final class EmailQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Email._ID,
                Email.RAW_CONTACT_ID,
                Email.ADDRESS,
        };

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int ADDRESS = 2;
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

    private static final class NicknameQuery {
        public static final String TABLE = Tables.DATA;

        public static final String SELECTION =
                DataColumns.MIMETYPE_ID + "=? AND " + Data.DATA1 + " NOT NULL";

        public static final String COLUMNS[] = {
                Nickname._ID,
                Nickname.RAW_CONTACT_ID,
                Nickname.NAME,
        };

        public static final int ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int NAME = 2;
    }

    /**
     * Inserts name lookup rows for all nicknames in the database.
     */
    private void insertNicknameLookup(SQLiteDatabase db, SQLiteStatement nameLookupInsert) {
        final long mimeTypeId = lookupMimeTypeId(db, Nickname.CONTENT_ITEM_TYPE);
        Cursor cursor = db.query(NicknameQuery.TABLE, NicknameQuery.COLUMNS,
                NicknameQuery.SELECTION, new String[] {String.valueOf(mimeTypeId)},
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

    public String extractHandleFromEmailAddress(String email) {
        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return null;
        }

        String address = tokens[0].getAddress();
        int at = address.indexOf('@');
        if (at != -1) {
            return address.substring(0, at);
        }
        return null;
    }

    public String extractAddressFromEmailAddress(String email) {
        Rfc822Token[] tokens = Rfc822Tokenizer.tokenize(email);
        if (tokens.length == 0) {
            return null;
        }

        return tokens[0].getAddress();
    }

    private long lookupMimeTypeId(SQLiteDatabase db, String mimeType) {
        try {
            return DatabaseUtils.longForQuery(db,
                    "SELECT " + MimetypesColumns._ID +
                    " FROM " + Tables.MIMETYPES +
                    " WHERE " + MimetypesColumns.MIMETYPE
                            + "='" + mimeType + "'", null);
        } catch (SQLiteDoneException e) {
            // No rows of this type in the database
            return -1;
        }
    }

    private void bindString(SQLiteStatement stmt, int index, String value) {
        if (value == null) {
            stmt.bindNull(index);
        } else {
            stmt.bindString(index, value);
        }
    }

    /**
     * Adds index stats into the SQLite database to force it to always use the lookup indexes.
     */
    private void updateSqliteStats(SQLiteDatabase db) {

        // Specific stats strings are based on an actual large database after running ANALYZE
        try {
            updateIndexStats(db, Tables.CONTACTS,
                    "contacts_restricted_index", "10000 9000");
            updateIndexStats(db, Tables.CONTACTS,
                    "contacts_has_phone_index", "10000 500");
            updateIndexStats(db, Tables.CONTACTS,
                    "contacts_visible_index", "10000 500 1");

            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contacts_source_id_index", "10000 1 1 1");
            updateIndexStats(db, Tables.RAW_CONTACTS,
                    "raw_contacts_contact_id_index", "10000 2");

            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "name_lookup_raw_contact_id_index", "10000 3");
            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "name_lookup_index", "10000 3 2 2 1");
            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "sqlite_autoindex_name_lookup_1", "10000 3 2 1");

            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_index", "10000 2 2 1");
            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_min_match_index", "10000 2 2 1");

            updateIndexStats(db, Tables.DATA,
                    "data_mimetype_data1_index", "60000 5000 2");
            updateIndexStats(db, Tables.DATA,
                    "data_raw_contact_id", "60000 10");

            updateIndexStats(db, Tables.GROUPS,
                    "groups_source_id_index", "50 1 1 1");

            updateIndexStats(db, Tables.NICKNAME_LOOKUP,
                    "sqlite_autoindex_name_lookup_1", "500 2 1");

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
    private void updateIndexStats(SQLiteDatabase db, String table, String index,
            String stats) {
        db.execSQL("DELETE FROM sqlite_stat1 WHERE tbl='" + table + "' AND idx='" + index + "';");
        db.execSQL("INSERT INTO sqlite_stat1 (tbl,idx,stat)"
                + " VALUES ('" + table + "','" + index + "','" + stats + "');");
    }

    @Override
    public synchronized SQLiteDatabase getWritableDatabase() {
        SQLiteDatabase db = super.getWritableDatabase();
        if (mReopenDatabase) {
            mReopenDatabase = false;
            close();
            db = super.getWritableDatabase();
        }
        return db;
    }

    /**
     * Wipes all data except mime type and package lookup tables.
     */
    public void wipeData() {
        SQLiteDatabase db = getWritableDatabase();

        db.execSQL("DELETE FROM " + Tables.ACCOUNTS + ";");
        db.execSQL("INSERT INTO " + Tables.ACCOUNTS + " VALUES(NULL, NULL)");

        db.execSQL("DELETE FROM " + Tables.CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.RAW_CONTACTS + ";");
        db.execSQL("DELETE FROM " + Tables.DATA + ";");
        db.execSQL("DELETE FROM " + Tables.PHONE_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.NAME_LOOKUP + ";");
        db.execSQL("DELETE FROM " + Tables.GROUPS + ";");
        db.execSQL("DELETE FROM " + Tables.AGGREGATION_EXCEPTIONS + ";");
        db.execSQL("DELETE FROM " + Tables.SETTINGS + ";");
        db.execSQL("DELETE FROM " + Tables.ACTIVITIES + ";");
        db.execSQL("DELETE FROM " + Tables.CALLS + ";");

        // Note: we are not removing reference data from Tables.NICKNAME_LOOKUP
    }

    public NameSplitter createNameSplitter() {
        return new NameSplitter(
                mContext.getString(com.android.internal.R.string.common_name_prefixes),
                mContext.getString(com.android.internal.R.string.common_last_name_prefixes),
                mContext.getString(com.android.internal.R.string.common_name_suffixes),
                mContext.getString(com.android.internal.R.string.common_name_conjunctions),
                Locale.getDefault());
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

    /**
     * Perform an internal string-to-integer lookup using the compiled
     * {@link SQLiteStatement} provided, using the in-memory cache to speed up
     * lookups. If a mapping isn't found in cache or database, it will be
     * created. All new, uncached answers are added to the cache automatically.
     *
     * @param query Compiled statement used to query for the mapping.
     * @param insert Compiled statement used to insert a new mapping when no
     *            existing one is found in cache or from query.
     * @param value Value to find mapping for.
     * @param cache In-memory cache of previous answers.
     * @return An unique integer mapping for the given value.
     */
    private long getCachedId(SQLiteStatement query, SQLiteStatement insert,
            String value, HashMap<String, Long> cache) {
        // Try an in-memory cache lookup
        if (cache.containsKey(value)) {
            return cache.get(value);
        }

        long id = -1;
        try {
            // Try searching database for mapping
            DatabaseUtils.bindObjectToProgram(query, 1, value);
            id = query.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            // Nothing found, so try inserting new mapping
            DatabaseUtils.bindObjectToProgram(insert, 1, value);
            id = insert.executeInsert();
        }

        if (id != -1) {
            // Cache and return the new answer
            cache.put(value, id);
            return id;
        } else {
            // Otherwise throw if no mapping found or created
            throw new IllegalStateException("Couldn't find or create internal "
                    + "lookup table entry for value " + value);
        }
    }

    /**
     * Convert a package name into an integer, using {@link Tables#PACKAGES} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getPackageId(String packageName) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        return getCachedId(mPackageQuery, mPackageInsert, packageName, mPackageCache);
    }

    /**
     * Convert a mimetype into an integer, using {@link Tables#MIMETYPES} for
     * lookups and possible allocation of new IDs as needed.
     */
    public long getMimeTypeId(String mimetype) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        return getMimeTypeIdNoDbCheck(mimetype);
    }

    private long getMimeTypeIdNoDbCheck(String mimetype) {
        return getCachedId(mMimetypeQuery, mMimetypeInsert, mimetype, mMimetypeCache);
    }

    /**
     * Find the mimetype for the given {@link Data#_ID}.
     */
    public String getDataMimeType(long dataId) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        try {
            // Try database query to find mimetype
            DatabaseUtils.bindObjectToProgram(mDataMimetypeQuery, 1, dataId);
            String mimetype = mDataMimetypeQuery.simpleQueryForString();
            return mimetype;
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    /**
     * Find the mime-type for the given {@link Activities#_ID}.
     */
    public String getActivityMimeType(long activityId) {
        // Make sure compiled statements are ready by opening database
        getReadableDatabase();
        try {
            // Try database query to find mimetype
            DatabaseUtils.bindObjectToProgram(mActivitiesMimetypeQuery, 1, activityId);
            String mimetype = mActivitiesMimetypeQuery.simpleQueryForString();
            return mimetype;
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return null
            return null;
        }
    }

    /**
     * Update {@link Contacts#IN_VISIBLE_GROUP} for all contacts.
     */
    public void updateAllVisible() {
        SQLiteDatabase db = getWritableDatabase();
        final long groupMembershipMimetypeId = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        String[] selectionArgs = new String[]{String.valueOf(groupMembershipMimetypeId)};

        // There are a couple questions that can be asked regarding the
        // following two update statements:
        //
        // Q: Why do we run these two queries separately? They seem like they could be combined.
        // A: This is a result of painstaking experimentation.  Turns out that the most
        // important optimization is to make sure we never update a value to its current value.
        // Changing 0 to 0 is unexpectedly expensive - SQLite actually writes the unchanged
        // rows back to disk.  The other consideration is that the CONTACT_IS_VISIBLE condition
        // is very complex and executing it twice in the same statement ("if contact_visible !=
        // CONTACT_IS_VISIBLE change it to CONTACT_IS_VISIBLE") is more expensive than running
        // two update statements.
        //
        // Q: How come we are using db.update instead of compiled statements?
        // A: This is a limitation of the compiled statement API. It does not return the
        // number of rows changed.  As you will see later in this method we really need
        // to know how many rows have been changed.

        // First update contacts that are currently marked as invisible, but need to be visible
        ContentValues values = new ContentValues();
        values.put(Contacts.IN_VISIBLE_GROUP, 1);
        int countMadeVisible = db.update(Tables.CONTACTS, values,
                Contacts.IN_VISIBLE_GROUP + "=0" + " AND (" + Clauses.CONTACT_IS_VISIBLE + ")=1",
                selectionArgs);

        // Next update contacts that are currently marked as visible, but need to be invisible
        values.put(Contacts.IN_VISIBLE_GROUP, 0);
        int countMadeInvisible = db.update(Tables.CONTACTS, values,
                Contacts.IN_VISIBLE_GROUP + "=1" + " AND (" + Clauses.CONTACT_IS_VISIBLE + ")=0",
                selectionArgs);

        if (countMadeVisible != 0 || countMadeInvisible != 0) {
            // TODO break out the fields (contact_in_visible_group, sort_key, sort_key_alt) into
            // a separate table.
            // Rationale: The following statement will take a very long time on
            // a large database even though we are only changing one field from 0 to 1 or from
            // 1 to 0.  The reason for the slowness is that SQLite will need to write the whole
            // page even when only one bit on it changes. Changing the visibility of a
            // significant number of contacts will likely read and write almost the entire
            // raw_contacts table.  So, the solution is to break out into a separate table
            // the changing field along with the sort keys used for index-based sorting.
            // That table will occupy a smaller number of pages, so rewriting it would
            // not be as expensive.
            mVisibleUpdateRawContacts.execute();
        }
    }

    /**
     * Update {@link Contacts#IN_VISIBLE_GROUP} for a specific contact.
     */
    public void updateContactVisible(long contactId) {
        final long groupMembershipMimetypeId = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        mVisibleSpecificUpdate.bindLong(1, groupMembershipMimetypeId);
        mVisibleSpecificUpdate.bindLong(2, contactId);
        mVisibleSpecificUpdate.execute();

        mVisibleSpecificUpdateRawContacts.bindLong(1, contactId);
        mVisibleSpecificUpdateRawContacts.execute();
    }

    /**
     * Returns contact ID for the given contact or zero if it is NULL.
     */
    public long getContactId(long rawContactId) {
        getReadableDatabase();
        try {
            DatabaseUtils.bindObjectToProgram(mContactIdQuery, 1, rawContactId);
            return mContactIdQuery.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            // No valid mapping found, so return 0
            return 0;
        }
    }

    public int getAggregationMode(long rawContactId) {
        getReadableDatabase();
        try {
            DatabaseUtils.bindObjectToProgram(mAggregationModeQuery, 1, rawContactId);
            return (int)mAggregationModeQuery.simpleQueryForLong();
        } catch (SQLiteDoneException e) {
            // No valid row found, so return "disabled"
            return RawContacts.AGGREGATION_MODE_DISABLED;
        }
    }

    public void buildPhoneLookupAndRawContactQuery(SQLiteQueryBuilder qb, String number) {
        String minMatch = PhoneNumberUtils.toCallerIDMinMatch(number);
        qb.setTables(Tables.DATA_JOIN_RAW_CONTACTS +
                " JOIN " + Tables.PHONE_LOOKUP
                + " ON(" + DataColumns.CONCRETE_ID + "=" + PhoneLookupColumns.DATA_ID + ")");

        StringBuilder sb = new StringBuilder();
        sb.append(PhoneLookupColumns.MIN_MATCH + "='");
        sb.append(minMatch);
        sb.append("' AND PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        DatabaseUtils.appendEscapedSQLString(sb, number);
        sb.append(mUseStrictPhoneNumberComparison ? ", 1)" : ", 0)");

        qb.appendWhere(sb.toString());
    }

    public void buildPhoneLookupAndContactQuery(SQLiteQueryBuilder qb, String number) {
        String minMatch = PhoneNumberUtils.toCallerIDMinMatch(number);
        StringBuilder sb = new StringBuilder();
        appendPhoneLookupTables(sb, minMatch, true);
        qb.setTables(sb.toString());

        sb = new StringBuilder();
        appendPhoneLookupSelection(sb, number);
        qb.appendWhere(sb.toString());
    }

    public String buildPhoneLookupAsNestedQuery(String number) {
        StringBuilder sb = new StringBuilder();
        final String minMatch = PhoneNumberUtils.toCallerIDMinMatch(number);
        sb.append("(SELECT DISTINCT raw_contact_id" + " FROM ");
        appendPhoneLookupTables(sb, minMatch, false);
        sb.append(" WHERE ");
        appendPhoneLookupSelection(sb, number);
        sb.append(")");
        return sb.toString();
    }

    private void appendPhoneLookupTables(StringBuilder sb, final String minMatch,
            boolean joinContacts) {
        sb.append(Tables.RAW_CONTACTS);
        if (joinContacts) {
            sb.append(" JOIN " + getContactView() + " contacts_view"
                    + " ON (contacts_view._id = raw_contacts.contact_id)");
        }
        sb.append(", (SELECT data_id FROM phone_lookup "
                + "WHERE (" + Tables.PHONE_LOOKUP + "." + PhoneLookupColumns.MIN_MATCH + " = '");
        sb.append(minMatch);
        sb.append("')) AS lookup, " + Tables.DATA);
    }

    private void appendPhoneLookupSelection(StringBuilder sb, String number) {
        sb.append("lookup.data_id=data._id AND data.raw_contact_id=raw_contacts._id"
                + " AND PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        DatabaseUtils.appendEscapedSQLString(sb, number);
        sb.append(mUseStrictPhoneNumberComparison ? ", 1)" : ", 0)");
    }

    public String getUseStrictPhoneNumberComparisonParameter() {
        return mUseStrictPhoneNumberComparison ? "1" : "0";
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

        SQLiteStatement nicknameLookupInsert = db.compileStatement("INSERT INTO "
                + Tables.NICKNAME_LOOKUP + "(" + NicknameLookupColumns.NAME + ","
                + NicknameLookupColumns.CLUSTER + ") VALUES (?,?)");

        try {
            for (int clusterId = 0; clusterId < strings.length; clusterId++) {
                String[] names = strings[clusterId].split(",");
                for (int j = 0; j < names.length; j++) {
                    String name = NameNormalizer.normalize(names[j]);
                    try {
                        DatabaseUtils.bindObjectToProgram(nicknameLookupInsert, 1, name);
                        DatabaseUtils.bindObjectToProgram(nicknameLookupInsert, 2,
                                String.valueOf(clusterId));
                        nicknameLookupInsert.executeInsert();
                    } catch (SQLiteException e) {

                        // Print the exception and keep going - this is not a fatal error
                        Log.e(TAG, "Cannot insert nickname: " + names[j], e);
                    }
                }
            }
        } finally {
            nicknameLookupInsert.close();
        }
    }

    public static void copyStringValue(ContentValues toValues, String toKey,
            ContentValues fromValues, String fromKey) {
        if (fromValues.containsKey(fromKey)) {
            toValues.put(toKey, fromValues.getAsString(fromKey));
        }
    }

    public static void copyLongValue(ContentValues toValues, String toKey,
            ContentValues fromValues, String fromKey) {
        if (fromValues.containsKey(fromKey)) {
            long longValue;
            Object value = fromValues.get(fromKey);
            if (value instanceof Boolean) {
                if ((Boolean)value) {
                    longValue = 1;
                } else {
                    longValue = 0;
                }
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
     * Delete the aggregate contact if it has no constituent raw contacts other
     * than the supplied one.
     */
    public void removeContactIfSingleton(long rawContactId) {
        SQLiteDatabase db = getWritableDatabase();

        // Obtain contact ID from the supplied raw contact ID
        String contactIdFromRawContactId = "(SELECT " + RawContacts.CONTACT_ID + " FROM "
                + Tables.RAW_CONTACTS + " WHERE " + RawContacts._ID + "=" + rawContactId + ")";

        // Find other raw contacts in the same aggregate contact
        String otherRawContacts = "(SELECT contacts1." + RawContacts._ID + " FROM "
                + Tables.RAW_CONTACTS + " contacts1 JOIN " + Tables.RAW_CONTACTS + " contacts2 ON ("
                + "contacts1." + RawContacts.CONTACT_ID + "=contacts2." + RawContacts.CONTACT_ID
                + ") WHERE contacts1." + RawContacts._ID + "!=" + rawContactId + ""
                + " AND contacts2." + RawContacts._ID + "=" + rawContactId + ")";

        db.execSQL("DELETE FROM " + Tables.CONTACTS
                + " WHERE " + Contacts._ID + "=" + contactIdFromRawContactId
                + " AND NOT EXISTS " + otherRawContacts + ";");
    }

    /**
     * Returns the value from the {@link Tables#PROPERTIES} table.
     */
    public String getProperty(String key, String defaultValue) {
        Cursor cursor = getReadableDatabase().query(Tables.PROPERTIES,
                new String[]{PropertiesColumns.PROPERTY_VALUE},
                PropertiesColumns.PROPERTY_KEY + "=?",
                new String[]{key}, null, null, null);
        String value = null;
        try {
            if (cursor.moveToFirst()) {
                value = cursor.getString(0);
            }
        } finally {
            cursor.close();
        }

        return value != null ? value : defaultValue;
    }

    /**
     * Stores a key-value pair in the {@link Tables#PROPERTIES} table.
     */
    public void setProperty(String key, String value) {
        ContentValues values = new ContentValues();
        values.put(PropertiesColumns.PROPERTY_KEY, key);
        values.put(PropertiesColumns.PROPERTY_VALUE, value);
        getWritableDatabase().replace(Tables.PROPERTIES, null, values);
    }

    /**
     * Check if {@link Binder#getCallingUid()} should be allowed access to
     * {@link RawContacts#IS_RESTRICTED} data.
     */
    boolean hasAccessToRestrictedData() {
        final PackageManager pm = mContext.getPackageManager();
        final String[] callerPackages = pm.getPackagesForUid(Binder.getCallingUid());

        // Has restricted access if caller matches any packages
        for (String callerPackage : callerPackages) {
            if (hasAccessToRestrictedData(callerPackage)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if requestingPackage should be allowed access to
     * {@link RawContacts#IS_RESTRICTED} data.
     */
    boolean hasAccessToRestrictedData(String requestingPackage) {
        if (mUnrestrictedPackages != null) {
            for (String allowedPackage : mUnrestrictedPackages) {
                if (allowedPackage.equals(requestingPackage)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getDataView() {
        return getDataView(false);
    }

    public String getDataView(boolean requireRestrictedView) {
        return (hasAccessToRestrictedData() && !requireRestrictedView) ?
                Views.DATA_ALL : Views.DATA_RESTRICTED;
    }

    public String getRawContactView() {
        return getRawContactView(false);
    }

    public String getRawContactView(boolean requireRestrictedView) {
        return (hasAccessToRestrictedData() && !requireRestrictedView) ?
                Views.RAW_CONTACTS_ALL : Views.RAW_CONTACTS_RESTRICTED;
    }

    public String getContactView() {
        return getContactView(false);
    }

    public String getContactView(boolean requireRestrictedView) {
        return (hasAccessToRestrictedData() && !requireRestrictedView) ?
                Views.CONTACTS_ALL : Views.CONTACTS_RESTRICTED;
    }

    public String getGroupView() {
        return Views.GROUPS_ALL;
    }

    public String getContactEntitiesView() {
        return getContactEntitiesView(false);
    }

    public String getContactEntitiesView(boolean requireRestrictedView) {
        return (hasAccessToRestrictedData() && !requireRestrictedView) ?
                Tables.CONTACT_ENTITIES : Tables.CONTACT_ENTITIES_RESTRICTED;
    }

    /**
     * Test if any of the columns appear in the given projection.
     */
    public boolean isInProjection(String[] projection, String... columns) {
        if (projection == null) {
            return true;
        }

        // Optimized for a single-column test
        if (columns.length == 1) {
            String column = columns[0];
            for (String test : projection) {
                if (column.equals(test)) {
                    return true;
                }
            }
        } else {
            for (String test : projection) {
                for (String column : columns) {
                    if (column.equals(test)) {
                        return true;
                    }
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
        if (userName != null) {
            sb.append(userName);
        } else {
            sb.append(callingUid);
        }

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
}
