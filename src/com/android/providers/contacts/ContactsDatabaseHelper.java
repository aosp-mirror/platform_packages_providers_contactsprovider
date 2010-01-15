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
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.os.Binder;
import android.os.Bundle;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.SocialContract.Activities;
import android.telephony.PhoneNumberUtils;
import android.util.Log;

import java.util.HashMap;

/**
 * Database helper for contacts. Designed as a singleton to make sure that all
 * {@link android.content.ContentProvider} users get the same reference.
 * Provides handy methods for maintaining package and mime-type lookup tables.
 */
/* package */ class ContactsDatabaseHelper extends SQLiteOpenHelper {
    private static final String TAG = "ContactsDatabaseHelper";

    private static final int DATABASE_VERSION = 105;

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

        public static final String DATA_JOIN_MIMETYPES = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id)";

        public static final String DATA_JOIN_RAW_CONTACTS = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        public static final String DATA_JOIN_MIMETYPE_RAW_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        public static final String DATA_JOIN_RAW_CONTACTS_GROUPS = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)"
                + "LEFT OUTER JOIN groups ON (groups._id = data." + GroupMembership.GROUP_ROW_ID
                + ")";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS = "data "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String RAW_CONTACTS_JOIN_CONTACTS = "raw_contacts "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

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

        public static final String DATA_INNER_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS = "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS_GROUPS =
                "data "
                + "JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN groups "
                + "  ON (mimetypes.mimetype='" + GroupMembership.CONTENT_ITEM_TYPE + "' "
                + "      AND groups._id = data." + GroupMembership.GROUP_ROW_ID + ") "
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

        public static final String GROUPS_JOIN_PACKAGES_DATA_RAW_CONTACTS_CONTACTS = "groups "
                + "LEFT OUTER JOIN packages ON (groups.package_id = packages._id) "
                + "LEFT OUTER JOIN data "
                + "  ON (groups._id = data." + GroupMembership.GROUP_ROW_ID + ") "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

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
        public static final String CONCRETE_DISPLAY_NAME = Tables.CONTACTS + "."
                + Contacts.DISPLAY_NAME;

        public static final String CONCRETE_TIMES_CONTACTED = Tables.CONTACTS + "."
                + Contacts.TIMES_CONTACTED;
        public static final String CONCRETE_LAST_TIME_CONTACTED = Tables.CONTACTS + "."
                + Contacts.LAST_TIME_CONTACTED;
        public static final String CONCRETE_STARRED = Tables.CONTACTS + "." + Contacts.STARRED;
        public static final String CONCRETE_CUSTOM_RINGTONE = Tables.CONTACTS + "."
                + Contacts.CUSTOM_RINGTONE;
        public static final String CONCRETE_SEND_TO_VOICEMAIL = Tables.CONTACTS + "."
                + Contacts.SEND_TO_VOICEMAIL;
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

        public static final String DISPLAY_NAME = "display_name";
        public static final String DISPLAY_NAME_SOURCE = "display_name_source";
        public static final String AGGREGATION_NEEDED = "aggregation_needed";
    }

    /**
     * Types of data used to produce the display name for a contact. Listed in the order
     * of increasing priority.
     */
    public interface DisplayNameSources {
        int UNDEFINED = 0;
        int EMAIL = 10;
        int PHONE = 20;
        int ORGANIZATION = 30;
        int NICKNAME = 35;
        int STRUCTURED_NAME = 40;
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

        // This is the highest name lookup type code plus one
        public static final int TYPE_COUNT = 6;

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
    private SQLiteStatement mVisibleUpdate;
    private SQLiteStatement mVisibleSpecificUpdate;

    private boolean mReopenDatabase = false;

    private static ContactsDatabaseHelper sSingleton = null;

    private boolean mUseStrictPhoneNumberComparation;

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
        mUseStrictPhoneNumberComparation =
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

        // Compile statements for updating visibility
        final String visibleUpdate = "UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.IN_VISIBLE_GROUP + "=(" + Clauses.CONTACT_IS_VISIBLE + ")";

        mVisibleUpdate = db.compileStatement(visibleUpdate);
        mVisibleSpecificUpdate = db.compileStatement(visibleUpdate + " WHERE "
                + ContactsColumns.CONCRETE_ID + "=?");

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
                "UNIQUE(" + StatusUpdates.PROTOCOL + ", " + StatusUpdates.CUSTOM_PROTOCOL
                    + ", " + StatusUpdates.IM_HANDLE + ", " + StatusUpdates.IM_ACCOUNT + ")" +
        ");");

        db.execSQL("CREATE INDEX IF NOT EXISTS " + DATABASE_PRESENCE + ".presenceIndex" + " ON "
                + Tables.PRESENCE + " (" + PresenceColumns.RAW_CONTACT_ID + ");");

        db.execSQL("CREATE TABLE IF NOT EXISTS "
                        + DATABASE_PRESENCE + "." + Tables.AGGREGATED_PRESENCE + " ("+
                AggregatedPresenceColumns.CONTACT_ID
                        + " INTEGER PRIMARY KEY REFERENCES contacts(_id)," +
                StatusUpdates.PRESENCE_STATUS + " INTEGER" +
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

        String replaceAggregatePresenceSql =
            "INSERT OR REPLACE INTO " + Tables.AGGREGATED_PRESENCE + "("
                    + AggregatedPresenceColumns.CONTACT_ID + ", "
                    + StatusUpdates.PRESENCE_STATUS + ")" +
            " SELECT " + PresenceColumns.CONTACT_ID + ","
                        + "MAX(" + StatusUpdates.PRESENCE_STATUS + ")" +
                    " FROM " + Tables.PRESENCE +
                    " WHERE " + PresenceColumns.CONTACT_ID
                        + "=NEW." + PresenceColumns.CONTACT_ID + ";";

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
                Contacts.DISPLAY_NAME + " TEXT," +
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
                Contacts.IN_VISIBLE_GROUP + "," +
                Contacts.DISPLAY_NAME + " COLLATE LOCALIZED" +
        ");");

        db.execSQL("CREATE INDEX contacts_has_phone_index ON " + Tables.CONTACTS + " (" +
                Contacts.HAS_PHONE_NUMBER +
        ");");

        db.execSQL("CREATE INDEX contacts_restricted_index ON " + Tables.CONTACTS + " (" +
                ContactsColumns.SINGLE_IS_RESTRICTED +
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
                RawContactsColumns.DISPLAY_NAME + " TEXT," +
                RawContactsColumns.DISPLAY_NAME_SOURCE + " INTEGER NOT NULL DEFAULT " +
                        DisplayNameSources.UNDEFINED + "," +
                RawContacts.SYNC1 + " TEXT, " +
                RawContacts.SYNC2 + " TEXT, " +
                RawContacts.SYNC3 + " TEXT, " +
                RawContacts.SYNC4 + " TEXT " +
        ");");

        db.execSQL("CREATE TRIGGER raw_contacts_times_contacted UPDATE OF " +
                    RawContacts.LAST_TIME_CONTACTED + " ON " + Tables.RAW_CONTACTS + " " +
                "BEGIN " +
                    "UPDATE " + Tables.RAW_CONTACTS + " SET "
                        + RawContacts.TIMES_CONTACTED + " = " + "" +
                            "(new." + RawContacts.TIMES_CONTACTED + " + 1)"
                        + " WHERE _id = new._id;" +
                "END");

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
                PhoneLookupColumns.NORMALIZED_NUMBER + " TEXT NOT NULL" +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.NORMALIZED_NUMBER + "," +
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

        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + "," +
                NameLookupColumns.NAME_TYPE + ", " +
                NameLookupColumns.RAW_CONTACT_ID +
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

        createContactsViews(db);
        createGroupsView(db);
        createContactEntitiesView(db);
        createContactsTriggers(db);

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

    private void createContactsTriggers(SQLiteDatabase db) {

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
        db.execSQL("CREATE TRIGGER contacts_times_contacted UPDATE OF " +
                Contacts.LAST_TIME_CONTACTED + " ON " + Tables.CONTACTS + " " +
            "BEGIN " +
                "UPDATE " + Tables.CONTACTS + " SET "
                    + Contacts.TIMES_CONTACTED + " = " + "" +
                            "(new." + Contacts.TIMES_CONTACTED + " + 1)"
                    + " WHERE _id = new._id;" +
            "END");

        /*
         * Triggers that update {@link RawContacts#VERSION} when the contact is
         * marked for deletion or any time a data row is inserted, updated or
         * deleted.
         */
        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.RAW_CONTACTS + "_marked_deleted;");
        db.execSQL("CREATE TRIGGER " + Tables.RAW_CONTACTS + "_marked_deleted "
                + "   BEFORE UPDATE ON " + Tables.RAW_CONTACTS
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET "
                +         RawContacts.VERSION + "=OLD." + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + RawContacts._ID
                + "       AND NEW." + RawContacts.DELETED + "!= OLD." + RawContacts.DELETED + ";"
                + " END");

        db.execSQL("DROP TRIGGER IF EXISTS " + Tables.DATA + "_updated;");
        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_updated BEFORE UPDATE ON " + Tables.DATA
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
                + "   BEFORE UPDATE ON " + Tables.GROUPS
                + " BEGIN "
                + "   UPDATE " + Tables.GROUPS
                + "     SET "
                +         Groups.VERSION + "=OLD." + Groups.VERSION + "+1"
                + "     WHERE " + Groups._ID + "=OLD." + Groups._ID + ";"
                + " END");
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

        String dataSelect = "SELECT "
                + DataColumns.CONCRETE_ID + " AS " + Data._ID + ","
                + Data.RAW_CONTACT_ID + ", "
                + RawContacts.CONTACT_ID + ", "
                + syncColumns + ", "
                + dataColumns + ", "
                + contactOptionColumns + ", "
                + ContactsColumns.CONCRETE_DISPLAY_NAME + " AS " + Contacts.DISPLAY_NAME + ", "
                + Contacts.LOOKUP_KEY + ", "
                + Contacts.PHOTO_ID + ", "
                + Contacts.IN_VISIBLE_GROUP + ", "
                + ContactsColumns.LAST_STATUS_UPDATE_ID + ", "
                + Tables.GROUPS + "." + Groups.SOURCE_ID + " AS " + GroupMembership.GROUP_SOURCE_ID
                + " FROM " + Tables.DATA
                + " JOIN " + Tables.MIMETYPES + " ON ("
                +   DataColumns.CONCRETE_MIMETYPE_ID + "=" + MimetypesColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.RAW_CONTACTS + " ON ("
                +   DataColumns.CONCRETE_RAW_CONTACT_ID + "=" + RawContactsColumns.CONCRETE_ID + ")"
                + " JOIN " + Tables.CONTACTS + " ON ("
                +   RawContacts.CONTACT_ID + "=" + Tables.CONTACTS + "." + Contacts._ID + ")"
                + " LEFT OUTER JOIN " + Tables.PACKAGES + " ON ("
                +   DataColumns.CONCRETE_PACKAGE_ID + "=" + PackagesColumns.CONCRETE_ID + ")"
                + " LEFT OUTER JOIN " + Tables.GROUPS + " ON ("
                +   MimetypesColumns.CONCRETE_MIMETYPE + "='" + GroupMembership.CONTENT_ITEM_TYPE
                +   "' AND " + GroupsColumns.CONCRETE_ID + "="
                        + Tables.DATA + "." + GroupMembership.GROUP_ROW_ID + ")";

        db.execSQL("CREATE VIEW " + Views.DATA_ALL + " AS " + dataSelect);
        db.execSQL("CREATE VIEW " + Views.DATA_RESTRICTED + " AS " + dataSelect + " WHERE "
                + RawContacts.IS_RESTRICTED + "=0");

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
                + rawContactOptionColumns + ", "
                + syncColumns
                + " FROM " + Tables.RAW_CONTACTS;

        db.execSQL("CREATE VIEW " + Views.RAW_CONTACTS_ALL + " AS " + rawContactsSelect);
        db.execSQL("CREATE VIEW " + Views.RAW_CONTACTS_RESTRICTED + " AS " + rawContactsSelect
                + " WHERE " + RawContacts.IS_RESTRICTED + "=0");

        String contactsColumns =
                ContactsColumns.CONCRETE_CUSTOM_RINGTONE
                        + " AS " + Contacts.CUSTOM_RINGTONE + ", "
                + ContactsColumns.CONCRETE_DISPLAY_NAME
                        + " AS " + Contacts.DISPLAY_NAME + ", "
                + Contacts.IN_VISIBLE_GROUP + ", "
                + Contacts.HAS_PHONE_NUMBER + ", "
                + Contacts.LOOKUP_KEY + ", "
                + Contacts.PHOTO_ID + ", "
                + Contacts.IN_VISIBLE_GROUP + ", "
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
                + " FROM " + Tables.CONTACTS;

        String restrictedContactsSelect = "SELECT "
                + ContactsColumns.CONCRETE_ID + " AS " + Contacts._ID + ","
                + contactsColumns
                + " FROM " + Tables.CONTACTS
                + " WHERE " + ContactsColumns.SINGLE_IS_RESTRICTED + "=0";

        db.execSQL("CREATE VIEW " + Views.CONTACTS_ALL + " AS " + contactsSelect);
        db.execSQL("CREATE VIEW " + Views.CONTACTS_RESTRICTED + " AS " + restrictedContactsSelect);
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

        if (oldVersion == 99) {
            createContactEntitiesView(db);
            oldVersion++;
        }

        if (oldVersion == 100) {
            db.execSQL("CREATE INDEX IF NOT EXISTS mimetypes_mimetype_index ON "
                    + Tables.MIMETYPES + " ("
                            + MimetypesColumns.MIMETYPE + ","
                            + MimetypesColumns._ID + ");");
            updateIndexStats(db, Tables.MIMETYPES,
                    "mimetypes_mimetype_index", "50 1 1");

            createContactsViews(db);
            oldVersion++;
        }

        if (oldVersion == 101) {
            createContactsTriggers(db);
            oldVersion++;
        }

        if (oldVersion == 102) {
            LegacyApiSupport.createViews(db);
            oldVersion++;
        }

        if (oldVersion == 103) {
            createContactEntitiesView(db);
            oldVersion++;
        }

        if (oldVersion == 104) {
            LegacyApiSupport.createViews(db);
            LegacyApiSupport.createSettingsTable(db);
            oldVersion++;
        }

        if (oldVersion != newVersion) {
            throw new IllegalStateException(
                    "error upgrading the database to version " + newVersion);
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
                    "name_lookup_index", "10000 3 2 2");
            updateIndexStats(db, Tables.NAME_LOOKUP,
                    "sqlite_autoindex_name_lookup_1", "10000 3 2 1");

            updateIndexStats(db, Tables.PHONE_LOOKUP,
                    "phone_lookup_index", "10000 2 2 1");

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
    private void updateIndexStats(SQLiteDatabase db, String table, String index, String stats) {
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

        db.execSQL("VACUUM;");
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
        final long groupMembershipMimetypeId = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        mVisibleUpdate.bindLong(1, groupMembershipMimetypeId);
        mVisibleUpdate.execute();
    }

    /**
     * Update {@link Contacts#IN_VISIBLE_GROUP} for a specific contact.
     */
    public void updateContactVisible(long aggId) {
        final long groupMembershipMimetypeId = getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);
        mVisibleSpecificUpdate.bindLong(1, groupMembershipMimetypeId);
        mVisibleSpecificUpdate.bindLong(2, aggId);
        mVisibleSpecificUpdate.execute();
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
        String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
        qb.setTables(Tables.DATA_JOIN_RAW_CONTACTS +
                " JOIN " + Tables.PHONE_LOOKUP
                + " ON(" + DataColumns.CONCRETE_ID + "=" + PhoneLookupColumns.DATA_ID + ")");

        StringBuilder sb = new StringBuilder();
        sb.append(PhoneLookupColumns.NORMALIZED_NUMBER + " GLOB '");
        sb.append(normalizedNumber);
        sb.append("*' AND PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        DatabaseUtils.appendEscapedSQLString(sb, number);
        sb.append(mUseStrictPhoneNumberComparation ? ", 1)" : ", 0)");

        qb.appendWhere(sb.toString());
    }

    public void buildPhoneLookupAndContactQuery(SQLiteQueryBuilder qb, String number) {
        String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
        StringBuilder sb = new StringBuilder();
        appendPhoneLookupTables(sb, normalizedNumber, true);
        qb.setTables(sb.toString());

        sb = new StringBuilder();
        appendPhoneLookupSelection(sb, number);
        qb.appendWhere(sb.toString());
    }

    public String buildPhoneLookupAsNestedQuery(String number) {
        StringBuilder sb = new StringBuilder();
        final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
        sb.append("(SELECT DISTINCT raw_contact_id" + " FROM ");
        appendPhoneLookupTables(sb, normalizedNumber, false);
        sb.append(" WHERE ");
        appendPhoneLookupSelection(sb, number);
        sb.append(")");
        return sb.toString();
    }

    private void appendPhoneLookupTables(StringBuilder sb, final String normalizedNumber,
            boolean joinContacts) {
        sb.append(Tables.RAW_CONTACTS);
        if (joinContacts) {
            sb.append(" JOIN " + getContactView() + " contacts"
                    + " ON (contacts._id = raw_contacts.contact_id)");
        }
        sb.append(", (SELECT data_id FROM phone_lookup "
                + "WHERE (phone_lookup.normalized_number GLOB '");
        sb.append(normalizedNumber);
        sb.append("*')) AS lookup, " + Tables.DATA);
    }

    private void appendPhoneLookupSelection(StringBuilder sb, String number) {
        sb.append("lookup.data_id=data._id AND data.raw_contact_id=raw_contacts._id"
                + " AND PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        DatabaseUtils.appendEscapedSQLString(sb, number);
        sb.append(mUseStrictPhoneNumberComparation ? ", 1)" : ", 0)");
    }

    /**
     * Loads common nickname mappings into the database.
     */
    private void loadNicknameLookupTable(SQLiteDatabase db) {
        String[] strings = mContext.getResources().getStringArray(
                com.android.internal.R.array.common_nicknames);
        if (strings == null || strings.length == 0) {
            return;
        }

        SQLiteStatement nicknameLookupInsert = db.compileStatement("INSERT INTO "
                + Tables.NICKNAME_LOOKUP + "(" + NicknameLookupColumns.NAME + ","
                + NicknameLookupColumns.CLUSTER + ") VALUES (?,?)");

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
}
