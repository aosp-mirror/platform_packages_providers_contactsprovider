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

import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.provider.BaseColumns;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.SocialContract.Activities;
import android.telephony.PhoneNumberUtils;
import android.util.Log;

import java.util.HashMap;

/**
 * Database open helper for contacts and social activity data. Designed as a
 * singleton to make sure that all {@link android.content.ContentProvider} users get the same
 * reference. Provides handy methods for maintaining package and mime-type
 * lookup tables.
 */
/* package */ class OpenHelper extends SQLiteOpenHelper {
    private static final String TAG = "OpenHelper";

    private static final int DATABASE_VERSION = 55;
    private static final String DATABASE_NAME = "contacts2.db";
    private static final String DATABASE_PRESENCE = "presence_db";

    public interface Delegate {
        void createDatabase(SQLiteDatabase db);
    }

    public interface Tables {
        public static final String ACCOUNTS = "accounts";
        public static final String CONTACTS = "contacts";
        public static final String RAW_CONTACTS = "raw_contacts";
        public static final String PACKAGES = "packages";
        public static final String MIMETYPES = "mimetypes";
        public static final String PHONE_LOOKUP = "phone_lookup";
        public static final String NAME_LOOKUP = "name_lookup";
        public static final String AGGREGATION_EXCEPTIONS = "agg_exceptions";
        public static final String DATA = "data";
        public static final String GROUPS = "groups";
        public static final String PRESENCE = "presence";
        public static final String NICKNAME_LOOKUP = "nickname_lookup";

        public static final String CONTACTS_JOIN_PRESENCE_PRIMARY_PHONE = "contacts "
                + "LEFT OUTER JOIN raw_contacts ON (contacts._id = raw_contacts.contact_id) "
                + "LEFT OUTER JOIN presence ON (raw_contacts._id = presence.raw_contact_id) "
                + "LEFT OUTER JOIN data ON (primary_phone_id = data._id)";

        public static final String DATA_JOIN_MIMETYPES = "data "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id)";

        public static final String DATA_JOIN_MIMETYPE_RAW_CONTACTS = "data "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        public static final String DATA_JOIN_RAW_CONTACTS_GROUPS = "data "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)"
                + "LEFT OUTER JOIN groups ON (groups._id = data." + GroupMembership.GROUP_ROW_ID
                + ")";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS = "data "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS = "data "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_MIMETYPES_RAW_CONTACTS_CONTACTS = "data "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_CONTACTS_GROUPS =
                "data "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN groups "
                + "  ON (mimetypes.mimetype='" + GroupMembership.CONTENT_ITEM_TYPE + "' "
                + "      AND groups._id = data." + GroupMembership.GROUP_ROW_ID + ") "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
                + "LEFT OUTER JOIN contacts ON (raw_contacts.contact_id = contacts._id)";

        public static final String DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_GROUPS = "data "
                + "LEFT OUTER JOIN packages ON (data.package_id = packages._id) "
                + "LEFT OUTER JOIN mimetypes ON (data.mimetype_id = mimetypes._id) "
                + "LEFT OUTER JOIN raw_contacts ON (data.raw_contact_id = raw_contacts._id) "
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

        public static final String AGGREGATION_EXCEPTIONS_JOIN_RAW_CONTACTS = "agg_exceptions "
                + "INNER JOIN raw_contacts raw_contacts1 "
                + "ON (agg_exceptions.raw_contact_id1 = raw_contacts1._id) ";

        public static final String AGGREGATION_EXCEPTIONS_JOIN_RAW_CONTACTS_TWICE =
                "agg_exceptions "
                + "INNER JOIN raw_contacts raw_contacts1 "
                + "ON (agg_exceptions.raw_contact_id1 = raw_contacts1._id) "
                + "INNER JOIN raw_contacts raw_contacts2 "
                + "ON (agg_exceptions.raw_contact_id2 = raw_contacts2._id) ";
    }

    public interface Clauses {
        public static final String WHERE_IM_MATCHES = MimetypesColumns.MIMETYPE + "=" + Im.MIMETYPE
                + " AND " + Im.PROTOCOL + "=? AND " + Im.DATA + "=?";

        public static final String WHERE_EMAIL_MATCHES = MimetypesColumns.MIMETYPE + "="
                + Email.MIMETYPE + " AND " + Email.DATA + "=?";

        public static final String MIMETYPE_IS_GROUP_MEMBERSHIP = MimetypesColumns.CONCRETE_MIMETYPE
                + "='" + GroupMembership.CONTENT_ITEM_TYPE + "'";

        public static final String BELONGS_TO_GROUP = DataColumns.CONCRETE_GROUP_ID + "="
                + GroupsColumns.CONCRETE_ID;

        public static final String HAS_PRIMARY_PHONE = "("
                + ContactsColumns.OPTIMAL_PRIMARY_PHONE_ID + " IS NOT NULL OR "
                + ContactsColumns.FALLBACK_PRIMARY_PHONE_ID + " IS NOT NULL)";

        // TODO: add in check against package_visible
        public static final String IN_VISIBLE_GROUP = "SELECT MIN(COUNT(" + DataColumns.CONCRETE_ID
                + "),1) FROM " + Tables.DATA_JOIN_RAW_CONTACTS_GROUPS + " WHERE "
                + DataColumns.MIMETYPE_ID + "=? AND " + RawContacts.CONTACT_ID + "="
                + ContactsColumns.CONCRETE_ID + " AND " + Groups.GROUP_VISIBLE + "=1";

        public static final String GROUP_HAS_ACCOUNT_AND_SOURCE_ID =
                Groups.SOURCE_ID + "=? AND "
                        + Groups.ACCOUNT_NAME + "=? AND "
                        + Groups.ACCOUNT_TYPE + "=?";
    }

    public interface ContactsColumns {
        public static final String OPTIMAL_PRIMARY_PHONE_ID = "optimal_phone_id";
        public static final String OPTIMAL_PRIMARY_PHONE_IS_RESTRICTED =
                "optimal_phone_is_restricted";
        public static final String FALLBACK_PRIMARY_PHONE_ID = "fallback_phone_id";

        public static final String OPTIMAL_PRIMARY_EMAIL_ID = "optimal_email_id";
        public static final String OPTIMAL_PRIMARY_EMAIL_IS_RESTRICTED =
                "optimal_email_is_restricted";
        public static final String FALLBACK_PRIMARY_EMAIL_ID = "fallback_email_id";

        public static final String SINGLE_IS_RESTRICTED = "single_is_restricted";

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
        public static final String DISPLAY_NAME = "display_name";
    }

    public interface DataColumns {
        public static final String PACKAGE_ID = "package_id";
        public static final String MIMETYPE_ID = "mimetype_id";

        public static final String CONCRETE_ID = Tables.DATA + "." + BaseColumns._ID;
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
        public static final String _ID = BaseColumns._ID;
        public static final String RAW_CONTACT_ID = "raw_contact_id";
        public static final String NORMALIZED_NAME = "normalized_name";
        public static final String NAME_TYPE = "name_type";
    }

    public final static class NameLookupType {
        public static final int FULL_NAME = 0;
        public static final int FULL_NAME_CONCATENATED = 1;
        public static final int FULL_NAME_REVERSE = 2;
        public static final int FULL_NAME_REVERSE_CONCATENATED = 3;
        public static final int FULL_NAME_WITH_NICKNAME = 4;
        public static final int FULL_NAME_WITH_NICKNAME_REVERSE = 5;
        public static final int GIVEN_NAME_ONLY = 6;
        public static final int GIVEN_NAME_ONLY_AS_NICKNAME = 7;
        public static final int FAMILY_NAME_ONLY = 8;
        public static final int FAMILY_NAME_ONLY_AS_NICKNAME = 9;
        public static final int NICKNAME = 10;
        public static final int EMAIL_BASED_NICKNAME = 11;

        // This is the highest name lookup type code plus one
        public static final int TYPE_COUNT = 12;

        public static boolean isBasedOnStructuredName(int nameLookupType) {
            return nameLookupType != NameLookupType.EMAIL_BASED_NICKNAME
                    && nameLookupType != NameLookupType.NICKNAME;
        }
    }

    public interface PackagesColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String PACKAGE = "package";
    }

    public interface MimetypesColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String MIMETYPE = "mimetype";

        public static final String CONCRETE_ID = Tables.MIMETYPES + "." + BaseColumns._ID;
        public static final String CONCRETE_MIMETYPE = Tables.MIMETYPES + "." + MIMETYPE;
    }

    public interface AggregationExceptionColumns {
        public static final String _ID = BaseColumns._ID;
        public static final String RAW_CONTACT_ID1 = "raw_contact_id1";
        public static final String RAW_CONTACT_ID2 = "raw_contact_id2";
    }

    public interface NicknameLookupColumns {
        public static final String NAME = "name";
        public static final String CLUSTER = "cluster";
    }

    private static final String[] NICKNAME_LOOKUP_COLUMNS = new String[] {
        NicknameLookupColumns.CLUSTER
    };

    private static final int COL_NICKNAME_LOOKUP_CLUSTER = 0;

    /** In-memory cache of previously found mimetype mappings */
    private final HashMap<String, Long> mMimetypeCache = new HashMap<String, Long>();
    /** In-memory cache of previously found package name mappings */
    private final HashMap<String, Long> mPackageCache = new HashMap<String, Long>();


    /** Compiled statements for querying and inserting mappings */
    private SQLiteStatement mMimetypeQuery;
    private SQLiteStatement mPackageQuery;
    private SQLiteStatement mContactIdQuery;
    private SQLiteStatement mAggregationModeQuery;
    private SQLiteStatement mContactIdUpdate;
    private SQLiteStatement mMimetypeInsert;
    private SQLiteStatement mPackageInsert;
    private SQLiteStatement mNameLookupInsert;

    private SQLiteStatement mDataMimetypeQuery;
    private SQLiteStatement mActivitiesMimetypeQuery;

    private final Context mContext;
    private final SyncStateContentProviderHelper mSyncState;
    private HashMap<String, String[]> mNicknameClusterCache;

    /** Compiled statements for updating {@link Contacts#IN_VISIBLE_GROUP}. */
    private SQLiteStatement mVisibleAllUpdate;
    private SQLiteStatement mVisibleSpecificUpdate;

    private Delegate mDelegate;

    private static OpenHelper sSingleton = null;

    public static synchronized OpenHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new OpenHelper(context);
        }
        return sSingleton;
    }

    /**
     * Private constructor, callers except unit tests should obtain an instance through
     * {@link #getInstance(android.content.Context)} instead.
     */
    /* package */ OpenHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        Log.i(TAG, "Creating OpenHelper");

        mContext = context;
        mSyncState = new SyncStateContentProviderHelper();
    }

    public Delegate getDelegate() {
        return mDelegate;
    }

    public void setDelegate(Delegate delegate) {
        mDelegate = delegate;
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
        mContactIdUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContacts.CONTACT_ID + "=?" + " WHERE " + RawContacts._ID + "=?");
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
        mNameLookupInsert = db.compileStatement("INSERT INTO " + Tables.NAME_LOOKUP + "("
                + NameLookupColumns.RAW_CONTACT_ID + "," + NameLookupColumns.NAME_TYPE + ","
                + NameLookupColumns.NORMALIZED_NAME + ") VALUES (?,?,?)");

        final String visibleUpdate = "UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.IN_VISIBLE_GROUP + "= (" + Clauses.IN_VISIBLE_GROUP + ")";

        mVisibleAllUpdate = db.compileStatement(visibleUpdate);
        mVisibleSpecificUpdate = db.compileStatement(visibleUpdate + " WHERE "
                + ContactsColumns.CONCRETE_ID + "=?");

        // Make sure we have an in-memory presence table
        final String tableName = DATABASE_PRESENCE + "." + Tables.PRESENCE;
        final String indexName = DATABASE_PRESENCE + ".presenceIndex";

        db.execSQL("ATTACH DATABASE ':memory:' AS " + DATABASE_PRESENCE + ";");
        db.execSQL("CREATE TABLE IF NOT EXISTS " + tableName + " ("+
                Presence._ID + " INTEGER PRIMARY KEY," +
                Presence.RAW_CONTACT_ID + " INTEGER REFERENCES raw_contacts(_id)," +
                Presence.DATA_ID + " INTEGER REFERENCES data(_id)," +
                Presence.IM_PROTOCOL + " TEXT," +
                Presence.IM_HANDLE + " TEXT," +
                Presence.IM_ACCOUNT + " TEXT," +
                Presence.PRESENCE_STATUS + " INTEGER," +
                Presence.PRESENCE_CUSTOM_STATUS + " TEXT," +
                "UNIQUE(" + Presence.IM_PROTOCOL + ", " + Presence.IM_HANDLE + ", "
                        + Presence.IM_ACCOUNT + ")" +
        ");");

        db.execSQL("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + Tables.PRESENCE + " ("
                + Presence.RAW_CONTACT_ID + ");");
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
                ContactsColumns.OPTIMAL_PRIMARY_PHONE_ID + " INTEGER REFERENCES data(_id)," +
                ContactsColumns.OPTIMAL_PRIMARY_PHONE_IS_RESTRICTED + " INTEGER DEFAULT 0," +
                ContactsColumns.FALLBACK_PRIMARY_PHONE_ID + " INTEGER REFERENCES data(_id)," +
                ContactsColumns.OPTIMAL_PRIMARY_EMAIL_ID + " INTEGER REFERENCES data(_id)," +
                ContactsColumns.OPTIMAL_PRIMARY_EMAIL_IS_RESTRICTED + " INTEGER DEFAULT 0," +
                ContactsColumns.FALLBACK_PRIMARY_EMAIL_ID + " INTEGER REFERENCES data(_id)," +
                ContactsColumns.SINGLE_IS_RESTRICTED + " INTEGER REFERENCES package(_id)" +
        ");");

        // Contacts table
        db.execSQL("CREATE TABLE " + Tables.RAW_CONTACTS + " (" +
                RawContacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                RawContacts.IS_RESTRICTED + " INTEGER DEFAULT 0," +
                RawContacts.ACCOUNT_NAME + " STRING DEFAULT NULL, " +
                RawContacts.ACCOUNT_TYPE + " STRING DEFAULT NULL, " +
                RawContacts.SOURCE_ID + " TEXT," +
                RawContacts.VERSION + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.DIRTY + " INTEGER NOT NULL DEFAULT 1," +
                RawContacts.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.CONTACT_ID + " INTEGER," +
                RawContacts.AGGREGATION_MODE + " INTEGER NOT NULL DEFAULT " +
                        RawContacts.AGGREGATION_MODE_DEFAULT + "," +
                RawContacts.CUSTOM_RINGTONE + " TEXT," +
                RawContacts.SEND_TO_VOICEMAIL + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                RawContacts.LAST_TIME_CONTACTED + " INTEGER," +
                RawContacts.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                RawContactsColumns.DISPLAY_NAME + " TEXT" +
        ");");

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
                Data.RAW_CONTACT_ID + " INTEGER NOT NULL," +
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
                Data.DATA15 + " TEXT" +
        ");");

        /**
         * Automatically delete Data rows when a raw contact is deleted.
         */
        db.execSQL("CREATE TRIGGER " + Tables.RAW_CONTACTS + "_deleted "
                + "   BEFORE DELETE ON " + Tables.RAW_CONTACTS
                + " BEGIN "
                + "   DELETE FROM " + Tables.DATA
                + "     WHERE " + Data.RAW_CONTACT_ID + "=OLD." + RawContacts._ID + ";"
                + "   DELETE FROM " + Tables.PHONE_LOOKUP
                + "     WHERE " + PhoneLookupColumns.RAW_CONTACT_ID + "=OLD." + RawContacts._ID + ";"
                + " END");

        /**
         * Triggers that set {@link RawContacts#DIRTY} and update {@link RawContacts#VERSION}
         * when the contact is marked for deletion or any time a data row is inserted, updated
         * or deleted.
         */
        db.execSQL("CREATE TRIGGER " + Tables.RAW_CONTACTS + "_marked_deleted "
                + "   BEFORE UPDATE ON " + Tables.RAW_CONTACTS
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET "
                +         RawContacts.VERSION + "=OLD." + RawContacts.VERSION + "+1, "
                +         RawContacts.DIRTY + "=1"
                + "     WHERE " + RawContacts._ID + "=OLD." + RawContacts._ID
                + "       AND NEW." + RawContacts.DELETED + "!= OLD." + RawContacts.DELETED + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_updated AFTER UPDATE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.DATA
                + "     SET " + Data.DATA_VERSION + "=OLD." + Data.DATA_VERSION + "+1 "
                + "     WHERE " + Data._ID + "=OLD." + Data._ID + ";"
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET " + RawContacts.DIRTY + "=1, "
                + "         " +	RawContacts.VERSION + "=" + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + Data.RAW_CONTACT_ID + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_inserted BEFORE INSERT ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET " + RawContacts.DIRTY + "=1, "
                + "         " + RawContacts.VERSION + "=" + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=NEW." + Data.RAW_CONTACT_ID + ";"
                + " END");

        db.execSQL("CREATE TRIGGER " + Tables.DATA + "_deleted BEFORE DELETE ON " + Tables.DATA
                + " BEGIN "
                + "   UPDATE " + Tables.RAW_CONTACTS
                + "     SET " + RawContacts.DIRTY + "=1,"
                + "         " + RawContacts.VERSION + "=" + RawContacts.VERSION + "+1 "
                + "     WHERE " + RawContacts._ID + "=OLD." + Data.RAW_CONTACT_ID + ";"
                + "   DELETE FROM " + Tables.PHONE_LOOKUP
                + "     WHERE " + PhoneLookupColumns.DATA_ID + "=OLD." + Data._ID + ";"
                + " END");

        // Private phone numbers table used for lookup
        db.execSQL("CREATE TABLE " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns._ID + " INTEGER PRIMARY KEY," +
                PhoneLookupColumns.DATA_ID + " INTEGER REFERENCES data(_id) NOT NULL," +
                PhoneLookupColumns.RAW_CONTACT_ID
                        + " INTEGER REFERENCES raw_contacts(_id) NOT NULL," +
                PhoneLookupColumns.NORMALIZED_NUMBER + " TEXT NOT NULL" +
        ");");

        db.execSQL("CREATE INDEX phone_lookup_index ON " + Tables.PHONE_LOOKUP + " (" +
                PhoneLookupColumns.NORMALIZED_NUMBER + " ASC, " +
                PhoneLookupColumns.RAW_CONTACT_ID + ", " +
                PhoneLookupColumns.DATA_ID +
        ");");

        // Private name/nickname table used for lookup
        db.execSQL("CREATE TABLE " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                NameLookupColumns.RAW_CONTACT_ID
                        + " INTEGER REFERENCES raw_contacts(_id) NOT NULL," +
                NameLookupColumns.NORMALIZED_NAME + " TEXT," +
                NameLookupColumns.NAME_TYPE + " INTEGER" +
        ");");

        db.execSQL("CREATE INDEX name_lookup_index ON " + Tables.NAME_LOOKUP + " (" +
                NameLookupColumns.NORMALIZED_NAME + " ASC, " +
                NameLookupColumns.NAME_TYPE + " ASC, " +
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
                Groups.DIRTY + " INTEGER NOT NULL DEFAULT 1," +
                Groups.TITLE + " TEXT," +
                Groups.TITLE_RES + " INTEGER," +
                Groups.NOTES + " TEXT," +
                Groups.SYSTEM_ID + " TEXT," +
                Groups.GROUP_VISIBLE + " INTEGER" +
        ");");

        db.execSQL("CREATE TRIGGER " + Tables.GROUPS + "_updated1 "
                + "   BEFORE UPDATE ON " + Tables.GROUPS
                + " BEGIN "
                + "   UPDATE " + Tables.GROUPS
                + "     SET "
                +         Groups.VERSION + "=OLD." + Groups.VERSION + "+1, "
                +         Groups.DIRTY + "=1"
                + "     WHERE " + Groups._ID + "=OLD." + Groups._ID + ";"
                + " END");

        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                AggregationExceptions.TYPE + " INTEGER NOT NULL, " +
                AggregationExceptionColumns.RAW_CONTACT_ID1
                        + " INTEGER REFERENCES raw_contacts(_id), " +
                AggregationExceptionColumns.RAW_CONTACT_ID2
                        + " INTEGER REFERENCES raw_contacts(_id)" +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index1 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns.RAW_CONTACT_ID1 + ", " +
                AggregationExceptionColumns.RAW_CONTACT_ID2 +
        ");");

        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS aggregation_exception_index2 ON " +
                Tables.AGGREGATION_EXCEPTIONS + " (" +
                AggregationExceptionColumns.RAW_CONTACT_ID2 + ", " +
                AggregationExceptionColumns.RAW_CONTACT_ID1 +
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

        loadNicknameLookupTable(db);
        if (mDelegate != null) {
            mDelegate.createDatabase(db);
        }
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG, "Upgrading from version " + oldVersion + " to " + newVersion
                + ", data will be lost!");

        db.execSQL("DROP TABLE IF EXISTS " + Tables.ACCOUNTS + ";");
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

        // TODO: we should not be dropping agg_exceptions and contact_options. In case that table's
        // schema changes, we should try to preserve the data, because it was entered by the user
        // and has never been synched to the server.
        db.execSQL("DROP TABLE IF EXISTS " + Tables.AGGREGATION_EXCEPTIONS + ";");

        onCreate(db);

        // TODO: eventually when this supports upgrades we should do something like the following:
//        if (!upgradeDatabase(db, oldVersion, newVersion)) {
//            mSyncState.discardSyncData(db, null /* all accounts */);
//            ContentResolver.requestSync(null /* all accounts */,
//                    mContentUri.getAuthority(), new Bundle());
//        }
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
        db.execSQL("DELETE FROM " + Tables.ACTIVITIES + ";");

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
    private synchronized long getCachedId(SQLiteStatement query, SQLiteStatement insert,
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
        mVisibleAllUpdate.bindLong(1, groupMembershipMimetypeId);
        mVisibleAllUpdate.execute();
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
     * Updates the contact ID for the specified contact.
     */
    public void setContactId(long rawContactId, long contactId) {
        getWritableDatabase();
        DatabaseUtils.bindObjectToProgram(mContactIdUpdate, 1, contactId);
        DatabaseUtils.bindObjectToProgram(mContactIdUpdate, 2, rawContactId);
        mContactIdUpdate.execute();
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
            // No valid mapping found, so return -1
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

    /**
     * Inserts a record in the {@link Tables#NAME_LOOKUP} table.
     */
    public void insertNameLookup(long rawContactId, int lookupType, String name) {
        getWritableDatabase();
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 1, rawContactId);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 2, lookupType);
        DatabaseUtils.bindObjectToProgram(mNameLookupInsert, 3, name);
        mNameLookupInsert.executeInsert();
    }

    public static void buildPhoneLookupQuery(SQLiteQueryBuilder qb, final String number) {
        final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
        final StringBuilder tables = new StringBuilder();
        tables.append(Tables.RAW_CONTACTS + ", (SELECT data_id FROM phone_lookup "
                + "WHERE (phone_lookup.normalized_number GLOB '");
        tables.append(normalizedNumber);
        tables.append("*')) AS lookup, " + Tables.DATA_JOIN_MIMETYPES);
        qb.setTables(tables.toString());
        qb.appendWhere("lookup.data_id=data._id AND data.raw_contact_id=raw_contacts._id AND ");
        qb.appendWhere("PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
        qb.appendWhereEscapeString(number);
        qb.appendWhere(")");
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

    /**
     * Returns common nickname cluster IDs for a given name. For example, it
     * will return the same value for "Robert", "Bob" and "Rob". Some names belong to multiple
     * clusters, e.g. Leo could be Leonard or Leopold.
     *
     * May return null.
     *
     * @param normalizedName A normalized first name, see {@link NameNormalizer#normalize}.
     */
    public String[] getCommonNicknameClusters(String normalizedName) {
        if (mNicknameClusterCache == null) {
            mNicknameClusterCache = new HashMap<String, String[]>();
        }

        synchronized (mNicknameClusterCache) {
            if (mNicknameClusterCache.containsKey(normalizedName)) {
                return mNicknameClusterCache.get(normalizedName);
            }
        }

        String[] clusters = null;
        SQLiteDatabase db = getReadableDatabase();

        Cursor cursor = db.query(Tables.NICKNAME_LOOKUP, NICKNAME_LOOKUP_COLUMNS,
                NicknameLookupColumns.NAME + "=?", new String[] { normalizedName },
                null, null, null);
        try {
            int count = cursor.getCount();
            if (count > 0) {
                clusters = new String[count];
                for (int i = 0; i < count; i++) {
                    cursor.moveToNext();
                    clusters[i] = cursor.getString(COL_NICKNAME_LOOKUP_CLUSTER);
                }
            }
        } finally {
            cursor.close();
        }

        synchronized (mNicknameClusterCache) {
            mNicknameClusterCache.put(normalizedName, clusters);
        }

        return clusters;
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
            } else {
                longValue = ((Number) value).longValue();
            }
            toValues.put(toKey, longValue);
        }
    }

    public SyncStateContentProviderHelper getSyncState() {
        return mSyncState;
    }
}
