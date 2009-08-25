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

import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.ExtensionsColumns;
import com.android.providers.contacts.OpenHelper.GroupsColumns;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.PhoneColumns;
import com.android.providers.contacts.OpenHelper.PresenceColumns;
import com.android.providers.contacts.OpenHelper.RawContactsColumns;
import com.android.providers.contacts.OpenHelper.Tables;

import android.accounts.Account;
import android.app.SearchManager;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.Contacts.ContactMethods;
import android.provider.Contacts.People;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;

import java.util.HashMap;

public class LegacyApiSupport implements OpenHelper.Delegate {

    private static final String TAG = "ContactsProviderV1";

    private static final String NON_EXISTENT_ACCOUNT_TYPE = "android.INVALID_ACCOUNT_TYPE";
    private static final String NON_EXISTENT_ACCOUNT_NAME = "invalid";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int PEOPLE = 1;
    private static final int PEOPLE_ID = 2;
    private static final int PEOPLE_UPDATE_CONTACT_TIME = 3;
    private static final int ORGANIZATIONS = 4;
    private static final int ORGANIZATIONS_ID = 5;
    private static final int PEOPLE_CONTACTMETHODS = 6;
    private static final int PEOPLE_CONTACTMETHODS_ID = 7;
    private static final int CONTACTMETHODS = 8;
    private static final int CONTACTMETHODS_ID = 9;
    private static final int PEOPLE_PHONES = 10;
    private static final int PEOPLE_PHONES_ID = 11;
    private static final int PHONES = 12;
    private static final int PHONES_ID = 13;
    private static final int EXTENSIONS = 14;
    private static final int EXTENSIONS_ID = 15;
    private static final int PEOPLE_EXTENSIONS = 16;
    private static final int PEOPLE_EXTENSIONS_ID = 17;
    private static final int GROUPS = 18;
    private static final int GROUPS_ID = 19;
    private static final int GROUPMEMBERSHIP = 20;
    private static final int GROUPMEMBERSHIP_ID = 21;
    private static final int PEOPLE_GROUPMEMBERSHIP = 22;
    private static final int PEOPLE_GROUPMEMBERSHIP_ID = 23;
    private static final int PEOPLE_PHOTO = 24;
    private static final int PHOTOS = 25;
    private static final int PHOTOS_ID = 26;
    private static final int PEOPLE_FILTER = 29;
    private static final int DELETED_PEOPLE = 30;
    private static final int DELETED_GROUPS = 31;
    private static final int SEARCH_SUGGESTIONS = 32;
    private static final int SEARCH_SHORTCUT = 33;
    private static final int PHONES_FILTER = 34;

    private static final String PEOPLE_JOINS =
            " LEFT OUTER JOIN data name ON (raw_contacts._id = name.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = name.mimetype_id)"
                    + "='" + StructuredName.CONTENT_ITEM_TYPE + "')"
            + " LEFT OUTER JOIN data organization ON (raw_contacts._id = organization.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = organization.mimetype_id)"
                    + "='" + Organization.CONTENT_ITEM_TYPE + "' AND organization.is_primary)"
            + " LEFT OUTER JOIN data email ON (raw_contacts._id = email.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = email.mimetype_id)"
                    + "='" + Email.CONTENT_ITEM_TYPE + "' AND email.is_primary)"
            + " LEFT OUTER JOIN data note ON (raw_contacts._id = note.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = note.mimetype_id)"
                    + "='" + Note.CONTENT_ITEM_TYPE + "')"
            + " LEFT OUTER JOIN data phone ON (raw_contacts._id = phone.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = phone.mimetype_id)"
                    + "='" + Phone.CONTENT_ITEM_TYPE + "' AND phone.is_primary)";

    public static final String DATA_JOINS =
            " JOIN mimetypes ON (mimetypes._id = data.mimetype_id)"
            + " JOIN raw_contacts ON (raw_contacts._id = data.raw_contact_id)"
            + PEOPLE_JOINS;

    public static final String PRESENCE_JOINS =
            " LEFT OUTER JOIN presence ON ("
            + " presence.presence_id = (SELECT max(presence_id) FROM presence"
            + " WHERE view_v1_people._id = presence_raw_contact_id))";

    private static final String PHONETIC_NAME_SQL = "trim(trim("
            + "ifnull(name." + StructuredName.PHONETIC_GIVEN_NAME + ",' ')||' '||"
            + "ifnull(name." + StructuredName.PHONETIC_MIDDLE_NAME + ",' '))||' '||"
            + "ifnull(name." + StructuredName.PHONETIC_FAMILY_NAME + ",' ')) ";

    private static final String CONTACT_METHOD_KIND_SQL =
            "CAST ((CASE WHEN mimetype='" + Email.CONTENT_ITEM_TYPE + "'"
                + " THEN " + android.provider.Contacts.KIND_EMAIL
                + " ELSE"
                    + " (CASE WHEN mimetype='" + Im.CONTENT_ITEM_TYPE +"'"
                        + " THEN " + android.provider.Contacts.KIND_IM
                        + " ELSE"
                        + " (CASE WHEN mimetype='" + StructuredPostal.CONTENT_ITEM_TYPE + "'"
                            + " THEN "  + android.provider.Contacts.KIND_POSTAL
                            + " ELSE"
                                + " NULL"
                            + " END)"
                        + " END)"
                + " END) AS INTEGER)";

    private static final String IM_PROTOCOL_SQL =
            "(CASE WHEN " + Presence.PROTOCOL + "=" + Im.PROTOCOL_CUSTOM
                + " THEN 'custom:'||" + Presence.CUSTOM_PROTOCOL
                + " ELSE 'pre:'||" + Presence.PROTOCOL
                + " END)";

    private static String CONTACT_METHOD_DATA_SQL =
            "(CASE WHEN " + Data.MIMETYPE + "='" + Im.CONTENT_ITEM_TYPE + "'"
                + " THEN (CASE WHEN " + Tables.DATA + "." + Im.PROTOCOL + "=" + Im.PROTOCOL_CUSTOM
                    + " THEN 'custom:'||" + Tables.DATA + "." + Im.CUSTOM_PROTOCOL
                    + " ELSE 'pre:'||" + Tables.DATA + "." + Im.PROTOCOL
                    + " END)"
                + " ELSE " + DataColumns.CONCRETE_DATA2
                + " END)";


    public interface LegacyTables {
        public static final String PEOPLE = "view_v1_people";
        public static final String PEOPLE_JOIN_PRESENCE = "view_v1_people" + PRESENCE_JOINS;
        public static final String GROUPS = "view_v1_groups";
        public static final String ORGANIZATIONS = "view_v1_organizations";
        public static final String CONTACT_METHODS = "view_v1_contact_methods";
        public static final String PHONES = "view_v1_phones";
        public static final String EXTENSIONS = "view_v1_extensions";
        public static final String GROUP_MEMBERSHIP = "view_v1_group_membership";
        public static final String PHOTOS = "view_v1_photos";
        public static final String PRESENCE_JOIN_CONTACTS = Tables.PRESENCE +
                " LEFT OUTER JOIN " + Tables.RAW_CONTACTS
                + " ON (" + Tables.PRESENCE + "." + PresenceColumns.RAW_CONTACT_ID + "="
                + RawContactsColumns.CONCRETE_ID + ")";
    }

    private static final String[] ORGANIZATION_MIME_TYPES = new String[] {
        Organization.CONTENT_ITEM_TYPE
    };

    private static final String[] CONTACT_METHOD_MIME_TYPES = new String[] {
        Email.CONTENT_ITEM_TYPE,
        Im.CONTENT_ITEM_TYPE,
        StructuredPostal.CONTENT_ITEM_TYPE,
    };

    private static final String[] PHONE_MIME_TYPES = new String[] {
        Phone.CONTENT_ITEM_TYPE
    };

    private interface PhotoQuery {
        String[] COLUMNS = { Data._ID };

        int _ID = 0;
    }

    /**
     * A custom data row that is used to store legacy photo data fields no
     * longer directly supported by the API.
     */
    private interface LegacyPhotoData {
        public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/photo_v1_extras";

        public static final String PHOTO_DATA_ID = Data.DATA1;
        public static final String LOCAL_VERSION = Data.DATA2;
        public static final String DOWNLOAD_REQUIRED = Data.DATA3;
        public static final String EXISTS_ON_SERVER = Data.DATA4;
        public static final String SYNC_ERROR = Data.DATA5;
    }

    public static final String LEGACY_PHOTO_JOIN =
            " LEFT OUTER JOIN data legacy_photo ON (raw_contacts._id = legacy_photo.raw_contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = legacy_photo.mimetype_id)"
                + "='" + LegacyPhotoData.CONTENT_ITEM_TYPE + "'"
            + " AND " + DataColumns.CONCRETE_ID + " = legacy_photo." + LegacyPhotoData.PHOTO_DATA_ID
            + ")";

    private static final HashMap<String, String> sPeopleProjectionMap;
    private static final HashMap<String, String> sOrganizationProjectionMap;
    private static final HashMap<String, String> sContactMethodProjectionMap;
    private static final HashMap<String, String> sPhoneProjectionMap;
    private static final HashMap<String, String> sExtensionProjectionMap;
    private static final HashMap<String, String> sGroupProjectionMap;
    private static final HashMap<String, String> sGroupMembershipProjectionMap;
    private static final HashMap<String, String> sPhotoProjectionMap;


    static {

        // Contacts URI matching table
        UriMatcher matcher = sUriMatcher;

        String authority = android.provider.Contacts.AUTHORITY;
        matcher.addURI(authority, "extensions", EXTENSIONS);
        matcher.addURI(authority, "extensions/#", EXTENSIONS_ID);
        matcher.addURI(authority, "groups", GROUPS);
        matcher.addURI(authority, "groups/#", GROUPS_ID);
//        matcher.addURI(authority, "groups/name/*/members", GROUP_NAME_MEMBERS);
//        matcher.addURI(authority, "groups/name/*/members/filter/*",
//                GROUP_NAME_MEMBERS_FILTER);
//        matcher.addURI(authority, "groups/system_id/*/members", GROUP_SYSTEM_ID_MEMBERS);
//        matcher.addURI(authority, "groups/system_id/*/members/filter/*",
//                GROUP_SYSTEM_ID_MEMBERS_FILTER);
        matcher.addURI(authority, "groupmembership", GROUPMEMBERSHIP);
        matcher.addURI(authority, "groupmembership/#", GROUPMEMBERSHIP_ID);
//        matcher.addURI(authority, "groupmembershipraw", GROUPMEMBERSHIP_RAW);
        matcher.addURI(authority, "people", PEOPLE);
//        matcher.addURI(authority, "people/strequent", PEOPLE_STREQUENT);
//        matcher.addURI(authority, "people/strequent/filter/*", PEOPLE_STREQUENT_FILTER);
        matcher.addURI(authority, "people/filter/*", PEOPLE_FILTER);
//        matcher.addURI(authority, "people/with_phones_filter/*",
//                PEOPLE_WITH_PHONES_FILTER);
//        matcher.addURI(authority, "people/with_email_or_im_filter/*",
//                PEOPLE_WITH_EMAIL_OR_IM_FILTER);
        matcher.addURI(authority, "people/#", PEOPLE_ID);
        matcher.addURI(authority, "people/#/extensions", PEOPLE_EXTENSIONS);
        matcher.addURI(authority, "people/#/extensions/#", PEOPLE_EXTENSIONS_ID);
        matcher.addURI(authority, "people/#/phones", PEOPLE_PHONES);
        matcher.addURI(authority, "people/#/phones/#", PEOPLE_PHONES_ID);
//        matcher.addURI(authority, "people/#/phones_with_presence",
//                PEOPLE_PHONES_WITH_PRESENCE);
        matcher.addURI(authority, "people/#/photo", PEOPLE_PHOTO);
//        matcher.addURI(authority, "people/#/photo/data", PEOPLE_PHOTO_DATA);
        matcher.addURI(authority, "people/#/contact_methods", PEOPLE_CONTACTMETHODS);
//        matcher.addURI(authority, "people/#/contact_methods_with_presence",
//                PEOPLE_CONTACTMETHODS_WITH_PRESENCE);
        matcher.addURI(authority, "people/#/contact_methods/#", PEOPLE_CONTACTMETHODS_ID);
//        matcher.addURI(authority, "people/#/organizations", PEOPLE_ORGANIZATIONS);
//        matcher.addURI(authority, "people/#/organizations/#", PEOPLE_ORGANIZATIONS_ID);
        matcher.addURI(authority, "people/#/groupmembership", PEOPLE_GROUPMEMBERSHIP);
        matcher.addURI(authority, "people/#/groupmembership/#", PEOPLE_GROUPMEMBERSHIP_ID);
//        matcher.addURI(authority, "people/raw", PEOPLE_RAW);
//        matcher.addURI(authority, "people/owner", PEOPLE_OWNER);
        matcher.addURI(authority, "people/#/update_contact_time",
                PEOPLE_UPDATE_CONTACT_TIME);
        matcher.addURI(authority, "deleted_people", DELETED_PEOPLE);
        matcher.addURI(authority, "deleted_groups", DELETED_GROUPS);
        matcher.addURI(authority, "phones", PHONES);
//        matcher.addURI(authority, "phones_with_presence", PHONES_WITH_PRESENCE);
        matcher.addURI(authority, "phones/filter/*", PHONES_FILTER);
//        matcher.addURI(authority, "phones/filter_name/*", PHONES_FILTER_NAME);
//        matcher.addURI(authority, "phones/mobile_filter_name/*",
//                PHONES_MOBILE_FILTER_NAME);
        matcher.addURI(authority, "phones/#", PHONES_ID);
        matcher.addURI(authority, "photos", PHOTOS);
        matcher.addURI(authority, "photos/#", PHOTOS_ID);
        matcher.addURI(authority, "contact_methods", CONTACTMETHODS);
//        matcher.addURI(authority, "contact_methods/email", CONTACTMETHODS_EMAIL);
//        matcher.addURI(authority, "contact_methods/email/*", CONTACTMETHODS_EMAIL_FILTER);
        matcher.addURI(authority, "contact_methods/#", CONTACTMETHODS_ID);
//        matcher.addURI(authority, "contact_methods/with_presence",
//                CONTACTMETHODS_WITH_PRESENCE);
        matcher.addURI(authority, "organizations", ORGANIZATIONS);
        matcher.addURI(authority, "organizations/#", ORGANIZATIONS_ID);
//        matcher.addURI(authority, "voice_dialer_timestamp", VOICE_DIALER_TIMESTAMP);
        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_QUERY,
                SEARCH_SUGGESTIONS);
        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_QUERY + "/*",
                SEARCH_SUGGESTIONS);
        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/#",
                SEARCH_SHORTCUT);
//        matcher.addURI(authority, "settings", SETTINGS);
//
//        matcher.addURI(authority, "live_folders/people", LIVE_FOLDERS_PEOPLE);
//        matcher.addURI(authority, "live_folders/people/*",
//                LIVE_FOLDERS_PEOPLE_GROUP_NAME);
//        matcher.addURI(authority, "live_folders/people_with_phones",
//                LIVE_FOLDERS_PEOPLE_WITH_PHONES);
//        matcher.addURI(authority, "live_folders/favorites",
//                LIVE_FOLDERS_PEOPLE_FAVORITES);


        HashMap<String, String> peopleProjectionMap = new HashMap<String, String>();
        peopleProjectionMap.put(People.NAME, People.NAME);
        peopleProjectionMap.put(People.DISPLAY_NAME, People.DISPLAY_NAME);
        peopleProjectionMap.put(People.PHONETIC_NAME, People.PHONETIC_NAME);
        peopleProjectionMap.put(People.NOTES, People.NOTES);
        peopleProjectionMap.put(People.TIMES_CONTACTED, People.TIMES_CONTACTED);
        peopleProjectionMap.put(People.LAST_TIME_CONTACTED, People.LAST_TIME_CONTACTED);
        peopleProjectionMap.put(People.CUSTOM_RINGTONE, People.CUSTOM_RINGTONE);
        peopleProjectionMap.put(People.SEND_TO_VOICEMAIL, People.SEND_TO_VOICEMAIL);
        peopleProjectionMap.put(People.STARRED, People.STARRED);

        sPeopleProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sPeopleProjectionMap.put(People._ID, People._ID);
        sPeopleProjectionMap.put(People.PRIMARY_ORGANIZATION_ID, People.PRIMARY_ORGANIZATION_ID);
        sPeopleProjectionMap.put(People.PRIMARY_EMAIL_ID, People.PRIMARY_EMAIL_ID);
        sPeopleProjectionMap.put(People.PRIMARY_PHONE_ID, People.PRIMARY_PHONE_ID);
        sPeopleProjectionMap.put(People.NUMBER, People.NUMBER);
        sPeopleProjectionMap.put(People.TYPE, People.TYPE);
        sPeopleProjectionMap.put(People.LABEL, People.LABEL);
        sPeopleProjectionMap.put(People.NUMBER_KEY, People.NUMBER_KEY);
        sPeopleProjectionMap.put(People.IM_PROTOCOL, IM_PROTOCOL_SQL + " AS " + People.IM_PROTOCOL);
        sPeopleProjectionMap.put(People.IM_HANDLE, People.IM_HANDLE);
        sPeopleProjectionMap.put(People.IM_ACCOUNT, People.IM_ACCOUNT);
        sPeopleProjectionMap.put(People.PRESENCE_STATUS, People.PRESENCE_STATUS);
        sPeopleProjectionMap.put(People.PRESENCE_CUSTOM_STATUS, People.PRESENCE_CUSTOM_STATUS);

        sOrganizationProjectionMap = new HashMap<String, String>();
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations._ID,
                android.provider.Contacts.Organizations._ID);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.PERSON_ID,
                android.provider.Contacts.Organizations.PERSON_ID);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.ISPRIMARY,
                android.provider.Contacts.Organizations.ISPRIMARY);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.COMPANY,
                android.provider.Contacts.Organizations.COMPANY);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.TYPE,
                android.provider.Contacts.Organizations.TYPE);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.LABEL,
                android.provider.Contacts.Organizations.LABEL);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.TITLE,
                android.provider.Contacts.Organizations.TITLE);

        sContactMethodProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sContactMethodProjectionMap.put(ContactMethods._ID, ContactMethods._ID);
        sContactMethodProjectionMap.put(ContactMethods.PERSON_ID, ContactMethods.PERSON_ID);
        sContactMethodProjectionMap.put(ContactMethods.KIND, ContactMethods.KIND);
        sContactMethodProjectionMap.put(ContactMethods.ISPRIMARY, ContactMethods.ISPRIMARY);
        sContactMethodProjectionMap.put(ContactMethods.TYPE, ContactMethods.TYPE);
        sContactMethodProjectionMap.put(ContactMethods.DATA, ContactMethods.DATA);
        sContactMethodProjectionMap.put(ContactMethods.LABEL, ContactMethods.LABEL);
        sContactMethodProjectionMap.put(ContactMethods.AUX_DATA, ContactMethods.AUX_DATA);

        sPhoneProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones._ID,
                android.provider.Contacts.Phones._ID);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.PERSON_ID,
                android.provider.Contacts.Phones.PERSON_ID);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.ISPRIMARY,
                android.provider.Contacts.Phones.ISPRIMARY);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.NUMBER,
                android.provider.Contacts.Phones.NUMBER);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.TYPE,
                android.provider.Contacts.Phones.TYPE);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.LABEL,
                android.provider.Contacts.Phones.LABEL);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.NUMBER_KEY,
                android.provider.Contacts.Phones.NUMBER_KEY);

        sExtensionProjectionMap = new HashMap<String, String>();
        sExtensionProjectionMap.put(android.provider.Contacts.Extensions._ID,
                android.provider.Contacts.Extensions._ID);
        sExtensionProjectionMap.put(android.provider.Contacts.Extensions.PERSON_ID,
                android.provider.Contacts.Extensions.PERSON_ID);
        sExtensionProjectionMap.put(android.provider.Contacts.Extensions.NAME,
                android.provider.Contacts.Extensions.NAME);
        sExtensionProjectionMap.put(android.provider.Contacts.Extensions.VALUE,
                android.provider.Contacts.Extensions.VALUE);

        sGroupProjectionMap = new HashMap<String, String>();
        sGroupProjectionMap.put(android.provider.Contacts.Groups._ID,
                android.provider.Contacts.Groups._ID);
        sGroupProjectionMap.put(android.provider.Contacts.Groups.NAME,
                android.provider.Contacts.Groups.NAME);
        sGroupProjectionMap.put(android.provider.Contacts.Groups.NOTES,
                android.provider.Contacts.Groups.NOTES);
        sGroupProjectionMap.put(android.provider.Contacts.Groups.SYSTEM_ID,
                android.provider.Contacts.Groups.SYSTEM_ID);

        sGroupMembershipProjectionMap = new HashMap<String, String>();
        sGroupMembershipProjectionMap.put(android.provider.Contacts.GroupMembership._ID,
                android.provider.Contacts.GroupMembership._ID);
        sGroupMembershipProjectionMap.put(android.provider.Contacts.GroupMembership.PERSON_ID,
                android.provider.Contacts.GroupMembership.PERSON_ID);
        sGroupMembershipProjectionMap.put(android.provider.Contacts.GroupMembership.GROUP_ID,
                android.provider.Contacts.GroupMembership.GROUP_ID);

        sPhotoProjectionMap = new HashMap<String, String>();
        sPhotoProjectionMap.put(android.provider.Contacts.Photos._ID,
                android.provider.Contacts.Photos._ID);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.PERSON_ID,
                android.provider.Contacts.Photos.PERSON_ID);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.DATA,
                android.provider.Contacts.Photos.DATA);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.LOCAL_VERSION,
                android.provider.Contacts.Photos.LOCAL_VERSION);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.DOWNLOAD_REQUIRED,
                android.provider.Contacts.Photos.DOWNLOAD_REQUIRED);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.EXISTS_ON_SERVER,
                android.provider.Contacts.Photos.EXISTS_ON_SERVER);
        sPhotoProjectionMap.put(android.provider.Contacts.Photos.SYNC_ERROR,
                android.provider.Contacts.Photos.SYNC_ERROR);
    }

    private final Context mContext;
    private final OpenHelper mOpenHelper;
    private final ContactsProvider2 mContactsProvider;
    private final NameSplitter mPhoneticNameSplitter;
    private final GlobalSearchSupport mGlobalSearchSupport;

    /** Precompiled sql statement for incrementing times contacted for a contact */
    private final SQLiteStatement mLastTimeContactedUpdate;

    private final ContentValues mValues = new ContentValues();
    private Account mAccount;

    public LegacyApiSupport(Context context, OpenHelper openHelper,
            ContactsProvider2 contactsProvider, GlobalSearchSupport globalSearchSupport) {
        mContext = context;
        mContactsProvider = contactsProvider;
        mOpenHelper = openHelper;
        mGlobalSearchSupport = globalSearchSupport;
        mOpenHelper.setDelegate(this);

        mPhoneticNameSplitter = new NameSplitter("", "", "",
                context.getString(com.android.internal.R.string.common_name_conjunctions));

        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        mLastTimeContactedUpdate = db.compileStatement("UPDATE " + Tables.RAW_CONTACTS + " SET "
                + RawContacts.TIMES_CONTACTED + "="
                + RawContacts.TIMES_CONTACTED + "+1,"
                + RawContacts.LAST_TIME_CONTACTED + "=? WHERE "
                + RawContacts._ID + "=?");
    }

    private void ensureDefaultAccount() {
        if (mAccount == null) {
            mAccount = mContactsProvider.getDefaultAccount();
            if (mAccount == null) {

                // This fall-through account will not match any data in the database, which
                // is the expected behavior
                mAccount = new Account(NON_EXISTENT_ACCOUNT_NAME, NON_EXISTENT_ACCOUNT_TYPE);
            }
        }
    }

    public void createDatabase(SQLiteDatabase db) {

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.PEOPLE + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.PEOPLE + " AS SELECT " +
                RawContactsColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.People._ID + ", " +
                "name." + StructuredName.DISPLAY_NAME
                        + " AS " + People.NAME + ", " +
                Tables.RAW_CONTACTS + "." + RawContactsColumns.DISPLAY_NAME
                        + " AS " + People.DISPLAY_NAME + ", " +
                PHONETIC_NAME_SQL
                        + " AS " + People.PHONETIC_NAME + " , " +
                "note." + Note.NOTE
                        + " AS " + People.NOTES + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.TIMES_CONTACTED
                        + " AS " + People.TIMES_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.LAST_TIME_CONTACTED
                        + " AS " + People.LAST_TIME_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.CUSTOM_RINGTONE
                        + " AS " + People.CUSTOM_RINGTONE + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.SEND_TO_VOICEMAIL
                        + " AS " + People.SEND_TO_VOICEMAIL + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED
                        + " AS " + People.STARRED + ", " +
                "organization." + Data._ID
                        + " AS " + People.PRIMARY_ORGANIZATION_ID + ", " +
                "email." + Data._ID
                        + " AS " + People.PRIMARY_EMAIL_ID + ", " +
                "phone." + Data._ID
                        + " AS " + People.PRIMARY_PHONE_ID + ", " +
                "phone." + Phone.NUMBER
                        + " AS " + People.NUMBER + ", " +
                "phone." + Phone.TYPE
                        + " AS " + People.TYPE + ", " +
                "phone." + Phone.LABEL
                        + " AS " + People.LABEL + ", " +
                "phone." + PhoneColumns.NORMALIZED_NUMBER
                        + " AS " + People.NUMBER_KEY +
                " FROM " + Tables.RAW_CONTACTS + PEOPLE_JOINS +
                " WHERE " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                        + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.ORGANIZATIONS + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.ORGANIZATIONS + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.Organizations._ID + ", " +
                Data.RAW_CONTACT_ID
                        + " AS " + android.provider.Contacts.Organizations.PERSON_ID + ", " +
                Data.IS_PRIMARY
                        + " AS " + android.provider.Contacts.Organizations.ISPRIMARY + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                Organization.COMPANY
                        + " AS " + android.provider.Contacts.Organizations.COMPANY + ", " +
                Organization.TYPE
                        + " AS " + android.provider.Contacts.Organizations.TYPE + ", " +
                Organization.LABEL
                        + " AS " + android.provider.Contacts.Organizations.LABEL + ", " +
                Organization.TITLE
                        + " AS " + android.provider.Contacts.Organizations.TITLE +
                " FROM " + Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS +
                " WHERE " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Organization.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                        + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.CONTACT_METHODS + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.CONTACT_METHODS + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + ContactMethods._ID + ", " +
                DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " AS " + ContactMethods.PERSON_ID + ", " +
                CONTACT_METHOD_KIND_SQL
                        + " AS " + ContactMethods.KIND + ", " +
                DataColumns.CONCRETE_IS_PRIMARY
                        + " AS " + ContactMethods.ISPRIMARY + ", " +
                DataColumns.CONCRETE_DATA1
                        + " AS " + ContactMethods.TYPE + ", " +
                CONTACT_METHOD_DATA_SQL
                        + " AS " + ContactMethods.DATA + ", " +
                DataColumns.CONCRETE_DATA3
                        + " AS " + ContactMethods.LABEL + ", " +
                DataColumns.CONCRETE_DATA14
                        + " AS " + ContactMethods.AUX_DATA + ", " +
                "name." + StructuredName.DISPLAY_NAME
                        + " AS " + ContactMethods.NAME + ", " +
                Tables.RAW_CONTACTS + "." + RawContactsColumns.DISPLAY_NAME
                        + " AS " + ContactMethods.DISPLAY_NAME + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                PHONETIC_NAME_SQL
                        + " AS " + ContactMethods.PHONETIC_NAME + " , " +
                "note." + Note.NOTE
                        + " AS " + ContactMethods.NOTES + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.TIMES_CONTACTED
                        + " AS " + ContactMethods.TIMES_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.LAST_TIME_CONTACTED
                        + " AS " + ContactMethods.LAST_TIME_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.CUSTOM_RINGTONE
                        + " AS " + ContactMethods.CUSTOM_RINGTONE + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.SEND_TO_VOICEMAIL
                        + " AS " + ContactMethods.SEND_TO_VOICEMAIL + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED
                        + " AS " + ContactMethods.STARRED +
                " FROM " + Tables.DATA + DATA_JOINS +
                " WHERE " + ContactMethods.KIND + " IS NOT NULL"
                    + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                    + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");


        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.PHONES + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.PHONES + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.Phones._ID + ", " +
                DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " AS " + android.provider.Contacts.Phones.PERSON_ID + ", " +
                DataColumns.CONCRETE_IS_PRIMARY
                        + " AS " + android.provider.Contacts.Phones.ISPRIMARY + ", " +
                Tables.DATA + "." + Phone.NUMBER
                        + " AS " + android.provider.Contacts.Phones.NUMBER + ", " +
                Tables.DATA + "." + Phone.TYPE
                        + " AS " + android.provider.Contacts.Phones.TYPE + ", " +
                Tables.DATA + "." + Phone.LABEL
                        + " AS " + android.provider.Contacts.Phones.LABEL + ", " +
                PhoneColumns.CONCRETE_NORMALIZED_NUMBER
                        + " AS " + android.provider.Contacts.Phones.NUMBER_KEY + ", " +
                "name." + StructuredName.DISPLAY_NAME
                        + " AS " + android.provider.Contacts.Phones.NAME + ", " +
                Tables.RAW_CONTACTS + "." + RawContactsColumns.DISPLAY_NAME
                        + " AS " + android.provider.Contacts.Phones.DISPLAY_NAME + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                PHONETIC_NAME_SQL
                        + " AS " + android.provider.Contacts.Phones.PHONETIC_NAME + " , " +
                "note." + Note.NOTE
                        + " AS " + android.provider.Contacts.Phones.NOTES + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.TIMES_CONTACTED
                        + " AS " + android.provider.Contacts.Phones.TIMES_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.LAST_TIME_CONTACTED
                        + " AS " + android.provider.Contacts.Phones.LAST_TIME_CONTACTED + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.CUSTOM_RINGTONE
                        + " AS " + android.provider.Contacts.Phones.CUSTOM_RINGTONE + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.SEND_TO_VOICEMAIL
                        + " AS " + android.provider.Contacts.Phones.SEND_TO_VOICEMAIL + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.STARRED
                        + " AS " + android.provider.Contacts.Phones.STARRED +
                " FROM " + Tables.DATA + DATA_JOINS +
                " WHERE " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Phone.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                        + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.EXTENSIONS + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.EXTENSIONS + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.Extensions._ID + ", " +
                DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " AS " + android.provider.Contacts.Extensions.PERSON_ID + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                ExtensionsColumns.NAME
                        + " AS " + android.provider.Contacts.Extensions.NAME + ", " +
                ExtensionsColumns.VALUE
                        + " AS " + android.provider.Contacts.Extensions.VALUE +
                " FROM " + Tables.DATA_JOIN_MIMETYPE_RAW_CONTACTS +
                " WHERE " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + android.provider.Contacts.Extensions.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                        + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.GROUPS + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.GROUPS + " AS SELECT " +
                GroupsColumns.CONCRETE_ID + " AS " + android.provider.Contacts.Groups._ID + ", " +
                Groups.ACCOUNT_NAME + ", " +
                Groups.ACCOUNT_TYPE + ", " +
                Groups.TITLE + " AS " + android.provider.Contacts.Groups.NAME + ", " +
                Groups.NOTES + " AS " + android.provider.Contacts.Groups.NOTES + " , " +
                Groups.SYSTEM_ID + " AS " + android.provider.Contacts.Groups.SYSTEM_ID +
                " FROM " + Tables.GROUPS +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.GROUP_MEMBERSHIP + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.GROUP_MEMBERSHIP + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.GroupMembership._ID + ", " +
                DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " AS " + android.provider.Contacts.GroupMembership.PERSON_ID + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.ACCOUNT_NAME
                        + " AS " +  RawContacts.ACCOUNT_NAME + ", " +
                Tables.RAW_CONTACTS + "." + RawContacts.ACCOUNT_TYPE
                        + " AS " +  RawContacts.ACCOUNT_TYPE + ", " +
                GroupMembership.GROUP_ROW_ID
                        + " AS " + android.provider.Contacts.GroupMembership.GROUP_ID + ", " +
                Groups.TITLE
                        + " AS " + android.provider.Contacts.GroupMembership.NAME + ", " +
                Groups.NOTES
                        + " AS " + android.provider.Contacts.GroupMembership.NOTES + " , " +
                Groups.SYSTEM_ID
                        + " AS " + android.provider.Contacts.GroupMembership.SYSTEM_ID +
                " FROM " + Tables.DATA_JOIN_PACKAGES_MIMETYPES_RAW_CONTACTS_GROUPS +
                " WHERE " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + GroupMembership.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0" +
        ";");

        db.execSQL("DROP VIEW IF EXISTS " + LegacyTables.PHOTOS + ";");
        db.execSQL("CREATE VIEW " + LegacyTables.PHOTOS + " AS SELECT " +
                DataColumns.CONCRETE_ID
                        + " AS " + android.provider.Contacts.Photos._ID + ", " +
                DataColumns.CONCRETE_RAW_CONTACT_ID
                        + " AS " + android.provider.Contacts.Photos.PERSON_ID + ", " +
                RawContacts.ACCOUNT_NAME + ", " +
                RawContacts.ACCOUNT_TYPE + ", " +
                Tables.DATA + "." + Photo.PHOTO
                        + " AS " + android.provider.Contacts.Photos.DATA + ", " +
                "legacy_photo." + LegacyPhotoData.EXISTS_ON_SERVER
                        + " AS " + android.provider.Contacts.Photos.EXISTS_ON_SERVER + ", " +
                "legacy_photo." + LegacyPhotoData.DOWNLOAD_REQUIRED
                        + " AS " + android.provider.Contacts.Photos.DOWNLOAD_REQUIRED + ", " +
                "legacy_photo." + LegacyPhotoData.LOCAL_VERSION
                        + " AS " + android.provider.Contacts.Photos.LOCAL_VERSION + ", " +
                "legacy_photo." + LegacyPhotoData.SYNC_ERROR
                        + " AS " + android.provider.Contacts.Photos.SYNC_ERROR +
                " FROM " + Tables.DATA + DATA_JOINS + LEGACY_PHOTO_JOIN +
                " WHERE " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Photo.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Tables.RAW_CONTACTS + "." + RawContacts.DELETED + "=0"
                        + " AND " + RawContacts.IS_RESTRICTED + "=0" +
        ";");
    }

    public Uri insert(Uri uri, ContentValues values) {
        final int match = sUriMatcher.match(uri);
        long id = 0;
        switch (match) {
            case PEOPLE:
                id = insertPeople(values);
                break;

            case ORGANIZATIONS:
                id = insertOrganization(values);
                break;

            case PEOPLE_CONTACTMETHODS: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                id = insertContactMethod(rawContactId, values);
                break;
            }

            case CONTACTMETHODS: {
                long rawContactId = getRequiredValue(values, ContactMethods.PERSON_ID);
                id = insertContactMethod(rawContactId, values);
                break;
            }

            case PHONES: {
                long rawContactId = getRequiredValue(values,
                        android.provider.Contacts.Phones.PERSON_ID);
                id = insertPhone(rawContactId, values);
                break;
            }

            case EXTENSIONS: {
                long rawContactId = getRequiredValue(values,
                        android.provider.Contacts.Extensions.PERSON_ID);
                id = insertExtension(rawContactId, values);
                break;
            }

            case GROUPS:
                id = insertGroup(values);
                break;

            case GROUPMEMBERSHIP: {
                long rawContactId = getRequiredValue(values,
                        android.provider.Contacts.GroupMembership.PERSON_ID);
                long groupId = getRequiredValue(values,
                        android.provider.Contacts.GroupMembership.GROUP_ID);
                id = insertGroupMembership(rawContactId, groupId);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if (id < 0) {
            return null;
        }

        final Uri result = ContentUris.withAppendedId(uri, id);
        onChange(result);
        return result;
    }

    private long getRequiredValue(ContentValues values, String column) {
        if (!values.containsKey(column)) {
            throw new RuntimeException("Required value: " + column);
        }

        return values.getAsLong(column);
    }

    private long insertPeople(ContentValues values) {
        ensureDefaultAccount();

        mValues.clear();

        OpenHelper.copyStringValue(mValues, RawContacts.CUSTOM_RINGTONE,
                values, People.CUSTOM_RINGTONE);
        OpenHelper.copyLongValue(mValues, RawContacts.SEND_TO_VOICEMAIL,
                values, People.SEND_TO_VOICEMAIL);
        OpenHelper.copyLongValue(mValues, RawContacts.LAST_TIME_CONTACTED,
                values, People.LAST_TIME_CONTACTED);
        OpenHelper.copyLongValue(mValues, RawContacts.TIMES_CONTACTED,
                values, People.TIMES_CONTACTED);
        OpenHelper.copyLongValue(mValues, RawContacts.STARRED,
                values, People.STARRED);
        mValues.put(RawContacts.ACCOUNT_NAME, mAccount.name);
        mValues.put(RawContacts.ACCOUNT_TYPE, mAccount.type);
        Uri contactUri = mContactsProvider.insert(RawContacts.CONTENT_URI, mValues);
        long rawContactId = ContentUris.parseId(contactUri);

        if (values.containsKey(People.NAME) || values.containsKey(People.PHONETIC_NAME)) {
            mValues.clear();
            mValues.put(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            mValues.put(ContactsContract.Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);
            OpenHelper.copyStringValue(mValues, StructuredName.DISPLAY_NAME,
                    values, People.NAME);
            if (values.containsKey(People.PHONETIC_NAME)) {
                String phoneticName = values.getAsString(People.PHONETIC_NAME);
                NameSplitter.Name parsedName = new NameSplitter.Name();
                mPhoneticNameSplitter.split(parsedName, phoneticName);
                mValues.put(StructuredName.PHONETIC_GIVEN_NAME, parsedName.getGivenNames());
                mValues.put(StructuredName.PHONETIC_MIDDLE_NAME, parsedName.getMiddleName());
                mValues.put(StructuredName.PHONETIC_FAMILY_NAME, parsedName.getFamilyName());
            }

            mContactsProvider.insert(ContactsContract.Data.CONTENT_URI, mValues);
        }

        if (values.containsKey(People.NOTES)) {
            mValues.clear();
            mValues.put(Data.RAW_CONTACT_ID, rawContactId);
            mValues.put(Data.MIMETYPE, Note.CONTENT_ITEM_TYPE);
            OpenHelper.copyStringValue(mValues, Note.NOTE, values, People.NOTES);
            mContactsProvider.insert(Data.CONTENT_URI, mValues);
        }

        // TODO instant aggregation
        return rawContactId;
    }

    private long insertOrganization(ContentValues values) {
        mValues.clear();

        OpenHelper.copyLongValue(mValues, Data.RAW_CONTACT_ID,
                values, android.provider.Contacts.Organizations.PERSON_ID);
        mValues.put(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE);

        OpenHelper.copyLongValue(mValues, Data.IS_PRIMARY,
                values, android.provider.Contacts.Organizations.ISPRIMARY);

        OpenHelper.copyStringValue(mValues, Organization.COMPANY,
                values, android.provider.Contacts.Organizations.COMPANY);

        // TYPE values happen to remain the same between V1 and V2 - can just copy the value
        OpenHelper.copyLongValue(mValues, Organization.TYPE,
                values, android.provider.Contacts.Organizations.TYPE);

        OpenHelper.copyStringValue(mValues, Organization.LABEL,
                values, android.provider.Contacts.Organizations.LABEL);
        OpenHelper.copyStringValue(mValues, Organization.TITLE,
                values, android.provider.Contacts.Organizations.TITLE);

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);

        return ContentUris.parseId(uri);
    }

    private long insertPhone(long rawContactId, ContentValues values) {
        mValues.clear();

        mValues.put(Data.RAW_CONTACT_ID, rawContactId);
        mValues.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);

        OpenHelper.copyLongValue(mValues, Data.IS_PRIMARY,
                values, android.provider.Contacts.Phones.ISPRIMARY);

        OpenHelper.copyStringValue(mValues, Phone.NUMBER,
                values, android.provider.Contacts.Phones.NUMBER);

        // TYPE values happen to remain the same between V1 and V2 - can just copy the value
        OpenHelper.copyLongValue(mValues, Phone.TYPE,
                values, android.provider.Contacts.Phones.TYPE);

        OpenHelper.copyStringValue(mValues, Phone.LABEL,
                values, android.provider.Contacts.Phones.LABEL);

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);

        return ContentUris.parseId(uri);
    }

    private long insertContactMethod(long rawContactId, ContentValues values) {
        Integer kind = values.getAsInteger(ContactMethods.KIND);
        if (kind == null) {
            throw new RuntimeException("Required value: " + ContactMethods.KIND);
        }

        mValues.clear();
        mValues.put(Data.RAW_CONTACT_ID, rawContactId);

        OpenHelper.copyLongValue(mValues, Data.IS_PRIMARY, values, ContactMethods.ISPRIMARY);

        switch (kind) {
            case android.provider.Contacts.KIND_EMAIL: {
                copyCommonFields(values, Email.CONTENT_ITEM_TYPE, Email.TYPE, Email.LABEL,
                        Data.DATA14);
                OpenHelper.copyStringValue(mValues, Email.DATA, values, ContactMethods.DATA);
                break;
            }

            case android.provider.Contacts.KIND_IM: {
                String protocol = values.getAsString(ContactMethods.DATA);
                if (protocol.startsWith("pre:")) {
                    mValues.put(Im.PROTOCOL, Integer.parseInt(protocol.substring(4)));
                } else if (protocol.startsWith("custom:")) {
                    mValues.put(Im.PROTOCOL, Im.PROTOCOL_CUSTOM);
                    mValues.put(Im.CUSTOM_PROTOCOL, protocol.substring(7));
                }

                copyCommonFields(values, Im.CONTENT_ITEM_TYPE, Im.TYPE, Im.LABEL, Data.DATA14);
                break;
            }

            case android.provider.Contacts.KIND_POSTAL: {
                copyCommonFields(values, StructuredPostal.CONTENT_ITEM_TYPE, StructuredPostal.TYPE,
                        StructuredPostal.LABEL, Data.DATA14);
                OpenHelper.copyStringValue(mValues, StructuredPostal.FORMATTED_ADDRESS, values,
                        ContactMethods.DATA);
                break;
            }
        }

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);
        return ContentUris.parseId(uri);
    }

    private void copyCommonFields(ContentValues values, String mimeType, String typeColumn,
            String labelColumn, String auxDataColumn) {
        mValues.put(Data.MIMETYPE, mimeType);
        OpenHelper.copyLongValue(mValues, typeColumn, values, ContactMethods.TYPE);
        OpenHelper.copyStringValue(mValues, labelColumn, values, ContactMethods.LABEL);
        OpenHelper.copyStringValue(mValues, auxDataColumn, values, ContactMethods.AUX_DATA);
    }

    private long insertExtension(long rawContactId, ContentValues values) {
        mValues.clear();

        mValues.put(Data.RAW_CONTACT_ID, rawContactId);
        mValues.put(Data.MIMETYPE, android.provider.Contacts.Extensions.CONTENT_ITEM_TYPE);

        OpenHelper.copyStringValue(mValues, ExtensionsColumns.NAME,
                values, android.provider.Contacts.People.Extensions.NAME);
        OpenHelper.copyStringValue(mValues, ExtensionsColumns.VALUE,
                values, android.provider.Contacts.People.Extensions.VALUE);

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);
        return ContentUris.parseId(uri);
    }

    private long insertGroup(ContentValues values) {
        ensureDefaultAccount();
        mValues.clear();

        OpenHelper.copyStringValue(mValues, Groups.TITLE,
                values, android.provider.Contacts.Groups.NAME);
        OpenHelper.copyStringValue(mValues, Groups.NOTES,
                values, android.provider.Contacts.Groups.NOTES);
        OpenHelper.copyStringValue(mValues, Groups.SYSTEM_ID,
                values, android.provider.Contacts.Groups.SYSTEM_ID);

        mValues.put(Groups.ACCOUNT_NAME, mAccount.name);
        mValues.put(Groups.ACCOUNT_TYPE, mAccount.type);

        Uri uri = mContactsProvider.insert(Groups.CONTENT_URI, mValues);
        return ContentUris.parseId(uri);
    }

    private long insertGroupMembership(long rawContactId, long groupId) {
        mValues.clear();

        mValues.put(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
        mValues.put(GroupMembership.RAW_CONTACT_ID, rawContactId);
        mValues.put(GroupMembership.GROUP_ROW_ID, groupId);

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);
        return ContentUris.parseId(uri);
    }

    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final int match = sUriMatcher.match(uri);
        int count = 0;
        switch(match) {
            case PEOPLE_UPDATE_CONTACT_TIME:
                count = updateContactTime(uri, values);
                break;

            case PEOPLE_PHOTO: {
                long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
                return updatePhoto(rawContactId, values);
            }

            case PHOTOS:
                // TODO
                break;

            case PHOTOS_ID:
                // TODO
                break;


            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if (count > 0) {
            mContext.getContentResolver().notifyChange(uri, null);
        }
        return count;
    }


    private int updateContactTime(Uri uri, ContentValues values) {

        // TODO check sanctions

        long lastTimeContacted;
        if (values.containsKey(People.LAST_TIME_CONTACTED)) {
            lastTimeContacted = values.getAsLong(People.LAST_TIME_CONTACTED);
        } else {
            lastTimeContacted = System.currentTimeMillis();
        }

        long rawContactId = Long.parseLong(uri.getPathSegments().get(1));
        long contactId = mOpenHelper.getContactId(rawContactId);
        if (contactId != 0) {
            mContactsProvider.updateContactTime(contactId, lastTimeContacted);
        } else {
            mLastTimeContactedUpdate.bindLong(1, lastTimeContacted);
            mLastTimeContactedUpdate.bindLong(2, rawContactId);
            mLastTimeContactedUpdate.execute();
        }
        return 1;
    }

    private int updatePhoto(long rawContactId, ContentValues values) {

        // TODO check sanctions

        int count;

        long dataId = -1;
        Cursor c = mContactsProvider.query(Data.CONTENT_URI, PhotoQuery.COLUMNS,
                Data.RAW_CONTACT_ID + "=" + rawContactId + " AND "
                        + Data.MIMETYPE + "=" + mOpenHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE),
                null, null);
        try {
            if (c.moveToFirst()) {
                dataId = c.getLong(PhotoQuery._ID);
            }
        } finally {
            c.close();
        }

        mValues.clear();
        byte[] bytes = values.getAsByteArray(android.provider.Contacts.Photos.DATA);
        mValues.put(Photo.PHOTO, bytes);

        if (dataId == -1) {
            mValues.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
            mValues.put(Data.RAW_CONTACT_ID, rawContactId);
            Uri dataUri = mContactsProvider.insert(Data.CONTENT_URI, mValues);
            dataId = ContentUris.parseId(dataUri);
            count = 1;
        } else {
            Uri dataUri = ContentUris.withAppendedId(Data.CONTENT_URI, dataId);
            count = mContactsProvider.update(dataUri, mValues, null, null);
        }

        mValues.clear();
        OpenHelper.copyStringValue(mValues, LegacyPhotoData.LOCAL_VERSION,
                values, android.provider.Contacts.Photos.LOCAL_VERSION);
        OpenHelper.copyStringValue(mValues, LegacyPhotoData.DOWNLOAD_REQUIRED,
                values, android.provider.Contacts.Photos.DOWNLOAD_REQUIRED);
        OpenHelper.copyStringValue(mValues, LegacyPhotoData.EXISTS_ON_SERVER,
                values, android.provider.Contacts.Photos.EXISTS_ON_SERVER);
        OpenHelper.copyStringValue(mValues, LegacyPhotoData.SYNC_ERROR,
                values, android.provider.Contacts.Photos.SYNC_ERROR);

        int updated = mContactsProvider.update(Data.CONTENT_URI, mValues,
                Data.MIMETYPE + "='" + LegacyPhotoData.CONTENT_ITEM_TYPE + "'"
                        + " AND " + Data.RAW_CONTACT_ID + "=" + rawContactId
                        + " AND " + LegacyPhotoData.PHOTO_DATA_ID + "=" + dataId, null);
        if (updated == 0) {
            mValues.put(Data.RAW_CONTACT_ID, rawContactId);
            mValues.put(Data.MIMETYPE, LegacyPhotoData.CONTENT_ITEM_TYPE);
            mValues.put(LegacyPhotoData.PHOTO_DATA_ID, dataId);
            mContactsProvider.insert(Data.CONTENT_URI, mValues);
        }

        return count;
    }

    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final int match = sUriMatcher.match(uri);
        int count = 0;
        switch (match) {
            case PEOPLE_ID:
                count = mContactsProvider.deleteRawContact(ContentUris.parseId(uri), false);
                break;

            case ORGANIZATIONS_ID:
                count = mContactsProvider.deleteData(ContentUris.parseId(uri),
                        ORGANIZATION_MIME_TYPES);
                break;

            case CONTACTMETHODS_ID:
                count = mContactsProvider.deleteData(ContentUris.parseId(uri),
                        CONTACT_METHOD_MIME_TYPES);
                break;

            case PHONES_ID:
                count = mContactsProvider.deleteData(ContentUris.parseId(uri),
                        PHONE_MIME_TYPES);
                break;

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        return count;
    }

    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder, String limit) {
        ensureDefaultAccount();

        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case PEOPLE: {
                qb.setTables(LegacyTables.PEOPLE_JOIN_PRESENCE);
                qb.setProjectionMap(sPeopleProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;
            }

            case PEOPLE_ID:
                qb.setTables(LegacyTables.PEOPLE_JOIN_PRESENCE);
                qb.setProjectionMap(sPeopleProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + People._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_FILTER: {
                qb.setTables(LegacyTables.PEOPLE_JOIN_PRESENCE);
                qb.setProjectionMap(sPeopleProjectionMap);
                applyRawContactsAccount(qb, uri);
                String filterParam = uri.getPathSegments().get(2);
                qb.appendWhere(" AND " + People._ID + " IN "
                        + mContactsProvider.getRawContactsByFilterAsNestedQuery(filterParam));
                break;
            }

            case ORGANIZATIONS:
                qb.setTables(LegacyTables.ORGANIZATIONS);
                qb.setProjectionMap(sOrganizationProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;

            case ORGANIZATIONS_ID:
                qb.setTables(LegacyTables.ORGANIZATIONS);
                qb.setProjectionMap(sOrganizationProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Organizations._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case CONTACTMETHODS:
                qb.setTables(LegacyTables.CONTACT_METHODS);
                qb.setProjectionMap(sContactMethodProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;

            case CONTACTMETHODS_ID:
                qb.setTables(LegacyTables.CONTACT_METHODS);
                qb.setProjectionMap(sContactMethodProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + ContactMethods._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_CONTACTMETHODS:
                qb.setTables(LegacyTables.CONTACT_METHODS);
                qb.setProjectionMap(sContactMethodProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + ContactMethods.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + ContactMethods.KIND + " IS NOT NULL");
                break;

            case PEOPLE_CONTACTMETHODS_ID:
                qb.setTables(LegacyTables.CONTACT_METHODS);
                qb.setProjectionMap(sContactMethodProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + ContactMethods.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + ContactMethods._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                qb.appendWhere(" AND " + ContactMethods.KIND + " IS NOT NULL");
                break;

            case PHONES:
                qb.setTables(LegacyTables.PHONES);
                qb.setProjectionMap(sPhoneProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;

            case PHONES_ID:
                qb.setTables(LegacyTables.PHONES);
                qb.setProjectionMap(sPhoneProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Phones._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PHONES_FILTER:
                qb.setTables(LegacyTables.PHONES);
                qb.setProjectionMap(sPhoneProjectionMap);
                applyRawContactsAccount(qb, uri);
                if (uri.getPathSegments().size() > 2) {
                    String filterParam = uri.getLastPathSegment();
                    qb.appendWhere(" AND person =");
                    qb.appendWhere(mOpenHelper.buildPhoneLookupAsNestedQuery(filterParam));
                }
                break;

            case PEOPLE_PHONES:
                qb.setTables(LegacyTables.PHONES);
                qb.setProjectionMap(sPhoneProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Phones.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_PHONES_ID:
                qb.setTables(LegacyTables.PHONES);
                qb.setProjectionMap(sPhoneProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Phones.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + android.provider.Contacts.Phones._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                break;

            case EXTENSIONS:
                qb.setTables(LegacyTables.EXTENSIONS);
                qb.setProjectionMap(sExtensionProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;

            case EXTENSIONS_ID:
                qb.setTables(LegacyTables.EXTENSIONS);
                qb.setProjectionMap(sExtensionProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Extensions._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_EXTENSIONS:
                qb.setTables(LegacyTables.EXTENSIONS);
                qb.setProjectionMap(sExtensionProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Extensions.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_EXTENSIONS_ID:
                qb.setTables(LegacyTables.EXTENSIONS);
                qb.setProjectionMap(sExtensionProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Extensions.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + android.provider.Contacts.Extensions._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                break;

            case GROUPS:
                qb.setTables(LegacyTables.GROUPS);
                qb.setProjectionMap(sGroupProjectionMap);
                applyGroupAccount(qb, uri);
                break;

            case GROUPS_ID:
                qb.setTables(LegacyTables.GROUPS);
                qb.setProjectionMap(sGroupProjectionMap);
                applyGroupAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Groups._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case GROUPMEMBERSHIP:
                qb.setTables(LegacyTables.GROUP_MEMBERSHIP);
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                applyRawContactsAccount(qb, uri);
                break;

            case GROUPMEMBERSHIP_ID:
                qb.setTables(LegacyTables.GROUP_MEMBERSHIP);
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.GroupMembership._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_GROUPMEMBERSHIP:
                qb.setTables(LegacyTables.GROUP_MEMBERSHIP);
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.GroupMembership.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case PEOPLE_GROUPMEMBERSHIP_ID:
                qb.setTables(LegacyTables.GROUP_MEMBERSHIP);
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.GroupMembership.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + android.provider.Contacts.GroupMembership._ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                break;

            case PEOPLE_PHOTO:
                qb.setTables(LegacyTables.PHOTOS);
                qb.setProjectionMap(sPhotoProjectionMap);
                applyRawContactsAccount(qb, uri);
                qb.appendWhere(" AND " + android.provider.Contacts.Photos.PERSON_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                limit = "1";
                break;

            case SEARCH_SUGGESTIONS:

                // No legacy compatibility for search suggestions
                return mGlobalSearchSupport.handleSearchSuggestionsQuery(db, uri, limit);

            case SEARCH_SHORTCUT: {
                long contactId = ContentUris.parseId(uri);
                return mGlobalSearchSupport.handleSearchShortcutRefresh(db, contactId, projection);
            }

            case DELETED_PEOPLE:
            case DELETED_GROUPS:
                throw new UnsupportedOperationException();

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs,
                groupBy, null, sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(mContext.getContentResolver(), RawContacts.CONTENT_URI);
        }
        return c;
    }

    private void applyRawContactsAccount(SQLiteQueryBuilder qb, Uri uri) {
        StringBuilder sb = new StringBuilder();
        sb.append(RawContacts.ACCOUNT_NAME + "=");
        DatabaseUtils.appendEscapedSQLString(sb, mAccount.name);
        sb.append(" AND " + RawContacts.ACCOUNT_TYPE + "=");
        DatabaseUtils.appendEscapedSQLString(sb, mAccount.type);
        qb.appendWhere(sb.toString());
    }

    private void applyGroupAccount(SQLiteQueryBuilder qb, Uri uri) {
        StringBuilder sb = new StringBuilder();
        sb.append(Groups.ACCOUNT_NAME + "=");
        DatabaseUtils.appendEscapedSQLString(sb, mAccount.name);
        sb.append(" AND " + Groups.ACCOUNT_TYPE + "=");
        DatabaseUtils.appendEscapedSQLString(sb, mAccount.type);
        qb.appendWhere(sb.toString());
    }

    /**
     * Called when a change has been made.
     *
     * @param uri the uri that the change was made to
     */
    private void onChange(Uri uri) {
        mContext.getContentResolver().notifyChange(android.provider.Contacts.CONTENT_URI, null);
    }
}
