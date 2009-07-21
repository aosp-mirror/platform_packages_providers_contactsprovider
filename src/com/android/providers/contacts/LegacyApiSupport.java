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

import com.android.providers.contacts.OpenHelper.ContactsColumns;
import com.android.providers.contacts.OpenHelper.DataColumns;
import com.android.providers.contacts.OpenHelper.MimetypesColumns;
import com.android.providers.contacts.OpenHelper.PhoneColumns;
import com.android.providers.contacts.OpenHelper.Tables;

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
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Postal;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;

import java.util.HashMap;

public class LegacyApiSupport {

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

    private static final String PEOPLE_JOINS =
            " LEFT OUTER JOIN data name ON (contacts._id = name.contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = name.mimetype_id)"
                    + "='" + StructuredName.CONTENT_ITEM_TYPE + "')"
            + " LEFT OUTER JOIN data organization ON (contacts._id = organization.contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = organization.mimetype_id)"
                    + "='" + Organization.CONTENT_ITEM_TYPE + "' AND organization.is_primary)"
            + " LEFT OUTER JOIN data email ON (contacts._id = email.contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = email.mimetype_id)"
                    + "='" + Email.CONTENT_ITEM_TYPE + "' AND email.is_primary)"
            + " LEFT OUTER JOIN data note ON (contacts._id = note.contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = note.mimetype_id)"
                    + "='" + Note.CONTENT_ITEM_TYPE + "')"
            + " LEFT OUTER JOIN data phone ON (contacts._id = phone.contact_id"
            + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = phone.mimetype_id)"
                    + "='" + Phone.CONTENT_ITEM_TYPE + "' AND phone.is_primary)";

    private static final String PHONETIC_NAME_SQL = "trim(trim("
            + "ifnull(name." + StructuredName.PHONETIC_GIVEN_NAME + ",' ')||' '||"
            + "ifnull(name." + StructuredName.PHONETIC_MIDDLE_NAME + ",' '))||' '||"
            + "ifnull(name." + StructuredName.PHONETIC_FAMILY_NAME + ",' ')) ";

    private static final String CONTACT_METHOD_KIND_SQL =
            "(CASE WHEN mimetype='" + Email.CONTENT_ITEM_TYPE + "'"
                + " THEN " + android.provider.Contacts.KIND_EMAIL
                + " ELSE"
                    + " (CASE WHEN mimetype='" + Im.CONTENT_ITEM_TYPE +"'"
                        + " THEN " + android.provider.Contacts.KIND_IM
                        + " ELSE"
                        + " (CASE WHEN mimetype='" + Postal.CONTENT_ITEM_TYPE + "'"
                            + " THEN "  + android.provider.Contacts.KIND_POSTAL
                            + " ELSE"
                                + " NULL"
                            + " END)"
                        + " END)"
                + " END)";

    public interface LegacyTables {
        public static final String PEOPLE = "contacts" + PEOPLE_JOINS;

        public static final String DATA = "data"
                + " JOIN mimetypes ON (mimetypes._id = data.mimetype_id)"
                + " JOIN contacts ON (contacts._id = data.contact_id)"
                + PEOPLE_JOINS;
    }

    private static final String[] ORGANIZATION_MIME_TYPES = new String[] {
        Organization.CONTENT_ITEM_TYPE
    };

    private static final String[] CONTACT_METHOD_MIME_TYPES = new String[] {
        Email.CONTENT_ITEM_TYPE,
        Im.CONTENT_ITEM_TYPE,
        Postal.CONTENT_ITEM_TYPE,
    };

    private static final String[] PHONE_MIME_TYPES = new String[] {
        Phone.CONTENT_ITEM_TYPE
    };

    private static final HashMap<String, String> sPeopleProjectionMap;
    private static final HashMap<String, String> sOrganizationProjectionMap;
    private static final HashMap<String, String> sContactMethodProjectionMap;
    private static final HashMap<String, String> sPhoneProjectionMap;

    static {

        // Contacts URI matching table
        UriMatcher matcher = sUriMatcher;

        String authority = android.provider.Contacts.AUTHORITY;
//        matcher.addURI(authority, "extensions", EXTENSIONS);
//        matcher.addURI(authority, "extensions/#", EXTENSIONS_ID);
//        matcher.addURI(authority, "groups", GROUPS);
//        matcher.addURI(authority, "groups/#", GROUPS_ID);
//        matcher.addURI(authority, "groups/name/*/members", GROUP_NAME_MEMBERS);
//        matcher.addURI(authority, "groups/name/*/members/filter/*",
//                GROUP_NAME_MEMBERS_FILTER);
//        matcher.addURI(authority, "groups/system_id/*/members", GROUP_SYSTEM_ID_MEMBERS);
//        matcher.addURI(authority, "groups/system_id/*/members/filter/*",
//                GROUP_SYSTEM_ID_MEMBERS_FILTER);
//        matcher.addURI(authority, "groupmembership", GROUPMEMBERSHIP);
//        matcher.addURI(authority, "groupmembership/#", GROUPMEMBERSHIP_ID);
//        matcher.addURI(authority, "groupmembershipraw", GROUPMEMBERSHIP_RAW);
        matcher.addURI(authority, "people", PEOPLE);
//        matcher.addURI(authority, "people/strequent", PEOPLE_STREQUENT);
//        matcher.addURI(authority, "people/strequent/filter/*", PEOPLE_STREQUENT_FILTER);
//        matcher.addURI(authority, "people/filter/*", PEOPLE_FILTER);
//        matcher.addURI(authority, "people/with_phones_filter/*",
//                PEOPLE_WITH_PHONES_FILTER);
//        matcher.addURI(authority, "people/with_email_or_im_filter/*",
//                PEOPLE_WITH_EMAIL_OR_IM_FILTER);
        matcher.addURI(authority, "people/#", PEOPLE_ID);
//        matcher.addURI(authority, "people/#/extensions", PEOPLE_EXTENSIONS);
//        matcher.addURI(authority, "people/#/extensions/#", PEOPLE_EXTENSIONS_ID);
        matcher.addURI(authority, "people/#/phones", PEOPLE_PHONES);
        matcher.addURI(authority, "people/#/phones/#", PEOPLE_PHONES_ID);
//        matcher.addURI(authority, "people/#/phones_with_presence",
//                PEOPLE_PHONES_WITH_PRESENCE);
//        matcher.addURI(authority, "people/#/photo", PEOPLE_PHOTO);
//        matcher.addURI(authority, "people/#/photo/data", PEOPLE_PHOTO_DATA);
        matcher.addURI(authority, "people/#/contact_methods", PEOPLE_CONTACTMETHODS);
//        matcher.addURI(authority, "people/#/contact_methods_with_presence",
//                PEOPLE_CONTACTMETHODS_WITH_PRESENCE);
        matcher.addURI(authority, "people/#/contact_methods/#", PEOPLE_CONTACTMETHODS_ID);
//        matcher.addURI(authority, "people/#/organizations", PEOPLE_ORGANIZATIONS);
//        matcher.addURI(authority, "people/#/organizations/#", PEOPLE_ORGANIZATIONS_ID);
//        matcher.addURI(authority, "people/#/groupmembership", PEOPLE_GROUPMEMBERSHIP);
//        matcher.addURI(authority, "people/#/groupmembership/#", PEOPLE_GROUPMEMBERSHIP_ID);
//        matcher.addURI(authority, "people/raw", PEOPLE_RAW);
//        matcher.addURI(authority, "people/owner", PEOPLE_OWNER);
        matcher.addURI(authority, "people/#/update_contact_time",
                PEOPLE_UPDATE_CONTACT_TIME);
//        matcher.addURI(authority, "deleted_people", DELETED_PEOPLE);
//        matcher.addURI(authority, "deleted_groups", DELETED_GROUPS);
        matcher.addURI(authority, "phones", PHONES);
//        matcher.addURI(authority, "phones_with_presence", PHONES_WITH_PRESENCE);
//        matcher.addURI(authority, "phones/filter/*", PHONES_FILTER);
//        matcher.addURI(authority, "phones/filter_name/*", PHONES_FILTER_NAME);
//        matcher.addURI(authority, "phones/mobile_filter_name/*",
//                PHONES_MOBILE_FILTER_NAME);
        matcher.addURI(authority, "phones/#", PHONES_ID);
//        matcher.addURI(authority, "photos", PHOTOS);
//        matcher.addURI(authority, "photos/#", PHOTOS_ID);
        matcher.addURI(authority, "contact_methods", CONTACTMETHODS);
//        matcher.addURI(authority, "contact_methods/email", CONTACTMETHODS_EMAIL);
//        matcher.addURI(authority, "contact_methods/email/*", CONTACTMETHODS_EMAIL_FILTER);
        matcher.addURI(authority, "contact_methods/#", CONTACTMETHODS_ID);
//        matcher.addURI(authority, "contact_methods/with_presence",
//                CONTACTMETHODS_WITH_PRESENCE);
//        matcher.addURI(authority, "presence", PRESENCE);
//        matcher.addURI(authority, "presence/#", PRESENCE_ID);
        matcher.addURI(authority, "organizations", ORGANIZATIONS);
        matcher.addURI(authority, "organizations/#", ORGANIZATIONS_ID);
//        matcher.addURI(authority, "voice_dialer_timestamp", VOICE_DIALER_TIMESTAMP);
//        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_QUERY,
//                SEARCH_SUGGESTIONS);
//        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_QUERY + "/*",
//                SEARCH_SUGGESTIONS);
//        matcher.addURI(authority, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/#",
//                SEARCH_SHORTCUT);
//        matcher.addURI(authority, "settings", SETTINGS);
//
//        matcher.addURI(authority, "live_folders/people", LIVE_FOLDERS_PEOPLE);
//        matcher.addURI(authority, "live_folders/people/*",
//                LIVE_FOLDERS_PEOPLE_GROUP_NAME);
//        matcher.addURI(authority, "live_folders/people_with_phones",
//                LIVE_FOLDERS_PEOPLE_WITH_PHONES);
//        matcher.addURI(authority, "live_folders/favorites",
//                LIVE_FOLDERS_PEOPLE_FAVORITES);
//
//        // Call log URI matching table
//        matcher.addURI(CALL_LOG_AUTHORITY, "calls", CALLS);
//        matcher.addURI(CALL_LOG_AUTHORITY, "calls/filter/*", CALLS_FILTER);
//        matcher.addURI(CALL_LOG_AUTHORITY, "calls/#", CALLS_ID);


        HashMap<String, String> peopleProjectionMap = new HashMap<String, String>();
        peopleProjectionMap.put(People.NAME,
                "name." + StructuredName.DISPLAY_NAME + " AS " + People.NAME);
        peopleProjectionMap.put(People.DISPLAY_NAME,
                Tables.CONTACTS + "." + ContactsColumns.DISPLAY_NAME + " AS " + People.DISPLAY_NAME);
        peopleProjectionMap.put(People.PHONETIC_NAME, PHONETIC_NAME_SQL + "AS " + People.PHONETIC_NAME);
        peopleProjectionMap.put(People.NOTES,
                "note." + Note.NOTE + " AS " + People.NOTES);
        peopleProjectionMap.put(People.TIMES_CONTACTED,
                Tables.CONTACTS + "." + Contacts.TIMES_CONTACTED
                + " AS " + People.TIMES_CONTACTED);
        peopleProjectionMap.put(People.LAST_TIME_CONTACTED,
                Tables.CONTACTS + "." + Contacts.LAST_TIME_CONTACTED
                + " AS " + People.LAST_TIME_CONTACTED);
        peopleProjectionMap.put(People.CUSTOM_RINGTONE,
                Tables.CONTACTS + "." + Contacts.CUSTOM_RINGTONE
                + " AS " + People.CUSTOM_RINGTONE);
        peopleProjectionMap.put(People.SEND_TO_VOICEMAIL,
                Tables.CONTACTS + "." + Contacts.SEND_TO_VOICEMAIL
                + " AS " + People.SEND_TO_VOICEMAIL);
        peopleProjectionMap.put(People.STARRED,
                Tables.CONTACTS + "." + Contacts.STARRED
                + " AS " + People.STARRED);

        sPeopleProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sPeopleProjectionMap.put(People.PRIMARY_ORGANIZATION_ID,
                "organization." + Data._ID + " AS " + People.PRIMARY_ORGANIZATION_ID);
        sPeopleProjectionMap.put(People.PRIMARY_EMAIL_ID,
                "email." + Data._ID + " AS " + People.PRIMARY_EMAIL_ID);
        sPeopleProjectionMap.put(People.PRIMARY_PHONE_ID,
                "phone." + Data._ID + " AS " + People.PRIMARY_PHONE_ID);
        sPeopleProjectionMap.put(People.NUMBER,
                "phone." + Phone.NUMBER + " AS " + People.NUMBER);
        sPeopleProjectionMap.put(People.TYPE,
                "phone." + Phone.TYPE + " AS " + People.TYPE);
        sPeopleProjectionMap.put(People.LABEL,
                "phone." + Phone.LABEL + " AS " + People.LABEL);
        sPeopleProjectionMap.put(People.NUMBER_KEY,
                "phone." + PhoneColumns.NORMALIZED_NUMBER + " AS " + People.NUMBER_KEY);

        sOrganizationProjectionMap = new HashMap<String, String>();
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.PERSON_ID,
                Data.CONTACT_ID + " AS " + android.provider.Contacts.Organizations.PERSON_ID);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.ISPRIMARY,
                Data.IS_PRIMARY + " AS " + android.provider.Contacts.Organizations.ISPRIMARY);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.COMPANY,
                Organization.COMPANY + " AS " + android.provider.Contacts.Organizations.COMPANY);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.TYPE,
                Organization.TYPE + " AS " + android.provider.Contacts.Organizations.TYPE);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.LABEL,
                Organization.LABEL + " AS " + android.provider.Contacts.Organizations.LABEL);
        sOrganizationProjectionMap.put(android.provider.Contacts.Organizations.TITLE,
                Organization.TITLE + " AS " + android.provider.Contacts.Organizations.TITLE);

        sContactMethodProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sContactMethodProjectionMap.put(ContactMethods.PERSON_ID,
                DataColumns.CONCRETE_CONTACT_ID + " AS " + ContactMethods.PERSON_ID);
        sContactMethodProjectionMap.put(ContactMethods.KIND,
                CONTACT_METHOD_KIND_SQL + " AS " + ContactMethods.KIND);
        sContactMethodProjectionMap.put(ContactMethods.ISPRIMARY,
                DataColumns.CONCRETE_IS_PRIMARY + " AS " + ContactMethods.ISPRIMARY);
        sContactMethodProjectionMap.put(ContactMethods.TYPE,
                DataColumns.CONCRETE_DATA1 + " AS " + ContactMethods.TYPE);
        sContactMethodProjectionMap.put(ContactMethods.DATA,
                DataColumns.CONCRETE_DATA2 + " AS " + ContactMethods.DATA);
        sContactMethodProjectionMap.put(ContactMethods.LABEL,
                DataColumns.CONCRETE_DATA3 + " AS " + ContactMethods.LABEL);
        sContactMethodProjectionMap.put(ContactMethods.AUX_DATA,
                DataColumns.CONCRETE_DATA4 + " AS " + ContactMethods.AUX_DATA);

        sPhoneProjectionMap = new HashMap<String, String>(peopleProjectionMap);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.PERSON_ID,
                DataColumns.CONCRETE_CONTACT_ID
                        + " AS " + android.provider.Contacts.Phones.PERSON_ID);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.ISPRIMARY,
                DataColumns.CONCRETE_IS_PRIMARY
                        + " AS " + android.provider.Contacts.Phones.ISPRIMARY);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.NUMBER,
                Tables.DATA + "." + Phone.NUMBER
                        + " AS " + android.provider.Contacts.Phones.NUMBER);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.TYPE,
                Tables.DATA + "." + Phone.TYPE
                        + " AS " + android.provider.Contacts.Phones.TYPE);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.LABEL,
                Tables.DATA + "." + Phone.LABEL
                        + " AS " + android.provider.Contacts.Phones.LABEL);
        sPhoneProjectionMap.put(android.provider.Contacts.Phones.NUMBER_KEY,
                PhoneColumns.CONCRETE_NORMALIZED_NUMBER
                        + " AS " + android.provider.Contacts.Phones.NUMBER_KEY);
    }

    private final Context mContext;
    private final OpenHelper mOpenHelper;
    private final ContactsProvider2 mContactsProvider;
    private final NameSplitter mPhoneticNameSplitter;

    /** Precompiled sql statement for incrementing times contacted for an aggregate */
    private final SQLiteStatement mLastTimeContactedUpdate;

    private final ContentValues mValues = new ContentValues();

    public LegacyApiSupport(Context context, OpenHelper openHelper,
            ContactsProvider2 contactsProvider) {
        mContext = context;
        mContactsProvider = contactsProvider;
        mOpenHelper = openHelper;

        mPhoneticNameSplitter = new NameSplitter("", "", "",
                context.getString(com.android.internal.R.string.common_name_conjunctions));

        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        mLastTimeContactedUpdate = db.compileStatement("UPDATE " + Tables.CONTACTS + " SET "
                + Contacts.TIMES_CONTACTED + "="
                + Contacts.TIMES_CONTACTED + "+1,"
                + Contacts.LAST_TIME_CONTACTED + "=? WHERE "
                + Contacts._ID + "=?");
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

            case PEOPLE_CONTACTMETHODS:
                long contactId = Long.parseLong(uri.getPathSegments().get(1));
                id = insertContactMethod(contactId, values);
                break;

            case CONTACTMETHODS:
                id = insertContactMethod(getRequiredContactIdValue(values), values);
                break;

            case PHONES:
                id = insertPhone(getRequiredContactIdValue(values), values);
                break;

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

    private long getRequiredContactIdValue(ContentValues values) {
        if (!values.containsKey(ContactMethods.PERSON_ID)) {
            throw new RuntimeException("Required value: " + ContactMethods.PERSON_ID);
        }

        return values.getAsLong(ContactMethods.PERSON_ID);
    }

    private long insertPeople(ContentValues values) {
        mValues.clear();

        // TODO: remove this once not required
        mValues.put(ContactsContract.Contacts.PACKAGE, "DefaultPackage");

        OpenHelper.copyStringValue(mValues, Contacts.CUSTOM_RINGTONE,
                values, People.CUSTOM_RINGTONE);
        OpenHelper.copyLongValue(mValues, Contacts.SEND_TO_VOICEMAIL,
                values, People.SEND_TO_VOICEMAIL);
        OpenHelper.copyLongValue(mValues, Contacts.LAST_TIME_CONTACTED,
                values, People.LAST_TIME_CONTACTED);
        OpenHelper.copyLongValue(mValues, Contacts.TIMES_CONTACTED,
                values, People.TIMES_CONTACTED);
        OpenHelper.copyLongValue(mValues, Contacts.STARRED,
                values, People.STARRED);
        Uri contactUri = mContactsProvider.insert(Contacts.CONTENT_URI, mValues);
        long contactId = ContentUris.parseId(contactUri);

        if (values.containsKey(People.NAME) || values.containsKey(People.PHONETIC_NAME)) {
            mValues.clear();
            mValues.put(ContactsContract.Data.CONTACT_ID, contactId);
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
            mValues.put(Data.CONTACT_ID, contactId);
            mValues.put(Data.MIMETYPE, Note.CONTENT_ITEM_TYPE);
            OpenHelper.copyStringValue(mValues, Note.NOTE, values, People.NOTES);
            mContactsProvider.insert(Data.CONTENT_URI, mValues);
        }

        // TODO instant aggregation
        return contactId;
    }

    private long insertOrganization(ContentValues values) {
        mValues.clear();

        OpenHelper.copyLongValue(mValues, Data.CONTACT_ID,
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

    private long insertPhone(long requiredContactIdValue, ContentValues values) {
        mValues.clear();

        OpenHelper.copyLongValue(mValues, Data.CONTACT_ID,
                values, android.provider.Contacts.Phones.PERSON_ID);
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

    private long insertContactMethod(long contactId, ContentValues values) {
        Integer kind = values.getAsInteger(ContactMethods.KIND);
        if (kind == null) {
            throw new RuntimeException("Required value: " + ContactMethods.KIND);
        }

        mValues.clear();
        mValues.put(Data.CONTACT_ID, contactId);

        OpenHelper.copyLongValue(mValues, Data.IS_PRIMARY, values, ContactMethods.ISPRIMARY);

        switch (kind) {
            case android.provider.Contacts.KIND_EMAIL:
                copyCommonFields(values, Email.CONTENT_ITEM_TYPE, Email.TYPE, Email.LABEL,
                        Email.DATA, Data.DATA4);
                break;

            case android.provider.Contacts.KIND_IM:
                copyCommonFields(values, Im.CONTENT_ITEM_TYPE, Im.TYPE, Im.LABEL,
                        Email.DATA, Data.DATA4);
                break;

            case android.provider.Contacts.KIND_POSTAL:
                copyCommonFields(values, Postal.CONTENT_ITEM_TYPE, Postal.TYPE, Postal.LABEL,
                        Postal.DATA, Data.DATA4);
                break;
        }

        Uri uri = mContactsProvider.insert(Data.CONTENT_URI, mValues);
        return ContentUris.parseId(uri);
    }

    private void copyCommonFields(ContentValues values, String mimeType, String typeColumn,
            String labelColumn, String dataColumn, String auxDataColumn) {
        mValues.put(Data.MIMETYPE, mimeType);
        OpenHelper.copyLongValue(mValues, typeColumn, values, ContactMethods.TYPE);
        OpenHelper.copyStringValue(mValues, labelColumn, values, ContactMethods.LABEL);
        OpenHelper.copyStringValue(mValues, dataColumn, values, ContactMethods.DATA);
        OpenHelper.copyStringValue(mValues, auxDataColumn, values, ContactMethods.AUX_DATA);
    }

    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final int match = sUriMatcher.match(uri);
        int count;
        switch(match) {
            case PEOPLE_UPDATE_CONTACT_TIME:
                count = updateContactTime(uri, values);
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

        long contactId = Long.parseLong(uri.getPathSegments().get(1));
        long aggregateId = mOpenHelper.getAggregateId(contactId);
        if (aggregateId != 0) {
            mContactsProvider.updateContactTime(aggregateId, lastTimeContacted);
        } else {
            mLastTimeContactedUpdate.bindLong(1, lastTimeContacted);
            mLastTimeContactedUpdate.bindLong(2, contactId);
            mLastTimeContactedUpdate.execute();
        }
        return 1;
    }

    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final int match = sUriMatcher.match(uri);
        int count = 0;
        switch (match) {
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
            String sortOrder) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;
        String limit = null;

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case PEOPLE: {
                qb.setTables(LegacyTables.PEOPLE);
                qb.setProjectionMap(sPeopleProjectionMap);
                break;
            }

            case PEOPLE_ID:
                qb.setTables(LegacyTables.PEOPLE);
                qb.setProjectionMap(sPeopleProjectionMap);
                qb.appendWhere("contacts._id=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case ORGANIZATIONS:
                // TODO
                break;

            case ORGANIZATIONS_ID:
                // TODO exclude mimetype from this join
                qb.setTables(Tables.DATA_JOIN_MIMETYPE_CONTACTS);
                qb.setProjectionMap(sOrganizationProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;

            case CONTACTMETHODS:
                // TODO
                break;

            case CONTACTMETHODS_ID:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sContactMethodProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + ContactMethods.KIND + " IS NOT NULL");
                break;

            case PEOPLE_CONTACTMETHODS:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sContactMethodProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_CONTACT_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + ContactMethods.KIND + " IS NOT NULL");
                break;

            case PEOPLE_CONTACTMETHODS_ID:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sContactMethodProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_CONTACT_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + DataColumns.CONCRETE_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                qb.appendWhere(" AND " + ContactMethods.KIND + " IS NOT NULL");
                break;

            case PHONES:
                // TODO
                break;

            case PHONES_ID:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sPhoneProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Phone.CONTENT_ITEM_TYPE + "'");
                break;

            case PEOPLE_PHONES:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sPhoneProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_CONTACT_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Phone.CONTENT_ITEM_TYPE + "'");
                break;

            case PEOPLE_PHONES_ID:
                qb.setTables(LegacyTables.DATA);
                qb.setProjectionMap(sPhoneProjectionMap);
                mContactsProvider.applyDataRestrictionExceptions(qb);
                qb.appendWhere(" AND " + DataColumns.CONCRETE_CONTACT_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND " + DataColumns.CONCRETE_ID + "=");
                qb.appendWhere(uri.getPathSegments().get(3));
                qb.appendWhere(" AND " + MimetypesColumns.CONCRETE_MIMETYPE + "='"
                        + Phone.CONTENT_ITEM_TYPE + "'");
                break;

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs,
                groupBy, null, sortOrder, limit);
        if (c != null) {
            c.setNotificationUri(mContext.getContentResolver(), Contacts.CONTENT_URI);
        }
        DatabaseUtils.dumpCursor(c);
        return c;
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
