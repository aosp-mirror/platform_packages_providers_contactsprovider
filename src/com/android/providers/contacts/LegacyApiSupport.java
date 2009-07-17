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
import com.android.providers.contacts.OpenHelper.Tables;

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.Contacts.People;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.Data;

import java.util.HashMap;

public class LegacyApiSupport {

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int PEOPLE = 1;
    private static final int PEOPLE_ID = 2;
    private static final int PEOPLE_UPDATE_CONTACT_TIME = 3;
    private static final int ORGANIZATIONS = 4;
    private static final int ORGANIZATIONS_ID = 5;

    public interface LegacyTables {
        public static final String PEOPLE = "contacts"
                + " LEFT OUTER JOIN data name ON (contacts._id = name.contact_id"
                + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = name.mimetype_id)"
                        + "='" + StructuredName.CONTENT_ITEM_TYPE + "')"
                + " LEFT OUTER JOIN data organization ON (contacts._id = organization.contact_id"
                + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = organization.mimetype_id)"
                        + "='" + Organization.CONTENT_ITEM_TYPE + "' AND organization.is_primary)"
                + " LEFT OUTER JOIN data note ON (contacts._id = note.contact_id"
                + " AND (SELECT mimetype FROM mimetypes WHERE mimetypes._id = note.mimetype_id)"
                        + "='" + Note.CONTENT_ITEM_TYPE + "')";
    }

    private static final HashMap<String, String> sPeopleProjectionMap;
    private static final HashMap<String, String> sOrganizationProjectionMap;

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
//        matcher.addURI(authority, "people/#/phones", PEOPLE_PHONES);
//        matcher.addURI(authority, "people/#/phones_with_presence",
//                PEOPLE_PHONES_WITH_PRESENCE);
//        matcher.addURI(authority, "people/#/photo", PEOPLE_PHOTO);
//        matcher.addURI(authority, "people/#/photo/data", PEOPLE_PHOTO_DATA);
//        matcher.addURI(authority, "people/#/phones/#", PEOPLE_PHONES_ID);
//        matcher.addURI(authority, "people/#/contact_methods", PEOPLE_CONTACTMETHODS);
//        matcher.addURI(authority, "people/#/contact_methods_with_presence",
//                PEOPLE_CONTACTMETHODS_WITH_PRESENCE);
//        matcher.addURI(authority, "people/#/contact_methods/#", PEOPLE_CONTACTMETHODS_ID);
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
//        matcher.addURI(authority, "phones", PHONES);
//        matcher.addURI(authority, "phones_with_presence", PHONES_WITH_PRESENCE);
//        matcher.addURI(authority, "phones/filter/*", PHONES_FILTER);
//        matcher.addURI(authority, "phones/filter_name/*", PHONES_FILTER_NAME);
//        matcher.addURI(authority, "phones/mobile_filter_name/*",
//                PHONES_MOBILE_FILTER_NAME);
//        matcher.addURI(authority, "phones/#", PHONES_ID);
//        matcher.addURI(authority, "photos", PHOTOS);
//        matcher.addURI(authority, "photos/#", PHOTOS_ID);
//        matcher.addURI(authority, "contact_methods", CONTACTMETHODS);
//        matcher.addURI(authority, "contact_methods/email", CONTACTMETHODS_EMAIL);
//        matcher.addURI(authority, "contact_methods/email/*", CONTACTMETHODS_EMAIL_FILTER);
//        matcher.addURI(authority, "contact_methods/#", CONTACTMETHODS_ID);
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

        sPeopleProjectionMap = new HashMap<String, String>();

        sPeopleProjectionMap.put(People.NAME,
                "name." + StructuredName.DISPLAY_NAME + " AS " + People.NAME);
        sPeopleProjectionMap.put(People.DISPLAY_NAME,
                Tables.CONTACTS + "." + ContactsColumns.DISPLAY_NAME + " AS " + People.DISPLAY_NAME);
        sPeopleProjectionMap.put(People.PHONETIC_NAME, "trim(trim("
                + "ifnull(name." + StructuredName.PHONETIC_GIVEN_NAME + ",' ')||' '||"
                + "ifnull(name." + StructuredName.PHONETIC_MIDDLE_NAME + ",' '))||' '||"
                + "ifnull(name." + StructuredName.PHONETIC_FAMILY_NAME + ",' ')) "
                + "AS " + People.PHONETIC_NAME);
        sPeopleProjectionMap.put(People.NOTES,
                "note." + Note.NOTE + " AS " + People.NOTES);
        sPeopleProjectionMap.put(People.TIMES_CONTACTED,
                Tables.CONTACTS + "." + Contacts.TIMES_CONTACTED
                + " AS " + People.TIMES_CONTACTED);
        sPeopleProjectionMap.put(People.LAST_TIME_CONTACTED,
                Tables.CONTACTS + "." + Contacts.LAST_TIME_CONTACTED
                + " AS " + People.LAST_TIME_CONTACTED);
        sPeopleProjectionMap.put(People.CUSTOM_RINGTONE,
                Tables.CONTACTS + "." + Contacts.CUSTOM_RINGTONE
                + " AS " + People.CUSTOM_RINGTONE);
        sPeopleProjectionMap.put(People.SEND_TO_VOICEMAIL,
                Tables.CONTACTS + "." + Contacts.SEND_TO_VOICEMAIL
                + " AS " + People.SEND_TO_VOICEMAIL);
        sPeopleProjectionMap.put(People.STARRED,
                Tables.CONTACTS + "." + Contacts.STARRED
                + " AS " + People.STARRED);
        sPeopleProjectionMap.put(People.PRIMARY_ORGANIZATION_ID,
                "organization." + Data._ID
                + " AS " + People.PRIMARY_ORGANIZATION_ID);

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
    }

    private final Context mContext;
    private final OpenHelper mOpenHelper;
    private final ContactsProvider2 mContactsProvider;
    private final NameSplitter mPhoneticNameSplitter;

    /** Precomipled sql statement for incrementing times contacted for an aggregate */
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
                deleteOrganization(ContentUris.parseId(uri));
                break;

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        return count;
    }


    private void deleteOrganization(long dataId) {
        mContactsProvider.deleteData(dataId, Organization.CONTENT_ITEM_TYPE);
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
                qb.setTables(Tables.DATA);
                qb.setProjectionMap(sOrganizationProjectionMap);
                qb.appendWhere("data._id=");
                qb.appendWhere(uri.getPathSegments().get(1));
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
