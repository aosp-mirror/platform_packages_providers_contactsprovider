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
 * limitations under the License.
 */

package com.android.providers.contacts;

import android.content.ContentProviderOperation;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.OperationApplicationException;
import android.content.ContentProviderOperation.Builder;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
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
import android.text.TextUtils;
import android.util.Log;

import java.util.ArrayList;

public class LegacyContactImporter {

    public static final String TAG = "LegacyContactImporter";

    public static final String DEFAULT_ACCOUNT_TYPE = "com.google.GAIA";
    private static final String DATABASE_NAME = "contacts.db";

    private static final String CONTACTS_FEED_URL = "http://www.google.com/m8/feeds/contacts/";
    private static final String GROUPS_FEED_URL = "http://www.google.com/m8/feeds/groups/";
    private static final String PHOTO_FEED_URL = "http://www.google.com/m8/feeds/photos/media/";

    private final Context mContext;
    private final ContactsProvider2 mContactsProvider;
    private ContentValues mValues = new ContentValues();
    private ContentResolver mResolver;
    private boolean mPhoneticNameAvailable = true;

    public LegacyContactImporter(Context context, ContactsProvider2 contactsProvider) {
        mContext = context;
        mContactsProvider = contactsProvider;
        mResolver = mContactsProvider.getContext().getContentResolver();
    }

    public void importContacts() throws Exception {

        SQLiteDatabase sourceDb = null;
        try {
            String path = mContext.getDatabasePath(DATABASE_NAME).getPath();
            sourceDb = SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY);
            int version = sourceDb.getVersion();

            // Upgrade to version 78 was the latest that wiped out data.  Might as well follow suit
            // and ignore earlier versions.
            if (version < 78) {
                return;
            }

            Log.w(TAG, "Importing contacts from " + path);

            if (version < 80) {
                mPhoneticNameAvailable = false;
            }

            /*
             * At this point there should be no data in the contacts provider, but in case
             * some was inserted by mistake, we should remove it.  The main reason for this
             * is that we will be preserving original contact IDs and don't want to run into
             * any collisions.
             */
            mContactsProvider.wipeData();

            importGroups(sourceDb);
            importPeople(sourceDb);
            importOrganizations(sourceDb);
            importPhones(sourceDb);
            importContactMethods(sourceDb);
            importPhotos(sourceDb);
            importGroupMemberships(sourceDb);
            importCalls(sourceDb);

            // Deleted contacts should be inserted after everything else, because
            // the legacy table does not provide an _ID field - the _ID field
            // will be autoincremented
            importDeletedPeople(sourceDb);

            Log.w(TAG, "Contact import completed");
        } finally {
            if (sourceDb != null) sourceDb.close();
        }
    }

    private interface GroupsQuery {
        String TABLE = "groups";

        String[] COLUMNS = {
                "_id", "name", "notes", "should_sync", "system_id", "_sync_account", "_sync_id",
                "_sync_dirty",
        };

        static int ID = 0;
        static int NAME = 1;
        static int NOTES = 2;
        static int SHOULD_SYNC = 3;            // TODO add this feature to Groups
        static int SYSTEM_ID = 4;

        static int _SYNC_ACCOUNT = 5;
        static int _SYNC_ID = 6;
        static int _SYNC_DIRTY = 7;
    }

    private void importGroups(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(GroupsQuery.TABLE, GroupsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertGroup(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertGroup(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long id = c.getLong(GroupsQuery.ID);

        Builder builder = ContentProviderOperation.newInsert(Groups.CONTENT_URI)
                .withValue(Groups._ID, id)
                .withValue(Groups.TITLE, c.getString(GroupsQuery.NAME))
                .withValue(Groups.NOTES, c.getString(GroupsQuery.NOTES))
                .withValue(Groups.SYSTEM_ID, c.getString(GroupsQuery.SYSTEM_ID))
                .withValue(Groups.DIRTY, c.getInt(GroupsQuery._SYNC_DIRTY))
                .withValue(Groups.GROUP_VISIBLE, 1);

        String account = c.getString(GroupsQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            String syncId = c.getString(GroupsQuery._SYNC_ID);
            String sourceId = buildGroupSourceId(account, syncId);
            builder.withValue(Groups.ACCOUNT_NAME, account)
                    .withValue(Groups.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE)
                    .withValue(Groups.SOURCE_ID, sourceId);
        }
        operations.add(builder.build());

        mContactsProvider.applyBatch(operations);
    }

    private String buildGroupSourceId(String account, String syncId) {
        if (account == null || syncId == null) {
            return null;
        }

        return GROUPS_FEED_URL + account + "/base/" + syncId;
    }

    private interface PeopleQuery {
        String TABLE = "people";

        String[] COLUMNS_WITHOUT_PHONETIC_NAME = {
                "_id", "name", "notes", "times_contacted", "last_time_contacted", "starred",
                "primary_phone", "primary_organization", "primary_email", "custom_ringtone",
                "send_to_voicemail", "_sync_account", "_sync_id", "_sync_time", "_sync_local_id",
                "_sync_dirty",
        };

        String[] COLUMNS_WITH_PHONETIC_NAME = {
                "_id", "name", "notes", "times_contacted", "last_time_contacted", "starred",
                "primary_phone", "primary_organization", "primary_email", "custom_ringtone",
                "send_to_voicemail", "_sync_account", "_sync_id", "_sync_time", "_sync_local_id",
                "_sync_dirty", "phonetic_name",
        };

        static int _ID = 0;
        static int NAME = 1;
        static int NOTES = 2;
        static int TIMES_CONTACTED = 3;
        static int LAST_TIME_CONTACTED = 4;
        static int STARRED = 5;
        static int PRIMARY_PHONE = 6;
        static int PRIMARY_ORGANIZATION = 7;
        static int PRIMARY_EMAIL = 8;
        static int CUSTOM_RINGTONE = 9;
        static int SEND_TO_VOICEMAIL = 10;

        static int _SYNC_ACCOUNT = 11;
        static int _SYNC_ID = 12;
        static int _SYNC_TIME = 13;
        static int _SYNC_LOCAL_ID = 14;
        static int _SYNC_DIRTY = 15;

        static int PHONETIC_NAME = 16;
    }

    private void importPeople(SQLiteDatabase sourceDb) throws OperationApplicationException {
        String[] columns = mPhoneticNameAvailable ? PeopleQuery.COLUMNS_WITH_PHONETIC_NAME :
            PeopleQuery.COLUMNS_WITHOUT_PHONETIC_NAME;
        Cursor c = sourceDb.query(PeopleQuery.TABLE, columns, null, null, null, null,
                null);
        try {
            while (c.moveToNext()) {
                insertPerson(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertPerson(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long id = c.getLong(PeopleQuery._ID);

        Builder builder = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts._ID, id)
                .withValue(RawContacts.CUSTOM_RINGTONE,
                            c.getString(PeopleQuery.CUSTOM_RINGTONE))
                .withValue(RawContacts.DIRTY,
                            c.getString(PeopleQuery._SYNC_DIRTY))
                .withValue(RawContacts.LAST_TIME_CONTACTED,
                            c.getLong(PeopleQuery.LAST_TIME_CONTACTED))
                .withValue(RawContacts.SEND_TO_VOICEMAIL,
                            c.getInt(PeopleQuery.SEND_TO_VOICEMAIL))
                .withValue(RawContacts.STARRED,
                            c.getInt(PeopleQuery.STARRED))
                .withValue(RawContacts.TIMES_CONTACTED,
                            c.getLong(PeopleQuery.TIMES_CONTACTED))
                .withValue(RawContacts.SYNC1, c.getString(PeopleQuery._SYNC_TIME))
                .withValue(RawContacts.SYNC2, c.getString(PeopleQuery._SYNC_LOCAL_ID));

        String account = c.getString(PeopleQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            String syncId = c.getString(PeopleQuery._SYNC_ID);
            String sourceId = buildRawContactSourceId(account, syncId);
            builder.withValue(RawContacts.ACCOUNT_NAME, account)
                    .withValue(RawContacts.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE)
                    .withValue(RawContacts.SOURCE_ID, sourceId);
        }
        operations.add(builder.build());


        String name = c.getString(PeopleQuery.NAME);
        builder = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, id)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.DISPLAY_NAME, name);

        if (mPhoneticNameAvailable) {
            // TODO: add the ability to insert an unstructured phonetic name
            String phoneticName = c.getString(PeopleQuery.PHONETIC_NAME);
        }

        operations.add(builder.build());

        String notes = c.getString(PeopleQuery.NOTES);
        if (!TextUtils.isEmpty(notes)) {
            operations.add(ContentProviderOperation.newInsert(Data.CONTENT_URI)
                    .withValue(Data.RAW_CONTACT_ID, id)
                    .withValue(Data.MIMETYPE, Note.CONTENT_ITEM_TYPE)
                    .withValue(Note.NOTE, notes)
                    .build());
        }
        mContactsProvider.applyBatch(operations);
    }

    private String buildRawContactSourceId(String account, String syncId) {
        if (account == null || syncId == null) {
            return null;
        }

        return CONTACTS_FEED_URL + account + "/base/" + syncId;
    }

    private interface OrganizationsQuery {
        String TABLE = "organizations";

        String[] COLUMNS = {
                "person", "company", "title", "isprimary", "type", "label",
        };

        static int PERSON = 0;
        static int COMPANY = 1;
        static int TITLE = 2;
        static int ISPRIMARY = 3;
        static int TYPE = 4;
        static int LABEL = 5;
    }

    private void importOrganizations(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(OrganizationsQuery.TABLE, OrganizationsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertOrganization(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertOrganization(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long personId = c.getLong(OrganizationsQuery.PERSON);

        operations.add(ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, personId)
                .withValue(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE)
                .withValue(Data.IS_PRIMARY, c.getString(OrganizationsQuery.ISPRIMARY))
                .withValue(Organization.COMPANY, c.getString(OrganizationsQuery.COMPANY))
                .withValue(Organization.TITLE, c.getString(OrganizationsQuery.TITLE))
                .withValue(Organization.TYPE, c.getString(OrganizationsQuery.TYPE))
                .withValue(Organization.LABEL, c.getString(OrganizationsQuery.LABEL))
                .build());

        mContactsProvider.applyBatch(operations);
    }

    private interface ContactMethodsQuery {
        String TABLE = "contact_methods";

        String[] COLUMNS = {
                "person", "kind", "data", "aux_data", "type", "label", "isprimary",
        };

        static int PERSON = 0;
        static int KIND = 1;
        static int DATA = 2;
        static int AUX_DATA = 3;
        static int TYPE = 4;
        static int LABEL = 5;
        static int ISPRIMARY = 6;
    }

    private void importContactMethods(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(ContactMethodsQuery.TABLE, ContactMethodsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertContactMethod(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertContactMethod(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long personId = c.getLong(ContactMethodsQuery.PERSON);
        int kind = c.getInt(ContactMethodsQuery.KIND);
        String data = c.getString(ContactMethodsQuery.DATA);
        String auxData = c.getString(ContactMethodsQuery.AUX_DATA);
        String type = c.getString(ContactMethodsQuery.TYPE);
        String label = c.getString(ContactMethodsQuery.LABEL);
        int primary = c.getInt(ContactMethodsQuery.ISPRIMARY);

        Builder builder = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, personId)
                .withValue(Data.IS_PRIMARY, primary);

        switch (kind) {
            case android.provider.Contacts.KIND_EMAIL:
                builder.withValue(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE)
                        .withValue(Email.TYPE, type)
                        .withValue(Email.LABEL, label)
                        .withValue(Email.DATA, data)
                        .withValue(Data.DATA14, auxData);
                break;

            case android.provider.Contacts.KIND_IM:
                builder.withValue(Data.MIMETYPE, Im.CONTENT_ITEM_TYPE)
                        .withValue(Im.TYPE, type)
                        .withValue(Im.LABEL, label)
                        .withValue(Im.DATA, data)
                        .withValue(Data.DATA14, auxData);
                break;

            case android.provider.Contacts.KIND_POSTAL:
                builder.withValue(Data.MIMETYPE, StructuredPostal.CONTENT_ITEM_TYPE)
                        .withValue(StructuredPostal.TYPE, type)
                        .withValue(StructuredPostal.LABEL, label)
                        .withValue(StructuredPostal.FORMATTED_ADDRESS, data)
                        .withValue(Data.DATA14, auxData);
                break;
        }

        operations.add(builder.build());

        mContactsProvider.applyBatch(operations);
    }

    private interface PhonesQuery {
        String TABLE = "phones";

        String[] COLUMNS = {
                "person", "type", "number", "label", "isprimary",
        };

        static int PERSON = 0;
        static int TYPE = 1;
        static int NUMBER = 2;
        static int LABEL = 3;
        static int ISPRIMARY = 4;
    }

    private void importPhones(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(PhonesQuery.TABLE, PhonesQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertPhone(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertPhone(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long personId = c.getLong(PhonesQuery.PERSON);

        operations.add(ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, personId)
                .withValue(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE)
                .withValue(Data.IS_PRIMARY, c.getString(PhonesQuery.ISPRIMARY))
                .withValue(Phone.NUMBER, c.getString(PhonesQuery.NUMBER))
                .withValue(Phone.TYPE, c.getString(PhonesQuery.TYPE))
                .withValue(Phone.LABEL, c.getString(PhonesQuery.LABEL))
                .build());

        mContactsProvider.applyBatch(operations);
    }

    private interface PhotosQuery {
        String TABLE = "photos";

        String[] COLUMNS = {
                "person", "data", "_sync_id", "_sync_account"
        };

        static int PERSON = 0;
        static int DATA = 1;
        static int _SYNC_ID = 2;
        static int _SYNC_ACCOUNT = 3;
    }

    private void importPhotos(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(PhotosQuery.TABLE, PhotosQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertPhoto(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertPhoto(Cursor c) throws OperationApplicationException {
        if (c.isNull(PhotosQuery.DATA)) {
            return;
        }

        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long personId = c.getLong(PhotosQuery.PERSON);

        Builder builder = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, personId)
                .withValue(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE)
                .withValue(Photo.PHOTO, c.getBlob(PhotosQuery.DATA));
        String account = c.getString(PhotosQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            String syncId = c.getString(PhotosQuery._SYNC_ID);
            String sourceId = buildPhotoSourceId(account, syncId);
            builder.withValue(Data.SYNC1, sourceId);
        }
        operations.add(builder.build());

        mContactsProvider.applyBatch(operations);
    }

    private String buildPhotoSourceId(String account, String syncId) {
        if (account == null || syncId == null) {
            return null;
        }

        return PHOTO_FEED_URL + account + "/" + syncId;
    }

    private interface GroupMembershipQuery {
        String TABLE = "groupmembership";

        String[] COLUMNS = {
                "person", "group_id", "group_sync_account", "group_sync_id"
        };

        static int PERSON_ID = 0;
        static int GROUP_ID = 1;
        static int GROUP_SYNC_ACCOUNT = 2;
        static int GROUP_SYNC_ID = 3;
    }

    private void importGroupMemberships(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(GroupMembershipQuery.TABLE, GroupMembershipQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertGroupMembership(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertGroupMembership(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        long personId = c.getLong(GroupMembershipQuery.PERSON_ID);

        long groupId = 0;
        if (c.isNull(GroupMembershipQuery.GROUP_ID)) {
            String account = c.getString(GroupMembershipQuery.GROUP_SYNC_ACCOUNT);
            if (!TextUtils.isEmpty(account)) {
                String syncId = c.getString(GroupMembershipQuery.GROUP_SYNC_ID);
                String sourceId = buildGroupSourceId(account, syncId);

                Cursor cursor = mContactsProvider.query(Groups.CONTENT_URI,
                        new String[]{Groups._ID}, Groups.SOURCE_ID + "=?", new String[]{sourceId},
                        null);
                try {
                    if (cursor.moveToFirst()) {
                        groupId = cursor.getLong(0);
                    }
                } finally {
                    cursor.close();
                }

                if (groupId == 0) {
                    ContentValues values = new ContentValues();
                    values.put(Groups.ACCOUNT_NAME, account);
                    values.put(Groups.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE);
                    values.put(Groups.GROUP_VISIBLE, true);
                    values.put(Groups.SOURCE_ID, sourceId);
                    Uri groupUri = mContactsProvider.insert(Groups.CONTENT_URI, values);
                    groupId = ContentUris.parseId(groupUri);
                }
            }
        } else {
            groupId = c.getLong(GroupMembershipQuery.GROUP_ID);
        }

        operations.add(ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValue(Data.RAW_CONTACT_ID, personId)
                .withValue(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE)
                .withValue(GroupMembership.RAW_CONTACT_ID, personId)
                .withValue(GroupMembership.GROUP_ROW_ID, groupId)
                .build());

        mContactsProvider.applyBatch(operations);
    }

    private interface CallsQuery {
        String TABLE = "calls";

        String[] COLUMNS = {
                "_id", "number", "date", "duration", "type", "new", "name", "numbertype",
                "numberlabel"
        };

        static int ID = 0;
        static int NUMBER = 1;
        static int DATE = 2;
        static int DURATION = 3;
        static int TYPE = 4;
        static int NEW = 5;
        static int NAME = 6;
        static int NUMBER_TYPE = 7;
        static int NUMBER_LABEL = 8;
    }

    private void importCalls(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(CallsQuery.TABLE, CallsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertCall(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertCall(Cursor c) throws OperationApplicationException {

        // Cannot use batch operations here, because call log is serviced by a separate provider
        mValues.clear();
        mValues.put(Calls._ID, c.getLong(CallsQuery.ID));
        mValues.put(Calls.NUMBER, c.getString(CallsQuery.NUMBER));
        mValues.put(Calls.DATE, c.getLong(CallsQuery.DATE));
        mValues.put(Calls.DURATION, c.getLong(CallsQuery.DURATION));
        mValues.put(Calls.NEW, c.getLong(CallsQuery.NEW));
        mValues.put(Calls.TYPE, c.getLong(CallsQuery.TYPE));
        mValues.put(Calls.CACHED_NAME, c.getString(CallsQuery.NAME));
        mValues.put(Calls.CACHED_NUMBER_LABEL, c.getString(CallsQuery.NUMBER_LABEL));
        mValues.put(Calls.CACHED_NUMBER_TYPE, c.getString(CallsQuery.NUMBER_TYPE));

        // TODO: confirm that we can use the CallLogProvider at this point, that it is guaranteed
        // to have been registered.
        mResolver.insert(Calls.CONTENT_URI, mValues);
    }

    private interface DeletedPeopleQuery {
        String TABLE = "_deleted_people";

        String[] COLUMNS = {
                "_sync_id", "_sync_account"
        };

        static int _SYNC_ID = 0;
        static int _SYNC_ACCOUNT = 1;
    }

    private void importDeletedPeople(SQLiteDatabase sourceDb) throws OperationApplicationException {
        Cursor c = sourceDb.query(DeletedPeopleQuery.TABLE, DeletedPeopleQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertDeletedPerson(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertDeletedPerson(Cursor c) throws OperationApplicationException {
        ArrayList<ContentProviderOperation> operations = new ArrayList<ContentProviderOperation>();

        String account = c.getString(DeletedPeopleQuery._SYNC_ACCOUNT);
        String syncId = c.getString(DeletedPeopleQuery._SYNC_ID);
        String sourceId = buildRawContactSourceId(account, syncId);
        operations.add(ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValue(RawContacts.ACCOUNT_NAME, account)
                .withValue(RawContacts.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE)
                .withValue(RawContacts.SOURCE_ID, sourceId)
                .withValue(RawContacts.DELETED, 1)
                .build());

        mContactsProvider.applyBatch(operations);
    }
}
