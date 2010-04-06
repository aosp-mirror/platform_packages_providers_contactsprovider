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

import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteStatement;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.Contacts;
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
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Log;

import java.io.File;

public class LegacyContactImporter {

    public static final String TAG = "LegacyContactImporter";

    private static final int MAX_ATTEMPTS = 5;
    private static final int DELAY_BETWEEN_ATTEMPTS = 2000;

    public static final String DEFAULT_ACCOUNT_TYPE = "com.google";
    private static final String DATABASE_NAME = "contacts.db";

    private static final int INSERT_BATCH_SIZE = 200;

    /**
     * Estimated increase in database size after import.
     */
    private static final long DATABASE_SIZE_MULTIPLIER = 4;

    /**
     * Estimated minimum database size in megabytes.
     */
    private static final long DATABASE_MIN_SIZE = 5;

    private final Context mContext;
    private final ContactsProvider2 mContactsProvider;
    private ContactsDatabaseHelper mDbHelper;
    private ContentValues mValues = new ContentValues();
    private ContentResolver mResolver;
    private boolean mPhoneticNameAvailable = true;

    private SQLiteDatabase mSourceDb;
    private SQLiteDatabase mTargetDb;

    private NameSplitter mNameSplitter;
    private int mBatchCounter;

    private int mContactCount;

    private long mStructuredNameMimetypeId;
    private long mNoteMimetypeId;
    private long mOrganizationMimetypeId;
    private long mPhoneMimetypeId;
    private long mEmailMimetypeId;
    private long mImMimetypeId;
    private long mPostalMimetypeId;
    private long mPhotoMimetypeId;
    private long mGroupMembershipMimetypeId;

    private long mEstimatedStorageRequirement = DATABASE_MIN_SIZE;

    public LegacyContactImporter(Context context, ContactsProvider2 contactsProvider) {
        mContext = context;
        mContactsProvider = contactsProvider;
        mResolver = mContactsProvider.getContext().getContentResolver();
    }

    public boolean importContacts() throws Exception {
        String path = mContext.getDatabasePath(DATABASE_NAME).getPath();
        File file = new File(path);
        if (!file.exists()) {
            Log.i(TAG, "Legacy contacts database does not exist at " + path);
            return true;
        }

        Log.w(TAG, "Importing contacts from " + path);

        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            try {
                mSourceDb = SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY);
                importContactsFromLegacyDb();
                Log.i(TAG, "Imported legacy contacts: " + mContactCount);
                mContactsProvider.notifyChange();
                return true;

            } catch (SQLiteException e) {
                Log.e(TAG, "Database import exception. Will retry in " + DELAY_BETWEEN_ATTEMPTS
                        + "ms", e);

                // We could get a "database locked" exception here, in which
                // case we should retry
                Thread.sleep(DELAY_BETWEEN_ATTEMPTS);

            } finally {
                if (mSourceDb != null) {
                    mSourceDb.close();
                }
            }
        }

        long oldDatabaseSize = file.length();
        mEstimatedStorageRequirement = oldDatabaseSize * DATABASE_SIZE_MULTIPLIER / 1024 / 1024;
        if (mEstimatedStorageRequirement < DATABASE_MIN_SIZE) {
            mEstimatedStorageRequirement = DATABASE_MIN_SIZE;
        }

        return false;
    }

    public long getEstimatedStorageRequirement() {
        return mEstimatedStorageRequirement;
    }

    private void importContactsFromLegacyDb() {
        int version = mSourceDb.getVersion();

        // Upgrade to version 78 was the latest that wiped out data.  Might as well follow suit
        // and ignore earlier versions.
        if (version < 78) {
            return;
        }

        if (version < 80) {
            mPhoneticNameAvailable = false;
        }

        mDbHelper = (ContactsDatabaseHelper)mContactsProvider.getDatabaseHelper();
        mTargetDb = mDbHelper.getWritableDatabase();

        mStructuredNameMimetypeId = mDbHelper.getMimeTypeId(StructuredName.CONTENT_ITEM_TYPE);
        mNoteMimetypeId = mDbHelper.getMimeTypeId(Note.CONTENT_ITEM_TYPE);
        mOrganizationMimetypeId = mDbHelper.getMimeTypeId(Organization.CONTENT_ITEM_TYPE);
        mPhoneMimetypeId = mDbHelper.getMimeTypeId(Phone.CONTENT_ITEM_TYPE);
        mEmailMimetypeId = mDbHelper.getMimeTypeId(Email.CONTENT_ITEM_TYPE);
        mImMimetypeId = mDbHelper.getMimeTypeId(Im.CONTENT_ITEM_TYPE);
        mPostalMimetypeId = mDbHelper.getMimeTypeId(StructuredPostal.CONTENT_ITEM_TYPE);
        mPhotoMimetypeId = mDbHelper.getMimeTypeId(Photo.CONTENT_ITEM_TYPE);
        mGroupMembershipMimetypeId =
                mDbHelper.getMimeTypeId(GroupMembership.CONTENT_ITEM_TYPE);

        mNameSplitter = mContactsProvider.getNameSplitter();

        mTargetDb.beginTransaction();
        try {
            checkForImportFailureTest();

            /*
             * At this point there should be no data in the contacts provider, but in case
             * some was inserted by mistake, we should remove it. The main reason for this
             * is that we will be preserving original contact IDs and don't want to run into
             * any collisions.
             */
            mContactsProvider.wipeData();

            importGroups();
            importPeople();
            importOrganizations();
            importPhones();
            importContactMethods();
            importPhotos();
            importGroupMemberships();
            updateDisplayNamesAndLookupKeys();

            // Deleted contacts should be inserted after everything else, because
            // the legacy table does not provide an _ID field - the _ID field
            // will be autoincremented
            importDeletedPeople();

            mDbHelper.updateAllVisible();

            mTargetDb.setTransactionSuccessful();
        } finally {
            mTargetDb.endTransaction();
        }

        importCalls();
    }

    /**
     * This is used for simulating an import failure. Insert a row into the "settings"
     * table with key='TEST' and then proceed with the upgrade.  Remove the record
     * after verifying the failure handling.
     */
    private void checkForImportFailureTest() {
        long isTest = DatabaseUtils.longForQuery(mSourceDb,
                "SELECT COUNT(*) FROM settings WHERE key='TEST'", null);
        if (isTest != 0) {
            throw new SQLiteException("Testing import failure.");
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

    private interface GroupsInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.GROUPS + "(" +
                Groups._ID + "," +
                Groups.TITLE + "," +
                Groups.NOTES + "," +
                Groups.SYSTEM_ID + "," +
                Groups.DIRTY + "," +
                Groups.GROUP_VISIBLE + "," +
                Groups.ACCOUNT_NAME + "," +
                Groups.ACCOUNT_TYPE + "," +
                Groups.SOURCE_ID +
        ") VALUES (?,?,?,?,?,?,?,?,?)";

        int ID = 1;
        int TITLE = 2;
        int NOTES = 3;
        int SYSTEM_ID = 4;
        int DIRTY = 5;
        int GROUP_VISIBLE = 6;
        int ACCOUNT_NAME = 7;
        int ACCOUNT_TYPE = 8;
        int SOURCE_ID = 9;
    }

    private void importGroups() {
        SQLiteStatement insert = mTargetDb.compileStatement(GroupsInsert.INSERT_SQL);
        Cursor c = mSourceDb.query(GroupsQuery.TABLE, GroupsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertGroup(c, insert);
            }
        } finally {
            c.close();
            insert.close();
        }
    }

    private void insertGroup(Cursor c, SQLiteStatement insert) {
        long id = c.getLong(GroupsQuery.ID);

        insert.bindLong(GroupsInsert.ID, id);
        bindString(insert, GroupsInsert.TITLE, c.getString(GroupsQuery.NAME));
        bindString(insert, GroupsInsert.NOTES, c.getString(GroupsQuery.NOTES));
        bindString(insert, GroupsInsert.SYSTEM_ID, c.getString(GroupsQuery.SYSTEM_ID));
        insert.bindLong(GroupsInsert.DIRTY, c.getLong(GroupsQuery._SYNC_DIRTY));
        insert.bindLong(GroupsInsert.GROUP_VISIBLE, 1);

        String account = c.getString(GroupsQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            bindString(insert, GroupsInsert.ACCOUNT_NAME, account);
            bindString(insert, GroupsInsert.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE);
            bindString(insert, GroupsInsert.SOURCE_ID, c.getString(GroupsQuery._SYNC_ID));
        } else {
            insert.bindNull(GroupsInsert.ACCOUNT_NAME);
            insert.bindNull(GroupsInsert.ACCOUNT_TYPE);
            insert.bindNull(GroupsInsert.SOURCE_ID);
        }
        insert(insert);
    }

    private interface PeopleQuery {
        String TABLE = "people";

        String NAME_SQL =
                "(CASE WHEN (name IS NOT NULL AND name != '') "
                    + "THEN name "
                + "ELSE "
                    + "(CASE WHEN primary_organization is NOT NULL THEN "
                        + "(SELECT company FROM organizations WHERE "
                            + "organizations._id = primary_organization) "
                    + "ELSE "
                        + "(CASE WHEN primary_phone IS NOT NULL THEN "
                            +"(SELECT number FROM phones WHERE phones._id = primary_phone) "
                        + "ELSE "
                            + "(CASE WHEN primary_email IS NOT NULL THEN "
                                + "(SELECT data FROM contact_methods WHERE "
                                    + "contact_methods._id = primary_email) "
                            + "ELSE "
                                + "null "
                            + "END) "
                        + "END) "
                    + "END) "
                + "END) ";


        String[] COLUMNS_WITH_DISPLAY_NAME_WITHOUT_PHONETIC_NAME = {
                "_id", NAME_SQL, "notes", "times_contacted", "last_time_contacted", "starred",
                "primary_phone", "primary_organization", "primary_email", "custom_ringtone",
                "send_to_voicemail", "_sync_account", "_sync_id", "_sync_time", "_sync_local_id",
                "_sync_dirty",
        };

        String[] COLUMNS_WITH_DISPLAY_NAME_WITH_PHONETIC_NAME = {
                "_id", NAME_SQL, "notes", "times_contacted", "last_time_contacted", "starred",
                "primary_phone", "primary_organization", "primary_email", "custom_ringtone",
                "send_to_voicemail", "_sync_account", "_sync_id", "_sync_time", "_sync_local_id",
                "_sync_dirty", "phonetic_name",
        };

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


    private interface RawContactsInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.RAW_CONTACTS + "(" +
                RawContacts._ID + "," +
                RawContacts.CONTACT_ID + "," +
                RawContacts.CUSTOM_RINGTONE + "," +
                RawContacts.DIRTY + "," +
                RawContacts.LAST_TIME_CONTACTED + "," +
                RawContacts.SEND_TO_VOICEMAIL + "," +
                RawContacts.STARRED + "," +
                RawContacts.TIMES_CONTACTED + "," +
                RawContacts.SYNC1 + "," +
                RawContacts.SYNC2 + "," +
                RawContacts.ACCOUNT_NAME + "," +
                RawContacts.ACCOUNT_TYPE + "," +
                RawContacts.SOURCE_ID + "," +
                RawContactsColumns.DISPLAY_NAME + "," +
                RawContactsColumns.CONTACT_IN_VISIBLE_GROUP +
         ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int ID = 1;
        int CONTACT_ID = 2;
        int CUSTOM_RINGTONE = 3;
        int DIRTY = 4;
        int LAST_TIME_CONTACTED = 5;
        int SEND_TO_VOICEMAIL = 6;
        int STARRED = 7;
        int TIMES_CONTACTED = 8;
        int SYNC1 = 9;
        int SYNC2 = 10;
        int ACCOUNT_NAME = 11;
        int ACCOUNT_TYPE = 12;
        int SOURCE_ID = 13;
        int DISPLAY_NAME = 14;
        int CONTACT_IN_VISIBLE_GROUP = 15;
    }

    private interface ContactsInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.CONTACTS + "(" +
                Contacts._ID + "," +
                Contacts.CUSTOM_RINGTONE + "," +
                Contacts.LAST_TIME_CONTACTED + "," +
                Contacts.SEND_TO_VOICEMAIL + "," +
                Contacts.STARRED + "," +
                Contacts.TIMES_CONTACTED + "," +
                Contacts.NAME_RAW_CONTACT_ID + "," +
                Contacts.IN_VISIBLE_GROUP +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int ID = 1;
        int CUSTOM_RINGTONE = 2;
        int LAST_TIME_CONTACTED = 3;
        int SEND_TO_VOICEMAIL = 4;
        int STARRED = 5;
        int TIMES_CONTACTED = 6;
        int NAME_RAW_CONTACT_ID = 7;
        int IN_VISIBLE_GROUP = 8;
    }

    private interface StructuredNameInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                StructuredName.DISPLAY_NAME + "," +
                StructuredName.PREFIX + "," +
                StructuredName.GIVEN_NAME + "," +
                StructuredName.MIDDLE_NAME + "," +
                StructuredName.FAMILY_NAME + "," +
                StructuredName.SUFFIX + "," +
                StructuredName.FULL_NAME_STYLE + "," +
                StructuredName.PHONETIC_FAMILY_NAME + "," +
                StructuredName.PHONETIC_MIDDLE_NAME + "," +
                StructuredName.PHONETIC_GIVEN_NAME + "," +
                StructuredName.PHONETIC_NAME_STYLE +
         ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int DISPLAY_NAME = 3;
        int PREFIX = 4;
        int GIVEN_NAME = 5;
        int MIDDLE_NAME = 6;
        int FAMILY_NAME = 7;
        int SUFFIX = 8;
        int FULL_NAME_STYLE = 9;
        int PHONETIC_FAMILY_NAME = 10;
        int PHONETIC_MIDDLE_NAME = 11;
        int PHONETIC_GIVEN_NAME = 12;
        int PHONETIC_NAME_STYLE = 13;
    }

    private interface NoteInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Note.NOTE +
         ") VALUES (?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int NOTE = 3;
    }

    private void importPeople() {
        SQLiteStatement rawContactInsert = mTargetDb.compileStatement(RawContactsInsert.INSERT_SQL);
        SQLiteStatement contactInsert = mTargetDb.compileStatement(ContactsInsert.INSERT_SQL);
        SQLiteStatement structuredNameInsert =
                mTargetDb.compileStatement(StructuredNameInsert.INSERT_SQL);
        SQLiteStatement noteInsert = mTargetDb.compileStatement(NoteInsert.INSERT_SQL);
        try {
            String[] columns = mPhoneticNameAvailable
                    ? PeopleQuery.COLUMNS_WITH_DISPLAY_NAME_WITH_PHONETIC_NAME
                    : PeopleQuery.COLUMNS_WITH_DISPLAY_NAME_WITHOUT_PHONETIC_NAME;
            Cursor c = mSourceDb.query(PeopleQuery.TABLE, columns, "name IS NULL", null, null,
                    null, null);
            try {
                while (c.moveToNext()) {
                    insertRawContact(c, rawContactInsert);
                    insertContact(c, contactInsert);
                    insertNote(c, noteInsert);
                    mContactCount++;
                }
            } finally {
                c.close();
            }

            columns = mPhoneticNameAvailable
                    ? PeopleQuery.COLUMNS_WITH_PHONETIC_NAME
                    : PeopleQuery.COLUMNS_WITHOUT_PHONETIC_NAME;
            c = mSourceDb.query(PeopleQuery.TABLE, columns, "name IS NOT NULL", null, null, null,
                    null);
            try {
                while (c.moveToNext()) {
                    long id = insertRawContact(c, rawContactInsert);
                    insertContact(c, contactInsert);
                    insertStructuredName(c, structuredNameInsert);
                    insertNote(c, noteInsert);
                    mContactCount++;
                }
            } finally {
                c.close();
            }
        } finally {
            rawContactInsert.close();
            contactInsert.close();
            structuredNameInsert.close();
            noteInsert.close();
        }
    }

    private long insertRawContact(Cursor c, SQLiteStatement insert) {
        long id = c.getLong(PeopleQuery._ID);
        insert.bindLong(RawContactsInsert.ID, id);
        insert.bindLong(RawContactsInsert.CONTACT_ID, id);
        bindString(insert, RawContactsInsert.CUSTOM_RINGTONE,
                c.getString(PeopleQuery.CUSTOM_RINGTONE));
        bindString(insert, RawContactsInsert.DIRTY,
                c.getString(PeopleQuery._SYNC_DIRTY));
        insert.bindLong(RawContactsInsert.LAST_TIME_CONTACTED,
                c.getLong(PeopleQuery.LAST_TIME_CONTACTED));
        insert.bindLong(RawContactsInsert.SEND_TO_VOICEMAIL,
                c.getLong(PeopleQuery.SEND_TO_VOICEMAIL));
        insert.bindLong(RawContactsInsert.STARRED,
                c.getLong(PeopleQuery.STARRED));
        insert.bindLong(RawContactsInsert.TIMES_CONTACTED,
                c.getLong(PeopleQuery.TIMES_CONTACTED));
        bindString(insert, RawContactsInsert.SYNC1,
                c.getString(PeopleQuery._SYNC_TIME));
        bindString(insert, RawContactsInsert.SYNC2,
                c.getString(PeopleQuery._SYNC_LOCAL_ID));
        bindString(insert, RawContactsInsert.DISPLAY_NAME,
                c.getString(PeopleQuery.NAME));
        insert.bindLong(RawContactsInsert.CONTACT_IN_VISIBLE_GROUP, 1);

        String account = c.getString(PeopleQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            bindString(insert, RawContactsInsert.ACCOUNT_NAME, account);
            bindString(insert, RawContactsInsert.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE);
            bindString(insert, RawContactsInsert.SOURCE_ID, c.getString(PeopleQuery._SYNC_ID));
        } else {
            insert.bindNull(RawContactsInsert.ACCOUNT_NAME);
            insert.bindNull(RawContactsInsert.ACCOUNT_TYPE);
            insert.bindNull(RawContactsInsert.SOURCE_ID);
        }
        insert(insert);
        return id;
    }

    private void insertContact(Cursor c, SQLiteStatement insert) {
        long id = c.getLong(PeopleQuery._ID);
        insert.bindLong(ContactsInsert.ID, id);
        bindString(insert, ContactsInsert.CUSTOM_RINGTONE,
                c.getString(PeopleQuery.CUSTOM_RINGTONE));
        insert.bindLong(ContactsInsert.LAST_TIME_CONTACTED,
                c.getLong(PeopleQuery.LAST_TIME_CONTACTED));
        insert.bindLong(ContactsInsert.SEND_TO_VOICEMAIL,
                c.getLong(PeopleQuery.SEND_TO_VOICEMAIL));
        insert.bindLong(ContactsInsert.STARRED,
                c.getLong(PeopleQuery.STARRED));
        insert.bindLong(ContactsInsert.TIMES_CONTACTED,
                c.getLong(PeopleQuery.TIMES_CONTACTED));
        insert.bindLong(ContactsInsert.NAME_RAW_CONTACT_ID, id);
        insert.bindLong(ContactsInsert.IN_VISIBLE_GROUP, 1);

        insert(insert);
    }

    private void insertStructuredName(Cursor c, SQLiteStatement insert) {
        String name = c.getString(PeopleQuery.NAME);
        if (TextUtils.isEmpty(name)) {
            return;
        }

        long id = c.getLong(PeopleQuery._ID);

        insert.bindLong(StructuredNameInsert.RAW_CONTACT_ID, id);
        insert.bindLong(StructuredNameInsert.MIMETYPE_ID, mStructuredNameMimetypeId);
        bindString(insert, StructuredNameInsert.DISPLAY_NAME, name);

        NameSplitter.Name splitName = new NameSplitter.Name();
        mNameSplitter.split(splitName, name);

        bindString(insert, StructuredNameInsert.PREFIX,
                splitName.getPrefix());
        bindString(insert, StructuredNameInsert.GIVEN_NAME,
                splitName.getGivenNames());
        bindString(insert, StructuredNameInsert.MIDDLE_NAME,
                splitName.getMiddleName());
        bindString(insert, StructuredNameInsert.FAMILY_NAME,
                splitName.getFamilyName());
        bindString(insert, StructuredNameInsert.SUFFIX,
                splitName.getSuffix());
        final String joined = mNameSplitter.join(splitName, true);
        bindString(insert, StructuredNameInsert.DISPLAY_NAME, joined);

        if (mPhoneticNameAvailable) {
            String phoneticName = c.getString(PeopleQuery.PHONETIC_NAME);
            if (phoneticName != null) {
                int index = phoneticName.indexOf(' ');
                if (index == -1) {
                    splitName.phoneticFamilyName = phoneticName;
                } else {
                    splitName.phoneticFamilyName = phoneticName.substring(0, index).trim();
                    splitName.phoneticGivenName = phoneticName.substring(index + 1).trim();
                }
            }
        }

        mNameSplitter.guessNameStyle(splitName);

        int fullNameStyle = splitName.getFullNameStyle();
        insert.bindLong(StructuredNameInsert.FULL_NAME_STYLE,
                fullNameStyle);
        bindString(insert, StructuredNameInsert.PHONETIC_FAMILY_NAME,
                splitName.phoneticFamilyName);
        bindString(insert, StructuredNameInsert.PHONETIC_MIDDLE_NAME,
                splitName.phoneticMiddleName);
        bindString(insert, StructuredNameInsert.PHONETIC_GIVEN_NAME,
                splitName.phoneticGivenName);
        insert.bindLong(StructuredNameInsert.PHONETIC_NAME_STYLE,
                splitName.phoneticNameStyle);

        long dataId = insert(insert);

        mContactsProvider.insertNameLookupForStructuredName(id, dataId, name,
                mNameSplitter.getAdjustedFullNameStyle(fullNameStyle));
        if (splitName.phoneticFamilyName != null
                || splitName.phoneticMiddleName != null
                || splitName.phoneticGivenName != null) {
            mContactsProvider.insertNameLookupForPhoneticName(id, dataId,
                    splitName.phoneticFamilyName,
                    splitName.phoneticMiddleName,
                    splitName.phoneticGivenName);
        }
    }

    private void insertNote(Cursor c, SQLiteStatement insert) {
        String notes = c.getString(PeopleQuery.NOTES);

        if (TextUtils.isEmpty(notes)) {
            return;
        }

        long id = c.getLong(PeopleQuery._ID);
        insert.bindLong(NoteInsert.RAW_CONTACT_ID, id);
        insert.bindLong(NoteInsert.MIMETYPE_ID, mNoteMimetypeId);
        bindString(insert, NoteInsert.NOTE, notes);
        insert(insert);
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

    private interface OrganizationInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Data.IS_PRIMARY + "," +
                Data.IS_SUPER_PRIMARY + "," +
                Organization.COMPANY + "," +
                Organization.TITLE + "," +
                Organization.TYPE + "," +
                Organization.LABEL +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int IS_PRIMARY = 3;
        int IS_SUPER_PRIMARY = 4;
        int COMPANY = 5;
        int TITLE = 6;
        int TYPE = 7;
        int LABEL = 8;
    }

    private void importOrganizations() {
        SQLiteStatement insert = mTargetDb.compileStatement(OrganizationInsert.INSERT_SQL);
        Cursor c = mSourceDb.query(OrganizationsQuery.TABLE, OrganizationsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertOrganization(c, insert);
            }
        } finally {
            c.close();
            insert.close();
        }
    }

    private void insertOrganization(Cursor c, SQLiteStatement insert) {
        long id = c.getLong(OrganizationsQuery.PERSON);
        insert.bindLong(OrganizationInsert.RAW_CONTACT_ID, id);
        insert.bindLong(OrganizationInsert.MIMETYPE_ID, mOrganizationMimetypeId);
        bindString(insert, OrganizationInsert.IS_PRIMARY, c.getString(OrganizationsQuery.ISPRIMARY));
        bindString(insert, OrganizationInsert.IS_SUPER_PRIMARY,
                c.getString(OrganizationsQuery.ISPRIMARY));
        bindString(insert, OrganizationInsert.COMPANY, c.getString(OrganizationsQuery.COMPANY));
        bindString(insert, OrganizationInsert.TITLE, c.getString(OrganizationsQuery.TITLE));
        bindString(insert, OrganizationInsert.TYPE, c.getString(OrganizationsQuery.TYPE));
        bindString(insert, OrganizationInsert.LABEL, c.getString(OrganizationsQuery.LABEL));
        insert(insert);
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

    private interface EmailInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Data.IS_PRIMARY + "," +
                Data.IS_SUPER_PRIMARY + "," +
                Email.DATA + "," +
                Email.TYPE + "," +
                Email.LABEL + "," +
                Data.DATA14 +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int IS_PRIMARY = 3;
        int IS_SUPER_PRIMARY = 4;
        int DATA = 5;
        int TYPE = 6;
        int LABEL = 7;
        int AUX_DATA = 8;
    }

    private interface ImInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Data.IS_PRIMARY + "," +
                Data.IS_SUPER_PRIMARY + "," +
                Im.DATA + "," +
                Im.TYPE + "," +
                Im.LABEL + "," +
                Data.DATA14 +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int IS_PRIMARY = 3;
        int IS_SUPER_PRIMARY = 4;
        int DATA = 5;
        int TYPE = 6;
        int LABEL = 7;
        int AUX_DATA = 8;
    }

    private interface PostalInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Data.IS_PRIMARY + "," +
                Data.IS_SUPER_PRIMARY + "," +
                StructuredPostal.FORMATTED_ADDRESS + "," +
                StructuredPostal.TYPE + "," +
                StructuredPostal.LABEL + "," +
                Data.DATA14 +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int IS_PRIMARY = 3;
        int IS_SUPER_PRIMARY = 4;
        int DATA = 5;
        int TYPE = 6;
        int LABEL = 7;
        int AUX_DATA = 8;
    }

    private void importContactMethods() {
        SQLiteStatement emailInsert = mTargetDb.compileStatement(EmailInsert.INSERT_SQL);
        SQLiteStatement imInsert = mTargetDb.compileStatement(ImInsert.INSERT_SQL);
        SQLiteStatement postalInsert = mTargetDb.compileStatement(PostalInsert.INSERT_SQL);
        Cursor c = mSourceDb.query(ContactMethodsQuery.TABLE, ContactMethodsQuery.COLUMNS, null,
                null, null, null, null);
        try {
            while (c.moveToNext()) {
                int kind = c.getInt(ContactMethodsQuery.KIND);
                switch (kind) {
                    case android.provider.Contacts.KIND_EMAIL:
                        insertEmail(c, emailInsert);
                        break;

                    case android.provider.Contacts.KIND_IM:
                        insertIm(c, imInsert);
                        break;

                    case android.provider.Contacts.KIND_POSTAL:
                        insertPostal(c, postalInsert);
                        break;
                }
            }
        } finally {
            c.close();
            emailInsert.close();
            imInsert.close();
            postalInsert.close();
        }

    }

    private void insertEmail(Cursor c, SQLiteStatement insert) {
        long personId = c.getLong(ContactMethodsQuery.PERSON);
        String email = c.getString(ContactMethodsQuery.DATA);

        insert.bindLong(EmailInsert.RAW_CONTACT_ID, personId);
        insert.bindLong(EmailInsert.MIMETYPE_ID, mEmailMimetypeId);
        bindString(insert, EmailInsert.IS_PRIMARY, c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, EmailInsert.IS_SUPER_PRIMARY, c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, EmailInsert.DATA, email);
        bindString(insert, EmailInsert.AUX_DATA, c.getString(ContactMethodsQuery.AUX_DATA));
        bindString(insert, EmailInsert.TYPE, c.getString(ContactMethodsQuery.TYPE));
        bindString(insert, EmailInsert.LABEL, c.getString(ContactMethodsQuery.LABEL));

        long dataId = insert(insert);
        mContactsProvider.insertNameLookupForEmail(personId, dataId, email);
    }

    private void insertIm(Cursor c, SQLiteStatement insert) {
        long personId = c.getLong(ContactMethodsQuery.PERSON);

        insert.bindLong(ImInsert.RAW_CONTACT_ID, personId);
        insert.bindLong(ImInsert.MIMETYPE_ID, mImMimetypeId);
        bindString(insert, ImInsert.IS_PRIMARY, c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, ImInsert.IS_SUPER_PRIMARY, c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, ImInsert.DATA, c.getString(ContactMethodsQuery.DATA));
        bindString(insert, ImInsert.AUX_DATA, c.getString(ContactMethodsQuery.AUX_DATA));
        bindString(insert, ImInsert.TYPE, c.getString(ContactMethodsQuery.TYPE));
        bindString(insert, ImInsert.LABEL, c.getString(ContactMethodsQuery.LABEL));
        insert(insert);
    }

    private void insertPostal(Cursor c, SQLiteStatement insert) {
        long personId = c.getLong(ContactMethodsQuery.PERSON);

        insert.bindLong(PostalInsert.RAW_CONTACT_ID, personId);
        insert.bindLong(PostalInsert.MIMETYPE_ID, mPostalMimetypeId);
        bindString(insert, PostalInsert.IS_PRIMARY, c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, PostalInsert.IS_SUPER_PRIMARY,
                c.getString(ContactMethodsQuery.ISPRIMARY));
        bindString(insert, PostalInsert.DATA, c.getString(ContactMethodsQuery.DATA));
        bindString(insert, PostalInsert.AUX_DATA, c.getString(ContactMethodsQuery.AUX_DATA));
        bindString(insert, PostalInsert.TYPE, c.getString(ContactMethodsQuery.TYPE));
        bindString(insert, PostalInsert.LABEL, c.getString(ContactMethodsQuery.LABEL));
        insert(insert);
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

    private interface PhoneInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Data.IS_PRIMARY + "," +
                Data.IS_SUPER_PRIMARY + "," +
                Phone.NUMBER + "," +
                Phone.TYPE + "," +
                Phone.LABEL + "," +
                PhoneColumns.NORMALIZED_NUMBER +
         ") VALUES (?,?,?,?,?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int IS_PRIMARY = 3;
        int IS_SUPER_PRIMARY = 4;
        int NUMBER = 5;
        int TYPE = 6;
        int LABEL = 7;
        int NORMALIZED_NUMBER = 8;
    }

    private interface PhoneLookupInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.PHONE_LOOKUP + "(" +
                PhoneLookupColumns.RAW_CONTACT_ID + "," +
                PhoneLookupColumns.DATA_ID + "," +
                PhoneLookupColumns.NORMALIZED_NUMBER + "," +
                PhoneLookupColumns.MIN_MATCH +
         ") VALUES (?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int DATA_ID = 2;
        int NORMALIZED_NUMBER = 3;
        int MIN_MATCH = 4;
    }

    private interface HasPhoneNumberUpdate {
        String UPDATE_SQL = "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.HAS_PHONE_NUMBER + "=1 WHERE " + Contacts._ID + "=?";

        int CONTACT_ID = 1;
    }

    private void importPhones() {
        SQLiteStatement phoneInsert = mTargetDb.compileStatement(PhoneInsert.INSERT_SQL);
        SQLiteStatement phoneLookupInsert =
                mTargetDb.compileStatement(PhoneLookupInsert.INSERT_SQL);
        SQLiteStatement hasPhoneUpdate =
                mTargetDb.compileStatement(HasPhoneNumberUpdate.UPDATE_SQL);
        Cursor c = mSourceDb.query(PhonesQuery.TABLE, PhonesQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertPhone(c, phoneInsert, phoneLookupInsert, hasPhoneUpdate);
            }
        } finally {
            c.close();
            phoneInsert.close();
            phoneLookupInsert.close();
            hasPhoneUpdate.close();
        }
    }

    private void insertPhone(Cursor c, SQLiteStatement phoneInsert,
            SQLiteStatement phoneLookupInsert, SQLiteStatement hasPhoneUpdate) {
        long lastUpdatedContact = -1;
        long id = c.getLong(PhonesQuery.PERSON);
        String number = c.getString(PhonesQuery.NUMBER);
        String normalizedNumber = null;
        if (number != null) {
            normalizedNumber = PhoneNumberUtils.getStrippedReversed(number);
        }
        phoneInsert.bindLong(PhoneInsert.RAW_CONTACT_ID, id);
        phoneInsert.bindLong(PhoneInsert.MIMETYPE_ID, mPhoneMimetypeId);
        bindString(phoneInsert, PhoneInsert.IS_PRIMARY, c.getString(PhonesQuery.ISPRIMARY));
        bindString(phoneInsert, PhoneInsert.IS_SUPER_PRIMARY, c.getString(PhonesQuery.ISPRIMARY));
        bindString(phoneInsert, PhoneInsert.NUMBER, number);
        bindString(phoneInsert, PhoneInsert.TYPE, c.getString(PhonesQuery.TYPE));
        bindString(phoneInsert, PhoneInsert.LABEL, c.getString(PhonesQuery.LABEL));
        bindString(phoneInsert, PhoneInsert.NORMALIZED_NUMBER, normalizedNumber);

        long dataId = insert(phoneInsert);
        if (normalizedNumber != null) {
            phoneLookupInsert.bindLong(PhoneLookupInsert.RAW_CONTACT_ID, id);
            phoneLookupInsert.bindLong(PhoneLookupInsert.DATA_ID, dataId);
            phoneLookupInsert.bindString(PhoneLookupInsert.NORMALIZED_NUMBER, normalizedNumber);
            phoneLookupInsert.bindString(PhoneLookupInsert.MIN_MATCH,
                    PhoneNumberUtils.toCallerIDMinMatch(number));
            insert(phoneLookupInsert);

            if (lastUpdatedContact != id) {
                lastUpdatedContact = id;
                hasPhoneUpdate.bindLong(HasPhoneNumberUpdate.CONTACT_ID, id);
                hasPhoneUpdate.execute();
            }
        }
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

    private interface PhotoInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                Photo.PHOTO + "," +
                Data.SYNC1 +
         ") VALUES (?,?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int PHOTO = 3;
        int SYNC1 = 4;
    }

    private interface PhotoIdUpdate {
        String UPDATE_SQL = "UPDATE " + Tables.CONTACTS +
                " SET " + Contacts.PHOTO_ID + "=? WHERE " + Contacts._ID + "=?";

        int PHOTO_ID = 1;
        int CONTACT_ID = 2;
    }

    private void importPhotos() {
        SQLiteStatement insert = mTargetDb.compileStatement(PhotoInsert.INSERT_SQL);
        SQLiteStatement photoIdUpdate = mTargetDb.compileStatement(PhotoIdUpdate.UPDATE_SQL);
        Cursor c = mSourceDb.query(PhotosQuery.TABLE, PhotosQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertPhoto(c, insert, photoIdUpdate);
            }
        } finally {
            c.close();
            insert.close();
            photoIdUpdate.close();
        }
    }

    private void insertPhoto(Cursor c, SQLiteStatement insert, SQLiteStatement photoIdUpdate) {
        if (c.isNull(PhotosQuery.DATA)) {
            return;
        }

        long personId = c.getLong(PhotosQuery.PERSON);

        insert.bindLong(PhotoInsert.RAW_CONTACT_ID, personId);
        insert.bindLong(PhotoInsert.MIMETYPE_ID, mPhotoMimetypeId);
        insert.bindBlob(PhotoInsert.PHOTO, c.getBlob(PhotosQuery.DATA));

        String account = c.getString(PhotosQuery._SYNC_ACCOUNT);
        if (!TextUtils.isEmpty(account)) {
            bindString(insert, PhotoInsert.SYNC1, c.getString(PhotosQuery._SYNC_ID));
        } else {
            insert.bindNull(PhotoInsert.SYNC1);
        }

        long rowId = insert(insert);
        photoIdUpdate.bindLong(PhotoIdUpdate.PHOTO_ID, rowId);
        photoIdUpdate.bindLong(PhotoIdUpdate.CONTACT_ID, personId);
        photoIdUpdate.execute();
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

    private interface GroupMembershipInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.DATA + "(" +
                Data.RAW_CONTACT_ID + "," +
                DataColumns.MIMETYPE_ID + "," +
                GroupMembership.GROUP_ROW_ID +
         ") VALUES (?,?,?)";

        int RAW_CONTACT_ID = 1;
        int MIMETYPE_ID = 2;
        int GROUP_ROW_ID = 3;
    }

    private void importGroupMemberships() {
        SQLiteStatement insert = mTargetDb.compileStatement(GroupMembershipInsert.INSERT_SQL);
        Cursor c = mSourceDb.query(GroupMembershipQuery.TABLE, GroupMembershipQuery.COLUMNS, null,
                null, null, null, null);
        try {
            while (c.moveToNext()) {
                insertGroupMembership(c, insert);
            }
        } finally {
            c.close();
            insert.close();
        }
    }

    private void insertGroupMembership(Cursor c, SQLiteStatement insert) {
        long personId = c.getLong(GroupMembershipQuery.PERSON_ID);

        long groupId = 0;
        if (c.isNull(GroupMembershipQuery.GROUP_ID)) {
            String account = c.getString(GroupMembershipQuery.GROUP_SYNC_ACCOUNT);
            if (!TextUtils.isEmpty(account)) {
                String syncId = c.getString(GroupMembershipQuery.GROUP_SYNC_ID);

                Cursor cursor = mTargetDb.query(Tables.GROUPS,
                        new String[]{Groups._ID}, Groups.SOURCE_ID + "=?", new String[]{syncId},
                        null, null, null);
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
                    values.put(Groups.SOURCE_ID, syncId);
                    groupId = mTargetDb.insert(Tables.GROUPS, null, values);
                }
            }
        } else {
            groupId = c.getLong(GroupMembershipQuery.GROUP_ID);
        }

        insert.bindLong(GroupMembershipInsert.RAW_CONTACT_ID, personId);
        insert.bindLong(GroupMembershipInsert.MIMETYPE_ID, mGroupMembershipMimetypeId);
        insert.bindLong(GroupMembershipInsert.GROUP_ROW_ID, groupId);
        insert(insert);
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

    private void importCalls() {
        Cursor c = mSourceDb.query(CallsQuery.TABLE, CallsQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertCall(c);
            }
        } finally {
            c.close();
        }
    }

    private void insertCall(Cursor c) {

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

    private void updateDisplayNamesAndLookupKeys() {
        // Compute display names, sort keys, lookup key, etc. for all Raw Cont
        Cursor cursor = mResolver.query(RawContacts.CONTENT_URI,
                new String[] { RawContacts._ID }, null, null, null);
        try {
            while (cursor.moveToNext()) {
                long rawContactId = cursor.getLong(0);
                mContactsProvider.updateRawContactDisplayName(mTargetDb, rawContactId);
                mContactsProvider.updateLookupKeyForRawContact(mTargetDb, rawContactId);
            }
        } finally {
            cursor.close();
        }
    }

    private interface DeletedPeopleQuery {
        String TABLE = "_deleted_people";

        String[] COLUMNS = {
                "_sync_id", "_sync_account"
        };

        static int _SYNC_ID = 0;
        static int _SYNC_ACCOUNT = 1;
    }

    private interface DeletedRawContactInsert {
        String INSERT_SQL = "INSERT INTO " + Tables.RAW_CONTACTS + "(" +
                RawContacts.ACCOUNT_NAME + "," +
                RawContacts.ACCOUNT_TYPE + "," +
                RawContacts.SOURCE_ID + "," +
                RawContacts.DELETED + "," +
                RawContacts.AGGREGATION_MODE +
         ") VALUES (?,?,?,?,?)";


        int ACCOUNT_NAME = 1;
        int ACCOUNT_TYPE = 2;
        int SOURCE_ID = 3;
        int DELETED = 4;
        int AGGREGATION_MODE = 5;
    }

    private void importDeletedPeople() {
        SQLiteStatement insert = mTargetDb.compileStatement(DeletedRawContactInsert.INSERT_SQL);
        Cursor c = mSourceDb.query(DeletedPeopleQuery.TABLE, DeletedPeopleQuery.COLUMNS, null, null,
                null, null, null);
        try {
            while (c.moveToNext()) {
                insertDeletedPerson(c, insert);
            }
        } finally {
            c.close();
            insert.close();
        }
    }

    private void insertDeletedPerson(Cursor c, SQLiteStatement insert) {
        String account = c.getString(DeletedPeopleQuery._SYNC_ACCOUNT);
        if (account == null) {
            return;
        }

        insert.bindString(DeletedRawContactInsert.ACCOUNT_NAME, account);
        insert.bindString(DeletedRawContactInsert.ACCOUNT_TYPE, DEFAULT_ACCOUNT_TYPE);
        bindString(insert, DeletedRawContactInsert.SOURCE_ID,
                c.getString(DeletedPeopleQuery._SYNC_ID));
        insert.bindLong(DeletedRawContactInsert.DELETED, 1);
        insert.bindLong(DeletedRawContactInsert.AGGREGATION_MODE,
                RawContacts.AGGREGATION_MODE_DISABLED);
        insert(insert);
    }

    private void bindString(SQLiteStatement insert, int index, String string) {
        if (string == null) {
            insert.bindNull(index);
        } else {
            insert.bindString(index, string);
        }
    }

    private long insert(SQLiteStatement insertStatement) {
        long rowId = insertStatement.executeInsert();
        if (rowId == 0) {
            throw new RuntimeException("Insert failed");
        }

        mBatchCounter++;
        if (mBatchCounter >= INSERT_BATCH_SIZE) {
            mTargetDb.setTransactionSuccessful();
            mTargetDb.endTransaction();
            mTargetDb.beginTransaction();
            mBatchCounter = 0;
        }
        return rowId;
    }
}
