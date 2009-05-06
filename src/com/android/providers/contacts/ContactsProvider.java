/*
 * Copyright (C) 2006 The Android Open Source Project
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

import android.app.SearchManager;
import android.content.AbstractSyncableContentProvider;
import android.content.AbstractTableMerger;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.UriMatcher;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.CursorJoiner;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDoneException;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.Bundle;
import android.os.ParcelFileDescriptor;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.provider.Contacts;
import android.provider.Contacts.ContactMethods;
import android.provider.Contacts.Extensions;
import android.provider.Contacts.GroupMembership;
import android.provider.Contacts.Groups;
import android.provider.Contacts.GroupsColumns;
import android.provider.Contacts.Intents;
import android.provider.Contacts.Organizations;
import android.provider.Contacts.People;
import android.provider.Contacts.PeopleColumns;
import android.provider.Contacts.Phones;
import android.provider.Contacts.Photos;
import android.provider.Contacts.Presence;
import android.provider.Contacts.PresenceColumns;
import android.provider.LiveFolders;
import android.provider.SyncConstValue;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Config;
import android.util.Log;

import com.android.internal.database.ArrayListCursor;
import com.google.android.collect.Maps;
import com.google.android.collect.Sets;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ContactsProvider extends AbstractSyncableContentProvider {
    private static final String STREQUENT_ORDER_BY = "times_contacted DESC, display_name ASC";
    private static final String STREQUENT_LIMIT =
            "(SELECT COUNT(*) FROM people WHERE starred = 1) + 25";

    private static final String PEOPLE_PHONES_JOIN =
            "people LEFT OUTER JOIN phones ON people.primary_phone=phones._id "
            + "LEFT OUTER JOIN presence ON (presence." + Presence.PERSON_ID + "=people._id)";

    private static final String PEOPLE_PHONES_PHOTOS_JOIN =
            "people LEFT OUTER JOIN phones ON people.primary_phone=phones._id "
            + "LEFT OUTER JOIN presence ON (presence." + Presence.PERSON_ID + "=people._id) "
            + "LEFT OUTER JOIN photos ON (photos." + Photos.PERSON_ID + "=people._id)";

    private static final String PEOPLE_PHONES_PHOTOS_ORGANIZATIONS_JOIN =
            "people LEFT OUTER JOIN phones ON people.primary_phone=phones._id "
            + "LEFT OUTER JOIN presence ON (presence." + Presence.PERSON_ID + "=people._id) "
            + "LEFT OUTER JOIN photos ON (photos." + Photos.PERSON_ID + "=people._id) "
            + "LEFT OUTER JOIN organizations ON (organizations._id=people.primary_organization)";

    private static final String GTALK_PROTOCOL_STRING =
            ContactMethods.encodePredefinedImProtocol(ContactMethods.PROTOCOL_GOOGLE_TALK);

    private static final String[] ID_TYPE_PROJECTION = new String[]{"_id", "type"};

    private static final String[] sIsPrimaryProjectionWithoutKind =
            new String[]{"isprimary", "person", "_id"};
    private static final String[] sIsPrimaryProjectionWithKind =
            new String[]{"isprimary", "person", "_id", "kind"};

    private static final String WHERE_ID = "_id=?";

    private static final String sGroupsJoinString;

    private static final String PREFS_NAME_OWNER = "owner-info";
    private static final String PREF_OWNER_ID = "owner-id";

    /** this is suitable for use by insert/update/delete/query and may be passed
     * as a method call parameter. Only insert/update/delete/query should call .clear() on it */
    private final ContentValues mValues = new ContentValues();

    /** this is suitable for local use in methods and should never be passed as a parameter to
     * other methods (other than the DB layer) */
    private final ContentValues mValuesLocal = new ContentValues();

    private String[] mAccounts = new String[0];
    private final Object mAccountsLock = new Object();

    private DatabaseUtils.InsertHelper mDeletedPeopleInserter;
    private DatabaseUtils.InsertHelper mPeopleInserter;
    private int mIndexPeopleSyncId;
    private int mIndexPeopleSyncTime;
    private int mIndexPeopleSyncVersion;
    private int mIndexPeopleSyncDirty;
    private int mIndexPeopleSyncAccount;
    private int mIndexPeopleName;
    private int mIndexPeoplePhoneticName;
    private int mIndexPeopleNotes;
    private DatabaseUtils.InsertHelper mGroupsInserter;
    private DatabaseUtils.InsertHelper mPhotosInserter;
    private int mIndexPhotosPersonId;
    private int mIndexPhotosSyncId;
    private int mIndexPhotosSyncTime;
    private int mIndexPhotosSyncVersion;
    private int mIndexPhotosSyncDirty;
    private int mIndexPhotosSyncAccount;
    private int mIndexPhotosExistsOnServer;
    private int mIndexPhotosSyncError;
    private DatabaseUtils.InsertHelper mContactMethodsInserter;
    private int mIndexContactMethodsPersonId;
    private int mIndexContactMethodsLabel;
    private int mIndexContactMethodsKind;
    private int mIndexContactMethodsType;
    private int mIndexContactMethodsData;
    private int mIndexContactMethodsAuxData;
    private int mIndexContactMethodsIsPrimary;
    private DatabaseUtils.InsertHelper mOrganizationsInserter;
    private int mIndexOrganizationsPersonId;
    private int mIndexOrganizationsLabel;
    private int mIndexOrganizationsType;
    private int mIndexOrganizationsCompany;
    private int mIndexOrganizationsTitle;
    private int mIndexOrganizationsIsPrimary;
    private DatabaseUtils.InsertHelper mExtensionsInserter;
    private int mIndexExtensionsPersonId;
    private int mIndexExtensionsName;
    private int mIndexExtensionsValue;
    private DatabaseUtils.InsertHelper mGroupMembershipInserter;
    private int mIndexGroupMembershipPersonId;
    private int mIndexGroupMembershipGroupSyncAccount;
    private int mIndexGroupMembershipGroupSyncId;
    private DatabaseUtils.InsertHelper mCallsInserter;
    private DatabaseUtils.InsertHelper mPhonesInserter;
    private int mIndexPhonesPersonId;
    private int mIndexPhonesLabel;
    private int mIndexPhonesType;
    private int mIndexPhonesNumber;
    private int mIndexPhonesNumberKey;
    private int mIndexPhonesIsPrimary;

    public ContactsProvider() {
        super(DATABASE_NAME, DATABASE_VERSION, Contacts.CONTENT_URI);
    }

    @Override
    protected void onDatabaseOpened(SQLiteDatabase db) {
        maybeCreatePresenceTable(db);

        // Mark all the tables as syncable
        db.markTableSyncable(sPeopleTable, sDeletedPeopleTable);
        db.markTableSyncable(sPhonesTable, Phones.PERSON_ID, sPeopleTable);
        db.markTableSyncable(sContactMethodsTable, ContactMethods.PERSON_ID, sPeopleTable);
        db.markTableSyncable(sOrganizationsTable, Organizations.PERSON_ID, sPeopleTable);
        db.markTableSyncable(sGroupmembershipTable, GroupMembership.PERSON_ID, sPeopleTable);
        db.markTableSyncable(sExtensionsTable, Extensions.PERSON_ID, sPeopleTable);
        db.markTableSyncable(sGroupsTable, sDeletedGroupsTable);

        mDeletedPeopleInserter = new DatabaseUtils.InsertHelper(db, sDeletedPeopleTable);
        mPeopleInserter = new DatabaseUtils.InsertHelper(db, sPeopleTable);
        mIndexPeopleSyncId = mPeopleInserter.getColumnIndex(People._SYNC_ID);
        mIndexPeopleSyncTime = mPeopleInserter.getColumnIndex(People._SYNC_TIME);
        mIndexPeopleSyncVersion = mPeopleInserter.getColumnIndex(People._SYNC_VERSION);
        mIndexPeopleSyncDirty = mPeopleInserter.getColumnIndex(People._SYNC_DIRTY);
        mIndexPeopleSyncAccount = mPeopleInserter.getColumnIndex(People._SYNC_ACCOUNT);
        mIndexPeopleName = mPeopleInserter.getColumnIndex(People.NAME);
        mIndexPeoplePhoneticName = mPeopleInserter.getColumnIndex(People.PHONETIC_NAME);
        mIndexPeopleNotes = mPeopleInserter.getColumnIndex(People.NOTES);

        mGroupsInserter = new DatabaseUtils.InsertHelper(db, sGroupsTable);

        mPhotosInserter = new DatabaseUtils.InsertHelper(db, sPhotosTable);
        mIndexPhotosPersonId = mPhotosInserter.getColumnIndex(Photos.PERSON_ID);
        mIndexPhotosSyncId = mPhotosInserter.getColumnIndex(Photos._SYNC_ID);
        mIndexPhotosSyncTime = mPhotosInserter.getColumnIndex(Photos._SYNC_TIME);
        mIndexPhotosSyncVersion = mPhotosInserter.getColumnIndex(Photos._SYNC_VERSION);
        mIndexPhotosSyncDirty = mPhotosInserter.getColumnIndex(Photos._SYNC_DIRTY);
        mIndexPhotosSyncAccount = mPhotosInserter.getColumnIndex(Photos._SYNC_ACCOUNT);
        mIndexPhotosSyncError = mPhotosInserter.getColumnIndex(Photos.SYNC_ERROR);
        mIndexPhotosExistsOnServer = mPhotosInserter.getColumnIndex(Photos.EXISTS_ON_SERVER);

        mContactMethodsInserter = new DatabaseUtils.InsertHelper(db, sContactMethodsTable);
        mIndexContactMethodsPersonId = mContactMethodsInserter.getColumnIndex(ContactMethods.PERSON_ID);
        mIndexContactMethodsLabel = mContactMethodsInserter.getColumnIndex(ContactMethods.LABEL);
        mIndexContactMethodsKind = mContactMethodsInserter.getColumnIndex(ContactMethods.KIND);
        mIndexContactMethodsType = mContactMethodsInserter.getColumnIndex(ContactMethods.TYPE);
        mIndexContactMethodsData = mContactMethodsInserter.getColumnIndex(ContactMethods.DATA);
        mIndexContactMethodsAuxData = mContactMethodsInserter.getColumnIndex(ContactMethods.AUX_DATA);
        mIndexContactMethodsIsPrimary = mContactMethodsInserter.getColumnIndex(ContactMethods.ISPRIMARY);

        mOrganizationsInserter = new DatabaseUtils.InsertHelper(db, sOrganizationsTable);
        mIndexOrganizationsPersonId = mOrganizationsInserter.getColumnIndex(Organizations.PERSON_ID);
        mIndexOrganizationsLabel = mOrganizationsInserter.getColumnIndex(Organizations.LABEL);
        mIndexOrganizationsType = mOrganizationsInserter.getColumnIndex(Organizations.TYPE);
        mIndexOrganizationsCompany = mOrganizationsInserter.getColumnIndex(Organizations.COMPANY);
        mIndexOrganizationsTitle = mOrganizationsInserter.getColumnIndex(Organizations.TITLE);
        mIndexOrganizationsIsPrimary = mOrganizationsInserter.getColumnIndex(Organizations.ISPRIMARY);

        mExtensionsInserter = new DatabaseUtils.InsertHelper(db, sExtensionsTable);
        mIndexExtensionsPersonId = mExtensionsInserter.getColumnIndex(Extensions.PERSON_ID);
        mIndexExtensionsName = mExtensionsInserter.getColumnIndex(Extensions.NAME);
        mIndexExtensionsValue = mExtensionsInserter.getColumnIndex(Extensions.VALUE);

        mGroupMembershipInserter = new DatabaseUtils.InsertHelper(db, sGroupmembershipTable);
        mIndexGroupMembershipPersonId = mGroupMembershipInserter.getColumnIndex(GroupMembership.PERSON_ID);
        mIndexGroupMembershipGroupSyncAccount = mGroupMembershipInserter.getColumnIndex(GroupMembership.GROUP_SYNC_ACCOUNT);
        mIndexGroupMembershipGroupSyncId = mGroupMembershipInserter.getColumnIndex(GroupMembership.GROUP_SYNC_ID);

        mCallsInserter = new DatabaseUtils.InsertHelper(db, sCallsTable);

        mPhonesInserter = new DatabaseUtils.InsertHelper(db, sPhonesTable);
        mIndexPhonesPersonId = mPhonesInserter.getColumnIndex(Phones.PERSON_ID);
        mIndexPhonesLabel = mPhonesInserter.getColumnIndex(Phones.LABEL);
        mIndexPhonesType = mPhonesInserter.getColumnIndex(Phones.TYPE);
        mIndexPhonesNumber = mPhonesInserter.getColumnIndex(Phones.NUMBER);
        mIndexPhonesNumberKey = mPhonesInserter.getColumnIndex(Phones.NUMBER_KEY);
        mIndexPhonesIsPrimary = mPhonesInserter.getColumnIndex(Phones.ISPRIMARY);
    }

    @Override
    protected boolean upgradeDatabase(SQLiteDatabase db, int oldVersion, int newVersion) {
        boolean upgradeWasLossless = true;
        if (oldVersion < 71) {
            Log.w(TAG, "Upgrading database from version " + oldVersion + " to " +
                    newVersion + ", which will destroy all old data");
            dropTables(db);
            bootstrapDatabase(db);
            return false; // this was lossy
        }
        if (oldVersion == 71) {
            Log.i(TAG, "Upgrading contacts database from version " + oldVersion + " to " +
                    newVersion + ", which will preserve existing data");

            db.delete("_sync_state", null, null);
            mValuesLocal.clear();
            mValuesLocal.putNull(Photos._SYNC_VERSION);
            mValuesLocal.putNull(Photos._SYNC_TIME);
            db.update(sPhotosTable, mValuesLocal, null, null);
            getContext().getContentResolver().startSync(Contacts.CONTENT_URI, new Bundle());
            oldVersion = 72;
        }
        if (oldVersion == 72) {
            Log.i(TAG, "Upgrading contacts database from version " + oldVersion + " to " +
                    newVersion + ", which will preserve existing data");

            // use new token format from 73
            db.execSQL("delete from peopleLookup");
            try {
                DatabaseUtils.longForQuery(db,
                        "SELECT _TOKENIZE('peopleLookup', _id, name, ' ') from people;",
                        null);
            } catch (SQLiteDoneException ex) {
                // it is ok to throw this, 
                // it just means you don't have data in people table
            }
            oldVersion = 73;
        }
        // There was a bug for a while in the upgrade logic where going from 72 to 74 would skip
        // the step from 73 to 74, so 74 to 75 just tries the same steps, and gracefully handles
        // errors in case the device was started freshly at 74.
        if (oldVersion == 73 || oldVersion == 74) {
            Log.i(TAG, "Upgrading contacts database from version " + oldVersion + " to " +
                    newVersion + ", which will preserve existing data");

            try {
                db.execSQL("ALTER TABLE calls ADD name TEXT;");
                db.execSQL("ALTER TABLE calls ADD numbertype INTEGER;");
                db.execSQL("ALTER TABLE calls ADD numberlabel TEXT;");
            } catch (SQLiteException sqle) {
                // Maybe the table was altered already... Shouldn't be an issue.
            }
            oldVersion = 75;
        }
        // There were some indices added in version 76
        if (oldVersion == 75) {
            Log.i(TAG, "Upgrading contacts database from version " + oldVersion + " to " +
                    newVersion + ", which will preserve existing data");

            // add the new indices
            db.execSQL("CREATE INDEX IF NOT EXISTS groupsSyncDirtyIndex"
                    + " ON groups (" + Groups._SYNC_DIRTY + ");");
            db.execSQL("CREATE INDEX IF NOT EXISTS photosSyncDirtyIndex"
                    + " ON photos (" + Photos._SYNC_DIRTY + ");");
            db.execSQL("CREATE INDEX IF NOT EXISTS peopleSyncDirtyIndex"
                    + " ON people (" + People._SYNC_DIRTY + ");");
            oldVersion = 76;
        }

        if (oldVersion == 76 || oldVersion == 77) {
            db.execSQL("DELETE FROM people");
            db.execSQL("DELETE FROM groups");
            db.execSQL("DELETE FROM photos");
            db.execSQL("DELETE FROM _deleted_people");
            db.execSQL("DELETE FROM _deleted_groups");
            upgradeWasLossless = false;
            oldVersion = 78;
        }

        if (oldVersion == 78) {
            db.execSQL("UPDATE photos SET _sync_dirty=0 where _sync_dirty is null;");
            oldVersion = 79;
        }

        if (oldVersion == 79) {
            try {
                db.execSQL("ALTER TABLE people ADD phonetic_name TEXT COLLATE LOCALIZED;");
            } catch (SQLiteException sqle) {
                // Maybe the table was altered already... Shouldn't be an issue.
            }
            oldVersion = 80;
        }

        return upgradeWasLossless;
    }

    protected void dropTables(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS people");
        db.execSQL("DROP TABLE IF EXISTS peopleLookup");
        db.execSQL("DROP TABLE IF EXISTS _deleted_people");
        db.execSQL("DROP TABLE IF EXISTS phones");
        db.execSQL("DROP TABLE IF EXISTS contact_methods");
        db.execSQL("DROP TABLE IF EXISTS calls");
        db.execSQL("DROP TABLE IF EXISTS organizations");
        db.execSQL("DROP TABLE IF EXISTS voice_dialer_timestamp");
        db.execSQL("DROP TABLE IF EXISTS groups");
        db.execSQL("DROP TABLE IF EXISTS _deleted_groups");
        db.execSQL("DROP TABLE IF EXISTS groupmembership");
        db.execSQL("DROP TABLE IF EXISTS photos");
        db.execSQL("DROP TABLE IF EXISTS extensions");
        db.execSQL("DROP TABLE IF EXISTS settings");
    }

    @Override
    protected void bootstrapDatabase(SQLiteDatabase db) {
        super.bootstrapDatabase(db);
        db.execSQL("CREATE TABLE people (" +
                    People._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    People._SYNC_ACCOUNT + " TEXT," + // From the sync source
                    People._SYNC_ID + " TEXT," + // From the sync source
                    People._SYNC_TIME + " TEXT," + // From the sync source
                    People._SYNC_VERSION + " TEXT," + // From the sync source
                    People._SYNC_LOCAL_ID + " INTEGER," + // Used while syncing, not persistent
                    People._SYNC_DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                                                       // if syncable, non-zero if the record
                                                       // has local, unsynced, changes
                    People._SYNC_MARK + " INTEGER," + // Used to filter out new rows

                    People.NAME + " TEXT COLLATE LOCALIZED," +
                    People.NOTES + " TEXT COLLATE LOCALIZED," +
                    People.TIMES_CONTACTED + " INTEGER NOT NULL DEFAULT 0," +
                    People.LAST_TIME_CONTACTED + " INTEGER," +
                    People.STARRED + " INTEGER NOT NULL DEFAULT 0," +
                    People.PRIMARY_PHONE_ID + " INTEGER REFERENCES phones(_id)," +
                    People.PRIMARY_ORGANIZATION_ID + " INTEGER REFERENCES organizations(_id)," +
                    People.PRIMARY_EMAIL_ID + " INTEGER REFERENCES contact_methods(_id)," +
                    People.PHOTO_VERSION + " TEXT," +
                    People.CUSTOM_RINGTONE + " TEXT," +
                    People.SEND_TO_VOICEMAIL + " INTEGER," +
                    People.PHONETIC_NAME + " TEXT COLLATE LOCALIZED" +
                    ");");

        db.execSQL("CREATE INDEX peopleNameIndex ON people (" + People.NAME + ");");
        db.execSQL("CREATE INDEX peopleSyncDirtyIndex ON people (" + People._SYNC_DIRTY + ");");
        db.execSQL("CREATE INDEX peopleSyncIdIndex ON people (" + People._SYNC_ID + ");");
        
        db.execSQL("CREATE TRIGGER people_timesContacted UPDATE OF last_time_contacted ON people " +
                    "BEGIN " +
                        "UPDATE people SET "
                            + People.TIMES_CONTACTED + " = (new." + People.TIMES_CONTACTED + " + 1)"
                            + " WHERE _id = new._id;" +
                    "END");

        // table of all the groups that exist for an account
        db.execSQL("CREATE TABLE groups (" +
                Groups._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                Groups._SYNC_ACCOUNT + " TEXT," + // From the sync source
                Groups._SYNC_ID + " TEXT," + // From the sync source
                Groups._SYNC_TIME + " TEXT," + // From the sync source
                Groups._SYNC_VERSION + " TEXT," + // From the sync source
                Groups._SYNC_LOCAL_ID + " INTEGER," + // Used while syncing, not persistent
                Groups._SYNC_DIRTY + " INTEGER NOT NULL DEFAULT 0," +
                                                          // if syncable, non-zero if the record
                                                          // has local, unsynced, changes
                Groups._SYNC_MARK + " INTEGER," + // Used to filter out new rows

                Groups.NAME + " TEXT NOT NULL," +
                Groups.NOTES + " TEXT," +
                Groups.SHOULD_SYNC + " INTEGER NOT NULL DEFAULT 0," +
                Groups.SYSTEM_ID + " TEXT," +
                "UNIQUE(" +
                Groups.NAME + ","  + Groups.SYSTEM_ID + "," + Groups._SYNC_ACCOUNT + ")" +
                ");");

        db.execSQL("CREATE INDEX groupsSyncDirtyIndex ON groups (" + Groups._SYNC_DIRTY + ");");

        if (!isTemporary()) {
            // Add the system groups, since we always need them.
            db.execSQL("INSERT INTO groups (" + Groups.NAME + ", " + Groups.SYSTEM_ID + ") VALUES "
                    + "('" + Groups.GROUP_MY_CONTACTS + "', '" + Groups.GROUP_MY_CONTACTS + "')");
        }

        db.execSQL("CREATE TABLE peopleLookup (" +
                    "token TEXT," +
                    "source INTEGER REFERENCES people(_id)" +
                    ");");
        db.execSQL("CREATE INDEX peopleLookupIndex ON peopleLookup (" +
                    "token," +
                    "source" +
                    ");");

        db.execSQL("CREATE TABLE photos ("
                + Photos._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
                + Photos.EXISTS_ON_SERVER + " INTEGER NOT NULL DEFAULT 0,"
                + Photos.PERSON_ID + " INTEGER REFERENCES people(_id), "
                + Photos.LOCAL_VERSION + " TEXT,"
                + Photos.DATA + " BLOB,"
                + Photos.SYNC_ERROR + " TEXT,"
                + Photos._SYNC_ACCOUNT + " TEXT,"
                + Photos._SYNC_ID + " TEXT,"
                + Photos._SYNC_TIME + " TEXT,"
                + Photos._SYNC_VERSION + " TEXT,"
                + Photos._SYNC_LOCAL_ID + " INTEGER,"
                + Photos._SYNC_DIRTY + " INTEGER NOT NULL DEFAULT 0,"
                + Photos._SYNC_MARK + " INTEGER,"
                + "UNIQUE(" + Photos.PERSON_ID + ") "
                + ")");

        db.execSQL("CREATE INDEX photosSyncDirtyIndex ON photos (" + Photos._SYNC_DIRTY + ");");
        db.execSQL("CREATE INDEX photoPersonIndex ON photos (person);");

        // Delete the photo row when the people row is deleted
        db.execSQL(""
                + " CREATE TRIGGER peopleDeleteAndPhotos DELETE ON people "
                + " BEGIN"
                + "   DELETE FROM photos WHERE person=OLD._id;"
                + " END");

        db.execSQL("CREATE TABLE _deleted_people (" +
                    "_sync_version TEXT," + // From the sync source
                    "_sync_id TEXT," +
                    (isTemporary() ? "_sync_local_id INTEGER," : "") + // Used while syncing,
                    "_sync_account TEXT," +
                    "_sync_mark INTEGER)"); // Used to filter out new rows

        db.execSQL("CREATE TABLE _deleted_groups (" +
                    "_sync_version TEXT," + // From the sync source
                    "_sync_id TEXT," +
                    (isTemporary() ? "_sync_local_id INTEGER," : "") + // Used while syncing,
                    "_sync_account TEXT," +
                    "_sync_mark INTEGER)"); // Used to filter out new rows

        db.execSQL("CREATE TABLE phones (" +
                    "_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "person INTEGER REFERENCES people(_id)," +
                    "type INTEGER NOT NULL," + // kind specific (home, work, etc)
                    "number TEXT," +
                    "number_key TEXT," +
                    "label TEXT," +
                    "isprimary INTEGER NOT NULL DEFAULT 0" +
                    ");");
        db.execSQL("CREATE INDEX phonesIndex1 ON phones (person);");
        db.execSQL("CREATE INDEX phonesIndex2 ON phones (number_key);");

        db.execSQL("CREATE TABLE contact_methods (" +
                    "_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "person INTEGER REFERENCES people(_id)," +
                    "kind INTEGER NOT NULL," + // the kind of contact method
                    "data TEXT," +
                    "aux_data TEXT," +
                    "type INTEGER NOT NULL," + // kind specific (home, work, etc)
                    "label TEXT," +
                    "isprimary INTEGER NOT NULL DEFAULT 0" +
                    ");");
        db.execSQL("CREATE INDEX contactMethodsPeopleIndex "
                + "ON contact_methods (person);");

        // The table for recent calls is here so we can do table joins
        // on people, phones, and calls all in one place.
        db.execSQL("CREATE TABLE calls (" +
                    "_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "number TEXT," +
                    "date INTEGER," +
                    "duration INTEGER," +
                    "type INTEGER," +
                    "new INTEGER," +
                    "name TEXT," +
                    "numbertype INTEGER," +
                    "numberlabel TEXT" +
                    ");");

        // Various settings for the contacts sync adapter. The _sync_account column may
        // be null, but it must not be the empty string.
        db.execSQL("CREATE TABLE settings (" +
                    "_id INTEGER PRIMARY KEY," +
                    "_sync_account TEXT," +
                    "key STRING NOT NULL," +
                    "value STRING " +
                    ");");

        // The table for the organizations of a person.
        db.execSQL("CREATE TABLE organizations (" +
                    "_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "company TEXT," +
                    "title TEXT," +
                    "isprimary INTEGER NOT NULL DEFAULT 0," +
                    "type INTEGER NOT NULL," + // kind specific (home, work, etc)
                    "label TEXT," +
                    "person INTEGER REFERENCES people(_id)" +
                    ");");
        db.execSQL("CREATE INDEX organizationsIndex1 ON organizations (person);");

        // The table for the extensions of a person.
        db.execSQL("CREATE TABLE extensions (" +
                    "_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "name TEXT NOT NULL," +
                    "value TEXT NOT NULL," +
                    "person INTEGER REFERENCES people(_id)," +
                    "UNIQUE(person, name)" +
                    ");");
        db.execSQL("CREATE INDEX extensionsIndex1 ON extensions (person, name);");

        // The table for the groups of a person.
        db.execSQL("CREATE TABLE groupmembership (" +
                "_id INTEGER PRIMARY KEY," +
                "person INTEGER REFERENCES people(_id)," +
                "group_id INTEGER REFERENCES groups(_id)," +
                "group_sync_account STRING," +
                "group_sync_id STRING" +
                ");");
        db.execSQL("CREATE INDEX groupmembershipIndex1 ON groupmembership (person, group_id);");
        db.execSQL("CREATE INDEX groupmembershipIndex2 ON groupmembership (group_id, person);");
        db.execSQL("CREATE INDEX groupmembershipIndex3 ON groupmembership "
                + "(group_sync_account, group_sync_id);");

        // Trigger to completely remove a contacts data when they're deleted
        db.execSQL("CREATE TRIGGER contact_cleanup DELETE ON people " +
                    "BEGIN " +
                        "DELETE FROM peopleLookup WHERE source = old._id;" +
                        "DELETE FROM phones WHERE person = old._id;" +
                        "DELETE FROM contact_methods WHERE person = old._id;" +
                        "DELETE FROM organizations WHERE person = old._id;" +
                        "DELETE FROM groupmembership WHERE person = old._id;" +
                        "DELETE FROM extensions WHERE person = old._id;" +
                    "END");

        // Trigger to disassociate the groupmembership from the groups when an
        // groups entry is deleted
        db.execSQL("CREATE TRIGGER groups_cleanup DELETE ON groups " +
                    "BEGIN " +
                        "UPDATE groupmembership SET group_id = null WHERE group_id = old._id;" +
                    "END");

        // Trigger to move an account_people row to _deleted_account_people when it is deleted
        db.execSQL("CREATE TRIGGER groups_to_deleted DELETE ON groups " +
                    "WHEN old._sync_id is not null " +
                    "BEGIN " +
                        "INSERT INTO _deleted_groups " +
                            "(_sync_id, _sync_account, _sync_version) " +
                            "VALUES (old._sync_id, old._sync_account, " +
                            "old._sync_version);" +
                    "END");

        // Triggers to keep the peopleLookup table up to date
        db.execSQL("CREATE TRIGGER peopleLookup_update UPDATE OF name ON people " +
                    "BEGIN " +
                        "DELETE FROM peopleLookup WHERE source = new._id;" +
                        "SELECT _TOKENIZE('peopleLookup', new._id, new.name, ' ');" +
                    "END");
        db.execSQL("CREATE TRIGGER peopleLookup_insert AFTER INSERT ON people " +
                    "BEGIN " +
                        "SELECT _TOKENIZE('peopleLookup', new._id, new.name, ' ');" +
                    "END");

        // Triggers to set the _sync_dirty flag when a phone is changed,
        // inserted or deleted
        db.execSQL("CREATE TRIGGER phones_update UPDATE ON phones " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");
        db.execSQL("CREATE TRIGGER phones_insert INSERT ON phones " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=new.person;" +
                    "END");
        db.execSQL("CREATE TRIGGER phones_delete DELETE ON phones " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");

        // Triggers to set the _sync_dirty flag when a contact_method is
        // changed, inserted or deleted
        db.execSQL("CREATE TRIGGER contact_methods_update UPDATE ON contact_methods " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");
        db.execSQL("CREATE TRIGGER contact_methods_insert INSERT ON contact_methods " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=new.person;" +
                    "END");
        db.execSQL("CREATE TRIGGER contact_methods_delete DELETE ON contact_methods " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");

        // Triggers for when an organization is changed, inserted or deleted
        db.execSQL("CREATE TRIGGER organizations_update AFTER UPDATE ON organizations " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER organizations_insert INSERT ON organizations " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=new.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER organizations_delete DELETE ON organizations " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");

        // Triggers for when an groupmembership is changed, inserted or deleted
        db.execSQL("CREATE TRIGGER groupmembership_update AFTER UPDATE ON groupmembership " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER groupmembership_insert INSERT ON groupmembership " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=new.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER groupmembership_delete DELETE ON groupmembership " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");

        // Triggers for when an extension is changed, inserted or deleted
        db.execSQL("CREATE TRIGGER extensions_update AFTER UPDATE ON extensions " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER extensions_insert INSERT ON extensions " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=new.person; " +
                    "END");
        db.execSQL("CREATE TRIGGER extensions_delete DELETE ON extensions " +
                    "BEGIN " +
                        "UPDATE people SET _sync_dirty=1 WHERE people._id=old.person;" +
                    "END");

        createTypeLabelTrigger(db, sPhonesTable, "INSERT");
        createTypeLabelTrigger(db, sPhonesTable, "UPDATE");
        createTypeLabelTrigger(db, sOrganizationsTable, "INSERT");
        createTypeLabelTrigger(db, sOrganizationsTable, "UPDATE");
        createTypeLabelTrigger(db, sContactMethodsTable, "INSERT");
        createTypeLabelTrigger(db, sContactMethodsTable, "UPDATE");

        // Temporary table that holds a time stamp of the last time data the voice
        // dialer is interested in has changed so the grammar won't need to be
        // recompiled when unused data is changed.
        db.execSQL("CREATE TABLE voice_dialer_timestamp (" +
                   "_id INTEGER PRIMARY KEY," +
                   "timestamp INTEGER" +
                   ");");
        db.execSQL("INSERT INTO voice_dialer_timestamp (_id, timestamp) VALUES " +
                       "(1, strftime('%s', 'now'));");
        db.execSQL("CREATE TRIGGER timestamp_trigger1 AFTER UPDATE ON phones " +
                   "BEGIN " +
                       "UPDATE voice_dialer_timestamp SET timestamp=strftime('%s', 'now') "+
                           "WHERE _id=1;" +
                   "END");
        db.execSQL("CREATE TRIGGER timestamp_trigger2 AFTER UPDATE OF name ON people " +
                   "BEGIN " +
                       "UPDATE voice_dialer_timestamp SET timestamp=strftime('%s', 'now') " +
                           "WHERE _id=1;" +
                   "END");
    }

    private void createTypeLabelTrigger(SQLiteDatabase db, String table, String operation) {
        final String name = table + "_" + operation + "_typeAndLabel";
        db.execSQL("CREATE TRIGGER " + name + " AFTER " + operation + " ON " + table
                + "   WHEN (NEW.type != 0 AND NEW.label IS NOT NULL) OR "
                + "        (NEW.type = 0 AND NEW.label IS NULL)"
                + "   BEGIN "
                + "     SELECT RAISE (ABORT, 'exactly one of type or label must be set'); "
                + "   END");
    }

    private void maybeCreatePresenceTable(SQLiteDatabase db) {
        // Load the presence table from the presence_db. Just create the table
        // if we are
        String cpDbName;
        if (!isTemporary()) {
            db.execSQL("ATTACH DATABASE ':memory:' AS presence_db;");
            cpDbName = "presence_db.";
        } else {
            cpDbName = "";
        }
        db.execSQL("CREATE TABLE IF NOT EXISTS " + cpDbName + "presence ("+
                    Presence._ID + " INTEGER PRIMARY KEY," +
                    Presence.PERSON_ID + " INTEGER REFERENCES people(_id)," +
                    Presence.IM_PROTOCOL + " TEXT," +
                    Presence.IM_HANDLE + " TEXT," +
                    Presence.IM_ACCOUNT + " TEXT," +
                    Presence.PRESENCE_STATUS + " INTEGER," +
                    Presence.PRESENCE_CUSTOM_STATUS + " TEXT," +
                    "UNIQUE(" + Presence.IM_PROTOCOL + ", " + Presence.IM_HANDLE + ", "
                            + Presence.IM_ACCOUNT + ")" +
                    ");");

        db.execSQL("CREATE INDEX IF NOT EXISTS " + cpDbName + "presenceIndex ON presence ("
                + Presence.PERSON_ID + ");");
    }

    @SuppressWarnings("deprecation")
    private String buildPeopleLookupWhereClause(String filterParam) {
        StringBuilder filter = new StringBuilder(
                "people._id IN (SELECT source FROM peopleLookup WHERE token GLOB ");
        // NOTE: Query parameters won't work here since the SQL compiler
        // needs to parse the actual string to know that it can use the
        // index to do a prefix scan.
        DatabaseUtils.appendEscapedSQLString(filter, 
                DatabaseUtils.getHexCollationKey(filterParam) + "*");
        filter.append(')');
        return filter.toString();
    }

    @Override
    public Cursor queryInternal(Uri url, String[] projectionIn,
            String selection, String[] selectionArgs, String sort) {

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        Uri notificationUri = Contacts.CONTENT_URI;
        StringBuilder whereClause;
        String groupBy = null;

        // Generate the body of the query
        int match = sURIMatcher.match(url);

        if (Config.LOGV) Log.v(TAG, "ContactsProvider.query: url=" + url + ", match is " + match);

        switch (match) {
            case DELETED_GROUPS:
                if (!isTemporary()) {
                    throw new UnsupportedOperationException();
                }

                qb.setTables(sDeletedGroupsTable);
                break;

            case GROUPS_ID:
                qb.appendWhere("_id=");
                qb.appendWhere(url.getPathSegments().get(1));
                // fall through
            case GROUPS:
                qb.setTables(sGroupsTable);
                qb.setProjectionMap(sGroupsProjectionMap);
                break;

            case SETTINGS:
                qb.setTables(sSettingsTable);
                break;

            case PEOPLE_GROUPMEMBERSHIP_ID:
                qb.appendWhere("groupmembership._id=");
                qb.appendWhere(url.getPathSegments().get(3));
                qb.appendWhere(" AND ");
                // fall through
            case PEOPLE_GROUPMEMBERSHIP:
                qb.appendWhere(sGroupsJoinString + " AND ");
                qb.appendWhere("person=" + url.getPathSegments().get(1));
                qb.setTables("groups, groupmembership");
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                break;

            case GROUPMEMBERSHIP_ID:
                qb.appendWhere("groupmembership._id=");
                qb.appendWhere(url.getPathSegments().get(1));
                qb.appendWhere(" AND ");
                // fall through
            case GROUPMEMBERSHIP:
                qb.setTables("groups, groupmembership");
                qb.setProjectionMap(sGroupMembershipProjectionMap);
                qb.appendWhere(sGroupsJoinString);
                break;

            case GROUPMEMBERSHIP_RAW:
                qb.setTables("groupmembership");
                break;

            case GROUP_NAME_MEMBERS_FILTER:
                if (url.getPathSegments().size() > 5) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                    qb.appendWhere(" AND ");
                }
                // fall through
            case GROUP_NAME_MEMBERS:
                qb.setTables(PEOPLE_PHONES_JOIN);
                qb.setProjectionMap(sPeopleProjectionMap);
                qb.appendWhere(buildGroupNameMatchWhereClause(url.getPathSegments().get(2)));
                break;
                
            case GROUP_SYSTEM_ID_MEMBERS_FILTER:
                if (url.getPathSegments().size() > 5) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                    qb.appendWhere(" AND ");
                }
                // fall through
            case GROUP_SYSTEM_ID_MEMBERS:
                qb.setTables(PEOPLE_PHONES_JOIN);
                qb.setProjectionMap(sPeopleProjectionMap);
                qb.appendWhere(buildGroupSystemIdMatchWhereClause(url.getPathSegments().get(2)));
                break;

            case PEOPLE:
                qb.setTables(PEOPLE_PHONES_JOIN);
                qb.setProjectionMap(sPeopleProjectionMap);
                break;
            case PEOPLE_RAW:
                qb.setTables(sPeopleTable);
                break;

            case PEOPLE_OWNER:
                return queryOwner(projectionIn);

            case PEOPLE_WITH_PHONES_FILTER:

                qb.appendWhere("number IS NOT NULL AND ");

                // Fall through.

            case PEOPLE_FILTER: {
                qb.setTables(PEOPLE_PHONES_JOIN);
                qb.setProjectionMap(sPeopleProjectionMap);
                if (url.getPathSegments().size() > 2) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                }
                break;
            }

            case PEOPLE_WITH_EMAIL_OR_IM_FILTER:
                String email = url.getPathSegments().get(2);
                whereClause = new StringBuilder();
                
                // Match any E-mail or IM contact methods where data exactly
                // matches the provided string.
                whereClause.append(ContactMethods.DATA);
                whereClause.append("=");
                DatabaseUtils.appendEscapedSQLString(whereClause, email);
                whereClause.append(" AND (kind = " + Contacts.KIND_EMAIL +
                        " OR kind = " + Contacts.KIND_IM + ")");
                qb.appendWhere(whereClause.toString());
                
                qb.setTables("people INNER JOIN contact_methods on (people._id = contact_methods.person)");
                qb.setProjectionMap(sPeopleWithEmailOrImProjectionMap);
                
                // Prevent returning the same person for multiple matches
                groupBy = "contact_methods.person";

                qb.setDistinct(true);
                break;
                
            case PHOTOS_ID:
                qb.appendWhere("_id="+url.getPathSegments().get(1));
                // Fall through.
            case PHOTOS:
                qb.setTables(sPhotosTable);
                qb.setProjectionMap(sPhotosProjectionMap);
                break;

            case PEOPLE_PHOTO:
                qb.appendWhere("person="+url.getPathSegments().get(1));
                qb.setTables(sPhotosTable);
                qb.setProjectionMap(sPhotosProjectionMap);
                break;

            case SEARCH_SUGGESTIONS: {
                // Force the default sort order, since the SearchManage doesn't ask for things
                // sorted, though they should be
                if (sort != null && !People.DEFAULT_SORT_ORDER.equals(sort)) {
                    throw new IllegalArgumentException("Sort ordering not allowed for this URI");
                }
                sort = SearchManager.SUGGEST_COLUMN_TEXT_1 + " COLLATE LOCALIZED ASC";

                // This will either setup the query builder so we can run the proper query below
                // and return null, or it will return a cursor with the results already in it.
                Cursor c = handleSearchSuggestionsQuery(url, qb);
                if (c != null) {
                    return c;
                }
                break;
            }
            case SEARCH_SHORTCUT: {
                qb.setTables(PEOPLE_PHONES_PHOTOS_ORGANIZATIONS_JOIN);
                qb.setProjectionMap(sSearchSuggestionsProjectionMap);
                qb.appendWhere(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID + "=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            }
            case PEOPLE_STREQUENT: {
                // Build the first query for starred
                qb.setTables(PEOPLE_PHONES_PHOTOS_JOIN);
                qb.setProjectionMap(sStrequentStarredProjectionMap);
                final String starredQuery = qb.buildQuery(projectionIn, "starred = 1",
                        null, null, null, null,
                        null /* limit */);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                qb.setTables(PEOPLE_PHONES_PHOTOS_JOIN);
                qb.setProjectionMap(sPeopleWithPhotoProjectionMap);
                final String frequentQuery = qb.buildQuery(projectionIn,
                        "times_contacted > 0 AND starred = 0", null, null, null, null, null);

                // Put them together
                final String query = qb.buildUnionQuery(new String[] {starredQuery, frequentQuery},
                        STREQUENT_ORDER_BY, STREQUENT_LIMIT);
                final SQLiteDatabase db = getDatabase();
                Cursor c = db.rawQueryWithFactory(null, query, null, sPeopleTable);
                if ((c != null) && !isTemporary()) {
                    c.setNotificationUri(getContext().getContentResolver(), notificationUri);
                }
                return c;
            }
            case PEOPLE_STREQUENT_FILTER: {
                // Build the first query for starred
                qb.setTables(PEOPLE_PHONES_PHOTOS_JOIN);
                qb.setProjectionMap(sStrequentStarredProjectionMap);
                if (url.getPathSegments().size() > 3) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                }
                final String starredQuery = qb.buildQuery(projectionIn, "starred = 1",
                        null, null, null, null,
                        null /* limit */);

                // Build the second query for frequent
                qb = new SQLiteQueryBuilder();
                qb.setTables(PEOPLE_PHONES_PHOTOS_JOIN);
                qb.setProjectionMap(sPeopleWithPhotoProjectionMap);
                if (url.getPathSegments().size() > 3) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                }
                final String frequentQuery = qb.buildQuery(projectionIn,
                        "times_contacted > 0 AND starred = 0", null, null, null, null, null);

                // Put them together
                final String query = qb.buildUnionQuery(new String[] {starredQuery, frequentQuery},
                        STREQUENT_ORDER_BY, null);
                final SQLiteDatabase db = getDatabase();
                Cursor c = db.rawQueryWithFactory(null, query, null, sPeopleTable);
                if ((c != null) && !isTemporary()) {
                    c.setNotificationUri(getContext().getContentResolver(), notificationUri);
                }
                return c;
            }
            case DELETED_PEOPLE:
                if (isTemporary()) {
                    qb.setTables("_deleted_people");
                    break;
                }
                throw new UnsupportedOperationException();
            case PEOPLE_ID:
                qb.setTables("people LEFT OUTER JOIN phones ON people.primary_phone=phones._id "
                        + "LEFT OUTER JOIN presence ON (presence." + Presence.PERSON_ID
                        + "=people._id)");
                qb.setProjectionMap(sPeopleProjectionMap);
                qb.appendWhere("people._id=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            case PEOPLE_PHONES:
                qb.setTables("phones, people");
                qb.setProjectionMap(sPhonesProjectionMap);
                qb.appendWhere("people._id = phones.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            case PEOPLE_PHONES_ID:
                qb.setTables("phones, people");
                qb.setProjectionMap(sPhonesProjectionMap);
                qb.appendWhere("people._id = phones.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                qb.appendWhere(" AND phones._id=");
                qb.appendWhere(url.getPathSegments().get(3));
                break;

            case PEOPLE_PHONES_WITH_PRESENCE:
                qb.appendWhere("people._id=?");
                selectionArgs = appendSelectionArg(selectionArgs, url.getPathSegments().get(1));
                // Fall through.

            case PHONES_WITH_PRESENCE:
                qb.setTables("phones JOIN people ON (phones.person = people._id)"
                        + " LEFT OUTER JOIN presence ON (presence.person = people._id)");
                qb.setProjectionMap(sPhonesWithPresenceProjectionMap);
                break;

            case PEOPLE_CONTACTMETHODS:
                qb.setTables("contact_methods, people");
                qb.setProjectionMap(sContactMethodsProjectionMap);
                qb.appendWhere("people._id = contact_methods.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            case PEOPLE_CONTACTMETHODS_ID:
                qb.setTables("contact_methods, people");
                qb.setProjectionMap(sContactMethodsProjectionMap);
                qb.appendWhere("people._id = contact_methods.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                qb.appendWhere(" AND contact_methods._id=");
                qb.appendWhere(url.getPathSegments().get(3));
                break;
            case PEOPLE_ORGANIZATIONS:
                qb.setTables("organizations, people");
                qb.setProjectionMap(sOrganizationsProjectionMap);
                qb.appendWhere("people._id = organizations.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            case PEOPLE_ORGANIZATIONS_ID:
                qb.setTables("organizations, people");
                qb.setProjectionMap(sOrganizationsProjectionMap);
                qb.appendWhere("people._id = organizations.person AND person=");
                qb.appendWhere(url.getPathSegments().get(1));
                qb.appendWhere(" AND organizations._id=");
                qb.appendWhere(url.getPathSegments().get(3));
                break;
            case PHONES:
                qb.setTables("phones, people");
                qb.appendWhere("people._id = phones.person");
                qb.setProjectionMap(sPhonesProjectionMap);
                break;
            case PHONES_ID:
                qb.setTables("phones, people");
                qb.appendWhere("people._id = phones.person AND phones._id="
                        + url.getPathSegments().get(1));
                qb.setProjectionMap(sPhonesProjectionMap);
                break;
            case ORGANIZATIONS:
                qb.setTables("organizations, people");
                qb.appendWhere("people._id = organizations.person");
                qb.setProjectionMap(sOrganizationsProjectionMap);
                break;
            case ORGANIZATIONS_ID:
                qb.setTables("organizations, people");
                qb.appendWhere("people._id = organizations.person AND organizations._id="
                        + url.getPathSegments().get(1));
                qb.setProjectionMap(sOrganizationsProjectionMap);
                break;
            case PHONES_MOBILE_FILTER_NAME:
                qb.appendWhere("type=" + Contacts.PhonesColumns.TYPE_MOBILE + " AND ");

                // Fall through.

            case PHONES_FILTER_NAME:
                qb.setTables("phones JOIN people ON (people._id = phones.person)");
                qb.setProjectionMap(sPhonesProjectionMap);
                if (url.getPathSegments().size() > 2) {
                    qb.appendWhere(buildPeopleLookupWhereClause(url.getLastPathSegment()));
                }
                break;

            case PHONES_FILTER: {
                String phoneNumber = url.getPathSegments().get(2);
                String indexable = PhoneNumberUtils.toCallerIDMinMatch(phoneNumber);
                StringBuilder subQuery = new StringBuilder();
                if (TextUtils.isEmpty(sort)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sort = People.DEFAULT_SORT_ORDER;
                }

                subQuery.append("people, (SELECT * FROM phones WHERE (phones.number_key GLOB '");
                subQuery.append(indexable);
                subQuery.append("*')) AS phones");
                qb.setTables(subQuery.toString());
                qb.appendWhere("phones.person=people._id AND PHONE_NUMBERS_EQUAL(phones.number, ");
                qb.appendWhereEscapeString(phoneNumber);
                qb.appendWhere(")");
                qb.setProjectionMap(sPhonesProjectionMap);
                break;
            }
            case CONTACTMETHODS:
                qb.setTables("contact_methods, people");
                qb.setProjectionMap(sContactMethodsProjectionMap);
                qb.appendWhere("people._id = contact_methods.person");
                break;
            case CONTACTMETHODS_ID:
                qb.setTables("contact_methods LEFT OUTER JOIN people ON contact_methods.person = people._id");
                qb.setProjectionMap(sContactMethodsProjectionMap);
                qb.appendWhere("contact_methods._id=");
                qb.appendWhere(url.getPathSegments().get(1));
                break;
            case CONTACTMETHODS_EMAIL_FILTER:
                String pattern = url.getPathSegments().get(2);
                whereClause = new StringBuilder();

                // TODO This is going to be REALLY slow.  Come up with
                // something faster.
                whereClause.append(ContactMethods.KIND);
                whereClause.append('=');
                whereClause.append('\'');
                whereClause.append(Contacts.KIND_EMAIL);
                whereClause.append("' AND (UPPER(");
                whereClause.append(ContactMethods.NAME);
                whereClause.append(") GLOB ");
                DatabaseUtils.appendEscapedSQLString(whereClause, pattern + "*");
                whereClause.append(" OR UPPER(");
                whereClause.append(ContactMethods.NAME);
                whereClause.append(") GLOB ");
                DatabaseUtils.appendEscapedSQLString(whereClause, "* " + pattern + "*");
                whereClause.append(") AND ");
                qb.appendWhere(whereClause.toString());

                // Fall through.

            case CONTACTMETHODS_EMAIL:
                qb.setTables("contact_methods INNER JOIN people on (contact_methods.person = people._id)");
                qb.setProjectionMap(sEmailSearchProjectionMap);
                qb.appendWhere("kind = " + Contacts.KIND_EMAIL);
                qb.setDistinct(true);
                break;

            case PEOPLE_CONTACTMETHODS_WITH_PRESENCE:
                qb.appendWhere("people._id=?");
                selectionArgs = appendSelectionArg(selectionArgs, url.getPathSegments().get(1));
                // Fall through.

            case CONTACTMETHODS_WITH_PRESENCE:
                qb.setTables("contact_methods JOIN people ON (contact_methods.person = people._id)"
                        + " LEFT OUTER JOIN presence ON "
                        // Match gtalk presence items
                        + "((kind=" + Contacts.KIND_EMAIL +
                            " AND im_protocol='"
                            + ContactMethods.encodePredefinedImProtocol(
                                    ContactMethods.PROTOCOL_GOOGLE_TALK)
                            + "' AND data=im_handle)"
                        + " OR "
                        // Match IM presence items
                        + "(kind=" + Contacts.KIND_IM
                            + " AND data=im_handle AND aux_data=im_protocol))");
                qb.setProjectionMap(sContactMethodsWithPresenceProjectionMap);
                break;

            case CALLS:
                qb.setTables("calls");
                qb.setProjectionMap(sCallsProjectionMap);
                notificationUri = CallLog.CONTENT_URI;
                break;
            case CALLS_ID:
                qb.setTables("calls");
                qb.setProjectionMap(sCallsProjectionMap);
                qb.appendWhere("calls._id=");
                qb.appendWhere(url.getPathSegments().get(1));
                notificationUri = CallLog.CONTENT_URI;
                break;
            case CALLS_FILTER: {
                qb.setTables("calls");
                qb.setProjectionMap(sCallsProjectionMap);

                String phoneNumber = url.getPathSegments().get(2);
                qb.appendWhere("PHONE_NUMBERS_EQUAL(number, ");
                qb.appendWhereEscapeString(phoneNumber);
                qb.appendWhere(")");
                notificationUri = CallLog.CONTENT_URI;
                break;
            }

            case PRESENCE:
                qb.setTables("presence LEFT OUTER JOIN people on (presence." + Presence.PERSON_ID
                        + "= people._id)");
                qb.setProjectionMap(sPresenceProjectionMap);
                break;
            case PRESENCE_ID:
                qb.setTables("presence LEFT OUTER JOIN people on (presence." + Presence.PERSON_ID
                        + "= people._id)");
                qb.appendWhere("presence._id=");
                qb.appendWhere(url.getLastPathSegment());
                break;
            case VOICE_DIALER_TIMESTAMP:
                qb.setTables("voice_dialer_timestamp");
                qb.appendWhere("_id=1");
                break;

            case PEOPLE_EXTENSIONS_ID:
                qb.appendWhere("extensions._id=" + url.getPathSegments().get(3) + " AND ");
                // fall through
            case PEOPLE_EXTENSIONS:
                qb.appendWhere("person=" + url.getPathSegments().get(1));
                qb.setTables(sExtensionsTable);
                qb.setProjectionMap(sExtensionsProjectionMap);
                break;

            case EXTENSIONS_ID:
                qb.appendWhere("extensions._id=" + url.getPathSegments().get(1));
                // fall through
            case EXTENSIONS:
                qb.setTables(sExtensionsTable);
                qb.setProjectionMap(sExtensionsProjectionMap);
                break;

            case LIVE_FOLDERS_PEOPLE:
                qb.setTables("people LEFT OUTER JOIN photos ON (people._id = photos.person)");
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                break;
                
            case LIVE_FOLDERS_PEOPLE_WITH_PHONES:
                qb.setTables("people LEFT OUTER JOIN photos ON (people._id = photos.person)");
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(People.PRIMARY_PHONE_ID + " IS NOT NULL");
                break;

            case LIVE_FOLDERS_PEOPLE_FAVORITES:
                qb.setTables("people LEFT OUTER JOIN photos ON (people._id = photos.person)");
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(People.STARRED + " <> 0");
                break;

            case LIVE_FOLDERS_PEOPLE_GROUP_NAME:
                qb.setTables("people LEFT OUTER JOIN photos ON (people._id = photos.person)");
                qb.setProjectionMap(sLiveFoldersProjectionMap);
                qb.appendWhere(buildGroupNameMatchWhereClause(url.getLastPathSegment()));
                break;

            default:
                throw new IllegalArgumentException("Unknown URL " + url);
        }

        // run the query
        final SQLiteDatabase db = getDatabase();
        Cursor c = qb.query(db, projectionIn, selection, selectionArgs,
                groupBy, null, sort);
        if ((c != null) && !isTemporary()) {
            c.setNotificationUri(getContext().getContentResolver(), notificationUri);
        }
        return c;
    }


    /**
     * Build a WHERE clause that restricts the query to match people that are a member of
     * a particular system group.  The projection map of the query must include {@link People#_ID}.
     *
     * @param groupSystemId The system group id (e.g {@link Groups#GROUP_MY_CONTACTS})
     * @return The where clause.
     */
    private CharSequence buildGroupSystemIdMatchWhereClause(String groupSystemId) {
        return "people._id IN (SELECT person FROM groupmembership JOIN groups " +
                "ON (group_id=groups._id OR " +
                "(group_sync_id = groups._sync_id AND " +
                    "group_sync_account = groups._sync_account)) "+
                "WHERE " + Groups.SYSTEM_ID + "="
                + DatabaseUtils.sqlEscapeString(groupSystemId) + ")";
    }

    /**
     * Build a WHERE clause that restricts the query to match people that are a member of
     * a group with a particular name. The projection map of the query must include
     * {@link People#_ID}.
     *
     * @param groupName The name of the group
     * @return The where clause.
     */
    private CharSequence buildGroupNameMatchWhereClause(String groupName) {
        return "people._id IN (SELECT person FROM groupmembership JOIN groups " +
                "ON (group_id=groups._id OR " +
                "(group_sync_id = groups._sync_id AND " +
                    "group_sync_account = groups._sync_account)) "+
                "WHERE " + Groups.NAME + "="
                + DatabaseUtils.sqlEscapeString(groupName) + ")";
    }

    private Cursor queryOwner(String[] projection) {
        // Check the permissions
        getContext().enforceCallingPermission("android.permission.READ_OWNER_DATA",
                "No permission to access owner info");

        // Read the owner id
        SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME_OWNER,
                Context.MODE_PRIVATE);
        long ownerId = prefs.getLong(PREF_OWNER_ID, 0);

        // Run the query
        return queryInternal(ContentUris.withAppendedId(People.CONTENT_URI, ownerId), projection,
                null, null, null);
    }

    /**
     * Append a string to a selection args array
     *
     * @param selectionArgs the old arg
     * @param newArg the new arg to append
     * @return a new string array with all of the args
     */
    private String[] appendSelectionArg(String[] selectionArgs, String newArg) {
        if (selectionArgs == null || selectionArgs.length == 0) {
            return new String[] { newArg };
        } else {
            int length = selectionArgs.length;
            String[] newArgs = new String[length + 1];
            System.arraycopy(selectionArgs, 0, newArgs, 0, length);
            newArgs[length] = newArg;
            return newArgs;
        }
    }

    /**
     * Either sets up the query builder so we can run the proper query against the database
     * and returns null, or returns a cursor with the results already in it.
     *
     * @param url the URL passed for the suggestion
     * @param qb the query builder to use if a query needs to be run on the database
     * @return null with qb configured for a query, a cursor with the results already in it.
     */
    private Cursor handleSearchSuggestionsQuery(Uri url, SQLiteQueryBuilder qb) {
        qb.setTables(PEOPLE_PHONES_PHOTOS_ORGANIZATIONS_JOIN);
        qb.setProjectionMap(sSearchSuggestionsProjectionMap);
        if (url.getPathSegments().size() > 1) {
            // A search term was entered, use it to filter

            // only match within 'my contacts'
            // TODO: match the 'display group' instead of hard coding 'my contacts'
            // once that information is factored out of the shared prefs of the contacts
            // app into this content provider.
            qb.appendWhere(buildGroupSystemIdMatchWhereClause(Groups.GROUP_MY_CONTACTS));
            qb.appendWhere(" AND ");            

            // match the query
            final String searchClause = url.getLastPathSegment();
            if (!TextUtils.isDigitsOnly(searchClause)) {
                qb.appendWhere(buildPeopleLookupWhereClause(searchClause));
            } else {
                final String[] columnNames = new String[] {
                        "_id",
                        SearchManager.SUGGEST_COLUMN_TEXT_1,
                        SearchManager.SUGGEST_COLUMN_TEXT_2,
                        SearchManager.SUGGEST_COLUMN_ICON_1,
                        SearchManager.SUGGEST_COLUMN_INTENT_DATA,
                        SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
                };

                Resources r = getContext().getResources();
                String s;
                int i;

                ArrayList<Object> dialNumber = new ArrayList<Object>();
                dialNumber.add(0);  // _id
                s = r.getString(com.android.internal.R.string.dial_number_using, searchClause);
                i = s.indexOf('\n');
                if (i < 0) {
                    dialNumber.add(s);
                    dialNumber.add("");
                } else {
                    dialNumber.add(s.substring(0, i));
                    dialNumber.add(s.substring(i + 1));
                }
                dialNumber.add(String.valueOf(android.R.drawable.sym_action_call));
                dialNumber.add("tel:" + searchClause);
                dialNumber.add(Intents.SEARCH_SUGGESTION_DIAL_NUMBER_CLICKED);  

                ArrayList<Object> createContact = new ArrayList<Object>();
                createContact.add(1);  // _id
                s = r.getString(com.android.internal.R.string.create_contact_using, searchClause);
                i = s.indexOf('\n');
                if (i < 0) {
                    createContact.add(s);
                    createContact.add("");
                } else {
                    createContact.add(s.substring(0, i));
                    createContact.add(s.substring(i + 1));
                }
                // TODO: add a "create contact" icon
                createContact.add(String.valueOf(android.R.drawable.ic_menu_add));
                createContact.add("tel:" + searchClause);
                createContact.add(Intents.SEARCH_SUGGESTION_CREATE_CONTACT_CLICKED);

                ArrayList<ArrayList> rows = new ArrayList<ArrayList>();
                rows.add(dialNumber);
                rows.add(createContact);

                ArrayListCursor cursor = new ArrayListCursor(columnNames, rows);
                return cursor;
            }
        }
        return null;
    }

    @Override
    public String getType(Uri url) {
        int match = sURIMatcher.match(url);
        switch (match) {
            case EXTENSIONS:
            case PEOPLE_EXTENSIONS:
                return Extensions.CONTENT_TYPE;
            case EXTENSIONS_ID:
            case PEOPLE_EXTENSIONS_ID:
                return Extensions.CONTENT_ITEM_TYPE;
            case PEOPLE:
                return "vnd.android.cursor.dir/person";
            case PEOPLE_ID:
                return "vnd.android.cursor.item/person";
            case PEOPLE_PHONES:
                return "vnd.android.cursor.dir/phone";
            case PEOPLE_PHONES_ID:
                return "vnd.android.cursor.item/phone";
            case PEOPLE_CONTACTMETHODS:
                return "vnd.android.cursor.dir/contact-methods";
            case PEOPLE_CONTACTMETHODS_ID:
                return getContactMethodType(url);
            case PHONES:
                return "vnd.android.cursor.dir/phone";
            case PHONES_ID:
                return "vnd.android.cursor.item/phone";
            case PHONES_FILTER:
            case PHONES_FILTER_NAME:
            case PHONES_MOBILE_FILTER_NAME:
                return "vnd.android.cursor.dir/phone";
            case PHOTOS_ID:
                return "vnd.android.cursor.item/photo";
            case PHOTOS:
                return "vnd.android.cursor.dir/photo";
            case PEOPLE_PHOTO:
                return "vnd.android.cursor.item/photo";
            case CONTACTMETHODS:
                return "vnd.android.cursor.dir/contact-methods";
            case CONTACTMETHODS_ID:
                return getContactMethodType(url);
            case CONTACTMETHODS_EMAIL:
            case CONTACTMETHODS_EMAIL_FILTER:
                return "vnd.android.cursor.dir/email";
            case CALLS:
                return "vnd.android.cursor.dir/calls";
            case CALLS_ID:
                return "vnd.android.cursor.item/calls";
            case ORGANIZATIONS:
                return "vnd.android.cursor.dir/organizations";
            case ORGANIZATIONS_ID:
                return "vnd.android.cursor.item/organization";
            case CALLS_FILTER:
                return "vnd.android.cursor.dir/calls";
            case SEARCH_SUGGESTIONS:
                return SearchManager.SUGGEST_MIME_TYPE;
            case SEARCH_SHORTCUT:
                return SearchManager.SHORTCUT_MIME_TYPE;
            default:
                throw new IllegalArgumentException("Unknown URL");
        }
    }

    private String getContactMethodType(Uri url)
    {
        String mime = null;

        Cursor c = query(url, new String[] {ContactMethods.KIND}, null, null, null);
        if (c != null) {
            try {
                if (c.moveToFirst()) {
                    int kind = c.getInt(0);
                    switch (kind) {
                    case Contacts.KIND_EMAIL:
                        mime = "vnd.android.cursor.item/email";
                        break;

                    case Contacts.KIND_IM:
                        mime = "vnd.android.cursor.item/jabber-im";
                        break;

                    case Contacts.KIND_POSTAL:
                        mime = "vnd.android.cursor.item/postal-address";
                        break;
                    }
                }
            } finally {
                c.close();
            }
        }
        return mime;
    }

    private ContentValues queryAndroidStarredGroupId(String account) {
        String whereString;
        String[] whereArgs;
        if (!TextUtils.isEmpty(account)) {
            whereString = "_sync_account=? AND name=?";
            whereArgs = new String[]{account, Groups.GROUP_ANDROID_STARRED};
        } else {
            whereString = "_sync_account is null AND name=?";
            whereArgs = new String[]{Groups.GROUP_ANDROID_STARRED};
        }
        Cursor cursor = getDatabase().query(sGroupsTable,
                new String[]{Groups._ID, Groups._SYNC_ID, Groups._SYNC_ACCOUNT},
                whereString, whereArgs, null, null, null);
        try {
            if (cursor.moveToNext()) {
                ContentValues result = new ContentValues();
                result.put(Groups._ID, cursor.getLong(0));
                result.put(Groups._SYNC_ID, cursor.getString(1));
                result.put(Groups._SYNC_ACCOUNT, cursor.getString(2));
                return result;
            }
            return null;
        } finally {
            cursor.close();
        }
    }

    @Override
    public Uri insertInternal(Uri url, ContentValues initialValues) {
        Uri resultUri = null;
        long rowID;

        final SQLiteDatabase db = getDatabase();
        int match = sURIMatcher.match(url);
        switch (match) {
            case PEOPLE_GROUPMEMBERSHIP:
            case GROUPMEMBERSHIP: {
                mValues.clear();
                mValues.putAll(initialValues);
                if (match == PEOPLE_GROUPMEMBERSHIP) {
                    mValues.put(GroupMembership.PERSON_ID,
                            Long.valueOf(url.getPathSegments().get(1)));
                }
                resultUri = insertIntoGroupmembership(mValues);
            }
            break;

            case PEOPLE_OWNER:
                return insertOwner(initialValues);

            case PEOPLE_EXTENSIONS:
            case EXTENSIONS: {
                ContentValues newMap = new ContentValues(initialValues);
                if (match == PEOPLE_EXTENSIONS) {
                    newMap.put(Extensions.PERSON_ID,
                            Long.valueOf(url.getPathSegments().get(1)));
                }
                rowID = mExtensionsInserter.insert(newMap);
                if (rowID > 0) {
                    resultUri = ContentUris.withAppendedId(Extensions.CONTENT_URI, rowID);
                }
            }
            break;

            case PHOTOS: {
                if (!isTemporary()) {
                    throw new UnsupportedOperationException();
                }
                rowID = mPhotosInserter.insert(initialValues);
                if (rowID > 0) {
                    resultUri = ContentUris.withAppendedId(Photos.CONTENT_URI, rowID);
                }
            }
            break;

            case GROUPS: {
                ContentValues newMap = new ContentValues(initialValues);
                ensureSyncAccountIsSet(newMap);
                newMap.put(Groups._SYNC_DIRTY, 1);
                // Insert into the groups table
                rowID = mGroupsInserter.insert(newMap);
                if (rowID > 0) {
                    resultUri = ContentUris.withAppendedId(Groups.CONTENT_URI, rowID);
                    if (!isTemporary() && newMap.containsKey(Groups.SHOULD_SYNC)) {
                        final String account = newMap.getAsString(Groups._SYNC_ACCOUNT);
                        if (!TextUtils.isEmpty(account)) {
                            final ContentResolver cr = getContext().getContentResolver();
                            onLocalChangesForAccount(cr, account, false);
                        }
                    }
                }
            }
            break;

            case PEOPLE_RAW:
            case PEOPLE: {
                mValues.clear();
                mValues.putAll(initialValues);
                ensureSyncAccountIsSet(mValues);
                mValues.put(People._SYNC_DIRTY, 1);
                // Insert into the people table
                rowID = mPeopleInserter.insert(mValues);
                if (rowID > 0) {
                    resultUri = ContentUris.withAppendedId(People.CONTENT_URI, rowID);
                    if (!isTemporary()) {
                        String account = mValues.getAsString(People._SYNC_ACCOUNT);
                        Long starredValue = mValues.getAsLong(People.STARRED);
                        final String syncId = mValues.getAsString(People._SYNC_ID);
                        boolean isStarred = starredValue != null && starredValue != 0;
                        fixupGroupMembershipAfterPeopleUpdate(account, rowID, isStarred);
                        // create a photo row for this person
                        mDb.delete(sPhotosTable, "person=" + rowID, null);
                        mValues.clear();
                        mValues.put(Photos.PERSON_ID, rowID);
                        mValues.put(Photos._SYNC_ACCOUNT, account);
                        mValues.put(Photos._SYNC_ID, syncId);
                        mValues.put(Photos._SYNC_DIRTY, 0);
                        mPhotosInserter.insert(mValues);
                    }
                }
            }
            break;

            case DELETED_PEOPLE: {
                if (isTemporary()) {
                    // Insert into the people table
                    rowID = db.insert("_deleted_people", "_sync_id", initialValues);
                    if (rowID > 0) {
                        resultUri = Uri.parse("content://contacts/_deleted_people/" + rowID);
                    }
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            break;

            case DELETED_GROUPS: {
                if (isTemporary()) {
                    rowID = db.insert(sDeletedGroupsTable, Groups._SYNC_ID,
                            initialValues);
                    if (rowID > 0) {
                        resultUri =ContentUris.withAppendedId(
                                Groups.DELETED_CONTENT_URI, rowID);
                    }
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            break;

            case PEOPLE_PHONES:
            case PHONES: {
                mValues.clear();
                mValues.putAll(initialValues);
                if (match == PEOPLE_PHONES) {
                    mValues.put(Contacts.Phones.PERSON_ID,
                            Long.valueOf(url.getPathSegments().get(1)));
                }
                String number = mValues.getAsString(Contacts.Phones.NUMBER);
                if (number != null) {
                    mValues.put("number_key", PhoneNumberUtils.getStrippedReversed(number));
                }

                rowID = insertAndFixupPrimary(Contacts.KIND_PHONE, mValues);
                resultUri = ContentUris.withAppendedId(Phones.CONTENT_URI, rowID);
            }
            break;

            case CONTACTMETHODS:
            case PEOPLE_CONTACTMETHODS: {
                mValues.clear();
                mValues.putAll(initialValues);
                if (match == PEOPLE_CONTACTMETHODS) {
                    mValues.put("person", url.getPathSegments().get(1));
                }
                Integer kind = mValues.getAsInteger(ContactMethods.KIND);
                if (kind == null) {
                    throw new IllegalArgumentException("you must specify the ContactMethods.KIND");
                }
                rowID = insertAndFixupPrimary(kind, mValues);
                if (rowID > 0) {
                    resultUri = ContentUris.withAppendedId(ContactMethods.CONTENT_URI, rowID);
                }
            }
            break;

            case CALLS: {
                rowID = mCallsInserter.insert(initialValues);
                if (rowID > 0) {
                    resultUri = Uri.parse("content://call_log/calls/" + rowID);
                }
            }
            break;

            case PRESENCE: {
                final String handle = initialValues.getAsString(Presence.IM_HANDLE);
                final String protocol = initialValues.getAsString(Presence.IM_PROTOCOL);
                if (TextUtils.isEmpty(handle) || TextUtils.isEmpty(protocol)) {
                    throw new IllegalArgumentException("IM_PROTOCOL and IM_HANDLE are required");
                }

                // Look for the contact for this presence update
                StringBuilder query = new StringBuilder("SELECT ");
                query.append(ContactMethods.PERSON_ID);
                query.append(" FROM contact_methods WHERE (kind=");
                query.append(Contacts.KIND_IM);
                query.append(" AND ");
                query.append(ContactMethods.DATA);
                query.append("=? AND ");
                query.append(ContactMethods.AUX_DATA);
                query.append("=?)");

                String[] selectionArgs;
                if (GTALK_PROTOCOL_STRING.equals(protocol)) {
                    // For gtalk accounts we usually don't have an explicit IM
                    // entry, so also look for the email address as well
                    query.append(" OR (");
                    query.append("kind=");
                    query.append(Contacts.KIND_EMAIL);
                    query.append(" AND ");
                    query.append(ContactMethods.DATA);
                    query.append("=?)");
                    selectionArgs = new String[] { handle, protocol, handle };
                } else {
                    selectionArgs = new String[] { handle, protocol };
                }

                Cursor c = db.rawQueryWithFactory(null, query.toString(), selectionArgs, null);

                long personId = 0;
                try {
                    if (c.moveToFirst()) {
                        personId = c.getLong(0);
                    } else {
                        // No contact found, return a null URI
                        return null;
                    }
                } finally {
                    c.close();
                }

                mValues.clear();
                mValues.putAll(initialValues);
                mValues.put(Presence.PERSON_ID, personId);

                // Insert the presence update
                rowID = db.replace("presence", null, mValues);
                if (rowID > 0) {
                    resultUri = Uri.parse("content://contacts/presence/" + rowID);
                }
            }
            break;

            case PEOPLE_ORGANIZATIONS:
            case ORGANIZATIONS: {
                ContentValues newMap = new ContentValues(initialValues);
                if (match == PEOPLE_ORGANIZATIONS) {
                    newMap.put(Contacts.Phones.PERSON_ID,
                            Long.valueOf(url.getPathSegments().get(1)));
                }
                rowID = insertAndFixupPrimary(Contacts.KIND_ORGANIZATION, newMap);
                if (rowID > 0) {
                    resultUri = Uri.parse("content://contacts/organizations/" + rowID);
                }
            }
            break;
            default:
                throw new UnsupportedOperationException("Cannot insert into URL: " + url);
        }

        return resultUri;
    }

    @Override
    protected void onAccountsChanged(String[] accountsArray) {
        super.onAccountsChanged(accountsArray);
        synchronized (mAccountsLock) {
            mAccounts = new String[accountsArray.length];
            System.arraycopy(accountsArray, 0, mAccounts, 0, mAccounts.length);
        }
    }

    private void ensureSyncAccountIsSet(ContentValues values) {
        synchronized (mAccountsLock) {
            String account = values.getAsString(SyncConstValue._SYNC_ACCOUNT);
            if (account == null && mAccounts.length > 0) {
                values.put(SyncConstValue._SYNC_ACCOUNT, mAccounts[0]);
            }
        }
    }

    private Uri insertOwner(ContentValues values) {
        // Check the permissions
        getContext().enforceCallingPermission("android.permission.WRITE_OWNER_DATA",
                "No permission to set owner info");

        // Insert the owner info
        Uri uri = insertInternal(People.CONTENT_URI, values);

        // Record which person is the owner
        long id = ContentUris.parseId(uri);
        SharedPreferences.Editor prefs = getContext().getSharedPreferences(PREFS_NAME_OWNER,
                Context.MODE_PRIVATE).edit();
        prefs.putLong(PREF_OWNER_ID, id);
        prefs.commit();
        return uri;
    }

    private Uri insertIntoGroupmembership(ContentValues values) {
        String groupSyncAccount = values.getAsString(GroupMembership.GROUP_SYNC_ACCOUNT);
        String groupSyncId = values.getAsString(GroupMembership.GROUP_SYNC_ID);
        final Long personId = values.getAsLong(GroupMembership.PERSON_ID);
        if (!values.containsKey(GroupMembership.GROUP_ID)) {
            if (TextUtils.isEmpty(groupSyncAccount) || TextUtils.isEmpty(groupSyncId)) {
                throw new IllegalArgumentException(
                        "insertIntoGroupmembership: no GROUP_ID wasn't specified and non-empty "
                        + "GROUP_SYNC_ID and GROUP_SYNC_ACCOUNT fields weren't specifid, "
                        + values);
            }
            if (0 != DatabaseUtils.longForQuery(getDatabase(), ""
                    + "SELECT COUNT(*) "
                    + "FROM groupmembership "
                    + "WHERE group_sync_id=? AND person=?",
                    new String[]{groupSyncId, String.valueOf(personId)})) {
                final String errorMessage =
                        "insertIntoGroupmembership: a row with this server key already exists, "
                                + values;
                if (Config.LOGD) Log.d(TAG, errorMessage);
                return null;
            }
        } else {
            long groupId = values.getAsLong(GroupMembership.GROUP_ID);
            if (!TextUtils.isEmpty(groupSyncAccount) || !TextUtils.isEmpty(groupSyncId)) {
                throw new IllegalArgumentException(
                        "insertIntoGroupmembership: GROUP_ID was specified but "
                        + "GROUP_SYNC_ID and GROUP_SYNC_ACCOUNT fields were also specifid, "
                        + values);
            }
            if (0 != DatabaseUtils.longForQuery(getDatabase(),
                    "SELECT COUNT(*) FROM groupmembership where group_id=? AND person=?",
                    new String[]{String.valueOf(groupId), String.valueOf(personId)})) {
                final String errorMessage =
                        "insertIntoGroupmembership: a row with this local key already exists, "
                                + values;
                if (Config.LOGD) Log.d(TAG, errorMessage);
                return null;
            }
        }

        long rowId = mGroupMembershipInserter.insert(values);
        if (rowId <= 0) {
            final String errorMessage = "insertIntoGroupmembership: the insert failed, values are "
                    + values;
            if (Config.LOGD) Log.d(TAG, errorMessage);
            return null;
        }

        // set the STARRED column in the people row if this group is the GROUP_ANDROID_STARRED
        if (!isTemporary() && queryGroupMembershipContainsStarred(personId)) {
            fixupPeopleStarred(personId, true);
        }

        return ContentUris.withAppendedId(GroupMembership.CONTENT_URI, rowId);
    }

    private void fixupGroupMembershipAfterPeopleUpdate(String account, long personId,
            boolean makeStarred) {
        ContentValues starredGroupInfo = queryAndroidStarredGroupId(account);
        if (makeStarred) {
            if (starredGroupInfo == null) {
                // we need to add the starred group
                mValuesLocal.clear();
                mValuesLocal.put(Groups.NAME, Groups.GROUP_ANDROID_STARRED);
                mValuesLocal.put(Groups._SYNC_DIRTY, 1);
                mValuesLocal.put(Groups._SYNC_ACCOUNT, account);
                long groupId = mGroupsInserter.insert(mValuesLocal);
                starredGroupInfo = new ContentValues();
                starredGroupInfo.put(Groups._ID, groupId);
                starredGroupInfo.put(Groups._SYNC_ACCOUNT, account);
                // don't put the _SYNC_ID in here since we don't know it yet
            }

            final Long groupId = starredGroupInfo.getAsLong(Groups._ID);
            final String syncId = starredGroupInfo.getAsString(Groups._SYNC_ID);
            final String syncAccount = starredGroupInfo.getAsString(Groups._SYNC_ACCOUNT);

            // check that either groupId is set or the syncId/Account is set
            final boolean hasSyncId = !TextUtils.isEmpty(syncId);
            final boolean hasGroupId = groupId != null;
            if (!hasGroupId && !hasSyncId) {
                throw new IllegalStateException("at least one of the groupId or "
                        + "the syncId must be set, " + starredGroupInfo);
            }
            
            // now add this person to the group
            mValuesLocal.clear();
            mValuesLocal.put(GroupMembership.PERSON_ID, personId);
            mValuesLocal.put(GroupMembership.GROUP_ID, groupId);
            mValuesLocal.put(GroupMembership.GROUP_SYNC_ID, syncId);
            mValuesLocal.put(GroupMembership.GROUP_SYNC_ACCOUNT, syncAccount);
            mGroupMembershipInserter.insert(mValuesLocal);
        } else {
            if (starredGroupInfo != null) {
                // delete the groupmembership rows for this person that match the starred group id
                String syncAccount = starredGroupInfo.getAsString(Groups._SYNC_ACCOUNT);
                String syncId = starredGroupInfo.getAsString(Groups._SYNC_ID);
                if (!TextUtils.isEmpty(syncId)) {
                    mDb.delete(sGroupmembershipTable,
                            "person=? AND group_sync_id=? AND group_sync_account=?",
                            new String[]{String.valueOf(personId), syncId, syncAccount});
                } else {
                    mDb.delete(sGroupmembershipTable, "person=? AND group_id=?",
                            new String[]{
                                    Long.toString(personId),
                                    Long.toString(starredGroupInfo.getAsLong(Groups._ID))});
                }
            }
        }
    }

    private int fixupPeopleStarred(long personId, boolean inStarredGroup) {
        mValuesLocal.clear();
        mValuesLocal.put(People.STARRED, inStarredGroup ? 1 : 0);
        return getDatabase().update(sPeopleTable, mValuesLocal, WHERE_ID,
                new String[]{String.valueOf(personId)});
    }

    private String kindToTable(int kind) {
        switch (kind) {
            case Contacts.KIND_EMAIL: return sContactMethodsTable;
            case Contacts.KIND_POSTAL: return sContactMethodsTable;
            case Contacts.KIND_IM: return sContactMethodsTable;
            case Contacts.KIND_PHONE: return sPhonesTable;
            case Contacts.KIND_ORGANIZATION: return sOrganizationsTable;
            default: throw new IllegalArgumentException("unknown kind, " + kind);
        }
    }

    private DatabaseUtils.InsertHelper kindToInserter(int kind) {
        switch (kind) {
            case Contacts.KIND_EMAIL: return mContactMethodsInserter;
            case Contacts.KIND_POSTAL: return mContactMethodsInserter;
            case Contacts.KIND_IM: return mContactMethodsInserter;
            case Contacts.KIND_PHONE: return mPhonesInserter;
            case Contacts.KIND_ORGANIZATION: return mOrganizationsInserter;
            default: throw new IllegalArgumentException("unknown kind, " + kind);
        }
    }

    private long insertAndFixupPrimary(int kind, ContentValues values) {
        final String table = kindToTable(kind);
        boolean isPrimary = false;
        Long personId = null;

        if (!isTemporary()) {
            // when you add a item, if isPrimary or if there is no primary,
            // make this it, set the isPrimary flag, and clear other primary flags
            isPrimary = values.containsKey("isprimary")
                    && (values.getAsInteger("isprimary") != 0);
            personId = values.getAsLong("person");
            if (!isPrimary) {
                // make it primary anyway if this person doesn't have any rows of this type yet
                StringBuilder sb = new StringBuilder("person=" + personId);
                if (sContactMethodsTable.equals(table)) {
                    sb.append(" AND kind=");
                    sb.append(kind);
                }
                final boolean isFirstRowOfType = DatabaseUtils.longForQuery(getDatabase(),
                        "SELECT count(*) FROM " + table + " where " + sb.toString(), null) == 0;
                isPrimary = isFirstRowOfType;
            }

            values.put("isprimary", isPrimary ? 1 : 0);
        }

        // do the actual insert
        long newRowId = kindToInserter(kind).insert(values);

        if (newRowId <= 0) {
            throw new RuntimeException("error while inserting into " + table + ", " + values);
        }

        if (!isTemporary()) {
            // If this row was made the primary then clear the other isprimary flags and update
            // corresponding people row, if necessary.
            if (isPrimary) {
                clearOtherIsPrimary(kind, personId, newRowId);
                if (kind == Contacts.KIND_PHONE) {
                    updatePeoplePrimary(personId, People.PRIMARY_PHONE_ID, newRowId);
                } else if (kind == Contacts.KIND_EMAIL) {
                    updatePeoplePrimary(personId, People.PRIMARY_EMAIL_ID, newRowId);
                } else if (kind == Contacts.KIND_ORGANIZATION) {
                    updatePeoplePrimary(personId, People.PRIMARY_ORGANIZATION_ID, newRowId);
                }
            }
        }

        return newRowId;
    }

    @Override
    public int deleteInternal(Uri url, String userWhere, String[] whereArgs) {
        String tableToChange;
        String changedItemId;

        final int matchedUriId = sURIMatcher.match(url);
        switch (matchedUriId) {
            case GROUPMEMBERSHIP_ID:
                return deleteFromGroupMembership(Long.parseLong(url.getPathSegments().get(1)),
                        userWhere, whereArgs);
            case GROUPS:
                return deleteFromGroups(userWhere, whereArgs);
            case GROUPS_ID:
                changedItemId = url.getPathSegments().get(1);
                return deleteFromGroups(addIdToWhereClause(changedItemId, userWhere), whereArgs);
            case EXTENSIONS:
                tableToChange = sExtensionsTable;
                changedItemId = null;
                break;
            case EXTENSIONS_ID:
                tableToChange = sExtensionsTable;
                changedItemId = url.getPathSegments().get(1);
                break;
            case PEOPLE_RAW:
            case PEOPLE:
                return deleteFromPeople(null, userWhere, whereArgs);
            case PEOPLE_ID:
                return deleteFromPeople(url.getPathSegments().get(1), userWhere, whereArgs);
            case PEOPLE_PHONES_ID:
                tableToChange = sPhonesTable;
                changedItemId = url.getPathSegments().get(3);
                break;
            case PEOPLE_CONTACTMETHODS_ID:
                tableToChange = sContactMethodsTable;
                changedItemId = url.getPathSegments().get(3);
                break;
            case PHONES_ID:
                tableToChange = sPhonesTable;
                changedItemId = url.getPathSegments().get(1);
                break;
            case ORGANIZATIONS_ID:
                tableToChange = sOrganizationsTable;
                changedItemId = url.getPathSegments().get(1);
                break;
            case CONTACTMETHODS_ID:
                tableToChange = sContactMethodsTable;
                changedItemId = url.getPathSegments().get(1);
                break;
            case PRESENCE:
                tableToChange = "presence";
                changedItemId = null;
                break;
            case CALLS:
                tableToChange = "calls";
                changedItemId = null;
                break;
            default:
                throw new UnsupportedOperationException("Cannot delete that URL: " + url);
        }

        String where = addIdToWhereClause(changedItemId, userWhere);
        IsPrimaryInfo oldPrimaryInfo = null;
        switch (matchedUriId) {
            case PEOPLE_PHONES_ID:
            case PHONES_ID:
            case ORGANIZATIONS_ID:
                oldPrimaryInfo = lookupIsPrimaryInfo(tableToChange,
                        sIsPrimaryProjectionWithoutKind, where, whereArgs);
                break;

            case PEOPLE_CONTACTMETHODS_ID:
            case CONTACTMETHODS_ID:
                oldPrimaryInfo = lookupIsPrimaryInfo(tableToChange,
                        sIsPrimaryProjectionWithKind, where, whereArgs);
                break;
        }

        final SQLiteDatabase db = getDatabase();
        int count = db.delete(tableToChange, where, whereArgs);
        if (count > 0) {
            if (oldPrimaryInfo != null && oldPrimaryInfo.isPrimary) {
                fixupPrimaryAfterDelete(oldPrimaryInfo.kind,
                        oldPrimaryInfo.id, oldPrimaryInfo.person);
            }
        }

        return count;
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        int match = sURIMatcher.match(uri);
        switch (match) {
            default:
                throw new UnsupportedOperationException(uri.toString());
        }
    }

    private int deleteFromGroupMembership(long rowId, String where, String[] whereArgs) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables("groups, groupmembership");
        qb.setProjectionMap(sGroupMembershipProjectionMap);
        qb.appendWhere(sGroupsJoinString);
        qb.appendWhere(" AND groupmembership._id=" + rowId);
        Cursor cursor = qb.query(getDatabase(), null, where, whereArgs, null, null, null);
        try {
            final int indexPersonId = cursor.getColumnIndexOrThrow(GroupMembership.PERSON_ID);
            final int indexName = cursor.getColumnIndexOrThrow(GroupMembership.NAME);
            while (cursor.moveToNext()) {
                if (Groups.GROUP_ANDROID_STARRED.equals(cursor.getString(indexName))) {
                    fixupPeopleStarred(cursor.getLong(indexPersonId), false);
                }
            }
        } finally {
            cursor.close();
        }

        return mDb.delete(sGroupmembershipTable,
                addIdToWhereClause(String.valueOf(rowId), where),
                whereArgs);
    }

    private int deleteFromPeople(String rowId, String where, String[] whereArgs) {
        final SQLiteDatabase db = getDatabase();
        where = addIdToWhereClause(rowId, where);
        Cursor cursor = db.query(sPeopleTable, null, where, whereArgs, null, null, null);
        try {
            final int idxSyncId = cursor.getColumnIndexOrThrow(People._SYNC_ID);
            final int idxSyncAccount = cursor.getColumnIndexOrThrow(People._SYNC_ACCOUNT);
            final int idxSyncVersion = cursor.getColumnIndexOrThrow(People._SYNC_VERSION);
            final int dstIdxSyncId = mDeletedPeopleInserter.getColumnIndex(SyncConstValue._SYNC_ID);
            final int dstIdxSyncAccount =
                    mDeletedPeopleInserter.getColumnIndex(SyncConstValue._SYNC_ACCOUNT);
            final int dstIdxSyncVersion =
                    mDeletedPeopleInserter.getColumnIndex(SyncConstValue._SYNC_VERSION);
            while (cursor.moveToNext()) {
                final String syncId = cursor.getString(idxSyncId);
                if (TextUtils.isEmpty(syncId)) continue;
                // insert into deleted table
                mDeletedPeopleInserter.prepareForInsert();
                mDeletedPeopleInserter.bind(dstIdxSyncId, syncId);
                mDeletedPeopleInserter.bind(dstIdxSyncAccount, cursor.getString(idxSyncAccount));
                mDeletedPeopleInserter.bind(dstIdxSyncVersion, cursor.getString(idxSyncVersion));
                mDeletedPeopleInserter.execute();
            }
        } finally {
            cursor.close();
        }

        // perform the actual delete
        return db.delete(sPeopleTable, where, whereArgs);
    }

    private int deleteFromGroups(String where, String[] whereArgs) {
        HashSet<String> modifiedAccounts = Sets.newHashSet();
        Cursor cursor = getDatabase().query(sGroupsTable, null, where, whereArgs,
                null, null, null);
        try {
            final int indexName = cursor.getColumnIndexOrThrow(Groups.NAME);
            final int indexSyncAccount = cursor.getColumnIndexOrThrow(Groups._SYNC_ACCOUNT);
            final int indexSyncId = cursor.getColumnIndexOrThrow(Groups._SYNC_ID);
            final int indexId = cursor.getColumnIndexOrThrow(Groups._ID);
            final int indexShouldSync = cursor.getColumnIndexOrThrow(Groups.SHOULD_SYNC);
            while (cursor.moveToNext()) {
                String oldName = cursor.getString(indexName);
                String syncAccount = cursor.getString(indexSyncAccount);
                String syncId = cursor.getString(indexSyncId);
                boolean shouldSync = cursor.getLong(indexShouldSync) != 0;
                long id = cursor.getLong(indexId);
                fixupPeopleStarredOnGroupRename(oldName, null, id);
                if (!TextUtils.isEmpty(syncAccount) && !TextUtils.isEmpty(syncId)) {
                    fixupPeopleStarredOnGroupRename(oldName, null, syncAccount, syncId);
                }
                if (!TextUtils.isEmpty(syncAccount) && shouldSync) {
                    modifiedAccounts.add(syncAccount);
                }
            }
        } finally {
            cursor.close();
        }

        int numRows = mDb.delete(sGroupsTable, where, whereArgs);
        if (numRows > 0) {
            if (!isTemporary()) {
                final ContentResolver cr = getContext().getContentResolver();
                for (String account : modifiedAccounts) {
                    onLocalChangesForAccount(cr, account, true);
                }
            }
        }
        return numRows;
    }

    /**
     * Called when local changes are made, so subclasses have
     * an opportunity to react as they see fit.
     *
     * @param resolver the content resolver to use
     * @param account the account the changes are tied to
     */
    protected void onLocalChangesForAccount(final ContentResolver resolver, String account,
            boolean groupsModified) {
        // Do nothing
    }

    private void fixupPrimaryAfterDelete(int kind, Long itemId, Long personId) {
        final String table = kindToTable(kind);
        // when you delete an item with isPrimary,
        // select a new one as isPrimary and clear the primary if no more items
        Long newPrimaryId = findNewPrimary(kind, personId, itemId);

        // we found a new primary, set its isprimary flag
        if (newPrimaryId != null) {
            mValuesLocal.clear();
            mValuesLocal.put("isprimary", 1);
            if (getDatabase().update(table, mValuesLocal, "_id=" + newPrimaryId, null) != 1) {
                throw new RuntimeException("error updating " + table + ", _id "
                        + newPrimaryId + ", values " + mValuesLocal);
            }
        }

        // if this kind's primary status should be reflected in the people row, update it
        if (kind == Contacts.KIND_PHONE) {
            updatePeoplePrimary(personId, People.PRIMARY_PHONE_ID, newPrimaryId);
        } else if (kind == Contacts.KIND_EMAIL) {
            updatePeoplePrimary(personId, People.PRIMARY_EMAIL_ID, newPrimaryId);
        } else if (kind == Contacts.KIND_ORGANIZATION) {
            updatePeoplePrimary(personId, People.PRIMARY_ORGANIZATION_ID, newPrimaryId);
        }
    }

    @Override
    public int updateInternal(Uri url, ContentValues values, String userWhere, String[] whereArgs) {
        final SQLiteDatabase db = getDatabase();
        String tableToChange;
        String changedItemId;
        final int matchedUriId = sURIMatcher.match(url);
        switch (matchedUriId) {
            case GROUPS_ID:
                changedItemId = url.getPathSegments().get(1);
                return updateGroups(values,
                        addIdToWhereClause(changedItemId, userWhere), whereArgs);

            case PEOPLE_EXTENSIONS_ID:
                tableToChange = sExtensionsTable;
                changedItemId = url.getPathSegments().get(3);
                break;

            case EXTENSIONS_ID:
                tableToChange = sExtensionsTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case PEOPLE_UPDATE_CONTACT_TIME:
                if (values.size() != 1 || !values.containsKey(People.LAST_TIME_CONTACTED)) {
                    throw new IllegalArgumentException(
                            "You may only use " + url + " to update People.LAST_TIME_CONTACTED");
                }
                tableToChange = sPeopleTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case PEOPLE_ID:
                mValues.clear();
                mValues.putAll(values);
                mValues.put(Photos._SYNC_DIRTY, 1);
                values = mValues;
                tableToChange = sPeopleTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case PEOPLE_PHONES_ID:
                tableToChange = sPhonesTable;
                changedItemId = url.getPathSegments().get(3);
                break;

            case PEOPLE_CONTACTMETHODS_ID:
                tableToChange = sContactMethodsTable;
                changedItemId = url.getPathSegments().get(3);
                break;

            case PHONES_ID:
                tableToChange = sPhonesTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case PEOPLE_PHOTO:
            case PHOTOS_ID:
                mValues.clear();
                mValues.putAll(values);

                // The _SYNC_DIRTY flag should only be set if the data was modified and if
                // it isn't already provided. 
                if (!mValues.containsKey(Photos._SYNC_DIRTY) && mValues.containsKey(Photos.DATA)) {
                    mValues.put(Photos._SYNC_DIRTY, 1);
                }
                StringBuilder where;
                if (matchedUriId == PEOPLE_PHOTO) {
                    where = new StringBuilder("_id=" + url.getPathSegments().get(1));
                } else {
                    where = new StringBuilder("person=" + url.getPathSegments().get(1));
                }
                if (!TextUtils.isEmpty(userWhere)) {
                    where.append(" AND (");
                    where.append(userWhere);
                    where.append(')');
                }
                return db.update(sPhotosTable, mValues, where.toString(), whereArgs);

            case ORGANIZATIONS_ID:
                tableToChange = sOrganizationsTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case CONTACTMETHODS_ID:
                tableToChange = sContactMethodsTable;
                changedItemId = url.getPathSegments().get(1);
                break;

            case SETTINGS:
                if (whereArgs != null) {
                    throw new IllegalArgumentException(
                            "you aren't allowed to specify where args when updating settings");
                }
                if (userWhere != null) {
                    throw new IllegalArgumentException(
                            "you aren't allowed to specify a where string when updating settings");
                }
                return updateSettings(values);

            case CALLS:
                tableToChange = "calls";
                changedItemId = null;
                break;

            case CALLS_ID:
                tableToChange = "calls";
                changedItemId = url.getPathSegments().get(1);
                break;

            default:
                throw new UnsupportedOperationException("Cannot update URL: " + url);
        }

        String where = addIdToWhereClause(changedItemId, userWhere);
        int numRowsUpdated = db.update(tableToChange, values, where, whereArgs);

        if (numRowsUpdated > 0 && changedItemId != null) {
            long itemId = Long.parseLong(changedItemId);
            switch (matchedUriId) {
                case ORGANIZATIONS_ID:
                    fixupPrimaryAfterUpdate(
                            Contacts.KIND_ORGANIZATION, null, itemId,
                            values.getAsInteger(Organizations.ISPRIMARY));
                    break;

                case PHONES_ID:
                case PEOPLE_PHONES_ID:
                    fixupPrimaryAfterUpdate(
                            Contacts.KIND_PHONE, matchedUriId == PEOPLE_PHONES_ID
                                    ? Long.parseLong(url.getPathSegments().get(1))
                                    : null, itemId,
                            values.getAsInteger(Phones.ISPRIMARY));
                    break;

                case CONTACTMETHODS_ID:
                case PEOPLE_CONTACTMETHODS_ID:
                    IsPrimaryInfo isPrimaryInfo = lookupIsPrimaryInfo(sContactMethodsTable,
                            sIsPrimaryProjectionWithKind, where, whereArgs);
                    fixupPrimaryAfterUpdate(
                            isPrimaryInfo.kind, isPrimaryInfo.person, itemId,
                            values.getAsInteger(ContactMethods.ISPRIMARY));
                    break;

                case PEOPLE_ID:
                    boolean hasStarred = values.containsKey(People.STARRED);
                    boolean hasPrimaryPhone = values.containsKey(People.PRIMARY_PHONE_ID);
                    boolean hasPrimaryOrganization =
                            values.containsKey(People.PRIMARY_ORGANIZATION_ID);
                    boolean hasPrimaryEmail = values.containsKey(People.PRIMARY_EMAIL_ID);
                    if (hasStarred || hasPrimaryPhone || hasPrimaryOrganization
                            || hasPrimaryEmail) {
                        Cursor c = mDb.query(sPeopleTable, null,
                                where, whereArgs, null, null, null);
                        try {
                            int indexAccount = c.getColumnIndexOrThrow(People._SYNC_ACCOUNT);
                            int indexId = c.getColumnIndexOrThrow(People._ID);
                            Long starredValue = values.getAsLong(People.STARRED);
                            Long primaryPhone = values.getAsLong(People.PRIMARY_PHONE_ID);
                            Long primaryOrganization =
                                    values.getAsLong(People.PRIMARY_ORGANIZATION_ID);
                            Long primaryEmail = values.getAsLong(People.PRIMARY_EMAIL_ID);
                            while (c.moveToNext()) {
                                final long personId = c.getLong(indexId);
                                if (hasStarred) {
                                    fixupGroupMembershipAfterPeopleUpdate(c.getString(indexAccount),
                                            personId, starredValue != null && starredValue != 0);
                                }

                                if (hasPrimaryPhone) {
                                    if (primaryPhone == null) {
                                        throw new IllegalArgumentException(
                                                "the value of PRIMARY_PHONE_ID must not be null");
                                    }
                                    setIsPrimary(Contacts.KIND_PHONE, personId, primaryPhone);
                                }
                                if (hasPrimaryOrganization) {
                                    if (primaryOrganization == null) {
                                        throw new IllegalArgumentException(
                                                "the value of PRIMARY_ORGANIZATION_ID must "
                                                        + "not be null");
                                    }
                                    setIsPrimary(Contacts.KIND_ORGANIZATION, personId,
                                            primaryOrganization);
                                }
                                if (hasPrimaryEmail) {
                                    if (primaryEmail == null) {
                                        throw new IllegalArgumentException(
                                                "the value of PRIMARY_EMAIL_ID must not be null");
                                    }
                                    setIsPrimary(Contacts.KIND_EMAIL, personId, primaryEmail);
                                }
                            }
                        } finally {
                            c.close();
                        }
                    }
                    break;
            }
        }

        return numRowsUpdated;
    }

    private int updateSettings(ContentValues values) {
        final SQLiteDatabase db = getDatabase();
        final String account = values.getAsString(Contacts.Settings._SYNC_ACCOUNT);
        final String key = values.getAsString(Contacts.Settings.KEY);
        if (key == null) {
            throw new IllegalArgumentException("you must specify the key when updating settings");
        }
        if (account == null) {
            db.delete(sSettingsTable, "_sync_account IS NULL AND key=?", new String[]{key});
        } else {
            if (TextUtils.isEmpty(account)) {
                throw new IllegalArgumentException("account cannot be the empty string, " + values);
            }
            db.delete(sSettingsTable, "_sync_account=? AND key=?", new String[]{account, key});
        }
        long rowId = db.insert(sSettingsTable, Contacts.Settings.KEY, values);
        if (rowId < 0) {
            throw new SQLException("error updating settings with " + values);
        }
        return 1;
    }

    private int updateGroups(ContentValues values, String where, String[] whereArgs) {
        for (Map.Entry<String, Object> entry : values.valueSet()) {
            final String column = entry.getKey();
            if (!Groups.NAME.equals(column) && !Groups.NOTES.equals(column)
                    && !Groups.SYSTEM_ID.equals(column) && !Groups.SHOULD_SYNC.equals(column)) {
                throw new IllegalArgumentException(
                        "you are not allowed to change column " + column);
            }
        }

        Set<String> modifiedAccounts = Sets.newHashSet();
        final SQLiteDatabase db = getDatabase();
        if (values.containsKey(Groups.NAME) || values.containsKey(Groups.SHOULD_SYNC)) {
            String newName = values.getAsString(Groups.NAME);
            Cursor cursor = db.query(sGroupsTable, null, where, whereArgs, null, null, null);
            try {
                final int indexName = cursor.getColumnIndexOrThrow(Groups.NAME);
                final int indexSyncAccount = cursor.getColumnIndexOrThrow(Groups._SYNC_ACCOUNT);
                final int indexSyncId = cursor.getColumnIndexOrThrow(Groups._SYNC_ID);
                final int indexId = cursor.getColumnIndexOrThrow(Groups._ID);
                while (cursor.moveToNext()) {
                    String syncAccount = cursor.getString(indexSyncAccount);
                    if (values.containsKey(Groups.NAME)) {
                        String oldName = cursor.getString(indexName);
                        String syncId = cursor.getString(indexSyncId);
                        long id = cursor.getLong(indexId);
                        fixupPeopleStarredOnGroupRename(oldName, newName, id);
                        if (!TextUtils.isEmpty(syncAccount) && !TextUtils.isEmpty(syncId)) {
                            fixupPeopleStarredOnGroupRename(oldName, newName, syncAccount, syncId);
                        }
                    }
                    if (!TextUtils.isEmpty(syncAccount) && values.containsKey(Groups.SHOULD_SYNC)) {
                        modifiedAccounts.add(syncAccount);
                    }
                }
            } finally {
                cursor.close();
            }
        }

        int numRows = db.update(sGroupsTable, values, where, whereArgs);
        if (numRows > 0) {
            if (!isTemporary()) {
                final ContentResolver cr = getContext().getContentResolver();
                for (String account : modifiedAccounts) {
                    onLocalChangesForAccount(cr, account, true);
                }
            }
        }
        return numRows;
    }

    void fixupPeopleStarredOnGroupRename(String oldName, String newName,
            String where, String[] whereArgs) {
        if (TextUtils.equals(oldName, newName)) return;

        int starredValue;
        if (Groups.GROUP_ANDROID_STARRED.equals(newName)) {
            starredValue = 1;
        } else if (Groups.GROUP_ANDROID_STARRED.equals(oldName)) {
            starredValue = 0;
        } else {
            return;
        }

        getDatabase().execSQL("UPDATE people SET starred=" + starredValue + " WHERE _id in ("
                + "SELECT person "
                + "FROM groups, groupmembership "
                + "WHERE " + where + " AND " + sGroupsJoinString + ")",
                whereArgs);
    }

    void fixupPeopleStarredOnGroupRename(String oldName, String newName,
            String syncAccount, String syncId) {
        fixupPeopleStarredOnGroupRename(oldName, newName, "_sync_account=? AND _sync_id=?",
                new String[]{syncAccount, syncId});
    }

    void fixupPeopleStarredOnGroupRename(String oldName, String newName, long groupId) {
        fixupPeopleStarredOnGroupRename(oldName, newName, "group_id=?",
                new String[]{String.valueOf(groupId)});
    }

    private void fixupPrimaryAfterUpdate(int kind, Long personId, Long changedItemId,
            Integer isPrimaryValue) {
        final String table = kindToTable(kind);

        // - when you update isPrimary to true,
        //   make the changed item the primary, clear others
        // - when you update isPrimary to false,
        //   select a new one as isPrimary, clear the primary if no more phones
        if (isPrimaryValue != null) {
            if (personId == null) {
                personId = lookupPerson(table, changedItemId);
            }
            
            boolean isPrimary = isPrimaryValue != 0;
            Long newPrimary = changedItemId;
            if (!isPrimary) {
                newPrimary = findNewPrimary(kind, personId, changedItemId);
            }
            clearOtherIsPrimary(kind, personId, changedItemId);

            if (kind == Contacts.KIND_PHONE) {
                updatePeoplePrimary(personId, People.PRIMARY_PHONE_ID, newPrimary);
            } else if (kind == Contacts.KIND_EMAIL) {
                updatePeoplePrimary(personId, People.PRIMARY_EMAIL_ID, newPrimary);
            } else if (kind == Contacts.KIND_ORGANIZATION) {
                updatePeoplePrimary(personId, People.PRIMARY_ORGANIZATION_ID, newPrimary);
            }
        }
    }

    /**
     * Queries table to find the value of the person column for the row with _id. There must
     * be exactly one row that matches this id.
     * @param table the table to query
     * @param id the id of the row to query
     * @return the value of the person column for the specified row, returned as a String.
     */
    private long lookupPerson(String table, long id) {
        return DatabaseUtils.longForQuery(
                getDatabase(),
                "SELECT person FROM " + table + " where _id=" + id,
                null);
    }

    /**
     * Used to pass around information about a row that has the isprimary column.
     */
    private class IsPrimaryInfo {
        boolean isPrimary;
        Long person;
        Long id;
        Integer kind;
    }

    /**
     * Queries the table to determine the state of the row's isprimary column and the kind.
     * The where and whereArgs must be sufficient to match either 0 or 1 row.
     * @param table the table of rows to consider, supports "phones" and "contact_methods"
     * @param projection the projection to use to get the columns that pertain to table
     * @param where used in conjunction with the whereArgs to identify the row
     * @param where used in conjunction with the where string to identify the row
     * @return the IsPrimaryInfo about the matched row, or null if no row was matched
     */
    private IsPrimaryInfo lookupIsPrimaryInfo(String table, String[] projection, String where,
            String[] whereArgs) {
        Cursor cursor = getDatabase().query(table, projection, where, whereArgs, null, null, null);
        try {
            if (!(cursor.getCount() <= 1)) {
                throw new IllegalArgumentException("expected only zero or one rows, got "
                        + DatabaseUtils.dumpCursorToString(cursor));
            }
            if (!cursor.moveToFirst()) return null;
            IsPrimaryInfo info = new IsPrimaryInfo();
            info.isPrimary = cursor.getInt(0) != 0;
            info.person = cursor.getLong(1);
            info.id = cursor.getLong(2);
            if (projection == sIsPrimaryProjectionWithKind) {
                info.kind = cursor.getInt(3);
            } else {
                if (sPhonesTable.equals(table)) {
                    info.kind = Contacts.KIND_PHONE;
                } else if (sOrganizationsTable.equals(table)) {
                    info.kind = Contacts.KIND_ORGANIZATION;
                } else {
                    throw new IllegalArgumentException("unexpected table, " + table);
                }
            }
            return info;
        } finally {
            cursor.close();
        }
    }

    /**
     * Returns the rank of the table-specific type, used when deciding which row
     * should be primary when none are primary. The lower the rank the better the type.
     * @param table supports "phones", "contact_methods" and "organizations"
     * @param type the table-specific type from the TYPE column
     * @return the rank of the table-specific type, the lower the better
     */
    private int getRankOfType(String table, int type) {
        if (table.equals(sPhonesTable)) {
            switch (type) {
                case Contacts.Phones.TYPE_MOBILE: return 0;
                case Contacts.Phones.TYPE_WORK: return 1;
                case Contacts.Phones.TYPE_HOME: return 2;
                case Contacts.Phones.TYPE_PAGER: return 3;
                case Contacts.Phones.TYPE_CUSTOM: return 4;
                case Contacts.Phones.TYPE_OTHER: return 5;
                case Contacts.Phones.TYPE_FAX_WORK: return 6;
                case Contacts.Phones.TYPE_FAX_HOME: return 7;
                default: return 1000;
            }
        }

        if (table.equals(sContactMethodsTable)) {
            switch (type) {
                case Contacts.ContactMethods.TYPE_HOME: return 0;
                case Contacts.ContactMethods.TYPE_WORK: return 1;
                case Contacts.ContactMethods.TYPE_CUSTOM: return 2;
                case Contacts.ContactMethods.TYPE_OTHER: return 3;
                default: return 1000;
            }
        }

        if (table.equals(sOrganizationsTable)) {
            switch (type) {
                case Organizations.TYPE_WORK: return 0;
                case Organizations.TYPE_CUSTOM: return 1;
                case Organizations.TYPE_OTHER: return 2;
                default: return 1000;
            }
        }

        throw new IllegalArgumentException("unexpected table, " + table);
    }

    /**
     * Determines which of the rows in table for the personId should be picked as the primary
     * row based on the rank of the row's type.
     * @param kind the kind of contact
     * @param personId used to limit the rows to those pertaining to this person
     * @param itemId optional, a row to ignore
     * @return the _id of the row that should be the new primary. Is null if there are no
     *   matching rows.
     */
    private Long findNewPrimary(int kind, Long personId, Long itemId) {
        final String table = kindToTable(kind);
        if (personId == null) throw new IllegalArgumentException("personId must not be null");
        StringBuilder sb = new StringBuilder();
        sb.append("person=");
        sb.append(personId);
        if (itemId != null) {
            sb.append(" and _id!=");
            sb.append(itemId);
        }
        if (sContactMethodsTable.equals(table)) {
            sb.append(" and ");
            sb.append(ContactMethods.KIND);
            sb.append("=");
            sb.append(kind);
        }

        Cursor cursor = getDatabase().query(table, ID_TYPE_PROJECTION, sb.toString(),
                null, null, null, null);
        try {
            Long newPrimaryId = null;
            int bestRank = -1;
            while (cursor.moveToNext()) {
                final int rank = getRankOfType(table, cursor.getInt(1));
                if (bestRank == -1 || rank < bestRank) {
                    newPrimaryId = cursor.getLong(0);
                    bestRank = rank;
                }
            }
            return newPrimaryId;
        } finally {
            cursor.close();
        }
    }

    private void setIsPrimary(int kind, long personId, long itemId) {
        final String table = kindToTable(kind);
        StringBuilder sb = new StringBuilder();
        sb.append("person=");
        sb.append(personId);

        if (sContactMethodsTable.equals(table)) {
            sb.append(" and ");
            sb.append(ContactMethods.KIND);
            sb.append("=");
            sb.append(kind);
        }

        final String where = sb.toString();
        getDatabase().execSQL(
                "UPDATE " + table + " SET isprimary=(_id=" + itemId + ") WHERE " + where);
    }

    /**
     * Clears the isprimary flag for all rows other than the itemId.
     * @param kind the kind of item
     * @param personId used to limit the updates to rows pertaining to this person
     * @param itemId which row to leave untouched
     */
    private void clearOtherIsPrimary(int kind, Long personId, Long itemId) {
        final String table = kindToTable(kind);
        if (personId == null) throw new IllegalArgumentException("personId must not be null");
        StringBuilder sb = new StringBuilder();
        sb.append("person=");
        sb.append(personId);
        if (itemId != null) {
            sb.append(" and _id!=");
            sb.append(itemId);
        }
        if (sContactMethodsTable.equals(table)) {
            sb.append(" and ");
            sb.append(ContactMethods.KIND);
            sb.append("=");
            sb.append(kind);
        }

        mValuesLocal.clear();
        mValuesLocal.put("isprimary", 0);
        getDatabase().update(table, mValuesLocal, sb.toString(), null);
    }

    /**
     * Set the specified primary column for the person. This is used to make the people
     * row reflect the isprimary flag in the people or contactmethods tables, which is
     * authoritative.
     * @param personId the person to modify
     * @param column the name of the primary column (phone or email)
     * @param primaryId the new value to write into the primary column
     */
    private void updatePeoplePrimary(Long personId, String column, Long primaryId) {
        mValuesLocal.clear();
        mValuesLocal.put(column, primaryId);
        getDatabase().update(sPeopleTable, mValuesLocal, "_id=" + personId, null);
    }

    private static String addIdToWhereClause(String id, String where) {
        if (id != null) {
            StringBuilder whereSb = new StringBuilder("_id=");
            whereSb.append(id);
            if (!TextUtils.isEmpty(where)) {
                whereSb.append(" AND (");
                whereSb.append(where);
                whereSb.append(')');
            }
            return whereSb.toString();
        } else {
            return where;
        }
    }

    private boolean queryGroupMembershipContainsStarred(long personId) {
        // TODO: Part 1 of 2 part hack to work around a bug in reusing SQLiteStatements
        SQLiteStatement mGroupsMembershipQuery = null;

        if (mGroupsMembershipQuery == null) {
            String query =
                "SELECT COUNT(*) FROM groups, groupmembership WHERE "
                + sGroupsJoinString + " AND person=? AND groups.name=?";
            mGroupsMembershipQuery = getDatabase().compileStatement(query);
        }
        long result = DatabaseUtils.longForQuery(mGroupsMembershipQuery,
                new String[]{String.valueOf(personId), Groups.GROUP_ANDROID_STARRED});

        // TODO: Part 2 of 2 part hack to work around a bug in reusing SQLiteStatements
        mGroupsMembershipQuery.close();

        return result != 0;
    }

    @Override
    public boolean changeRequiresLocalSync(Uri uri) {
        final int match = sURIMatcher.match(uri);
        switch (match) {
            // Changes to these URIs cannot cause syncable data to be changed, so don't
            // bother trying to sync them.
            case CALLS:
            case CALLS_FILTER:
            case CALLS_ID:
            case PRESENCE:
            case PRESENCE_ID:
            case PEOPLE_UPDATE_CONTACT_TIME:
                return false;

            default:
                return true;
        }
    }

    @Override
    protected Iterable<? extends AbstractTableMerger> getMergers() {
        ArrayList<AbstractTableMerger> list = new ArrayList<AbstractTableMerger> ();
        list.add(new PersonMerger());
        list.add(new GroupMerger());
        list.add(new PhotoMerger());
        return list;
    }

    protected static String sPeopleTable = "people";
    protected static Uri sPeopleRawURL = Uri.parse("content://contacts/people/raw/");
    protected static String sDeletedPeopleTable = "_deleted_people";
    protected static Uri sDeletedPeopleURL = Uri.parse("content://contacts/deleted_people/");
    protected static String sGroupsTable = "groups";
    protected static String sSettingsTable = "settings";
    protected static Uri sGroupsURL = Uri.parse("content://contacts/groups/");
    protected static String sDeletedGroupsTable = "_deleted_groups";
    protected static Uri sDeletedGroupsURL =
            Uri.parse("content://contacts/deleted_groups/");
    protected static String sPhonesTable = "phones";
    protected static String sOrganizationsTable = "organizations";
    protected static String sContactMethodsTable = "contact_methods";
    protected static String sGroupmembershipTable = "groupmembership";
    protected static String sPhotosTable = "photos";
    protected static Uri sPhotosURL = Uri.parse("content://contacts/photos/");
    protected static String sExtensionsTable = "extensions";
    protected static String sCallsTable = "calls";

    protected class PersonMerger extends AbstractTableMerger
    {
        private ContentValues mValues = new ContentValues();
        Map<String, SQLiteCursor> mCursorMap = Maps.newHashMap();
        public PersonMerger()
        {
            super(getDatabase(),
                    sPeopleTable, sPeopleRawURL, sDeletedPeopleTable, sDeletedPeopleURL);
        }

        @Override
        protected void notifyChanges() {
            // notify that a change has occurred.
            getContext().getContentResolver().notifyChange(Contacts.CONTENT_URI,
                    null /* observer */, false /* do not sync to network */);
        }

        @Override
        public void insertRow(ContentProvider diffs, Cursor diffsCursor) {
            final SQLiteDatabase db = getDatabase();

            Long localPrimaryPhoneId = null;
            Long localPrimaryEmailId = null;
            Long localPrimaryOrganizationId = null;

            // Copy the person
            mPeopleInserter.prepareForInsert();
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People._SYNC_ID, mPeopleInserter, mIndexPeopleSyncId);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People._SYNC_TIME, mPeopleInserter, mIndexPeopleSyncTime);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People._SYNC_VERSION, mPeopleInserter, mIndexPeopleSyncVersion);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People._SYNC_DIRTY, mPeopleInserter, mIndexPeopleSyncDirty);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People._SYNC_ACCOUNT, mPeopleInserter, mIndexPeopleSyncAccount);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People.NAME, mPeopleInserter, mIndexPeopleName);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People.PHONETIC_NAME, mPeopleInserter, mIndexPeoplePhoneticName);
            DatabaseUtils.cursorStringToInsertHelper(diffsCursor, People.NOTES, mPeopleInserter, mIndexPeopleNotes);
            long localPersonID = mPeopleInserter.execute();

            Cursor c;
            final SQLiteDatabase diffsDb = ((ContactsProvider) diffs).getDatabase();
            long diffsPersonID = diffsCursor.getLong(diffsCursor.getColumnIndexOrThrow(People._ID));

            // Copy the Photo info
            c = doSubQuery(diffsDb, sPhotosTable, null, diffsPersonID, null);
            try {
                if (c.moveToNext()) {
                    mDb.delete(sPhotosTable, "person=" + localPersonID, null);
                    mPhotosInserter.prepareForInsert();
                    DatabaseUtils.cursorStringToInsertHelper(c, Photos._SYNC_ID,
                            mPhotosInserter, mIndexPhotosSyncId);
                    DatabaseUtils.cursorStringToInsertHelper(c, Photos._SYNC_TIME,
                            mPhotosInserter, mIndexPhotosSyncTime);
                    DatabaseUtils.cursorStringToInsertHelper(c, Photos._SYNC_VERSION,
                            mPhotosInserter, mIndexPhotosSyncVersion);
                    DatabaseUtils.cursorStringToInsertHelper(c, Photos._SYNC_ACCOUNT,
                            mPhotosInserter, mIndexPhotosSyncAccount);
                    DatabaseUtils.cursorStringToInsertHelper(c, Photos.EXISTS_ON_SERVER,
                            mPhotosInserter, mIndexPhotosExistsOnServer);
                    mPhotosInserter.bind(mIndexPhotosSyncError, (String)null);
                    mPhotosInserter.bind(mIndexPhotosSyncDirty, 0);
                    mPhotosInserter.bind(mIndexPhotosPersonId, localPersonID);
                    mPhotosInserter.execute();
                }
            } finally {
                c.deactivate();
            }

            // Copy all phones
            c = doSubQuery(diffsDb, sPhonesTable, null, diffsPersonID, sPhonesTable + "._id");
            if (c != null) {
                Long newPrimaryId = null;
                int bestRank = -1;
                final int labelIndex = c.getColumnIndexOrThrow(Phones.LABEL);
                final int typeIndex = c.getColumnIndexOrThrow(Phones.TYPE);
                final int numberIndex = c.getColumnIndexOrThrow(Phones.NUMBER);
                final int keyIndex = c.getColumnIndexOrThrow(Phones.NUMBER_KEY);
                final int primaryIndex = c.getColumnIndexOrThrow(Phones.ISPRIMARY);
                while(c.moveToNext()) {
                    final int type = c.getInt(typeIndex);
                    final int isPrimaryValue = c.getInt(primaryIndex);
                    mPhonesInserter.prepareForInsert();
                    mPhonesInserter.bind(mIndexPhonesPersonId, localPersonID);
                    mPhonesInserter.bind(mIndexPhonesLabel, c.getString(labelIndex));
                    mPhonesInserter.bind(mIndexPhonesType, type);
                    mPhonesInserter.bind(mIndexPhonesNumber, c.getString(numberIndex));
                    mPhonesInserter.bind(mIndexPhonesNumberKey, c.getString(keyIndex));
                    mPhonesInserter.bind(mIndexPhonesIsPrimary, isPrimaryValue);
                    long rowId = mPhonesInserter.execute();

                    if (isPrimaryValue != 0) {
                        if (localPrimaryPhoneId != null) {
                            throw new IllegalArgumentException(
                                    "more than one phone was marked as primary, "
                                            + DatabaseUtils.dumpCursorToString(c));
                        }
                        localPrimaryPhoneId = rowId;
                    }

                    if (localPrimaryPhoneId == null) {
                        final int rank = getRankOfType(sPhonesTable, type);
                        if (bestRank == -1 || rank < bestRank) {
                            newPrimaryId = rowId;
                            bestRank = rank;
                        }
                    }
                }
                c.deactivate();

                if (localPrimaryPhoneId == null) {
                    localPrimaryPhoneId = newPrimaryId;
                }
            }

            // Copy all contact_methods
            c = doSubQuery(diffsDb, sContactMethodsTable, null, diffsPersonID,
                    sContactMethodsTable + "._id");
            if (c != null) {
                Long newPrimaryId = null;
                int bestRank = -1;
                final int labelIndex = c.getColumnIndexOrThrow(ContactMethods.LABEL);
                final int kindIndex = c.getColumnIndexOrThrow(ContactMethods.KIND);
                final int typeIndex = c.getColumnIndexOrThrow(ContactMethods.TYPE);
                final int dataIndex = c.getColumnIndexOrThrow(ContactMethods.DATA);
                final int auxDataIndex = c.getColumnIndexOrThrow(ContactMethods.AUX_DATA);
                final int primaryIndex = c.getColumnIndexOrThrow(ContactMethods.ISPRIMARY);
                while(c.moveToNext()) {
                    final int type = c.getInt(typeIndex);
                    final int kind = c.getInt(kindIndex);
                    final int isPrimaryValue = c.getInt(primaryIndex);
                    mContactMethodsInserter.prepareForInsert();
                    mContactMethodsInserter.bind(mIndexContactMethodsPersonId, localPersonID);
                    mContactMethodsInserter.bind(mIndexContactMethodsLabel, c.getString(labelIndex));
                    mContactMethodsInserter.bind(mIndexContactMethodsKind, kind);
                    mContactMethodsInserter.bind(mIndexContactMethodsType, type);
                    mContactMethodsInserter.bind(mIndexContactMethodsData, c.getString(dataIndex));
                    mContactMethodsInserter.bind(mIndexContactMethodsAuxData, c.getString(auxDataIndex));
                    mContactMethodsInserter.bind(mIndexContactMethodsIsPrimary, isPrimaryValue);
                    long rowId = mContactMethodsInserter.execute();
                    if ((kind == Contacts.KIND_EMAIL) && (isPrimaryValue != 0)) {
                        if (localPrimaryEmailId != null) {
                            throw new IllegalArgumentException(
                                    "more than one email was marked as primary, "
                                            + DatabaseUtils.dumpCursorToString(c));
                        }
                        localPrimaryEmailId = rowId;
                    }

                    if (localPrimaryEmailId == null) {
                        final int rank = getRankOfType(sContactMethodsTable, type);
                        if (bestRank == -1 || rank < bestRank) {
                            newPrimaryId = rowId;
                            bestRank = rank;
                        }
                    }
                }
                c.deactivate();

                if (localPrimaryEmailId == null) {
                    localPrimaryEmailId = newPrimaryId;
                }
            }

            // Copy all organizations
            c = doSubQuery(diffsDb, sOrganizationsTable, null, diffsPersonID,
                    sOrganizationsTable + "._id");
            try {
                Long newPrimaryId = null;
                int bestRank = -1;
                final int labelIndex = c.getColumnIndexOrThrow(Organizations.LABEL);
                final int typeIndex = c.getColumnIndexOrThrow(Organizations.TYPE);
                final int companyIndex = c.getColumnIndexOrThrow(Organizations.COMPANY);
                final int titleIndex = c.getColumnIndexOrThrow(Organizations.TITLE);
                final int primaryIndex = c.getColumnIndexOrThrow(Organizations.ISPRIMARY);
                while(c.moveToNext()) {
                    final int type = c.getInt(typeIndex);
                    final int isPrimaryValue = c.getInt(primaryIndex);
                    mOrganizationsInserter.prepareForInsert();
                    mOrganizationsInserter.bind(mIndexOrganizationsPersonId, localPersonID);
                    mOrganizationsInserter.bind(mIndexOrganizationsLabel, c.getString(labelIndex));
                    mOrganizationsInserter.bind(mIndexOrganizationsType, type);
                    mOrganizationsInserter.bind(mIndexOrganizationsCompany, c.getString(companyIndex));
                    mOrganizationsInserter.bind(mIndexOrganizationsTitle, c.getString(titleIndex));
                    mOrganizationsInserter.bind(mIndexOrganizationsIsPrimary, isPrimaryValue);
                    long rowId = mOrganizationsInserter.execute();
                    if (isPrimaryValue != 0) {
                        if (localPrimaryOrganizationId != null) {
                            throw new IllegalArgumentException(
                                    "more than one organization was marked as primary, "
                                            + DatabaseUtils.dumpCursorToString(c));
                        }
                        localPrimaryOrganizationId = rowId;
                    }

                    if (localPrimaryOrganizationId == null) {
                        final int rank = getRankOfType(sOrganizationsTable, type);
                        if (bestRank == -1 || rank < bestRank) {
                            newPrimaryId = rowId;
                            bestRank = rank;
                        }
                    }
                }

                if (localPrimaryOrganizationId == null) {
                    localPrimaryOrganizationId = newPrimaryId;
                }
            } finally {
                c.deactivate();
            }

            // Copy all groupmembership rows
            c = doSubQuery(diffsDb, sGroupmembershipTable, null, diffsPersonID,
                    sGroupmembershipTable + "._id");
            try {
                final int accountIndex =
                    c.getColumnIndexOrThrow(GroupMembership.GROUP_SYNC_ACCOUNT);
                final int idIndex = c.getColumnIndexOrThrow(GroupMembership.GROUP_SYNC_ID);
                while(c.moveToNext()) {
                    mGroupMembershipInserter.prepareForInsert();
                    mGroupMembershipInserter.bind(mIndexGroupMembershipPersonId, localPersonID);
                    mGroupMembershipInserter.bind(mIndexGroupMembershipGroupSyncAccount, c.getString(accountIndex));
                    mGroupMembershipInserter.bind(mIndexGroupMembershipGroupSyncId, c.getString(idIndex));
                    mGroupMembershipInserter.execute();
                }
            } finally {
                c.deactivate();
            }

            // Copy all extensions rows
            c = doSubQuery(diffsDb, sExtensionsTable, null, diffsPersonID, sExtensionsTable + "._id");
            try {
                final int nameIndex = c.getColumnIndexOrThrow(Extensions.NAME);
                final int valueIndex = c.getColumnIndexOrThrow(Extensions.VALUE);
                while(c.moveToNext()) {
                    mExtensionsInserter.prepareForInsert();
                    mExtensionsInserter.bind(mIndexExtensionsPersonId, localPersonID);
                    mExtensionsInserter.bind(mIndexExtensionsName, c.getString(nameIndex));
                    mExtensionsInserter.bind(mIndexExtensionsValue, c.getString(valueIndex));
                    mExtensionsInserter.execute();
                }
            } finally {
                c.deactivate();
            }

            // Update the _SYNC_DIRTY flag of the person. We have to do this
            // after inserting since the updated of the phones, contact
            // methods and organizations will fire a sql trigger that will
            // cause this flag to be set.
            mValues.clear();
            mValues.put(People._SYNC_DIRTY, 0);
            mValues.put(People.PRIMARY_PHONE_ID, localPrimaryPhoneId);
            mValues.put(People.PRIMARY_EMAIL_ID, localPrimaryEmailId);
            mValues.put(People.PRIMARY_ORGANIZATION_ID, localPrimaryOrganizationId);
            final boolean isStarred = queryGroupMembershipContainsStarred(localPersonID);
            mValues.put(People.STARRED, isStarred ? 1 : 0);
            db.update(mTable, mValues, People._ID + '=' + localPersonID, null);
        }

        @Override
        public void updateRow(long localPersonID, ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localPersonID, null, diffs, diffsCursor, false);
        }

        @Override
        public void resolveRow(long localPersonID, String syncID,
                ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localPersonID, syncID, diffs, diffsCursor, true);
        }

        protected void updateOrResolveRow(long localPersonID, String syncID,
                ContentProvider diffs, Cursor diffsCursor, boolean conflicts) {
            final SQLiteDatabase db = getDatabase();
            // The local version of localPersonId's record has changed. This
            // person also has a changed record in the diffs. Merge the changes
            // in the following way:
            //  - if any fields in the people table changed use the server's
            //    version
            //  - for phones, emails, addresses, compute the join of all unique
            //    subrecords. If any of the subrecords has changes in both
            //    places then choose the server version of the subrecord
            //
            // Limitation: deletes of phones, emails, or addresses are ignored
            // when the record has changed on both the client and the server

            long diffsPersonID = diffsCursor.getLong(diffsCursor.getColumnIndexOrThrow("_id"));

            // Join the server phones, organizations, and contact_methods with the local ones.
            //  - Add locally any that exist only on the server.
            //  - If the row conflicts, delete locally any that exist only on the client.
            //  - If the row doesn't conflict, ignore any that exist only on the client.
            //  - Update any that exist in both places.

            Map<Integer, Long> primaryLocal = new HashMap<Integer, Long>();
            Map<Integer, Long> primaryDiffs = new HashMap<Integer, Long>();

            Cursor cRemote;
            Cursor cLocal;

            // Phones
            cRemote = null;
            cLocal = null;
            final SQLiteDatabase diffsDb = ((ContactsProvider) diffs).getDatabase();
            try {
                cLocal = doSubQuery(db, sPhonesTable, null, localPersonID, sPhonesKeyOrderBy);
                cRemote = doSubQuery(diffsDb, sPhonesTable,
                        null, diffsPersonID, sPhonesKeyOrderBy);

                final int idColLocal = cLocal.getColumnIndexOrThrow(Phones._ID);
                final int isPrimaryColLocal = cLocal.getColumnIndexOrThrow(Phones.ISPRIMARY);
                final int isPrimaryColRemote = cRemote.getColumnIndexOrThrow(Phones.ISPRIMARY);

                CursorJoiner joiner =
                        new CursorJoiner(cLocal, sPhonesKeyColumns, cRemote, sPhonesKeyColumns);
                for (CursorJoiner.Result joinResult : joiner) {
                    switch(joinResult) {
                        case LEFT:
                            if (!conflicts) {
                                db.delete(sPhonesTable,
                                        Phones._ID + "=" + cLocal.getLong(idColLocal), null);
                            } else {
                                if (cLocal.getLong(isPrimaryColLocal) != 0) {
                                    savePrimaryId(primaryLocal, Contacts.KIND_PHONE,
                                            cLocal.getLong(idColLocal));
                                }
                            }
                            break;

                        case RIGHT:
                        case BOTH:
                            mValues.clear();
                            DatabaseUtils.cursorIntToContentValues(
                                    cRemote, Phones.TYPE, mValues);
                            DatabaseUtils.cursorStringToContentValues(
                                    cRemote, Phones.LABEL, mValues);
                            DatabaseUtils.cursorStringToContentValues(
                                    cRemote, Phones.NUMBER, mValues);
                            DatabaseUtils.cursorStringToContentValues(
                                    cRemote, Phones.NUMBER_KEY, mValues);
                            DatabaseUtils.cursorIntToContentValues(
                                    cRemote, Phones.ISPRIMARY, mValues);

                            long localId;
                            if (joinResult == CursorJoiner.Result.RIGHT) {
                                mValues.put(Phones.PERSON_ID, localPersonID);
                                localId = mPhonesInserter.insert(mValues);
                            } else {
                                localId = cLocal.getLong(idColLocal);
                                db.update(sPhonesTable, mValues, "_id =" + localId, null);
                            }
                            if (cRemote.getLong(isPrimaryColRemote) != 0) {
                                savePrimaryId(primaryDiffs, Contacts.KIND_PHONE, localId);
                            }
                            break;
                    }
                }
            } finally {
                if (cRemote != null) cRemote.deactivate();
                if (cLocal != null) cLocal.deactivate();
            }

            // Contact methods
            cRemote = null;
            cLocal = null;
            try {
                cLocal = doSubQuery(db,
                        sContactMethodsTable, null, localPersonID, sContactMethodsKeyOrderBy);
                cRemote = doSubQuery(diffsDb,
                        sContactMethodsTable, null, diffsPersonID, sContactMethodsKeyOrderBy);

                final int idColLocal = cLocal.getColumnIndexOrThrow(ContactMethods._ID);
                final int kindColLocal = cLocal.getColumnIndexOrThrow(ContactMethods.KIND);
                final int kindColRemote = cRemote.getColumnIndexOrThrow(ContactMethods.KIND);
                final int isPrimaryColLocal =
                        cLocal.getColumnIndexOrThrow(ContactMethods.ISPRIMARY);
                final int isPrimaryColRemote =
                        cRemote.getColumnIndexOrThrow(ContactMethods.ISPRIMARY);

                CursorJoiner joiner = new CursorJoiner(
                        cLocal, sContactMethodsKeyColumns, cRemote, sContactMethodsKeyColumns);
                for (CursorJoiner.Result joinResult : joiner) {
                    switch(joinResult) {
                        case LEFT:
                            if (!conflicts) {
                                db.delete(sContactMethodsTable, ContactMethods._ID + "="
                                        + cLocal.getLong(idColLocal), null);
                            } else {
                                if (cLocal.getLong(isPrimaryColLocal) != 0) {
                                    savePrimaryId(primaryLocal, cLocal.getInt(kindColLocal),
                                            cLocal.getLong(idColLocal));
                                }
                            }
                            break;

                        case RIGHT:
                        case BOTH:
                            mValues.clear();
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    ContactMethods.LABEL, mValues);
                            DatabaseUtils.cursorIntToContentValues(cRemote,
                                    ContactMethods.TYPE, mValues);
                            DatabaseUtils.cursorIntToContentValues(cRemote,
                                    ContactMethods.KIND, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    ContactMethods.DATA, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    ContactMethods.AUX_DATA, mValues);
                            DatabaseUtils.cursorIntToContentValues(cRemote,
                                    ContactMethods.ISPRIMARY, mValues);

                            long localId;
                            if (joinResult == CursorJoiner.Result.RIGHT) {
                                mValues.put(ContactMethods.PERSON_ID, localPersonID);
                                localId = mContactMethodsInserter.insert(mValues);
                            } else {
                                localId = cLocal.getLong(idColLocal);
                                db.update(sContactMethodsTable, mValues, "_id =" + localId, null);
                            }
                            if (cRemote.getLong(isPrimaryColRemote) != 0) {
                                savePrimaryId(primaryDiffs, cRemote.getInt(kindColRemote), localId);
                            }
                            break;
                    }
                }
            } finally {
                if (cRemote != null) cRemote.deactivate();
                if (cLocal != null) cLocal.deactivate();
            }

            // Organizations
            cRemote = null;
            cLocal = null;
            try {
                cLocal = doSubQuery(db,
                        sOrganizationsTable, null, localPersonID, sOrganizationsKeyOrderBy);
                cRemote = doSubQuery(diffsDb,
                        sOrganizationsTable, null, diffsPersonID, sOrganizationsKeyOrderBy);

                final int idColLocal = cLocal.getColumnIndexOrThrow(Organizations._ID);
                final int isPrimaryColLocal =
                        cLocal.getColumnIndexOrThrow(ContactMethods.ISPRIMARY);
                final int isPrimaryColRemote =
                        cRemote.getColumnIndexOrThrow(ContactMethods.ISPRIMARY);
                CursorJoiner joiner = new CursorJoiner(
                        cLocal, sOrganizationsKeyColumns, cRemote, sOrganizationsKeyColumns);
                for (CursorJoiner.Result joinResult : joiner) {
                    switch(joinResult) {
                        case LEFT:
                            if (!conflicts) {
                                db.delete(sOrganizationsTable,
                                        Phones._ID + "=" + cLocal.getLong(idColLocal), null);
                            } else {
                                if (cLocal.getLong(isPrimaryColLocal) != 0) {
                                    savePrimaryId(primaryLocal, Contacts.KIND_ORGANIZATION,
                                            cLocal.getLong(idColLocal));
                                }
                            }
                            break;

                        case RIGHT:
                        case BOTH:
                            mValues.clear();
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    Organizations.LABEL, mValues);
                            DatabaseUtils.cursorIntToContentValues(cRemote,
                                    Organizations.TYPE, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    Organizations.COMPANY, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    Organizations.TITLE, mValues);
                            DatabaseUtils.cursorIntToContentValues(cRemote,
                                    Organizations.ISPRIMARY, mValues);
                            long localId;
                            if (joinResult == CursorJoiner.Result.RIGHT) {
                                mValues.put(Organizations.PERSON_ID, localPersonID);
                                localId = mOrganizationsInserter.insert(mValues);
                            } else {
                                localId = cLocal.getLong(idColLocal);
                                db.update(sOrganizationsTable, mValues,
                                        "_id =" + localId, null /* whereArgs */);
                            }
                            if (cRemote.getLong(isPrimaryColRemote) != 0) {
                                savePrimaryId(primaryDiffs, Contacts.KIND_ORGANIZATION, localId);
                            }
                            break;
                    }
                }
            } finally {
                if (cRemote != null) cRemote.deactivate();
                if (cLocal != null) cLocal.deactivate();
            }

            // Groupmembership
            cRemote = null;
            cLocal = null;
            try {
                cLocal = doSubQuery(db,
                        sGroupmembershipTable, null, localPersonID, sGroupmembershipKeyOrderBy);
                cRemote = doSubQuery(diffsDb,
                        sGroupmembershipTable, null, diffsPersonID, sGroupmembershipKeyOrderBy);

                final int idColLocal = cLocal.getColumnIndexOrThrow(GroupMembership._ID);
                CursorJoiner joiner = new CursorJoiner(
                        cLocal, sGroupmembershipKeyColumns, cRemote, sGroupmembershipKeyColumns);
                for (CursorJoiner.Result joinResult : joiner) {
                    switch(joinResult) {
                        case LEFT:
                            if (!conflicts) {
                                db.delete(sGroupmembershipTable,
                                        Phones._ID + "=" + cLocal.getLong(idColLocal), null);
                            }
                            break;

                        case RIGHT:
                        case BOTH:
                            mValues.clear();
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    GroupMembership.GROUP_SYNC_ACCOUNT, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    GroupMembership.GROUP_SYNC_ID, mValues);
                            if (joinResult == CursorJoiner.Result.RIGHT) {
                                mValues.put(GroupMembership.PERSON_ID, localPersonID);
                                mGroupMembershipInserter.insert(mValues);
                            } else {
                                db.update(sGroupmembershipTable, mValues,
                                        "_id =" + cLocal.getLong(idColLocal), null /* whereArgs */);
                            }
                            break;
                    }
                }
            } finally {
                if (cRemote != null) cRemote.deactivate();
                if (cLocal != null) cLocal.deactivate();
            }

            // Extensions
            cRemote = null;
            cLocal = null;
            try {
                cLocal = doSubQuery(db,
                        sExtensionsTable, null, localPersonID, Extensions.NAME);
                cRemote = doSubQuery(diffsDb,
                        sExtensionsTable, null, diffsPersonID, Extensions.NAME);

                final int idColLocal = cLocal.getColumnIndexOrThrow(Extensions._ID);
                CursorJoiner joiner = new CursorJoiner(
                        cLocal, sExtensionsKeyColumns, cRemote, sExtensionsKeyColumns);
                for (CursorJoiner.Result joinResult : joiner) {
                    switch(joinResult) {
                        case LEFT:
                            if (!conflicts) {
                                db.delete(sExtensionsTable,
                                        Phones._ID + "=" + cLocal.getLong(idColLocal), null);
                            }
                            break;

                        case RIGHT:
                        case BOTH:
                            mValues.clear();
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    Extensions.NAME, mValues);
                            DatabaseUtils.cursorStringToContentValues(cRemote,
                                    Extensions.VALUE, mValues);
                            if (joinResult == CursorJoiner.Result.RIGHT) {
                                mValues.put(Extensions.PERSON_ID, localPersonID);
                                mExtensionsInserter.insert(mValues);
                            } else {
                                db.update(sExtensionsTable, mValues,
                                        "_id =" + cLocal.getLong(idColLocal), null /* whereArgs */);
                            }
                            break;
                    }
                }
            } finally {
                if (cRemote != null) cRemote.deactivate();
                if (cLocal != null) cLocal.deactivate();
            }

            // Copy the Photo's server id and account so that the merger will find it
            cRemote = doSubQuery(diffsDb, sPhotosTable, null, diffsPersonID, null);
            try {
                if(cRemote.moveToNext()) {
                    mValues.clear();
                    DatabaseUtils.cursorStringToContentValues(cRemote, Photos._SYNC_ID, mValues);
                    DatabaseUtils.cursorStringToContentValues(cRemote, Photos._SYNC_ACCOUNT, mValues);
                    db.update(sPhotosTable, mValues, Photos.PERSON_ID + '=' + localPersonID, null);
                }
            } finally {
                cRemote.deactivate();
            }

            // make sure there is exactly one primary set for each of these types
            Long primaryPhoneId = setSinglePrimary(
                    primaryDiffs, primaryLocal, localPersonID, Contacts.KIND_PHONE);

            Long primaryEmailId = setSinglePrimary(
                    primaryDiffs, primaryLocal, localPersonID, Contacts.KIND_EMAIL);

            Long primaryOrganizationId = setSinglePrimary(
                    primaryDiffs, primaryLocal, localPersonID, Contacts.KIND_ORGANIZATION);

            setSinglePrimary(primaryDiffs, primaryLocal, localPersonID, Contacts.KIND_IM);

            setSinglePrimary(primaryDiffs, primaryLocal, localPersonID, Contacts.KIND_POSTAL);

            // Update the person
            mValues.clear();
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People._SYNC_ID, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People._SYNC_TIME, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People._SYNC_VERSION, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People._SYNC_ACCOUNT, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People.NAME, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People.PHONETIC_NAME, mValues);
            DatabaseUtils.cursorStringToContentValues(diffsCursor, People.NOTES, mValues);
            mValues.put(People.PRIMARY_PHONE_ID, primaryPhoneId);
            mValues.put(People.PRIMARY_EMAIL_ID, primaryEmailId);
            mValues.put(People.PRIMARY_ORGANIZATION_ID, primaryOrganizationId);
            final boolean isStarred = queryGroupMembershipContainsStarred(localPersonID);
            mValues.put(People.STARRED, isStarred ? 1 : 0);
            mValues.put(People._SYNC_DIRTY, conflicts ? 1 : 0);
            db.update(mTable, mValues, People._ID + '=' + localPersonID, null);
        }

        private void savePrimaryId(Map<Integer, Long> primaryDiffs, Integer kind, long localId) {
            if (primaryDiffs.containsKey(kind)) {
                throw new IllegalArgumentException("more than one of kind "
                        + kind + " was marked as primary");
            }
            primaryDiffs.put(kind, localId);
        }

        private Long setSinglePrimary(
                Map<Integer, Long> diffsMap,
                Map<Integer, Long> localMap,
                long localPersonID, int kind) {
            Long primaryId = diffsMap.containsKey(kind) ? diffsMap.get(kind) : null;
            if (primaryId == null) {
                primaryId = localMap.containsKey(kind) ? localMap.get(kind) : null;
            }
            if (primaryId == null) {
                primaryId = findNewPrimary(kind, localPersonID, null);
            }
            clearOtherIsPrimary(kind, localPersonID, primaryId);
            return primaryId;
        }

        /**
         * Returns a cursor on the specified table that selects rows where
         * the "person" column is equal to the personId parameter. The cursor
         * is also saved and may be returned in future calls where db and table
         * parameter are the same. In that case the projection and orderBy parameters
         * are ignored, so one must take care to not change those parameters across
         * multiple calls to the same db/table.
         * <p>
         * Since the cursor may be saced by this call, the caller must be sure to not
         * close the cursor, though they still must deactivate it when they are done
         * with it.
         */
        private Cursor doSubQuery(SQLiteDatabase db, String table, String[] projection,
                long personId, String orderBy) {
            final String[] selectArgs = new String[]{Long.toString(personId)};
            final String key = (db == getDatabase() ? "local_" : "remote_") + table;
            SQLiteCursor cursor = mCursorMap.get(key);

            // don't use the cached cursor if it is from a different DB
            if (cursor != null && cursor.getDatabase() != db) {
                cursor.close();
                cursor = null;
            }

            // If we can't find a cached cursor then create a new one and add it to the cache.
            // Otherwise just change the selection arguments and requery it.
            if (cursor == null) {
                cursor = (SQLiteCursor)db.query(table, projection, "person=?", selectArgs,
                        null, null, orderBy);
                mCursorMap.put(key, cursor);
            } else {
                cursor.setSelectionArguments(selectArgs);
                cursor.requery();
            }
            return cursor;
        }
    }

    protected class GroupMerger extends AbstractTableMerger {
        private ContentValues mValues = new ContentValues();

        private static final String UNSYNCED_GROUP_BY_NAME_WHERE_CLAUSE =
                Groups._SYNC_ID + " is null AND "
                        + Groups._SYNC_ACCOUNT + " is null AND "
                        + Groups.NAME + "=?";

        private static final String UNSYNCED_GROUP_BY_SYSTEM_ID_WHERE_CLAUSE =
                Groups._SYNC_ID + " is null AND "
                        + Groups._SYNC_ACCOUNT + " is null AND "
                        + Groups.SYSTEM_ID + "=?";

        public GroupMerger()
        {
            super(getDatabase(), sGroupsTable, sGroupsURL, sDeletedGroupsTable, sDeletedGroupsURL);
        }

        @Override
        protected void notifyChanges() {
            // notify that a change has occurred.
            getContext().getContentResolver().notifyChange(Contacts.CONTENT_URI,
                    null /* observer */, false /* do not sync to network */);
        }

        @Override
        public void insertRow(ContentProvider diffs, Cursor cursor) {
            // if an unsynced group with this name already exists then update it, otherwise
            // insert a new group
            mValues.clear();
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_ID, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_TIME, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_VERSION, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_ACCOUNT, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.NAME, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.NOTES, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.SYSTEM_ID, mValues);
            mValues.put(Groups._SYNC_DIRTY, 0);

            final String systemId = mValues.getAsString(Groups.SYSTEM_ID);
            boolean rowUpdated = false;
            if (TextUtils.isEmpty(systemId)) {
                rowUpdated = getDatabase().update(mTable, mValues,
                    UNSYNCED_GROUP_BY_NAME_WHERE_CLAUSE,
                    new String[]{mValues.getAsString(Groups.NAME)}) > 0;
            } else {
                rowUpdated = getDatabase().update(mTable, mValues,
                    UNSYNCED_GROUP_BY_SYSTEM_ID_WHERE_CLAUSE,
                    new String[]{systemId}) > 0;
            }
            if (!rowUpdated) {
                mGroupsInserter.insert(mValues);
            } else {
                // We may have just synced the metadata for a groups we previously marked for
                // syncing.
                final ContentResolver cr = getContext().getContentResolver();
                final String account = mValues.getAsString(Groups._SYNC_ACCOUNT);
                onLocalChangesForAccount(cr, account, false);
            }

            String oldName = null;
            String newName = cursor.getString(cursor.getColumnIndexOrThrow(Groups.NAME));
            String account = cursor.getString(cursor.getColumnIndexOrThrow(Groups._SYNC_ACCOUNT));
            String syncId = cursor.getString(cursor.getColumnIndexOrThrow(Groups._SYNC_ID));
            // this must come after the insert, otherwise the join won't work
            fixupPeopleStarredOnGroupRename(oldName, newName, account, syncId);
        }

        @Override
        public void updateRow(long localId, ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localId, null, diffs, diffsCursor, false);
        }

        @Override
        public void resolveRow(long localId, String syncID,
                ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localId, syncID, diffs, diffsCursor, true);
        }

        protected void updateOrResolveRow(long localRowId, String syncID,
                ContentProvider diffs, Cursor cursor, boolean conflicts) {
            final SQLiteDatabase db = getDatabase();

            String oldName = DatabaseUtils.stringForQuery(db,
                    "select name from groups where _id=" + localRowId, null);
            String newName = cursor.getString(cursor.getColumnIndexOrThrow(Groups.NAME));
            String account = cursor.getString(cursor.getColumnIndexOrThrow(Groups._SYNC_ACCOUNT));
            String syncId = cursor.getString(cursor.getColumnIndexOrThrow(Groups._SYNC_ID));
            // this can come before or after the delete
            fixupPeopleStarredOnGroupRename(oldName, newName, account, syncId);

            mValues.clear();
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_ID, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_TIME, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_VERSION, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups._SYNC_ACCOUNT, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.NAME, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.NOTES, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Groups.SYSTEM_ID, mValues);
            mValues.put(Groups._SYNC_DIRTY, 0);
            db.update(mTable, mValues, Groups._ID + '=' + localRowId, null);
        }

        @Override
        public void deleteRow(Cursor cursor) {
            // we have to read this row from the DB since the projection that is used
            // by cursor doesn't necessarily contain the columns we need
            Cursor c = getDatabase().query(sGroupsTable, null,
                    "_id=" + cursor.getLong(cursor.getColumnIndexOrThrow(Groups._ID)),
                    null, null, null, null);
            try {
                c.moveToNext();
                String oldName = c.getString(c.getColumnIndexOrThrow(Groups.NAME));
                String newName = null;
                String account = c.getString(c.getColumnIndexOrThrow(Groups._SYNC_ACCOUNT));
                String syncId = c.getString(c.getColumnIndexOrThrow(Groups._SYNC_ID));
                String systemId = c.getString(c.getColumnIndexOrThrow(Groups.SYSTEM_ID));
                if (!TextUtils.isEmpty(systemId)) {
                    // We don't support deleting of system groups, but due to a server bug they
                    // occasionally get sent. Ignore the delete.
                    Log.w(TAG, "ignoring a delete for a system group: " +
                            DatabaseUtils.dumpCurrentRowToString(c));
                    cursor.moveToNext();
                    return;
                }

                // this must come before the delete, since the join won't work once this row is gone
                fixupPeopleStarredOnGroupRename(oldName, newName, account, syncId);
            } finally {
                c.close();
            }

            cursor.deleteRow();
        }
    }

    protected class PhotoMerger extends AbstractTableMerger {
        private ContentValues mValues = new ContentValues();

        public PhotoMerger() {
            super(getDatabase(), sPhotosTable, sPhotosURL, null, null);
        }

        @Override
        protected void notifyChanges() {
            // notify that a change has occurred.
            getContext().getContentResolver().notifyChange(Contacts.CONTENT_URI,
                    null /* observer */, false /* do not sync to network */);
        }

        @Override
        public void insertRow(ContentProvider diffs, Cursor cursor) {
            // This photo may correspond to a contact that is in the delete table. If so then
            // ignore this insert.
            String syncId = cursor.getString(cursor.getColumnIndexOrThrow(Photos._SYNC_ID));
            boolean contactIsDeleted = DatabaseUtils.longForQuery(getDatabase(),
                    "select count(*) from _deleted_people where _sync_id=?",
                    new String[]{syncId}) > 0;
            if (contactIsDeleted) {
                return;
            }

            throw new UnsupportedOperationException(
                    "the photo row is inserted by PersonMerger.insertRow");
        }

        @Override
        public void updateRow(long localId, ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localId, null, diffs, diffsCursor, false);
        }

        @Override
        public void resolveRow(long localId, String syncID,
                ContentProvider diffs, Cursor diffsCursor) {
            updateOrResolveRow(localId, syncID, diffs, diffsCursor, true);
        }

        protected void updateOrResolveRow(long localRowId, String syncID,
                ContentProvider diffs, Cursor cursor, boolean conflicts) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "PhotoMerger.updateOrResolveRow: localRowId " + localRowId
                        + ", syncId " + syncID + ", conflicts " + conflicts
                        + ", server row " + DatabaseUtils.dumpCurrentRowToString(cursor));
            }
            mValues.clear();
            DatabaseUtils.cursorStringToContentValues(cursor, Photos._SYNC_TIME, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Photos._SYNC_VERSION, mValues);
            DatabaseUtils.cursorStringToContentValues(cursor, Photos.EXISTS_ON_SERVER, mValues);
            // reset the error field to allow the phone to attempt to redownload the photo.
            mValues.put(Photos.SYNC_ERROR, (String)null);

            // If the photo didn't change locally and the server doesn't have a photo for this
            // contact then delete the local photo.
            long syncDirty = DatabaseUtils.longForQuery(getDatabase(),
                    "SELECT _sync_dirty FROM photos WHERE _id=" + localRowId
                            + " UNION SELECT 0 AS _sync_dirty ORDER BY _sync_dirty DESC LIMIT 1",
                    null);
            if (syncDirty == 0) {
                if (mValues.getAsInteger(Photos.EXISTS_ON_SERVER) == 0) {
                    mValues.put(Photos.DATA, (String)null);
                    mValues.put(Photos.LOCAL_VERSION, mValues.getAsString(Photos.LOCAL_VERSION));
                }
                // if it does exist on the server then we will attempt to download it later
            }
            // if it does conflict then we will send the client version of the photo to
            // the server later. That will trigger a new sync of the photo data which will
            // cause this method to be called again, at which time the row will no longer
            // conflict. We will then download the photo we just sent to the server and
            // set the LOCAL_VERSION to match the data we just downloaded.

            getDatabase().update(mTable, mValues, Photos._ID + '=' + localRowId, null);
        }

        @Override
        public void deleteRow(Cursor cursor) {
            // this row is never deleted explicitly, instead it is deleted by a trigger on
            // the people table
            cursor.moveToNext();
        }
    }

    private static final String TAG = "ContactsProvider";

    /* package private */ static final String DATABASE_NAME = "contacts.db";
    /* package private */ static final int DATABASE_VERSION = 80;

    protected static final String CONTACTS_AUTHORITY = "contacts";
    protected static final String CALL_LOG_AUTHORITY = "call_log";

    private static final int PEOPLE_BASE = 0;
    private static final int PEOPLE = PEOPLE_BASE;
    private static final int PEOPLE_FILTER = PEOPLE_BASE + 1;
    private static final int PEOPLE_ID = PEOPLE_BASE + 2;
    private static final int PEOPLE_PHONES = PEOPLE_BASE + 3;
    private static final int PEOPLE_PHONES_ID = PEOPLE_BASE + 4;
    private static final int PEOPLE_CONTACTMETHODS = PEOPLE_BASE + 5;
    private static final int PEOPLE_CONTACTMETHODS_ID = PEOPLE_BASE + 6;
    private static final int PEOPLE_RAW = PEOPLE_BASE + 7;
    private static final int PEOPLE_WITH_PHONES_FILTER = PEOPLE_BASE + 8;
    private static final int PEOPLE_STREQUENT = PEOPLE_BASE + 9;
    private static final int PEOPLE_STREQUENT_FILTER = PEOPLE_BASE + 10;
    private static final int PEOPLE_ORGANIZATIONS = PEOPLE_BASE + 11;
    private static final int PEOPLE_ORGANIZATIONS_ID = PEOPLE_BASE + 12;
    private static final int PEOPLE_GROUPMEMBERSHIP = PEOPLE_BASE + 13;
    private static final int PEOPLE_GROUPMEMBERSHIP_ID = PEOPLE_BASE + 14;
    private static final int PEOPLE_PHOTO = PEOPLE_BASE + 15;
    private static final int PEOPLE_EXTENSIONS = PEOPLE_BASE + 16;
    private static final int PEOPLE_EXTENSIONS_ID = PEOPLE_BASE + 17;
    private static final int PEOPLE_CONTACTMETHODS_WITH_PRESENCE = PEOPLE_BASE + 18;
    private static final int PEOPLE_OWNER = PEOPLE_BASE + 19;
    private static final int PEOPLE_UPDATE_CONTACT_TIME = PEOPLE_BASE + 20;
    private static final int PEOPLE_PHONES_WITH_PRESENCE = PEOPLE_BASE + 21;
    private static final int PEOPLE_WITH_EMAIL_OR_IM_FILTER = PEOPLE_BASE + 22;

    private static final int DELETED_BASE = 1000;
    private static final int DELETED_PEOPLE = DELETED_BASE;
    private static final int DELETED_GROUPS = DELETED_BASE + 1;

    private static final int PHONES_BASE = 2000;
    private static final int PHONES = PHONES_BASE;
    private static final int PHONES_ID = PHONES_BASE + 1;
    private static final int PHONES_FILTER = PHONES_BASE + 2;
    private static final int PHONES_FILTER_NAME = PHONES_BASE + 3;
    private static final int PHONES_MOBILE_FILTER_NAME = PHONES_BASE + 4;
    private static final int PHONES_WITH_PRESENCE = PHONES_BASE + 5;

    private static final int CONTACTMETHODS_BASE = 3000;
    private static final int CONTACTMETHODS = CONTACTMETHODS_BASE;
    private static final int CONTACTMETHODS_ID = CONTACTMETHODS_BASE + 1;
    private static final int CONTACTMETHODS_EMAIL = CONTACTMETHODS_BASE + 2;
    private static final int CONTACTMETHODS_EMAIL_FILTER = CONTACTMETHODS_BASE + 3;
    private static final int CONTACTMETHODS_WITH_PRESENCE = CONTACTMETHODS_BASE + 4;

    private static final int CALLS_BASE = 4000;
    private static final int CALLS = CALLS_BASE;
    private static final int CALLS_ID = CALLS_BASE + 1;
    private static final int CALLS_FILTER = CALLS_BASE + 2;

    private static final int PRESENCE_BASE = 5000;
    private static final int PRESENCE = PRESENCE_BASE;
    private static final int PRESENCE_ID = PRESENCE_BASE + 1;

    private static final int ORGANIZATIONS_BASE = 6000;
    private static final int ORGANIZATIONS = ORGANIZATIONS_BASE;
    private static final int ORGANIZATIONS_ID = ORGANIZATIONS_BASE + 1;

    private static final int VOICE_DIALER_TIMESTAMP = 7000;
    private static final int SEARCH_SUGGESTIONS = 7001;
    private static final int SEARCH_SHORTCUT = 7002;

    private static final int GROUPS_BASE = 8000;
    private static final int GROUPS = GROUPS_BASE;
    private static final int GROUPS_ID = GROUPS_BASE + 2;
    private static final int GROUP_NAME_MEMBERS = GROUPS_BASE + 3;
    private static final int GROUP_NAME_MEMBERS_FILTER = GROUPS_BASE + 4;
    private static final int GROUP_SYSTEM_ID_MEMBERS = GROUPS_BASE + 5;
    private static final int GROUP_SYSTEM_ID_MEMBERS_FILTER = GROUPS_BASE + 6;

    private static final int GROUPMEMBERSHIP_BASE = 9000;
    private static final int GROUPMEMBERSHIP = GROUPMEMBERSHIP_BASE;
    private static final int GROUPMEMBERSHIP_ID = GROUPMEMBERSHIP_BASE + 2;
    private static final int GROUPMEMBERSHIP_RAW = GROUPMEMBERSHIP_BASE + 3;

    private static final int PHOTOS_BASE = 10000;
    private static final int PHOTOS = PHOTOS_BASE;
    private static final int PHOTOS_ID = PHOTOS_BASE + 1;

    private static final int EXTENSIONS_BASE = 11000;
    private static final int EXTENSIONS = EXTENSIONS_BASE;
    private static final int EXTENSIONS_ID = EXTENSIONS_BASE + 2;

    private static final int SETTINGS = 12000;
    
    private static final int LIVE_FOLDERS_BASE = 13000;
    private static final int LIVE_FOLDERS_PEOPLE = LIVE_FOLDERS_BASE + 1;
    private static final int LIVE_FOLDERS_PEOPLE_GROUP_NAME = LIVE_FOLDERS_BASE + 2;
    private static final int LIVE_FOLDERS_PEOPLE_WITH_PHONES = LIVE_FOLDERS_BASE + 3;
    private static final int LIVE_FOLDERS_PEOPLE_FAVORITES = LIVE_FOLDERS_BASE + 4;

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final HashMap<String, String> sGroupsProjectionMap;
    private static final HashMap<String, String> sPeopleProjectionMap;
    private static final HashMap<String, String> sPeopleWithPhotoProjectionMap;
    private static final HashMap<String, String> sPeopleWithEmailOrImProjectionMap;
    /** Used to force items to the top of a times_contacted list */
    private static final HashMap<String, String> sStrequentStarredProjectionMap;
    private static final HashMap<String, String> sCallsProjectionMap;
    private static final HashMap<String, String> sPhonesProjectionMap;
    private static final HashMap<String, String> sPhonesWithPresenceProjectionMap;
    private static final HashMap<String, String> sContactMethodsProjectionMap;
    private static final HashMap<String, String> sContactMethodsWithPresenceProjectionMap;
    private static final HashMap<String, String> sPresenceProjectionMap;
    private static final HashMap<String, String> sEmailSearchProjectionMap;
    private static final HashMap<String, String> sOrganizationsProjectionMap;
    private static final HashMap<String, String> sSearchSuggestionsProjectionMap;
    private static final HashMap<String, String> sGroupMembershipProjectionMap;
    private static final HashMap<String, String> sPhotosProjectionMap;
    private static final HashMap<String, String> sExtensionsProjectionMap;
    private static final HashMap<String, String> sLiveFoldersProjectionMap;

    private static final String sPhonesKeyOrderBy;
    private static final String sContactMethodsKeyOrderBy;
    private static final String sOrganizationsKeyOrderBy;
    private static final String sGroupmembershipKeyOrderBy;

    private static final String DISPLAY_NAME_SQL
            = "(CASE WHEN (name IS NOT NULL AND name != '') "
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
    
    private static final String PHONETICALLY_SORTABLE_STRING_SQL =
        "GET_PHONETICALLY_SORTABLE_STRING("
            + "CASE WHEN (phonetic_name IS NOT NULL AND phonetic_name != '') "
                + "THEN phonetic_name "
            + "ELSE "
                + "(CASE WHEN (name is NOT NULL AND name != '')"
                    + "THEN name "
                + "ELSE "
                    + "(CASE WHEN primary_email IS NOT NULL THEN "
                        + "(SELECT data FROM contact_methods WHERE "
                            + "contact_methods._id = primary_email) "
                    + "ELSE "
                        + "(CASE WHEN primary_phone IS NOT NULL THEN "
                            + "(SELECT number FROM phones WHERE phones._id = primary_phone) "
                        + "ELSE "
                            + "null "
                        + "END) "
                    + "END) "
                + "END) "
            + "END"
        + ")";
    
    private static final String PRIMARY_ORGANIZATION_WHEN_SQL
            = " WHEN primary_organization is NOT NULL THEN "
            + "(SELECT company FROM organizations WHERE organizations._id = primary_organization)";

    private static final String PRIMARY_PHONE_WHEN_SQL
            = " WHEN primary_phone IS NOT NULL THEN "
            + "(SELECT number FROM phones WHERE phones._id = primary_phone)";

    private static final String PRIMARY_EMAIL_WHEN_SQL
            = " WHEN primary_email IS NOT NULL THEN "
            + "(SELECT data FROM contact_methods WHERE contact_methods._id = primary_email)";

    // The outer CASE is for figuring out what info DISPLAY_NAME_SQL returned.
    // We then pick the next piece of info, to avoid the two lines in the search 
    // suggestion being identical.
    private static final String SUGGEST_DESCRIPTION_SQL
            = "(CASE"
                // DISPLAY_NAME_SQL returns name, try org, phone, email
                + " WHEN (name IS NOT NULL AND name != '') THEN "
                    + "(CASE"
                        + PRIMARY_ORGANIZATION_WHEN_SQL
                        + PRIMARY_PHONE_WHEN_SQL
                        + PRIMARY_EMAIL_WHEN_SQL
                        + " ELSE null END)"
                // DISPLAY_NAME_SQL returns org, try phone, email
                + " WHEN primary_organization is NOT NULL THEN "
                    + "(CASE"
                        + PRIMARY_PHONE_WHEN_SQL
                        + PRIMARY_EMAIL_WHEN_SQL
                        + " ELSE null END)"
                // DISPLAY_NAME_SQL returns phone, try email
                + " WHEN primary_phone IS NOT NULL THEN "
                    + "(CASE"
                        + PRIMARY_EMAIL_WHEN_SQL
                        + " ELSE null END)"
                // DISPLAY_NAME_SQL returns email or NULL, return NULL
                + " ELSE null END)";

    private static final String PRESENCE_ICON_SQL
            = "(CASE"
                + buildPresenceStatusWhen(People.OFFLINE)
                + buildPresenceStatusWhen(People.INVISIBLE)
                + buildPresenceStatusWhen(People.AWAY)
                + buildPresenceStatusWhen(People.IDLE)
                + buildPresenceStatusWhen(People.DO_NOT_DISTURB)
                + buildPresenceStatusWhen(People.AVAILABLE)
                + " ELSE null END)";

    private static String buildPresenceStatusWhen(int status) {
        return " WHEN " + Presence.PRESENCE_STATUS + " = " + status
            + " THEN " + Presence.getPresenceIconResourceId(status);
    }

    private static final String[] sPhonesKeyColumns;
    private static final String[] sContactMethodsKeyColumns;
    private static final String[] sOrganizationsKeyColumns;
    private static final String[] sGroupmembershipKeyColumns;
    private static final String[] sExtensionsKeyColumns;

    static private String buildOrderBy(String table, String... columns) {
        StringBuilder sb = null;
        for (String column : columns) {
            if (sb == null) {
                sb = new StringBuilder();
            } else {
                sb.append(", ");
            }
            sb.append(table);
            sb.append('.');
            sb.append(column);
        }
        return (sb == null) ? "" : sb.toString();
    }

    static {
        // Contacts URI matching table
        UriMatcher matcher = sURIMatcher;
        matcher.addURI(CONTACTS_AUTHORITY, "extensions", EXTENSIONS);
        matcher.addURI(CONTACTS_AUTHORITY, "extensions/#", EXTENSIONS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "groups", GROUPS);
        matcher.addURI(CONTACTS_AUTHORITY, "groups/#", GROUPS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "groups/name/*/members", GROUP_NAME_MEMBERS);
        matcher.addURI(CONTACTS_AUTHORITY, "groups/name/*/members/filter/*",
                GROUP_NAME_MEMBERS_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "groups/system_id/*/members", GROUP_SYSTEM_ID_MEMBERS);
        matcher.addURI(CONTACTS_AUTHORITY, "groups/system_id/*/members/filter/*",
                GROUP_SYSTEM_ID_MEMBERS_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "groupmembership", GROUPMEMBERSHIP);
        matcher.addURI(CONTACTS_AUTHORITY, "groupmembership/#", GROUPMEMBERSHIP_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "groupmembershipraw", GROUPMEMBERSHIP_RAW);
        matcher.addURI(CONTACTS_AUTHORITY, "people", PEOPLE);
        matcher.addURI(CONTACTS_AUTHORITY, "people/strequent", PEOPLE_STREQUENT);
        matcher.addURI(CONTACTS_AUTHORITY, "people/strequent/filter/*", PEOPLE_STREQUENT_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "people/filter/*", PEOPLE_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "people/with_phones_filter/*",
                PEOPLE_WITH_PHONES_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "people/with_email_or_im_filter/*",
                PEOPLE_WITH_EMAIL_OR_IM_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#", PEOPLE_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/extensions", PEOPLE_EXTENSIONS);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/extensions/#", PEOPLE_EXTENSIONS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/phones", PEOPLE_PHONES);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/phones_with_presence",
                PEOPLE_PHONES_WITH_PRESENCE);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/photo", PEOPLE_PHOTO);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/phones/#", PEOPLE_PHONES_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/contact_methods", PEOPLE_CONTACTMETHODS);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/contact_methods_with_presence",
                PEOPLE_CONTACTMETHODS_WITH_PRESENCE);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/contact_methods/#", PEOPLE_CONTACTMETHODS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/organizations", PEOPLE_ORGANIZATIONS);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/organizations/#", PEOPLE_ORGANIZATIONS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/groupmembership", PEOPLE_GROUPMEMBERSHIP);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/groupmembership/#", PEOPLE_GROUPMEMBERSHIP_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "people/raw", PEOPLE_RAW);
        matcher.addURI(CONTACTS_AUTHORITY, "people/owner", PEOPLE_OWNER);
        matcher.addURI(CONTACTS_AUTHORITY, "people/#/update_contact_time",
                PEOPLE_UPDATE_CONTACT_TIME);
        matcher.addURI(CONTACTS_AUTHORITY, "deleted_people", DELETED_PEOPLE);
        matcher.addURI(CONTACTS_AUTHORITY, "deleted_groups", DELETED_GROUPS);
        matcher.addURI(CONTACTS_AUTHORITY, "phones", PHONES);
        matcher.addURI(CONTACTS_AUTHORITY, "phones_with_presence", PHONES_WITH_PRESENCE);
        matcher.addURI(CONTACTS_AUTHORITY, "phones/filter/*", PHONES_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "phones/filter_name/*", PHONES_FILTER_NAME);
        matcher.addURI(CONTACTS_AUTHORITY, "phones/mobile_filter_name/*",
                PHONES_MOBILE_FILTER_NAME);
        matcher.addURI(CONTACTS_AUTHORITY, "phones/#", PHONES_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "photos", PHOTOS);
        matcher.addURI(CONTACTS_AUTHORITY, "photos/#", PHOTOS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "contact_methods", CONTACTMETHODS);
        matcher.addURI(CONTACTS_AUTHORITY, "contact_methods/email", CONTACTMETHODS_EMAIL);
        matcher.addURI(CONTACTS_AUTHORITY, "contact_methods/email/*", CONTACTMETHODS_EMAIL_FILTER);
        matcher.addURI(CONTACTS_AUTHORITY, "contact_methods/#", CONTACTMETHODS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "contact_methods/with_presence",
                CONTACTMETHODS_WITH_PRESENCE);
        matcher.addURI(CONTACTS_AUTHORITY, "presence", PRESENCE);
        matcher.addURI(CONTACTS_AUTHORITY, "presence/#", PRESENCE_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "organizations", ORGANIZATIONS);
        matcher.addURI(CONTACTS_AUTHORITY, "organizations/#", ORGANIZATIONS_ID);
        matcher.addURI(CONTACTS_AUTHORITY, "voice_dialer_timestamp", VOICE_DIALER_TIMESTAMP);
        matcher.addURI(CONTACTS_AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY,
                SEARCH_SUGGESTIONS);
        matcher.addURI(CONTACTS_AUTHORITY, SearchManager.SUGGEST_URI_PATH_QUERY + "/*",
                SEARCH_SUGGESTIONS);
        matcher.addURI(CONTACTS_AUTHORITY, SearchManager.SUGGEST_URI_PATH_SHORTCUT + "/#",
                SEARCH_SHORTCUT);
        matcher.addURI(CONTACTS_AUTHORITY, "settings", SETTINGS);

        matcher.addURI(CONTACTS_AUTHORITY, "live_folders/people", LIVE_FOLDERS_PEOPLE);
        matcher.addURI(CONTACTS_AUTHORITY, "live_folders/people/*",
                LIVE_FOLDERS_PEOPLE_GROUP_NAME);
        matcher.addURI(CONTACTS_AUTHORITY, "live_folders/people_with_phones",
                LIVE_FOLDERS_PEOPLE_WITH_PHONES);
        matcher.addURI(CONTACTS_AUTHORITY, "live_folders/favorites",
                LIVE_FOLDERS_PEOPLE_FAVORITES);

        // Call log URI matching table
        matcher.addURI(CALL_LOG_AUTHORITY, "calls", CALLS);
        matcher.addURI(CALL_LOG_AUTHORITY, "calls/filter/*", CALLS_FILTER);
        matcher.addURI(CALL_LOG_AUTHORITY, "calls/#", CALLS_ID);

        HashMap<String, String> map;

        // Create the common people columns
        HashMap<String, String> peopleColumns = new HashMap<String, String>();
        peopleColumns.put(PeopleColumns.NAME, People.NAME);
        peopleColumns.put(PeopleColumns.NOTES, People.NOTES);
        peopleColumns.put(PeopleColumns.TIMES_CONTACTED, People.TIMES_CONTACTED);
        peopleColumns.put(PeopleColumns.LAST_TIME_CONTACTED, People.LAST_TIME_CONTACTED);
        peopleColumns.put(PeopleColumns.STARRED, People.STARRED);
        peopleColumns.put(PeopleColumns.CUSTOM_RINGTONE, People.CUSTOM_RINGTONE);
        peopleColumns.put(PeopleColumns.SEND_TO_VOICEMAIL, People.SEND_TO_VOICEMAIL);
        peopleColumns.put(PeopleColumns.PHONETIC_NAME, People.PHONETIC_NAME);
        peopleColumns.put(PeopleColumns.DISPLAY_NAME,
                DISPLAY_NAME_SQL + " AS " + People.DISPLAY_NAME);
        peopleColumns.put(PeopleColumns.SORT_STRING,
                PHONETICALLY_SORTABLE_STRING_SQL + " AS " + People.SORT_STRING);
        
        // Create the common groups columns
        HashMap<String, String> groupsColumns = new HashMap<String, String>();
        groupsColumns.put(GroupsColumns.NAME, Groups.NAME);
        groupsColumns.put(GroupsColumns.NOTES, Groups.NOTES);
        groupsColumns.put(GroupsColumns.SYSTEM_ID, Groups.SYSTEM_ID);
        groupsColumns.put(GroupsColumns.SHOULD_SYNC, Groups.SHOULD_SYNC);

        // Create the common presence columns
        HashMap<String, String> presenceColumns = new HashMap<String, String>();
        presenceColumns.put(PresenceColumns.IM_PROTOCOL, PresenceColumns.IM_PROTOCOL);
        presenceColumns.put(PresenceColumns.IM_HANDLE, PresenceColumns.IM_HANDLE);
        presenceColumns.put(PresenceColumns.IM_ACCOUNT, PresenceColumns.IM_ACCOUNT);
        presenceColumns.put(PresenceColumns.PRESENCE_STATUS, PresenceColumns.PRESENCE_STATUS);
        presenceColumns.put(PresenceColumns.PRESENCE_CUSTOM_STATUS,
                PresenceColumns.PRESENCE_CUSTOM_STATUS);

        // Create the common sync columns
        HashMap<String, String> syncColumns = new HashMap<String, String>();
        syncColumns.put(SyncConstValue._SYNC_ID, SyncConstValue._SYNC_ID);
        syncColumns.put(SyncConstValue._SYNC_TIME, SyncConstValue._SYNC_TIME);
        syncColumns.put(SyncConstValue._SYNC_VERSION, SyncConstValue._SYNC_VERSION);
        syncColumns.put(SyncConstValue._SYNC_LOCAL_ID, SyncConstValue._SYNC_LOCAL_ID);
        syncColumns.put(SyncConstValue._SYNC_DIRTY, SyncConstValue._SYNC_DIRTY);
        syncColumns.put(SyncConstValue._SYNC_ACCOUNT, SyncConstValue._SYNC_ACCOUNT);

        // Phones columns
        HashMap<String, String> phonesColumns = new HashMap<String, String>();
        phonesColumns.put(Phones.NUMBER, Phones.NUMBER);
        phonesColumns.put(Phones.NUMBER_KEY, Phones.NUMBER_KEY);
        phonesColumns.put(Phones.TYPE, Phones.TYPE);
        phonesColumns.put(Phones.LABEL, Phones.LABEL);

        // People projection map
        map = new HashMap<String, String>();
        map.put(People._ID, "people._id AS " + People._ID);
        peopleColumns.put(People.PRIMARY_PHONE_ID, People.PRIMARY_PHONE_ID);
        peopleColumns.put(People.PRIMARY_EMAIL_ID, People.PRIMARY_EMAIL_ID);
        peopleColumns.put(People.PRIMARY_ORGANIZATION_ID, People.PRIMARY_ORGANIZATION_ID);
        map.putAll(peopleColumns);
        map.putAll(phonesColumns);
        map.putAll(syncColumns);
        map.putAll(presenceColumns);
        sPeopleProjectionMap = map;

        // People with photo projection map
        map = new HashMap<String, String>(sPeopleProjectionMap);
        map.put("photo_data", "photos.data AS photo_data");
        sPeopleWithPhotoProjectionMap = map;
        
        // People with E-mail or IM projection map
        map = new HashMap<String, String>();
        map.put(People._ID, "people._id AS " + People._ID);
        map.put(ContactMethods.DATA, "contact_methods." + ContactMethods.DATA + " AS " + ContactMethods.DATA);
        map.put(ContactMethods.KIND, "contact_methods." + ContactMethods.KIND + " AS " + ContactMethods.KIND);
        map.putAll(peopleColumns);
        sPeopleWithEmailOrImProjectionMap = map;
        
        // Groups projection map
        map = new HashMap<String, String>();
        map.put(Groups._ID, Groups._ID);
        map.putAll(groupsColumns);
        map.putAll(syncColumns);
        sGroupsProjectionMap = map;

        // Group Membership projection map
        map = new HashMap<String, String>();
        map.put(GroupMembership._ID, "groupmembership._id AS " + GroupMembership._ID);
        map.put(GroupMembership.PERSON_ID, GroupMembership.PERSON_ID);
        map.put(GroupMembership.GROUP_ID, "groups._id AS " + GroupMembership.GROUP_ID);
        map.put(GroupMembership.GROUP_SYNC_ACCOUNT, GroupMembership.GROUP_SYNC_ACCOUNT);
        map.put(GroupMembership.GROUP_SYNC_ID, GroupMembership.GROUP_SYNC_ID);
        map.putAll(groupsColumns);
        sGroupMembershipProjectionMap = map;

        // Use this when you need to force items to the top of a times_contacted list
        map = new HashMap<String, String>(sPeopleProjectionMap);
        map.put(People.TIMES_CONTACTED, Long.MAX_VALUE + " AS " + People.TIMES_CONTACTED);
        map.put("photo_data", "photos.data AS photo_data");
        sStrequentStarredProjectionMap = map;

        // Calls projection map
        map = new HashMap<String, String>();
        map.put(Calls._ID, Calls._ID);
        map.put(Calls.NUMBER, Calls.NUMBER);
        map.put(Calls.DATE, Calls.DATE);
        map.put(Calls.DURATION, Calls.DURATION);
        map.put(Calls.TYPE, Calls.TYPE);
        map.put(Calls.NEW, Calls.NEW);
        map.put(Calls.CACHED_NAME, Calls.CACHED_NAME);
        map.put(Calls.CACHED_NUMBER_TYPE, Calls.CACHED_NUMBER_TYPE);
        map.put(Calls.CACHED_NUMBER_LABEL, Calls.CACHED_NUMBER_LABEL);
        sCallsProjectionMap = map;

        // Phones projection map
        map = new HashMap<String, String>();
        map.put(Phones._ID, "phones._id AS " + Phones._ID);
        map.putAll(phonesColumns);
        map.put(Phones.PERSON_ID, "phones.person AS " + Phones.PERSON_ID);
        map.put(Phones.ISPRIMARY, Phones.ISPRIMARY);
        map.putAll(peopleColumns);
        sPhonesProjectionMap = map;

        // Phones with presence projection map
        map = new HashMap<String, String>(sPhonesProjectionMap);
        map.putAll(presenceColumns);
        sPhonesWithPresenceProjectionMap = map;

        // Organizations projection map
        map = new HashMap<String, String>();
        map.put(Organizations._ID, "organizations._id AS " + Organizations._ID);
        map.put(Organizations.LABEL, Organizations.LABEL);
        map.put(Organizations.TYPE, Organizations.TYPE);
        map.put(Organizations.PERSON_ID, Organizations.PERSON_ID);
        map.put(Organizations.COMPANY, Organizations.COMPANY);
        map.put(Organizations.TITLE, Organizations.TITLE);
        map.put(Organizations.ISPRIMARY, Organizations.ISPRIMARY);
        sOrganizationsProjectionMap = map;

        // Extensions projection map
        map = new HashMap<String, String>();
        map.put(Extensions._ID, Extensions._ID);
        map.put(Extensions.NAME, Extensions.NAME);
        map.put(Extensions.VALUE, Extensions.VALUE);
        map.put(Extensions.PERSON_ID, Extensions.PERSON_ID);
        sExtensionsProjectionMap = map;

        // Contact methods projection map
        map = new HashMap<String, String>();
        map.put(ContactMethods._ID, "contact_methods._id AS " + ContactMethods._ID);
        map.put(ContactMethods.KIND, ContactMethods.KIND);
        map.put(ContactMethods.TYPE, ContactMethods.TYPE);
        map.put(ContactMethods.LABEL, ContactMethods.LABEL);
        map.put(ContactMethods.DATA, ContactMethods.DATA);
        map.put(ContactMethods.AUX_DATA, ContactMethods.AUX_DATA);
        map.put(ContactMethods.PERSON_ID, "contact_methods.person AS " + ContactMethods.PERSON_ID);
        map.put(ContactMethods.ISPRIMARY, ContactMethods.ISPRIMARY);
        map.putAll(peopleColumns);
        sContactMethodsProjectionMap = map;

        // Contact methods with presence projection map
        map = new HashMap<String, String>(sContactMethodsProjectionMap);
        map.putAll(presenceColumns);
        sContactMethodsWithPresenceProjectionMap = map;

        // Email search projection map
        map = new HashMap<String, String>();
        map.put(ContactMethods.NAME, ContactMethods.NAME);
        map.put(ContactMethods.DATA, ContactMethods.DATA);
        map.put(ContactMethods._ID, "contact_methods._id AS " + ContactMethods._ID);
        sEmailSearchProjectionMap = map;

        // Presence projection map
        map = new HashMap<String, String>();
        map.put(Presence._ID, "presence._id AS " + Presence._ID);
        map.putAll(presenceColumns);
        map.putAll(peopleColumns);
        sPresenceProjectionMap = map;

        // Search suggestions projection map
        map = new HashMap<String, String>();
        map.put(SearchManager.SUGGEST_COLUMN_TEXT_1,
                DISPLAY_NAME_SQL + " AS " + SearchManager.SUGGEST_COLUMN_TEXT_1);
        map.put(SearchManager.SUGGEST_COLUMN_TEXT_2,
                SUGGEST_DESCRIPTION_SQL + " AS " + SearchManager.SUGGEST_COLUMN_TEXT_2);
        map.put(SearchManager.SUGGEST_COLUMN_ICON_1,
                com.android.internal.R.drawable.ic_contact_picture
                + " AS " + SearchManager.SUGGEST_COLUMN_ICON_1);
        map.put(SearchManager.SUGGEST_COLUMN_ICON_1_BITMAP,
                Photos.DATA + " AS " + SearchManager.SUGGEST_COLUMN_ICON_1_BITMAP);
        map.put(SearchManager.SUGGEST_COLUMN_ICON_2,
                PRESENCE_ICON_SQL + " AS " + SearchManager.SUGGEST_COLUMN_ICON_2);
        map.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID,
                "people._id AS " + SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID);
        map.put(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
                "people._id AS " + SearchManager.SUGGEST_COLUMN_SHORTCUT_ID);
        map.put(People._ID, "people._id AS " + People._ID);
        sSearchSuggestionsProjectionMap = map;

        // Photos projection map
        map = new HashMap<String, String>();
        map.put(Photos._ID, Photos._ID);
        map.put(Photos.LOCAL_VERSION, Photos.LOCAL_VERSION);
        map.put(Photos.EXISTS_ON_SERVER, Photos.EXISTS_ON_SERVER);
        map.put(Photos.SYNC_ERROR, Photos.SYNC_ERROR);
        map.put(Photos.PERSON_ID, Photos.PERSON_ID);
        map.put(Photos.DATA, Photos.DATA);
        map.put(Photos.DOWNLOAD_REQUIRED, ""
                + "(exists_on_server!=0 "
                + " AND sync_error IS NULL "
                + " AND (local_version IS NULL OR _sync_version != local_version)) "
                + "AS " + Photos.DOWNLOAD_REQUIRED);
        map.putAll(syncColumns);
        sPhotosProjectionMap = map;

        // Live folder projection
        map = new HashMap<String, String>();
        map.put(LiveFolders._ID, "people._id AS " + LiveFolders._ID);
        map.put(LiveFolders.NAME, DISPLAY_NAME_SQL + " AS " + LiveFolders.NAME);
        // TODO: Put contact photo back when we have a way to display a default icon
        // for contacts without a photo
        // map.put(LiveFolders.ICON_BITMAP, Photos.DATA + " AS " + LiveFolders.ICON_BITMAP);
        sLiveFoldersProjectionMap = map;
        
        // Order by statements
        sPhonesKeyOrderBy = buildOrderBy(sPhonesTable, Phones.NUMBER);
        sContactMethodsKeyOrderBy = buildOrderBy(sContactMethodsTable,
                ContactMethods.DATA, ContactMethods.KIND);
        sOrganizationsKeyOrderBy = buildOrderBy(sOrganizationsTable, Organizations.COMPANY);
        sGroupmembershipKeyOrderBy =
                buildOrderBy(sGroupmembershipTable, GroupMembership.GROUP_SYNC_ACCOUNT);

        sPhonesKeyColumns = new String[]{Phones.NUMBER};
        sContactMethodsKeyColumns = new String[]{ContactMethods.DATA, ContactMethods.KIND};
        sOrganizationsKeyColumns = new String[]{Organizations.COMPANY};
        sGroupmembershipKeyColumns = new String[]{GroupMembership.GROUP_SYNC_ACCOUNT};
        sExtensionsKeyColumns = new String[]{Extensions.NAME};

        String groupJoinByLocalId = "groups._id=groupmembership.group_id";
        String groupJoinByServerId = "("
                + "groups._sync_account=groupmembership.group_sync_account"
                + " AND "
                + "groups._sync_id=groupmembership.group_sync_id"
                + ")";
        sGroupsJoinString = "(" + groupJoinByLocalId + " OR " + groupJoinByServerId + ")";
    }
}
