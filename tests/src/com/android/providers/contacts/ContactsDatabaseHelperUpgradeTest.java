/*
 * Copyright (C) 2015 The Android Open Source Project
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

import static com.android.providers.contacts.TestUtils.cv;
import static com.android.providers.contacts.TestUtils.executeSqlFromAssetFile;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.BaseColumns;
import android.provider.CallLog.Calls;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.DeletedContacts;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.DisplayNameSources;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.MetadataSync;
import android.provider.ContactsContract.MetadataSyncState;
import android.provider.ContactsContract.PhotoFiles;
import android.provider.ContactsContract.PinnedPositions;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.StreamItemPhotos;
import android.provider.ContactsContract.StreamItems;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.test.suitebuilder.annotation.LargeTest;

import com.android.providers.contacts.ContactsDatabaseHelper.AccountsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregatedPresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregationExceptionColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataUsageStatColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DirectoryColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.GroupsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MetadataSyncColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MetadataSyncStateColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.MimetypesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.NicknameLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PackagesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PhoneLookupColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.PreAuthorizedUris;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.StatusUpdatesColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.testutil.TestUtil;
import com.android.providers.contacts.util.PropertyUtils;

/**
 * Unit tests for database create/upgrade operations in  {@link ContactsDatabaseHelper}.
 *
 * Tests are only performed after version 1108, based on the sql dump asset in
 * ContactsDatabaseHelperUpgradeTest#CONTACTS2_DB_1108_ASSET_NAME
 *
 * Run the test like this: <code> runtest -c com.android.providers.contacts.ContactsDatabaseHelperUpgradeTest
 * contactsprov </code>
 */
@LargeTest
public class ContactsDatabaseHelperUpgradeTest extends BaseDatabaseHelperUpgradeTest {

    private static final String CONTACTS2_DB_1108_ASSET_NAME = "upgradeTest/contacts2_1108.sql";

    /**
     * The helper instance.  Note we just use it to call the upgrade method.  The database
     * hold by this instance is not used in this test.
     */
    private ContactsDatabaseHelper mHelper;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mHelper = ContactsDatabaseHelper.getNewInstanceForTest(getContext(),
                TestUtils.getContactsDatabaseFilename(getContext()));
        mHelper.onConfigure(mDb);
    }

    @Override
    protected void tearDown() throws Exception {
        mHelper.close();
        super.tearDown();
    }

    @Override
    protected String getDatabaseFilename() {
        return TestUtils.getContactsDatabaseFilename(getContext(), "-upgrade-test");
    }

    public void testDatabaseCreate() {
        mHelper.onCreate(mDb);
        assertDatabaseStructureSameAsList(TABLE_LIST, /* isNewDatabase =*/ true);
    }

    public void testDatabaseUpgrade_UpgradeToCurrent() {
        create1108(mDb);
        int oldVersion = upgrade(1108, 1200);
        oldVersion = upgradeTo1201(oldVersion);
        oldVersion = upgrade(oldVersion, ContactsDatabaseHelper.DATABASE_VERSION);

        assertDatabaseStructureSameAsList(TABLE_LIST, /* isNewDatabase =*/ false);
    }

    /**
     * Upgrade the database version by version, and test it in each step
     */
    public void testDatabaseUpgrade_Incremental() {
        create1108(mDb);

        int oldVersion = 1108;
        oldVersion = upgradeTo1109(oldVersion);
        oldVersion = upgrade(oldVersion, ContactsDatabaseHelper.DATABASE_VERSION);
        assertEquals(ContactsDatabaseHelper.DATABASE_VERSION, oldVersion);
        assertDatabaseStructureSameAsList(TABLE_LIST, /* isNewDatabase =*/ false);
    }

    private int upgradeTo1109(int upgradeFrom) {
        final int MY_VERSION = 1109;
        mHelper.onUpgrade(mDb, upgradeFrom, MY_VERSION);
        TableStructure calls = new TableStructure(mDb, "calls");
        calls.assertHasColumn(Calls.LAST_MODIFIED, INTEGER, false, "0");

        TableStructure voicemailStatus = new TableStructure(mDb, "voicemail_status");
        voicemailStatus.assertHasColumn(Status.QUOTA_OCCUPIED, INTEGER, false, "-1");
        voicemailStatus.assertHasColumn(Status.QUOTA_TOTAL, INTEGER, false, "-1");

        return MY_VERSION;
    }

    private int upgradeTo1201(int upgradeFrom) {
        final int MY_VERSION = 1201;

        executeSqlFromAssetFile(getTestContext(), mDb, "upgradeTest/pre_upgrade1201.sql");

        mHelper.onUpgrade(mDb, upgradeFrom, MY_VERSION);

        try (Cursor c = mDb.rawQuery("select * from contacts order by _id", null)) {
            BaseContactsProvider2Test.assertCursorValuesOrderly(c,
                    cv(Contacts._ID, 1,
                            "last_time_contacted", 0,
                            "x_last_time_contacted", 9940760264L,
                            "times_contacted", 0,
                            "x_times_contacted", 4
                            ),
                    cv(
                            "last_time_contacted", 0,
                            "x_last_time_contacted", 0,
                            "times_contacted", 0,
                            "x_times_contacted", 0
                    ));
        }

        try (Cursor c = mDb.rawQuery("select * from raw_contacts order by _id", null)) {
            BaseContactsProvider2Test.assertCursorValuesOrderly(c,
                    cv("_id", 1,
                            "last_time_contacted", 0,
                            "x_last_time_contacted", 9940760264L,
                            "times_contacted", 0,
                            "x_times_contacted", 4
                    ),
                    cv(
                            "last_time_contacted", 0,
                            "x_last_time_contacted", 0,
                            "times_contacted", 0,
                            "x_times_contacted", 0
                    ));
        }

        try (Cursor c = mDb.rawQuery("select * from data_usage_stat", null)) {
            BaseContactsProvider2Test.assertCursorValuesOrderly(c,
                    cv(
                            "last_time_used", 0,
                            "x_last_time_used", 9940760264L,
                            "times_used", 0,
                            "x_times_used", 4
                    ));
        }

        return MY_VERSION;
    }

    private int upgrade(int upgradeFrom, int upgradeTo) {
        if (upgradeFrom < upgradeTo) {
            mHelper.onUpgrade(mDb, upgradeFrom, upgradeTo);
        }
        return upgradeTo;
    }

    /**
     * A snapshot of onCreate() at version 1108, for testing upgrades. Future tests should upgrade
     * incrementally from this version.
     */
    private void create1108(SQLiteDatabase db) {
        executeSqlFromAssetFile(getTestContext(), db, CONTACTS2_DB_1108_ASSET_NAME);
    }

    /**
     * The structure of all tables in current version database.
     */
    private static final TableColumn[] PROPERTIES_COLUMNS = new TableColumn[] {
            new TableColumn(PropertyUtils.PropertiesColumns.PROPERTY_KEY, TEXT, false, null),
            new TableColumn(PropertyUtils.PropertiesColumns.PROPERTY_VALUE, TEXT, false, null),
    };

    private static final TableColumn[] ACCOUNTS_COLUMNS = new TableColumn[] {
            new TableColumn(AccountsColumns._ID, INTEGER, false, null),
            new TableColumn(AccountsColumns.ACCOUNT_NAME, TEXT, false, null),
            new TableColumn(AccountsColumns.ACCOUNT_TYPE, TEXT, false, null),
            new TableColumn(AccountsColumns.DATA_SET, TEXT, false, null),
    };

    private static final TableColumn[] CONTACTS_COLUMNS = new TableColumn[] {
            new TableColumn(BaseColumns._ID, INTEGER, false, null),
            new TableColumn(Contacts.NAME_RAW_CONTACT_ID, INTEGER, false, null),
            new TableColumn(Contacts.PHOTO_ID, INTEGER, false, null),
            new TableColumn(Contacts.PHOTO_FILE_ID, INTEGER, false, null),
            new TableColumn(Contacts.CUSTOM_RINGTONE, TEXT, false, null),
            new TableColumn(Contacts.SEND_TO_VOICEMAIL, INTEGER, true, "0"),
            new TableColumn(Contacts.RAW_TIMES_CONTACTED, INTEGER, true, "0"),
            new TableColumn(Contacts.RAW_LAST_TIME_CONTACTED, INTEGER, false, null),
            new TableColumn(Contacts.LR_TIMES_CONTACTED, INTEGER, true, "0"),
            new TableColumn(Contacts.LR_LAST_TIME_CONTACTED, INTEGER, false, null),
            new TableColumn(Contacts.STARRED, INTEGER, true, "0"),
            new TableColumn(Contacts.PINNED, INTEGER, true,
                    String.valueOf(PinnedPositions.UNPINNED)),
            new TableColumn(Contacts.HAS_PHONE_NUMBER, INTEGER, true, "0"),
            new TableColumn(Contacts.LOOKUP_KEY, TEXT, false, null),
            new TableColumn(ContactsColumns.LAST_STATUS_UPDATE_ID, INTEGER, false, null),
            new TableColumn(Contacts.CONTACT_LAST_UPDATED_TIMESTAMP, INTEGER, false, null),
    };

    private static final TableColumn[] DELETED_CONTACTS_COLUMNS = new TableColumn[] {
            new TableColumn(DeletedContacts.CONTACT_ID, INTEGER, false, null),
            new TableColumn(DeletedContacts.CONTACT_DELETED_TIMESTAMP, INTEGER, true, "0"),
    };

    private static final TableColumn[] RAW_CONTACTS_COLUMNS = new TableColumn[] {
            new TableColumn(RawContacts._ID, INTEGER, false, null),
            new TableColumn(RawContactsColumns.ACCOUNT_ID, INTEGER, false, null),
            new TableColumn(RawContacts.SOURCE_ID, TEXT, false, null),
            new TableColumn(RawContacts.BACKUP_ID, TEXT, false, null),
            new TableColumn(RawContacts.RAW_CONTACT_IS_READ_ONLY, INTEGER, true, "0"),
            new TableColumn(RawContacts.VERSION, INTEGER, true, "1"),
            new TableColumn(RawContacts.DIRTY, INTEGER, true, "0"),
            new TableColumn(RawContacts.DELETED, INTEGER, true, "0"),
            new TableColumn(RawContacts.METADATA_DIRTY, INTEGER, true, "0"),
            new TableColumn(RawContacts.CONTACT_ID, INTEGER, false, null),
            new TableColumn(RawContacts.AGGREGATION_MODE, INTEGER, true,
                    String.valueOf(RawContacts.AGGREGATION_MODE_DEFAULT)),
            new TableColumn(RawContactsColumns.AGGREGATION_NEEDED, INTEGER, true, "1"),
            new TableColumn(RawContacts.CUSTOM_RINGTONE, TEXT, false, null),
            new TableColumn(RawContacts.SEND_TO_VOICEMAIL, INTEGER, true, "0"),
            new TableColumn(RawContacts.RAW_TIMES_CONTACTED, INTEGER, true, "0"),
            new TableColumn(RawContacts.RAW_LAST_TIME_CONTACTED, INTEGER, false, null),
            new TableColumn(RawContacts.LR_TIMES_CONTACTED, INTEGER, true, "0"),
            new TableColumn(RawContacts.LR_LAST_TIME_CONTACTED, INTEGER, false, null),
            new TableColumn(RawContacts.STARRED, INTEGER, true, "0"),
            new TableColumn(RawContacts.PINNED, INTEGER, true,
                    String.valueOf(PinnedPositions.UNPINNED)),
            new TableColumn(RawContacts.DISPLAY_NAME_PRIMARY, TEXT, false, null),
            new TableColumn(RawContacts.DISPLAY_NAME_ALTERNATIVE, TEXT, false, null),
            new TableColumn(RawContacts.DISPLAY_NAME_SOURCE, INTEGER, true, String.valueOf(
                    DisplayNameSources.UNDEFINED)),
            new TableColumn(RawContacts.PHONETIC_NAME, TEXT, false, null),
            new TableColumn(RawContacts.PHONETIC_NAME_STYLE, TEXT, false, null),
            new TableColumn(RawContacts.SORT_KEY_PRIMARY, TEXT, false, null),
            new TableColumn(RawContactsColumns.PHONEBOOK_LABEL_PRIMARY, TEXT, false, null),
            new TableColumn(RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY, INTEGER, false, null),
            new TableColumn(RawContacts.SORT_KEY_ALTERNATIVE, TEXT, false, null),
            new TableColumn(RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, TEXT, false, null),
            new TableColumn(RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE, INTEGER, false, null),
            new TableColumn(RawContactsColumns.NAME_VERIFIED_OBSOLETE, INTEGER, true, "0"),
            new TableColumn(RawContacts.SYNC1, TEXT, false, null),
            new TableColumn(RawContacts.SYNC2, TEXT, false, null),
            new TableColumn(RawContacts.SYNC3, TEXT, false, null),
            new TableColumn(RawContacts.SYNC4, TEXT, false, null),
    };

    private static final TableColumn[] STREAM_ITEMS_COLUMNS = new TableColumn[] {
            new TableColumn(StreamItems._ID, INTEGER, false, null),
            new TableColumn(StreamItems.RAW_CONTACT_ID, INTEGER, true, null),
            new TableColumn(StreamItems.RES_PACKAGE, TEXT, false, null),
            new TableColumn(StreamItems.RES_ICON, TEXT, false, null),
            new TableColumn(StreamItems.RES_LABEL, TEXT, false, null),
            new TableColumn(StreamItems.TEXT, TEXT, false, null),
            new TableColumn(StreamItems.TIMESTAMP, INTEGER, true, null),
            new TableColumn(StreamItems.COMMENTS, TEXT, false, null),
            new TableColumn(StreamItems.SYNC1, TEXT, false, null),
            new TableColumn(StreamItems.SYNC2, TEXT, false, null),
            new TableColumn(StreamItems.SYNC3, TEXT, false, null),
            new TableColumn(StreamItems.SYNC4, TEXT, false, null),
    };

    private static final TableColumn[] STREAM_ITEM_PHOTOS_COLUMNS = new TableColumn[] {
            new TableColumn(StreamItemPhotos._ID, INTEGER, false, null),
            new TableColumn(StreamItemPhotos.STREAM_ITEM_ID, INTEGER, true, null),
            new TableColumn(StreamItemPhotos.SORT_INDEX, INTEGER, false, null),
            new TableColumn(StreamItemPhotos.PHOTO_FILE_ID, INTEGER, true, null),
            new TableColumn(StreamItemPhotos.SYNC1, TEXT, false, null),
            new TableColumn(StreamItemPhotos.SYNC2, TEXT, false, null),
            new TableColumn(StreamItemPhotos.SYNC3, TEXT, false, null),
            new TableColumn(StreamItemPhotos.SYNC4, TEXT, false, null),
    };

    private static final TableColumn[] PHOTO_FILES_COLUMNS = new TableColumn[] {
            new TableColumn(PhotoFiles._ID, INTEGER, false, null),
            new TableColumn(PhotoFiles.HEIGHT, INTEGER, true, null),
            new TableColumn(PhotoFiles.WIDTH, INTEGER, true, null),
            new TableColumn(PhotoFiles.FILESIZE, INTEGER, true, null),
    };

    private static final TableColumn[] PACKAGES_COLUMNS = new TableColumn[] {
            new TableColumn(PackagesColumns._ID, INTEGER, false, null),
            new TableColumn(PackagesColumns.PACKAGE, TEXT, true, null),
    };

    private static final TableColumn[] MIMETYPES_COLUMNS = new TableColumn[] {
            new TableColumn(MimetypesColumns._ID, INTEGER, false, null),
            new TableColumn(MimetypesColumns.MIMETYPE, TEXT, true, null),
    };

    private static final TableColumn[] DATA_COLUMNS = new TableColumn[] {
            new TableColumn(Data._ID, INTEGER, false, null),
            new TableColumn(DataColumns.PACKAGE_ID, INTEGER, false, null),
            new TableColumn(DataColumns.MIMETYPE_ID, INTEGER, true, null),
            new TableColumn(Data.RAW_CONTACT_ID, INTEGER, true, null),
            new TableColumn(Data.HASH_ID, TEXT, false, null),
            new TableColumn(Data.IS_READ_ONLY, INTEGER, true, "0"),
            new TableColumn(Data.IS_PRIMARY, INTEGER, true, "0"),
            new TableColumn(Data.IS_SUPER_PRIMARY, INTEGER, true, "0"),
            new TableColumn(Data.DATA_VERSION, INTEGER, true, "0"),
            new TableColumn(Data.DATA1, TEXT, false, null),
            new TableColumn(Data.DATA2, TEXT, false, null),
            new TableColumn(Data.DATA3, TEXT, false, null),
            new TableColumn(Data.DATA4, TEXT, false, null),
            new TableColumn(Data.DATA5, TEXT, false, null),
            new TableColumn(Data.DATA6, TEXT, false, null),
            new TableColumn(Data.DATA7, TEXT, false, null),
            new TableColumn(Data.DATA8, TEXT, false, null),
            new TableColumn(Data.DATA9, TEXT, false, null),
            new TableColumn(Data.DATA10, TEXT, false, null),
            new TableColumn(Data.DATA11, TEXT, false, null),
            new TableColumn(Data.DATA12, TEXT, false, null),
            new TableColumn(Data.DATA13, TEXT, false, null),
            new TableColumn(Data.DATA14, TEXT, false, null),
            new TableColumn(Data.DATA15, TEXT, false, null),
            new TableColumn(Data.SYNC1, TEXT, false, null),
            new TableColumn(Data.SYNC2, TEXT, false, null),
            new TableColumn(Data.SYNC3, TEXT, false, null),
            new TableColumn(Data.SYNC4, TEXT, false, null),
            new TableColumn(Data.CARRIER_PRESENCE, INTEGER, true, "0"),
            new TableColumn(Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME, TEXT, false, null),
            new TableColumn(Data.PREFERRED_PHONE_ACCOUNT_ID, TEXT, false, null),
    };

    private static final TableColumn[] PHONE_LOOKUP_COLUMNS = new TableColumn[] {
            new TableColumn(PhoneLookupColumns.DATA_ID, INTEGER, true, null),
            new TableColumn(PhoneLookupColumns.RAW_CONTACT_ID, INTEGER, true, null),
            new TableColumn(PhoneLookupColumns.NORMALIZED_NUMBER, TEXT, true, null),
            new TableColumn(PhoneLookupColumns.MIN_MATCH, TEXT, true, null),
    };

    private static final TableColumn[] NAME_LOOKUP_COLUMNS = new TableColumn[] {
            new TableColumn(NameLookupColumns.DATA_ID, INTEGER, true, null),
            new TableColumn(NameLookupColumns.RAW_CONTACT_ID, INTEGER, true, null),
            new TableColumn(NameLookupColumns.NORMALIZED_NAME, TEXT, true, null),
            new TableColumn(NameLookupColumns.NAME_TYPE, INTEGER, true, null),
    };

    private static final TableColumn[] NICKNAME_LOOKUP_COLUMNS = new TableColumn[] {
            new TableColumn(NicknameLookupColumns.NAME, TEXT, false, null),
            new TableColumn(NicknameLookupColumns.CLUSTER, TEXT, false, null),
    };

    private static final TableColumn[] GROUPS_COLUMNS = new TableColumn[] {
            new TableColumn(Groups._ID, INTEGER, false, null),
            new TableColumn(GroupsColumns.PACKAGE_ID, INTEGER, false, null),
            new TableColumn(GroupsColumns.ACCOUNT_ID, INTEGER, false, null),
            new TableColumn(Groups.SOURCE_ID, TEXT, false, null),
            new TableColumn(Groups.VERSION, INTEGER, true, "1"),
            new TableColumn(Groups.DIRTY, INTEGER, true, "0"),
            new TableColumn(Groups.TITLE, TEXT, false, null),
            new TableColumn(Groups.TITLE_RES, INTEGER, false, null),
            new TableColumn(Groups.NOTES, TEXT, false, null),
            new TableColumn(Groups.SYSTEM_ID, TEXT, false, null),
            new TableColumn(Groups.DELETED, INTEGER, true, "0"),
            new TableColumn(Groups.GROUP_VISIBLE, INTEGER, true, "0"),
            new TableColumn(Groups.SHOULD_SYNC, INTEGER, true, "1"),
            new TableColumn(Groups.AUTO_ADD, INTEGER, true, "0"),
            new TableColumn(Groups.FAVORITES, INTEGER, true, "0"),
            new TableColumn(Groups.GROUP_IS_READ_ONLY, INTEGER, true, "0"),
            new TableColumn(Groups.SYNC1, TEXT, false, null),
            new TableColumn(Groups.SYNC2, TEXT, false, null),
            new TableColumn(Groups.SYNC3, TEXT, false, null),
            new TableColumn(Groups.SYNC4, TEXT, false, null),
    };

    private static final TableColumn[] AGGREGATION_EXCEPTIONS_COLUMNS = new TableColumn[] {
            new TableColumn(AggregationExceptionColumns._ID, INTEGER, false, null),
            new TableColumn(AggregationExceptions.TYPE, INTEGER, true, null),
            new TableColumn(AggregationExceptions.RAW_CONTACT_ID1, INTEGER, false, null),
            new TableColumn(AggregationExceptions.RAW_CONTACT_ID2, INTEGER, false, null),
    };

    private static final TableColumn[] SETTINGS_COLUMNS = new TableColumn[] {
            new TableColumn(Settings.ACCOUNT_NAME, STRING, true, null),
            new TableColumn(Settings.ACCOUNT_TYPE, STRING, true, null),
            new TableColumn(Settings.DATA_SET, STRING, false, null),
            new TableColumn(Settings.UNGROUPED_VISIBLE, INTEGER, true, "0"),
            new TableColumn(Settings.SHOULD_SYNC, INTEGER, true, "1"),
    };

    private static final TableColumn[] VISIBLE_CONTACTS_COLUMNS = new TableColumn[] {
            new TableColumn(Contacts._ID, INTEGER, false, null),
    };

    private static final TableColumn[] DEFAULT_DIRECTORY_COLUMNS = new TableColumn[] {
            new TableColumn(Contacts._ID, INTEGER, false, null),
    };

    private static final TableColumn[] CALLS_COLUMNS = new TableColumn[] {
            new TableColumn(Calls._ID, INTEGER, false, null),
            new TableColumn(Calls.NUMBER, TEXT, false, null),
            new TableColumn(Calls.NUMBER_PRESENTATION, INTEGER, true,
                    String.valueOf(Calls.PRESENTATION_ALLOWED)),
            new TableColumn(Calls.POST_DIAL_DIGITS, TEXT, true, "''"),
            new TableColumn(Calls.DATE, INTEGER, false, null),
            new TableColumn(Calls.DURATION, INTEGER, false, null),
            new TableColumn(Calls.DATA_USAGE, INTEGER, false, null),
            new TableColumn(Calls.TYPE, INTEGER, false, null),
            new TableColumn(Calls.FEATURES, INTEGER, true, "0"),
            new TableColumn(Calls.PHONE_ACCOUNT_COMPONENT_NAME, TEXT, false, null),
            new TableColumn(Calls.PHONE_ACCOUNT_ID, TEXT, false, null),
            new TableColumn(Calls.PHONE_ACCOUNT_ADDRESS, TEXT, false, null),
            new TableColumn(Calls.PHONE_ACCOUNT_HIDDEN, INTEGER, true, "0"),
            new TableColumn(Calls.SUB_ID, INTEGER, false, "-1"),
            new TableColumn(Calls.NEW, INTEGER, false, null),
            new TableColumn(Calls.CACHED_NAME, TEXT, false, null),
            new TableColumn(Calls.CACHED_NUMBER_TYPE, INTEGER, false, null),
            new TableColumn(Calls.CACHED_NUMBER_LABEL, TEXT, false, null),
            new TableColumn(Calls.COUNTRY_ISO, TEXT, false, null),
            new TableColumn(Calls.VOICEMAIL_URI, TEXT, false, null),
            new TableColumn(Calls.IS_READ, INTEGER, false, null),
            new TableColumn(Calls.GEOCODED_LOCATION, TEXT, false, null),
            new TableColumn(Calls.CACHED_LOOKUP_URI, TEXT, false, null),
            new TableColumn(Calls.CACHED_MATCHED_NUMBER, TEXT, false, null),
            new TableColumn(Calls.CACHED_NORMALIZED_NUMBER, TEXT, false, null),
            new TableColumn(Calls.CACHED_PHOTO_ID, INTEGER, true, "0"),
            new TableColumn(Calls.CACHED_PHOTO_URI, TEXT, false, null),
            new TableColumn(Calls.CACHED_FORMATTED_NUMBER, TEXT, false, null),
            new TableColumn(Calls.ADD_FOR_ALL_USERS, INTEGER, true, "1"),
            new TableColumn(Calls.LAST_MODIFIED, INTEGER, false, "0"),
            new TableColumn(Voicemails._DATA, TEXT, false, null),
            new TableColumn(Voicemails.HAS_CONTENT, INTEGER, false, null),
            new TableColumn(Voicemails.MIME_TYPE, TEXT, false, null),
            new TableColumn(Voicemails.SOURCE_DATA, TEXT, false, null),
            new TableColumn(Voicemails.SOURCE_PACKAGE, TEXT, false, null),
            new TableColumn(Voicemails.TRANSCRIPTION, TEXT, false, null),
            new TableColumn(Voicemails.STATE, INTEGER, false, null),
            new TableColumn(Voicemails.DIRTY, INTEGER, true, "0"),
            new TableColumn(Voicemails.DELETED, INTEGER, true, "0"),
    };

    private static final TableColumn[] VOICEMAIL_STATUS_COLUMNS = new TableColumn[] {
            new TableColumn(Status._ID, INTEGER, false, null),
            new TableColumn(Status.SOURCE_PACKAGE, TEXT, true, null),
            new TableColumn(Status.PHONE_ACCOUNT_COMPONENT_NAME, TEXT, false, null),
            new TableColumn(Status.PHONE_ACCOUNT_ID, TEXT, false, null),
            new TableColumn(Status.SETTINGS_URI, TEXT, false, null),
            new TableColumn(Status.VOICEMAIL_ACCESS_URI, TEXT, false, null),
            new TableColumn(Status.CONFIGURATION_STATE, INTEGER, false, null),
            new TableColumn(Status.DATA_CHANNEL_STATE, INTEGER, false, null),
            new TableColumn(Status.NOTIFICATION_CHANNEL_STATE, INTEGER, false, null),
            new TableColumn(Status.QUOTA_OCCUPIED, INTEGER, false, "-1"),
            new TableColumn(Status.QUOTA_TOTAL, INTEGER, false, "-1"),
    };

    private static final TableColumn[] STATUS_UPDATES_COLUMNS = new TableColumn[] {
            new TableColumn(StatusUpdatesColumns.DATA_ID, INTEGER, false, null),
            new TableColumn(StatusUpdates.STATUS, TEXT, false, null),
            new TableColumn(StatusUpdates.STATUS_TIMESTAMP, INTEGER, false, null),
            new TableColumn(StatusUpdates.STATUS_RES_PACKAGE, TEXT, false, null),
            new TableColumn(StatusUpdates.STATUS_LABEL, INTEGER, false, null),
            new TableColumn(StatusUpdates.STATUS_ICON, INTEGER, false, null),
    };

    private static final TableColumn[] DIRECTORIES_COLUMNS = new TableColumn[] {
            new TableColumn(Directory._ID, INTEGER, false, null),
            new TableColumn(Directory.PACKAGE_NAME, TEXT, true, null),
            new TableColumn(Directory.DIRECTORY_AUTHORITY, TEXT, true, null),
            new TableColumn(Directory.TYPE_RESOURCE_ID, INTEGER, false, null),
            new TableColumn(DirectoryColumns.TYPE_RESOURCE_NAME, TEXT, false, null),
            new TableColumn(Directory.ACCOUNT_TYPE, TEXT, false, null),
            new TableColumn(Directory.ACCOUNT_NAME, TEXT, false, null),
            new TableColumn(Directory.DISPLAY_NAME, TEXT, false, null),
            new TableColumn(Directory.EXPORT_SUPPORT, INTEGER, true,
                    String.valueOf(Directory.EXPORT_SUPPORT_NONE)),
            new TableColumn(Directory.SHORTCUT_SUPPORT, INTEGER, true,
                    String.valueOf(Directory.SHORTCUT_SUPPORT_NONE)),
            new TableColumn(Directory.PHOTO_SUPPORT, INTEGER, true,
                    String.valueOf(Directory.PHOTO_SUPPORT_NONE)),
    };

    private static final TableColumn[] DATA_USAGE_STAT_COLUMNS = new TableColumn[] {
            new TableColumn(DataUsageStatColumns._ID, INTEGER, false, null),
            new TableColumn(DataUsageStatColumns.DATA_ID, INTEGER, true, null),
            new TableColumn(DataUsageStatColumns.USAGE_TYPE_INT, INTEGER, true, "0"),
            new TableColumn(DataUsageStatColumns.RAW_TIMES_USED, INTEGER, true, "0"),
            new TableColumn(DataUsageStatColumns.RAW_LAST_TIME_USED, INTEGER, true, "0"),
            new TableColumn(DataUsageStatColumns.LR_TIMES_USED, INTEGER, true, "0"),
            new TableColumn(DataUsageStatColumns.LR_LAST_TIME_USED, INTEGER, true, "0"),
    };

    private static final TableColumn[] METADATA_SYNC_COLUMNS = new TableColumn[] {
            new TableColumn(MetadataSync._ID, INTEGER, false, null),
            new TableColumn(MetadataSync.RAW_CONTACT_BACKUP_ID, TEXT, true, null),
            new TableColumn(MetadataSyncColumns.ACCOUNT_ID, INTEGER, true, null),
            new TableColumn(MetadataSync.DATA, TEXT, false, null),
            new TableColumn(MetadataSync.DELETED, INTEGER, true, "0"),
    };

    private static final TableColumn[] PRE_AUTHORIZED_URIS_COLUMNS = new TableColumn[] {
            new TableColumn(PreAuthorizedUris._ID, INTEGER, false, null),
            new TableColumn(PreAuthorizedUris.URI, STRING, true, null),
            new TableColumn(PreAuthorizedUris.EXPIRATION, INTEGER, true, "0"),
    };

    private static final TableColumn[] METADATA_SYNC_STATE_COLUMNS = new TableColumn[] {
            new TableColumn(MetadataSyncState._ID, INTEGER, false, null),
            new TableColumn(MetadataSyncStateColumns.ACCOUNT_ID, INTEGER, true, null),
            new TableColumn(MetadataSyncState.STATE, BLOB, false, null),
    };

    private static final TableColumn[] PRESENCE_COLUMNS = new TableColumn[] {
            new TableColumn(StatusUpdates.DATA_ID, INTEGER, false, null),
            new TableColumn(StatusUpdates.PROTOCOL, INTEGER, true, null),
            new TableColumn(StatusUpdates.CUSTOM_PROTOCOL, TEXT, false, null),
            new TableColumn(StatusUpdates.IM_HANDLE, TEXT, false, null),
            new TableColumn(StatusUpdates.IM_ACCOUNT, TEXT, false, null),
            new TableColumn(PresenceColumns.CONTACT_ID, INTEGER, false, null),
            new TableColumn(PresenceColumns.RAW_CONTACT_ID, INTEGER, false, null),
            new TableColumn(StatusUpdates.PRESENCE, INTEGER, false, null),
            new TableColumn(StatusUpdates.CHAT_CAPABILITY, INTEGER, true, "0")
    };

    private static final TableColumn[] AGGREGATED_PRESENCE_COLUMNS = new TableColumn[] {
            new TableColumn(AggregatedPresenceColumns.CONTACT_ID, INTEGER, false, null),
            new TableColumn(StatusUpdates.PRESENCE, INTEGER, false, null),
            new TableColumn(StatusUpdates.CHAT_CAPABILITY, INTEGER, true, "0")
    };

    private static final TableListEntry[] TABLE_LIST = {
            new TableListEntry(PropertyUtils.Tables.PROPERTIES, PROPERTIES_COLUMNS),
            new TableListEntry(Tables.ACCOUNTS, ACCOUNTS_COLUMNS),
            new TableListEntry(Tables.CONTACTS, CONTACTS_COLUMNS),
            new TableListEntry(Tables.DELETED_CONTACTS, DELETED_CONTACTS_COLUMNS),
            new TableListEntry(Tables.RAW_CONTACTS, RAW_CONTACTS_COLUMNS),
            new TableListEntry(Tables.STREAM_ITEMS, STREAM_ITEMS_COLUMNS),
            new TableListEntry(Tables.STREAM_ITEM_PHOTOS, STREAM_ITEM_PHOTOS_COLUMNS),
            new TableListEntry(Tables.PHOTO_FILES, PHOTO_FILES_COLUMNS),
            new TableListEntry(Tables.PACKAGES, PACKAGES_COLUMNS),
            new TableListEntry(Tables.MIMETYPES, MIMETYPES_COLUMNS),
            new TableListEntry(Tables.DATA, DATA_COLUMNS),
            new TableListEntry(Tables.PHONE_LOOKUP, PHONE_LOOKUP_COLUMNS),
            new TableListEntry(Tables.NAME_LOOKUP, NAME_LOOKUP_COLUMNS),
            new TableListEntry(Tables.NICKNAME_LOOKUP, NICKNAME_LOOKUP_COLUMNS),
            new TableListEntry(Tables.GROUPS, GROUPS_COLUMNS),
            new TableListEntry(Tables.AGGREGATION_EXCEPTIONS, AGGREGATION_EXCEPTIONS_COLUMNS),
            new TableListEntry(Tables.SETTINGS, SETTINGS_COLUMNS),
            new TableListEntry(Tables.VISIBLE_CONTACTS, VISIBLE_CONTACTS_COLUMNS),
            new TableListEntry(Tables.DEFAULT_DIRECTORY, DEFAULT_DIRECTORY_COLUMNS),
            new TableListEntry("calls", CALLS_COLUMNS, false),
            new TableListEntry("voicemail_status", VOICEMAIL_STATUS_COLUMNS, false),
            new TableListEntry(Tables.STATUS_UPDATES, STATUS_UPDATES_COLUMNS),
            new TableListEntry(Tables.DIRECTORIES, DIRECTORIES_COLUMNS),
            new TableListEntry(Tables.DATA_USAGE_STAT, DATA_USAGE_STAT_COLUMNS),
            new TableListEntry(Tables.METADATA_SYNC, METADATA_SYNC_COLUMNS),
            new TableListEntry(Tables.PRE_AUTHORIZED_URIS, PRE_AUTHORIZED_URIS_COLUMNS),
            new TableListEntry(Tables.METADATA_SYNC_STATE, METADATA_SYNC_STATE_COLUMNS),
            new TableListEntry(Tables.PRESENCE, PRESENCE_COLUMNS),
            new TableListEntry(Tables.AGGREGATED_PRESENCE, AGGREGATED_PRESENCE_COLUMNS)
    };

}

