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

import static com.android.providers.contacts.TestUtils.cv;
import static com.android.providers.contacts.TestUtils.dumpCursor;

import android.accounts.Account;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds.Callable;
import android.provider.ContactsContract.CommonDataKinds.Contactables;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.SipAddress;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.DataUsageFeedback;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.DisplayNameSources;
import android.provider.ContactsContract.DisplayPhoto;
import android.provider.ContactsContract.FullNameStyle;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.MetadataSync;
import android.provider.ContactsContract.MetadataSyncState;
import android.provider.ContactsContract.PhoneLookup;
import android.provider.ContactsContract.PhoneticNameStyle;
import android.provider.ContactsContract.PinnedPositions;
import android.provider.ContactsContract.Profile;
import android.provider.ContactsContract.ProviderStatus;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.RawContactsEntity;
import android.provider.ContactsContract.SearchSnippets;
import android.provider.ContactsContract.Settings;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.StreamItemPhotos;
import android.provider.ContactsContract.StreamItems;
import android.provider.OpenableColumns;
import android.telephony.PhoneNumberUtils;
import android.test.MoreAsserts;
import android.test.suitebuilder.annotation.LargeTest;
import android.text.TextUtils;
import android.util.ArraySet;

import com.android.internal.util.ArrayUtils;
import com.android.providers.contacts.ContactsActor.AlteringUserContext;
import com.android.providers.contacts.ContactsActor.MockUserManager;
import com.android.providers.contacts.ContactsDatabaseHelper.AggregationExceptionColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.ContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DataUsageStatColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.DbProperties;
import com.android.providers.contacts.ContactsDatabaseHelper.PresenceColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.MetadataEntryParser.AggregationData;
import com.android.providers.contacts.MetadataEntryParser.FieldData;
import com.android.providers.contacts.MetadataEntryParser.MetadataEntry;
import com.android.providers.contacts.MetadataEntryParser.RawContactInfo;
import com.android.providers.contacts.MetadataEntryParser.UsageStats;
import com.android.providers.contacts.testutil.CommonDatabaseUtils;
import com.android.providers.contacts.testutil.ContactUtil;
import com.android.providers.contacts.testutil.DataUtil;
import com.android.providers.contacts.testutil.DatabaseAsserts;
import com.android.providers.contacts.testutil.DeletedContactUtil;
import com.android.providers.contacts.testutil.RawContactUtil;
import com.android.providers.contacts.testutil.TestUtil;
import com.android.providers.contacts.tests.R;
import com.android.providers.contacts.util.NullContentProvider;
import com.android.providers.contacts.util.UserUtils;

import com.google.android.collect.Lists;
import com.google.android.collect.Sets;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Unit tests for {@link ContactsProvider2}.
 *
 * Run the test like this:
 * <code>
   adb shell am instrument -e class com.android.providers.contacts.ContactsProvider2Test -w \
           com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactsProvider2Test extends BaseContactsProvider2Test {

    private static final String TAG = ContactsProvider2Test.class.getSimpleName();

    private static final int MIN_MATCH = 7;

    private int mOldMinMatch1;
    private int mOldMinMatch2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper dbHelper = cp.getThreadActiveDatabaseHelperForTest();
        mOldMinMatch1 = PhoneNumberUtils.getMinMatchForTest();
        mOldMinMatch2 = dbHelper.getMinMatchForTest();
        PhoneNumberUtils.setMinMatchForTest(MIN_MATCH);
        dbHelper.setMinMatchForTest(MIN_MATCH);
    }

    @Override
    protected void tearDown() throws Exception {
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper dbHelper = cp.getThreadActiveDatabaseHelperForTest();
        PhoneNumberUtils.setMinMatchForTest(mOldMinMatch1);
        dbHelper.setMinMatchForTest(mOldMinMatch2);
        super.tearDown();
    }

    public void testConvertEnterpriseUriWithEnterpriseDirectoryToLocalUri() {
        String phoneNumber = "886";
        String directory = String.valueOf(Directory.ENTERPRISE_DEFAULT);
        boolean isSip = true;
        Uri enterpriseUri = Phone.ENTERPRISE_CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY, directory)
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        Uri localUri = ContactsProvider2.convertToLocalUri(enterpriseUri, Phone.CONTENT_URI);
        Uri expectedUri = Phone.CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY,
                        String.valueOf(Directory.DEFAULT))
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        assertUriEquals(expectedUri, localUri);
    }

    public void testConvertEnterpriseUriWithPersonalDirectoryToLocalUri() {
        String phoneNumber = "886";
        String directory = String.valueOf(Directory.DEFAULT);
        boolean isSip = true;
        Uri enterpriseUri = Phone.ENTERPRISE_CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY, directory)
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        Uri localUri = ContactsProvider2.convertToLocalUri(enterpriseUri, Phone.CONTENT_URI);
        Uri expectedUri = Phone.CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY,
                        String.valueOf(Directory.DEFAULT))
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        assertUriEquals(expectedUri, localUri);
    }

    public void testConvertEnterpriseUriWithoutDirectoryToLocalUri() {
        String phoneNumber = "886";
        boolean isSip = true;
        Uri enterpriseUri = Phone.ENTERPRISE_CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        Uri localUri = ContactsProvider2.convertToLocalUri(enterpriseUri, Phone.CONTENT_URI);
        Uri expectedUri = Phone.CONTENT_URI.buildUpon().appendPath(phoneNumber)
                .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS,
                        String.valueOf(isSip)).build();
        assertUriEquals(expectedUri, localUri);
    }

    public void testContactsProjection() {
        assertProjection(Contacts.CONTENT_URI, new String[]{
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP
        });
    }

    public void testContactsStrequentProjection() {
        assertProjection(Contacts.CONTENT_STREQUENT_URI, new String[]{
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                DataUsageStatColumns.LR_TIMES_USED,
                DataUsageStatColumns.LR_LAST_TIME_USED,
        });
    }

    public void testContactsStrequentPhoneOnlyProjection() {
        assertProjection(Contacts.CONTENT_STREQUENT_URI.buildUpon()
                    .appendQueryParameter(ContactsContract.STREQUENT_PHONE_ONLY, "true").build(),
                new String[] {
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                DataUsageStatColumns.LR_TIMES_USED,
                DataUsageStatColumns.LR_LAST_TIME_USED,
                Phone.NUMBER,
                Phone.TYPE,
                Phone.LABEL,
                Phone.IS_SUPER_PRIMARY,
                Phone.CONTACT_ID
        });
    }

    public void testContactsWithSnippetProjection() {
        assertProjection(Contacts.CONTENT_FILTER_URI.buildUpon().appendPath("nothing").build(),
            new String[]{
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                SearchSnippets.SNIPPET,
        });
    }

    public void testRawContactsProjection() {
        assertProjection(RawContacts.CONTENT_URI, new String[]{
                RawContacts._ID,
                RawContacts.CONTACT_ID,
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.DATA_SET,
                RawContacts.ACCOUNT_TYPE_AND_DATA_SET,
                RawContacts.SOURCE_ID,
                RawContacts.BACKUP_ID,
                RawContacts.VERSION,
                RawContacts.RAW_CONTACT_IS_USER_PROFILE,
                RawContacts.DIRTY,
                RawContacts.METADATA_DIRTY,
                RawContacts.DELETED,
                RawContacts.DISPLAY_NAME_PRIMARY,
                RawContacts.DISPLAY_NAME_ALTERNATIVE,
                RawContacts.DISPLAY_NAME_SOURCE,
                RawContacts.PHONETIC_NAME,
                RawContacts.PHONETIC_NAME_STYLE,
                RawContacts.SORT_KEY_PRIMARY,
                RawContacts.SORT_KEY_ALTERNATIVE,
                RawContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                RawContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                RawContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                RawContacts.TIMES_CONTACTED,
                RawContacts.LAST_TIME_CONTACTED,
                RawContacts.CUSTOM_RINGTONE,
                RawContacts.SEND_TO_VOICEMAIL,
                RawContacts.STARRED,
                RawContacts.PINNED,
                RawContacts.AGGREGATION_MODE,
                RawContacts.SYNC1,
                RawContacts.SYNC2,
                RawContacts.SYNC3,
                RawContacts.SYNC4,
        });
    }

    public void testDataProjection() {
        assertProjection(Data.CONTENT_URI, new String[]{
                Data._ID,
                Data.RAW_CONTACT_ID,
                Data.HASH_ID,
                Data.DATA_VERSION,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.RES_PACKAGE,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.CARRIER_PRESENCE,
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                Data.PREFERRED_PHONE_ACCOUNT_ID,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
                Data.CONTACT_ID,
                Data.PRESENCE,
                Data.CHAT_CAPABILITY,
                Data.STATUS,
                Data.STATUS_TIMESTAMP,
                Data.STATUS_RES_PACKAGE,
                Data.STATUS_LABEL,
                Data.STATUS_ICON,
                Data.TIMES_USED,
                Data.LAST_TIME_USED,
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.DATA_SET,
                RawContacts.ACCOUNT_TYPE_AND_DATA_SET,
                RawContacts.SOURCE_ID,
                RawContacts.BACKUP_ID,
                RawContacts.VERSION,
                RawContacts.DIRTY,
                RawContacts.RAW_CONTACT_IS_USER_PROFILE,
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                GroupMembership.GROUP_SOURCE_ID,
        });
    }

    public void testDistinctDataProjection() {
        assertProjection(Phone.CONTENT_FILTER_URI.buildUpon().appendPath("123").build(),
            new String[]{
                Data._ID,
                Data.HASH_ID,
                Data.DATA_VERSION,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.RES_PACKAGE,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.CARRIER_PRESENCE,
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                Data.PREFERRED_PHONE_ACCOUNT_ID,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
                Data.CONTACT_ID,
                Data.PRESENCE,
                Data.CHAT_CAPABILITY,
                Data.STATUS,
                Data.STATUS_TIMESTAMP,
                Data.STATUS_RES_PACKAGE,
                Data.STATUS_LABEL,
                Data.STATUS_ICON,
                Data.TIMES_USED,
                Data.LAST_TIME_USED,
                RawContacts.RAW_CONTACT_IS_USER_PROFILE,
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.CUSTOM_RINGTONE,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.LOOKUP_KEY,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                GroupMembership.GROUP_SOURCE_ID,
        });
    }

    public void testEntityProjection() {
        assertProjection(
            Uri.withAppendedPath(ContentUris.withAppendedId(Contacts.CONTENT_URI, 0),
                    Contacts.Entity.CONTENT_DIRECTORY),
            new String[]{
                Contacts.Entity._ID,
                Contacts.Entity.DATA_ID,
                Contacts.Entity.RAW_CONTACT_ID,
                Data.DATA_VERSION,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.RES_PACKAGE,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.CARRIER_PRESENCE,
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                Data.PREFERRED_PHONE_ACCOUNT_ID,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
                Data.CONTACT_ID,
                Data.PRESENCE,
                Data.CHAT_CAPABILITY,
                Data.STATUS,
                Data.STATUS_TIMESTAMP,
                Data.STATUS_RES_PACKAGE,
                Data.STATUS_LABEL,
                Data.STATUS_ICON,
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.DATA_SET,
                RawContacts.ACCOUNT_TYPE_AND_DATA_SET,
                RawContacts.SOURCE_ID,
                RawContacts.BACKUP_ID,
                RawContacts.VERSION,
                RawContacts.DELETED,
                RawContacts.DIRTY,
                RawContacts.SYNC1,
                RawContacts.SYNC2,
                RawContacts.SYNC3,
                RawContacts.SYNC4,
                Contacts._ID,
                Contacts.DISPLAY_NAME_PRIMARY,
                Contacts.DISPLAY_NAME_ALTERNATIVE,
                Contacts.DISPLAY_NAME_SOURCE,
                Contacts.PHONETIC_NAME,
                Contacts.PHONETIC_NAME_STYLE,
                Contacts.SORT_KEY_PRIMARY,
                Contacts.SORT_KEY_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_LABEL_PRIMARY,
                ContactsColumns.PHONEBOOK_BUCKET_PRIMARY,
                ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE,
                ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE,
                Contacts.LAST_TIME_CONTACTED,
                Contacts.TIMES_CONTACTED,
                Contacts.STARRED,
                Contacts.PINNED,
                Contacts.IN_DEFAULT_DIRECTORY,
                Contacts.IN_VISIBLE_GROUP,
                Contacts.PHOTO_ID,
                Contacts.PHOTO_FILE_ID,
                Contacts.PHOTO_URI,
                Contacts.PHOTO_THUMBNAIL_URI,
                Contacts.CUSTOM_RINGTONE,
                Contacts.SEND_TO_VOICEMAIL,
                Contacts.IS_USER_PROFILE,
                Contacts.LOOKUP_KEY,
                Contacts.NAME_RAW_CONTACT_ID,
                Contacts.HAS_PHONE_NUMBER,
                Contacts.CONTACT_PRESENCE,
                Contacts.CONTACT_CHAT_CAPABILITY,
                Contacts.CONTACT_STATUS,
                Contacts.CONTACT_STATUS_TIMESTAMP,
                Contacts.CONTACT_STATUS_RES_PACKAGE,
                Contacts.CONTACT_STATUS_LABEL,
                Contacts.CONTACT_STATUS_ICON,
                Contacts.CONTACT_LAST_UPDATED_TIMESTAMP,
                GroupMembership.GROUP_SOURCE_ID,
                Contacts.Entity.TIMES_USED,
                Contacts.Entity.LAST_TIME_USED,
        });
    }

    public void testRawEntityProjection() {
        assertProjection(RawContactsEntity.CONTENT_URI, new String[]{
                RawContacts.Entity.DATA_ID,
                RawContacts._ID,
                RawContacts.CONTACT_ID,
                RawContacts.ACCOUNT_NAME,
                RawContacts.ACCOUNT_TYPE,
                RawContacts.DATA_SET,
                RawContacts.ACCOUNT_TYPE_AND_DATA_SET,
                RawContacts.SOURCE_ID,
                RawContacts.BACKUP_ID,
                RawContacts.VERSION,
                RawContacts.DIRTY,
                RawContacts.DELETED,
                RawContacts.SYNC1,
                RawContacts.SYNC2,
                RawContacts.SYNC3,
                RawContacts.SYNC4,
                RawContacts.STARRED,
                RawContacts.RAW_CONTACT_IS_USER_PROFILE,
                Data.DATA_VERSION,
                Data.IS_PRIMARY,
                Data.IS_SUPER_PRIMARY,
                Data.RES_PACKAGE,
                Data.MIMETYPE,
                Data.DATA1,
                Data.DATA2,
                Data.DATA3,
                Data.DATA4,
                Data.DATA5,
                Data.DATA6,
                Data.DATA7,
                Data.DATA8,
                Data.DATA9,
                Data.DATA10,
                Data.DATA11,
                Data.DATA12,
                Data.DATA13,
                Data.DATA14,
                Data.DATA15,
                Data.CARRIER_PRESENCE,
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                Data.PREFERRED_PHONE_ACCOUNT_ID,
                Data.SYNC1,
                Data.SYNC2,
                Data.SYNC3,
                Data.SYNC4,
                GroupMembership.GROUP_SOURCE_ID,
        });
    }

    public void testPhoneLookupProjection() {
        assertProjection(PhoneLookup.CONTENT_FILTER_URI.buildUpon().appendPath("123").build(),
            new String[]{
                PhoneLookup._ID,
                PhoneLookup.CONTACT_ID,
                PhoneLookup.DATA_ID,
                PhoneLookup.LOOKUP_KEY,
                PhoneLookup.DISPLAY_NAME_SOURCE,
                PhoneLookup.DISPLAY_NAME,
                PhoneLookup.DISPLAY_NAME_ALTERNATIVE,
                PhoneLookup.PHONETIC_NAME,
                PhoneLookup.PHONETIC_NAME_STYLE,
                PhoneLookup.SORT_KEY_PRIMARY,
                PhoneLookup.SORT_KEY_ALTERNATIVE,
                PhoneLookup.LAST_TIME_CONTACTED,
                PhoneLookup.TIMES_CONTACTED,
                PhoneLookup.STARRED,
                PhoneLookup.IN_DEFAULT_DIRECTORY,
                PhoneLookup.IN_VISIBLE_GROUP,
                PhoneLookup.PHOTO_FILE_ID,
                PhoneLookup.PHOTO_ID,
                PhoneLookup.PHOTO_URI,
                PhoneLookup.PHOTO_THUMBNAIL_URI,
                PhoneLookup.CUSTOM_RINGTONE,
                PhoneLookup.HAS_PHONE_NUMBER,
                PhoneLookup.SEND_TO_VOICEMAIL,
                PhoneLookup.NUMBER,
                PhoneLookup.TYPE,
                PhoneLookup.LABEL,
                PhoneLookup.NORMALIZED_NUMBER,
                Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                Data.PREFERRED_PHONE_ACCOUNT_ID,
        });
    }

    public void testPhoneLookupEnterpriseProjection() {
        assertProjection(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI
                        .buildUpon().appendPath("123").build(),
                new String[]{
                        PhoneLookup._ID,
                        PhoneLookup.CONTACT_ID,
                        PhoneLookup.DATA_ID,
                        PhoneLookup.LOOKUP_KEY,
                        PhoneLookup.DISPLAY_NAME_SOURCE,
                        PhoneLookup.DISPLAY_NAME,
                        PhoneLookup.DISPLAY_NAME_ALTERNATIVE,
                        PhoneLookup.PHONETIC_NAME,
                        PhoneLookup.PHONETIC_NAME_STYLE,
                        PhoneLookup.SORT_KEY_PRIMARY,
                        PhoneLookup.SORT_KEY_ALTERNATIVE,
                        PhoneLookup.LAST_TIME_CONTACTED,
                        PhoneLookup.TIMES_CONTACTED,
                        PhoneLookup.STARRED,
                        PhoneLookup.IN_DEFAULT_DIRECTORY,
                        PhoneLookup.IN_VISIBLE_GROUP,
                        PhoneLookup.PHOTO_FILE_ID,
                        PhoneLookup.PHOTO_ID,
                        PhoneLookup.PHOTO_URI,
                        PhoneLookup.PHOTO_THUMBNAIL_URI,
                        PhoneLookup.CUSTOM_RINGTONE,
                        PhoneLookup.HAS_PHONE_NUMBER,
                        PhoneLookup.SEND_TO_VOICEMAIL,
                        PhoneLookup.NUMBER,
                        PhoneLookup.TYPE,
                        PhoneLookup.LABEL,
                        PhoneLookup.NORMALIZED_NUMBER,
                        Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME,
                        Data.PREFERRED_PHONE_ACCOUNT_ID,
                });
    }

    public void testSipPhoneLookupProjection() {
        assertContainProjection(PhoneLookup.CONTENT_FILTER_URI.buildUpon().appendPath("123")
                        .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS, "1")
                        .build(),
                new String[] {
                        PhoneLookup._ID,
                        PhoneLookup.CONTACT_ID,
                        PhoneLookup.DATA_ID,
                        PhoneLookup.LOOKUP_KEY,
                        PhoneLookup.DISPLAY_NAME,
                        PhoneLookup.LAST_TIME_CONTACTED,
                        PhoneLookup.TIMES_CONTACTED,
                        PhoneLookup.STARRED,
                        PhoneLookup.IN_DEFAULT_DIRECTORY,
                        PhoneLookup.IN_VISIBLE_GROUP,
                        PhoneLookup.PHOTO_FILE_ID,
                        PhoneLookup.PHOTO_ID,
                        PhoneLookup.PHOTO_URI,
                        PhoneLookup.PHOTO_THUMBNAIL_URI,
                        PhoneLookup.CUSTOM_RINGTONE,
                        PhoneLookup.HAS_PHONE_NUMBER,
                        PhoneLookup.SEND_TO_VOICEMAIL,
                        PhoneLookup.NUMBER,
                        PhoneLookup.TYPE,
                        PhoneLookup.LABEL,
                        PhoneLookup.NORMALIZED_NUMBER,
                });
    }

    public void testSipPhoneLookupEnterpriseProjection() {
        assertContainProjection(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI
                        .buildUpon()
                        .appendPath("123")
                        .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS, "1")
                        .build(),
                new String[] {
                        PhoneLookup._ID,
                        PhoneLookup.CONTACT_ID,
                        PhoneLookup.DATA_ID,
                        PhoneLookup.LOOKUP_KEY,
                        PhoneLookup.DISPLAY_NAME,
                        PhoneLookup.LAST_TIME_CONTACTED,
                        PhoneLookup.TIMES_CONTACTED,
                        PhoneLookup.STARRED,
                        PhoneLookup.IN_DEFAULT_DIRECTORY,
                        PhoneLookup.IN_VISIBLE_GROUP,
                        PhoneLookup.PHOTO_FILE_ID,
                        PhoneLookup.PHOTO_ID,
                        PhoneLookup.PHOTO_URI,
                        PhoneLookup.PHOTO_THUMBNAIL_URI,
                        PhoneLookup.CUSTOM_RINGTONE,
                        PhoneLookup.HAS_PHONE_NUMBER,
                        PhoneLookup.SEND_TO_VOICEMAIL,
                        PhoneLookup.NUMBER,
                        PhoneLookup.TYPE,
                        PhoneLookup.LABEL,
                        PhoneLookup.NORMALIZED_NUMBER,
                });
    }

    public void testGroupsProjection() {
        assertProjection(Groups.CONTENT_URI, new String[]{
                Groups._ID,
                Groups.ACCOUNT_NAME,
                Groups.ACCOUNT_TYPE,
                Groups.DATA_SET,
                Groups.ACCOUNT_TYPE_AND_DATA_SET,
                Groups.SOURCE_ID,
                Groups.DIRTY,
                Groups.VERSION,
                Groups.RES_PACKAGE,
                Groups.TITLE,
                Groups.TITLE_RES,
                Groups.GROUP_VISIBLE,
                Groups.SYSTEM_ID,
                Groups.DELETED,
                Groups.NOTES,
                Groups.SHOULD_SYNC,
                Groups.FAVORITES,
                Groups.AUTO_ADD,
                Groups.GROUP_IS_READ_ONLY,
                Groups.SYNC1,
                Groups.SYNC2,
                Groups.SYNC3,
                Groups.SYNC4,
        });
    }

    public void testGroupsSummaryProjection() {
        assertProjection(Groups.CONTENT_SUMMARY_URI, new String[]{
                Groups._ID,
                Groups.ACCOUNT_NAME,
                Groups.ACCOUNT_TYPE,
                Groups.DATA_SET,
                Groups.ACCOUNT_TYPE_AND_DATA_SET,
                Groups.SOURCE_ID,
                Groups.DIRTY,
                Groups.VERSION,
                Groups.RES_PACKAGE,
                Groups.TITLE,
                Groups.TITLE_RES,
                Groups.GROUP_VISIBLE,
                Groups.SYSTEM_ID,
                Groups.DELETED,
                Groups.NOTES,
                Groups.SHOULD_SYNC,
                Groups.FAVORITES,
                Groups.AUTO_ADD,
                Groups.GROUP_IS_READ_ONLY,
                Groups.SYNC1,
                Groups.SYNC2,
                Groups.SYNC3,
                Groups.SYNC4,
                Groups.SUMMARY_COUNT,
                Groups.SUMMARY_WITH_PHONES,
                Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT,
        });
    }

    public void testAggregateExceptionProjection() {
        assertProjection(AggregationExceptions.CONTENT_URI, new String[]{
                AggregationExceptionColumns._ID,
                AggregationExceptions.TYPE,
                AggregationExceptions.RAW_CONTACT_ID1,
                AggregationExceptions.RAW_CONTACT_ID2,
        });
    }

    public void testSettingsProjection() {
        assertProjection(Settings.CONTENT_URI, new String[]{
                Settings.ACCOUNT_NAME,
                Settings.ACCOUNT_TYPE,
                Settings.DATA_SET,
                Settings.UNGROUPED_VISIBLE,
                Settings.SHOULD_SYNC,
                Settings.ANY_UNSYNCED,
                Settings.UNGROUPED_COUNT,
                Settings.UNGROUPED_WITH_PHONES,
        });
    }

    public void testStatusUpdatesProjection() {
        assertProjection(StatusUpdates.CONTENT_URI, new String[]{
                PresenceColumns.RAW_CONTACT_ID,
                StatusUpdates.DATA_ID,
                StatusUpdates.IM_ACCOUNT,
                StatusUpdates.IM_HANDLE,
                StatusUpdates.PROTOCOL,
                StatusUpdates.CUSTOM_PROTOCOL,
                StatusUpdates.PRESENCE,
                StatusUpdates.CHAT_CAPABILITY,
                StatusUpdates.STATUS,
                StatusUpdates.STATUS_TIMESTAMP,
                StatusUpdates.STATUS_RES_PACKAGE,
                StatusUpdates.STATUS_ICON,
                StatusUpdates.STATUS_LABEL,
        });
    }

    public void testDirectoryProjection() {
        assertProjection(Directory.CONTENT_URI, new String[]{
                Directory._ID,
                Directory.PACKAGE_NAME,
                Directory.TYPE_RESOURCE_ID,
                Directory.DISPLAY_NAME,
                Directory.DIRECTORY_AUTHORITY,
                Directory.ACCOUNT_TYPE,
                Directory.ACCOUNT_NAME,
                Directory.EXPORT_SUPPORT,
                Directory.SHORTCUT_SUPPORT,
                Directory.PHOTO_SUPPORT,
        });
    }

    public void testProviderStatusProjection() {
        assertProjection(ProviderStatus.CONTENT_URI, new String[]{
                ProviderStatus.STATUS,
                ProviderStatus.DATABASE_CREATION_TIMESTAMP,
        });
    }

    public void testRawContactsInsert() {
        ContentValues values = new ContentValues();

        values.put(RawContacts.ACCOUNT_NAME, "a");
        values.put(RawContacts.ACCOUNT_TYPE, "b");
        values.put(RawContacts.DATA_SET, "ds");
        values.put(RawContacts.SOURCE_ID, "c");
        values.put(RawContacts.VERSION, 42);
        values.put(RawContacts.DIRTY, 1);
        values.put(RawContacts.DELETED, 1);
        values.put(RawContacts.AGGREGATION_MODE, RawContacts.AGGREGATION_MODE_DISABLED);
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(RawContacts.LAST_TIME_CONTACTED, 86400 + 123);
        values.put(RawContacts.TIMES_CONTACTED, 12);
        values.put(RawContacts.STARRED, 1);
        values.put(RawContacts.SYNC1, "e");
        values.put(RawContacts.SYNC2, "f");
        values.put(RawContacts.SYNC3, "g");
        values.put(RawContacts.SYNC4, "h");

        Uri rowUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rowUri);

        values.put(RawContacts.LAST_TIME_CONTACTED, 0);
        values.put(RawContacts.TIMES_CONTACTED, 0);

        assertStoredValues(rowUri, values);
        assertNetworkNotified(true);
    }

    public void testDataDirectoryWithLookupUri() {
        ContentValues values = new ContentValues();

        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        insertPhoneNumber(rawContactId, "555-GOOG-411");
        insertEmail(rawContactId, "google@android.com");

        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);

        // Complete and valid lookup URI
        Uri lookupUri = ContactsContract.Contacts.getLookupUri(contactId, lookupKey);
        Uri dataUri = Uri.withAppendedPath(lookupUri, Contacts.Data.CONTENT_DIRECTORY);

        assertDataRows(dataUri, values);

        // Complete but stale lookup URI
        lookupUri = ContactsContract.Contacts.getLookupUri(contactId + 1, lookupKey);
        dataUri = Uri.withAppendedPath(lookupUri, Contacts.Data.CONTENT_DIRECTORY);
        assertDataRows(dataUri, values);

        // Incomplete lookup URI (lookup key only, no contact ID)
        dataUri = Uri.withAppendedPath(Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI,
                lookupKey), Contacts.Data.CONTENT_DIRECTORY);
        assertDataRows(dataUri, values);
    }

    private void assertDataRows(Uri dataUri, ContentValues values) {
        Cursor cursor = mResolver.query(dataUri, new String[]{ Data.DATA1 }, null, null, Data._ID);
        assertEquals(3, cursor.getCount());
        cursor.moveToFirst();
        values.put(Data.DATA1, "John Doe");
        assertCursorValues(cursor, values);

        cursor.moveToNext();
        values.put(Data.DATA1, "555-GOOG-411");
        assertCursorValues(cursor, values);

        cursor.moveToNext();
        values.put(Data.DATA1, "google@android.com");
        assertCursorValues(cursor, values);

        cursor.close();
    }

    public void testContactEntitiesWithIdBasedUri() {
        ContentValues values = new ContentValues();
        Account account1 = new Account("act1", "actype1");
        Account account2 = new Account("act2", "actype2");

        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, account1);
        insertImHandle(rawContactId1, Im.PROTOCOL_GOOGLE_TALK, null, "gtalk");
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "gtalk", StatusUpdates.IDLE, "Busy", 90,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, account2);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri entityUri = Uri.withAppendedPath(contactUri, Contacts.Entity.CONTENT_DIRECTORY);

        assertEntityRows(entityUri, contactId, rawContactId1, rawContactId2);
    }

    public void testContactEntitiesWithLookupUri() {
        ContentValues values = new ContentValues();
        Account account1 = new Account("act1", "actype1");
        Account account2 = new Account("act2", "actype2");

        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, account1);
        insertImHandle(rawContactId1, Im.PROTOCOL_GOOGLE_TALK, null, "gtalk");
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "gtalk", StatusUpdates.IDLE, "Busy", 90,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, account2);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);
        String lookupKey = queryLookupKey(contactId);

        // First try with a matching contact ID
        Uri contactLookupUri = ContactsContract.Contacts.getLookupUri(contactId, lookupKey);
        Uri entityUri = Uri.withAppendedPath(contactLookupUri, Contacts.Entity.CONTENT_DIRECTORY);
        assertEntityRows(entityUri, contactId, rawContactId1, rawContactId2);

        // Now try with a contact ID mismatch
        contactLookupUri = ContactsContract.Contacts.getLookupUri(contactId + 1, lookupKey);
        entityUri = Uri.withAppendedPath(contactLookupUri, Contacts.Entity.CONTENT_DIRECTORY);
        assertEntityRows(entityUri, contactId, rawContactId1, rawContactId2);

        // Now try without an ID altogether
        contactLookupUri = Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, lookupKey);
        entityUri = Uri.withAppendedPath(contactLookupUri, Contacts.Entity.CONTENT_DIRECTORY);
        assertEntityRows(entityUri, contactId, rawContactId1, rawContactId2);
    }

    private void assertEntityRows(Uri entityUri, long contactId, long rawContactId1,
            long rawContactId2) {
        ContentValues values = new ContentValues();

        Cursor cursor = mResolver.query(entityUri, null, null, null,
                Contacts.Entity.RAW_CONTACT_ID + "," + Contacts.Entity.DATA_ID);
        assertEquals(3, cursor.getCount());

        // First row - name
        cursor.moveToFirst();
        values.put(Contacts.Entity.CONTACT_ID, contactId);
        values.put(Contacts.Entity.RAW_CONTACT_ID, rawContactId1);
        values.put(Contacts.Entity.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);
        values.put(Contacts.Entity.DATA1, "John Doe");
        values.put(Contacts.Entity.ACCOUNT_NAME, "act1");
        values.put(Contacts.Entity.ACCOUNT_TYPE, "actype1");
        values.put(Contacts.Entity.DISPLAY_NAME, "John Doe");
        values.put(Contacts.Entity.DISPLAY_NAME_ALTERNATIVE, "Doe, John");
        values.put(Contacts.Entity.NAME_RAW_CONTACT_ID, rawContactId1);
        values.put(Contacts.Entity.CONTACT_CHAT_CAPABILITY, StatusUpdates.CAPABILITY_HAS_CAMERA);
        values.put(Contacts.Entity.CONTACT_PRESENCE, StatusUpdates.IDLE);
        values.put(Contacts.Entity.CONTACT_STATUS, "Busy");
        values.putNull(Contacts.Entity.PRESENCE);
        assertCursorValues(cursor, values);

        // Second row - IM
        cursor.moveToNext();
        values.put(Contacts.Entity.CONTACT_ID, contactId);
        values.put(Contacts.Entity.RAW_CONTACT_ID, rawContactId1);
        values.put(Contacts.Entity.MIMETYPE, Im.CONTENT_ITEM_TYPE);
        values.put(Contacts.Entity.DATA1, "gtalk");
        values.put(Contacts.Entity.ACCOUNT_NAME, "act1");
        values.put(Contacts.Entity.ACCOUNT_TYPE, "actype1");
        values.put(Contacts.Entity.DISPLAY_NAME, "John Doe");
        values.put(Contacts.Entity.DISPLAY_NAME_ALTERNATIVE, "Doe, John");
        values.put(Contacts.Entity.NAME_RAW_CONTACT_ID, rawContactId1);
        values.put(Contacts.Entity.CONTACT_CHAT_CAPABILITY, StatusUpdates.CAPABILITY_HAS_CAMERA);
        values.put(Contacts.Entity.CONTACT_PRESENCE, StatusUpdates.IDLE);
        values.put(Contacts.Entity.CONTACT_STATUS, "Busy");
        values.put(Contacts.Entity.PRESENCE, StatusUpdates.IDLE);
        assertCursorValues(cursor, values);

        // Third row - second raw contact, not data
        cursor.moveToNext();
        values.put(Contacts.Entity.CONTACT_ID, contactId);
        values.put(Contacts.Entity.RAW_CONTACT_ID, rawContactId2);
        values.putNull(Contacts.Entity.MIMETYPE);
        values.putNull(Contacts.Entity.DATA_ID);
        values.putNull(Contacts.Entity.DATA1);
        values.put(Contacts.Entity.ACCOUNT_NAME, "act2");
        values.put(Contacts.Entity.ACCOUNT_TYPE, "actype2");
        values.put(Contacts.Entity.DISPLAY_NAME, "John Doe");
        values.put(Contacts.Entity.DISPLAY_NAME_ALTERNATIVE, "Doe, John");
        values.put(Contacts.Entity.NAME_RAW_CONTACT_ID, rawContactId1);
        values.put(Contacts.Entity.CONTACT_CHAT_CAPABILITY, StatusUpdates.CAPABILITY_HAS_CAMERA);
        values.put(Contacts.Entity.CONTACT_PRESENCE, StatusUpdates.IDLE);
        values.put(Contacts.Entity.CONTACT_STATUS, "Busy");
        values.putNull(Contacts.Entity.PRESENCE);
        assertCursorValues(cursor, values);

        cursor.close();
    }

    public void testDataInsert() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");

        ContentValues values = new ContentValues();
        putDataValues(values, rawContactId);
        Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);
        long dataId = ContentUris.parseId(dataUri);

        long contactId = queryContactId(rawContactId);
        values.put(RawContacts.CONTACT_ID, contactId);
        assertStoredValues(dataUri, values);

        values.remove(Photo.PHOTO);// Remove byte[] value.
        assertSelection(Data.CONTENT_URI, values, Data._ID, dataId);

        // Access the same data through the directory under RawContacts
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        Uri rawContactDataUri =
                Uri.withAppendedPath(rawContactUri, RawContacts.Data.CONTENT_DIRECTORY);
        assertSelection(rawContactDataUri, values, Data._ID, dataId);

        // Access the same data through the directory under Contacts
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri contactDataUri = Uri.withAppendedPath(contactUri, Contacts.Data.CONTENT_DIRECTORY);
        assertSelection(contactDataUri, values, Data._ID, dataId);
        assertNetworkNotified(true);
    }

    public void testDataInsertAndUpdateHashId() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");

        // Insert a data with non-photo mimetype.
        ContentValues values = new ContentValues();
        putDataValues(values, rawContactId);
        Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);

        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper helper = cp.getDatabaseHelper();
        String data1 = values.getAsString(Data.DATA1);
        String data2 = values.getAsString(Data.DATA2);
        String combineString = data1+data2;
        String hashId = helper.generateHashIdForData(combineString.getBytes());
        assertStoredValue(dataUri, Data.HASH_ID, hashId);

        // Update the data with primary, and check if hash_id is not changed.
        values.remove(Data.DATA1);
        values.remove(Data.DATA2);
        values.remove(Data.DATA15);
        values.put(Data.IS_PRIMARY, "1");
        mResolver.update(dataUri, values, null, null);
        assertStoredValue(dataUri, Data.IS_PRIMARY, "1");
        assertStoredValue(dataUri, Data.HASH_ID, hashId);

        // Update the data with new data1.
        values = new ContentValues();
        putDataValues(values, rawContactId);
        String newData1 = "Newone";
        values.put(Data.DATA1, newData1);
        mResolver.update(dataUri, values, null, null);
        combineString = newData1+data2;
        String newHashId = helper.generateHashIdForData(combineString.getBytes());
        assertStoredValue(dataUri, Data.HASH_ID, newHashId);

        // Update the data with a new Data2.
        values.remove(Data.DATA1);
        values.put(Data.DATA2, "Newtwo");
        combineString = "NewoneNewtwo";
        String testHashId = helper.generateHashIdForData(combineString.getBytes());
        mResolver.update(dataUri, values, null, null);
        assertStoredValue(dataUri, Data.HASH_ID, testHashId);

        // Update the data with a new data1 + data2.
        values.put(Data.DATA1, "one");
        combineString = "oneNewtwo";
        testHashId = helper.generateHashIdForData(combineString.getBytes());
        mResolver.update(dataUri, values, null, null);
        assertStoredValue(dataUri, Data.HASH_ID, testHashId);

        // Update the data with null data1 and null data2.
        values.putNull(Data.DATA1);
        values.putNull(Data.DATA2);
        mResolver.update(dataUri, values, null, null);
        assertStoredValue(dataUri, Data.HASH_ID, null);
    }

    public void testDataInsertAndUpdateHashId_Photo() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");

        // Insert a data with photo mimetype.
        ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.put(Data.DATA1, "testData1");
        values.put(Data.DATA2, "testData2");
        Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);

        // Check for photo data's hashId is correct or not.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper helper = cp.getDatabaseHelper();
        String hashId = helper.getPhotoHashId();
        assertStoredValue(dataUri, Data.HASH_ID, hashId);

        // Update the data with new DATA1, and check if hash_id is not changed.
        values.put(Data.DATA1, "newData1");
        mResolver.update(dataUri, values, null, null);
        assertStoredValue(dataUri, Data.DATA1, "newData1");
        assertStoredValue(dataUri, Data.HASH_ID, hashId);
    }

    public void testDataInsertPhoneNumberTooLongIsTrimmed() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");

        ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            sb.append("12345");
        }
        final String phoneNumber1500Chars = sb.toString();
        values.put(Phone.NUMBER, phoneNumber1500Chars);

        Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);
        final long dataId = ContentUris.parseId(dataUri);

        sb.setLength(0);
        for (int i = 0; i < 200; i++) {
            sb.append("12345");
        }
        final String phoneNumber1000Chars = sb.toString();
        final ContentValues expected = new ContentValues();
        expected.put(Phone.NUMBER, phoneNumber1000Chars);
        assertSelection(dataUri, expected, Data._ID, dataId);
    }

    public void testRawContactDataQuery() {
        Account account1 = new Account("a", "b");
        Account account2 = new Account("c", "d");
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account1);
        Uri dataUri1 = DataUtil.insertStructuredName(mResolver, rawContactId1, "John", "Doe");
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, account2);
        Uri dataUri2 = DataUtil.insertStructuredName(mResolver, rawContactId2, "Jane", "Doe");

        Uri uri1 = TestUtil.maybeAddAccountQueryParameters(dataUri1, account1);
        Uri uri2 = TestUtil.maybeAddAccountQueryParameters(dataUri2, account2);
        assertStoredValue(uri1, Data._ID, ContentUris.parseId(dataUri1)) ;
        assertStoredValue(uri2, Data._ID, ContentUris.parseId(dataUri2)) ;
    }

    public void testPhonesQuery() {

        ContentValues values = new ContentValues();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(RawContacts.LAST_TIME_CONTACTED, 86400 + 5);
        values.put(RawContacts.TIMES_CONTACTED, 54321);
        values.put(RawContacts.STARRED, 1);

        Uri rawContactUri = insertRawContact(values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Meghan", "Knox");
        Uri uri = insertPhoneNumber(rawContactId, "18004664411");
        long phoneId = ContentUris.parseId(uri);


        long contactId = queryContactId(rawContactId);
        values.clear();
        values.put(Data._ID, phoneId);
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(RawContacts.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values.put(Phone.NUMBER, "18004664411");
        values.put(Phone.TYPE, Phone.TYPE_HOME);
        values.putNull(Phone.LABEL);
        values.put(Contacts.DISPLAY_NAME, "Meghan Knox");
        values.put(Contacts.CUSTOM_RINGTONE, "d");
        values.put(Contacts.SEND_TO_VOICEMAIL, 1);
        values.put(Contacts.LAST_TIME_CONTACTED, 0);
        values.put(Contacts.TIMES_CONTACTED, 0);
        values.put(Contacts.STARRED, 1);

        assertStoredValues(ContentUris.withAppendedId(Phone.CONTENT_URI, phoneId), values);
    }

    public void testPhonesWithMergedContacts() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId1, "123456789", true);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId2, "123456789", true);

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);
        assertNotAggregated(rawContactId1, rawContactId2);

        ContentValues values1 = new ContentValues();
        values1.put(Contacts.DISPLAY_NAME, "123456789");
        values1.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values1.put(Phone.NUMBER, "123456789");

        // There are two phone numbers, so we should get two rows.
        assertStoredValues(Phone.CONTENT_URI, new ContentValues[] {values1, values1});

        // Now set the dedupe flag.  But still we should get two rows, because they're two
        // different contacts.  We only dedupe within each contact.
        final Uri dedupeUri = Phone.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.REMOVE_DUPLICATE_ENTRIES, "true")
                .build();
        assertStoredValues(dedupeUri, new ContentValues[] {values1, values1});

        // Now join them into a single contact.
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        assertAggregated(rawContactId1, rawContactId2, "123456789");

        // Contact merge won't affect the default result of Phone Uri, where we don't dedupe.
        assertStoredValues(Phone.CONTENT_URI, new ContentValues[] {values1, values1});

        // Now we dedupe them.
        assertStoredValues(dedupeUri, values1);
    }

    public void testPhonesNormalizedNumber() {
        final long rawContactId = RawContactUtil.createRawContact(mResolver);

        // Write both a number and a normalized number. Those should be written as-is
        final ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values.put(Phone.NUMBER, "1234");
        values.put(Phone.NORMALIZED_NUMBER, "5678");
        values.put(Phone.TYPE, Phone.TYPE_HOME);

        final Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);

        // Check the lookup table.
        assertEquals(1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "1234"), null, null));
        assertEquals(1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "5678"), null, null));

        // Check the data table.
        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "1234", Phone.NORMALIZED_NUMBER, "5678")
                );

        // Replace both in an UPDATE
        values.clear();
        values.put(Phone.NUMBER, "4321");
        values.put(Phone.NORMALIZED_NUMBER, "8765");
        mResolver.update(dataUri, values, null, null);
        assertEquals(0,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "1234"), null, null));
        assertEquals(1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "4321"), null, null));
        assertEquals(0,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "5678"), null, null));
        assertEquals(1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "8765"), null, null));

        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "4321", Phone.NORMALIZED_NUMBER, "8765")
                );

        // Replace only NUMBER ==> NORMALIZED_NUMBER will be inferred (we test that by making
        // sure the old manual value can not be found anymore)
        values.clear();
        values.put(Phone.NUMBER, "+1-800-466-5432");
        mResolver.update(dataUri, values, null, null);
        assertEquals(
                1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "+1-800-466-5432"), null,
                        null));
        assertEquals(0,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "8765"), null, null));

        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "+1-800-466-5432", Phone.NORMALIZED_NUMBER, "+18004665432")
                );

        // Replace only NORMALIZED_NUMBER ==> call is ignored, things will be unchanged
        values.clear();
        values.put(Phone.NORMALIZED_NUMBER, "8765");
        mResolver.update(dataUri, values, null, null);
        assertEquals(
                1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "+1-800-466-5432"), null,
                        null));
        assertEquals(0,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "8765"), null, null));

        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "+1-800-466-5432", Phone.NORMALIZED_NUMBER, "+18004665432")
                );

        // Replace NUMBER with an "invalid" number which can't be normalized.  It should clear
        // NORMALIZED_NUMBER.

        // 1. Set 999 to NORMALIZED_NUMBER explicitly.
        values.clear();
        values.put(Phone.NUMBER, "888");
        values.put(Phone.NORMALIZED_NUMBER, "999");
        mResolver.update(dataUri, values, null, null);

        assertEquals(1,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "999"), null, null));

        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "888", Phone.NORMALIZED_NUMBER, "999")
                );

        // 2. Set an invalid number to NUMBER.
        values.clear();
        values.put(Phone.NUMBER, "1");
        mResolver.update(dataUri, values, null, null);

        assertEquals(0,
                getCount(Uri.withAppendedPath(Phone.CONTENT_FILTER_URI, "999"), null, null));

        assertStoredValues(dataUri,
                cv(Phone.NUMBER, "1", Phone.NORMALIZED_NUMBER, null)
                );
    }

    public void testPhonesFilterQuery() {
        testPhonesFilterQueryInter(Phone.CONTENT_FILTER_URI);
    }

    /**
     * A convenient method for {@link #testPhonesFilterQuery()} and
     * {@link #testCallablesFilterQuery()}.
     *
     * This confirms if both URIs return identical results for phone-only contacts and
     * appropriately different results for contacts with sip addresses.
     *
     * @param baseFilterUri Either {@link Phone#CONTENT_FILTER_URI} or
     * {@link Callable#CONTENT_FILTER_URI}.
     */
    private void testPhonesFilterQueryInter(Uri baseFilterUri) {
        assertTrue("Unsupported Uri (" + baseFilterUri + ")",
                Phone.CONTENT_FILTER_URI.equals(baseFilterUri)
                        || Callable.CONTENT_FILTER_URI.equals(baseFilterUri));

        final long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "Hot",
                "Tamale", TestUtil.ACCOUNT_1);
        insertPhoneNumber(rawContactId1, "1-800-466-4411");

        final long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "Chilled",
                "Guacamole", TestUtil.ACCOUNT_2);
        insertPhoneNumber(rawContactId2, "1-800-466-5432");
        insertPhoneNumber(rawContactId2, "0@example.com", false, Phone.TYPE_PAGER);
        insertPhoneNumber(rawContactId2, "1@example.com", false, Phone.TYPE_PAGER);

        final Uri filterUri1 = Uri.withAppendedPath(baseFilterUri, "tamale");
        ContentValues values = new ContentValues();
        values.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values.put(Phone.NUMBER, "1-800-466-4411");
        values.put(Phone.TYPE, Phone.TYPE_HOME);
        values.putNull(Phone.LABEL);
        assertStoredValuesWithProjection(filterUri1, values);

        final Uri filterUri2 = Uri.withAppendedPath(baseFilterUri, "1-800-GOOG-411");
        assertStoredValues(filterUri2, values);

        final Uri filterUri3 = Uri.withAppendedPath(baseFilterUri, "18004664");
        assertStoredValues(filterUri3, values);

        final Uri filterUri4 = Uri.withAppendedPath(baseFilterUri, "encilada");
        assertEquals(0, getCount(filterUri4, null, null));

        final Uri filterUri5 = Uri.withAppendedPath(baseFilterUri, "*");
        assertEquals(0, getCount(filterUri5, null, null));

        ContentValues values1 = new ContentValues();
        values1.put(Contacts.DISPLAY_NAME, "Chilled Guacamole");
        values1.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values1.put(Phone.NUMBER, "1-800-466-5432");
        values1.put(Phone.TYPE, Phone.TYPE_HOME);
        values1.putNull(Phone.LABEL);

        ContentValues values2 = new ContentValues();
        values2.put(Contacts.DISPLAY_NAME, "Chilled Guacamole");
        values2.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values2.put(Phone.NUMBER, "0@example.com");
        values2.put(Phone.TYPE, Phone.TYPE_PAGER);
        values2.putNull(Phone.LABEL);

        ContentValues values3 = new ContentValues();
        values3.put(Contacts.DISPLAY_NAME, "Chilled Guacamole");
        values3.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values3.put(Phone.NUMBER, "1@example.com");
        values3.put(Phone.TYPE, Phone.TYPE_PAGER);
        values3.putNull(Phone.LABEL);

        final Uri filterUri6 = Uri.withAppendedPath(baseFilterUri, "Chilled");
        assertStoredValues(filterUri6, new ContentValues[]{values1, values2, values3});

        // Insert a SIP address. From here, Phone URI and Callable URI may return different results
        // than each other.
        insertSipAddress(rawContactId1, "sip_hot_tamale@example.com");
        insertSipAddress(rawContactId1, "sip:sip_hot@example.com");

        final Uri filterUri7 = Uri.withAppendedPath(baseFilterUri, "sip_hot");
        final Uri filterUri8 = Uri.withAppendedPath(baseFilterUri, "sip_hot_tamale");
        if (Callable.CONTENT_FILTER_URI.equals(baseFilterUri)) {
            ContentValues values4 = new ContentValues();
            values4.put(Contacts.DISPLAY_NAME, "Hot Tamale");
            values4.put(Data.MIMETYPE, SipAddress.CONTENT_ITEM_TYPE);
            values4.put(SipAddress.SIP_ADDRESS, "sip_hot_tamale@example.com");

            ContentValues values5 = new ContentValues();
            values5.put(Contacts.DISPLAY_NAME, "Hot Tamale");
            values5.put(Data.MIMETYPE, SipAddress.CONTENT_ITEM_TYPE);
            values5.put(SipAddress.SIP_ADDRESS, "sip:sip_hot@example.com");
            assertStoredValues(filterUri1, new ContentValues[] {values, values4, values5});

            assertStoredValues(filterUri7, new ContentValues[] {values4, values5});
            assertStoredValues(filterUri8, values4);
        } else {
            // Sip address should not affect Phone URI.
            assertStoredValuesWithProjection(filterUri1, values);
            assertEquals(0, getCount(filterUri7, null, null));
        }

        // Sanity test. Run tests for "Chilled Guacamole" again and see nothing changes
        // after the Sip address being inserted.
        assertStoredValues(filterUri2, values);
        assertEquals(0, getCount(filterUri4, null, null));
        assertEquals(0, getCount(filterUri5, null, null));
        assertStoredValues(filterUri6, new ContentValues[] {values1, values2, values3} );
    }

    public void testPhonesFilterSearchParams() {
        final long rid1 = RawContactUtil.createRawContactWithName(mResolver, "Dad", null);
        insertPhoneNumber(rid1, "123-456-7890");

        final long rid2 = RawContactUtil.createRawContactWithName(mResolver, "Mam", null);
        insertPhoneNumber(rid2, "323-123-4567");

        // By default, "dad" will match both the display name and the phone number.
        // Because "dad" is "323" after the dialpad conversion, it'll match "Mam" too.
        assertStoredValues(
                Phone.CONTENT_FILTER_URI.buildUpon().appendPath("dad").build(),
                cv(Phone.DISPLAY_NAME, "Dad", Phone.NUMBER, "123-456-7890"),
                cv(Phone.DISPLAY_NAME, "Mam", Phone.NUMBER, "323-123-4567")
                );
        assertStoredValues(
                Phone.CONTENT_FILTER_URI.buildUpon().appendPath("dad")
                    .appendQueryParameter(Phone.SEARCH_PHONE_NUMBER_KEY, "0")
                    .build(),
                cv(Phone.DISPLAY_NAME, "Dad", Phone.NUMBER, "123-456-7890")
                );

        assertStoredValues(
                Phone.CONTENT_FILTER_URI.buildUpon().appendPath("dad")
                    .appendQueryParameter(Phone.SEARCH_DISPLAY_NAME_KEY, "0")
                    .build(),
                cv(Phone.DISPLAY_NAME, "Mam", Phone.NUMBER, "323-123-4567")
                );
        assertStoredValues(
                Phone.CONTENT_FILTER_URI.buildUpon().appendPath("dad")
                        .appendQueryParameter(Phone.SEARCH_DISPLAY_NAME_KEY, "0")
                        .appendQueryParameter(Phone.SEARCH_PHONE_NUMBER_KEY, "0")
                        .build()
        );
    }

    public void testPhoneLookup() {
        ContentValues values = new ContentValues();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);

        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot", "Tamale");
        long dataId =
                Long.parseLong(insertPhoneNumber(rawContactId, "18004664411").getLastPathSegment());

        // We'll create two lookup records, 18004664411 and +18004664411, and the below lookup
        // will match both.

        Uri lookupUri1 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "8004664411");

        values.clear();
        values.put(PhoneLookup._ID, queryContactId(rawContactId));
        values.put(PhoneLookup.CONTACT_ID, queryContactId(rawContactId));
        values.put(PhoneLookup.DATA_ID, dataId);
        values.put(PhoneLookup.DISPLAY_NAME, "Hot Tamale");
        values.put(PhoneLookup.NUMBER, "18004664411");
        values.put(PhoneLookup.TYPE, Phone.TYPE_HOME);
        values.putNull(PhoneLookup.LABEL);
        values.put(PhoneLookup.CUSTOM_RINGTONE, "d");
        values.put(PhoneLookup.SEND_TO_VOICEMAIL, 1);
        assertStoredValues(lookupUri1, null, null, new ContentValues[] {values, values});

        // In the context that 8004664411 is a valid number, "4664411" as a
        // call id should  match to both "8004664411" and "+18004664411".
        Uri lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "4664411");
        assertEquals(2, getCount(lookupUri2, null, null));

        // A wrong area code 799 vs 800 should not be matched
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "7994664411");
        assertEquals(0, getCount(lookupUri2, null, null));
    }

    public void testSipPhoneLookup() {
        ContentValues values = new ContentValues();

        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot", "Tamale");
        long dataId =
                Long.parseLong(insertSipAddress(rawContactId, "abc@sip").getLastPathSegment());

        Uri lookupUri1 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "abc@sip")
                            .buildUpon()
                            .appendQueryParameter(PhoneLookup.QUERY_PARAMETER_SIP_ADDRESS, "1")
                            .build();

        values.clear();
        values.put(PhoneLookup._ID, dataId);
        values.put(PhoneLookup.CONTACT_ID, queryContactId(rawContactId));
        values.put(PhoneLookup.DATA_ID, dataId);
        values.put(PhoneLookup.DISPLAY_NAME, "Hot Tamale");
        values.put(PhoneLookup.NUMBER, "abc@sip");
        values.putNull(PhoneLookup.LABEL);
        assertStoredValues(lookupUri1, null, null, new ContentValues[] {values});

        // A wrong sip address should not be matched
        Uri lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "wrong@sip");
        assertEquals(0, getCount(lookupUri2, null, null));
    }

    public void testPhoneLookupStarUseCases() {
        // Create two raw contacts with numbers "*123" and "12 3". This is a real life example
        // from b/13195334.
        final ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "Emergency", /* familyName =*/ null);
        insertPhoneNumber(rawContactId, "*123");

        rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "Voicemail", /* familyName =*/ null);
        insertPhoneNumber(rawContactId, "12 3");

        // Verify: "123" returns the "Voicemail" raw contact id. It should not match
        // a phone number that starts with a "*".
        Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "123");
        values.clear();
        values.put(PhoneLookup.DISPLAY_NAME, "Voicemail");
        assertStoredValues(lookupUri, null, null, new ContentValues[] {values});

        lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "(1) 23");
        values.clear();
        values.put(PhoneLookup.DISPLAY_NAME, "Voicemail");
        assertStoredValues(lookupUri, null, null, new ContentValues[] {values});

        // Verify: "*123" returns the "Emergency" raw contact id.
        lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "*1-23");
        values.clear();
        values.put(PhoneLookup.DISPLAY_NAME, "Emergency");
        assertStoredValues(lookupUri, null, null, new ContentValues[] {values});
    }

    public void testPhoneLookupReturnsNothingRatherThanStar() {
        // Create Emergency raw contact with "*123456789" number.
        final ContentValues values = new ContentValues();
        final Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        final long rawContactId1 = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Emergency",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId1, "*123456789");

        // Lookup should return no results. It does not ignore stars even when no other matches.
        final Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "123456789");
        assertEquals(0, getCount(lookupUri, null, null));
    }

    public void testPhoneLookupReturnsNothingRatherThanMissStar() {
        // Create Voice Mail raw contact with "123456789" number.
        final ContentValues values = new ContentValues();
        final Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        final long rawContactId1 = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Voice mail",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId1, "123456789");

        // Lookup should return no results. It does not ignore stars even when no other matches.
        final Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "*123456789");
        assertEquals(0, getCount(lookupUri, null, null));
    }

    public void testPhoneLookupStarNoFallbackMatch() {
        final ContentValues values = new ContentValues();
        final Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        final long rawContactId1 = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Voice mail",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId1, "*011123456789");

        // The numbers "+123456789" and "*011123456789" are a "fallback" match. The + is equivalent
        // to "011". This lookup should return no results. Lookup does not ignore
        // stars, even when doing a fallback lookup.
        final Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "+123456789");
        assertEquals(0, getCount(lookupUri, null, null));
    }

    public void testPhoneLookupStarNotBreakFallbackMatching() {
        // Create a raw contact with a phone number starting with "011"
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, new ContentValues());
        long rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "No star",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId, "011123456789");

        // Create a raw contact with a phone number starting with "*011"
        rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, new ContentValues());
        rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "Has star",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId, "*011123456789");

        // A phone number starting with "+" can (fallback) match the same phone number starting
        // with "001". Verify that this fallback matching still occurs in the presence of
        // numbers starting with "*"s.
        final Uri lookupUri1 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI,
                "+123456789");
        final ContentValues values = new ContentValues();
        values.put(PhoneLookup.DISPLAY_NAME, "No star");
        assertStoredValues(lookupUri1, null, null, new ContentValues[]{values});
    }

    public void testPhoneLookupExplicitProjection() {
        final ContentValues values = new ContentValues();
        final Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        final long rawContactId1 = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "Voice mail",
                /* familyName =*/ null);
        insertPhoneNumber(rawContactId1, "+1234567");

        // Performing a query with a non-null projection with or without PhoneLookup.Number inside
        // it should not cause a crash.
        Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "1234567");
        String[] projection = new String[] {PhoneLookup.DISPLAY_NAME};
        mResolver.query(lookupUri, projection, null, null, null);
        projection = new String[] {PhoneLookup.DISPLAY_NAME, PhoneLookup.NUMBER};
        mResolver.query(lookupUri, projection, null, null, null);

        // Shouldn't crash for a fallback query either
        lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "*0111234567");
        projection = new String[] {PhoneLookup.DISPLAY_NAME};
        mResolver.query(lookupUri, projection, null, null, null);
        projection = new String[] {PhoneLookup.DISPLAY_NAME, PhoneLookup.NUMBER};
        mResolver.query(lookupUri, projection, null, null, null);
    }

    public void testPhoneLookupUseCases() {
        ContentValues values = new ContentValues();
        Uri rawContactUri;
        long rawContactId;
        Uri lookupUri2;

        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);

        // International format in contacts
        rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot", "Tamale");
        insertPhoneNumber(rawContactId, "+1-650-861-0000");

        values.clear();

        // match with international format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "+1 650 861 0000");
        assertEquals(1, getCount(lookupUri2, null, null));

        // match with national format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "650 861 0000");
        assertEquals(1, getCount(lookupUri2, null, null));

        // does not match with wrong area code
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "649 861 0000");
        assertEquals(0, getCount(lookupUri2, null, null));

        // does not match with missing digits in mistyped area code
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "5 861 0000");
        assertEquals(0, getCount(lookupUri2, null, null));

        // does not match with missing digit in mistyped area code
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "65 861 0000");
        assertEquals(0, getCount(lookupUri2, null, null));

        // National format in contacts
        values.clear();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot1", "Tamale");
        insertPhoneNumber(rawContactId, "650-861-0001");

        values.clear();

        // match with international format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "+1 650 861 0001");
        assertEquals(2, getCount(lookupUri2, null, null));

        // match with national format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "650 861 0001");
        assertEquals(2, getCount(lookupUri2, null, null));

        // Local format in contacts
        values.clear();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot2", "Tamale");
        insertPhoneNumber(rawContactId, "861-0002");

        values.clear();

        // match with international format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "+1 650 861 0002");
        assertEquals(1, getCount(lookupUri2, null, null));

        // match with national format
        lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "650 861 0002");
        assertEquals(1, getCount(lookupUri2, null, null));
    }

    public void testIntlPhoneLookupUseCases() {
        // Checks the logic that relies on phone_number_compare_loose(Gingerbread) as a fallback
        //for phone number lookups.
        String fullNumber = "01197297427289";

        ContentValues values = new ContentValues();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        long rawContactId = ContentUris.parseId(mResolver.insert(RawContacts.CONTENT_URI, values));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Senor", "Chang");
        insertPhoneNumber(rawContactId, fullNumber);

        // Full number should definitely match.
        assertEquals(2, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, fullNumber), null, null));

        // Shorter (local) number with 0 prefix should also match.
        assertEquals(2, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "097427289"), null, null));

        // Number with international (+972) prefix should also match.
        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "+97297427289"), null, null));

        // Same shorter number with dashes should match.
        assertEquals(2, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "09-742-7289"), null, null));

        // Same shorter number with spaces should match.
        assertEquals(2, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "09 742 7289"), null, null));

        // Some other number should not match.
        assertEquals(0, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "049102395"), null, null));
    }

    public void testPhoneLookupB5252190() {
        // Test cases from b/5252190
        String storedNumber = "796010101";

        ContentValues values = new ContentValues();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        long rawContactId = ContentUris.parseId(mResolver.insert(RawContacts.CONTENT_URI, values));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Senor", "Chang");
        insertPhoneNumber(rawContactId, storedNumber);

        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "0796010101"), null, null));

        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "+48796010101"), null, null));

        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "48796010101"), null, null));

        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "4-879-601-0101"), null, null));

        assertEquals(1, getCount(Uri.withAppendedPath(
                PhoneLookup.CONTENT_FILTER_URI, "4 879 601 0101"), null, null));
    }

    public void testPhoneLookupUseStrictPhoneNumberCompare() {
        // Test lookup cases when mUseStrictPhoneNumberComparison is true
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper dbHelper = cp.getThreadActiveDatabaseHelperForTest();
        // Get and save the original value of mUseStrictPhoneNumberComparison so that we
        // can restore it when we are done with the test
        final boolean oldUseStrict = dbHelper.getUseStrictPhoneNumberComparisonForTest();
        dbHelper.setUseStrictPhoneNumberComparisonForTest(true);


        try {
            String fullNumber = "01197297427289";
            ContentValues values = new ContentValues();
            values.put(RawContacts.CUSTOM_RINGTONE, "d");
            values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
            long rawContactId = ContentUris.parseId(
                    mResolver.insert(RawContacts.CONTENT_URI, values));
            DataUtil.insertStructuredName(mResolver, rawContactId, "Senor", "Chang");
            insertPhoneNumber(rawContactId, fullNumber);
            insertPhoneNumber(rawContactId, "5103337596");
            insertPhoneNumber(rawContactId, "+19012345678");
            // One match for full number
            assertEquals(1, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, fullNumber), null, null));

            // No matches for extra digit at the front
            assertEquals(0, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "55103337596"), null, null));
            // No matches for mispelled area code
            assertEquals(0, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "5123337596"), null, null));

            // One match for matching number with dashes
            assertEquals(1, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "510-333-7596"), null, null));

            // One match for matching number with international code
            assertEquals(1, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "+1-510-333-7596"), null, null));
            values.clear();

            // No matches for extra 0 in front
            assertEquals(0, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "0-510-333-7596"), null, null));
            values.clear();

            // No matches for different country code
            assertEquals(0, getCount(Uri.withAppendedPath(
                    PhoneLookup.CONTENT_FILTER_URI, "+819012345678"), null, null));
            values.clear();
        } finally {
            // restore the original value of mUseStrictPhoneNumberComparison
            // upon test completion or failure
            dbHelper.setUseStrictPhoneNumberComparisonForTest(oldUseStrict);
        }
    }

    /**
     * Test for enterprise caller-id, but with no corp profile.
     */
    public void testPhoneLookupEnterprise_noCorpProfile() throws Exception {

        Uri uri1 = Uri.withAppendedPath(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI, "408-111-1111");

        // No contacts profile, no data.
        assertEquals(0, getCount(uri1));

        // Insert a contact into the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Doe");
        insertPhoneNumber(rawContactId, "408-111-1111");

        // Do the query again and check the result.
        Cursor c = mResolver.query(uri1, null, null, null, null);
        try {
            assertEquals(1, c.getCount());
            c.moveToPosition(0);
            long contactId = c.getLong(c.getColumnIndex(PhoneLookup._ID));
            assertFalse(Contacts.isEnterpriseContactId(contactId)); // Make sure it's not rewritten.
        } finally {
            c.close();
        }
    }

    /**
     * Test for enterprise caller-id.  Corp profile exists, but it returns a null cursor.
     */
    public void testPhoneLookupEnterprise_withCorpProfile_nullResult() throws Exception {
        setUpNullCorpProvider();

        Uri uri1 = Uri.withAppendedPath(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI, "408-111-1111");

        // No contacts profile, no data.
        assertEquals(0, getCount(uri1));

        // Insert a contact into the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Doe");
        insertPhoneNumber(rawContactId, "408-111-1111");

        // Do the query again and check the result.
        Cursor c = mResolver.query(uri1, null, null, null, null);
        try {
            assertEquals(1, c.getCount());
            c.moveToPosition(0);
            long contactId = c.getLong(c.getColumnIndex(PhoneLookup._ID));
            assertFalse(Contacts.isEnterpriseContactId(contactId)); // Make sure it's not rewritten.
        } finally {
            c.close();
        }
    }

    /**
     * Set up the corp user / CP2 and returns the corp CP2 instance.
     *
     * Create a second instance of CP2, and add it to the resolver, with the "user-id@" authority.
     */
    private SynchronousContactsProvider2 setUpCorpProvider() throws Exception {
        mActor.mockUserManager.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CORP_USER);

        // Note here we use a standalone CP2 so it'll have its own db helper.
        // Also use AlteringUserContext here to report the corp user id.
        final int userId = MockUserManager.CORP_USER.id;
        SynchronousContactsProvider2 provider = mActor.addProvider(
                new SecondaryUserContactsProvider2(userId),
                "" + userId + "@com.android.contacts",
                new AlteringUserContext(mActor.getProviderContext(), userId));
        provider.wipeData();
        return provider;
    }

    /**
     * Similar to {@link #setUpCorpProvider}, but the corp CP2 set up with this will always return
     * null from query().
     */
    private void setUpNullCorpProvider() throws Exception {
        mActor.mockUserManager.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CORP_USER);

        mActor.addProvider(
                NullContentProvider.class,
                "" + MockUserManager.CORP_USER.id + "@com.android.contacts",
                new AlteringUserContext(mActor.getProviderContext(), MockUserManager.CORP_USER.id));
    }

    /**
     * Test for query of merged primary and work contacts.
     * <p/>
     * Note: in this test, we add one more provider instance for the authority
     * "10@com.android.contacts" and use it as the corp cp2.
     */
    public void testQueryMergedDataPhones() throws Exception {
        mActor.addPermissions("android.permission.INTERACT_ACROSS_USERS");

        // Insert a contact to the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Primary");

        insertPhoneNumber(rawContactId, "111-111-1111", false, false, Phone.TYPE_MOBILE);

        // Insert a contact to the corp CP2, with different name and phone number.
        final SynchronousContactsProvider2 corpCp2 = setUpCorpProvider();
        rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name.
        ContentValues cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact2 Corp",
                StructuredName.GIVEN_NAME, "Contact2",
                StructuredName.FAMILY_NAME, "Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);
        // Insert a number.
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "222-222-2222",
                Phone.TYPE, Phone.TYPE_MOBILE);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Insert another contact to to corp CP2, with different name phone number and phone type
        rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name.
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact3 Corp",
                StructuredName.GIVEN_NAME, "Contact3",
                StructuredName.FAMILY_NAME, "Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);
        // Insert a number
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "333-333-3333",
                Phone.TYPE, Phone.TYPE_HOME);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Execute the query to get the merged result.
        Cursor c = mResolver.query(Phone.ENTERPRISE_CONTENT_URI, new String[]{Phone.CONTACT_ID,
                Phone.DISPLAY_NAME, Phone.NUMBER}, Phone.TYPE + " = ?",
                new String[]{String.valueOf(Phone.TYPE_MOBILE)}, null);
        try {
            // Verify the primary contact.
            assertEquals(2, c.getCount());
            assertEquals(3, c.getColumnCount());
            c.moveToPosition(0);
            assertEquals("Contact1 Primary", c.getString(c.getColumnIndex(Phone.DISPLAY_NAME)));
            assertEquals("111-111-1111", c.getString(c.getColumnIndex(Phone.NUMBER)));
            long contactId = c.getLong(c.getColumnIndex(Phone.CONTACT_ID));
            assertFalse(Contacts.isEnterpriseContactId(contactId));

            // Verify the enterprise contact.
            c.moveToPosition(1);
            assertEquals("Contact2 Corp", c.getString(c.getColumnIndex(Phone.DISPLAY_NAME)));
            assertEquals("222-222-2222", c.getString(c.getColumnIndex(Phone.NUMBER)));
            contactId = c.getLong(c.getColumnIndex(Phone.CONTACT_ID));
            assertTrue(Contacts.isEnterpriseContactId(contactId));
        } finally {
            c.close();
        }
    }

    /**
     * Test for query of merged primary and work contacts.
     * <p/>
     * Note: in this test, we add one more provider instance for the authority
     * "10@com.android.contacts" and use it as the corp cp2.
     */
    public void testQueryMergedDataPhones_nullCorp() throws Exception {
        mActor.addPermissions("android.permission.INTERACT_ACROSS_USERS");

        // Insert a contact to the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Primary");

        insertPhoneNumber(rawContactId, "111-111-1111", false, false, Phone.TYPE_MOBILE);

        // Insert a contact to the corp CP2, with different name and phone number.
        setUpNullCorpProvider();

        // Execute the query to get the merged result.
        Cursor c = mResolver.query(Phone.ENTERPRISE_CONTENT_URI, new String[]{Phone.CONTACT_ID,
                        Phone.DISPLAY_NAME, Phone.NUMBER}, Phone.TYPE + " = ?",
                new String[]{String.valueOf(Phone.TYPE_MOBILE)}, null);
        try {
            // Verify the primary contact.
            assertEquals(1, c.getCount());
            assertEquals(3, c.getColumnCount());
            c.moveToPosition(0);
            assertEquals("Contact1 Primary", c.getString(c.getColumnIndex(Phone.DISPLAY_NAME)));
            assertEquals("111-111-1111", c.getString(c.getColumnIndex(Phone.NUMBER)));
            long contactId = c.getLong(c.getColumnIndex(Phone.CONTACT_ID));
            assertFalse(Contacts.isEnterpriseContactId(contactId));
        } finally {
            c.close();
        }
    }

    /**
     * Test for enterprise caller-id, with the corp profile.
     *
     * Note: in this test, we add one more provider instance for the authority
     * "10@com.android.contacts" and use it as the corp cp2.
     */
    public void testPhoneLookupEnterprise_withCorpProfile() throws Exception {
        final SynchronousContactsProvider2 corpCp2 = setUpCorpProvider();

        Uri uri1 = Uri.withAppendedPath(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI, "408-111-1111");
        Uri uri2 = Uri.withAppendedPath(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI, "408-222-2222");

        // First, test with no contacts on either profile.
        assertEquals(0, getCount(uri1));

        // Insert a contact to the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Doe");
        insertPhoneNumber(rawContactId, "408-111-1111");

        // Insert a contact to the corp CP2, with the same phone number, but with a different name.
        rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name
        ContentValues cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact2 Corp",
                StructuredName.GIVEN_NAME, "Contact2",
                StructuredName.FAMILY_NAME, "Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Insert a number
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "408-111-1111",
                Phone.TYPE, Phone.TYPE_HOME);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Insert one more contact to the corp CP2, with a different number.
        rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact3 Corp",
                StructuredName.GIVEN_NAME, "Contact3",
                StructuredName.FAMILY_NAME, "Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Insert a number
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "408-222-2222",
                Phone.TYPE, Phone.TYPE_HOME);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Okay, now execute queries and check the result.

        // The first URL hits the contact in the primary CP2.
        // There's also a contact with this phone number in the corp CP2, but that will be ignored.
        Cursor c = mResolver.query(uri1, null, null, null, null);
        try {
            assertEquals(1, c.getCount());
            c.moveToPosition(0);
            assertEquals("Contact1 Doe", c.getString(c.getColumnIndex(PhoneLookup.DISPLAY_NAME)));

            // Make sure it has a personal contact ID.
            long contactId = c.getLong(c.getColumnIndex(PhoneLookup._ID));
            assertFalse(Contacts.isEnterpriseContactId(contactId));
        } finally {
            c.close();
        }

        // Test for the second phone number, which only exists in the corp cp2.
        c = mResolver.query(uri2, null, null, null, null);
        try {
            // This one actually returns 2 identical rows, probably because of the join
            // in phone_lookup.  Callers only care the first row, so returning multiple identical
            // rows should be fine.
            assertTrue(c.getCount() > 0);
            c.moveToPosition(0);
            assertEquals("Contact3 Corp", c.getString(c.getColumnIndex(PhoneLookup.DISPLAY_NAME)));

            // Make sure it has a corp contact ID.
            long contactId = c.getLong(c.getColumnIndex(PhoneLookup._ID));
            assertTrue(Contacts.isEnterpriseContactId(contactId));
        } finally {
            c.close();
        }
    }

    public void testQueryRawContactEntitiesCorp_noCorpProfile() {
        mActor.addPermissions("android.permission.INTERACT_ACROSS_USERS");

        // Insert a contact into the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Doe");
        insertPhoneNumber(rawContactId, "408-111-1111");

        // No corp profile, no data.
        assertEquals(0, getCount(RawContactsEntity.CORP_CONTENT_URI));
    }

    public void testQueryRawContactEntitiesCorp_withCorpProfile() throws Exception {
        mActor.addPermissions("android.permission.INTERACT_ACROSS_USERS");

        // Insert a contact into the primary CP2.
        long rawContactId = ContentUris.parseId(
                mResolver.insert(RawContacts.CONTENT_URI, new ContentValues()));
        DataUtil.insertStructuredName(mResolver, rawContactId, "Contact1", "Doe");
        insertPhoneNumber(rawContactId, "408-111-1111");

        // Insert a contact into corp CP2.
        final SynchronousContactsProvider2 corpCp2 = setUpCorpProvider();
        rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name.
        ContentValues cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact2 Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);
        // Insert a number.
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "222-222-2222",
                Phone.TYPE, Phone.TYPE_MOBILE);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);

        // Do the query
        Cursor c = mResolver.query(RawContactsEntity.CORP_CONTENT_URI,
                new String[]{RawContactsEntity._ID, RawContactsEntity.DATA1},
                RawContactsEntity.MIMETYPE + "=?", new String[]{
                        StructuredName.CONTENT_ITEM_TYPE}, null);
        // The result should only contains corp data.
        assertEquals(1, c.getCount());
        assertEquals(2, c.getColumnCount());
        c.moveToPosition(0);
        long id = c.getLong(c.getColumnIndex(RawContactsEntity._ID));
        String data1 = c.getString(c.getColumnIndex(RawContactsEntity.DATA1));
        assertEquals("Contact2 Corp", data1);
        assertEquals(rawContactId, id);
        c.close();
    }

    public void testRewriteCorpDirectories() {
        // 6 columns
        final MatrixCursor c = new MatrixCursor(new String[] {
                Directory._ID,
                Directory.PACKAGE_NAME,
                Directory.TYPE_RESOURCE_ID,
                Directory.DISPLAY_NAME,
                Directory.ACCOUNT_TYPE,
                Directory.ACCOUNT_NAME,
        });

        // First, convert and make sure it returns an empty cursor.
        Cursor rewritten = ContactsProvider2.rewriteCorpDirectories(c);

        assertEquals(0, rewritten.getCount());
        assertEquals(6, rewritten.getColumnCount());

        c.addRow(new Object[] {
                5L, // Directory._ID
                "name", // Directory.PACKAGE_NAME
                123, // Directory.TYPE_RESOURCE_ID
                "display", // Directory.DISPLAY_NAME
                "atype", // Directory.ACCOUNT_TYPE
                "aname", // Directory.ACCOUNT_NAME
        });

        rewritten = ContactsProvider2.rewriteCorpDirectories(c);
        assertEquals(1, rewritten.getCount());
        assertEquals(6, rewritten.getColumnCount());

        rewritten.moveToPosition(0);
        int column = 0;
        assertEquals(1000000005L, rewritten.getLong(column++));
        assertEquals("name", rewritten.getString(column++));
        assertEquals(123, rewritten.getInt(column++));
        assertEquals("display", rewritten.getString(column++));
        assertEquals("atype", rewritten.getString(column++));
        assertEquals("aname", rewritten.getString(column++));
    }

    public void testPhoneUpdate() {
        ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Hot", "Tamale");
        Uri phoneUri = insertPhoneNumber(rawContactId, "18004664411");

        Uri lookupUri1 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "8004664411");
        Uri lookupUri2 = Uri.withAppendedPath(PhoneLookup.CONTENT_FILTER_URI, "8004664422");
        assertEquals(2, getCount(lookupUri1, null, null));
        assertEquals(0, getCount(lookupUri2, null, null));

        values.clear();
        values.put(Phone.NUMBER, "18004664422");
        mResolver.update(phoneUri, values, null, null);

        assertEquals(0, getCount(lookupUri1, null, null));
        assertEquals(2, getCount(lookupUri2, null, null));

        // Setting number to null will remove the phone lookup record
        values.clear();
        values.putNull(Phone.NUMBER);
        mResolver.update(phoneUri, values, null, null);

        assertEquals(0, getCount(lookupUri1, null, null));
        assertEquals(0, getCount(lookupUri2, null, null));

        // Let's restore that phone lookup record
        values.clear();
        values.put(Phone.NUMBER, "18004664422");
        mResolver.update(phoneUri, values, null, null);
        assertEquals(0, getCount(lookupUri1, null, null));
        assertEquals(2, getCount(lookupUri2, null, null));
        assertNetworkNotified(true);
    }

    /** Tests if {@link Callable#CONTENT_URI} returns both phones and sip addresses. */
    public void testCallablesQuery() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "Meghan", "Knox");
        long phoneId1 = ContentUris.parseId(insertPhoneNumber(rawContactId1, "18004664411"));
        long contactId1 = queryContactId(rawContactId1);

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");
        long sipAddressId2 = ContentUris.parseId(
                insertSipAddress(rawContactId2, "sip@example.com"));
        long contactId2 = queryContactId(rawContactId2);

        ContentValues values1 = new ContentValues();
        values1.put(Data._ID, phoneId1);
        values1.put(Data.RAW_CONTACT_ID, rawContactId1);
        values1.put(RawContacts.CONTACT_ID, contactId1);
        values1.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values1.put(Phone.NUMBER, "18004664411");
        values1.put(Phone.TYPE, Phone.TYPE_HOME);
        values1.putNull(Phone.LABEL);
        values1.put(Contacts.DISPLAY_NAME, "Meghan Knox");

        ContentValues values2 = new ContentValues();
        values2.put(Data._ID, sipAddressId2);
        values2.put(Data.RAW_CONTACT_ID, rawContactId2);
        values2.put(RawContacts.CONTACT_ID, contactId2);
        values2.put(Data.MIMETYPE, SipAddress.CONTENT_ITEM_TYPE);
        values2.put(SipAddress.SIP_ADDRESS, "sip@example.com");
        values2.put(Contacts.DISPLAY_NAME, "John Doe");

        assertEquals(2, getCount(Callable.CONTENT_URI, null, null));
        assertStoredValues(Callable.CONTENT_URI, new ContentValues[] { values1, values2 });
    }

    public void testCallablesFilterQuery() {
        testPhonesFilterQueryInter(Callable.CONTENT_FILTER_URI);
    }

    public void testEmailsQuery() {
        ContentValues values = new ContentValues();
        values.put(RawContacts.CUSTOM_RINGTONE, "d");
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(RawContacts.LAST_TIME_CONTACTED, 86400 + 5);
        values.put(RawContacts.TIMES_CONTACTED, 54321);
        values.put(RawContacts.STARRED, 1);

        Uri rawContactUri = insertRawContact(values);
        final long rawContactId = ContentUris.parseId(rawContactUri);

        DataUtil.insertStructuredName(mResolver, rawContactId, "Meghan", "Knox");
        final Uri emailUri = insertEmail(rawContactId, "meghan@acme.com");
        final long emailId = ContentUris.parseId(emailUri);

        final long contactId = queryContactId(rawContactId);
        values.clear();
        values.put(Data._ID, emailId);
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(RawContacts.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        values.put(Email.DATA, "meghan@acme.com");
        values.put(Email.TYPE, Email.TYPE_HOME);
        values.putNull(Email.LABEL);
        values.put(Contacts.DISPLAY_NAME, "Meghan Knox");
        values.put(Contacts.CUSTOM_RINGTONE, "d");
        values.put(Contacts.SEND_TO_VOICEMAIL, 1);
        values.put(Contacts.LAST_TIME_CONTACTED, 0);
        values.put(Contacts.TIMES_CONTACTED, 0);
        values.put(Contacts.STARRED, 1);

        assertStoredValues(Email.CONTENT_URI, values);
        assertStoredValues(ContentUris.withAppendedId(Email.CONTENT_URI, emailId), values);

        // Check if the provider detects duplicated email addresses.
        final Uri emailUri2 = insertEmail(rawContactId, "meghan@acme.com");
        final long emailId2 = ContentUris.parseId(emailUri2);
        final ContentValues values2 = new ContentValues(values);
        values2.put(Data._ID, emailId2);

        final Uri dedupeUri = Email.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.REMOVE_DUPLICATE_ENTRIES, "true")
                .build();

        // URI with ID should return a correct result.
        assertStoredValues(ContentUris.withAppendedId(Email.CONTENT_URI, emailId), values);
        assertStoredValues(ContentUris.withAppendedId(dedupeUri, emailId), values);
        assertStoredValues(ContentUris.withAppendedId(Email.CONTENT_URI, emailId2), values2);
        assertStoredValues(ContentUris.withAppendedId(dedupeUri, emailId2), values2);

        assertStoredValues(Email.CONTENT_URI, new ContentValues[] {values, values2});

        // If requested to remove duplicates, the query should return just one result,
        // whose _ID won't be deterministic.
        values.remove(Data._ID);
        assertStoredValues(dedupeUri, values);
    }

    public void testEmailsLookupQuery() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Hot", "Tamale");
        insertEmail(rawContactId, "tamale@acme.com");

        Uri filterUri1 = Uri.withAppendedPath(Email.CONTENT_LOOKUP_URI, "tamale@acme.com");
        ContentValues values = new ContentValues();
        values.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        values.put(Email.DATA, "tamale@acme.com");
        values.put(Email.TYPE, Email.TYPE_HOME);
        values.putNull(Email.LABEL);
        assertStoredValues(filterUri1, values);

        Uri filterUri2 = Uri.withAppendedPath(Email.CONTENT_LOOKUP_URI, "Ta<TaMale@acme.com>");
        assertStoredValues(filterUri2, values);

        Uri filterUri3 = Uri.withAppendedPath(Email.CONTENT_LOOKUP_URI, "encilada@acme.com");
        assertEquals(0, getCount(filterUri3, null, null));
    }

    public void testEmailsFilterQuery() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "Hot", "Tamale",
                TestUtil.ACCOUNT_1);
        insertEmail(rawContactId1, "tamale@acme.com");
        insertEmail(rawContactId1, "tamale@acme.com");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "Hot", "Tamale",
                TestUtil.ACCOUNT_2);
        insertEmail(rawContactId2, "tamale@acme.com");

        Uri filterUri1 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "tam");
        ContentValues values = new ContentValues();
        values.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        values.put(Email.DATA, "tamale@acme.com");
        values.put(Email.TYPE, Email.TYPE_HOME);
        values.putNull(Email.LABEL);
        assertStoredValuesWithProjection(filterUri1, values);

        Uri filterUri2 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "hot");
        assertStoredValuesWithProjection(filterUri2, values);

        Uri filterUri3 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "hot tamale");
        assertStoredValuesWithProjection(filterUri3, values);

        Uri filterUri4 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "tamale@acme");
        assertStoredValuesWithProjection(filterUri4, values);

        Uri filterUri5 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "encilada");
        assertEquals(0, getCount(filterUri5, null, null));
    }

    /**
     * Tests if ContactsProvider2 returns addresses according to registration order.
     */
    public void testEmailFilterDefaultSortOrder() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        insertEmail(rawContactId1, "address1@email.com");
        insertEmail(rawContactId1, "address2@email.com");
        insertEmail(rawContactId1, "address3@email.com");
        ContentValues v1 = new ContentValues();
        v1.put(Email.ADDRESS, "address1@email.com");
        ContentValues v2 = new ContentValues();
        v2.put(Email.ADDRESS, "address2@email.com");
        ContentValues v3 = new ContentValues();
        v3.put(Email.ADDRESS, "address3@email.com");

        Uri filterUri = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "address");
        assertStoredValuesOrderly(filterUri, new ContentValues[]{v1, v2, v3});
    }

    /**
     * Tests if ContactsProvider2 returns primary addresses before the other addresses.
     */
    public void testEmailFilterPrimaryAddress() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        insertEmail(rawContactId1, "address1@email.com");
        insertEmail(rawContactId1, "address2@email.com", true);
        ContentValues v1 = new ContentValues();
        v1.put(Email.ADDRESS, "address1@email.com");
        ContentValues v2 = new ContentValues();
        v2.put(Email.ADDRESS, "address2@email.com");

        Uri filterUri = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "address");
        assertStoredValuesOrderly(filterUri, new ContentValues[] { v2, v1 });
    }

    /**
     * Tests if ContactsProvider2 has email address associated with a primary account before the
     * other address.
     */
    public void testEmailFilterPrimaryAccount() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, TestUtil.ACCOUNT_1);
        insertEmail(rawContactId1, "account1@email.com");
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, TestUtil.ACCOUNT_2);
        insertEmail(rawContactId2, "account2@email.com");
        ContentValues v1 = new ContentValues();
        v1.put(Email.ADDRESS, "account1@email.com");
        ContentValues v2 = new ContentValues();
        v2.put(Email.ADDRESS, "account2@email.com");

        Uri filterUri1 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("acc")
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME, TestUtil.ACCOUNT_1.name)
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_TYPE, TestUtil.ACCOUNT_1.type)
                .build();
        assertStoredValuesOrderly(filterUri1, new ContentValues[] { v1, v2 });

        Uri filterUri2 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("acc")
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME, TestUtil.ACCOUNT_2.name)
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_TYPE, TestUtil.ACCOUNT_2.type)
                .build();
        assertStoredValuesOrderly(filterUri2, new ContentValues[] { v2, v1 });

        // Just with PRIMARY_ACCOUNT_NAME
        Uri filterUri3 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("acc")
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME, TestUtil.ACCOUNT_1.name)
                .build();
        assertStoredValuesOrderly(filterUri3, new ContentValues[]{v1, v2});

        Uri filterUri4 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("acc")
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME, TestUtil.ACCOUNT_2.name)
                .build();
        assertStoredValuesOrderly(filterUri4, new ContentValues[] { v2, v1 });
    }

    /**
     * Test emails with the same domain as primary account are ordered first.
     */
    public void testEmailFilterSameDomainAccountOrder() {
        final Account account = new Account("tester@email.com", "not_used");
        final long rawContactId = RawContactUtil.createRawContact(mResolver, account);
        insertEmail(rawContactId, "account1@testemail.com");
        insertEmail(rawContactId, "account1@email.com");

        final ContentValues v1 = cv(Email.ADDRESS, "account1@testemail.com");
        final ContentValues v2 = cv(Email.ADDRESS, "account1@email.com");

        Uri filterUri1 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("acc")
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_NAME, account.name)
                .appendQueryParameter(ContactsContract.PRIMARY_ACCOUNT_TYPE, account.type)
                .build();
        assertStoredValuesOrderly(filterUri1, v2, v1);
    }

    /**
     * Test "default" emails are sorted above emails used last.
     */
    public void testEmailFilterSuperPrimaryOverUsageSort() {
        final long rawContactId = RawContactUtil.createRawContact(mResolver, TestUtil.ACCOUNT_1);
        final Uri emailUri1 = insertEmail(rawContactId, "account1@testemail.com");
        final Uri emailUri2 = insertEmail(rawContactId, "account2@testemail.com");
        insertEmail(rawContactId, "account3@testemail.com", true, true);

        // Update account1 and account 2 to have higher usage.
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri1);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri1);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri2);

        final ContentValues v1 = cv(Email.ADDRESS, "account1@testemail.com");
        final ContentValues v2 = cv(Email.ADDRESS, "account2@testemail.com");
        final ContentValues v3 = cv(Email.ADDRESS, "account3@testemail.com");

        // Test that account 3 is first even though account 1 and 2 have higher usage.
        Uri filterUri = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "acc");
        assertStoredValuesOrderly(filterUri, v3, v1, v2);
    }

    public void testEmailFilterUsageOverPrimarySort() {
        final long rawContactId = RawContactUtil.createRawContact(mResolver, TestUtil.ACCOUNT_1);
        final Uri emailUri1 = insertEmail(rawContactId, "account1@testemail.com");
        final Uri emailUri2 = insertEmail(rawContactId, "account2@testemail.com");
        insertEmail(rawContactId, "account3@testemail.com", true);

        // Update account1 and account 2 to have higher usage.
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri1);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri1);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, emailUri2);

        final ContentValues v1 = cv(Email.ADDRESS, "account1@testemail.com");
        final ContentValues v2 = cv(Email.ADDRESS, "account2@testemail.com");
        final ContentValues v3 = cv(Email.ADDRESS, "account3@testemail.com");

        // No usage stats any more, so v3 is still the first.
        Uri filterUri = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "acc");
        assertStoredValuesOrderly(filterUri, v3, v1, v2);
    }

    /** Tests {@link DataUsageFeedback} correctly promotes a data row instead of a raw contact. */
    public void testEmailFilterSortOrderWithFeedback() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        String address1 = "address1@email.com";
        insertEmail(rawContactId1, address1);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        String address2 = "address2@email.com";
        insertEmail(rawContactId2, address2);
        String address3 = "address3@email.com";
        ContentUris.parseId(insertEmail(rawContactId2, address3));

        ContentValues v1 = new ContentValues();
        v1.put(Email.ADDRESS, "address1@email.com");
        ContentValues v2 = new ContentValues();
        v2.put(Email.ADDRESS, "address2@email.com");
        ContentValues v3 = new ContentValues();
        v3.put(Email.ADDRESS, "address3@email.com");

        Uri filterUri1 = Uri.withAppendedPath(Email.CONTENT_FILTER_URI, "address");
        Uri filterUri2 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("address")
                .appendQueryParameter(DataUsageFeedback.USAGE_TYPE,
                        DataUsageFeedback.USAGE_TYPE_CALL)
                .build();
        Uri filterUri3 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("address")
                .appendQueryParameter(DataUsageFeedback.USAGE_TYPE,
                        DataUsageFeedback.USAGE_TYPE_LONG_TEXT)
                .build();
        Uri filterUri4 = Email.CONTENT_FILTER_URI.buildUpon().appendPath("address")
                .appendQueryParameter(DataUsageFeedback.USAGE_TYPE,
                        DataUsageFeedback.USAGE_TYPE_SHORT_TEXT)
                .build();
        assertStoredValuesOrderly(filterUri1, new ContentValues[] { v1, v2, v3 });
        assertStoredValuesOrderly(filterUri2, new ContentValues[] { v1, v2, v3 });
        assertStoredValuesOrderly(filterUri3, new ContentValues[] { v1, v2, v3 });
        assertStoredValuesOrderly(filterUri4, new ContentValues[] { v1, v2, v3 });

        sendFeedback(address3, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, v3);

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, rawContactId1,
                        RawContacts.TIMES_CONTACTED, 0
                        ),
                cv(RawContacts._ID, rawContactId2,
                        RawContacts.TIMES_CONTACTED, 0
                        )
                );

        // No more interaction counter, so the order doesn't change.
        assertStoredValuesOrderly(filterUri1, new ContentValues[] { v1, v2, v3 });
        assertStoredValuesOrderly(filterUri3, new ContentValues[] { v1, v2, v3 });
    }

    public void testAddQueryParametersFromUri() {
        final ContactsProvider2 provider = (ContactsProvider2) getProvider();
        final Uri originalUri = Phone.CONTENT_FILTER_URI.buildUpon()
                .appendQueryParameter("a", "a")
                .appendQueryParameter("b", "b")
                .appendQueryParameter("c", "c").build();
        final Uri.Builder targetBuilder = Phone.CONTENT_FILTER_URI.buildUpon();
        provider.addQueryParametersFromUri(targetBuilder, originalUri,
                new ArraySet<String>(Arrays.asList(new String[] {
                        "b"
                })));
        final Uri targetUri = targetBuilder.build();
        assertEquals(1, targetUri.getQueryParameters("a").size());
        assertEquals(0, targetUri.getQueryParameters("b").size());
        assertEquals(1, targetUri.getQueryParameters("c").size());
    }

    private Uri buildContactsFilterUriWithDirectory(String directory) {
        return Contacts.CONTENT_FILTER_URI.buildUpon()
                .appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY, directory).build();
    }

    public void testTestInvalidDirectory() throws Exception {
        final ContactsProvider2 provider = (ContactsProvider2) getProvider();
        assertTrue(provider.isDirectoryParamValid(Contacts.CONTENT_FILTER_URI));
        assertFalse(provider.isDirectoryParamValid(buildContactsFilterUriWithDirectory("")));
        assertTrue(provider.isDirectoryParamValid(buildContactsFilterUriWithDirectory("0")));
        assertTrue(provider.isDirectoryParamValid(buildContactsFilterUriWithDirectory("123")));
        assertFalse(provider.isDirectoryParamValid(buildContactsFilterUriWithDirectory("abc")));
    }

    public void testQueryCorpContactsProvider() throws Exception {
        final ContactsProvider2 provider = (ContactsProvider2) getProvider();
        final MockUserManager um = mActor.mockUserManager;
        final Uri enterpriseUri =
                Uri.withAppendedPath(PhoneLookup.ENTERPRISE_CONTENT_FILTER_URI, "408-222-2222");
        final Uri invalidAuthorityUri = android.provider.Settings.Secure.CONTENT_URI;

        // No corp user.  Primary only.
        assertEquals(-1, UserUtils.getCorpUserId(mActor.getProviderContext()));
        assertEquals(0, provider.queryCorpContactsProvider(enterpriseUri, null, null, null,
                null, null).getCount());

        final SynchronousContactsProvider2 corpCp2 = setUpCorpProvider();
        // Insert a contact to the corp CP2
        long rawContactId = ContentUris.parseId(
                corpCp2.insert(RawContacts.CONTENT_URI, new ContentValues()));
        // Insert a name
        ContentValues cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE,
                StructuredName.DISPLAY_NAME, "Contact2 Corp",
                StructuredName.GIVEN_NAME, "Contact2",
                StructuredName.FAMILY_NAME, "Corp");
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);
        // Insert a number
        cv = cv(
                Data.RAW_CONTACT_ID, rawContactId,
                Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE,
                Phone.NUMBER, "408-222-2222",
                Phone.TYPE, Phone.TYPE_HOME);
        corpCp2.insert(ContactsContract.Data.CONTENT_URI, cv);
        // Primary + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CORP_USER);
        // It returns 2 identical rows, probably because of the join in phone_lookup.
        assertEquals(2, provider.queryCorpContactsProvider(enterpriseUri, null, null, null,
                null, null).getCount());
        try {
            provider.queryCorpContactsProvider(invalidAuthorityUri, null, null,
                    null, null, null);
            fail(invalidAuthorityUri.toString() + " should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    public void testUpdateFromMetadataEntry() {
        String accountType1 = "accountType1";
        String accountName1 = "accountName1";
        String dataSet1 = "plus";
        Account account1 = new Account(accountName1, accountType1);
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, account1);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        // Add backup_id for the raw contact.
        String backupId = "backupId100001";
        ContentValues values = new ContentValues();
        values.put(RawContacts.BACKUP_ID, backupId);
        assertEquals(1, mResolver.update(rawContactUri, values, null, null));

        String emailAddress = "address@email.com";
        Uri dataUri = insertEmail(rawContactId, emailAddress);
        String hashId = getStoredValue(dataUri, Data.HASH_ID);

        // Another data that should not be updated.
        String phoneNumber = "111-111-1111";
        Uri dataUri2 = insertPhoneNumber(rawContactId, phoneNumber);

        // Aggregation should be deleted from local since it doesn't exist in server.
        long toBeDeletedAggRawContactId = RawContactUtil.createRawContactWithName(
                mResolver, account1);
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId, toBeDeletedAggRawContactId);

        // Check if AggregationException table has one value.
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.RAW_CONTACT_ID1,
                rawContactId);
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.RAW_CONTACT_ID2,
                toBeDeletedAggRawContactId);
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.TYPE,
                AggregationExceptions.TYPE_KEEP_SEPARATE);

        String accountType2 = "accountType2";
        String accountName2 = "accountName2";
        Account account2 = new Account(accountName2, accountType2);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, account2);
        Uri rawContactUri2 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2);
        String backupId2 = "backupId100003";
        ContentValues values2 = new ContentValues();
        values2.put(RawContacts.BACKUP_ID, backupId2);
        assertEquals(1, mResolver.update(rawContactUri2, values2, null, null));

        String usageTypeString = "CALL";
        int lastTimeUsed = 1111111;
        int timesUsed = 5;
        String aggregationTypeString = "TOGETHER";
        int aggregationType = AggregationExceptions.TYPE_KEEP_TOGETHER;

        RawContactInfo rawContactInfo = new RawContactInfo(
                backupId, accountType1, accountName1, null);
        UsageStats usageStats = new UsageStats(usageTypeString, lastTimeUsed, timesUsed);
        ArrayList<UsageStats> usageStatsList = new ArrayList<>();
        usageStatsList.add(usageStats);
        FieldData fieldData = new FieldData(hashId, true, true, usageStatsList);
        ArrayList<FieldData> fieldDataList = new ArrayList<>();
        fieldDataList.add(fieldData);
        ArrayList<AggregationData> aggregationDataList = new ArrayList<>();
        MetadataEntry metadataEntry = new MetadataEntry(rawContactInfo,
                1, 1, 1, fieldDataList, aggregationDataList);

        ContactsProvider2 provider = (ContactsProvider2) getProvider();
        final ContactsDatabaseHelper helper =
                ((ContactsDatabaseHelper) provider.getDatabaseHelper());
        SQLiteDatabase db = helper.getWritableDatabase();

        // Before updating tables from MetadataEntry.
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, accountType1);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, accountName1);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "0");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "0");
        assertStoredValue(dataUri, Data.IS_PRIMARY, 0);
        assertStoredValue(dataUri, Data.IS_SUPER_PRIMARY, 0);

        // Update tables without aggregation first, since aggregator will affect pinned value.
        provider.updateFromMetaDataEntry(db, metadataEntry);

        // After updating tables from MetadataEntry.
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, accountType1);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, accountName1);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "1");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "1");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "1");
        assertStoredValue(dataUri, Data.IS_PRIMARY, 1);
        assertStoredValue(dataUri, Data.IS_SUPER_PRIMARY, 1);
        assertStoredValue(dataUri2, Data.IS_PRIMARY, 0);
        assertStoredValue(dataUri2, Data.IS_SUPER_PRIMARY, 0);
        final Uri dataUriWithUsageType = Data.CONTENT_URI.buildUpon().appendQueryParameter(
                DataUsageFeedback.USAGE_TYPE, usageTypeString).build();
        assertDataUsageZero(dataUriWithUsageType, emailAddress);

        // Update AggregationException table.
        RawContactInfo aggregationContact = new RawContactInfo(
                backupId2, accountType2, accountName2, null);
        AggregationData aggregationData = new AggregationData(
                rawContactInfo, aggregationContact, aggregationTypeString);
        aggregationDataList.add(aggregationData);
        metadataEntry = new MetadataEntry(rawContactInfo,
                1, 1, 1, fieldDataList, aggregationDataList);
        provider.updateFromMetaDataEntry(db, metadataEntry);

        // Check if AggregationException table is updated.
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.RAW_CONTACT_ID1,
                rawContactId);
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.RAW_CONTACT_ID2,
                rawContactId2);
        assertStoredValue(AggregationExceptions.CONTENT_URI, AggregationExceptions.TYPE,
                aggregationType);

        // After aggregation, check if rawContacts.starred/send_to_voicemail
        // were copied to contacts table.
        final long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        // The merged contact should be starred if any of the rawcontact is starred.
        assertStoredValue(contactUri, Contacts.STARRED, 1);
        // The merged contact should be send_to_voicemail
        // if all of the rawcontact is send_to_voicemail.
        assertStoredValue(contactUri, Contacts.SEND_TO_VOICEMAIL, 0);
    }

    public void testUpdateMetadataOnRawContactInsert() throws Exception {
        ContactMetadataProvider contactMetadataProvider = addProvider(
                ContactMetadataProviderTestable.class, MetadataSync.METADATA_AUTHORITY);
        // Reset the dbHelper to be the one ContactsProvider2 is using. Before this, two providers
        // are using different dbHelpers.
        contactMetadataProvider.setDatabaseHelper(((SynchronousContactsProvider2)
                mActor.provider).getDatabaseHelper());
        // Create an account first.
        String backupId = "backupId001";
        String accountType = "accountType";
        String accountName = "accountName";
        Account account = new Account(accountName, accountType);
        createAccount(accountName, accountType, null);

        // Insert a metadata to MetadataSync table.
        String data = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + accountType + ",\n" +
                "    \"account_name\": " + accountName + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"FOCUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";

        ContentValues insertedValues = new ContentValues();
        insertedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        insertedValues.put(MetadataSync.ACCOUNT_TYPE, accountType);
        insertedValues.put(MetadataSync.ACCOUNT_NAME, accountName);
        insertedValues.put(MetadataSync.DATA, data);
        mResolver.insert(MetadataSync.CONTENT_URI, insertedValues);

        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);
        // Insert a raw contact.
        long rawContactId = RawContactUtil.createRawContactWithBackupId(mResolver, backupId,
                account);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        // Check if the raw contact is not updated since Lychee is removed.
        assertStoredValue(rawContactUri, RawContacts._ID, rawContactId);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, accountType);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, accountName);
        assertStoredValue(rawContactUri, RawContacts.BACKUP_ID, backupId);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "0");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "0");
        // No metadata network notify.
        assertMetadataNetworkNotified(false);
    }

    public void testUpdateMetadataOnRawContactBackupIdChange() throws Exception {
        ContactMetadataProvider contactMetadataProvider = addProvider(
                ContactMetadataProviderTestable.class, MetadataSync.METADATA_AUTHORITY);
        // Reset the dbHelper to be the one ContactsProvider2 is using. Before this, two providers
        // are using different dbHelpers.
        contactMetadataProvider.setDatabaseHelper(((SynchronousContactsProvider2)
                mActor.provider).getDatabaseHelper());
        // Create an account first.
        String backupId = "backupId001";
        String accountType = "accountType";
        String accountName = "accountName";
        Account account = new Account(accountName, accountType);
        createAccount(accountName, accountType, null);

        // Insert a metadata to MetadataSync table.
        String data = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + accountType + ",\n" +
                "    \"account_name\": " + accountName + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"FOCUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";

        ContentValues insertedValues = new ContentValues();
        insertedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        insertedValues.put(MetadataSync.ACCOUNT_TYPE, accountType);
        insertedValues.put(MetadataSync.ACCOUNT_NAME, accountName);
        insertedValues.put(MetadataSync.DATA, data);
        mResolver.insert(MetadataSync.CONTENT_URI, insertedValues);

        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);
        // Insert a raw contact without backup_id.
        long rawContactId = RawContactUtil.createRawContact(mResolver, account);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        // Check if the raw contact is not updated because of no backup_id.
        assertStoredValue(rawContactUri, RawContacts._ID, rawContactId);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, accountType);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, accountName);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "0");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "0");

        // Update the raw contact with backup_id.
        ContentValues updatedValues = new ContentValues();
        updatedValues.put(RawContacts.BACKUP_ID, backupId);
        mResolver.update(RawContacts.CONTENT_URI, updatedValues, null, null);
        // Check if the raw contact is still not updated.
        assertStoredValue(rawContactUri, RawContacts._ID, rawContactId);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_TYPE, accountType);
        assertStoredValue(rawContactUri, RawContacts.ACCOUNT_NAME, accountName);
        assertStoredValue(rawContactUri, RawContacts.SEND_TO_VOICEMAIL, "0");
        assertStoredValue(rawContactUri, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri, RawContacts.PINNED, "0");
        // No metadata network notify.
        assertMetadataNetworkNotified(false);
    }

    public void testDeleteMetadataOnRawContactDelete() throws Exception {
        ContactMetadataProvider contactMetadataProvider = addProvider(
                ContactMetadataProviderTestable.class, MetadataSync.METADATA_AUTHORITY);
        // Reset the dbHelper to be the one ContactsProvider2 is using. Before this, two providers
        // are using different dbHelpers.
        contactMetadataProvider.setDatabaseHelper(((SynchronousContactsProvider2)
                mActor.provider).getDatabaseHelper());
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);
        // Create an account first.
        String backupId = "backupId001";
        String accountType = "accountType";
        String accountName = "accountName";
        Account account = new Account(accountName, accountType);
        createAccount(accountName, accountType, null);

        // Insert a raw contact.
        long rawContactId = RawContactUtil.createRawContactWithBackupId(mResolver, backupId,
                account);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        // Insert a metadata to MetadataSync table.
        String data = "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + accountType + ",\n" +
                "    \"account_name\": " + accountName + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"FOCUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";

        ContentValues insertedValues = new ContentValues();
        insertedValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        insertedValues.put(MetadataSync.ACCOUNT_TYPE, accountType);
        insertedValues.put(MetadataSync.ACCOUNT_NAME, accountName);
        insertedValues.put(MetadataSync.DATA, data);
        Uri metadataUri = mResolver.insert(MetadataSync.CONTENT_URI, insertedValues);

        // Delete raw contact.
        mResolver.delete(rawContactUri, null, null);
        // Check if the metadata is not deleted.
        assertStoredValue(metadataUri, MetadataSync.DELETED, "0");
        // check raw contact metadata_dirty column is not changed on raw contact deletion
        assertMetadataDirty(rawContactUri, false);
        // Lychee removed. Will not notify it.
        assertMetadataNetworkNotified(false);

        // Add another rawcontact and metadata, and don't delete them.
        // Insert a raw contact.
        String backupId2 = "newBackupId";
        long rawContactId2 = RawContactUtil.createRawContactWithBackupId(mResolver, backupId2,
                account);
        Uri rawContactUri2 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        // Insert a metadata to MetadataSync table.
        ContentValues insertedValues2 = new ContentValues();
        insertedValues2.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId2);
        insertedValues2.put(MetadataSync.ACCOUNT_TYPE, accountType);
        insertedValues2.put(MetadataSync.ACCOUNT_NAME, accountName);
        insertedValues2.put(MetadataSync.DATA, data);
        Uri metadataUri2 = mResolver.insert(MetadataSync.CONTENT_URI, insertedValues2);

        // Update raw contact but not delete.
        ContentValues values = new ContentValues();
        values.put(RawContacts.STARRED, "1");
        mResolver.update(rawContactUri2, values, null, null);

        // Check if the metadata is not marked as deleted.
        assertStoredValue(metadataUri2, MetadataSync.DELETED, "0");
        // Will not set metadata_dirty since Lychee is removed.
        assertMetadataDirty(rawContactUri2, false);
        // Will not notify Lychee since it's removed.
        assertMetadataNetworkNotified(false);
    }

    public void testPostalsQuery() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Alice", "Nextore");
        Uri dataUri = insertPostalAddress(rawContactId, "1600 Amphiteatre Ave, Mountain View");
        final long dataId = ContentUris.parseId(dataUri);

        final long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();
        values.put(Data._ID, dataId);
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(RawContacts.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, StructuredPostal.CONTENT_ITEM_TYPE);
        values.put(StructuredPostal.FORMATTED_ADDRESS, "1600 Amphiteatre Ave, Mountain View");
        values.put(Contacts.DISPLAY_NAME, "Alice Nextore");

        assertStoredValues(StructuredPostal.CONTENT_URI, values);
        assertStoredValues(ContentUris.withAppendedId(StructuredPostal.CONTENT_URI, dataId),
                values);
        assertSelection(StructuredPostal.CONTENT_URI, values, Data._ID, dataId);

        // Check if the provider detects duplicated addresses.
        Uri dataUri2 = insertPostalAddress(rawContactId, "1600 Amphiteatre Ave, Mountain View");
        final long dataId2 = ContentUris.parseId(dataUri2);
        final ContentValues values2 = new ContentValues(values);
        values2.put(Data._ID, dataId2);

        final Uri dedupeUri = StructuredPostal.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.REMOVE_DUPLICATE_ENTRIES, "true")
                .build();

        // URI with ID should return a correct result.
        assertStoredValues(ContentUris.withAppendedId(StructuredPostal.CONTENT_URI, dataId),
                values);
        assertStoredValues(ContentUris.withAppendedId(dedupeUri, dataId), values);
        assertStoredValues(ContentUris.withAppendedId(StructuredPostal.CONTENT_URI, dataId2),
                values2);
        assertStoredValues(ContentUris.withAppendedId(dedupeUri, dataId2), values2);

        assertStoredValues(StructuredPostal.CONTENT_URI, new ContentValues[] {values, values2});

        // If requested to remove duplicates, the query should return just one result,
        // whose _ID won't be deterministic.
        values.remove(Data._ID);
        assertStoredValues(dedupeUri, values);
    }

    public void testDataContentUriInvisibleQuery() {
        final ContentValues values = new ContentValues();
        final long contactId = createContact(values, "John", "Doe",
                "18004664411", "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                        StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO);

        final Uri uri = Data.CONTENT_URI.buildUpon().
                appendQueryParameter(Data.VISIBLE_CONTACTS_ONLY, "true").build();
        assertEquals(4, getCount(uri, null, null));

        markInvisible(contactId);

        assertEquals(0, getCount(uri, null, null));
    }

    public void testInDefaultDirectoryData() {
        final ContentValues values = new ContentValues();
        final long contactId = createContact(values, "John", "Doe",
                "18004664411", "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        final StringBuilder query = new StringBuilder()
                .append(Data.MIMETYPE).append("='").append(Email.CONTENT_ITEM_TYPE)
                .append("' AND ").append(Email.DATA).append("=? AND ")
                .append(Contacts.IN_DEFAULT_DIRECTORY).append("=1");

        assertEquals(1,
                getCount(Email.CONTENT_URI, query.toString(), new String[]{"goog411@acme.com"}));

        // Fire!
        markInvisible(contactId);

        // Verify: making a contact visible changes the IN_DEFAULT_DIRECTORY data value.
        assertEquals(0,
                getCount(Email.CONTENT_URI, query.toString(), new String[]{"goog411@acme.com"}));
    }

    public void testContactablesQuery() {
        final long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Hot",
                "Tamale");

        insertPhoneNumber(rawContactId, "510-123-5769");
        insertEmail(rawContactId, "tamale@acme.com");

        final ContentValues cv1 = new ContentValues();
        cv1.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        cv1.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        cv1.put(Email.DATA, "tamale@acme.com");
        cv1.put(Email.TYPE, Email.TYPE_HOME);
        cv1.putNull(Email.LABEL);

        final ContentValues cv2 = new ContentValues();
        cv2.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        cv2.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        cv2.put(Phone.DATA, "510-123-5769");
        cv2.put(Phone.TYPE, Phone.TYPE_HOME);
        cv2.putNull(Phone.LABEL);

        final Uri filterUri0 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "");
        assertEquals(0, getCount(filterUri0, null, null));

        final Uri filterUri1 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "tamale");
        assertStoredValues(filterUri1, cv1, cv2);

        final Uri filterUri2 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "hot");
        assertStoredValues(filterUri2, cv1, cv2);

        final Uri filterUri3 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "tamale@ac");
        assertStoredValues(filterUri3, cv1, cv2);

        final Uri filterUri4 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "510");
        assertStoredValues(filterUri4, cv1, cv2);

        final Uri filterUri5 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "cold");
        assertEquals(0, getCount(filterUri5, null, null));

        final Uri filterUri6 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI,
                "tamale@google");
        assertEquals(0, getCount(filterUri6, null, null));

        final Uri filterUri7 = Contactables.CONTENT_URI;
        assertStoredValues(filterUri7, cv1, cv2);
    }

    public void testContactablesMultipleQuery() {

        final long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Hot",
                "Tamale");
        insertPhoneNumber(rawContactId, "510-123-5769");
        insertEmail(rawContactId, "tamale@acme.com");
        insertEmail(rawContactId, "hot@google.com");

        final long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "Cold",
                "Tamago");
        insertEmail(rawContactId2, "eggs@farmers.org");

        final long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe");
        insertPhoneNumber(rawContactId3, "518-354-1111");
        insertEmail(rawContactId3, "doeadeer@afemaledeer.com");

        final ContentValues cv1 = new ContentValues();
        cv1.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        cv1.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        cv1.put(Email.DATA, "tamale@acme.com");
        cv1.put(Email.TYPE, Email.TYPE_HOME);
        cv1.putNull(Email.LABEL);

        final ContentValues cv2 = new ContentValues();
        cv2.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        cv2.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        cv2.put(Phone.DATA, "510-123-5769");
        cv2.put(Phone.TYPE, Phone.TYPE_HOME);
        cv2.putNull(Phone.LABEL);

        final ContentValues cv3 = new ContentValues();
        cv3.put(Contacts.DISPLAY_NAME, "Hot Tamale");
        cv3.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        cv3.put(Email.DATA, "hot@google.com");
        cv3.put(Email.TYPE, Email.TYPE_HOME);
        cv3.putNull(Email.LABEL);

        final ContentValues cv4 = new ContentValues();
        cv4.put(Contacts.DISPLAY_NAME, "Cold Tamago");
        cv4.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        cv4.put(Email.DATA, "eggs@farmers.org");
        cv4.put(Email.TYPE, Email.TYPE_HOME);
        cv4.putNull(Email.LABEL);

        final ContentValues cv5 = new ContentValues();
        cv5.put(Contacts.DISPLAY_NAME, "John Doe");
        cv5.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        cv5.put(Email.DATA, "doeadeer@afemaledeer.com");
        cv5.put(Email.TYPE, Email.TYPE_HOME);
        cv5.putNull(Email.LABEL);

        final ContentValues cv6 = new ContentValues();
        cv6.put(Contacts.DISPLAY_NAME, "John Doe");
        cv6.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        cv6.put(Phone.DATA, "518-354-1111");
        cv6.put(Phone.TYPE, Phone.TYPE_HOME);
        cv6.putNull(Phone.LABEL);

        final Uri filterUri1 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "tamale");

        assertStoredValues(filterUri1, cv1, cv2, cv3);

        final Uri filterUri2 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "hot");
        assertStoredValues(filterUri2, cv1, cv2, cv3);

        final Uri filterUri3 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "tam");
        assertStoredValues(filterUri3, cv1, cv2, cv3, cv4);

        final Uri filterUri4 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "518");
        assertStoredValues(filterUri4, cv5, cv6);

        final Uri filterUri5 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "doe");
        assertStoredValues(filterUri5, cv5, cv6);

        final Uri filterUri6 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI, "51");
        assertStoredValues(filterUri6, cv1, cv2, cv3, cv5, cv6);

        final Uri filterUri7 = Uri.withAppendedPath(Contactables.CONTENT_FILTER_URI,
                "tamale@google");
        assertEquals(0, getCount(filterUri7, null, null));

        final Uri filterUri8 = Contactables.CONTENT_URI;
        assertStoredValues(filterUri8, cv1, cv2, cv3, cv4, cv5, cv6);

        // test VISIBLE_CONTACTS_ONLY boolean parameter
        final Uri filterUri9 = filterUri6.buildUpon().appendQueryParameter(
                Contactables.VISIBLE_CONTACTS_ONLY, "true").build();
        assertStoredValues(filterUri9, cv1, cv2, cv3, cv5, cv6);
        // mark Hot Tamale as invisible - cv1, cv2, and cv3 should no longer be in the cursor
        markInvisible(queryContactId(rawContactId));
        assertStoredValues(filterUri9, cv5, cv6);
    }


    public void testQueryContactData() {
        ContentValues values = new ContentValues();
        long contactId = createContact(values, "John", "Doe",
                "18004664411", "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        values.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(contactUri, values);
    }

    public void testQueryContactWithStatusUpdate() {
        ContentValues values = new ContentValues();
        long contactId = createContact(values, "John", "Doe",
                "18004664411", "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);
        values.put(Contacts.CONTACT_CHAT_CAPABILITY, StatusUpdates.CAPABILITY_HAS_CAMERA);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        values.put(Contacts.TIMES_CONTACTED, 0);

        assertStoredValuesWithProjection(contactUri, values);
    }

    public void testQueryContactFilterByName() {
        ContentValues values = new ContentValues();
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO |
                StatusUpdates.CAPABILITY_HAS_VOICE);

        ContentValues nameValues = new ContentValues();
        nameValues.put(StructuredName.GIVEN_NAME, "Stu");
        nameValues.put(StructuredName.FAMILY_NAME, "Goulash");
        nameValues.put(StructuredName.PHONETIC_FAMILY_NAME, "goo");
        nameValues.put(StructuredName.PHONETIC_GIVEN_NAME, "LASH");
        Uri nameUri = DataUtil.insertStructuredName(mResolver, rawContactId, nameValues);

        long contactId = queryContactId(rawContactId);
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);

        Uri filterUri1 = Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI, "goulash");
        values.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValuesWithProjection(filterUri1, values);

        assertContactFilter(contactId, "goolash");
        assertContactFilter(contactId, "lash");

        assertContactFilterNoResult("goolish");

        // Phonetic name with given/family reversed should not match
        assertContactFilterNoResult("lashgoo");

        nameValues.clear();
        nameValues.put(StructuredName.PHONETIC_FAMILY_NAME, "ga");
        nameValues.put(StructuredName.PHONETIC_GIVEN_NAME, "losh");

        mResolver.update(nameUri, nameValues, null, null);

        assertContactFilter(contactId, "galosh");

        assertContactFilterNoResult("goolish");
    }

    public void testQueryContactFilterByEmailAddress() {
        ContentValues values = new ContentValues();
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO |
                StatusUpdates.CAPABILITY_HAS_VOICE);

        DataUtil.insertStructuredName(mResolver, rawContactId, "James", "Bond");

        long contactId = queryContactId(rawContactId);
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);

        Uri filterUri1 = Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI, "goog411@acme.com");
        values.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValuesWithProjection(filterUri1, values);

        assertContactFilter(contactId, "goog");
        assertContactFilter(contactId, "goog411");
        assertContactFilter(contactId, "goog411@");
        assertContactFilter(contactId, "goog411@acme");
        assertContactFilter(contactId, "goog411@acme.com");

        assertContactFilterNoResult("goog411@acme.combo");
        assertContactFilterNoResult("goog411@le.com");
        assertContactFilterNoResult("goolish");
    }

    public void testQueryContactFilterByPhoneNumber() {
        ContentValues values = new ContentValues();
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO |
                StatusUpdates.CAPABILITY_HAS_VOICE);

        DataUtil.insertStructuredName(mResolver, rawContactId, "James", "Bond");

        long contactId = queryContactId(rawContactId);
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);

        Uri filterUri1 = Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI, "18004664411");
        values.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValuesWithProjection(filterUri1, values);

        assertContactFilter(contactId, "18004664411");
        assertContactFilter(contactId, "1800466");
        assertContactFilter(contactId, "+18004664411");
        assertContactFilter(contactId, "8004664411");

        assertContactFilterNoResult("78004664411");
        assertContactFilterNoResult("18004664412");
        assertContactFilterNoResult("8884664411");
    }

    /**
     * Checks ContactsProvider2 works well with strequent Uris. The provider should return starred
     * contacts.
     */
    public void testQueryContactStrequent() {
        ContentValues values1 = new ContentValues();
        final String email1 = "a@acme.com";
        final String phoneNumber1 = "18004664411";
        final int timesContacted1 = 0;
        createContact(values1, "Noah", "Tever", phoneNumber1,
                email1, StatusUpdates.OFFLINE, timesContacted1, 0, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO);
        final String phoneNumber2 = "18004664412";
        ContentValues values2 = new ContentValues();
        createContact(values2, "Sam", "Times", phoneNumber2,
                "b@acme.com", StatusUpdates.INVISIBLE, 3, 0, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        ContentValues values3 = new ContentValues();
        final String phoneNumber3 = "18004664413";
        final int timesContacted3 = 9;
        createContact(values3, "Lotta", "Calling", phoneNumber3,
                "c@acme.com", StatusUpdates.AWAY, timesContacted3, 0, 0,
                StatusUpdates.CAPABILITY_HAS_VIDEO);
        ContentValues values4 = new ContentValues();
        final long rawContactId4 = createRawContact(values4, "Fay", "Veritt", null,
                "d@acme.com", StatusUpdates.AVAILABLE, 0, 1, 0,
                StatusUpdates.CAPABILITY_HAS_VIDEO | StatusUpdates.CAPABILITY_HAS_VOICE);

        // Starred contacts should be returned. TIMES_CONTACTED should be ignored and only data
        // usage feedback should be used for "frequently contacted" listing.
        assertStoredValues(Contacts.CONTENT_STREQUENT_URI, values4);

        // Send feedback for the 3rd phone number, pretending we called that person via phone.
        sendFeedback(phoneNumber3, DataUsageFeedback.USAGE_TYPE_CALL, values3);

        values3.put(Contacts.TIMES_CONTACTED, 0);

        // After the feedback, 3rd contact should be shown after starred one.
        assertStoredValuesOrderly(Contacts.CONTENT_STREQUENT_URI,
                new ContentValues[] { values4 });

        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);
        // Twice.
        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);

        // After the feedback, 1st and 3rd contacts should be shown after starred one.
        values1.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValuesOrderly(Contacts.CONTENT_STREQUENT_URI,
                new ContentValues[] { values4 });

        // With phone-only parameter, 1st and 4th contacts shouldn't be returned because:
        // 1st: feedbacks are only about email, not about phone call.
        // 4th: it has no phone number though starred.
        Uri phoneOnlyStrequentUri = Contacts.CONTENT_STREQUENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.STREQUENT_PHONE_ONLY, "true")
                .build();
        assertStoredValuesOrderly(phoneOnlyStrequentUri, new ContentValues[] { });
    }

    public void testQueryContactStrequentFrequentOrder() {
        // Prepare test data
        final long rid1 = RawContactUtil.createRawContact(mResolver);
        final long did1 = ContentUris.parseId(insertPhoneNumber(rid1, "1"));
        final long did1e = ContentUris.parseId(insertEmail(rid1, "1@email.com"));

        final long rid2 = RawContactUtil.createRawContact(mResolver);
        final long did2 = ContentUris.parseId(insertPhoneNumber(rid2, "2"));

        final long rid3 = RawContactUtil.createRawContact(mResolver);
        final long did3 = ContentUris.parseId(insertPhoneNumber(rid3, "3"));

        final long rid4 = RawContactUtil.createRawContact(mResolver);
        final long did4 = ContentUris.parseId(insertPhoneNumber(rid4, "4"));

        final long rid5 = RawContactUtil.createRawContact(mResolver);
        final long did5 = ContentUris.parseId(insertPhoneNumber(rid5, "5"));

        final long rid6 = RawContactUtil.createRawContact(mResolver);
        final long did6 = ContentUris.parseId(insertPhoneNumber(rid6, "6"));

        final long rid7 = RawContactUtil.createRawContact(mResolver);
        final long did7 = ContentUris.parseId(insertPhoneNumber(rid7, "7"));

        final long rid8 = RawContactUtil.createRawContact(mResolver);
        final long did8 = ContentUris.parseId(insertPhoneNumber(rid8, "8"));

        final long cid1 = queryContactId(rid1);
        final long cid2 = queryContactId(rid2);
        final long cid3 = queryContactId(rid3);
        final long cid4 = queryContactId(rid4);
        final long cid5 = queryContactId(rid5);
        final long cid6 = queryContactId(rid6);
        final long cid7 = queryContactId(rid7);
        final long cid8 = queryContactId(rid8);

        // Make sure they aren't aggregated.
        EvenMoreAsserts.assertUnique(cid1, cid2, cid3, cid4, cid5, cid6, cid7, cid8);

        // Prepare the clock
        sMockClock.install();

        // We check the timestamp in SQL, which doesn't know about the MockClock.  So we need to
        // use the  actual (roughly) time.

        final long nowInMillis = System.currentTimeMillis();
        final long oneDayAgoInMillis = (nowInMillis - 24L * 60 * 60 * 1000);
        final long fourDaysAgoInMillis = (nowInMillis - 4L * 24 * 60 * 60 * 1000);
        final long eightDaysAgoInMillis = (nowInMillis - 8L * 24 * 60 * 60 * 1000);
        final long fifteenDaysAgoInMillis = (nowInMillis - 15L * 24 * 60 * 60 * 1000);
        // All contacts older than 30 days will not be included in frequents
        final long thirtyOneDaysAgoInMillis = (nowInMillis - 31L * 24 * 60 * 60 * 1000);

        // Contacts in this bucket are considered more than 30 days old
        sMockClock.setCurrentTimeMillis(thirtyOneDaysAgoInMillis);

        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did1, did2);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did1);

        // Contacts in this bucket are considered more than 14 days old
        sMockClock.setCurrentTimeMillis(fifteenDaysAgoInMillis);

        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did3, did4);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did3);

        // Contacts in this bucket are considered more than 7 days old
        sMockClock.setCurrentTimeMillis(eightDaysAgoInMillis);

        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did5, did6);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did5);

        // Contact cid1 again, but it's an email, not a phone call.
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1e);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1e);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1e);

        // Contacts in this bucket are considered more than 3 days old
        sMockClock.setCurrentTimeMillis(fourDaysAgoInMillis);

        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did7);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did7);


        // Contacts in this bucket are considered less than 3 days old
        sMockClock.setCurrentTimeMillis(oneDayAgoInMillis);

        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did8);

        sMockClock.setCurrentTimeMillis(nowInMillis);

        // Check the order -- The regular frequent, which is contact based.
        // Note because we contacted cid1 8 days ago, it's been contacted 3 times, so it comes
        // before cid5 and cid6, which were contacted at the same time.
        // cid2 will not show up because it was contacted more than 30 days ago

        assertStoredValuesOrderly(Contacts.CONTENT_STREQUENT_URI);

        // Check the order -- phone only frequent, which is data based.
        // Note this is based on data, and only looks at phone numbers, so the order is different
        // now.
        // did1, did2 will not show up because they were used to make calls more than 30 days ago.
        assertStoredValuesOrderly(Contacts.CONTENT_STREQUENT_URI.buildUpon()
                    .appendQueryParameter(ContactsContract.STREQUENT_PHONE_ONLY, "1").build());
    }

    /**
     * Checks ContactsProvider2 works well with frequent Uri. The provider should return frequently
     * contacted person ordered by number of times contacted.
     */
    public void testQueryContactFrequent() {
        ContentValues values1 = new ContentValues();
        final String email1 = "a@acme.com";
        createContact(values1, "Noah", "Tever", "18004664411",
                email1, StatusUpdates.OFFLINE, 0, 0, 0, 0);
        ContentValues values2 = new ContentValues();
        final String email2 = "b@acme.com";
        createContact(values2, "Sam", "Times", "18004664412",
                email2, StatusUpdates.INVISIBLE, 0, 0, 0, 0);
        ContentValues values3 = new ContentValues();
        final String phoneNumber3 = "18004664413";
        final long contactId3 = createContact(values3, "Lotta", "Calling", phoneNumber3,
                "c@acme.com", StatusUpdates.AWAY, 0, 1, 0, 0);
        ContentValues values4 = new ContentValues();
        createContact(values4, "Fay", "Veritt", "18004664414",
                "d@acme.com", StatusUpdates.AVAILABLE, 0, 1, 0, 0);

        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);

        assertStoredValues(Contacts.CONTENT_FREQUENT_URI);

        // Pretend email was sent to the address twice.
        sendFeedback(email2, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values2);
        sendFeedback(email2, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values2);

        values1.put(Contacts.TIMES_CONTACTED, 0);
        values2.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI);

        for (int i = 0; i < 10; i++) {
            sendFeedback(phoneNumber3, DataUsageFeedback.USAGE_TYPE_CALL, values3);
        }

        values3.put(Contacts.TIMES_CONTACTED, 0);

        assertStoredValues(Contacts.CONTENT_FREQUENT_URI);


        // Test it works with selection/selectionArgs
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI,
                Contacts.STARRED + "=?", new String[] {"0"}
                );
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI,
                Contacts.STARRED + "=?", new String[] {"1"}
                );

        values3.put(Contacts.STARRED, 0);
        assertEquals(1,
                mResolver.update(Uri.withAppendedPath(Contacts.CONTENT_URI,
                        String.valueOf(contactId3)),
                values3, null, null));
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI,
                Contacts.STARRED + "=?", new String[] {"0"}
                );
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI,
                Contacts.STARRED + "=?", new String[] {"1"}
                );
    }

    public void testQueryContactFrequentExcludingInvisible() {
        ContentValues values1 = new ContentValues();
        final String email1 = "a@acme.com";
        final long cid1 = createContact(values1, "Noah", "Tever", "18004664411",
                email1, StatusUpdates.OFFLINE, 0, 0, 0, 0);
        ContentValues values2 = new ContentValues();
        final String email2 = "b@acme.com";
        final long cid2 = createContact(values2, "Sam", "Times", "18004664412",
                email2, StatusUpdates.INVISIBLE, 0, 0, 0, 0);

        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);
        sendFeedback(email2, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values2);

        // First, we have two contacts in frequent.
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI);

        // Contact 2 goes invisible.
        markInvisible(cid2);

        // Now we have only 1 frequent.
        assertStoredValues(Contacts.CONTENT_FREQUENT_URI);

    }

    public void testQueryDataUsageStat() {
        // Now all data usage stats are zero as of Q.

        ContentValues values1 = new ContentValues();
        final String email1 = "a@acme.com";
        final long cid1 = createContact(values1, "Noah", "Tever", "18004664411",
                email1, StatusUpdates.OFFLINE, 0, 0, 0, 0);

        sMockClock.install();
        sMockClock.setCurrentTimeMillis(100);

        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);

        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        sMockClock.setCurrentTimeMillis(86400 + 123);
        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_LONG_TEXT, values1);

        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        sMockClock.setCurrentTimeMillis(86400 * 3 + 123);
        for (int i = 0; i < 11; i++) {
            sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_SHORT_TEXT, values1);
        }

        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        final Uri dataUriWithUsageTypeLongText = Data.CONTENT_URI.buildUpon().appendQueryParameter(
                DataUsageFeedback.USAGE_TYPE, DataUsageFeedback.USAGE_TYPE_LONG_TEXT).build();

        assertDataUsageZero(dataUriWithUsageTypeLongText, "a@acme.com");

        sMockClock.setCurrentTimeMillis(86400 * 4 + 123);
        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_CALL, values1);
        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_CALL, values1);
        sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_CALL, values1);

        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        sMockClock.setCurrentTimeMillis(86400 * 5 + 123);
        for (int i = 0; i < 10; i++) {
            sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_CALL, values1);
        }
        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        sMockClock.setCurrentTimeMillis(86400 * 6 + 123);
        for (int i = 0; i < 10; i++) {
            sendFeedback(email1, DataUsageFeedback.USAGE_TYPE_CALL, values1);
        }
        assertDataUsageZero(Data.CONTENT_URI, "a@acme.com");

        final Uri dataUriWithUsageTypeCall = Data.CONTENT_URI.buildUpon().appendQueryParameter(
                DataUsageFeedback.USAGE_TYPE, DataUsageFeedback.USAGE_TYPE_CALL).build();

        assertDataUsageZero(dataUriWithUsageTypeCall, "a@acme.com");
    }

    public void testQueryContactGroup() {
        long groupId = createGroup(null, "testGroup", "Test Group");

        ContentValues values1 = new ContentValues();
        createContact(values1, "Best", "West", "18004664411",
                "west@acme.com", StatusUpdates.OFFLINE, 0, 0, groupId,
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        ContentValues values2 = new ContentValues();
        createContact(values2, "Rest", "East", "18004664422",
                "east@acme.com", StatusUpdates.AVAILABLE, 0, 0, 0,
                StatusUpdates.CAPABILITY_HAS_VOICE);

        Uri filterUri1 = Uri.withAppendedPath(Contacts.CONTENT_GROUP_URI, "Test Group");
        Cursor c = mResolver.query(filterUri1, null, null, null, Contacts._ID);
        assertEquals(1, c.getCount());
        c.moveToFirst();
        dumpCursor(c);
        assertCursorValues(c, values1);
        c.close();

        Uri filterUri2 = Uri.withAppendedPath(Contacts.CONTENT_GROUP_URI, "Test Group");
        c = mResolver.query(filterUri2, null, Contacts.DISPLAY_NAME + "=?",
                new String[] { "Best West" }, Contacts._ID);
        assertEquals(1, c.getCount());
        c.close();

        Uri filterUri3 = Uri.withAppendedPath(Contacts.CONTENT_GROUP_URI, "Next Group");
        c = mResolver.query(filterUri3, null, null, null, Contacts._ID);
        assertEquals(0, c.getCount());
        c.close();
    }

    private void expectNoSecurityException(String failureMessage, Uri uri, String[] projection,
            String selection, String[] selectionArgs, String sortOrder) {
        Cursor c = null;
        try {
            c = mResolver.query(uri, projection, selection, selectionArgs, sortOrder);
        } catch (SecurityException expected) {
            fail(failureMessage);
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    private void expectSecurityException(String failureMessage, Uri uri, String[] projection,
            String selection, String[] selectionArgs, String sortOrder) {
        Cursor c = null;
        try {
            c = mResolver.query(uri, projection, selection, selectionArgs, sortOrder);
            fail(failureMessage);
        } catch (SecurityException expected) {
            // The security exception is expected to occur because we're missing a permission.
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    public void testQueryProfileWithoutPermission() {
        createBasicProfileContact(new ContentValues());

        // Case 1: Retrieving profile contact.
        expectNoSecurityException(
                "Querying for the profile without READ_PROFILE access should succeed.",
                Profile.CONTENT_URI, null, null, null, Contacts._ID);

        // Case 2: Retrieving profile data.
        expectNoSecurityException(
                "Querying for the profile data without READ_PROFILE access should succeed.",
                Profile.CONTENT_URI.buildUpon().appendPath("data").build(),
                null, null, null, Contacts._ID);

        // Case 3: Retrieving profile entities.
        expectNoSecurityException(
                "Querying for the profile entities without READ_PROFILE access should succeed.",
                Profile.CONTENT_URI.buildUpon()
                        .appendPath("entities").build(), null, null, null, Contacts._ID);
    }

    public void testQueryProfileByContactIdWithoutReadPermission() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());
        long profileContactId = queryContactId(profileRawContactId);

        // A query for the profile contact by ID should not require READ_PROFILE.
        expectNoSecurityException(
                "Querying for the profile by contact ID without READ_PROFILE access should succeed",
                ContentUris.withAppendedId(Contacts.CONTENT_URI, profileContactId),
                null, null, null, Contacts._ID);
    }

    public void testQueryProfileByRawContactIdWithoutReadPermission() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        expectNoSecurityException(
                "Querying for the raw contact profile without READ_PROFILE access should succeed.",
                ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                        profileRawContactId), null, null, null, RawContacts._ID);
    }

    public void testQueryProfileRawContactWithoutReadPermission() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        // Case 1: Retrieve the overall raw contact set for the profile.
        expectNoSecurityException(
                "Querying for the raw contact profile without READ_PROFILE access should succeed.",
                Profile.CONTENT_RAW_CONTACTS_URI, null, null, null, null);

        // Case 2: Retrieve the raw contact profile data for the inserted raw contact ID.
        expectNoSecurityException(
                "Querying for the raw profile data without READ_PROFILE access should succeed.",
                ContentUris.withAppendedId(
                        Profile.CONTENT_RAW_CONTACTS_URI, profileRawContactId).buildUpon()
                        .appendPath("data").build(), null, null, null, null);

        // Case 3: Retrieve the raw contact profile entity for the inserted raw contact ID.
        expectNoSecurityException(
                "Querying for the raw profile entities without READ_PROFILE access should succeed.",
                ContentUris.withAppendedId(
                        Profile.CONTENT_RAW_CONTACTS_URI, profileRawContactId).buildUpon()
                        .appendPath("entity").build(), null, null, null, null);
    }

    public void testQueryProfileDataByDataIdWithoutReadPermission() {
        createBasicProfileContact(new ContentValues());
        Cursor c = mResolver.query(Profile.CONTENT_URI.buildUpon().appendPath("data").build(),
                new String[]{Data._ID, Data.MIMETYPE}, null, null, null);
        assertEquals(4, c.getCount());  // Photo, phone, email, name.
        c.moveToFirst();
        long profileDataId = c.getLong(0);
        c.close();

        expectNoSecurityException(
                "Querying for the data in the profile without READ_PROFILE access should succeed.",
                ContentUris.withAppendedId(Data.CONTENT_URI, profileDataId),
                null, null, null, null);
    }

    public void testQueryProfileDataWithoutReadPermission() {
        createBasicProfileContact(new ContentValues());

        expectNoSecurityException(
                "Querying for the data in the profile without READ_PROFILE access should succeed.",
                Profile.CONTENT_URI.buildUpon().appendPath("data").build(),
                null, null, null, null);
    }

    public void testInsertProfileWithoutWritePermission() {
        // Creating a non-profile contact should be fine.
        createBasicNonProfileContact(new ContentValues());

        try {
            createBasicProfileContact(new ContentValues());
        } catch (SecurityException expected) {
            fail("Creating a profile contact should not require WRITE_PROFILE access.");
        }
    }

    public void testInsertProfileDataWithoutWritePermission() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        try {
            insertEmail(profileRawContactId, "foo@bar.net", false);
        } catch (SecurityException expected) {
            fail("Inserting data into a profile contact should not require WRITE_PROFILE access.");
        }
    }

    public void testUpdateDataDoesNotRequireProfilePermission() {
        // Create a non-profile contact.
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver, "Domo", "Arigato");
        long dataId = getStoredLongValue(Data.CONTENT_URI,
                Data.RAW_CONTACT_ID + "=? AND " + Data.MIMETYPE + "=?",
                new String[]{String.valueOf(rawContactId), StructuredName.CONTENT_ITEM_TYPE},
                Data._ID);

        // Updates its name using a selection.
        ContentValues values = new ContentValues();
        values.put(StructuredName.GIVEN_NAME, "Bob");
        values.put(StructuredName.FAMILY_NAME, "Blob");
        mResolver.update(Data.CONTENT_URI, values, Data._ID + "=?",
                new String[]{String.valueOf(dataId)});

        // Check that the update went through.
        assertStoredValues(ContentUris.withAppendedId(Data.CONTENT_URI, dataId), values);
    }

    public void testQueryContactThenProfile() {
        ContentValues profileValues = new ContentValues();
        long profileRawContactId = createBasicProfileContact(profileValues);
        long profileContactId = queryContactId(profileRawContactId);

        ContentValues nonProfileValues = new ContentValues();
        long nonProfileRawContactId = createBasicNonProfileContact(nonProfileValues);
        long nonProfileContactId = queryContactId(nonProfileRawContactId);

        nonProfileValues.put(Contacts.TIMES_CONTACTED, 0);
        profileValues.put(Contacts.TIMES_CONTACTED, 0);

        assertStoredValues(Contacts.CONTENT_URI, nonProfileValues);

        assertStoredValues(Profile.CONTENT_URI, profileValues);
    }

    public void testQueryContactExcludeProfile() {
        // Create a profile contact (it should not be returned by the general contact URI).
        createBasicProfileContact(new ContentValues());

        // Create a non-profile contact - this should be returned.
        ContentValues nonProfileValues = new ContentValues();
        createBasicNonProfileContact(nonProfileValues);
        nonProfileValues.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(Contacts.CONTENT_URI, new ContentValues[] {nonProfileValues});
    }

    public void testQueryProfile() {
        ContentValues profileValues = new ContentValues();
        createBasicProfileContact(profileValues);

        profileValues.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(Profile.CONTENT_URI, profileValues);
    }

    private ContentValues[] getExpectedProfileDataValues() {
        // Expected photo data values (only field is the photo BLOB, which we can't check).
        ContentValues photoRow = new ContentValues();
        photoRow.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);

        // Expected phone data values.
        ContentValues phoneRow = new ContentValues();
        phoneRow.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        phoneRow.put(Phone.NUMBER, "18005554411");

        // Expected email data values.
        ContentValues emailRow = new ContentValues();
        emailRow.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        emailRow.put(Email.ADDRESS, "mia.prophyl@acme.com");

        // Expected name data values.
        ContentValues nameRow = new ContentValues();
        nameRow.put(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);
        nameRow.put(StructuredName.DISPLAY_NAME, "Mia Prophyl");
        nameRow.put(StructuredName.GIVEN_NAME, "Mia");
        nameRow.put(StructuredName.FAMILY_NAME, "Prophyl");

        return new ContentValues[]{photoRow, phoneRow, emailRow, nameRow};
    }

    public void testQueryProfileData() {
        createBasicProfileContact(new ContentValues());

        assertStoredValues(Profile.CONTENT_URI.buildUpon().appendPath("data").build(),
                getExpectedProfileDataValues());
    }

    public void testQueryProfileEntities() {
        createBasicProfileContact(new ContentValues());

        assertStoredValues(Profile.CONTENT_URI.buildUpon().appendPath("entities").build(),
                getExpectedProfileDataValues());
    }

    public void testQueryRawProfile() {
        ContentValues profileValues = new ContentValues();
        createBasicProfileContact(profileValues);

        // The raw contact view doesn't include the photo ID.
        profileValues.remove(Contacts.PHOTO_ID);
        profileValues.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(Profile.CONTENT_RAW_CONTACTS_URI, profileValues);
    }

    public void testQueryRawProfileById() {
        ContentValues profileValues = new ContentValues();
        long profileRawContactId = createBasicProfileContact(profileValues);

        // The raw contact view doesn't include the photo ID.
        profileValues.remove(Contacts.PHOTO_ID);
        profileValues.put(Contacts.TIMES_CONTACTED, 0);
        assertStoredValues(ContentUris.withAppendedId(
                Profile.CONTENT_RAW_CONTACTS_URI, profileRawContactId), profileValues);
    }

    public void testQueryRawProfileData() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        assertStoredValues(ContentUris.withAppendedId(
                Profile.CONTENT_RAW_CONTACTS_URI, profileRawContactId).buildUpon()
                .appendPath("data").build(), getExpectedProfileDataValues());
    }

    public void testQueryRawProfileEntity() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        assertStoredValues(ContentUris.withAppendedId(
                Profile.CONTENT_RAW_CONTACTS_URI, profileRawContactId).buildUpon()
                .appendPath("entity").build(), getExpectedProfileDataValues());
    }

    public void testQueryDataForProfile() {
        createBasicProfileContact(new ContentValues());

        assertStoredValues(Profile.CONTENT_URI.buildUpon().appendPath("data").build(),
                getExpectedProfileDataValues());
    }

    public void testUpdateProfileRawContact() {
        createBasicProfileContact(new ContentValues());
        ContentValues updatedValues = new ContentValues();
        updatedValues.put(RawContacts.SEND_TO_VOICEMAIL, 0);
        updatedValues.put(RawContacts.CUSTOM_RINGTONE, "rachmaninoff3");
        updatedValues.put(RawContacts.STARRED, 1);
        mResolver.update(Profile.CONTENT_RAW_CONTACTS_URI, updatedValues, null, null);

        assertStoredValues(Profile.CONTENT_RAW_CONTACTS_URI, updatedValues);
    }

    public void testInsertProfileWithDataSetTriggersAccountCreation() {
        // Check that we have no profile raw contacts.
        assertStoredValues(Profile.CONTENT_RAW_CONTACTS_URI, new ContentValues[]{});

        // Insert a profile record with a new data set.
        Account account = new Account("a", "b");
        String dataSet = "c";
        Uri profileUri = TestUtil.maybeAddAccountQueryParameters(Profile.CONTENT_RAW_CONTACTS_URI,
                account)
                .buildUpon().appendQueryParameter(RawContacts.DATA_SET, dataSet).build();
        ContentValues values = new ContentValues();
        long rawContactId = ContentUris.parseId(mResolver.insert(profileUri, values));
        values.put(RawContacts._ID, rawContactId);

        // Check that querying for the profile gets the created raw contact.
        assertStoredValues(Profile.CONTENT_RAW_CONTACTS_URI, values);
    }

    public void testLoadProfilePhoto() throws IOException {
        long rawContactId = createBasicProfileContact(new ContentValues());
        insertPhoto(rawContactId, R.drawable.earth_normal);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.THUMBNAIL),
                Contacts.openContactPhotoInputStream(mResolver, Profile.CONTENT_URI, false));
    }

    public void testLoadProfileDisplayPhoto() throws IOException {
        long rawContactId = createBasicProfileContact(new ContentValues());
        insertPhoto(rawContactId, R.drawable.earth_normal);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                Contacts.openContactPhotoInputStream(mResolver, Profile.CONTENT_URI, true));
    }

    public void testPhonesWithStatusUpdate() {

        ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        Uri photoUri = insertPhoto(rawContactId);
        long photoId = ContentUris.parseId(photoUri);
        insertPhoneNumber(rawContactId, "18004664411");
        insertPhoneNumber(rawContactId, "18004664412");
        insertEmail(rawContactId, "goog411@acme.com");
        insertEmail(rawContactId, "goog412@acme.com");

        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "goog411@acme.com",
                StatusUpdates.INVISIBLE, "Bad",
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "goog412@acme.com",
                StatusUpdates.AVAILABLE, "Good",
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VOICE);
        long contactId = queryContactId(rawContactId);

        Uri uri = Data.CONTENT_URI;

        Cursor c = mResolver.query(uri, null, RawContacts.CONTACT_ID + "=" + contactId + " AND "
                + Data.MIMETYPE + "='" + Phone.CONTENT_ITEM_TYPE + "'", null, Phone.NUMBER);
        assertEquals(2, c.getCount());

        c.moveToFirst();

        values.clear();
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.AVAILABLE);
        values.put(Contacts.CONTACT_STATUS, "Bad");
        values.put(Contacts.DISPLAY_NAME, "John Doe");
        values.put(Phone.NUMBER, "18004664411");
        values.putNull(Phone.LABEL);
        values.put(RawContacts.CONTACT_ID, contactId);
        assertCursorValues(c, values);

        c.moveToNext();

        values.clear();
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.AVAILABLE);
        values.put(Contacts.CONTACT_STATUS, "Bad");
        values.put(Contacts.DISPLAY_NAME, "John Doe");
        values.put(Phone.NUMBER, "18004664412");
        values.putNull(Phone.LABEL);
        values.put(RawContacts.CONTACT_ID, contactId);
        assertCursorValues(c, values);

        c.close();
    }

    public void testGroupQuery() {
        Account account1 = new Account("a", "b");
        Account account2 = new Account("c", "d");
        long groupId1 = createGroup(account1, "e", "f");
        long groupId2 = createGroup(account2, "g", "h");
        Uri uri1 = TestUtil.maybeAddAccountQueryParameters(Groups.CONTENT_URI, account1);
        Uri uri2 = TestUtil.maybeAddAccountQueryParameters(Groups.CONTENT_URI, account2);
        assertEquals(1, getCount(uri1, null, null));
        assertEquals(1, getCount(uri2, null, null));
        assertStoredValue(uri1, Groups._ID + "=" + groupId1, null, Groups._ID, groupId1) ;
        assertStoredValue(uri2, Groups._ID + "=" + groupId2, null, Groups._ID, groupId2) ;
    }

    public void testGroupInsert() {
        ContentValues values = new ContentValues();

        values.put(Groups.ACCOUNT_NAME, "a");
        values.put(Groups.ACCOUNT_TYPE, "b");
        values.put(Groups.DATA_SET, "ds");
        values.put(Groups.SOURCE_ID, "c");
        values.put(Groups.VERSION, 42);
        values.put(Groups.GROUP_VISIBLE, 1);
        values.put(Groups.TITLE, "d");
        values.put(Groups.TITLE_RES, 1234);
        values.put(Groups.NOTES, "e");
        values.put(Groups.RES_PACKAGE, "f");
        values.put(Groups.SYSTEM_ID, "g");
        values.put(Groups.DELETED, 1);
        values.put(Groups.SYNC1, "h");
        values.put(Groups.SYNC2, "i");
        values.put(Groups.SYNC3, "j");
        values.put(Groups.SYNC4, "k");

        Uri rowUri = mResolver.insert(Groups.CONTENT_URI, values);

        values.put(Groups.DIRTY, 1);
        assertStoredValues(rowUri, values);
    }

    public void testGroupCreationAfterMembershipInsert() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri groupMembershipUri = insertGroupMembership(rawContactId1, "gsid1");

        long groupId = assertSingleGroup(NO_LONG, mAccount, "gsid1", null);
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                rawContactId1, groupId, "gsid1");
    }

    public void testGroupReuseAfterMembershipInsert() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");
        Uri groupMembershipUri = insertGroupMembership(rawContactId1, "gsid1");

        assertSingleGroup(groupId1, mAccount, "gsid1", "title1");
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                rawContactId1, groupId1, "gsid1");
    }

    public void testGroupInsertFailureOnGroupIdConflict() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");

        ContentValues values = new ContentValues();
        values.put(GroupMembership.RAW_CONTACT_ID, rawContactId1);
        values.put(GroupMembership.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
        values.put(GroupMembership.GROUP_SOURCE_ID, "gsid1");
        values.put(GroupMembership.GROUP_ROW_ID, groupId1);
        try {
            mResolver.insert(Data.CONTENT_URI, values);
            fail("the insert was expected to fail, but it succeeded");
        } catch (IllegalArgumentException e) {
            // this was expected
        }
    }

    public void testGroupDelete_byAccountSelection() {
        final Account account1 = new Account("accountName1", "accountType1");
        final Account account2 = new Account("accountName2", "accountType2");

        final long groupId1 = createGroup(account1, "sourceId1", "title1");
        final long groupId2 = createGroup(account2, "sourceId2", "title2");
        final long groupId3 = createGroup(account2, "sourceId3", "title3");

        final int numDeleted = mResolver.delete(Groups.CONTENT_URI,
                Groups.ACCOUNT_NAME + "=? AND " + Groups.ACCOUNT_TYPE + "=?",
                new String[]{account2.name, account2.type});
        assertEquals(2, numDeleted);

        ContentValues v1 = new ContentValues();
        v1.put(Groups._ID, groupId1);
        v1.put(Groups.DELETED, 0);

        ContentValues v2 = new ContentValues();
        v2.put(Groups._ID, groupId2);
        v2.put(Groups.DELETED, 1);

        ContentValues v3 = new ContentValues();
        v3.put(Groups._ID, groupId3);
        v3.put(Groups.DELETED, 1);

        assertStoredValues(Groups.CONTENT_URI, new ContentValues[] { v1, v2, v3 });
    }

    public void testGroupDelete_byAccountParam() {
        final Account account1 = new Account("accountName1", "accountType1");
        final Account account2 = new Account("accountName2", "accountType2");

        final long groupId1 = createGroup(account1, "sourceId1", "title1");
        final long groupId2 = createGroup(account2, "sourceId2", "title2");
        final long groupId3 = createGroup(account2, "sourceId3", "title3");

        final int numDeleted = mResolver.delete(
                Groups.CONTENT_URI.buildUpon()
                    .appendQueryParameter(Groups.ACCOUNT_NAME, account2.name)
                    .appendQueryParameter(Groups.ACCOUNT_TYPE, account2.type)
                    .build(),
                null, null);
        assertEquals(2, numDeleted);

        ContentValues v1 = new ContentValues();
        v1.put(Groups._ID, groupId1);
        v1.put(Groups.DELETED, 0);

        ContentValues v2 = new ContentValues();
        v2.put(Groups._ID, groupId2);
        v2.put(Groups.DELETED, 1);

        ContentValues v3 = new ContentValues();
        v3.put(Groups._ID, groupId3);
        v3.put(Groups.DELETED, 1);

        assertStoredValues(Groups.CONTENT_URI, new ContentValues[] { v1, v2, v3 });
    }

    public void testGroupSummaryQuery() {
        final Account account1 = new Account("accountName1", "accountType1");
        final Account account2 = new Account("accountName2", "accountType2");
        final long groupId1 = createGroup(account1, "sourceId1", "title1");
        final long groupId2 = createGroup(account2, "sourceId2", "title2");
        final long groupId3 = createGroup(account2, "sourceId3", "title3");

        // Prepare raw contact id not used at all, to test group summary uri won't be confused
        // with it.
        final long rawContactId0 = RawContactUtil.createRawContactWithName(mResolver, "firstName0",
                "lastName0");

        final long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "firstName1",
                "lastName1");
        insertEmail(rawContactId1, "address1@email.com");
        insertGroupMembership(rawContactId1, groupId1);

        final long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "firstName2",
                "lastName2");
        insertEmail(rawContactId2, "address2@email.com");
        insertPhoneNumber(rawContactId2, "222-222-2222");
        insertGroupMembership(rawContactId2, groupId1);

        ContentValues v1 = new ContentValues();
        v1.put(Groups._ID, groupId1);
        v1.put(Groups.TITLE, "title1");
        v1.put(Groups.SOURCE_ID, "sourceId1");
        v1.put(Groups.ACCOUNT_NAME, account1.name);
        v1.put(Groups.ACCOUNT_TYPE, account1.type);
        v1.put(Groups.SUMMARY_COUNT, 2);
        v1.put(Groups.SUMMARY_WITH_PHONES, 1);

        ContentValues v2 = new ContentValues();
        v2.put(Groups._ID, groupId2);
        v2.put(Groups.TITLE, "title2");
        v2.put(Groups.SOURCE_ID, "sourceId2");
        v2.put(Groups.ACCOUNT_NAME, account2.name);
        v2.put(Groups.ACCOUNT_TYPE, account2.type);
        v2.put(Groups.SUMMARY_COUNT, 0);
        v2.put(Groups.SUMMARY_WITH_PHONES, 0);

        ContentValues v3 = new ContentValues();
        v3.put(Groups._ID, groupId3);
        v3.put(Groups.TITLE, "title3");
        v3.put(Groups.SOURCE_ID, "sourceId3");
        v3.put(Groups.ACCOUNT_NAME, account2.name);
        v3.put(Groups.ACCOUNT_TYPE, account2.type);
        v3.put(Groups.SUMMARY_COUNT, 0);
        v3.put(Groups.SUMMARY_WITH_PHONES, 0);

        assertStoredValues(Groups.CONTENT_SUMMARY_URI, new ContentValues[] { v1, v2, v3 });

        // Now rawContactId1 has two phone numbers.
        insertPhoneNumber(rawContactId1, "111-111-1111");
        insertPhoneNumber(rawContactId1, "111-111-1112");
        // Result should reflect it correctly (don't count phone numbers but raw contacts)
        v1.put(Groups.SUMMARY_WITH_PHONES, v1.getAsInteger(Groups.SUMMARY_WITH_PHONES) + 1);
        assertStoredValues(Groups.CONTENT_SUMMARY_URI, new ContentValues[] { v1, v2, v3 });

        // Introduce new raw contact, pretending the user added another info.
        final long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "firstName3",
                "lastName3");
        insertEmail(rawContactId3, "address3@email.com");
        insertPhoneNumber(rawContactId3, "333-333-3333");
        insertGroupMembership(rawContactId3, groupId2);
        v2.put(Groups.SUMMARY_COUNT, v2.getAsInteger(Groups.SUMMARY_COUNT) + 1);
        v2.put(Groups.SUMMARY_WITH_PHONES, v2.getAsInteger(Groups.SUMMARY_WITH_PHONES) + 1);

        assertStoredValues(Groups.CONTENT_SUMMARY_URI, new ContentValues[] { v1, v2, v3 });

        final Uri uri = Groups.CONTENT_SUMMARY_URI;

        // TODO Once SUMMARY_GROUP_COUNT_PER_ACCOUNT is supported remove all the if(false).
        if (false) {
            v1.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 1);
            v2.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 2);
            v3.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 2);
        } else {
            v1.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 0);
            v2.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 0);
            v3.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 0);
        }
        assertStoredValues(uri, new ContentValues[] { v1, v2, v3 });

        // Introduce another group in account1, testing SUMMARY_GROUP_COUNT_PER_ACCOUNT correctly
        // reflects the change.
        final long groupId4 = createGroup(account1, "sourceId4", "title4");
        if (false) {
            v1.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT,
                    v1.getAsInteger(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT) + 1);
        } else {
            v1.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 0);
        }
        ContentValues v4 = new ContentValues();
        v4.put(Groups._ID, groupId4);
        v4.put(Groups.TITLE, "title4");
        v4.put(Groups.SOURCE_ID, "sourceId4");
        v4.put(Groups.ACCOUNT_NAME, account1.name);
        v4.put(Groups.ACCOUNT_TYPE, account1.type);
        v4.put(Groups.SUMMARY_COUNT, 0);
        v4.put(Groups.SUMMARY_WITH_PHONES, 0);
        if (false) {
            v4.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT,
                    v1.getAsInteger(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT));
        } else {
            v4.put(Groups.SUMMARY_GROUP_COUNT_PER_ACCOUNT, 0);
        }
        assertStoredValues(uri, new ContentValues[] { v1, v2, v3, v4 });

        // We change the tables dynamically according to the requested projection.
        // Make sure the SUMMARY_COUNT column exists
        v1.clear();
        v1.put(Groups.SUMMARY_COUNT, 2);
        v2.clear();
        v2.put(Groups.SUMMARY_COUNT, 1);
        v3.clear();
        v3.put(Groups.SUMMARY_COUNT, 0);
        v4.clear();
        v4.put(Groups.SUMMARY_COUNT, 0);
        assertStoredValuesWithProjection(uri, new ContentValues[] { v1, v2, v3, v4 });
    }

    public void testSettingsQuery() {
        Account account1 = new Account("a", "b");
        Account account2 = new Account("c", "d");
        AccountWithDataSet account3 = new AccountWithDataSet("e", "f", "plus");
        createSettings(account1, "0", "0");
        createSettings(account2, "1", "1");
        createSettings(account3, "1", "0");
        Uri uri1 = TestUtil.maybeAddAccountQueryParameters(Settings.CONTENT_URI, account1);
        Uri uri2 = TestUtil.maybeAddAccountQueryParameters(Settings.CONTENT_URI, account2);
        Uri uri3 = Settings.CONTENT_URI.buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, account3.getAccountName())
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, account3.getAccountType())
                .appendQueryParameter(RawContacts.DATA_SET, account3.getDataSet())
                .build();
        assertEquals(1, getCount(uri1, null, null));
        assertEquals(1, getCount(uri2, null, null));
        assertEquals(1, getCount(uri3, null, null));
        assertStoredValue(uri1, Settings.SHOULD_SYNC, "0") ;
        assertStoredValue(uri1, Settings.UNGROUPED_VISIBLE, "0");
        assertStoredValue(uri2, Settings.SHOULD_SYNC, "1") ;
        assertStoredValue(uri2, Settings.UNGROUPED_VISIBLE, "1");
        assertStoredValue(uri3, Settings.SHOULD_SYNC, "1");
        assertStoredValue(uri3, Settings.UNGROUPED_VISIBLE, "0");
    }

    public void testSettingsInsertionPreventsDuplicates() {
        Account account1 = new Account("a", "b");
        AccountWithDataSet account2 = new AccountWithDataSet("c", "d", "plus");
        createSettings(account1, "0", "0");
        createSettings(account2, "1", "1");

        // Now try creating the settings rows again.  It should update the existing settings rows.
        createSettings(account1, "1", "0");
        assertStoredValue(Settings.CONTENT_URI,
                Settings.ACCOUNT_NAME + "=? AND " + Settings.ACCOUNT_TYPE + "=?",
                new String[] {"a", "b"}, Settings.SHOULD_SYNC, "1");

        createSettings(account2, "0", "1");
        assertStoredValue(Settings.CONTENT_URI,
                Settings.ACCOUNT_NAME + "=? AND " + Settings.ACCOUNT_TYPE + "=? AND " +
                Settings.DATA_SET + "=?",
                new String[] {"c", "d", "plus"}, Settings.SHOULD_SYNC, "0");
    }

    public void testDisplayNameParsingWhenPartsUnspecified() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertStructuredName(rawContactId, "Mr.", "John", "Kevin", "von Smith", "Jr.");
    }

    public void testDisplayNameParsingWhenPartsAreNull() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        values.putNull(StructuredName.GIVEN_NAME);
        values.putNull(StructuredName.FAMILY_NAME);
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
        assertStructuredName(rawContactId, "Mr.", "John", "Kevin", "von Smith", "Jr.");
    }

    public void testDisplayNameParsingWhenPartsSpecified() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        values.put(StructuredName.FAMILY_NAME, "Johnson");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        assertStructuredName(rawContactId, null, null, null, "Johnson", null);
    }

    public void testContactWithoutPhoneticName() {
        ContactLocaleUtils.setLocaleForTest(Locale.ENGLISH);
        final long rawContactId = RawContactUtil.createRawContact(mResolver, null);

        ContentValues values = new ContentValues();
        values.put(StructuredName.PREFIX, "Mr");
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.MIDDLE_NAME, "K.");
        values.put(StructuredName.FAMILY_NAME, "Doe");
        values.put(StructuredName.SUFFIX, "Jr.");
        Uri dataUri = DataUtil.insertStructuredName(mResolver, rawContactId, values);

        values.clear();
        values.put(RawContacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(RawContacts.DISPLAY_NAME_PRIMARY, "Mr John K. Doe, Jr.");
        values.put(RawContacts.DISPLAY_NAME_ALTERNATIVE, "Mr Doe, John K., Jr.");
        values.putNull(RawContacts.PHONETIC_NAME);
        values.put(RawContacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(RawContacts.SORT_KEY_PRIMARY, "John K. Doe, Jr.");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_PRIMARY, "J");
        values.put(RawContacts.SORT_KEY_ALTERNATIVE, "Doe, John K., Jr.");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "D");

        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertStoredValues(rawContactUri, values);

        values.clear();
        values.put(Contacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(Contacts.DISPLAY_NAME_PRIMARY, "Mr John K. Doe, Jr.");
        values.put(Contacts.DISPLAY_NAME_ALTERNATIVE, "Mr Doe, John K., Jr.");
        values.putNull(Contacts.PHONETIC_NAME);
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(Contacts.SORT_KEY_PRIMARY, "John K. Doe, Jr.");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "J");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "Doe, John K., Jr.");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "D");

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId));
        assertStoredValues(contactUri, values);

        // The same values should be available through a join with Data
        assertStoredValues(dataUri, values);
    }

    public void testContactWithChineseName() {
        if (!hasChineseCollator()) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.SIMPLIFIED_CHINESE);

        long rawContactId = RawContactUtil.createRawContact(mResolver, null);

        ContentValues values = new ContentValues();
        // "DUAN \u6BB5 XIAO \u5C0F TAO \u6D9B"
        values.put(StructuredName.DISPLAY_NAME, "\u6BB5\u5C0F\u6D9B");
        Uri dataUri = DataUtil.insertStructuredName(mResolver, rawContactId, values);

        values.clear();
        values.put(RawContacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(RawContacts.DISPLAY_NAME_PRIMARY, "\u6BB5\u5C0F\u6D9B");
        values.put(RawContacts.DISPLAY_NAME_ALTERNATIVE, "\u6BB5\u5C0F\u6D9B");
        values.putNull(RawContacts.PHONETIC_NAME);
        values.put(RawContacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(RawContacts.SORT_KEY_PRIMARY, "\u6BB5\u5C0F\u6D9B");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_PRIMARY, "D");
        values.put(RawContacts.SORT_KEY_ALTERNATIVE, "\u6BB5\u5C0F\u6D9B");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "D");

        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertStoredValues(rawContactUri, values);

        values.clear();
        values.put(Contacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(Contacts.DISPLAY_NAME_PRIMARY, "\u6BB5\u5C0F\u6D9B");
        values.put(Contacts.DISPLAY_NAME_ALTERNATIVE, "\u6BB5\u5C0F\u6D9B");
        values.putNull(Contacts.PHONETIC_NAME);
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(Contacts.SORT_KEY_PRIMARY, "\u6BB5\u5C0F\u6D9B");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "D");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "\u6BB5\u5C0F\u6D9B");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "D");

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId));
        assertStoredValues(contactUri, values);

        // The same values should be available through a join with Data
        assertStoredValues(dataUri, values);
    }

    public void testJapaneseNameContactInEnglishLocale() {
        // Need Japanese locale data for transliteration
        if (!hasJapaneseCollator()) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.US);
        long rawContactId = RawContactUtil.createRawContact(mResolver, null);

        ContentValues values = new ContentValues();
        values.put(StructuredName.GIVEN_NAME, "\u7A7A\u6D77");
        values.put(StructuredName.PHONETIC_GIVEN_NAME, "\u304B\u3044\u304F\u3046");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);

        long contactId = queryContactId(rawContactId);
        // en_US should behave same as ja_JP (match on Hiragana and Romaji
        // but not Pinyin)
        assertContactFilter(contactId, "\u304B\u3044\u304F\u3046");
        assertContactFilter(contactId, "kaiku");
        assertContactFilterNoResult("kong");
    }

    public void testContactWithJapaneseName() {
        if (!hasJapaneseCollator()) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.JAPAN);
        long rawContactId = RawContactUtil.createRawContact(mResolver, null);

        ContentValues values = new ContentValues();
        values.put(StructuredName.GIVEN_NAME, "\u7A7A\u6D77");
        values.put(StructuredName.PHONETIC_GIVEN_NAME, "\u304B\u3044\u304F\u3046");
        Uri dataUri = DataUtil.insertStructuredName(mResolver, rawContactId, values);

        values.clear();
        values.put(RawContacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(RawContacts.DISPLAY_NAME_PRIMARY, "\u7A7A\u6D77");
        values.put(RawContacts.DISPLAY_NAME_ALTERNATIVE, "\u7A7A\u6D77");
        values.put(RawContacts.PHONETIC_NAME, "\u304B\u3044\u304F\u3046");
        values.put(RawContacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.JAPANESE);
        values.put(RawContacts.SORT_KEY_PRIMARY, "\u304B\u3044\u304F\u3046");
        values.put(RawContacts.SORT_KEY_ALTERNATIVE, "\u304B\u3044\u304F\u3046");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_PRIMARY, "\u304B");
        values.put(RawContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "\u304B");

        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertStoredValues(rawContactUri, values);

        values.clear();
        values.put(Contacts.DISPLAY_NAME_SOURCE, DisplayNameSources.STRUCTURED_NAME);
        values.put(Contacts.DISPLAY_NAME_PRIMARY, "\u7A7A\u6D77");
        values.put(Contacts.DISPLAY_NAME_ALTERNATIVE, "\u7A7A\u6D77");
        values.put(Contacts.PHONETIC_NAME, "\u304B\u3044\u304F\u3046");
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.JAPANESE);
        values.put(Contacts.SORT_KEY_PRIMARY, "\u304B\u3044\u304F\u3046");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "\u304B\u3044\u304F\u3046");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "\u304B");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "\u304B");

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId));
        assertStoredValues(contactUri, values);

        // The same values should be available through a join with Data
        assertStoredValues(dataUri, values);

        long contactId = queryContactId(rawContactId);
        // ja_JP should match on Hiragana and Romaji but not Pinyin
        assertContactFilter(contactId, "\u304B\u3044\u304F\u3046");
        assertContactFilter(contactId, "kaiku");
        assertContactFilterNoResult("kong");
    }

    public void testDisplayNameUpdate() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        insertEmail(rawContactId1, "potato@acme.com", true);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        insertPhoneNumber(rawContactId2, "123456789", true);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        assertAggregated(rawContactId1, rawContactId2, "123456789");

        DataUtil.insertStructuredName(mResolver, rawContactId2, "Potato", "Head");

        assertAggregated(rawContactId1, rawContactId2, "Potato Head");
        assertNetworkNotified(true);
    }

    public void testDisplayNameFromData() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        assertStoredValue(uri, Contacts.DISPLAY_NAME, null);
        insertEmail(rawContactId, "mike@monstersinc.com");
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "mike@monstersinc.com");

        insertEmail(rawContactId, "james@monstersinc.com", true);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "james@monstersinc.com");

        insertPhoneNumber(rawContactId, "1-800-466-4411");
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "1-800-466-4411");

        // If there are title and company, the company is display name.
        values.clear();
        values.put(Organization.COMPANY, "Monsters Inc");
        Uri organizationUri = insertOrganization(rawContactId, values);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Monsters Inc");

        // If there is nickname, that is display name.
        insertNickname(rawContactId, "Sully");
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Sully");

        // If there is structured name, that is display name.
        values.clear();
        values.put(StructuredName.GIVEN_NAME, "James");
        values.put(StructuredName.MIDDLE_NAME, "P.");
        values.put(StructuredName.FAMILY_NAME, "Sullivan");
        DataUtil.insertStructuredName(mResolver, rawContactId, values);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "James P. Sullivan");
    }

    public void testDisplayNameFromOrganizationWithoutPhoneticName() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        // If there is title without company, the title is display name.
        values.clear();
        values.put(Organization.TITLE, "Protagonist");
        Uri organizationUri = insertOrganization(rawContactId, values);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Protagonist");

        // If there are title and company, the company is display name.
        values.clear();
        values.put(Organization.COMPANY, "Monsters Inc");
        mResolver.update(organizationUri, values, null, null);

        values.clear();
        values.put(Contacts.DISPLAY_NAME, "Monsters Inc");
        values.putNull(Contacts.PHONETIC_NAME);
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(Contacts.SORT_KEY_PRIMARY, "Monsters Inc");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "Monsters Inc");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "M");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "M");
        assertStoredValues(uri, values);
    }

    public void testDisplayNameFromOrganizationWithJapanesePhoneticName() {
        if (!hasJapaneseCollator()) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.JAPAN);
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        // If there is title without company, the title is display name.
        values.clear();
        values.put(Organization.COMPANY, "DoCoMo");
        values.put(Organization.PHONETIC_NAME, "\u30C9\u30B3\u30E2");
        Uri organizationUri = insertOrganization(rawContactId, values);

        values.clear();
        values.put(Contacts.DISPLAY_NAME, "DoCoMo");
        values.put(Contacts.PHONETIC_NAME, "\u30C9\u30B3\u30E2");
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.JAPANESE);
        values.put(Contacts.SORT_KEY_PRIMARY, "\u30C9\u30B3\u30E2");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "\u30C9\u30B3\u30E2");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "\u305F");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "\u305F");
        assertStoredValues(uri, values);
    }

    public void testDisplayNameFromOrganizationWithChineseName() {
        if (!hasChineseCollator()) {
            return;
        }
        ContactLocaleUtils.setLocaleForTest(Locale.SIMPLIFIED_CHINESE);

        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        // If there is title without company, the title is display name.
        values.clear();
        values.put(Organization.COMPANY, "\u4E2D\u56FD\u7535\u4FE1");
        Uri organizationUri = insertOrganization(rawContactId, values);

        values.clear();
        values.put(Contacts.DISPLAY_NAME, "\u4E2D\u56FD\u7535\u4FE1");
        values.putNull(Contacts.PHONETIC_NAME);
        values.put(Contacts.PHONETIC_NAME_STYLE, PhoneticNameStyle.UNDEFINED);
        values.put(Contacts.SORT_KEY_PRIMARY, "\u4E2D\u56FD\u7535\u4FE1");
        values.put(ContactsColumns.PHONEBOOK_LABEL_PRIMARY, "Z");
        values.put(Contacts.SORT_KEY_ALTERNATIVE, "\u4E2D\u56FD\u7535\u4FE1");
        values.put(ContactsColumns.PHONEBOOK_LABEL_ALTERNATIVE, "Z");
        assertStoredValues(uri, values);
    }

    public void testLookupByOrganization() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        values.clear();
        values.put(Organization.COMPANY, "acmecorp");
        values.put(Organization.TITLE, "president");
        Uri organizationUri = insertOrganization(rawContactId, values);

        assertContactFilter(contactId, "acmecorp");
        assertContactFilter(contactId, "president");

        values.clear();
        values.put(Organization.DEPARTMENT, "software");
        mResolver.update(organizationUri, values, null, null);

        assertContactFilter(contactId, "acmecorp");
        assertContactFilter(contactId, "president");

        values.clear();
        values.put(Organization.COMPANY, "incredibles");
        mResolver.update(organizationUri, values, null, null);

        assertContactFilter(contactId, "incredibles");
        assertContactFilter(contactId, "president");

        values.clear();
        values.put(Organization.TITLE, "director");
        mResolver.update(organizationUri, values, null, null);

        assertContactFilter(contactId, "incredibles");
        assertContactFilter(contactId, "director");

        values.clear();
        values.put(Organization.COMPANY, "monsters");
        values.put(Organization.TITLE, "scarer");
        mResolver.update(organizationUri, values, null, null);

        assertContactFilter(contactId, "monsters");
        assertContactFilter(contactId, "scarer");
    }

    private void assertContactFilter(long contactId, String filter) {
        Uri filterUri = Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI, Uri.encode(filter));
        assertStoredValue(filterUri, Contacts._ID, contactId);
    }

    private void assertContactFilterNoResult(String filter) {
        Uri filterUri = Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI, Uri.encode(filter));
        assertEquals(0, getCount(filterUri, null, null));
    }

    public void testSearchSnippetOrganization() throws Exception {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        // Some random data element
        insertEmail(rawContactId, "inc@corp.com");

        ContentValues values = new ContentValues();
        values.clear();
        values.put(Organization.COMPANY, "acmecorp");
        values.put(Organization.TITLE, "engineer");
        Uri organizationUri = insertOrganization(rawContactId, values);

        // Add another matching organization
        values.put(Organization.COMPANY, "acmeinc");
        insertOrganization(rawContactId, values);

        // Add another non-matching organization
        values.put(Organization.COMPANY, "corpacme");
        insertOrganization(rawContactId, values);

        // And another data element
        insertEmail(rawContactId, "emca@corp.com", true, Email.TYPE_CUSTOM, "Custom");

        Uri filterUri = buildFilterUri("acme", true);

        values.clear();
        values.put(Contacts._ID, contactId);
        values.put(SearchSnippets.SNIPPET, "acmecorp");
        assertContainsValues(filterUri, values);
    }

    public void testSearchSnippetEmail() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        Uri dataUri = insertEmail(rawContactId, "acme@corp.com", true, Email.TYPE_CUSTOM, "Custom");

        Uri filterUri = buildFilterUri("acme", true);

        values.clear();
        values.put(Contacts._ID, contactId);
        values.put(SearchSnippets.SNIPPET, "acme@corp.com");
        assertStoredValues(filterUri, values);
    }

    public void testCountPhoneNumberDigits() {
        assertEquals(10, ContactsProvider2.countPhoneNumberDigits("86 (0) 5-55-12-34"));
        assertEquals(10, ContactsProvider2.countPhoneNumberDigits("860 555-1234"));
        assertEquals(3, ContactsProvider2.countPhoneNumberDigits("860"));
        assertEquals(10, ContactsProvider2.countPhoneNumberDigits("8605551234"));
        assertEquals(6, ContactsProvider2.countPhoneNumberDigits("860555"));
        assertEquals(6, ContactsProvider2.countPhoneNumberDigits("860 555"));
        assertEquals(6, ContactsProvider2.countPhoneNumberDigits("860-555"));
        assertEquals(12, ContactsProvider2.countPhoneNumberDigits("+441234098765"));
        assertEquals(0, ContactsProvider2.countPhoneNumberDigits("44+1234098765"));
        assertEquals(0, ContactsProvider2.countPhoneNumberDigits("+441234098foo"));
    }

    public void testSearchSnippetPhone() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        DataUtil.insertStructuredName(mResolver, rawContactId, "Cave", "Johnson");
        insertPhoneNumber(rawContactId, "(860) 555-1234");

        values.clear();
        values.put(Contacts._ID, contactId);
        values.put(SearchSnippets.SNIPPET, "[(860) 555-1234]");

        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("86 (0) 5-55-12-34")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("860 555-1234")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("860")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("8605551234")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("860555")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("860 555")), values);
        assertStoredValues(Uri.withAppendedPath(Contacts.CONTENT_FILTER_URI,
                Uri.encode("860-555")), values);
    }

    private Uri buildFilterUri(String query, boolean deferredSnippeting) {
        Uri.Builder builder = Contacts.CONTENT_FILTER_URI.buildUpon()
                .appendPath(Uri.encode(query));
        if (deferredSnippeting) {
            builder.appendQueryParameter(ContactsContract.DEFERRED_SNIPPETING, "1");
        }
        return builder.build();
    }

    public ContentValues createSnippetContentValues(long contactId, String snippet) {
        final ContentValues values = new ContentValues();
        values.clear();
        values.put(Contacts._ID, contactId);
        values.put(SearchSnippets.SNIPPET, snippet);
        return values;
    }

    public void testSearchSnippetNickname() throws Exception {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = new ContentValues();

        Uri dataUri = insertNickname(rawContactId, "Incredible");

        Uri filterUri = buildFilterUri("inc", true);

        values.clear();
        values.put(Contacts._ID, contactId);
        values.put(SearchSnippets.SNIPPET, "Incredible");
        assertStoredValues(filterUri, values);
    }

    public void testSearchSnippetEmptyForNameInDisplayName() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        DataUtil.insertStructuredName(mResolver, rawContactId, "Cave", "Johnson");
        insertEmail(rawContactId, "cave@aperturescience.com", true);

        ContentValues snippet = createSnippetContentValues(contactId, "cave@aperturescience.com");

        assertContainsValues(buildFilterUri("cave", true), snippet);
        assertContainsValues(buildFilterUri("john", true), snippet);
    }

    public void testSearchSnippetEmptyForNicknameInDisplayName() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertNickname(rawContactId, "Caveman");
        insertEmail(rawContactId, "cave@aperturescience.com", true);

        ContentValues snippet = createSnippetContentValues(contactId, "cave@aperturescience.com");

        assertContainsValues(buildFilterUri("cave", true), snippet);
    }

    public void testSearchSnippetEmptyForCompanyInDisplayName() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues company = new ContentValues();
        company.clear();
        company.put(Organization.COMPANY, "Aperture Science");
        company.put(Organization.TITLE, "President");
        insertOrganization(rawContactId, company);
        insertEmail(rawContactId, "aperturepresident@aperturescience.com", true);

        ContentValues snippet = createSnippetContentValues(contactId, "aperturepresident");

        assertContainsValues(buildFilterUri("aperture", true), snippet);
    }

    public void testSearchSnippetEmptyForPhoneInDisplayName() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertPhoneNumber(rawContactId, "860-555-1234");
        insertEmail(rawContactId, "860@aperturescience.com", true);

        ContentValues snippet = createSnippetContentValues(contactId, "860-555-1234");

        assertContainsValues(buildFilterUri("860", true), snippet);
    }

    public void testSearchSnippetEmptyForEmailInDisplayName() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        insertEmail(rawContactId, "cave@aperturescience.com", true);
        insertNote(rawContactId, "Cave Johnson is president of Aperture Science");

        ContentValues snippet = createSnippetContentValues(contactId,
                "Cave Johnson is president of Aperture Science");

        assertContainsValues(buildFilterUri("cave", true), snippet);
    }

    public void testDisplayNameUpdateFromStructuredNameUpdate() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        Uri nameUri = DataUtil.insertStructuredName(mResolver, rawContactId, "Slinky", "Dog");

        long contactId = queryContactId(rawContactId);

        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Slinky Dog");

        ContentValues values = new ContentValues();
        values.putNull(StructuredName.FAMILY_NAME);

        mResolver.update(nameUri, values, null, null);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Slinky");

        values.putNull(StructuredName.GIVEN_NAME);

        mResolver.update(nameUri, values, null, null);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, null);

        values.put(StructuredName.FAMILY_NAME, "Dog");
        mResolver.update(nameUri, values, null, null);

        assertStoredValue(uri, Contacts.DISPLAY_NAME, "Dog");
    }

    public void testInsertDataWithContentProviderOperations() throws Exception {
        ContentProviderOperation cpo1 = ContentProviderOperation.newInsert(RawContacts.CONTENT_URI)
                .withValues(new ContentValues())
                .build();
        ContentProviderOperation cpo2 = ContentProviderOperation.newInsert(Data.CONTENT_URI)
                .withValueBackReference(Data.RAW_CONTACT_ID, 0)
                .withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, "John")
                .withValue(StructuredName.FAMILY_NAME, "Doe")
                .build();
        ContentProviderResult[] results =
                mResolver.applyBatch(ContactsContract.AUTHORITY, Lists.newArrayList(cpo1, cpo2));
        long contactId = queryContactId(ContentUris.parseId(results[0].uri));
        Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertStoredValue(uri, Contacts.DISPLAY_NAME, "John Doe");
    }

    public void testSendToVoicemailDefault() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        Cursor c = queryContact(contactId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Contacts.SEND_TO_VOICEMAIL));
        assertEquals(0, sendToVoicemail);
        c.close();
    }

    public void testSetSendToVoicemailAndRingtone() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertDirty(rawContactUri, true);
        clearDirty(rawContactUri);
        long contactId = queryContactId(rawContactId);

        updateSendToVoicemailAndRingtone(contactId, true, "foo");
        assertSendToVoicemailAndRingtone(contactId, true, "foo");
        assertNetworkNotified(false);
        assertMetadataNetworkNotified(false);
        assertDirty(rawContactUri, false);
        assertMetadataDirty(rawContactUri, false);

        updateSendToVoicemailAndRingtoneWithSelection(contactId, false, "bar");
        assertSendToVoicemailAndRingtone(contactId, false, "bar");
        assertNetworkNotified(false);
        assertMetadataNetworkNotified(false);
        assertDirty(rawContactUri, false);
        assertMetadataDirty(rawContactUri, false);
    }

    public void testSendToVoicemailAndRingtoneAfterAggregation() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "a", "b");
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, "foo");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "c", "d");
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, true, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Both contacts had "send to VM", the contact now has the same value
        assertSendToVoicemailAndRingtone(contactId1, true, "foo,bar"); // Either foo or bar
    }

    public void testDoNotSendToVoicemailAfterAggregation() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "e", "f");
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, null);

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "g", "h");
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, false, null);

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Since one of the contacts had "don't send to VM" that setting wins for the aggregate
        assertSendToVoicemailAndRingtone(queryContactId(rawContactId1), false, null);
    }

    public void testSetSendToVoicemailAndRingtonePreservedAfterJoinAndSplit() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "i", "j");
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, "foo");

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "k", "l");
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, false, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Split them
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        assertSendToVoicemailAndRingtone(queryContactId(rawContactId1), true, "foo");
        assertSendToVoicemailAndRingtone(queryContactId(rawContactId2), false, "bar");
    }

    public void testMarkMetadataDirtyAfterAggregation() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "i", "j");
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "k", "l");
        Uri rawContactUri1 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1);
        Uri rawContactUri2 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2);
        assertDirty(rawContactUri1, true);
        assertDirty(rawContactUri2, true);
        clearDirty(rawContactUri1);
        clearDirty(rawContactUri2);

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        assertDirty(rawContactUri1, false);
        assertDirty(rawContactUri2, false);
        assertMetadataDirty(rawContactUri1, false);
        assertMetadataDirty(rawContactUri2, false);
        assertNetworkNotified(false);
        assertMetadataNetworkNotified(false);
    }

    public void testStatusUpdateInsert() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        Uri imUri = insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");
        long dataId = ContentUris.parseId(imUri);

        ContentValues values = new ContentValues();
        values.put(StatusUpdates.DATA_ID, dataId);
        values.put(StatusUpdates.PROTOCOL, Im.PROTOCOL_AIM);
        values.putNull(StatusUpdates.CUSTOM_PROTOCOL);
        values.put(StatusUpdates.IM_HANDLE, "aim");
        values.put(StatusUpdates.PRESENCE, StatusUpdates.INVISIBLE);
        values.put(StatusUpdates.STATUS, "Hiding");
        values.put(StatusUpdates.STATUS_TIMESTAMP, 100);
        values.put(StatusUpdates.STATUS_RES_PACKAGE, "a.b.c");
        values.put(StatusUpdates.STATUS_ICON, 1234);
        values.put(StatusUpdates.STATUS_LABEL, 2345);

        Uri resultUri = mResolver.insert(StatusUpdates.CONTENT_URI, values);

        assertStoredValues(resultUri, values);

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        values.clear();
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);
        values.put(Contacts.CONTACT_STATUS, "Hiding");
        values.put(Contacts.CONTACT_STATUS_TIMESTAMP, 100);
        values.put(Contacts.CONTACT_STATUS_RES_PACKAGE, "a.b.c");
        values.put(Contacts.CONTACT_STATUS_ICON, 1234);
        values.put(Contacts.CONTACT_STATUS_LABEL, 2345);

        assertStoredValues(contactUri, values);

        values.clear();
        values.put(StatusUpdates.DATA_ID, dataId);
        values.put(StatusUpdates.STATUS, "Cloaked");
        values.put(StatusUpdates.STATUS_TIMESTAMP, 200);
        values.put(StatusUpdates.STATUS_RES_PACKAGE, "d.e.f");
        values.put(StatusUpdates.STATUS_ICON, 4321);
        values.put(StatusUpdates.STATUS_LABEL, 5432);
        mResolver.insert(StatusUpdates.CONTENT_URI, values);

        values.clear();
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.INVISIBLE);
        values.put(Contacts.CONTACT_STATUS, "Cloaked");
        values.put(Contacts.CONTACT_STATUS_TIMESTAMP, 200);
        values.put(Contacts.CONTACT_STATUS_RES_PACKAGE, "d.e.f");
        values.put(Contacts.CONTACT_STATUS_ICON, 4321);
        values.put(Contacts.CONTACT_STATUS_LABEL, 5432);
        assertStoredValues(contactUri, values);
    }

    public void testStatusUpdateInferAttribution() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        Uri imUri = insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");
        long dataId = ContentUris.parseId(imUri);

        ContentValues values = new ContentValues();
        values.put(StatusUpdates.DATA_ID, dataId);
        values.put(StatusUpdates.PROTOCOL, Im.PROTOCOL_AIM);
        values.put(StatusUpdates.IM_HANDLE, "aim");
        values.put(StatusUpdates.STATUS, "Hiding");

        Uri resultUri = mResolver.insert(StatusUpdates.CONTENT_URI, values);

        values.clear();
        values.put(StatusUpdates.DATA_ID, dataId);
        values.put(StatusUpdates.STATUS_LABEL, com.android.internal.R.string.imProtocolAim);
        values.put(StatusUpdates.STATUS, "Hiding");

        assertStoredValues(resultUri, values);
    }

    public void testStatusUpdateMatchingImOrEmail() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");
        insertImHandle(rawContactId, Im.PROTOCOL_CUSTOM, "my_im_proto", "my_im");
        insertEmail(rawContactId, "m@acme.com");

        // Match on IM (standard)
        insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", StatusUpdates.AVAILABLE, "Available",
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        // Match on IM (custom)
        insertStatusUpdate(Im.PROTOCOL_CUSTOM, "my_im_proto", "my_im", StatusUpdates.IDLE, "Idle",
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO);

        // Match on Email
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "m@acme.com", StatusUpdates.AWAY, "Away",
                StatusUpdates.CAPABILITY_HAS_VOICE);

        // No match
        insertStatusUpdate(Im.PROTOCOL_ICQ, null, "12345", StatusUpdates.DO_NOT_DISTURB, "Go away",
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        Cursor c = mResolver.query(StatusUpdates.CONTENT_URI, new String[] {
                StatusUpdates.DATA_ID, StatusUpdates.PROTOCOL, StatusUpdates.CUSTOM_PROTOCOL,
                StatusUpdates.PRESENCE, StatusUpdates.STATUS},
                PresenceColumns.RAW_CONTACT_ID + "=" + rawContactId, null, StatusUpdates.DATA_ID);
        assertTrue(c.moveToNext());
        assertStatusUpdate(c, Im.PROTOCOL_AIM, null, StatusUpdates.AVAILABLE, "Available");
        assertTrue(c.moveToNext());
        assertStatusUpdate(c, Im.PROTOCOL_CUSTOM, "my_im_proto", StatusUpdates.IDLE, "Idle");
        assertTrue(c.moveToNext());
        assertStatusUpdate(c, Im.PROTOCOL_GOOGLE_TALK, null, StatusUpdates.AWAY, "Away");
        assertFalse(c.moveToNext());
        c.close();

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        ContentValues values = new ContentValues();
        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.AVAILABLE);
        values.put(Contacts.CONTACT_STATUS, "Available");
        assertStoredValuesWithProjection(contactUri, values);
    }

    public void testStatusUpdateUpdateAndDelete() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        ContentValues values = new ContentValues();
        values.putNull(Contacts.CONTACT_PRESENCE);
        values.putNull(Contacts.CONTACT_STATUS);
        assertStoredValuesWithProjection(contactUri, values);

        insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", StatusUpdates.AWAY, "BUSY",
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", StatusUpdates.DO_NOT_DISTURB, "GO AWAY",
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        Uri statusUri =
            insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", StatusUpdates.AVAILABLE, "Available",
                    StatusUpdates.CAPABILITY_HAS_CAMERA);
        long statusId = ContentUris.parseId(statusUri);

        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.AVAILABLE);
        values.put(Contacts.CONTACT_STATUS, "Available");
        assertStoredValuesWithProjection(contactUri, values);

        // update status_updates table to set new values for
        //     status_updates.status
        //     status_updates.status_ts
        //     presence
        long updatedTs = 200;
        String testUpdate = "test_update";
        String selection = StatusUpdates.DATA_ID + "=" + statusId;
        values.clear();
        values.put(StatusUpdates.STATUS_TIMESTAMP, updatedTs);
        values.put(StatusUpdates.STATUS, testUpdate);
        values.put(StatusUpdates.PRESENCE, "presence_test");
        mResolver.update(StatusUpdates.CONTENT_URI, values,
                StatusUpdates.DATA_ID + "=" + statusId, null);
        assertStoredValuesWithProjection(StatusUpdates.CONTENT_URI, values);

        // update status_updates table to set new values for columns in status_updates table ONLY
        // i.e., no rows in presence table are to be updated.
        updatedTs = 300;
        testUpdate = "test_update_new";
        selection = StatusUpdates.DATA_ID + "=" + statusId;
        values.clear();
        values.put(StatusUpdates.STATUS_TIMESTAMP, updatedTs);
        values.put(StatusUpdates.STATUS, testUpdate);
        mResolver.update(StatusUpdates.CONTENT_URI, values,
                StatusUpdates.DATA_ID + "=" + statusId, null);
        // make sure the presence column value is still the old value
        values.put(StatusUpdates.PRESENCE, "presence_test");
        assertStoredValuesWithProjection(StatusUpdates.CONTENT_URI, values);

        // update status_updates table to set new values for columns in presence table ONLY
        // i.e., no rows in status_updates table are to be updated.
        selection = StatusUpdates.DATA_ID + "=" + statusId;
        values.clear();
        values.put(StatusUpdates.PRESENCE, "presence_test_new");
        mResolver.update(StatusUpdates.CONTENT_URI, values,
                StatusUpdates.DATA_ID + "=" + statusId, null);
        // make sure the status_updates table is not updated
        values.put(StatusUpdates.STATUS_TIMESTAMP, updatedTs);
        values.put(StatusUpdates.STATUS, testUpdate);
        assertStoredValuesWithProjection(StatusUpdates.CONTENT_URI, values);

        // effect "delete status_updates" operation and expect the following
        //   data deleted from status_updates table
        //   presence set to null
        mResolver.delete(StatusUpdates.CONTENT_URI, StatusUpdates.DATA_ID + "=" + statusId, null);
        values.clear();
        values.putNull(Contacts.CONTACT_PRESENCE);
        assertStoredValuesWithProjection(contactUri, values);
    }

    public void testStatusUpdateUpdateToNull() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        ContentValues values = new ContentValues();
        Uri statusUri =
            insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", StatusUpdates.AVAILABLE, "Available",
                    StatusUpdates.CAPABILITY_HAS_CAMERA);
        long statusId = ContentUris.parseId(statusUri);

        values.put(Contacts.CONTACT_PRESENCE, StatusUpdates.AVAILABLE);
        values.put(Contacts.CONTACT_STATUS, "Available");
        assertStoredValuesWithProjection(contactUri, values);

        values.clear();
        values.putNull(StatusUpdates.PRESENCE);
        mResolver.update(StatusUpdates.CONTENT_URI, values,
                StatusUpdates.DATA_ID + "=" + statusId, null);

        values.clear();
        values.putNull(Contacts.CONTACT_PRESENCE);
        values.put(Contacts.CONTACT_STATUS, "Available");
        assertStoredValuesWithProjection(contactUri, values);
    }

    public void testStatusUpdateWithTimestamp() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        insertImHandle(rawContactId, Im.PROTOCOL_AIM, null, "aim");
        insertImHandle(rawContactId, Im.PROTOCOL_GOOGLE_TALK, null, "gtalk");

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", 0, "Offline", 80,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);
        insertStatusUpdate(Im.PROTOCOL_AIM, null, "aim", 0, "Available", 100,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "gtalk", 0, "Busy", 90,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);

        // Should return the latest status
        ContentValues values = new ContentValues();
        values.put(Contacts.CONTACT_STATUS_TIMESTAMP, 100);
        values.put(Contacts.CONTACT_STATUS, "Available");
        assertStoredValuesWithProjection(contactUri, values);
    }

    private void assertStatusUpdate(Cursor c, int protocol, String customProtocol, int presence,
            String status) {
        ContentValues values = new ContentValues();
        values.put(StatusUpdates.PROTOCOL, protocol);
        values.put(StatusUpdates.CUSTOM_PROTOCOL, customProtocol);
        values.put(StatusUpdates.PRESENCE, presence);
        values.put(StatusUpdates.STATUS, status);
        assertCursorValues(c, values);
    }

    // Stream item query test cases.

    public void testQueryStreamItemsByRawContactId() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, values, mAccount);
        assertStoredValues(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                        RawContacts.StreamItems.CONTENT_DIRECTORY),
                values);
    }

    public void testQueryStreamItemsByContactId() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, values, null);
        assertStoredValues(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                        Contacts.StreamItems.CONTENT_DIRECTORY),
                values);
    }

    public void testQueryStreamItemsByLookupKey() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, values, null);
        assertStoredValues(
                Uri.withAppendedPath(
                        Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, lookupKey),
                        Contacts.StreamItems.CONTENT_DIRECTORY),
                values);
    }

    public void testQueryStreamItemsByLookupKeyAndContactId() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, values, null);
        assertStoredValues(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(
                                Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, lookupKey),
                                contactId),
                        Contacts.StreamItems.CONTENT_DIRECTORY),
                values);
    }

    public void testQueryStreamItems() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, values, null);
        assertStoredValues(StreamItems.CONTENT_URI, values);
    }

    public void testQueryStreamItemsWithSelection() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues firstValues = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, firstValues, null);

        ContentValues secondValues = buildGenericStreamItemValues();
        secondValues.put(StreamItems.TEXT, "Goodbye world");
        insertStreamItem(rawContactId, secondValues, null);

        // Select only the first stream item.
        assertStoredValues(StreamItems.CONTENT_URI, StreamItems.TEXT + "=?",
                new String[]{"Hello world"}, firstValues);

        // Select only the second stream item.
        assertStoredValues(StreamItems.CONTENT_URI, StreamItems.TEXT + "=?",
                new String[]{"Goodbye world"}, secondValues);
    }

    public void testQueryStreamItemById() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues firstValues = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, firstValues, null);
        long firstStreamItemId = ContentUris.parseId(resultUri);

        ContentValues secondValues = buildGenericStreamItemValues();
        secondValues.put(StreamItems.TEXT, "Goodbye world");
        resultUri = insertStreamItem(rawContactId, secondValues, null);
        long secondStreamItemId = ContentUris.parseId(resultUri);

        // Select only the first stream item.
        assertStoredValues(ContentUris.withAppendedId(StreamItems.CONTENT_URI, firstStreamItemId),
                firstValues);

        // Select only the second stream item.
        assertStoredValues(ContentUris.withAppendedId(StreamItems.CONTENT_URI, secondStreamItemId),
                secondValues);
    }

    // Stream item photo insertion + query test cases.

    public void testQueryStreamItemPhotoWithSelection() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, values, null);
        long streamItemId = ContentUris.parseId(resultUri);

        ContentValues photo1Values = buildGenericStreamItemPhotoValues(1);
        insertStreamItemPhoto(streamItemId, photo1Values, null);
        photo1Values.remove(StreamItemPhotos.PHOTO);  // Removed during processing.
        ContentValues photo2Values = buildGenericStreamItemPhotoValues(2);
        insertStreamItemPhoto(streamItemId, photo2Values, null);

        // Select only the first photo.
        assertStoredValues(StreamItems.CONTENT_PHOTO_URI, StreamItemPhotos.SORT_INDEX + "=?",
                new String[]{"1"}, photo1Values);
    }

    public void testQueryStreamItemPhotoByStreamItemId() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);

        // Insert a first stream item.
        ContentValues firstValues = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, firstValues, null);
        long firstStreamItemId = ContentUris.parseId(resultUri);

        // Insert a second stream item.
        ContentValues secondValues = buildGenericStreamItemValues();
        resultUri = insertStreamItem(rawContactId, secondValues, null);
        long secondStreamItemId = ContentUris.parseId(resultUri);

        // Add a photo to the first stream item.
        ContentValues photo1Values = buildGenericStreamItemPhotoValues(1);
        insertStreamItemPhoto(firstStreamItemId, photo1Values, null);
        photo1Values.remove(StreamItemPhotos.PHOTO);  // Removed during processing.

        // Add a photo to the second stream item.
        ContentValues photo2Values = buildGenericStreamItemPhotoValues(1);
        photo2Values.put(StreamItemPhotos.PHOTO, loadPhotoFromResource(
                R.drawable.nebula, PhotoSize.ORIGINAL));
        insertStreamItemPhoto(secondStreamItemId, photo2Values, null);
        photo2Values.remove(StreamItemPhotos.PHOTO);  // Removed during processing.

        // Select only the photos from the second stream item.
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(StreamItems.CONTENT_URI, secondStreamItemId),
                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY), photo2Values);
    }

    public void testQueryStreamItemPhotoByStreamItemPhotoId() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);

        // Insert a first stream item.
        ContentValues firstValues = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, firstValues, null);
        long firstStreamItemId = ContentUris.parseId(resultUri);

        // Insert a second stream item.
        ContentValues secondValues = buildGenericStreamItemValues();
        resultUri = insertStreamItem(rawContactId, secondValues, null);
        long secondStreamItemId = ContentUris.parseId(resultUri);

        // Add a photo to the first stream item.
        ContentValues photo1Values = buildGenericStreamItemPhotoValues(1);
        resultUri = insertStreamItemPhoto(firstStreamItemId, photo1Values, null);
        long firstPhotoId = ContentUris.parseId(resultUri);
        photo1Values.remove(StreamItemPhotos.PHOTO);  // Removed during processing.

        // Add a photo to the second stream item.
        ContentValues photo2Values = buildGenericStreamItemPhotoValues(1);
        photo2Values.put(StreamItemPhotos.PHOTO, loadPhotoFromResource(
                R.drawable.galaxy, PhotoSize.ORIGINAL));
        resultUri = insertStreamItemPhoto(secondStreamItemId, photo2Values, null);
        long secondPhotoId = ContentUris.parseId(resultUri);
        photo2Values.remove(StreamItemPhotos.PHOTO);  // Removed during processing.

        // Select the first photo.
        assertStoredValues(ContentUris.withAppendedId(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(StreamItems.CONTENT_URI, firstStreamItemId),
                        StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                firstPhotoId),
                photo1Values);

        // Select the second photo.
        assertStoredValues(ContentUris.withAppendedId(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(StreamItems.CONTENT_URI, secondStreamItemId),
                        StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                secondPhotoId),
                photo2Values);
    }

    // Stream item insertion test cases.

    public void testInsertStreamItemInProfileRequiresWriteProfileAccess() {
        long profileRawContactId = createBasicProfileContact(new ContentValues());

        // Try inserting a stream item. It should still succeed even without the profile permission.
        ContentValues values = buildGenericStreamItemValues();
        insertStreamItem(profileRawContactId, values, null);
    }

    public void testInsertStreamItemWithContentValues() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        values.put(StreamItems.RAW_CONTACT_ID, rawContactId);
        mResolver.insert(StreamItems.CONTENT_URI, values);
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.StreamItems.CONTENT_DIRECTORY), values);
    }

    public void testInsertStreamItemOverLimit() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        values.put(StreamItems.RAW_CONTACT_ID, rawContactId);

        List<Long> streamItemIds = Lists.newArrayList();

        // Insert MAX + 1 stream items.
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 6; i++) {
            values.put(StreamItems.TIMESTAMP, baseTime + i);
            Uri resultUri = mResolver.insert(StreamItems.CONTENT_URI, values);
            streamItemIds.add(ContentUris.parseId(resultUri));
        }
        Long doomedStreamItemId = streamItemIds.get(0);

        // There should only be MAX items.  The oldest one should have been cleaned up.
        Cursor c = mResolver.query(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                        RawContacts.StreamItems.CONTENT_DIRECTORY),
                new String[]{StreamItems._ID}, null, null, null);
        try {
            while(c.moveToNext()) {
                long streamItemId = c.getLong(0);
                streamItemIds.remove(streamItemId);
            }
        } finally {
            c.close();
        }

        assertEquals(1, streamItemIds.size());
    }

    public void testInsertStreamItemOlderThanOldestInLimit() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        values.put(StreamItems.RAW_CONTACT_ID, rawContactId);

        // Insert MAX stream items.
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            values.put(StreamItems.TIMESTAMP, baseTime + i);
            Uri resultUri = mResolver.insert(StreamItems.CONTENT_URI, values);
            assertNotSame("Expected non-0 stream item ID to be inserted",
                    0L, ContentUris.parseId(resultUri));
        }

        // Now try to insert a stream item that's older.  It should be deleted immediately
        // and return an ID of 0.
        values.put(StreamItems.TIMESTAMP, baseTime - 1);
        Uri resultUri = mResolver.insert(StreamItems.CONTENT_URI, values);
        assertEquals(0L, ContentUris.parseId(resultUri));
    }

    // Stream item photo insertion test cases.

    public void testInsertStreamItemsAndPhotosInBatch() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues streamItemValues = buildGenericStreamItemValues();
        ContentValues streamItemPhotoValues = buildGenericStreamItemPhotoValues(0);

        ArrayList<ContentProviderOperation> ops = Lists.newArrayList();
        ops.add(ContentProviderOperation.newInsert(
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                        RawContacts.StreamItems.CONTENT_DIRECTORY))
                .withValues(streamItemValues).build());
        for (int i = 0; i < 5; i++) {
            streamItemPhotoValues.put(StreamItemPhotos.SORT_INDEX, i);
            ops.add(ContentProviderOperation.newInsert(StreamItems.CONTENT_PHOTO_URI)
                    .withValues(streamItemPhotoValues)
                    .withValueBackReference(StreamItemPhotos.STREAM_ITEM_ID, 0)
                    .build());
        }
        mResolver.applyBatch(ContactsContract.AUTHORITY, ops);

        // Check that all five photos were inserted under the raw contact.
        Cursor c = mResolver.query(StreamItems.CONTENT_URI, new String[]{StreamItems._ID},
                StreamItems.RAW_CONTACT_ID + "=?", new String[]{String.valueOf(rawContactId)},
                null);
        long streamItemId = 0;
        try {
            assertEquals(1, c.getCount());
            c.moveToFirst();
            streamItemId = c.getLong(0);
        } finally {
            c.close();
        }

        c = mResolver.query(Uri.withAppendedPath(
                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                new String[]{StreamItemPhotos._ID, StreamItemPhotos.PHOTO_URI},
                null, null, null);
        try {
            assertEquals(5, c.getCount());
            byte[] expectedPhotoBytes = loadPhotoFromResource(
                    R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO);
            while (c.moveToNext()) {
                String photoUri = c.getString(1);
                EvenMoreAsserts.assertImageRawData(getContext(),
                        expectedPhotoBytes, mResolver.openInputStream(Uri.parse(photoUri)));
            }
        } finally {
            c.close();
        }
    }

    // Stream item update test cases.

    public void testUpdateStreamItemById() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, values, null);
        long streamItemId = ContentUris.parseId(resultUri);
        values.put(StreamItems.TEXT, "Goodbye world");
        mResolver.update(ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId), values,
                null, null);
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.StreamItems.CONTENT_DIRECTORY), values);
    }

    public void testUpdateStreamItemWithContentValues() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, values, null);
        long streamItemId = ContentUris.parseId(resultUri);
        values.put(StreamItems._ID, streamItemId);
        values.put(StreamItems.TEXT, "Goodbye world");
        mResolver.update(StreamItems.CONTENT_URI, values, null, null);
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.StreamItems.CONTENT_DIRECTORY), values);
    }

    // Stream item photo update test cases.

    public void testUpdateStreamItemPhotoById() throws IOException {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, values, null);
        long streamItemId = ContentUris.parseId(resultUri);
        ContentValues photoValues = buildGenericStreamItemPhotoValues(1);
        resultUri = insertStreamItemPhoto(streamItemId, photoValues, null);
        long streamItemPhotoId = ContentUris.parseId(resultUri);

        photoValues.put(StreamItemPhotos.PHOTO, loadPhotoFromResource(
                R.drawable.nebula, PhotoSize.ORIGINAL));
        Uri photoUri =
                ContentUris.withAppendedId(
                        Uri.withAppendedPath(
                                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                        streamItemPhotoId);
        mResolver.update(photoUri, photoValues, null, null);
        photoValues.remove(StreamItemPhotos.PHOTO);  // Removed during processing.
        assertStoredValues(photoUri, photoValues);

        // Check that the photo stored is the expected one.
        String displayPhotoUri = getStoredValue(photoUri, StreamItemPhotos.PHOTO_URI);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.nebula, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(Uri.parse(displayPhotoUri)));
    }

    public void testUpdateStreamItemPhotoWithContentValues() throws IOException {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues values = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, values, null);
        long streamItemId = ContentUris.parseId(resultUri);
        ContentValues photoValues = buildGenericStreamItemPhotoValues(1);
        resultUri = insertStreamItemPhoto(streamItemId, photoValues, null);
        long streamItemPhotoId = ContentUris.parseId(resultUri);

        photoValues.put(StreamItemPhotos._ID, streamItemPhotoId);
        photoValues.put(StreamItemPhotos.PHOTO, loadPhotoFromResource(
                R.drawable.nebula, PhotoSize.ORIGINAL));
        Uri photoUri =
                Uri.withAppendedPath(
                        ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                        StreamItems.StreamItemPhotos.CONTENT_DIRECTORY);
        mResolver.update(photoUri, photoValues, null, null);
        photoValues.remove(StreamItemPhotos.PHOTO);  // Removed during processing.
        assertStoredValues(photoUri, photoValues);

        // Check that the photo stored is the expected one.
        String displayPhotoUri = getStoredValue(photoUri, StreamItemPhotos.PHOTO_URI);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.nebula, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(Uri.parse(displayPhotoUri)));
    }

    // Stream item deletion test cases.

    public void testDeleteStreamItemById() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues firstValues = buildGenericStreamItemValues();
        Uri resultUri = insertStreamItem(rawContactId, firstValues, null);
        long firstStreamItemId = ContentUris.parseId(resultUri);

        ContentValues secondValues = buildGenericStreamItemValues();
        secondValues.put(StreamItems.TEXT, "Goodbye world");
        insertStreamItem(rawContactId, secondValues, null);

        // Delete the first stream item.
        mResolver.delete(ContentUris.withAppendedId(StreamItems.CONTENT_URI, firstStreamItemId),
                null, null);

        // Check that only the second item remains.
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.StreamItems.CONTENT_DIRECTORY), secondValues);
    }

    public void testDeleteStreamItemWithSelection() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        ContentValues firstValues = buildGenericStreamItemValues();
        insertStreamItem(rawContactId, firstValues, null);

        ContentValues secondValues = buildGenericStreamItemValues();
        secondValues.put(StreamItems.TEXT, "Goodbye world");
        insertStreamItem(rawContactId, secondValues, null);

        // Delete the first stream item with a custom selection.
        mResolver.delete(StreamItems.CONTENT_URI, StreamItems.TEXT + "=?",
                new String[]{"Hello world"});

        // Check that only the second item remains.
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.StreamItems.CONTENT_DIRECTORY), secondValues);
    }

    // Stream item photo deletion test cases.

    public void testDeleteStreamItemPhotoById() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long streamItemId = ContentUris.parseId(
                insertStreamItem(rawContactId, buildGenericStreamItemValues(), null));
        long streamItemPhotoId = ContentUris.parseId(
                insertStreamItemPhoto(streamItemId, buildGenericStreamItemPhotoValues(0), null));
        mResolver.delete(
                ContentUris.withAppendedId(
                        Uri.withAppendedPath(
                                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                        streamItemPhotoId), null, null);

        Cursor c = mResolver.query(StreamItems.CONTENT_PHOTO_URI,
                new String[]{StreamItemPhotos._ID},
                StreamItemPhotos.STREAM_ITEM_ID + "=?", new String[]{String.valueOf(streamItemId)},
                null);
        try {
            assertEquals("Expected photo to be deleted.", 0, c.getCount());
        } finally {
            c.close();
        }
    }

    public void testDeleteStreamItemPhotoWithSelection() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long streamItemId = ContentUris.parseId(
                insertStreamItem(rawContactId, buildGenericStreamItemValues(), null));
        ContentValues firstPhotoValues = buildGenericStreamItemPhotoValues(0);
        ContentValues secondPhotoValues = buildGenericStreamItemPhotoValues(1);
        insertStreamItemPhoto(streamItemId, firstPhotoValues, null);
        firstPhotoValues.remove(StreamItemPhotos.PHOTO);  // Removed while processing.
        insertStreamItemPhoto(streamItemId, secondPhotoValues, null);
        Uri photoUri = Uri.withAppendedPath(
                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY);
        mResolver.delete(photoUri, StreamItemPhotos.SORT_INDEX + "=1", null);

        assertStoredValues(photoUri, firstPhotoValues);
    }

    public void testDeleteStreamItemsWhenRawContactDeleted() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri streamItemUri = insertStreamItem(rawContactId,
                buildGenericStreamItemValues(), mAccount);
        Uri streamItemPhotoUri = insertStreamItemPhoto(ContentUris.parseId(streamItemUri),
                        buildGenericStreamItemPhotoValues(0), mAccount);
        mResolver.delete(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                null, null);

        ContentValues[] emptyValues = new ContentValues[0];

        // The stream item and its photo should be gone.
        assertStoredValues(streamItemUri, emptyValues);
        assertStoredValues(streamItemPhotoUri, emptyValues);
    }

    public void testQueryStreamItemLimit() {
        ContentValues values = new ContentValues();
        values.put(StreamItems.MAX_ITEMS, 5);
        assertStoredValues(StreamItems.CONTENT_LIMIT_URI, values);
    }

    // Tests for inserting or updating stream items as a side-effect of making status updates
    // (forward-compatibility of status updates into the new social stream API).

    public void testStreamItemInsertedOnStatusUpdate() {

        // This method of creating a raw contact automatically inserts a status update with
        // the status message "hacking".
        ContentValues values = new ContentValues();
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO |
                        StatusUpdates.CAPABILITY_HAS_VOICE);

        ContentValues expectedValues = new ContentValues();
        expectedValues.put(StreamItems.RAW_CONTACT_ID, rawContactId);
        expectedValues.put(StreamItems.TEXT, "hacking");
        assertStoredValues(RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.StreamItems.CONTENT_DIRECTORY).build(),
                expectedValues);
    }

    public void testStreamItemInsertedOnStatusUpdate_HtmlQuoting() {

        // This method of creating a raw contact automatically inserts a status update with
        // the status message "hacking".
        ContentValues values = new ContentValues();
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_VOICE);

        // Insert a new status update for the raw contact.
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "goog411@acme.com",
                StatusUpdates.INVISIBLE, "& <b> test &#39;", StatusUpdates.CAPABILITY_HAS_VOICE);

        ContentValues expectedValues = new ContentValues();
        expectedValues.put(StreamItems.RAW_CONTACT_ID, rawContactId);
        expectedValues.put(StreamItems.TEXT, "&amp; &lt;b&gt; test &amp;#39;");
        assertStoredValues(RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.StreamItems.CONTENT_DIRECTORY).build(),
                expectedValues);
    }

    public void testStreamItemUpdatedOnSecondStatusUpdate() {

        // This method of creating a raw contact automatically inserts a status update with
        // the status message "hacking".
        ContentValues values = new ContentValues();
        int chatMode = StatusUpdates.CAPABILITY_HAS_CAMERA | StatusUpdates.CAPABILITY_HAS_VIDEO |
                StatusUpdates.CAPABILITY_HAS_VOICE;
        long rawContactId = createRawContact(values, "18004664411",
                "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0, chatMode);

        // Insert a new status update for the raw contact.
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "goog411@acme.com",
                StatusUpdates.INVISIBLE, "finished hacking", chatMode);

        ContentValues expectedValues = new ContentValues();
        expectedValues.put(StreamItems.RAW_CONTACT_ID, rawContactId);
        expectedValues.put(StreamItems.TEXT, "finished hacking");
        assertStoredValues(RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.StreamItems.CONTENT_DIRECTORY).build(),
                expectedValues);
    }

    private ContentValues buildGenericStreamItemValues() {
        ContentValues values = new ContentValues();
        values.put(StreamItems.TEXT, "Hello world");
        values.put(StreamItems.TIMESTAMP, System.currentTimeMillis());
        values.put(StreamItems.COMMENTS, "Reshared by 123 others");
        return values;
    }

    private ContentValues buildGenericStreamItemPhotoValues(int sortIndex) {
        ContentValues values = new ContentValues();
        values.put(StreamItemPhotos.SORT_INDEX, sortIndex);
        values.put(StreamItemPhotos.PHOTO,
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.ORIGINAL));
        return values;
    }

    public void testSingleStatusUpdateRowPerContact() {
        int protocol1 = Im.PROTOCOL_GOOGLE_TALK;
        String handle1 = "test@gmail.com";

        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        insertImHandle(rawContactId1, protocol1, null, handle1);

        insertStatusUpdate(protocol1, null, handle1, StatusUpdates.AVAILABLE, "Green",
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        insertStatusUpdate(protocol1, null, handle1, StatusUpdates.AWAY, "Yellow",
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        insertStatusUpdate(protocol1, null, handle1, StatusUpdates.INVISIBLE, "Red",
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        Cursor c = queryContact(queryContactId(rawContactId1),
                new String[] {Contacts.CONTACT_PRESENCE, Contacts.CONTACT_STATUS});
        assertEquals(1, c.getCount());

        c.moveToFirst();
        assertEquals(StatusUpdates.INVISIBLE, c.getInt(0));
        assertEquals("Red", c.getString(1));
        c.close();
    }

    private void updateSendToVoicemailAndRingtone(long contactId, boolean sendToVoicemail,
            String ringtone) {
        ContentValues values = new ContentValues();
        values.put(Contacts.SEND_TO_VOICEMAIL, sendToVoicemail);
        if (ringtone != null) {
            values.put(Contacts.CUSTOM_RINGTONE, ringtone);
        }

        final Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        int count = mResolver.update(uri, values, null, null);
        assertEquals(1, count);
    }

    private void updateSendToVoicemailAndRingtoneWithSelection(long contactId,
            boolean sendToVoicemail, String ringtone) {
        ContentValues values = new ContentValues();
        values.put(Contacts.SEND_TO_VOICEMAIL, sendToVoicemail);
        if (ringtone != null) {
            values.put(Contacts.CUSTOM_RINGTONE, ringtone);
        }

        int count = mResolver.update(Contacts.CONTENT_URI, values, Contacts._ID + "=" + contactId,
                null);
        assertEquals(1, count);
    }

    private void assertSendToVoicemailAndRingtone(long contactId, boolean expectedSendToVoicemail,
            String expectedRingtone) {
        Cursor c = queryContact(contactId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Contacts.SEND_TO_VOICEMAIL));
        assertEquals(expectedSendToVoicemail ? 1 : 0, sendToVoicemail);
        String ringtone = c.getString(c.getColumnIndex(Contacts.CUSTOM_RINGTONE));
        if (expectedRingtone == null) {
            assertNull(ringtone);
        } else {
            assertTrue(ArrayUtils.contains(expectedRingtone.split(","), ringtone));
        }
        c.close();
    }

    public void testContactVisibilityUpdateOnMembershipChange() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        assertVisibility(rawContactId, "0");

        long visibleGroupId = createGroup(mAccount, "123", "Visible", 1);
        long invisibleGroupId = createGroup(mAccount, "567", "Invisible", 0);

        Uri membership1 = insertGroupMembership(rawContactId, visibleGroupId);
        assertVisibility(rawContactId, "1");

        Uri membership2 = insertGroupMembership(rawContactId, invisibleGroupId);
        assertVisibility(rawContactId, "1");

        mResolver.delete(membership1, null, null);
        assertVisibility(rawContactId, "0");

        ContentValues values = new ContentValues();
        values.put(GroupMembership.GROUP_ROW_ID, visibleGroupId);

        mResolver.update(membership2, values, null, null);
        assertVisibility(rawContactId, "1");
    }

    private void assertVisibility(long rawContactId, String expectedValue) {
        assertStoredValue(Contacts.CONTENT_URI, Contacts._ID + "=" + queryContactId(rawContactId),
                null, Contacts.IN_VISIBLE_GROUP, expectedValue);
    }

    public void testSupplyingBothValuesAndParameters() throws Exception {
        Account account = new Account("account 1", "type%/:1");
        Uri uri = ContactsContract.Groups.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.Groups.ACCOUNT_NAME, account.name)
                .appendQueryParameter(ContactsContract.Groups.ACCOUNT_TYPE, account.type)
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "true")
                .build();

        ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(uri);
        builder.withValue(ContactsContract.Groups.ACCOUNT_TYPE, account.type);
        builder.withValue(ContactsContract.Groups.ACCOUNT_NAME, account.name);
        builder.withValue(ContactsContract.Groups.SYSTEM_ID, "some id");
        builder.withValue(ContactsContract.Groups.TITLE, "some name");
        builder.withValue(ContactsContract.Groups.GROUP_VISIBLE, 1);

        mResolver.applyBatch(ContactsContract.AUTHORITY, Lists.newArrayList(builder.build()));

        builder = ContentProviderOperation.newInsert(uri);
        builder.withValue(ContactsContract.Groups.ACCOUNT_TYPE, account.type + "diff");
        builder.withValue(ContactsContract.Groups.ACCOUNT_NAME, account.name);
        builder.withValue(ContactsContract.Groups.SYSTEM_ID, "some other id");
        builder.withValue(ContactsContract.Groups.TITLE, "some other name");
        builder.withValue(ContactsContract.Groups.GROUP_VISIBLE, 1);

        try {
            mResolver.applyBatch(ContactsContract.AUTHORITY, Lists.newArrayList(builder.build()));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // Expected
        }
    }

    public void testContentEntityIterator() {
        // create multiple contacts and check that the selected ones are returned
        long id;

        long groupId1 = createGroup(mAccount, "gsid1", "title1");
        long groupId2 = createGroup(mAccount, "gsid2", "title2");

        id = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.SOURCE_ID, "c0");
        insertGroupMembership(id, "gsid1");
        insertEmail(id, "c0@email.com");
        insertPhoneNumber(id, "5551212c0");

        long c1 = id = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.SOURCE_ID,
                "c1");
        Uri id_1_0 = insertGroupMembership(id, "gsid1");
        Uri id_1_1 = insertGroupMembership(id, "gsid2");
        Uri id_1_2 = insertEmail(id, "c1@email.com");
        Uri id_1_3 = insertPhoneNumber(id, "5551212c1");

        long c2 = id = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.SOURCE_ID,
                "c2");
        Uri id_2_0 = insertGroupMembership(id, "gsid1");
        Uri id_2_1 = insertEmail(id, "c2@email.com");
        Uri id_2_2 = insertPhoneNumber(id, "5551212c2");

        long c3 = id = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.SOURCE_ID,
                "c3");
        Uri id_3_0 = insertGroupMembership(id, groupId2);
        Uri id_3_1 = insertEmail(id, "c3@email.com");
        Uri id_3_2 = insertPhoneNumber(id, "5551212c3");

        EntityIterator iterator = RawContacts.newEntityIterator(mResolver.query(
                TestUtil.maybeAddAccountQueryParameters(RawContactsEntity.CONTENT_URI, mAccount),
                null, RawContacts.SOURCE_ID + " in ('c1', 'c2', 'c3')", null, null));
        Entity entity;
        ContentValues[] subValues;
        entity = iterator.next();
        assertEquals(c1, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(4, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_1_0,
                GroupMembership.GROUP_ROW_ID, groupId1,
                GroupMembership.GROUP_SOURCE_ID, "gsid1");
        assertDataRow(subValues[1], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_1_1,
                GroupMembership.GROUP_ROW_ID, groupId2,
                GroupMembership.GROUP_SOURCE_ID, "gsid2");
        assertDataRow(subValues[2], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_1_2,
                Email.DATA, "c1@email.com");
        assertDataRow(subValues[3], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_1_3,
                Email.DATA, "5551212c1");

        entity = iterator.next();
        assertEquals(c2, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(3, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_2_0,
                GroupMembership.GROUP_ROW_ID, groupId1,
                GroupMembership.GROUP_SOURCE_ID, "gsid1");
        assertDataRow(subValues[1], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_2_1,
                Email.DATA, "c2@email.com");
        assertDataRow(subValues[2], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_2_2,
                Email.DATA, "5551212c2");

        entity = iterator.next();
        assertEquals(c3, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(3, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_3_0,
                GroupMembership.GROUP_ROW_ID, groupId2,
                GroupMembership.GROUP_SOURCE_ID, "gsid2");
        assertDataRow(subValues[1], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_3_1,
                Email.DATA, "c3@email.com");
        assertDataRow(subValues[2], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_3_2,
                Email.DATA, "5551212c3");

        assertFalse(iterator.hasNext());
        iterator.close();
    }

    public void testDataCreateUpdateDeleteByMimeType() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);

        ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, "testmimetype");
        values.put(Data.RES_PACKAGE, "oldpackage");
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.DATA1, "old1");
        values.put(Data.DATA2, "old2");
        values.put(Data.DATA3, "old3");
        values.put(Data.DATA4, "old4");
        values.put(Data.DATA5, "old5");
        values.put(Data.DATA6, "old6");
        values.put(Data.DATA7, "old7");
        values.put(Data.DATA8, "old8");
        values.put(Data.DATA9, "old9");
        values.put(Data.DATA10, "old10");
        values.put(Data.DATA11, "old11");
        values.put(Data.DATA12, "old12");
        values.put(Data.DATA13, "old13");
        values.put(Data.DATA14, "old14");
        values.put(Data.DATA15, "old15");
        values.put(Data.CARRIER_PRESENCE, 0);
        values.put(Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME, "oldcomponentname");
        values.put(Data.PREFERRED_PHONE_ACCOUNT_ID, "oldid");
        Uri uri = mResolver.insert(Data.CONTENT_URI, values);
        assertStoredValues(uri, values);
        assertNetworkNotified(true);

        values.clear();
        values.put(Data.RES_PACKAGE, "newpackage");
        values.put(Data.IS_PRIMARY, 0);
        values.put(Data.IS_SUPER_PRIMARY, 0);
        values.put(Data.DATA1, "new1");
        values.put(Data.DATA2, "new2");
        values.put(Data.DATA3, "new3");
        values.put(Data.DATA4, "new4");
        values.put(Data.DATA5, "new5");
        values.put(Data.DATA6, "new6");
        values.put(Data.DATA7, "new7");
        values.put(Data.DATA8, "new8");
        values.put(Data.DATA9, "new9");
        values.put(Data.DATA10, "new10");
        values.put(Data.DATA11, "new11");
        values.put(Data.DATA12, "new12");
        values.put(Data.DATA13, "new13");
        values.put(Data.DATA14, "new14");
        values.put(Data.DATA15, "new15");
        values.put(Data.CARRIER_PRESENCE, Data.CARRIER_PRESENCE_VT_CAPABLE);
        values.put(Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME, "newcomponentname");
        values.put(Data.PREFERRED_PHONE_ACCOUNT_ID, "newid");
        mResolver.update(Data.CONTENT_URI, values, Data.RAW_CONTACT_ID + "=" + rawContactId +
                " AND " + Data.MIMETYPE + "='testmimetype'", null);
        assertNetworkNotified(true);

        assertStoredValues(uri, values);

        int count = mResolver.delete(Data.CONTENT_URI, Data.RAW_CONTACT_ID + "=" + rawContactId
                + " AND " + Data.MIMETYPE + "='testmimetype'", null);
        assertEquals(1, count);
        assertEquals(0, getCount(Data.CONTENT_URI, Data.RAW_CONTACT_ID + "=" + rawContactId
                + " AND " + Data.MIMETYPE + "='testmimetype'", null));
        assertNetworkNotified(true);
    }

    public void testRawContactQuery() {
        Account account1 = new Account("a", "b");
        Account account2 = new Account("c", "d");
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, account1);
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, account2);

        Uri uri1 = TestUtil.maybeAddAccountQueryParameters(RawContacts.CONTENT_URI, account1);
        Uri uri2 = TestUtil.maybeAddAccountQueryParameters(RawContacts.CONTENT_URI, account2);
        assertEquals(1, getCount(uri1, null, null));
        assertEquals(1, getCount(uri2, null, null));
        assertStoredValue(uri1, RawContacts._ID, rawContactId1) ;
        assertStoredValue(uri2, RawContacts._ID, rawContactId2) ;

        Uri rowUri1 = ContentUris.withAppendedId(uri1, rawContactId1);
        Uri rowUri2 = ContentUris.withAppendedId(uri2, rawContactId2);
        assertStoredValue(rowUri1, RawContacts._ID, rawContactId1) ;
        assertStoredValue(rowUri2, RawContacts._ID, rawContactId2) ;
    }

    public void testRawContactDeletion() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        insertImHandle(rawContactId, Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com");
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com",
                StatusUpdates.AVAILABLE, null,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        long contactId = queryContactId(rawContactId);

        assertEquals(1, getCount(Uri.withAppendedPath(uri, RawContacts.Data.CONTENT_DIRECTORY),
                null, null));
        assertEquals(1, getCount(StatusUpdates.CONTENT_URI, PresenceColumns.RAW_CONTACT_ID + "="
                + rawContactId, null));

        mResolver.delete(uri, null, null);

        assertStoredValue(uri, RawContacts.DELETED, "1");
        assertNetworkNotified(true);

        Uri permanentDeletionUri = setCallerIsSyncAdapter(uri, mAccount);
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
        assertEquals(0, getCount(Uri.withAppendedPath(uri, RawContacts.Data.CONTENT_DIRECTORY),
                null, null));
        assertEquals(0, getCount(StatusUpdates.CONTENT_URI, PresenceColumns.RAW_CONTACT_ID + "="
                + rawContactId, null));
        assertEquals(0, getCount(Contacts.CONTENT_URI, Contacts._ID + "=" + contactId, null));
        assertNetworkNotified(false);
    }

    public void testRawContactDeletionKeepingAggregateContact() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, mAccount);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, mAccount);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);

        Uri uri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1);
        Uri permanentDeletionUri = setCallerIsSyncAdapter(uri, mAccount);
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
        assertEquals(1, getCount(Contacts.CONTENT_URI, Contacts._ID + "=" + contactId, null));
    }

    public void testRawContactDeletion_byAccountParam() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        insertImHandle(rawContactId, Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com");
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com",
                StatusUpdates.AVAILABLE, null,
                StatusUpdates.CAPABILITY_HAS_CAMERA);
        assertEquals(1, getCount(Uri.withAppendedPath(uri, RawContacts.Data.CONTENT_DIRECTORY),
                null, null));
        assertEquals(1, getCount(StatusUpdates.CONTENT_URI, PresenceColumns.RAW_CONTACT_ID + "="
                + rawContactId, null));

        // Do not delete if we are deleting with wrong account.
        Uri deleteWithWrongAccountUri =
            RawContacts.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.RawContacts.ACCOUNT_NAME, mAccountTwo.name)
                .appendQueryParameter(ContactsContract.RawContacts.ACCOUNT_TYPE, mAccountTwo.type)
                .build();
        int numDeleted = mResolver.delete(deleteWithWrongAccountUri, null, null);
        assertEquals(0, numDeleted);

        assertStoredValue(uri, RawContacts.DELETED, "0");

        // Delete if we are deleting with correct account.
        Uri deleteWithCorrectAccountUri =
            RawContacts.CONTENT_URI.buildUpon()
                .appendQueryParameter(ContactsContract.RawContacts.ACCOUNT_NAME, mAccount.name)
                .appendQueryParameter(ContactsContract.RawContacts.ACCOUNT_TYPE, mAccount.type)
                .build();
        numDeleted = mResolver.delete(deleteWithCorrectAccountUri, null, null);
        assertEquals(1, numDeleted);

        assertStoredValue(uri, RawContacts.DELETED, "1");
    }

    public void testRawContactDeletion_byAccountSelection() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        // Do not delete if we are deleting with wrong account.
        int numDeleted = mResolver.delete(RawContacts.CONTENT_URI,
                RawContacts.ACCOUNT_NAME + "=? AND " + RawContacts.ACCOUNT_TYPE + "=?",
                new String[] {mAccountTwo.name, mAccountTwo.type});
        assertEquals(0, numDeleted);

        assertStoredValue(uri, RawContacts.DELETED, "0");

        // Delete if we are deleting with correct account.
        numDeleted = mResolver.delete(RawContacts.CONTENT_URI,
                RawContacts.ACCOUNT_NAME + "=? AND " + RawContacts.ACCOUNT_TYPE + "=?",
                new String[] {mAccount.name, mAccount.type});
        assertEquals(1, numDeleted);

        assertStoredValue(uri, RawContacts.DELETED, "1");
    }

    /**
     * Test for {@link ContactsProvider2#stringToAccounts} and
     * {@link ContactsProvider2#accountsToString}.
     */
    public void testAccountsToString() {
        final Set<Account> EXPECTED_0 = Sets.newHashSet();
        final Set<Account> EXPECTED_1 = Sets.newHashSet(TestUtil.ACCOUNT_1);
        final Set<Account> EXPECTED_2 = Sets.newHashSet(TestUtil.ACCOUNT_2);
        final Set<Account> EXPECTED_1_2 = Sets.newHashSet(TestUtil.ACCOUNT_1, TestUtil.ACCOUNT_2);

        final Set<Account> ACTUAL_0 = Sets.newHashSet();
        final Set<Account> ACTUAL_1 = Sets.newHashSet(TestUtil.ACCOUNT_1);
        final Set<Account> ACTUAL_2 = Sets.newHashSet(TestUtil.ACCOUNT_2);
        final Set<Account> ACTUAL_1_2 = Sets.newHashSet(TestUtil.ACCOUNT_2, TestUtil.ACCOUNT_1);

        assertTrue(EXPECTED_0.equals(accountsToStringToAccounts(ACTUAL_0)));
        assertFalse(EXPECTED_0.equals(accountsToStringToAccounts(ACTUAL_1)));
        assertFalse(EXPECTED_0.equals(accountsToStringToAccounts(ACTUAL_2)));
        assertFalse(EXPECTED_0.equals(accountsToStringToAccounts(ACTUAL_1_2)));

        assertFalse(EXPECTED_1.equals(accountsToStringToAccounts(ACTUAL_0)));
        assertTrue(EXPECTED_1.equals(accountsToStringToAccounts(ACTUAL_1)));
        assertFalse(EXPECTED_1.equals(accountsToStringToAccounts(ACTUAL_2)));
        assertFalse(EXPECTED_1.equals(accountsToStringToAccounts(ACTUAL_1_2)));

        assertFalse(EXPECTED_2.equals(accountsToStringToAccounts(ACTUAL_0)));
        assertFalse(EXPECTED_2.equals(accountsToStringToAccounts(ACTUAL_1)));
        assertTrue(EXPECTED_2.equals(accountsToStringToAccounts(ACTUAL_2)));
        assertFalse(EXPECTED_2.equals(accountsToStringToAccounts(ACTUAL_1_2)));

        assertFalse(EXPECTED_1_2.equals(accountsToStringToAccounts(ACTUAL_0)));
        assertFalse(EXPECTED_1_2.equals(accountsToStringToAccounts(ACTUAL_1)));
        assertFalse(EXPECTED_1_2.equals(accountsToStringToAccounts(ACTUAL_2)));
        assertTrue(EXPECTED_1_2.equals(accountsToStringToAccounts(ACTUAL_1_2)));

        try {
            ContactsProvider2.stringToAccounts("x");
            fail("Didn't throw for malformed input");
        } catch (IllegalArgumentException expected) {
        }
    }

    private static final Set<Account> accountsToStringToAccounts(Set<Account> accounts) {
        return ContactsProvider2.stringToAccounts(ContactsProvider2.accountsToString(accounts));
    }

    /**
     * Test for {@link ContactsProvider2#haveAccountsChanged} and
     * {@link ContactsProvider2#saveAccounts}.
     */
    public void testHaveAccountsChanged() {
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();

        final Account[] ACCOUNTS_0 = new Account[] {};
        final Account[] ACCOUNTS_1 = new Account[] {TestUtil.ACCOUNT_1};
        final Account[] ACCOUNTS_2 = new Account[] {TestUtil.ACCOUNT_2};
        final Account[] ACCOUNTS_1_2 = new Account[] {TestUtil.ACCOUNT_1, TestUtil.ACCOUNT_2};
        final Account[] ACCOUNTS_2_1 = new Account[] {TestUtil.ACCOUNT_2, TestUtil.ACCOUNT_1};

        // Add ACCOUNT_1

        assertTrue(cp.haveAccountsChanged(ACCOUNTS_1));
        cp.saveAccounts(ACCOUNTS_1);
        assertFalse(cp.haveAccountsChanged(ACCOUNTS_1));

        // Add ACCOUNT_2

        assertTrue(cp.haveAccountsChanged(ACCOUNTS_1_2));
        // (try with reverse order)
        assertTrue(cp.haveAccountsChanged(ACCOUNTS_2_1));
        cp.saveAccounts(ACCOUNTS_1_2);
        assertFalse(cp.haveAccountsChanged(ACCOUNTS_1_2));
        // (try with reverse order)
        assertFalse(cp.haveAccountsChanged(ACCOUNTS_2_1));

        // Remove ACCOUNT_1

        assertTrue(cp.haveAccountsChanged(ACCOUNTS_2));
        cp.saveAccounts(ACCOUNTS_2);
        assertFalse(cp.haveAccountsChanged(ACCOUNTS_2));

        // Remove ACCOUNT_2

        assertTrue(cp.haveAccountsChanged(ACCOUNTS_0));
        cp.saveAccounts(ACCOUNTS_0);
        assertFalse(cp.haveAccountsChanged(ACCOUNTS_0));

        // Test with malformed DB property.

        final ContactsDatabaseHelper dbHelper = cp.getThreadActiveDatabaseHelperForTest();
        dbHelper.setProperty(DbProperties.KNOWN_ACCOUNTS, "x");

        // With malformed property the method always return true.
        assertTrue(cp.haveAccountsChanged(ACCOUNTS_0));
        assertTrue(cp.haveAccountsChanged(ACCOUNTS_1));
    }

    public void testAccountsUpdated() {
        // This is to ensure we do not delete contacts with null, null (account name, type)
        // accidentally.
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver, "James", "Sullivan");
        insertPhoneNumber(rawContactId3, "5234567890");
        Uri rawContact3 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId3);
        assertEquals(1, getCount(RawContacts.CONTENT_URI, null, null));

        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{mAccount, mAccountTwo});
        cp.onAccountsUpdated(new Account[]{mAccount, mAccountTwo});
        assertEquals(1, getCount(RawContacts.CONTENT_URI, null, null));
        assertStoredValue(rawContact3, RawContacts.ACCOUNT_NAME, null);
        assertStoredValue(rawContact3, RawContacts.ACCOUNT_TYPE, null);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, mAccount);
        insertEmail(rawContactId1, "account1@email.com");
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, mAccountTwo);
        insertEmail(rawContactId2, "account2@email.com");
        insertImHandle(rawContactId2, Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com");
        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, "deleteme@android.com",
                StatusUpdates.AVAILABLE, null,
                StatusUpdates.CAPABILITY_HAS_CAMERA);

        mActor.setAccounts(new Account[]{mAccount});
        cp.onAccountsUpdated(new Account[]{mAccount});
        assertEquals(2, getCount(RawContacts.CONTENT_URI, null, null));
        assertEquals(0, getCount(StatusUpdates.CONTENT_URI, PresenceColumns.RAW_CONTACT_ID + "="
                + rawContactId2, null));
    }

    public void testAccountDeletion() {
        Account readOnlyAccount = new Account("act", READ_ONLY_ACCOUNT_TYPE);
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{readOnlyAccount, mAccount});
        cp.onAccountsUpdated(new Account[]{readOnlyAccount, mAccount});

        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                readOnlyAccount);
        Uri photoUri1 = insertPhoto(rawContactId1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "john", "doe",
                mAccount);
        Uri photoUri2 = insertPhoto(rawContactId2);
        storeValue(photoUri2, Photo.IS_SUPER_PRIMARY, "1");

        assertAggregated(rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);

        // The display name should come from the writable account
        assertStoredValue(Uri.withAppendedPath(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.Data.CONTENT_DIRECTORY),
                Contacts.DISPLAY_NAME, "john doe");

        // The photo should be the one we marked as super-primary
        assertStoredValue(Contacts.CONTENT_URI, contactId,
                Contacts.PHOTO_ID, ContentUris.parseId(photoUri2));

        mActor.setAccounts(new Account[]{readOnlyAccount});
        // Remove the writable account
        cp.onAccountsUpdated(new Account[]{readOnlyAccount});

        // The display name should come from the remaining account
        assertStoredValue(Uri.withAppendedPath(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.Data.CONTENT_DIRECTORY),
                Contacts.DISPLAY_NAME, "John Doe");

        // The photo should be the remaining one
        assertStoredValue(Contacts.CONTENT_URI, contactId,
                Contacts.PHOTO_ID, ContentUris.parseId(photoUri1));
    }

    public void testStreamItemsCleanedUpOnAccountRemoval() {
        Account doomedAccount = new Account("doom", "doom");
        Account safeAccount = mAccount;
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{doomedAccount, safeAccount});
        cp.onAccountsUpdated(new Account[]{doomedAccount, safeAccount});

        // Create a doomed raw contact, stream item, and photo.
        long doomedRawContactId = RawContactUtil.createRawContactWithName(mResolver, doomedAccount);
        Uri doomedStreamItemUri =
                insertStreamItem(doomedRawContactId, buildGenericStreamItemValues(), doomedAccount);
        long doomedStreamItemId = ContentUris.parseId(doomedStreamItemUri);
        Uri doomedStreamItemPhotoUri = insertStreamItemPhoto(
                doomedStreamItemId, buildGenericStreamItemPhotoValues(0), doomedAccount);

        // Create a safe raw contact, stream item, and photo.
        long safeRawContactId = RawContactUtil.createRawContactWithName(mResolver, safeAccount);
        Uri safeStreamItemUri =
                insertStreamItem(safeRawContactId, buildGenericStreamItemValues(), safeAccount);
        long safeStreamItemId = ContentUris.parseId(safeStreamItemUri);
        Uri safeStreamItemPhotoUri = insertStreamItemPhoto(
                safeStreamItemId, buildGenericStreamItemPhotoValues(0), safeAccount);
        long safeStreamItemPhotoId = ContentUris.parseId(safeStreamItemPhotoUri);

        // Remove the doomed account.
        mActor.setAccounts(new Account[]{safeAccount});
        cp.onAccountsUpdated(new Account[]{safeAccount});

        // Check that the doomed stuff has all been nuked.
        ContentValues[] noValues = new ContentValues[0];
        assertStoredValues(ContentUris.withAppendedId(RawContacts.CONTENT_URI, doomedRawContactId),
                noValues);
        assertStoredValues(doomedStreamItemUri, noValues);
        assertStoredValues(doomedStreamItemPhotoUri, noValues);

        // Check that the safe stuff lives on.
        assertStoredValue(RawContacts.CONTENT_URI, safeRawContactId, RawContacts._ID,
                safeRawContactId);
        assertStoredValue(safeStreamItemUri, StreamItems._ID, safeStreamItemId);
        assertStoredValue(safeStreamItemPhotoUri, StreamItemPhotos._ID, safeStreamItemPhotoId);
    }

    public void testMetadataSyncCleanedUpOnAccountRemoval() throws Exception {
        Account doomedAccount = new Account("doom", "doom");
        createAccount(doomedAccount.name, doomedAccount.type, null);
        Account safeAccount = new Account("safe", "safe");
        createAccount(safeAccount.name, safeAccount.type, null);
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{doomedAccount, safeAccount});
        cp.onAccountsUpdated(new Account[]{doomedAccount, safeAccount});

        ContactMetadataProvider contactMetadataProvider = addProvider(
                ContactMetadataProviderTestable.class, MetadataSync.METADATA_AUTHORITY);
        // Reset the dbHelper to be the one ContactsProvider2 is using. Before this, two providers
        // are using different dbHelpers.
        contactMetadataProvider.setDatabaseHelper(((SynchronousContactsProvider2)
                mActor.provider).getDatabaseHelper());

        // Create a doomed metadata.
        String backupId = "backupIdForDoomed";
        ContentValues metadataValues = new ContentValues();
        metadataValues.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId);
        metadataValues.put(MetadataSync.ACCOUNT_TYPE, doomedAccount.type);
        metadataValues.put(MetadataSync.ACCOUNT_NAME, doomedAccount.name);
        metadataValues.put(MetadataSync.DATA,
                getDefaultMetadataJSONString(doomedAccount.type, doomedAccount.name, backupId));
        Uri doomedMetadataUri = mResolver.insert(MetadataSync.CONTENT_URI, metadataValues);
        // Create a doomed metadata sync state.
        ContentValues syncStateValues = new ContentValues();
        syncStateValues.put(MetadataSyncState.ACCOUNT_TYPE, doomedAccount.type);
        syncStateValues.put(MetadataSyncState.ACCOUNT_NAME, doomedAccount.name);
        syncStateValues.put(MetadataSyncState.STATE, "syncState");
        mResolver.insert(MetadataSyncState.CONTENT_URI, syncStateValues);

        // Create a safe metadata.
        String backupId2 = "backupIdForSafe";
        ContentValues insertedValues2 = new ContentValues();
        insertedValues2.put(MetadataSync.RAW_CONTACT_BACKUP_ID, backupId2);
        insertedValues2.put(MetadataSync.ACCOUNT_TYPE, safeAccount.type);
        insertedValues2.put(MetadataSync.ACCOUNT_NAME, safeAccount.name);
        insertedValues2.put(MetadataSync.DATA,
                getDefaultMetadataJSONString(safeAccount.type, safeAccount.name, backupId2));
        Uri safeMetadataUri = mResolver.insert(MetadataSync.CONTENT_URI, insertedValues2);
        // Create a safe metadata sync state.
        ContentValues syncStateValues2 = new ContentValues();
        syncStateValues2.put(MetadataSyncState.ACCOUNT_TYPE, safeAccount.type);
        syncStateValues2.put(MetadataSyncState.ACCOUNT_NAME, safeAccount.name);
        syncStateValues2.put(MetadataSyncState.STATE, "syncState2");
        mResolver.insert(MetadataSyncState.CONTENT_URI, syncStateValues2);

        // Remove the doomed account.
        mActor.setAccounts(new Account[]{safeAccount});
        cp.onAccountsUpdated(new Account[]{safeAccount});

        // Check that the doomed stuff has all been nuked.
        ContentValues[] noValues = new ContentValues[0];
        assertStoredValues(doomedMetadataUri, noValues);
        String selection = MetadataSyncState.ACCOUNT_NAME + "=?1 AND "
                + MetadataSyncState.ACCOUNT_TYPE + "=?2";
        String[] args = new String[]{doomedAccount.name, doomedAccount.type};
        final String[] projection = new String[]{MetadataSyncState.STATE};
        Cursor c = mResolver.query(MetadataSyncState.CONTENT_URI, projection, selection, args,
                null);
        assertEquals(0, c.getCount());

        // Check that the safe stuff lives on.
        assertStoredValue(safeMetadataUri, MetadataSync.RAW_CONTACT_BACKUP_ID, backupId2);
        args = new String[]{safeAccount.name, safeAccount.type};
        c = mResolver.query(MetadataSyncState.CONTENT_URI, projection, selection, args,
                null);
        assertEquals(1, c.getCount());
        c.moveToNext();
        assertEquals("syncState2", c.getString(0));
        c.close();
    }

    private String getDefaultMetadataJSONString(
            String accountType, String accountName, String backupId) {
        return "{\n" +
                "  \"unique_contact_id\": {\n" +
                "    \"account_type\": \"CUSTOM_ACCOUNT\",\n" +
                "    \"custom_account_type\": " + accountType + ",\n" +
                "    \"account_name\": " + accountName + ",\n" +
                "    \"contact_id\": " + backupId + ",\n" +
                "    \"data_set\": \"FOCUS\"\n" +
                "  },\n" +
                "  \"contact_prefs\": {\n" +
                "    \"send_to_voicemail\": true,\n" +
                "    \"starred\": true,\n" +
                "    \"pinned\": 1\n" +
                "  }\n" +
                "  }";
    }

    public void testContactDeletion() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                TestUtil.ACCOUNT_1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver, "John", "Doe",
                TestUtil.ACCOUNT_2);

        long contactId = queryContactId(rawContactId1);

        mResolver.delete(ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId), null, null);

        assertStoredValue(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1),
                RawContacts.DELETED, "1");
        assertStoredValue(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2),
                RawContacts.DELETED, "1");
    }

    public void testMarkAsDirtyParameter() {
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        Uri uri = DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        clearDirty(rawContactUri);
        Uri updateUri = setCallerIsSyncAdapter(uri, mAccount);

        ContentValues values = new ContentValues();
        values.put(StructuredName.FAMILY_NAME, "Dough");
        mResolver.update(updateUri, values, null, null);
        assertStoredValue(uri, StructuredName.FAMILY_NAME, "Dough");
        assertDirty(rawContactUri, false);
        assertNetworkNotified(false);
    }

    public void testDirtyWhenRawContactInsert() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        // When inserting a rawcontact without metadata.
        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertDirty(rawContactUri, false);
        assertMetadataDirty(rawContactUri, false);
        assertNetworkNotified(true);
        assertMetadataNetworkNotified(false);

        // When inserting a rawcontact with metadata.
        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.STARRED, 1);
        values.put(ContactsContract.RawContacts.ACCOUNT_NAME, mAccount.name);
        values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, mAccount.type);
        Uri rawContactId2Uri = mResolver.insert(RawContacts.CONTENT_URI, values);
        assertDirty(rawContactId2Uri, false);
        assertMetadataDirty(rawContactId2Uri, false);
        assertNetworkNotified(true);
        assertMetadataNetworkNotified(false);
    }

    public void testRawContactDirtyAndVersion() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        final long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI, rawContactId);
        assertDirty(uri, false);
        long version = getVersion(uri);

        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(ContactsContract.RawContacts.AGGREGATION_MODE,
                RawContacts.AGGREGATION_MODE_IMMEDIATE);
        assertEquals(1, mResolver.update(uri, values, null, null));
        assertEquals(version, getVersion(uri));

        assertDirty(uri, false);
        assertNetworkNotified(false);
        assertMetadataDirty(uri, false);
        assertMetadataNetworkNotified(false);

        Uri emailUri = insertEmail(rawContactId, "goo@woo.com");
        assertDirty(uri, true);
        assertNetworkNotified(true);
        ++version;
        assertEquals(version, getVersion(uri));
        clearDirty(uri);

        values = new ContentValues();
        values.put(Email.DATA, "goo@hoo.com");
        mResolver.update(emailUri, values, null, null);
        assertDirty(uri, true);
        assertNetworkNotified(true);
        ++version;
        assertEquals(version, getVersion(uri));
        clearDirty(uri);

        mResolver.delete(emailUri, null, null);
        assertDirty(uri, true);
        assertNetworkNotified(true);
        ++version;
        assertEquals(version, getVersion(uri));
    }

    public void testRawContactClearDirty() {
        final long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);
        long version = getVersion(uri);
        insertEmail(rawContactId, "goo@woo.com");
        assertDirty(uri, true);
        version++;
        assertEquals(version, getVersion(uri));

        clearDirty(uri);
        assertDirty(uri, false);
        assertEquals(version, getVersion(uri));
    }

    public void testRawContactDeletionSetsDirty() {
        final long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);
        long version = getVersion(uri);
        clearDirty(uri);
        assertDirty(uri, false);

        mResolver.delete(uri, null, null);
        assertStoredValue(uri, RawContacts.DELETED, "1");
        assertDirty(uri, true);
        assertNetworkNotified(true);
        version++;
        assertEquals(version, getVersion(uri));
    }

    public void testNotifyMetadataChangeForRawContactInsertBySyncAdapter() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        Uri uri = RawContacts.CONTENT_URI.buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, mAccount.name)
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, mAccount.type)
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, true + "")
                .build();

        long rawContactId = ContentUris.parseId(mResolver.insert(uri, new ContentValues()));
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertMetadataDirty(rawContactUri, false);
        // Will not notify Lychee since it's removed.
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForRawContactMetadataChange() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        ContentValues values = new ContentValues();
        values.put(Contacts.STARRED, 1);
        mResolver.update(contactUri, values, null, null);
        assertStoredValue(contactUri, Contacts.STARRED, 1);

        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        assertMetadataDirty(rawContactUri, false);
        assertMetadataNetworkNotified(false);

        clearMetadataDirty(rawContactUri);
        values = new ContentValues();
        values.put(Contacts.PINNED, 1);
        mResolver.update(contactUri, values, null, null);
        assertStoredValue(contactUri, Contacts.PINNED, 1);

        assertMetadataDirty(rawContactUri, false);
        assertMetadataNetworkNotified(false);

        clearMetadataDirty(rawContactUri);
        values = new ContentValues();
        values.put(Contacts.SEND_TO_VOICEMAIL, 1);
        mResolver.update(contactUri, values, null, null);
        assertStoredValue(contactUri, Contacts.SEND_TO_VOICEMAIL, 1);

        assertMetadataDirty(rawContactUri, false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForRawContactBackupIdChange() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId = RawContactUtil.createRawContact(mResolver, mAccount);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        // Make a metadata change to set metadata_dirty.
        ContentValues values = new ContentValues();
        values.put(RawContacts.SEND_TO_VOICEMAIL, "1");
        mResolver.update(rawContactUri, values, null, null);
        assertMetadataDirty(rawContactUri, false);

        // Update the backup_id and check metadata network should be notified.
        values = new ContentValues();
        values.put(RawContacts.BACKUP_ID, "newBackupId");
        mResolver.update(rawContactUri, values, null, null);
        assertStoredValue(rawContactUri, RawContacts.BACKUP_ID, "newBackupId");
        assertMetadataDirty(rawContactUri, false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForAggregationExceptionChange() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, new Account("b", "b"));

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1),
                false);
        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2),
                false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataNotDirtyForUsageStatsChange() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        final long rid1 = RawContactUtil.createRawContactWithName(mResolver, "contact", "a");
        final long did1a = ContentUris.parseId(insertEmail(rid1, "email_1_a@email.com"));
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1a);

        // Usage feedback no longer works, so "false".
        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rid1), false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForDataPrimarySettingInsert() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId1 = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri mailUri11 = insertEmail(rawContactId1, "test1@domain1.com", true, true);

        assertStoredValue(mailUri11, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri11, Data.IS_SUPER_PRIMARY, 1);
        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1),
                false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForDataPrimarySettingUpdate() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri mailUri1 = insertEmail(rawContactId, "test1@domain1.com");

        assertStoredValue(mailUri1, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri1, Data.IS_SUPER_PRIMARY, 0);

        ContentValues values = new ContentValues();
        values.put(Data.IS_SUPER_PRIMARY, 1);
        mResolver.update(mailUri1, values, null, null);

        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                false);
        assertMetadataNetworkNotified(false);
    }

    public void testMarkAsMetadataDirtyForDataDelete() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        long rawContactId = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri mailUri1 = insertEmail(rawContactId, "test1@domain1.com", true, true);

        mResolver.delete(mailUri1, null, null);

        assertMetadataDirty(ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                false);
        assertMetadataNetworkNotified(false);
    }

    public void testDeleteContactWithoutName() {
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, new ContentValues());
        long rawContactId = ContentUris.parseId(rawContactUri);

        Uri phoneUri = insertPhoneNumber(rawContactId, "555-123-45678", true);

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri lookupUri = Contacts.getLookupUri(mResolver, contactUri);

        int numDeleted = mResolver.delete(lookupUri, null, null);
        assertEquals(1, numDeleted);
    }

    public void testDeleteContactWithoutAnyData() {
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, new ContentValues());
        long rawContactId = ContentUris.parseId(rawContactUri);

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri lookupUri = Contacts.getLookupUri(mResolver, contactUri);

        int numDeleted = mResolver.delete(lookupUri, null, null);
        assertEquals(1, numDeleted);
    }

    public void testDeleteContactWithEscapedUri() {
        ContentValues values = new ContentValues();
        values.put(RawContacts.SOURCE_ID, "!@#$%^&*()_+=-/.,<>?;'\":[]}{\\|`~");
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri lookupUri = Contacts.getLookupUri(mResolver, contactUri);
        assertEquals(1, mResolver.delete(lookupUri, null, null));
    }

    public void testDeleteContactComposedOfSingleLocalRawContact() {
        // Create a raw contact in the local (null) account
        long rawContactId = RawContactUtil.createRawContact(mResolver, null);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Smith");

        // Delete the contact
        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertEquals(1, mResolver.delete(contactUri, null, null));

        // Assert that the raw contact was removed
        Cursor c1 = queryRawContact(rawContactId);
        assertEquals(0, c1.getCount());
        c1.close();

        // Assert that the contact was removed
        Cursor c2 = mResolver.query(contactUri, null, null, null, "");
        assertEquals(0, c2.getCount());
        c2.close();
    }

    public void testDeleteContactComposedOfTwoLocalRawContacts() {
        // Create a raw contact in the local (null) account
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, null);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "John", "Smith");

        // Create another local raw contact with the same name
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, null);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "John", "Smith");

        // Join the two raw contacts explicitly
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        // Check that the two raw contacts are aggregated together
        assertAggregated(rawContactId1, rawContactId2, "John Smith");

        // Delete the aggregate contact
        long contactId = queryContactId(rawContactId1);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertEquals(1, mResolver.delete(contactUri, null, null));

        // Assert that both of the local raw contacts were removed completely
        Cursor c1 = queryRawContact(rawContactId1);
        assertEquals(0, c1.getCount());
        c1.close();

        Cursor c2 = queryRawContact(rawContactId2);
        assertEquals(0, c2.getCount());
        c2.close();

        // Assert that the contact was removed
        Cursor c3 = queryContact(contactId);
        assertEquals(0, c3.getCount());
        c3.close();
    }

    public void testDeleteContactComposedOfSomeLocalRawContacts() {
        // Create a raw contact in the local (null) account
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, null);
        DataUtil.insertStructuredName(mResolver, rawContactId1, "John", "Smith");

        // Create another one in a non-local account with the same name
        long rawContactId2 = RawContactUtil.createRawContact(mResolver, mAccount);
        DataUtil.insertStructuredName(mResolver, rawContactId2, "John", "Smith");

        // Check that the two new raw contacts are aggregated together
        assertAggregated(rawContactId1, rawContactId2, "John Smith");

        // Delete the aggregate contact
        long contactId = queryContactId(rawContactId1);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertEquals(1, mResolver.delete(contactUri, null, null));

        // Assert that the local raw contact was removed completely
        Cursor c1 = queryRawContact(rawContactId1);
        assertEquals(0, c1.getCount());
        c1.close();

        // Assert that the non-local raw contact is still present just marked as deleted
        Cursor c2 = queryRawContact(rawContactId2);
        assertEquals(1, c2.getCount());
        assertTrue(c2.moveToFirst());
        assertEquals(1, c2.getInt(c2.getColumnIndex(RawContacts.DELETED)));
        c2.close();

        // Assert that the contact was removed
        Cursor c3 = queryContact(contactId);
        assertEquals(0, c3.getCount());
        c3.close();
    }

    public void testQueryContactWithEscapedUri() {
        ContentValues values = new ContentValues();
        values.put(RawContacts.SOURCE_ID, "!@#$%^&*()_+=-/.,<>?;'\":[]}{\\|`~");
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri lookupUri = Contacts.getLookupUri(mResolver, contactUri);
        Cursor c = mResolver.query(lookupUri, null, null, null, "");
        assertEquals(1, c.getCount());
        c.close();
    }

    public void testGetPhotoUri() {
        ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");
        long dataId = ContentUris.parseId(insertPhoto(rawContactId, R.drawable.earth_normal));
        long photoFileId = getStoredLongValue(Data.CONTENT_URI, Data._ID + "=?",
                new String[]{String.valueOf(dataId)}, Photo.PHOTO_FILE_ID);
        String photoUri = ContentUris.withAppendedId(DisplayPhoto.CONTENT_URI, photoFileId)
                .toString();

        assertStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, queryContactId(rawContactId)),
                Contacts.PHOTO_URI, photoUri);
    }

    public void testGetPhotoViaLookupUri() throws IOException {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        Uri lookupUri = Contacts.getLookupUri(mResolver, contactUri);
        String lookupKey = lookupUri.getPathSegments().get(2);
        insertPhoto(rawContactId, R.drawable.earth_small);
        byte[] thumbnail = loadPhotoFromResource(R.drawable.earth_small, PhotoSize.THUMBNAIL);

        // Two forms of lookup key URIs should be valid - one with the contact ID, one without.
        Uri photoLookupUriWithId = Uri.withAppendedPath(lookupUri, "photo");
        Uri photoLookupUriWithoutId = Contacts.CONTENT_LOOKUP_URI.buildUpon()
                .appendPath(lookupKey).appendPath("photo").build();

        // Try retrieving as a data record.
        ContentValues values = new ContentValues();
        values.put(Photo.PHOTO, thumbnail);
        assertStoredValues(photoLookupUriWithId, values);
        assertStoredValues(photoLookupUriWithoutId, values);

        // Try opening as an input stream.
        EvenMoreAsserts.assertImageRawData(getContext(),
                thumbnail, mResolver.openInputStream(photoLookupUriWithId));
        EvenMoreAsserts.assertImageRawData(getContext(),
                thumbnail, mResolver.openInputStream(photoLookupUriWithoutId));
    }

    public void testInputStreamForPhoto() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        insertPhoto(rawContactId);
        Uri photoUri = Uri.parse(getStoredValue(contactUri, Contacts.PHOTO_URI));
        Uri photoThumbnailUri = Uri.parse(getStoredValue(contactUri, Contacts.PHOTO_THUMBNAIL_URI));

        // Check the thumbnail.
        EvenMoreAsserts.assertImageRawData(getContext(), loadTestPhoto(PhotoSize.THUMBNAIL),
                mResolver.openInputStream(photoThumbnailUri));

        // Then check the display photo.  Note because we only inserted a small photo, but not a
        // display photo, this returns the thumbnail image itself, which was compressed at
        // the thumnail compression rate, which is why we compare to
        // loadTestPhoto(PhotoSize.THUMBNAIL) rather than loadTestPhoto(PhotoSize.DISPLAY_PHOTO)
        // here.
        // (In other words, loadTestPhoto(PhotoSize.DISPLAY_PHOTO) returns the same photo as
        // loadTestPhoto(PhotoSize.THUMBNAIL), except it's compressed at a lower compression rate.)
        EvenMoreAsserts.assertImageRawData(getContext(), loadTestPhoto(PhotoSize.THUMBNAIL),
                mResolver.openInputStream(photoUri));
    }

    public void testSuperPrimaryPhoto() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri photoUri1 = insertPhoto(rawContactId1, R.drawable.earth_normal);
        long photoId1 = ContentUris.parseId(photoUri1);

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, new Account("b", "b"));
        Uri photoUri2 = insertPhoto(rawContactId2, R.drawable.earth_normal);
        long photoId2 = ContentUris.parseId(photoUri2);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId1));

        long photoFileId1 = getStoredLongValue(Data.CONTENT_URI, Data._ID + "=?",
                new String[]{String.valueOf(photoId1)}, Photo.PHOTO_FILE_ID);
        String photoUri = ContentUris.withAppendedId(DisplayPhoto.CONTENT_URI, photoFileId1)
                .toString();
        assertStoredValue(contactUri, Contacts.PHOTO_ID, photoId1);
        assertStoredValue(contactUri, Contacts.PHOTO_URI, photoUri);

        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE,
                rawContactId1, rawContactId2);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);
        ContentValues values = new ContentValues();
        values.put(Data.IS_SUPER_PRIMARY, 1);
        mResolver.update(photoUri2, values, null, null);

        contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId1));
        assertStoredValue(contactUri, Contacts.PHOTO_ID, photoId2);

        mResolver.update(photoUri1, values, null, null);
        assertStoredValue(contactUri, Contacts.PHOTO_ID, photoId1);
    }

    public void testUpdatePhoto() {
        ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);
        DataUtil.insertStructuredName(mResolver, rawContactId, "John", "Doe");

        Uri twigUri = Uri.withAppendedPath(ContentUris.withAppendedId(Contacts.CONTENT_URI,
                queryContactId(rawContactId)), Contacts.Photo.CONTENT_DIRECTORY);

        values.clear();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.putNull(Photo.PHOTO);
        Uri dataUri = mResolver.insert(Data.CONTENT_URI, values);
        long photoId = ContentUris.parseId(dataUri);

        assertEquals(0, getCount(twigUri, null, null));

        values.clear();
        values.put(Photo.PHOTO, loadTestPhoto());
        mResolver.update(dataUri, values, null, null);
        assertNetworkNotified(true);

        long twigId = getStoredLongValue(twigUri, Data._ID);
        assertEquals(photoId, twigId);
    }

    public void testUpdateRawContactDataPhoto() {
        // setup a contact with a null photo
        ContentValues values = new ContentValues();
        Uri rawContactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        long rawContactId = ContentUris.parseId(rawContactUri);

        // setup a photo
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.putNull(Photo.PHOTO);

        // try to do an update before insert should return count == 0
        Uri dataUri = Uri.withAppendedPath(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId),
                RawContacts.Data.CONTENT_DIRECTORY);
        assertEquals(0, mResolver.update(dataUri, values, Data.MIMETYPE + "=?",
                new String[] {Photo.CONTENT_ITEM_TYPE}));

        mResolver.insert(Data.CONTENT_URI, values);

        // save a photo to the db
        values.clear();
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.put(Photo.PHOTO, loadTestPhoto());
        assertEquals(1, mResolver.update(dataUri, values, Data.MIMETYPE + "=?",
                new String[] {Photo.CONTENT_ITEM_TYPE}));

        // verify the photo
        Cursor storedPhoto = mResolver.query(dataUri, new String[] {Photo.PHOTO},
                Data.MIMETYPE + "=?", new String[] {Photo.CONTENT_ITEM_TYPE}, null);
        storedPhoto.moveToFirst();
        MoreAsserts.assertEquals(loadTestPhoto(PhotoSize.THUMBNAIL), storedPhoto.getBlob(0));
        storedPhoto.close();
    }

    public void testOpenDisplayPhotoForContactId() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        insertPhoto(rawContactId, R.drawable.earth_normal);
        Uri photoUri = Contacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(contactId))
                .appendPath(Contacts.Photo.DISPLAY_PHOTO).build();
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(photoUri));
    }

    public void testOpenDisplayPhotoForContactLookupKey() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);
        insertPhoto(rawContactId, R.drawable.earth_normal);
        Uri photoUri = Contacts.CONTENT_LOOKUP_URI.buildUpon()
                .appendPath(lookupKey)
                .appendPath(Contacts.Photo.DISPLAY_PHOTO).build();
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(photoUri));
    }

    public void testOpenDisplayPhotoForContactLookupKeyAndId() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);
        insertPhoto(rawContactId, R.drawable.earth_normal);
        Uri photoUri = Contacts.CONTENT_LOOKUP_URI.buildUpon()
                .appendPath(lookupKey)
                .appendPath(String.valueOf(contactId))
                .appendPath(Contacts.Photo.DISPLAY_PHOTO).build();
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(photoUri));
    }

    public void testOpenDisplayPhotoForRawContactId() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        insertPhoto(rawContactId, R.drawable.earth_normal);
        Uri photoUri = RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.DisplayPhoto.CONTENT_DIRECTORY).build();
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(photoUri));
    }

    public void testOpenDisplayPhotoByPhotoUri() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        insertPhoto(rawContactId, R.drawable.earth_normal);

        // Get the photo URI out and check the content.
        String photoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_URI);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(Uri.parse(photoUri)));
    }

    public void testPhotoUriForDisplayPhoto() {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        // Photo being inserted is larger than a thumbnail, so it will be stored as a file.
        long dataId = ContentUris.parseId(insertPhoto(rawContactId, R.drawable.earth_normal));
        String photoFileId = getStoredValue(ContentUris.withAppendedId(Data.CONTENT_URI, dataId),
                Photo.PHOTO_FILE_ID);
        String photoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_URI);

        // Check that the photo URI differs from the thumbnail.
        String thumbnailUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_THUMBNAIL_URI);
        assertFalse(photoUri.equals(thumbnailUri));

        // URI should be of the form display_photo/ID
        assertEquals(Uri.withAppendedPath(DisplayPhoto.CONTENT_URI, photoFileId).toString(),
                photoUri);
    }

    public void testPhotoUriForThumbnailPhoto() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        // Photo being inserted is a thumbnail, so it will only be stored in a BLOB.  The photo URI
        // will fall back to the thumbnail URI.
        insertPhoto(rawContactId, R.drawable.earth_small);
        String photoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_URI);

        // Check that the photo URI is equal to the thumbnail URI.
        String thumbnailUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_THUMBNAIL_URI);
        assertEquals(photoUri, thumbnailUri);

        // URI should be of the form contacts/ID/photo
        assertEquals(Uri.withAppendedPath(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.Photo.CONTENT_DIRECTORY).toString(),
                photoUri);

        // Loading the photo URI content should get the thumbnail.
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_small, PhotoSize.THUMBNAIL),
                mResolver.openInputStream(Uri.parse(photoUri)));
    }

    public void testWriteNewPhotoToAssetFile() throws Exception {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        // Load in a huge photo.
        final byte[] originalPhoto = loadPhotoFromResource(
                R.drawable.earth_huge, PhotoSize.ORIGINAL);

        // Write it out.
        final Uri writeablePhotoUri = RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.DisplayPhoto.CONTENT_DIRECTORY).build();
        writePhotoAsync(writeablePhotoUri, originalPhoto);

        // Check that the display photo and thumbnail have been set.
        String photoUri = null;
        for (int i = 0; i < 10 && photoUri == null; i++) {
            // Wait a tick for the photo processing to occur.
            Thread.sleep(100);
            photoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_URI);
        }

        assertFalse(TextUtils.isEmpty(photoUri));
        String thumbnailUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_THUMBNAIL_URI);
        assertFalse(TextUtils.isEmpty(thumbnailUri));
        assertNotSame(photoUri, thumbnailUri);

        // Check the content of the display photo and thumbnail.
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(Uri.parse(photoUri)));
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.THUMBNAIL),
                mResolver.openInputStream(Uri.parse(thumbnailUri)));
    }

    public void testWriteUpdatedPhotoToAssetFile() throws Exception {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);

        // Insert a large photo first.
        insertPhoto(rawContactId, R.drawable.earth_large);
        String largeEarthPhotoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId), Contacts.PHOTO_URI);

        // Load in a huge photo.
        byte[] originalPhoto = loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.ORIGINAL);

        // Write it out.
        Uri writeablePhotoUri = RawContacts.CONTENT_URI.buildUpon()
                .appendPath(String.valueOf(rawContactId))
                .appendPath(RawContacts.DisplayPhoto.CONTENT_DIRECTORY).build();
        writePhotoAsync(writeablePhotoUri, originalPhoto);

        // Allow a second for processing to occur.
        Thread.sleep(1000);

        // Check that the display photo URI has been modified.
        String hugeEarthPhotoUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId), Contacts.PHOTO_URI);
        assertFalse(hugeEarthPhotoUri.equals(largeEarthPhotoUri));

        // Check the content of the display photo and thumbnail.
        String hugeEarthThumbnailUri = getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId),
                Contacts.PHOTO_THUMBNAIL_URI);
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.DISPLAY_PHOTO),
                mResolver.openInputStream(Uri.parse(hugeEarthPhotoUri)));
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.THUMBNAIL),
                mResolver.openInputStream(Uri.parse(hugeEarthThumbnailUri)));

    }

    private void writePhotoAsync(final Uri uri, final byte[] photoBytes) throws Exception {
        AsyncTask<Object, Object, Object> task = new AsyncTask<Object, Object, Object>() {
            @Override
            protected Object doInBackground(Object... params) {
                OutputStream os;
                try {
                    os = mResolver.openOutputStream(uri, "rw");
                    os.write(photoBytes);
                    os.close();
                    return null;
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        };
        task.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, (Object[])null).get();
    }

    public void testPhotoDimensionLimits() {
        ContentValues values = new ContentValues();
        values.put(DisplayPhoto.DISPLAY_MAX_DIM, 256);
        values.put(DisplayPhoto.THUMBNAIL_MAX_DIM, 96);
        assertStoredValues(DisplayPhoto.CONTENT_MAX_DIMENSIONS_URI, values);
    }

    public void testPhotoStoreCleanup() throws IOException {
        SynchronousContactsProvider2 provider = (SynchronousContactsProvider2) mActor.provider;
        PhotoStore photoStore = provider.getPhotoStore();

        // Trigger an initial cleanup so another one won't happen while we're running this test.
        provider.cleanupPhotoStore();

        // Insert a couple of contacts with photos.
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver);
        long contactId1 = queryContactId(rawContactId1);
        long dataId1 = ContentUris.parseId(insertPhoto(rawContactId1, R.drawable.earth_normal));
        long photoFileId1 =
                getStoredLongValue(ContentUris.withAppendedId(Data.CONTENT_URI, dataId1),
                        Photo.PHOTO_FILE_ID);

        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver);
        long contactId2 = queryContactId(rawContactId2);
        long dataId2 = ContentUris.parseId(insertPhoto(rawContactId2, R.drawable.earth_normal));
        long photoFileId2 =
                getStoredLongValue(ContentUris.withAppendedId(Data.CONTENT_URI, dataId2),
                        Photo.PHOTO_FILE_ID);

        // Update the second raw contact with a different photo.
        ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId2);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.put(Photo.PHOTO, loadPhotoFromResource(R.drawable.earth_huge, PhotoSize.ORIGINAL));
        assertEquals(1, mResolver.update(Data.CONTENT_URI, values, Data._ID + "=?",
                new String[]{String.valueOf(dataId2)}));
        long replacementPhotoFileId =
                getStoredLongValue(ContentUris.withAppendedId(Data.CONTENT_URI, dataId2),
                        Photo.PHOTO_FILE_ID);

        // Insert a third raw contact that has a bogus photo file ID.
        long bogusFileId = 1234567;
        long rawContactId3 = RawContactUtil.createRawContactWithName(mResolver);
        long contactId3 = queryContactId(rawContactId3);
        values.clear();
        values.put(Data.RAW_CONTACT_ID, rawContactId3);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
        values.put(Photo.PHOTO, loadPhotoFromResource(R.drawable.earth_normal,
                PhotoSize.THUMBNAIL));
        values.put(Photo.PHOTO_FILE_ID, bogusFileId);
        values.put(DataRowHandlerForPhoto.SKIP_PROCESSING_KEY, true);
        mResolver.insert(Data.CONTENT_URI, values);

        // Insert a fourth raw contact with a stream item that has a photo, then remove that photo
        // from the photo store.
        Account socialAccount = new Account("social", "social");
        long rawContactId4 = RawContactUtil.createRawContactWithName(mResolver, socialAccount);
        Uri streamItemUri =
                insertStreamItem(rawContactId4, buildGenericStreamItemValues(), socialAccount);
        long streamItemId = ContentUris.parseId(streamItemUri);
        Uri streamItemPhotoUri = insertStreamItemPhoto(
                streamItemId, buildGenericStreamItemPhotoValues(0), socialAccount);
        long streamItemPhotoFileId = getStoredLongValue(streamItemPhotoUri,
                StreamItemPhotos.PHOTO_FILE_ID);
        photoStore.remove(streamItemPhotoFileId);

        // Also insert a bogus photo that nobody is using.
        long bogusPhotoId = photoStore.insert(new PhotoProcessor(loadPhotoFromResource(
                R.drawable.earth_huge, PhotoSize.ORIGINAL), 256, 96));

        // Manually trigger another cleanup in the provider.
        provider.cleanupPhotoStore();

        // The following things should have happened.

        // 1. Raw contact 1 and its photo remain unaffected.
        assertEquals(photoFileId1, (long) getStoredLongValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId1),
                Contacts.PHOTO_FILE_ID));

        // 2. Raw contact 2 retains its new photo.  The old one is deleted from the photo store.
        assertEquals(replacementPhotoFileId, (long) getStoredLongValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId2),
                Contacts.PHOTO_FILE_ID));
        assertNull(photoStore.get(photoFileId2));

        // 3. Raw contact 3 should have its photo file reference cleared.
        assertNull(getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId3),
                Contacts.PHOTO_FILE_ID));

        // 4. The bogus photo that nobody was using should be cleared from the photo store.
        assertNull(photoStore.get(bogusPhotoId));

        // 5. The bogus stream item photo should be cleared from the stream item.
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                new ContentValues[0]);
    }

    public void testPhotoStoreCleanupForProfile() {
        SynchronousContactsProvider2 provider = (SynchronousContactsProvider2) mActor.provider;
        PhotoStore profilePhotoStore = provider.getProfilePhotoStore();

        // Trigger an initial cleanup so another one won't happen while we're running this test.
        provider.switchToProfileModeForTest();
        provider.cleanupPhotoStore();

        // Create the profile contact and add a photo.
        Account socialAccount = new Account("social", "social");
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, socialAccount.name);
        values.put(RawContacts.ACCOUNT_TYPE, socialAccount.type);
        long profileRawContactId = createBasicProfileContact(values);
        long profileContactId = queryContactId(profileRawContactId);
        long dataId = ContentUris.parseId(
                insertPhoto(profileRawContactId, R.drawable.earth_normal));
        long profilePhotoFileId =
                getStoredLongValue(ContentUris.withAppendedId(Data.CONTENT_URI, dataId),
                        Photo.PHOTO_FILE_ID);

        // Also add a stream item with a photo.
        Uri streamItemUri =
                insertStreamItem(profileRawContactId, buildGenericStreamItemValues(),
                        socialAccount);
        long streamItemId = ContentUris.parseId(streamItemUri);
        Uri streamItemPhotoUri = insertStreamItemPhoto(
                streamItemId, buildGenericStreamItemPhotoValues(0), socialAccount);
        long streamItemPhotoFileId = getStoredLongValue(streamItemPhotoUri,
                StreamItemPhotos.PHOTO_FILE_ID);

        // Remove the stream item photo and the profile photo.
        profilePhotoStore.remove(profilePhotoFileId);
        profilePhotoStore.remove(streamItemPhotoFileId);

        // Manually trigger another cleanup in the provider.
        provider.switchToProfileModeForTest();
        provider.cleanupPhotoStore();

        // The following things should have happened.

        // The stream item photo should have been removed.
        assertStoredValues(Uri.withAppendedPath(
                ContentUris.withAppendedId(StreamItems.CONTENT_URI, streamItemId),
                StreamItems.StreamItemPhotos.CONTENT_DIRECTORY),
                new ContentValues[0]);

        // The profile photo should have been cleared.
        assertNull(getStoredValue(
                ContentUris.withAppendedId(Contacts.CONTENT_URI, profileContactId),
                Contacts.PHOTO_FILE_ID));

    }

    public void testOverwritePhotoWithThumbnail() throws IOException {
        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long contactId = queryContactId(rawContactId);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);

        // Write a regular-size photo.
        long dataId = ContentUris.parseId(insertPhoto(rawContactId, R.drawable.earth_normal));
        Long photoFileId = getStoredLongValue(contactUri, Contacts.PHOTO_FILE_ID);
        assertTrue(photoFileId != null && photoFileId > 0);

        // Now overwrite the photo with a thumbnail-sized photo.
        ContentValues update = new ContentValues();
        update.put(Photo.PHOTO, loadPhotoFromResource(R.drawable.earth_small, PhotoSize.ORIGINAL));
        mResolver.update(ContentUris.withAppendedId(Data.CONTENT_URI, dataId), update, null, null);

        // Photo file ID should have been nulled out, and the photo URI should be the same as the
        // thumbnail URI.
        assertNull(getStoredValue(contactUri, Contacts.PHOTO_FILE_ID));
        String photoUri = getStoredValue(contactUri, Contacts.PHOTO_URI);
        String thumbnailUri = getStoredValue(contactUri, Contacts.PHOTO_THUMBNAIL_URI);
        assertEquals(photoUri, thumbnailUri);

        // Retrieving the photo URI should get the thumbnail content.
        EvenMoreAsserts.assertImageRawData(getContext(),
                loadPhotoFromResource(R.drawable.earth_small, PhotoSize.THUMBNAIL),
                mResolver.openInputStream(Uri.parse(photoUri)));
    }

    public void testUpdateRawContactSetStarred() {
        long rawContactId1 = RawContactUtil.createRawContactWithName(mResolver);
        Uri rawContactUri1 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1);
        long rawContactId2 = RawContactUtil.createRawContactWithName(mResolver);
        Uri rawContactUri2 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2);
        setAggregationException(
                AggregationExceptions.TYPE_KEEP_TOGETHER, rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        assertStoredValue(contactUri, Contacts.STARRED, "0");

        assertDirty(rawContactUri1, true);
        assertDirty(rawContactUri2, true);
        clearDirty(rawContactUri1);
        clearDirty(rawContactUri2);

        ContentValues values = new ContentValues();
        values.put(RawContacts.STARRED, "1");

        mResolver.update(rawContactUri1, values, null, null);

        assertStoredValue(rawContactUri1, RawContacts.STARRED, "1");
        assertStoredValue(rawContactUri2, RawContacts.STARRED, "0");
        assertStoredValue(contactUri, Contacts.STARRED, "1");
        assertDirty(rawContactUri1, true);
        assertNetworkNotified(true);

        clearDirty(rawContactUri1);
        values.put(RawContacts.STARRED, "0");
        mResolver.update(rawContactUri1, values, null, null);

        assertStoredValue(rawContactUri1, RawContacts.STARRED, "0");
        assertStoredValue(rawContactUri2, RawContacts.STARRED, "0");
        assertStoredValue(contactUri, Contacts.STARRED, "0");
        assertDirty(rawContactUri1, true);
        assertNetworkNotified(true);

        clearDirty(rawContactUri1);
        values.put(Contacts.STARRED, "1");
        mResolver.update(contactUri, values, null, null);

        assertStoredValue(rawContactUri1, RawContacts.STARRED, "1");
        assertStoredValue(rawContactUri2, RawContacts.STARRED, "1");
        assertStoredValue(contactUri, Contacts.STARRED, "1");
        assertDirty(rawContactUri1, true);
        assertNetworkNotified(true);
    }

    public void testUpdateContactOptionsSetStarred() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        long contactId = queryContactId(rawContactId);
        String lookupKey = queryLookupKey(contactId);
        ContentValues values =new ContentValues();
        values.put(Contacts.STARRED, 1);

        Uri contactLookupUri = ContentUris.withAppendedId(
            Uri.withAppendedPath(Contacts.CONTENT_LOOKUP_URI, lookupKey), contactId);
        mResolver.update(contactLookupUri, values, null, null);
        assertNetworkNotified(true);
    }

    public void testSetAndClearSuperPrimaryEmail() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri mailUri11 = insertEmail(rawContactId1, "test1@domain1.com");
        Uri mailUri12 = insertEmail(rawContactId1, "test2@domain1.com");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver, new Account("b", "b"));
        Uri mailUri21 = insertEmail(rawContactId2, "test1@domain2.com");
        Uri mailUri22 = insertEmail(rawContactId2, "test2@domain2.com");

        assertStoredValue(mailUri11, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri11, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 0);

        // Set super primary on the first pair, primary on the second
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_SUPER_PRIMARY, 1);
            mResolver.update(mailUri11, values, null, null);
        }
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_SUPER_PRIMARY, 1);
            mResolver.update(mailUri22, values, null, null);
        }

        assertStoredValue(mailUri11, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri11, Data.IS_SUPER_PRIMARY, 1);
        assertStoredValue(mailUri12, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 1);

        // Clear primary on the first pair, make sure second is not affected and super_primary is
        // also cleared
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_PRIMARY, 0);
            mResolver.update(mailUri11, values, null, null);
        }

        assertStoredValue(mailUri11, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri11, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 1);

        // Ensure that we can only clear super_primary, if we specify the correct data row
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_SUPER_PRIMARY, 0);
            mResolver.update(mailUri21, values, null, null);
        }

        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 1);

        // Ensure that we can only clear primary, if we specify the correct data row
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_PRIMARY, 0);
            mResolver.update(mailUri21, values, null, null);
        }

        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 1);

        // Now clear super-primary for real
        {
            ContentValues values = new ContentValues();
            values.put(Data.IS_SUPER_PRIMARY, 0);
            mResolver.update(mailUri22, values, null, null);
        }

        assertStoredValue(mailUri11, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri11, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri12, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri21, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri22, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri22, Data.IS_SUPER_PRIMARY, 0);
    }

    /**
     * Common function for the testNewPrimaryIn* functions. Its four configurations
     * are each called from its own test
     */
    public void testChangingPrimary(boolean inUpdate, boolean withSuperPrimary) {
        long rawContactId = RawContactUtil.createRawContact(mResolver, new Account("a", "a"));
        Uri mailUri1 = insertEmail(rawContactId, "test1@domain1.com", true);

        if (withSuperPrimary) {
            final ContentValues values = new ContentValues();
            values.put(Data.IS_SUPER_PRIMARY, 1);
            mResolver.update(mailUri1, values, null, null);
        }

        assertStoredValue(mailUri1, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri1, Data.IS_SUPER_PRIMARY, withSuperPrimary ? 1 : 0);

        // Insert another item
        final Uri mailUri2;
        if (inUpdate) {
            mailUri2 = insertEmail(rawContactId, "test2@domain1.com");

            assertStoredValue(mailUri1, Data.IS_PRIMARY, 1);
            assertStoredValue(mailUri1, Data.IS_SUPER_PRIMARY, withSuperPrimary ? 1 : 0);
            assertStoredValue(mailUri2, Data.IS_PRIMARY, 0);
            assertStoredValue(mailUri2, Data.IS_SUPER_PRIMARY, 0);

            final ContentValues values = new ContentValues();
            values.put(Data.IS_PRIMARY, 1);
            mResolver.update(mailUri2, values, null, null);
        } else {
            // directly add as default
            mailUri2 = insertEmail(rawContactId, "test2@domain1.com", true);
        }

        // Ensure that primary has been unset on the first
        // If withSuperPrimary is set, also ensure that is has been moved to the new item
        assertStoredValue(mailUri1, Data.IS_PRIMARY, 0);
        assertStoredValue(mailUri1, Data.IS_SUPER_PRIMARY, 0);
        assertStoredValue(mailUri2, Data.IS_PRIMARY, 1);
        assertStoredValue(mailUri2, Data.IS_SUPER_PRIMARY, withSuperPrimary ? 1 : 0);
    }

    public void testNewPrimaryInInsert() {
        testChangingPrimary(false, false);
    }

    public void testNewPrimaryInInsertWithSuperPrimary() {
        testChangingPrimary(false, true);
    }

    public void testNewPrimaryInUpdate() {
        testChangingPrimary(true, false);
    }

    public void testNewPrimaryInUpdateWithSuperPrimary() {
        testChangingPrimary(true, true);
    }

    public void testContactSortOrder() {
        assertEquals(ContactsColumns.PHONEBOOK_BUCKET_PRIMARY + ", "
                     + Contacts.SORT_KEY_PRIMARY,
                     ContactsProvider2.getLocalizedSortOrder(Contacts.SORT_KEY_PRIMARY));
        assertEquals(ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE + ", "
                     + Contacts.SORT_KEY_ALTERNATIVE,
                     ContactsProvider2.getLocalizedSortOrder(Contacts.SORT_KEY_ALTERNATIVE));
        assertEquals(ContactsColumns.PHONEBOOK_BUCKET_PRIMARY + " DESC, "
                     + Contacts.SORT_KEY_PRIMARY + " DESC",
                     ContactsProvider2.getLocalizedSortOrder(Contacts.SORT_KEY_PRIMARY + " DESC"));
        String suffix = " COLLATE LOCALIZED DESC";
        assertEquals(ContactsColumns.PHONEBOOK_BUCKET_ALTERNATIVE + suffix
                     + ", " + Contacts.SORT_KEY_ALTERNATIVE + suffix,
                     ContactsProvider2.getLocalizedSortOrder(Contacts.SORT_KEY_ALTERNATIVE
                                                             + suffix));
    }

    public void testContactCounts() {
        Uri uri = Contacts.CONTENT_URI.buildUpon()
                .appendQueryParameter(Contacts.EXTRA_ADDRESS_BOOK_INDEX, "true").build();

        RawContactUtil.createRawContact(mResolver);
        RawContactUtil.createRawContactWithName(mResolver, "James", "Sullivan");
        RawContactUtil.createRawContactWithName(mResolver, "The Abominable", "Snowman");
        RawContactUtil.createRawContactWithName(mResolver, "Mike", "Wazowski");
        RawContactUtil.createRawContactWithName(mResolver, "randall", "boggs");
        RawContactUtil.createRawContactWithName(mResolver, "Boo", null);
        RawContactUtil.createRawContactWithName(mResolver, "Mary", null);
        RawContactUtil.createRawContactWithName(mResolver, "Roz", null);
        // Contacts with null display names get sorted to the end (using the number bucket)
        RawContactUtil.createRawContactWithName(mResolver, null, null);

        Cursor cursor = mResolver.query(uri,
                new String[]{Contacts.DISPLAY_NAME},
                null, null, Contacts.SORT_KEY_PRIMARY);

        assertFirstLetterValues(cursor, "B", "J", "M", "R", "T", "#");
        assertFirstLetterCounts(cursor,  1,   1,   2,   2,   1,   2);
        cursor.close();

        cursor = mResolver.query(uri,
                new String[]{Contacts.DISPLAY_NAME},
                null, null, Contacts.SORT_KEY_ALTERNATIVE + " COLLATE LOCALIZED DESC");

        assertFirstLetterValues(cursor, "#", "W", "S", "R", "M", "B");
        assertFirstLetterCounts(cursor,  2,   1,   2,   1,   1,   2);
        cursor.close();
    }

    private void assertFirstLetterValues(Cursor cursor, String... expected) {
        String[] actual = cursor.getExtras()
                .getStringArray(Contacts.EXTRA_ADDRESS_BOOK_INDEX_TITLES);
        MoreAsserts.assertEquals(expected, actual);
    }

    private void assertFirstLetterCounts(Cursor cursor, int... expected) {
        int[] actual = cursor.getExtras()
                .getIntArray(Contacts.EXTRA_ADDRESS_BOOK_INDEX_COUNTS);
        MoreAsserts.assertEquals(expected, actual);
    }

    public void testReadBooleanQueryParameter() {
        assertBooleanUriParameter("foo:bar", "bool", true, true);
        assertBooleanUriParameter("foo:bar", "bool", false, false);
        assertBooleanUriParameter("foo:bar?bool=0", "bool", true, false);
        assertBooleanUriParameter("foo:bar?bool=1", "bool", false, true);
        assertBooleanUriParameter("foo:bar?bool=false", "bool", true, false);
        assertBooleanUriParameter("foo:bar?bool=true", "bool", false, true);
        assertBooleanUriParameter("foo:bar?bool=FaLsE", "bool", true, false);
        assertBooleanUriParameter("foo:bar?bool=false&some=some", "bool", true, false);
        assertBooleanUriParameter("foo:bar?bool=1&some=some", "bool", false, true);
        assertBooleanUriParameter("foo:bar?some=bool", "bool", true, true);
        assertBooleanUriParameter("foo:bar?bool", "bool", true, true);
    }

    private void assertBooleanUriParameter(String uriString, String parameter,
            boolean defaultValue, boolean expectedValue) {
        assertEquals(expectedValue, ContactsProvider2.readBooleanQueryParameter(
                Uri.parse(uriString), parameter, defaultValue));
    }

    public void testGetQueryParameter() {
        assertQueryParameter("foo:bar", "param", null);
        assertQueryParameter("foo:bar?param", "param", null);
        assertQueryParameter("foo:bar?param=", "param", "");
        assertQueryParameter("foo:bar?param=val", "param", "val");
        assertQueryParameter("foo:bar?param=val&some=some", "param", "val");
        assertQueryParameter("foo:bar?some=some&param=val", "param", "val");
        assertQueryParameter("foo:bar?some=some&param=val&else=else", "param", "val");
        assertQueryParameter("foo:bar?param=john%40doe.com", "param", "john@doe.com");
        assertQueryParameter("foo:bar?some_param=val", "param", null);
        assertQueryParameter("foo:bar?some_param=val1&param=val2", "param", "val2");
        assertQueryParameter("foo:bar?some_param=val1&param=", "param", "");
        assertQueryParameter("foo:bar?some_param=val1&param", "param", null);
        assertQueryParameter("foo:bar?some_param=val1&another_param=val2&param=val3",
                "param", "val3");
        assertQueryParameter("foo:bar?some_param=val1&param=val2&some_param=val3",
                "param", "val2");
        assertQueryParameter("foo:bar?param=val1&some_param=val2", "param", "val1");
        assertQueryParameter("foo:bar?p=val1&pp=val2", "p", "val1");
        assertQueryParameter("foo:bar?pp=val1&p=val2", "p", "val2");
        assertQueryParameter("foo:bar?ppp=val1&pp=val2&p=val3", "p", "val3");
        assertQueryParameter("foo:bar?ppp=val&", "p", null);
    }

    public void testMissingAccountTypeParameter() {
        // Try querying for RawContacts only using ACCOUNT_NAME
        final Uri queryUri = RawContacts.CONTENT_URI.buildUpon().appendQueryParameter(
                RawContacts.ACCOUNT_NAME, "lolwut").build();
        try {
            final Cursor cursor = mResolver.query(queryUri, null, null, null, null);
            fail("Able to query with incomplete account query parameters");
        } catch (IllegalArgumentException e) {
            // Expected behavior.
        }
    }

    public void testInsertInconsistentAccountType() {
        // Try inserting RawContact with inconsistent Accounts
        final Account red = new Account("red", "red");
        final Account blue = new Account("blue", "blue");

        final ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, red.name);
        values.put(RawContacts.ACCOUNT_TYPE, red.type);

        final Uri insertUri = TestUtil.maybeAddAccountQueryParameters(RawContacts.CONTENT_URI,
                blue);
        try {
            mResolver.insert(insertUri, values);
            fail("Able to insert RawContact with inconsistent account details");
        } catch (IllegalArgumentException e) {
            // Expected behavior.
        }
    }

    public void testProviderStatusNoContactsNoAccounts() throws Exception {
        assertProviderStatus(ProviderStatus.STATUS_EMPTY);
    }

    public void testProviderStatusOnlyLocalContacts() throws Exception {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        assertProviderStatus(ProviderStatus.STATUS_NORMAL);
        mResolver.delete(
                ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId), null, null);
        assertProviderStatus(ProviderStatus.STATUS_EMPTY);
    }

    public void testProviderStatusWithAccounts() throws Exception {
        assertProviderStatus(ProviderStatus.STATUS_EMPTY);
        mActor.setAccounts(new Account[]{TestUtil.ACCOUNT_1});
        ((ContactsProvider2)getProvider()).onAccountsUpdated(new Account[]{TestUtil.ACCOUNT_1});
        assertProviderStatus(ProviderStatus.STATUS_NORMAL);
        mActor.setAccounts(new Account[0]);
        ((ContactsProvider2)getProvider()).onAccountsUpdated(new Account[0]);
        assertProviderStatus(ProviderStatus.STATUS_EMPTY);
    }

    private void assertProviderStatus(int expectedProviderStatus) {
        Cursor cursor = mResolver.query(ProviderStatus.CONTENT_URI,
                new String[]{ProviderStatus.STATUS}, null, null,
                null);
        assertTrue(cursor.moveToFirst());
        assertEquals(expectedProviderStatus, cursor.getInt(0));
        cursor.close();
    }

    public void testProperties() throws Exception {
        ContactsProvider2 provider = (ContactsProvider2)getProvider();
        ContactsDatabaseHelper helper = (ContactsDatabaseHelper)provider.getDatabaseHelper();
        assertNull(helper.getProperty("non-existent", null));
        assertEquals("default", helper.getProperty("non-existent", "default"));

        helper.setProperty("existent1", "string1");
        helper.setProperty("existent2", "string2");
        assertEquals("string1", helper.getProperty("existent1", "default"));
        assertEquals("string2", helper.getProperty("existent2", "default"));
        helper.setProperty("existent1", null);
        assertEquals("default", helper.getProperty("existent1", "default"));
    }

    private class VCardTestUriCreator {
        private String mLookup1;
        private String mLookup2;

        public VCardTestUriCreator(String lookup1, String lookup2) {
            super();
            mLookup1 = lookup1;
            mLookup2 = lookup2;
        }

        public Uri getUri1() {
            return Uri.withAppendedPath(Contacts.CONTENT_VCARD_URI, mLookup1);
        }

        public Uri getUri2() {
            return Uri.withAppendedPath(Contacts.CONTENT_VCARD_URI, mLookup2);
        }

        public Uri getCombinedUri() {
            return Uri.withAppendedPath(Contacts.CONTENT_MULTI_VCARD_URI,
                    Uri.encode(mLookup1 + ":" + mLookup2));
        }
    }

    private VCardTestUriCreator createVCardTestContacts() {
        final long rawContactId1 = RawContactUtil.createRawContact(mResolver, mAccount,
                RawContacts.SOURCE_ID, "4:12");
        DataUtil.insertStructuredName(mResolver, rawContactId1, "John", "Doe");

        final long rawContactId2 = RawContactUtil.createRawContact(mResolver, mAccount,
                RawContacts.SOURCE_ID, "3:4%121");
        DataUtil.insertStructuredName(mResolver, rawContactId2, "Jane", "Doh");

        final long contactId1 = queryContactId(rawContactId1);
        final long contactId2 = queryContactId(rawContactId2);
        final Uri contact1Uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId1);
        final Uri contact2Uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId2);
        final String lookup1 =
            Uri.encode(Contacts.getLookupUri(mResolver, contact1Uri).getPathSegments().get(2));
        final String lookup2 =
            Uri.encode(Contacts.getLookupUri(mResolver, contact2Uri).getPathSegments().get(2));
        return new VCardTestUriCreator(lookup1, lookup2);
    }

    public void testQueryMultiVCard() {
        // No need to create any contacts here, because the query for multiple vcards
        // does not go into the database at all
        Uri uri = Uri.withAppendedPath(Contacts.CONTENT_MULTI_VCARD_URI, Uri.encode("123:456"));
        Cursor cursor = mResolver.query(uri, null, null, null, null);
        assertEquals(1, cursor.getCount());
        assertTrue(cursor.moveToFirst());
        assertTrue(cursor.isNull(cursor.getColumnIndex(OpenableColumns.SIZE)));
        String filename = cursor.getString(cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME));

        // The resulting name contains date and time. Ensure that before and after are correct
        assertTrue(filename.startsWith("vcards_"));
        assertTrue(filename.endsWith(".vcf"));
        cursor.close();
    }

    public void testQueryFileSingleVCard() {
        final VCardTestUriCreator contacts = createVCardTestContacts();

        {
            Cursor cursor = mResolver.query(contacts.getUri1(), null, null, null, null);
            assertEquals(1, cursor.getCount());
            assertTrue(cursor.moveToFirst());
            assertTrue(cursor.isNull(cursor.getColumnIndex(OpenableColumns.SIZE)));
            String filename = cursor.getString(cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME));
            assertEquals("John Doe.vcf", filename);
            cursor.close();
        }

        {
            Cursor cursor = mResolver.query(contacts.getUri2(), null, null, null, null);
            assertEquals(1, cursor.getCount());
            assertTrue(cursor.moveToFirst());
            assertTrue(cursor.isNull(cursor.getColumnIndex(OpenableColumns.SIZE)));
            String filename = cursor.getString(cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME));
            assertEquals("Jane Doh.vcf", filename);
            cursor.close();
        }
    }

    public void testQueryFileProfileVCard() {
        createBasicProfileContact(new ContentValues());
        Cursor cursor = mResolver.query(Profile.CONTENT_VCARD_URI, null, null, null, null);
        assertEquals(1, cursor.getCount());
        assertTrue(cursor.moveToFirst());
        assertTrue(cursor.isNull(cursor.getColumnIndex(OpenableColumns.SIZE)));
        String filename = cursor.getString(cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME));
        assertEquals("Mia Prophyl.vcf", filename);
        cursor.close();
    }

    public void testOpenAssetFileMultiVCard() throws IOException {
        final VCardTestUriCreator contacts = createVCardTestContacts();

        final AssetFileDescriptor descriptor =
            mResolver.openAssetFileDescriptor(contacts.getCombinedUri(), "r");
        final FileInputStream inputStream = descriptor.createInputStream();
        String data = readToEnd(inputStream);
        inputStream.close();
        descriptor.close();

        // Ensure that the resulting VCard has both contacts
        assertTrue(data.contains("N:Doe;John;;;"));
        assertTrue(data.contains("N:Doh;Jane;;;"));
    }

    public void testOpenAssetFileSingleVCard() throws IOException {
        final VCardTestUriCreator contacts = createVCardTestContacts();

        // Ensure that the right VCard is being created in each case
        {
            final AssetFileDescriptor descriptor =
                mResolver.openAssetFileDescriptor(contacts.getUri1(), "r");
            final FileInputStream inputStream = descriptor.createInputStream();
            final String data = readToEnd(inputStream);
            inputStream.close();
            descriptor.close();

            assertTrue(data.contains("N:Doe;John;;;"));
            assertFalse(data.contains("N:Doh;Jane;;;"));
        }

        {
            final AssetFileDescriptor descriptor =
                mResolver.openAssetFileDescriptor(contacts.getUri2(), "r");
            final FileInputStream inputStream = descriptor.createInputStream();
            final String data = readToEnd(inputStream);
            inputStream.close();
            descriptor.close();

            assertFalse(data.contains("N:Doe;John;;;"));
            assertTrue(data.contains("N:Doh;Jane;;;"));
        }
    }

    public void testAutoGroupMembership() {
        long g1 = createGroup(mAccount, "g1", "t1", 0, true /* autoAdd */, false /* favorite */);
        long g2 = createGroup(mAccount, "g2", "t2", 0, false /* autoAdd */, false /* favorite */);
        long g3 = createGroup(mAccountTwo, "g3", "t3", 0, true /* autoAdd */, false /* favorite */);
        long g4 = createGroup(mAccountTwo, "g4", "t4", 0, false /* autoAdd */, false/* favorite */);
        long r1 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r2 = RawContactUtil.createRawContact(mResolver, mAccountTwo);
        long r3 = RawContactUtil.createRawContact(mResolver, null);

        Cursor c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        c = queryGroupMemberships(mAccountTwo);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g3, c.getLong(0));
            assertEquals(r2, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }
    }

    public void testNoAutoAddMembershipAfterGroupCreation() {
        long r1 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r2 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r3 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r4 = RawContactUtil.createRawContact(mResolver, mAccountTwo);
        long r5 = RawContactUtil.createRawContact(mResolver, mAccountTwo);
        long r6 = RawContactUtil.createRawContact(mResolver, null);

        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        long g1 = createGroup(mAccount, "g1", "t1", 0, true /* autoAdd */, false /* favorite */);
        long g2 = createGroup(mAccount, "g2", "t2", 0, false /* autoAdd */, false /* favorite */);
        long g3 = createGroup(mAccountTwo, "g3", "t3", 0, true /* autoAdd */, false/* favorite */);

        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
    }

    // create some starred and non-starred contacts, some associated with account, some not
    // favorites group created
    // the starred contacts should be added to group
    // favorites group removed
    // no change to starred status
    public void testFavoritesMembershipAfterGroupCreation() {
        long r1 = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.STARRED, "1");
        long r2 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r3 = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.STARRED, "1");
        long r4 = RawContactUtil.createRawContact(mResolver, mAccountTwo, RawContacts.STARRED, "1");
        long r5 = RawContactUtil.createRawContact(mResolver, mAccountTwo);
        long r6 = RawContactUtil.createRawContact(mResolver, null, RawContacts.STARRED, "1");
        long r7 = RawContactUtil.createRawContact(mResolver, null);

        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        long g1 = createGroup(mAccount, "g1", "t1", 0, false /* autoAdd */, true /* favorite */);
        long g2 = createGroup(mAccount, "g2", "t2", 0, false /* autoAdd */, false /* favorite */);
        long g3 = createGroup(mAccountTwo, "g3", "t3", 0, false /* autoAdd */, false/* favorite */);

        assertTrue(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertTrue(queryRawContactIsStarred(r3));
        assertTrue(queryRawContactIsStarred(r4));
        assertFalse(queryRawContactIsStarred(r5));
        assertTrue(queryRawContactIsStarred(r6));
        assertFalse(queryRawContactIsStarred(r7));

        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        Cursor c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r3, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        updateItem(RawContacts.CONTENT_URI, r6,
                RawContacts.ACCOUNT_NAME, mAccount.name,
                RawContacts.ACCOUNT_TYPE, mAccount.type);
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r3, c.getLong(1));
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r6, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        mResolver.delete(ContentUris.withAppendedId(Groups.CONTENT_URI, g1), null, null);

        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        assertTrue(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertTrue(queryRawContactIsStarred(r3));
        assertTrue(queryRawContactIsStarred(r4));
        assertFalse(queryRawContactIsStarred(r5));
        assertTrue(queryRawContactIsStarred(r6));
        assertFalse(queryRawContactIsStarred(r7));
    }

    public void testFavoritesGroupMembershipChangeAfterStarChange() {
        long g1 = createGroup(mAccount, "g1", "t1", 0, false /* autoAdd */, true /* favorite */);
        long g2 = createGroup(mAccount, "g2", "t2", 0, false /* autoAdd */, false/* favorite */);
        long g4 = createGroup(mAccountTwo, "g4", "t4", 0, false /* autoAdd */, true /* favorite */);
        long g5 = createGroup(mAccountTwo, "g5", "t5", 0, false /* autoAdd */, false/* favorite */);
        long r1 = RawContactUtil.createRawContact(mResolver, mAccount, RawContacts.STARRED, "1");
        long r2 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r3 = RawContactUtil.createRawContact(mResolver, mAccountTwo);

        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        Cursor c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        // remove the star from r1
        assertEquals(1, updateItem(RawContacts.CONTENT_URI, r1, RawContacts.STARRED, "0"));

        // Since no raw contacts are starred, there should be no group memberships.
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        // mark r1 as starred
        assertEquals(1, updateItem(RawContacts.CONTENT_URI, r1, RawContacts.STARRED, "1"));
        // Now that r1 is starred it should have a membership in the one groups from mAccount
        // that is marked as a favorite.
        // There should be no memberships in mAccountTwo since it has no starred raw contacts.
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        // remove the star from r1
        assertEquals(1, updateItem(RawContacts.CONTENT_URI, r1, RawContacts.STARRED, "0"));
        // Since no raw contacts are starred, there should be no group memberships.
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, queryContactId(r1));
        assertNotNull(contactUri);

        // mark r1 as starred via its contact lookup uri
        assertEquals(1, updateItem(contactUri, Contacts.STARRED, "1"));
        // Now that r1 is starred it should have a membership in the one groups from mAccount
        // that is marked as a favorite.
        // There should be no memberships in mAccountTwo since it has no starred raw contacts.
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        // remove the star from r1
        updateItem(contactUri, Contacts.STARRED, "0");
        // Since no raw contacts are starred, there should be no group memberships.
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
    }

    public void testStarChangedAfterGroupMembershipChange() {
        long g1 = createGroup(mAccount, "g1", "t1", 0, false /* autoAdd */, true /* favorite */);
        long g2 = createGroup(mAccount, "g2", "t2", 0, false /* autoAdd */, false/* favorite */);
        long g4 = createGroup(mAccountTwo, "g4", "t4", 0, false /* autoAdd */, true /* favorite */);
        long g5 = createGroup(mAccountTwo, "g5", "t5", 0, false /* autoAdd */, false/* favorite */);
        long r1 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r2 = RawContactUtil.createRawContact(mResolver, mAccount);
        long r3 = RawContactUtil.createRawContact(mResolver, mAccountTwo);

        assertFalse(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertFalse(queryRawContactIsStarred(r3));

        Cursor c;

        // add r1 to one favorites group
        // r1's star should automatically be set
        // r1 should automatically be added to the other favorites group
        Uri urir1g1 = insertGroupMembership(r1, g1);
        assertTrue(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertFalse(queryRawContactIsStarred(r3));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
        c = queryGroupMemberships(mAccount);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g1, c.getLong(0));
            assertEquals(r1, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        // remove r1 from one favorites group
        mResolver.delete(urir1g1, null, null);
        // r1's star should no longer be set
        assertFalse(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertFalse(queryRawContactIsStarred(r3));
        // there should be no membership rows
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));

        // add r3 to the one favorites group for that account
        // r3's star should automatically be set
        Uri urir3g4 = insertGroupMembership(r3, g4);
        assertFalse(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertTrue(queryRawContactIsStarred(r3));
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        c = queryGroupMemberships(mAccountTwo);
        try {
            assertTrue(c.moveToNext());
            assertEquals(g4, c.getLong(0));
            assertEquals(r3, c.getLong(1));
            assertFalse(c.moveToNext());
        } finally {
            c.close();
        }

        // remove r3 from the favorites group
        mResolver.delete(urir3g4, null, null);
        // r3's star should automatically be cleared
        assertFalse(queryRawContactIsStarred(r1));
        assertFalse(queryRawContactIsStarred(r2));
        assertFalse(queryRawContactIsStarred(r3));
        assertNoRowsAndClose(queryGroupMemberships(mAccount));
        assertNoRowsAndClose(queryGroupMemberships(mAccountTwo));
    }

    public void testReadOnlyRawContact() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        storeValue(rawContactUri, RawContacts.CUSTOM_RINGTONE, "first");
        storeValue(rawContactUri, RawContacts.RAW_CONTACT_IS_READ_ONLY, 1);

        storeValue(rawContactUri, RawContacts.CUSTOM_RINGTONE, "second");
        assertStoredValue(rawContactUri, RawContacts.CUSTOM_RINGTONE, "first");

        Uri syncAdapterUri = rawContactUri.buildUpon()
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "1")
                .build();
        storeValue(syncAdapterUri, RawContacts.CUSTOM_RINGTONE, "third");
        assertStoredValue(rawContactUri, RawContacts.CUSTOM_RINGTONE, "third");
    }

    public void testReadOnlyDataRow() {
        long rawContactId = RawContactUtil.createRawContact(mResolver);
        Uri emailUri = insertEmail(rawContactId, "email");
        Uri phoneUri = insertPhoneNumber(rawContactId, "555-1111");

        storeValue(emailUri, Data.IS_READ_ONLY, "1");
        storeValue(emailUri, Email.ADDRESS, "changed");
        storeValue(phoneUri, Phone.NUMBER, "555-2222");
        assertStoredValue(emailUri, Email.ADDRESS, "email");
        assertStoredValue(phoneUri, Phone.NUMBER, "555-2222");

        Uri syncAdapterUri = emailUri.buildUpon()
                .appendQueryParameter(ContactsContract.CALLER_IS_SYNCADAPTER, "1")
                .build();
        storeValue(syncAdapterUri, Email.ADDRESS, "changed");
        assertStoredValue(emailUri, Email.ADDRESS, "changed");
    }

    public void testContactWithReadOnlyRawContact() {
        long rawContactId1 = RawContactUtil.createRawContact(mResolver);
        Uri rawContactUri1 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId1);
        storeValue(rawContactUri1, RawContacts.CUSTOM_RINGTONE, "first");

        long rawContactId2 = RawContactUtil.createRawContact(mResolver);
        Uri rawContactUri2 = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId2);
        storeValue(rawContactUri2, RawContacts.CUSTOM_RINGTONE, "second");
        storeValue(rawContactUri2, RawContacts.RAW_CONTACT_IS_READ_ONLY, 1);

        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER,
                rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);

        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        storeValue(contactUri, Contacts.CUSTOM_RINGTONE, "rt");
        assertStoredValue(contactUri, Contacts.CUSTOM_RINGTONE, "rt");
        assertStoredValue(rawContactUri1, RawContacts.CUSTOM_RINGTONE, "rt");
        assertStoredValue(rawContactUri2, RawContacts.CUSTOM_RINGTONE, "second");
    }

    public void testNameParsingQuery() {
        Uri uri = ContactsContract.AUTHORITY_URI.buildUpon().appendPath("complete_name")
                .appendQueryParameter(StructuredName.DISPLAY_NAME, "Mr. John Q. Doe Jr.").build();
        Cursor cursor = mResolver.query(uri, null, null, null, null);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr. John Q. Doe Jr.");
        values.put(StructuredName.PREFIX, "Mr.");
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.MIDDLE_NAME, "Q.");
        values.put(StructuredName.FAMILY_NAME, "Doe");
        values.put(StructuredName.SUFFIX, "Jr.");
        values.put(StructuredName.FULL_NAME_STYLE, FullNameStyle.WESTERN);
        assertTrue(cursor.moveToFirst());
        assertCursorValues(cursor, values);
        cursor.close();
    }

    public void testNameConcatenationQuery() {
        Uri uri = ContactsContract.AUTHORITY_URI.buildUpon().appendPath("complete_name")
                .appendQueryParameter(StructuredName.PREFIX, "Mr")
                .appendQueryParameter(StructuredName.GIVEN_NAME, "John")
                .appendQueryParameter(StructuredName.MIDDLE_NAME, "Q.")
                .appendQueryParameter(StructuredName.FAMILY_NAME, "Doe")
                .appendQueryParameter(StructuredName.SUFFIX, "Jr.")
                .build();
        Cursor cursor = mResolver.query(uri, null, null, null, null);
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr John Q. Doe, Jr.");
        values.put(StructuredName.PREFIX, "Mr");
        values.put(StructuredName.GIVEN_NAME, "John");
        values.put(StructuredName.MIDDLE_NAME, "Q.");
        values.put(StructuredName.FAMILY_NAME, "Doe");
        values.put(StructuredName.SUFFIX, "Jr.");
        values.put(StructuredName.FULL_NAME_STYLE, FullNameStyle.WESTERN);
        assertTrue(cursor.moveToFirst());
        assertCursorValues(cursor, values);
        cursor.close();
    }

    public void testBuildSingleRowResult() {
        checkBuildSingleRowResult(
                new String[] {"b"},
                new String[] {"a", "b"},
                new Integer[] {1, 2},
                new Integer[] {2}
                );

        checkBuildSingleRowResult(
                new String[] {"b", "a", "b"},
                new String[] {"a", "b"},
                new Integer[] {1, 2},
                new Integer[] {2, 1, 2}
                );

        checkBuildSingleRowResult(
                null, // all columns
                new String[] {"a", "b"},
                new Integer[] {1, 2},
                new Integer[] {1, 2}
                );

        try {
            // Access non-existent column
            ContactsProvider2.buildSingleRowResult(new String[] {"a"}, new String[] {"b"},
                    new Object[] {1});
            fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    private void checkBuildSingleRowResult(String[] projection, String[] availableColumns,
            Object[] data, Integer[] expectedValues) {
        final Cursor c = ContactsProvider2.buildSingleRowResult(projection, availableColumns, data);
        try {
            assertTrue(c.moveToFirst());
            assertEquals(1, c.getCount());
            assertEquals(expectedValues.length, c.getColumnCount());

            for (int i = 0; i < expectedValues.length; i++) {
                assertEquals("column " + i, expectedValues[i], (Integer) c.getInt(i));
            }
        } finally {
            c.close();
        }
    }

    public void testMarkMetadataNotDirtyWhenDataUsageUpdate() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        final long rid1 = RawContactUtil.createRawContactWithName(mResolver, "contact", "a");
        final long did1a = ContentUris.parseId(insertEmail(rid1, "email_1_a@email.com"));
        final Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rid1);
        assertDirty(rawContactUri, true);
        clearDirty(rawContactUri);
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1a);

        assertDirty(rawContactUri, false);
        // Usage feedback no longer works, so "false".
        assertMetadataDirty(rawContactUri, false);
        assertNetworkNotified(false);
        assertMetadataNetworkNotified(false);
    }

    public void testDataUsageFeedbackAndDelete() {

        sMockClock.install();
        sMockClock.setCurrentTimeMillis(System.currentTimeMillis());
        final long startTime = sMockClock.currentTimeMillis();

        final long rid1 = RawContactUtil.createRawContactWithName(mResolver, "contact", "a");
        final long did1a = ContentUris.parseId(insertEmail(rid1, "email_1_a@email.com"));
        final long did1b = ContentUris.parseId(insertEmail(rid1, "email_1_b@email.com"));
        final long did1p = ContentUris.parseId(insertPhoneNumber(rid1, "555-555-5555"));

        final long rid2 = RawContactUtil.createRawContactWithName(mResolver, "contact", "b");
        final long did2a = ContentUris.parseId(insertEmail(rid2, "email_2_a@email.com"));
        final long did2p = ContentUris.parseId(insertPhoneNumber(rid2, "555-555-5556"));

        // Aggregate 1 and 2
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, rid1, rid2);

        final long rid3 = RawContactUtil.createRawContactWithName(mResolver, "contact", "c");
        final long did3a = ContentUris.parseId(insertEmail(rid3, "email_3@email.com"));
        final long did3p = ContentUris.parseId(insertPhoneNumber(rid3, "555-3333"));

        final long rid4 = RawContactUtil.createRawContactWithName(mResolver, "contact", "d");
        final long did4p = ContentUris.parseId(insertPhoneNumber(rid4, "555-4444"));

        final long cid1 = queryContactId(rid1);
        final long cid3 = queryContactId(rid3);
        final long cid4 = queryContactId(rid4);

        // Make sure 1+2, 3 and 4 aren't aggregated
        MoreAsserts.assertNotEqual(cid1, cid3);
        MoreAsserts.assertNotEqual(cid1, cid4);
        MoreAsserts.assertNotEqual(cid3, cid4);

        // time = startTime

        // First, there's no frequent.  (We use strequent here only because frequent is hidden
        // and may be removed someday.)
        assertRowCount(0, Contacts.CONTENT_STREQUENT_URI, null, null);

        // Test 1. touch data 1a
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1a);

        // (We no longer populate frequent, so 0.)
        assertRowCount(0, Contacts.CONTENT_STREQUENT_URI, null, null);

        sMockClock.advanceDay();

        // Test 2. touch data 1a, 2a and 3a
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_LONG_TEXT, did1a, did2a, did3a);

        // (We no longer populate frequent, so 0.)
        assertRowCount(0, Contacts.CONTENT_STREQUENT_URI, null, null);

        sMockClock.advanceDay();

        // Test 2. touch data 2p (call)
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_CALL, did2p);

        // (We no longer populate frequent, so 0.)
        assertRowCount(0, Contacts.CONTENT_STREQUENT_URI, null, null);

        sMockClock.advanceDay();

        // Test 3. touch data 2p and 3p (short text)
        updateDataUsageFeedback(DataUsageFeedback.USAGE_TYPE_SHORT_TEXT, did2p, did3p);

        // Let's check the tables.

        // Fist, check the data_usage_stat table, which has no public URI.
        assertStoredValuesDb("SELECT " + DataUsageStatColumns.DATA_ID +
                "," + DataUsageStatColumns.USAGE_TYPE_INT +
                "," + DataUsageStatColumns.RAW_TIMES_USED +
                "," + DataUsageStatColumns.RAW_LAST_TIME_USED +
                " FROM " + Tables.DATA_USAGE_STAT, null
                );

        // Next, check the raw_contacts table
        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, rid1,
                        RawContacts.TIMES_CONTACTED, 0,
                        RawContacts.LAST_TIME_CONTACTED, 0
                        ),
                cv(RawContacts._ID, rid2,
                        RawContacts.TIMES_CONTACTED, 0,
                        RawContacts.LAST_TIME_CONTACTED, 0
                        ),
                cv(RawContacts._ID, rid3,
                        RawContacts.TIMES_CONTACTED, 0,
                        RawContacts.LAST_TIME_CONTACTED, 0
                        ),
                cv(RawContacts._ID, rid4,
                        RawContacts.TIMES_CONTACTED, 0,
                        RawContacts.LAST_TIME_CONTACTED, 0
                        )
                );

        // Lastly, check the contacts table.

        // Note contact1.TIMES_CONTACTED = 4, even though raw_contact1.TIMES_CONTACTED +
        // raw_contact1.TIMES_CONTACTED = 5, because in test 2, data 1a and data 2a were touched
        // at once.
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, cid1,
                        Contacts.TIMES_CONTACTED, 0,
                        Contacts.LAST_TIME_CONTACTED, 0
                        ),
                cv(Contacts._ID, cid3,
                        Contacts.TIMES_CONTACTED, 0,
                        Contacts.LAST_TIME_CONTACTED, 0
                        ),
                cv(Contacts._ID, cid4,
                        Contacts.TIMES_CONTACTED, 0,
                        Contacts.LAST_TIME_CONTACTED, 0
                        )
                );

        // Let's test the delete too.
        assertTrue(mResolver.delete(DataUsageFeedback.DELETE_USAGE_URI, null, null) > 0);

        // Now there's no frequent.
        assertRowCount(0, Contacts.CONTENT_STREQUENT_URI, null, null);

        // No rows in the stats table.
        assertStoredValuesDb("SELECT " + DataUsageStatColumns.DATA_ID +
                " FROM " + Tables.DATA_USAGE_STAT, null,
                new ContentValues[0]);

        // The following values should all be 0 or null.
        assertRowCount(0, Contacts.CONTENT_URI, Contacts.TIMES_CONTACTED + ">0", null);
        assertRowCount(0, Contacts.CONTENT_URI, Contacts.LAST_TIME_CONTACTED + ">0", null);
        assertRowCount(0, RawContacts.CONTENT_URI, RawContacts.TIMES_CONTACTED + ">0", null);
        assertRowCount(0, RawContacts.CONTENT_URI, RawContacts.LAST_TIME_CONTACTED + ">0", null);

        // Calling it when there's no usage stats will still return a positive value.
        assertTrue(mResolver.delete(DataUsageFeedback.DELETE_USAGE_URI, null, null) > 0);
    }

    /*******************************************************
     * Delta api tests.
     */
    public void testContactDelete_hasDeleteLog() {
        sMockClock.install();
        long start = sMockClock.currentTimeMillis();
        DatabaseAsserts.ContactIdPair ids = assertContactCreateDelete();
        DatabaseAsserts.assertHasDeleteLogGreaterThan(mResolver, ids.mContactId, start);

        // Clean up. Must also remove raw contact.
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testContactDelete_marksRawContactsForDeletion() {
        DatabaseAsserts.ContactIdPair ids = assertContactCreateDelete(mAccount);

        String[] projection = new String[]{ContactsContract.RawContacts.DIRTY,
                ContactsContract.RawContacts.DELETED};
        String[] record = RawContactUtil.queryByRawContactId(mResolver, ids.mRawContactId,
                projection);
        assertEquals("1", record[0]);
        assertEquals("1", record[1]);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testContactDelete_checkRawContactContactId() {
        DatabaseAsserts.ContactIdPair ids = assertContactCreateDelete(mAccount);

        String[] projection = new String[]{ContactsContract.RawContacts.CONTACT_ID};
        String[] record = RawContactUtil.queryByRawContactId(mResolver, ids.mRawContactId,
                projection);
        assertNull(record[0]);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testContactUpdate_metadataChange() {
        // Enable metadataSync flag.
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        cp.setMetadataSyncForTest(true);

        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);
        Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, ids.mRawContactId);
        assertDirty(rawContactUri, true);
        clearDirty(rawContactUri);

        ContentValues values = new ContentValues();
        values.put(Contacts.PINNED, 1);

        ContactUtil.update(mResolver, ids.mContactId, values);
        assertDirty(rawContactUri, false);
        assertMetadataDirty(rawContactUri, false);
        assertNetworkNotified(false);
        assertMetadataNetworkNotified(false);
    }

    public void testContactUpdate_updatesContactUpdatedTimestamp() {
        sMockClock.install();
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        ContentValues values = new ContentValues();
        values.put(ContactsContract.Contacts.STARRED, 1);

        sMockClock.advance();
        ContactUtil.update(mResolver, ids.mContactId, values);

        long newTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);
        assertTrue(newTime > baseTime);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    // This implicitly tests the Contact create case.
    public void testRawContactCreate_updatesContactUpdatedTimestamp() {
        long startTime = System.currentTimeMillis();

        long rawContactId = RawContactUtil.createRawContactWithName(mResolver);
        long lastUpdated = getContactLastUpdatedTimestampByRawContactId(mResolver, rawContactId);

        assertTrue(lastUpdated > startTime);

        // Clean up
        RawContactUtil.delete(mResolver, rawContactId, true);
    }

    public void testRawContactUpdate_updatesContactUpdatedTimestamp() {
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.STARRED, 1);
        RawContactUtil.update(mResolver, ids.mRawContactId, values);

        long newTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);
        assertTrue(newTime > baseTime);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testRawContactPsuedoDelete_hasDeleteLogForContact() {
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        RawContactUtil.delete(mResolver, ids.mRawContactId, false);

        DatabaseAsserts.assertHasDeleteLogGreaterThan(mResolver, ids.mContactId, baseTime);

        // clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testRawContactDelete_hasDeleteLogForContact() {
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        RawContactUtil.delete(mResolver, ids.mRawContactId, true);

        DatabaseAsserts.assertHasDeleteLogGreaterThan(mResolver, ids.mContactId, baseTime);

        // already clean
    }

    private long getContactLastUpdatedTimestampByRawContactId(ContentResolver resolver,
            long rawContactId) {
        long contactId = RawContactUtil.queryContactIdByRawContactId(mResolver, rawContactId);
        MoreAsserts.assertNotEqual(CommonDatabaseUtils.NOT_FOUND, contactId);

        return ContactUtil.queryContactLastUpdatedTimestamp(mResolver, contactId);
    }

    public void testDataInsert_updatesContactLastUpdatedTimestamp() {
        sMockClock.install();
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);
        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        sMockClock.advance();
        insertPhoneNumberAndReturnDataId(ids.mRawContactId);

        long newTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);
        assertTrue(newTime > baseTime);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testDataDelete_updatesContactLastUpdatedTimestamp() {
        sMockClock.install();
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long dataId = insertPhoneNumberAndReturnDataId(ids.mRawContactId);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        sMockClock.advance();
        DataUtil.delete(mResolver, dataId);

        long newTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);
        assertTrue(newTime > baseTime);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    public void testDataUpdate_updatesContactLastUpdatedTimestamp() {
        sMockClock.install();
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver);

        long dataId = insertPhoneNumberAndReturnDataId(ids.mRawContactId);

        long baseTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);

        sMockClock.advance();
        ContentValues values = new ContentValues();
        values.put(ContactsContract.CommonDataKinds.Phone.NUMBER, "555-5555");
        DataUtil.update(mResolver, dataId, values);

        long newTime = ContactUtil.queryContactLastUpdatedTimestamp(mResolver, ids.mContactId);
        assertTrue(newTime > baseTime);

        // Clean up
        RawContactUtil.delete(mResolver, ids.mRawContactId, true);
    }

    private long insertPhoneNumberAndReturnDataId(long rawContactId) {
        Uri uri = insertPhoneNumber(rawContactId, "1-800-GOOG-411");
        return ContentUris.parseId(uri);
    }

    public void testDeletedContactsDelete_isUnsupported() {
        final Uri URI = ContactsContract.DeletedContacts.CONTENT_URI;
        DatabaseAsserts.assertDeleteIsUnsupported(mResolver, URI);

        Uri uri = ContentUris.withAppendedId(URI, 1L);
        DatabaseAsserts.assertDeleteIsUnsupported(mResolver, uri);
    }

    public void testDeletedContactsInsert_isUnsupported() {
        final Uri URI = ContactsContract.DeletedContacts.CONTENT_URI;
        DatabaseAsserts.assertInsertIsUnsupported(mResolver, URI);
    }


    public void testQueryDeletedContactsByContactId() {
        DatabaseAsserts.ContactIdPair ids = assertContactCreateDelete();

        MoreAsserts.assertNotEqual(CommonDatabaseUtils.NOT_FOUND,
                DeletedContactUtil.queryDeletedTimestampForContactId(mResolver, ids.mContactId));
    }

    public void testQueryDeletedContactsAll() {
        final int numDeletes = 10;

        // Since we cannot clean out delete log from previous tests, we need to account for that
        // by querying for the count first.
        final long startCount = DeletedContactUtil.getCount(mResolver);

        for (int i = 0; i < numDeletes; i++) {
            assertContactCreateDelete();
        }

        final long endCount = DeletedContactUtil.getCount(mResolver);

        assertEquals(numDeletes, endCount - startCount);
    }

    public void testQueryDeletedContactsSinceTimestamp() {
        sMockClock.install();

        // Before
        final HashSet<Long> beforeIds = new HashSet<Long>();
        beforeIds.add(assertContactCreateDelete().mContactId);
        beforeIds.add(assertContactCreateDelete().mContactId);

        final long start = sMockClock.currentTimeMillis();

        // After
        final HashSet<Long> afterIds = new HashSet<Long>();
        afterIds.add(assertContactCreateDelete().mContactId);
        afterIds.add(assertContactCreateDelete().mContactId);
        afterIds.add(assertContactCreateDelete().mContactId);

        final String[] projection = new String[]{
                ContactsContract.DeletedContacts.CONTACT_ID,
                ContactsContract.DeletedContacts.CONTACT_DELETED_TIMESTAMP
        };
        final List<String[]> records = DeletedContactUtil.querySinceTimestamp(mResolver, projection,
                start);
        for (String[] record : records) {
            // Check ids to make sure we only have the ones that came after the time.
            final long contactId = Long.parseLong(record[0]);
            assertFalse(beforeIds.contains(contactId));
            assertTrue(afterIds.contains(contactId));

            // Check times to make sure they came after
            assertTrue(Long.parseLong(record[1]) > start);
        }
    }

    /**
     * Creates a contact in the local account. Assert it's not present in the delete log.
     * Delete it. And assert that the contact record is no longer present.
     *
     * @return The contact id and raw contact id that was created.
     */
    private DatabaseAsserts.ContactIdPair assertContactCreateDelete() {
        return assertContactCreateDelete(null);
    }

    /**
     * Creates a contact in the given account. Assert it's not present in the delete log.
     * Delete it. And assert that the contact record is no longer present.
     * @return The contact id and raw contact id that was created.
     */
    private DatabaseAsserts.ContactIdPair assertContactCreateDelete(Account account) {
        DatabaseAsserts.ContactIdPair ids = DatabaseAsserts.assertAndCreateContact(mResolver,
                account);

        assertEquals(CommonDatabaseUtils.NOT_FOUND,
                DeletedContactUtil.queryDeletedTimestampForContactId(mResolver, ids.mContactId));

        sMockClock.advance();
        ContactUtil.delete(mResolver, ids.mContactId);

        assertFalse(ContactUtil.recordExistsForContactId(mResolver, ids.mContactId));

        return ids;
    }

    /**
     * End delta api tests.
     ******************************************************/

    /*******************************************************
     * Pinning support tests
     */
    public void testPinnedPositionsUpdate() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i2 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i3 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i4 = DatabaseAsserts.assertAndCreateContact(mResolver);

        final int unpinned = PinnedPositions.UNPINNED;

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i3.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i4.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, unpinned),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, unpinned),
                cv(RawContacts._ID, i3.mRawContactId, RawContacts.PINNED, unpinned),
                cv(RawContacts._ID, i4.mRawContactId, RawContacts.PINNED, unpinned)
        );

        final ArrayList<ContentProviderOperation> operations =
                new ArrayList<ContentProviderOperation>();

        operations.add(newPinningOperation(i1.mContactId, 1, true));
        operations.add(newPinningOperation(i3.mContactId, 3, true));
        operations.add(newPinningOperation(i4.mContactId, 2, false));

        CommonDatabaseUtils.applyBatch(mResolver, operations);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1, Contacts.STARRED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i3.mContactId, Contacts.PINNED, 3, Contacts.STARRED, 1),
                cv(Contacts._ID, i4.mContactId, Contacts.PINNED, 2, Contacts.STARRED, 0)
        );

        // Make sure the values are propagated to raw contacts

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, unpinned),
                cv(RawContacts._ID, i3.mRawContactId, RawContacts.PINNED, 3),
                cv(RawContacts._ID, i4.mRawContactId, RawContacts.PINNED, 2)
        );

        operations.clear();

        // Now unpin the contact
        operations.add(newPinningOperation(i3.mContactId, unpinned, false));

        CommonDatabaseUtils.applyBatch(mResolver, operations);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1, Contacts.STARRED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i3.mContactId, Contacts.PINNED, unpinned, Contacts.STARRED, 0),
                cv(Contacts._ID, i4.mContactId, Contacts.PINNED, 2, Contacts.STARRED, 0)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(Contacts._ID, i1.mRawContactId, RawContacts.PINNED, 1, RawContacts.STARRED, 1),
                cv(Contacts._ID, i2.mRawContactId, RawContacts.PINNED, unpinned,
                        RawContacts.STARRED, 0),
                cv(Contacts._ID, i3.mRawContactId, RawContacts.PINNED, unpinned,
                        RawContacts.STARRED, 0),
                cv(Contacts._ID, i4.mRawContactId, RawContacts.PINNED, 2, RawContacts.STARRED, 0)
        );
    }

    public void testPinnedPositionsAfterJoinAndSplit() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "A", "Smith");
        final DatabaseAsserts.ContactIdPair i2 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "B", "Smith");
        final DatabaseAsserts.ContactIdPair i3 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "C", "Smith");
        final DatabaseAsserts.ContactIdPair i4 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "D", "Smith");
        final DatabaseAsserts.ContactIdPair i5 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "E", "Smith");
        final DatabaseAsserts.ContactIdPair i6 = DatabaseAsserts.assertAndCreateContactWithName(
                mResolver, "F", "Smith");

        final ArrayList<ContentProviderOperation> operations =
                new ArrayList<ContentProviderOperation>();

        operations.add(newPinningOperation(i1.mContactId, 1, true));
        operations.add(newPinningOperation(i2.mContactId, 2, true));
        operations.add(newPinningOperation(i3.mContactId, 3, true));
        operations.add(newPinningOperation(i5.mContactId, 5, true));
        operations.add(newPinningOperation(i6.mContactId, 6, true));

        CommonDatabaseUtils.applyBatch(mResolver, operations);

        // aggregate raw contact 1 and 4 together.
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, i1.mRawContactId,
                i4.mRawContactId);

        // If only one contact is pinned, the resulting contact should inherit the pinned position
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 2),
                cv(Contacts._ID, i3.mContactId, Contacts.PINNED, 3),
                cv(Contacts._ID, i5.mContactId, Contacts.PINNED, 5),
                cv(Contacts._ID, i6.mContactId, Contacts.PINNED, 6)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, 2,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i3.mRawContactId, RawContacts.PINNED, 3,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i4.mRawContactId, RawContacts.PINNED, PinnedPositions.UNPINNED,
                        RawContacts.STARRED, 0),
                cv(RawContacts._ID, i5.mRawContactId, RawContacts.PINNED, 5,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i6.mRawContactId, RawContacts.PINNED, 6,
                        RawContacts.STARRED, 1)
        );

        // aggregate raw contact 2 and 3 together.
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, i2.mRawContactId,
                i3.mRawContactId);

        // If both raw contacts are pinned, the resulting contact should inherit the lower
        // pinned position
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 2),
                cv(Contacts._ID, i5.mContactId, Contacts.PINNED, 5),
                cv(Contacts._ID, i6.mContactId, Contacts.PINNED, 6)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, 2),
                cv(RawContacts._ID, i3.mRawContactId, RawContacts.PINNED, 3),
                cv(RawContacts._ID, i4.mRawContactId, RawContacts.PINNED,
                        PinnedPositions.UNPINNED),
                cv(RawContacts._ID, i5.mRawContactId, RawContacts.PINNED, 5),
                cv(RawContacts._ID, i6.mRawContactId, RawContacts.PINNED, 6)
        );

        // split the aggregated raw contacts
        setAggregationException(AggregationExceptions.TYPE_KEEP_SEPARATE, i1.mRawContactId,
                i4.mRawContactId);

        // raw contacts should be unpinned after being split, but still starred
        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, 2,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i3.mRawContactId, RawContacts.PINNED, 3,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i4.mRawContactId, RawContacts.PINNED, PinnedPositions.UNPINNED,
                        RawContacts.STARRED, 0),
                cv(RawContacts._ID, i5.mRawContactId, RawContacts.PINNED, 5,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i6.mRawContactId, RawContacts.PINNED, 6,
                        RawContacts.STARRED, 1)
        );

        // now demote contact 5
        operations.clear();
        operations.add(newPinningOperation(i5.mContactId, PinnedPositions.DEMOTED, false));
        CommonDatabaseUtils.applyBatch(mResolver, operations);

        // Get new contact Ids for contacts composing of raw contacts 1 and 4 because they have
        // changed.
        final long cId1 = RawContactUtil.queryContactIdByRawContactId(mResolver, i1.mRawContactId);
        final long cId4 = RawContactUtil.queryContactIdByRawContactId(mResolver, i4.mRawContactId);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, cId1, Contacts.PINNED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 2),
                cv(Contacts._ID, cId4, Contacts.PINNED, PinnedPositions.UNPINNED),
                cv(Contacts._ID, i5.mContactId, Contacts.PINNED, PinnedPositions.DEMOTED),
                cv(Contacts._ID, i6.mContactId, Contacts.PINNED, 6)
        );

        // aggregate contacts 5 and 6 together
        setAggregationException(AggregationExceptions.TYPE_KEEP_TOGETHER, i5.mRawContactId,
                i6.mRawContactId);

        // The resulting contact should have a pinned value of 6
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, cId1, Contacts.PINNED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 2),
                cv(Contacts._ID, cId4, Contacts.PINNED, PinnedPositions.UNPINNED),
                cv(Contacts._ID, i5.mContactId, Contacts.PINNED, 6)
        );
    }

    public void testPinnedPositionsDemoteIllegalArguments() {
        try {
            mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD,
                    null, null);
            fail();
        } catch (IllegalArgumentException expected) {
        }

        try {
            mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD,
                    "1.1", null);
            fail();
        } catch (IllegalArgumentException expected) {
        }

        try {
            mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD,
                    "NotANumber", null);
            fail();
        } catch (IllegalArgumentException expected) {
        }

        // Valid contact ID that does not correspond to an actual contact is silently ignored
        mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD, "999",
                null);
    }

    public void testPinnedPositionsAfterDemoteAndUndemote() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i2 = DatabaseAsserts.assertAndCreateContact(mResolver);

        // Pin contact 1 and demote contact 2
        final ArrayList<ContentProviderOperation> operations =
                new ArrayList<ContentProviderOperation>();
        operations.add(newPinningOperation(i1.mContactId, 1, true));
        operations.add(newPinningOperation(i2.mContactId, PinnedPositions.DEMOTED, false));
        CommonDatabaseUtils.applyBatch(mResolver, operations);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1, Contacts.STARRED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, PinnedPositions.DEMOTED,
                        Contacts.STARRED, 0)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, PinnedPositions.DEMOTED,
                        RawContacts.STARRED, 0)
        );

        // Now undemote both contacts
        mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD,
                String.valueOf(i1.mContactId), null);
        mResolver.call(ContactsContract.AUTHORITY_URI, PinnedPositions.UNDEMOTE_METHOD,
                String.valueOf(i2.mContactId), null);


        // Contact 1 remains pinned at 0, while contact 2 becomes unpinned
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1, Contacts.STARRED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, PinnedPositions.UNPINNED,
                        Contacts.STARRED, 0)
        );

        assertStoredValuesWithProjection(RawContacts.CONTENT_URI,
                cv(RawContacts._ID, i1.mRawContactId, RawContacts.PINNED, 1,
                        RawContacts.STARRED, 1),
                cv(RawContacts._ID, i2.mRawContactId, RawContacts.PINNED, PinnedPositions.UNPINNED,
                        RawContacts.STARRED, 0)
        );
    }

    /**
     * Verifies that any existing pinned contacts have their pinned positions incremented by one
     * after the upgrade step
     */
    public void testPinnedPositionsUpgradeTo906_PinnedContactsIncrementedByOne() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i2 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i3 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final ArrayList<ContentProviderOperation> operations =
                new ArrayList<ContentProviderOperation>();
        operations.add(newPinningOperation(i1.mContactId, 0, true));
        operations.add(newPinningOperation(i2.mContactId, 5, true));
        operations.add(newPinningOperation(i3.mContactId, Integer.MAX_VALUE - 2, true));
        CommonDatabaseUtils.applyBatch(mResolver, operations);

        final ContactsDatabaseHelper helper =
                ((ContactsDatabaseHelper) ((ContactsProvider2) getProvider()).getDatabaseHelper());
        SQLiteDatabase db = helper.getWritableDatabase();
        helper.upgradeToVersion906(db);
        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 1),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 6),
                cv(Contacts._ID, i3.mContactId, Contacts.PINNED, Integer.MAX_VALUE - 1)
        );
    }

    /**
     * Verifies that any unpinned contacts (or those with pinned position Integer.MAX_VALUE - 1)
     * have their pinned positions correctly set to 0 after the upgrade step.
     */
    public void testPinnedPositionsUpgradeTo906_UnpinnedValueCorrectlyUpdated() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final DatabaseAsserts.ContactIdPair i2 = DatabaseAsserts.assertAndCreateContact(mResolver);
        final ArrayList<ContentProviderOperation> operations =
                new ArrayList<ContentProviderOperation>();
        operations.add(newPinningOperation(i1.mContactId, Integer.MAX_VALUE -1 , true));
        operations.add(newPinningOperation(i2.mContactId, Integer.MAX_VALUE, true));
        CommonDatabaseUtils.applyBatch(mResolver, operations);

        final ContactsDatabaseHelper helper =
                ((ContactsDatabaseHelper) ((ContactsProvider2) getProvider()).getDatabaseHelper());
        SQLiteDatabase db = helper.getWritableDatabase();
        helper.upgradeToVersion906(db);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 0),
                cv(Contacts._ID, i2.mContactId, Contacts.PINNED, 0)
        );
    }

    /**
     * Tests the functionality of the
     * {@link ContactsContract.PinnedPositions#pin(ContentResolver, long, int)} API.
     */
    public void testPinnedPositions_ContactsContractPinnedPositionsPin() {
        final DatabaseAsserts.ContactIdPair i1 = DatabaseAsserts.assertAndCreateContact(mResolver);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, PinnedPositions.UNPINNED)
        );

        ContactsContract.PinnedPositions.pin(mResolver, i1.mContactId, 5);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, 5)
        );

        ContactsContract.PinnedPositions.pin(mResolver, i1.mContactId, PinnedPositions.UNPINNED);

        assertStoredValuesWithProjection(Contacts.CONTENT_URI,
                cv(Contacts._ID, i1.mContactId, Contacts.PINNED, PinnedPositions.UNPINNED)
        );
    }

    private ContentProviderOperation newPinningOperation(long id, int pinned, boolean star) {
        final Uri uri = Uri.withAppendedPath(Contacts.CONTENT_URI, String.valueOf(id));
        final ContentValues values = new ContentValues();
        values.put(Contacts.PINNED, pinned);
        values.put(Contacts.STARRED, star ? 1 : 0);
        return ContentProviderOperation.newUpdate(uri).withValues(values).build();
    }

    /**
     * End pinning support tests
     ******************************************************/

    public void testAuthorization_authorize() throws Exception {
        // Setup
        ContentValues values = new ContentValues();
        long id1 = createContact(values, "Noah", "Tever", "18004664411",
                "email@email.com", StatusUpdates.OFFLINE, 0, 0, 0, 0);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, id1);

        // Execute: pre authorize the contact
        Uri authorizedUri = getPreAuthorizedUri(contactUri);

        // Sanity check: URIs are different
        assertNotSame(authorizedUri, contactUri);

        // Verify: the URI is pre authorized
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        assertTrue(cp.isValidPreAuthorizedUri(authorizedUri));
    }

    public void testAuthorization_unauthorized() throws Exception {
        // Setup
        ContentValues values = new ContentValues();
        long id1 = createContact(values, "Noah", "Tever", "18004664411",
                "email@email.com", StatusUpdates.OFFLINE, 0, 0, 0, 0);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, id1);

        // Verify: the URI is *not* pre authorized
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        assertFalse(cp.isValidPreAuthorizedUri(contactUri));
    }

    public void testAuthorization_invalidAuthorization() throws Exception {
        // Setup
        ContentValues values = new ContentValues();
        long id1 = createContact(values, "Noah", "Tever", "18004664411",
                "email@email.com", StatusUpdates.OFFLINE, 0, 0, 0, 0);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, id1);

        // Execute: pre authorize the contact and then modify the resulting URI slightly
        Uri authorizedUri = getPreAuthorizedUri(contactUri);
        Uri almostAuthorizedUri = Uri.parse(authorizedUri.toString() + "2");

        // Verify: the URI is not pre authorized
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        assertFalse(cp.isValidPreAuthorizedUri(almostAuthorizedUri));
    }

    public void testAuthorization_expired() throws Exception {
        // Setup
        ContentValues values = new ContentValues();
        long id1 = createContact(values, "Noah", "Tever", "18004664411",
                "email@email.com", StatusUpdates.OFFLINE, 0, 0, 0, 0);
        Uri contactUri = ContentUris.withAppendedId(Contacts.CONTENT_URI, id1);
        sMockClock.install();

        // Execute: pre authorize the contact
        Uri authorizedUri = getPreAuthorizedUri(contactUri);
        sMockClock.setCurrentTimeMillis(sMockClock.currentTimeMillis() + 1000000);

        // Verify: the authorization for the URI expired
        final ContactsProvider2 cp = (ContactsProvider2) getProvider();
        assertFalse(cp.isValidPreAuthorizedUri(authorizedUri));
    }

    public void testAuthorization_contactUpgrade() throws Exception {
        ContactsDatabaseHelper helper =
                ((ContactsDatabaseHelper) ((ContactsProvider2) getProvider()).getDatabaseHelper());
        SQLiteDatabase db = helper.getWritableDatabase();

        // Perform the unit tests against an upgraded version of the database, instead of a freshly
        // created version of the database.
        helper.upgradeToVersion1002(db);
        testAuthorization_authorize();
        helper.upgradeToVersion1002(db);
        testAuthorization_expired();
        helper.upgradeToVersion1002(db);
        testAuthorization_expired();
        helper.upgradeToVersion1002(db);
        testAuthorization_invalidAuthorization();
    }

    private Uri getPreAuthorizedUri(Uri uri) {
        final Bundle uriBundle = new Bundle();
        uriBundle.putParcelable(ContactsContract.Authorization.KEY_URI_TO_AUTHORIZE, uri);
        final Bundle authResponse = mResolver.call(
                ContactsContract.AUTHORITY_URI,
                ContactsContract.Authorization.AUTHORIZATION_METHOD,
                null,
                uriBundle);
        return (Uri) authResponse.getParcelable(
                ContactsContract.Authorization.KEY_AUTHORIZED_URI);
    }

    /**
     * End Authorization Tests
     ******************************************************/

    private Cursor queryGroupMemberships(Account account) {
        Cursor c = mResolver.query(TestUtil.maybeAddAccountQueryParameters(Data.CONTENT_URI,
                        account),
                new String[] {GroupMembership.GROUP_ROW_ID, GroupMembership.RAW_CONTACT_ID},
                Data.MIMETYPE + "=?", new String[] {GroupMembership.CONTENT_ITEM_TYPE},
                GroupMembership.GROUP_SOURCE_ID);
        return c;
    }

    private String readToEnd(FileInputStream inputStream) {
        try {
            System.out.println("DECLARED INPUT STREAM LENGTH: " + inputStream.available());
            int ch;
            StringBuilder stringBuilder = new StringBuilder();
            int index = 0;
            while (true) {
                ch = inputStream.read();
                System.out.println("READ CHARACTER: " + index + " " + ch);
                if (ch == -1) {
                    break;
                }
                stringBuilder.append((char)ch);
                index++;
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            return null;
        }
    }

    private void assertQueryParameter(String uriString, String parameter, String expectedValue) {
        assertEquals(expectedValue, ContactsProvider2.getQueryParameter(
                Uri.parse(uriString), parameter));
    }

    private long createContact(ContentValues values, String firstName, String givenName,
            String phoneNumber, String email, int presenceStatus, int timesContacted, int starred,
            long groupId, int chatMode) {
        return createContact(values, firstName, givenName, phoneNumber, email, presenceStatus,
                timesContacted, starred, groupId, chatMode, false);
    }

    private long createContact(ContentValues values, String firstName, String givenName,
            String phoneNumber, String email, int presenceStatus, int timesContacted, int starred,
            long groupId, int chatMode, boolean isUserProfile) {
        return queryContactId(createRawContact(values, firstName, givenName, phoneNumber, email,
                presenceStatus, timesContacted, starred, groupId, chatMode, isUserProfile));
    }

    private long createRawContact(ContentValues values, String firstName, String givenName,
            String phoneNumber, String email, int presenceStatus, int timesContacted, int starred,
            long groupId, int chatMode) {
        long rawContactId = createRawContact(values, phoneNumber, email, presenceStatus,
                timesContacted, starred, groupId, chatMode);
        DataUtil.insertStructuredName(mResolver, rawContactId, firstName, givenName);
        return rawContactId;
    }

    private long createRawContact(ContentValues values, String firstName, String givenName,
            String phoneNumber, String email, int presenceStatus, int timesContacted, int starred,
            long groupId, int chatMode, boolean isUserProfile) {
        long rawContactId = createRawContact(values, phoneNumber, email, presenceStatus,
                timesContacted, starred, groupId, chatMode, isUserProfile);
        DataUtil.insertStructuredName(mResolver, rawContactId, firstName, givenName);
        return rawContactId;
    }

    private long createRawContact(ContentValues values, String phoneNumber, String email,
            int presenceStatus, int timesContacted, int starred, long groupId, int chatMode) {
        return createRawContact(values, phoneNumber, email, presenceStatus, timesContacted, starred,
                groupId, chatMode, false);
    }

    private long createRawContact(ContentValues values, String phoneNumber, String email,
            int presenceStatus, int timesContacted, int starred, long groupId, int chatMode,
            boolean isUserProfile) {
        values.put(RawContacts.STARRED, starred);
        values.put(RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(RawContacts.CUSTOM_RINGTONE, "beethoven5");
        values.put(RawContacts.TIMES_CONTACTED, timesContacted);

        Uri rawContactUri;
        if (isUserProfile) {
            rawContactUri = insertProfileRawContact(values);
        } else {
            rawContactUri = insertRawContact(values);
        }

        long rawContactId = ContentUris.parseId(rawContactUri);
        Uri photoUri = insertPhoto(rawContactId);
        long photoId = ContentUris.parseId(photoUri);
        values.put(Contacts.PHOTO_ID, photoId);
        if (!TextUtils.isEmpty(phoneNumber)) {
            insertPhoneNumber(rawContactId, phoneNumber);
        }
        if (!TextUtils.isEmpty(email)) {
            insertEmail(rawContactId, email);
        }

        insertStatusUpdate(Im.PROTOCOL_GOOGLE_TALK, null, email, presenceStatus, "hacking",
                chatMode, isUserProfile);

        if (groupId != 0) {
            insertGroupMembership(rawContactId, groupId);
        }

        return rawContactId;
    }

    /**
     * Creates a raw contact with pre-set values under the user's profile.
     * @param profileValues Values to be used to create the entry (common values will be
     *     automatically populated in createRawContact()).
     * @return the raw contact ID that was created.
     */
    private long createBasicProfileContact(ContentValues profileValues) {
        long profileRawContactId = createRawContact(profileValues, "Mia", "Prophyl",
                "18005554411", "mia.prophyl@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA, true);
        profileValues.put(Contacts.DISPLAY_NAME, "Mia Prophyl");
        return profileRawContactId;
    }

    /**
     * Creates a raw contact with pre-set values that is not under the user's profile.
     * @param nonProfileValues Values to be used to create the entry (common values will be
     *     automatically populated in createRawContact()).
     * @return the raw contact ID that was created.
     */
    private long createBasicNonProfileContact(ContentValues nonProfileValues) {
        long nonProfileRawContactId = createRawContact(nonProfileValues, "John", "Doe",
                "18004664411", "goog411@acme.com", StatusUpdates.INVISIBLE, 4, 1, 0,
                StatusUpdates.CAPABILITY_HAS_CAMERA, false);
        nonProfileValues.put(Contacts.DISPLAY_NAME, "John Doe");
        return nonProfileRawContactId;
    }

    private void putDataValues(ContentValues values, long rawContactId) {
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, "testmimetype");
        values.put(Data.RES_PACKAGE, "oldpackage");
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.DATA1, "one");
        values.put(Data.DATA2, "two");
        values.put(Data.DATA3, "three");
        values.put(Data.DATA4, "four");
        values.put(Data.DATA5, "five");
        values.put(Data.DATA6, "six");
        values.put(Data.DATA7, "seven");
        values.put(Data.DATA8, "eight");
        values.put(Data.DATA9, "nine");
        values.put(Data.DATA10, "ten");
        values.put(Data.DATA11, "eleven");
        values.put(Data.DATA12, "twelve");
        values.put(Data.DATA13, "thirteen");
        values.put(Data.DATA14, "fourteen");
        values.put(Data.DATA15, "fifteen".getBytes());
        values.put(Data.CARRIER_PRESENCE, Data.CARRIER_PRESENCE_VT_CAPABLE);
        values.put(Data.PREFERRED_PHONE_ACCOUNT_COMPONENT_NAME, "preferredcomponentname");
        values.put(Data.PREFERRED_PHONE_ACCOUNT_ID, "preferredid");
        values.put(Data.SYNC1, "sync1");
        values.put(Data.SYNC2, "sync2");
        values.put(Data.SYNC3, "sync3");
        values.put(Data.SYNC4, "sync4");
    }

    /**
     * @param data1 email address or phone number
     * @param usageType One of {@link DataUsageFeedback#USAGE_TYPE}
     * @param values ContentValues for this feedback. Useful for incrementing
     * {Contacts#TIMES_CONTACTED} in the ContentValue. Can be null.
     */
    private void sendFeedback(String data1, String usageType, ContentValues values) {
        final long dataId = getStoredLongValue(Data.CONTENT_URI,
                Data.DATA1 + "=?", new String[] { data1 }, Data._ID);
        assertEquals(0, updateDataUsageFeedback(usageType, dataId));
        if (values != null && values.containsKey(Contacts.TIMES_CONTACTED)) {
            values.put(Contacts.TIMES_CONTACTED, values.getAsInteger(Contacts.TIMES_CONTACTED) + 1);
        }
    }

    private void updateDataUsageFeedback(String usageType, Uri resultUri) {
        final long id = ContentUris.parseId(resultUri);
        final boolean successful = updateDataUsageFeedback(usageType, id) > 0;
        assertFalse(successful); // shouldn't succeed
    }

    private int updateDataUsageFeedback(String usageType, long... ids) {
        final StringBuilder idList = new StringBuilder();
        for (long id : ids) {
            if (idList.length() > 0) idList.append(",");
            idList.append(id);
        }
        return mResolver.update(DataUsageFeedback.FEEDBACK_URI.buildUpon()
                .appendPath(idList.toString())
                .appendQueryParameter(DataUsageFeedback.USAGE_TYPE, usageType)
                .build(), new ContentValues(), null, null);
    }

    private boolean hasChineseCollator() {
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.CHINA)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasJapaneseCollator() {
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.JAPAN)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasGermanCollator() {
        final Locale locale[] = Collator.getAvailableLocales();
        for (int i = 0; i < locale.length; i++) {
            if (locale[i].equals(Locale.GERMANY)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Asserts the equality of two Uri objects, ignoring the order of the query parameters.
     */
    public static void assertUriEquals(Uri expected, Uri actual) {
        assertEquals(expected.getScheme(), actual.getScheme());
        assertEquals(expected.getAuthority(), actual.getAuthority());
        assertEquals(expected.getPath(), actual.getPath());
        assertEquals(expected.getFragment(), actual.getFragment());
        Set<String> expectedParameterNames = expected.getQueryParameterNames();
        Set<String> actualParameterNames = actual.getQueryParameterNames();
        assertEquals(expectedParameterNames.size(), actualParameterNames.size());
        assertTrue(expectedParameterNames.containsAll(actualParameterNames));
        for (String parameterName : expectedParameterNames) {
            assertEquals(expected.getQueryParameter(parameterName),
                    actual.getQueryParameter(parameterName));
        }

    }
}
