/*
 * Copyright (C) 2024 The Android Open Source Project
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

import android.accounts.Account;
import android.content.ContentUris;
import android.content.ContentValues;
import android.net.Uri;
import android.platform.test.flag.junit.SetFlagsRule;
import android.provider.ContactsContract;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StreamItemPhotos;
import android.provider.ContactsContract.StreamItems;

import androidx.test.filters.MediumTest;

import com.android.providers.contacts.tests.R;
import com.android.providers.contacts.testutil.RawContactUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Unit tests for {@link ContactsProvider2} UnSync API.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -e class com.android.providers.contacts.UnSyncAccountsTest -w \
 * com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
@RunWith(JUnit4.class)
public class UnSyncAccountsTest extends BaseContactsProvider2Test {
    @ClassRule
    public static final SetFlagsRule.ClassRule mClassRule = new SetFlagsRule.ClassRule();

    @Rule
    public final SetFlagsRule mSetFlagsRule = mClassRule.createSetFlagsRule();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testAccountUnSynced() {
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
        storeValue(photoUri2, ContactsContract.CommonDataKinds.Photo.IS_SUPER_PRIMARY, "1");

        assertAggregated(rawContactId1, rawContactId2);

        long contactId = queryContactId(rawContactId1);

        // The display name should come from the writable account
        assertStoredValue(Uri.withAppendedPath(
                        ContentUris.withAppendedId(ContactsContract.Contacts.CONTENT_URI,
                                contactId),
                        ContactsContract.Contacts.Data.CONTENT_DIRECTORY),
                ContactsContract.Contacts.DISPLAY_NAME, "john doe");

        // The photo should be the one we marked as super-primary
        assertStoredValue(ContactsContract.Contacts.CONTENT_URI, contactId,
                ContactsContract.Contacts.PHOTO_ID, ContentUris.parseId(photoUri2));

        mActor.setAccounts(new Account[]{readOnlyAccount, mAccount});
        // Un Sync account.
        cp.unSyncAccounts(new Account[]{mAccount});

        // The display name should come from the remaining account
        assertStoredValue(Uri.withAppendedPath(
                        ContentUris.withAppendedId(ContactsContract.Contacts.CONTENT_URI,
                                contactId),
                        ContactsContract.Contacts.Data.CONTENT_DIRECTORY),
                ContactsContract.Contacts.DISPLAY_NAME, "John Doe");

        // The photo should be the remaining one
        assertStoredValue(ContactsContract.Contacts.CONTENT_URI, contactId,
                ContactsContract.Contacts.PHOTO_ID, ContentUris.parseId(photoUri1));
    }

    @Test
    public void testStreamItemsCleanedUpOnAccountUnSynced() {
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

        // UnSync the doomed account.
        cp.unSyncAccounts(new Account[]{doomedAccount});

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
}
