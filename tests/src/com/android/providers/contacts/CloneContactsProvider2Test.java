/*
 * Copyright (C) 2022 The Android Open Source Project
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

import static com.android.providers.contacts.ContactsActor.MockUserManager.CLONE_PROFILE_USER;

import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.OperationApplicationException;
import android.database.Cursor;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.RemoteException;
import android.provider.ContactsContract;
import androidx.test.filters.MediumTest;
import androidx.test.filters.SdkSuppress;

import java.util.ArrayList;

@MediumTest
@SdkSuppress(minSdkVersion = Build.VERSION_CODES.UPSIDE_DOWN_CAKE, codeName = "UpsideDownCake")
public class CloneContactsProvider2Test extends BaseContactsProvider2Test {

    private ContactsActor mCloneContactsActor;

    private SynchronousContactsProvider2 getCloneContactsProvider() {
        return (SynchronousContactsProvider2) mCloneContactsActor.provider;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mCloneContactsActor = new ContactsActor(
                new ContactsActor.AlteringUserContext(getContext(), CLONE_PROFILE_USER.id),
                getContextPackageName(), SynchronousContactsProvider2.class, getAuthority());
        mActor.mockUserManager.setUsers(ContactsActor.MockUserManager.PRIMARY_USER,
                CLONE_PROFILE_USER);
        mCloneContactsActor.mockUserManager.setUsers(ContactsActor.MockUserManager.PRIMARY_USER,
                CLONE_PROFILE_USER);
        mCloneContactsActor.mockUserManager.myUser = CLONE_PROFILE_USER.id;
        getCloneContactsProvider().wipeData();
    }

    private ContentValues getSampleContentValues() {
        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.ACCOUNT_NAME, "test@test.com");
        values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, "test.com");
        values.put(ContactsContract.RawContacts.CUSTOM_RINGTONE, "custom");
        values.put(ContactsContract.RawContacts.STARRED, "1");
        return values;
    }

    private void assertEqualContentValues(ContentValues contentValues, Cursor cursor) {
        assertEquals(contentValues.get(ContactsContract.RawContacts.ACCOUNT_NAME),
                cursor.getString(cursor.getColumnIndex(ContactsContract.RawContacts.ACCOUNT_NAME)));
        assertEquals(contentValues.get(ContactsContract.RawContacts.ACCOUNT_TYPE),
                cursor.getString(cursor.getColumnIndex(ContactsContract.RawContacts.ACCOUNT_TYPE)));
        assertEquals(contentValues.get(ContactsContract.RawContacts.CUSTOM_RINGTONE),
                cursor.getString(cursor.getColumnIndex(
                        ContactsContract.RawContacts.CUSTOM_RINGTONE)));
        assertEquals(contentValues.get(ContactsContract.RawContacts.STARRED),
                cursor.getString(cursor.getColumnIndex(
                        ContactsContract.RawContacts.STARRED)));
    }

    private void assertRejectedApplyBatchResults(ContentProviderResult[] res,
            ArrayList<ContentProviderOperation> ops) {
        assertEquals(ops.size(), res.length);
        for (int i = 0;i < ops.size();i++) {
            Uri expectedUri = ops.get(i).getUri()
                    .buildUpon()
                    .appendPath("0")
                    .build();
            assertUriEquals(expectedUri, res[i].uri);
        }
    }

    /**
     * Asserts that no contacts are returned when queried by the given contacts actor
     */
    private void assertContactsProviderEmpty(ContactsActor contactsActor) {
        Cursor cursor = contactsActor.resolver.query(ContactsContract.RawContacts.CONTENT_URI,
                new String[]{ContactsContract.RawContactsEntity._ID},
                null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(cursor);
        assertEquals(cursor.getCount(), 0);
    }

    private long insertRawContactsThroughPrimaryProvider(ContentValues values) {
        Uri resultUri = mActor.resolver.insert(ContactsContract.RawContacts.CONTENT_URI,
                values);
        assertNotNull(resultUri);
        return ContentUris.parseId(resultUri);
    }

    public void testAreContactWritesEnabled() {
        // Check that writes are disabled for clone CP2
        ContactsProvider2 cloneContactsProvider =
                (ContactsProvider2) mCloneContactsActor.provider;
        assertFalse(cloneContactsProvider.areContactWritesEnabled());

        // Check that writes are enabled for primary CP2
        ContactsProvider2 primaryContactsProvider = (ContactsProvider2) getProvider();
        assertTrue(primaryContactsProvider.areContactWritesEnabled());
    }

    public void testCloneContactsProviderInsert() {
        Uri resultUri =
                mCloneContactsActor.resolver.insert(ContactsContract.RawContacts.CONTENT_URI,
                        getSampleContentValues());

        // Here we expect a fakeUri returned to fail silently
        Uri expectedUri = ContactsContract.RawContacts.CONTENT_URI.buildUpon()
                .appendPath("0")
                .build();
        assertUriEquals(expectedUri, resultUri);
        assertContactsProviderEmpty(mCloneContactsActor);
    }

    public void testPrimaryContactsProviderInsert() {
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);
        Cursor cursor = mActor.resolver.query(ContentUris.withAppendedId(
                ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        assertTrue(cursor.moveToFirst());
        assertEquals(rawContactId,
                cursor.getLong(cursor.getColumnIndex(ContactsContract.RawContacts._ID)));
        assertEqualContentValues(inputContentValues, cursor);
    }

    public void testCloneContactsProviderUpdate() {
        // Insert contact through the primary clone provider
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);

        // Update display name in the input content values
        ContentValues updatedContentValues = getSampleContentValues();
        updatedContentValues.put(ContactsContract.RawContacts.STARRED,
                "0");
        updatedContentValues.put(ContactsContract.RawContacts.CUSTOM_RINGTONE,
                "beethoven5");

        // Call clone contacts provider update method to update the raw contact inserted earlier
        int updateResult = mCloneContactsActor.resolver.update(
                ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                updatedContentValues, null /* extras */);

        // Check results, no rows should have been affected
        assertEquals(0, updateResult);

        // Check values associated with rawContactId by querying the database
        Cursor cursor = mActor.resolver.query(ContentUris.withAppendedId(
                        ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        assertTrue(cursor.moveToFirst());
        assertEqualContentValues(inputContentValues, cursor);
    }

    public void testCloneContactsProviderDelete() {
        // Insert contact through the primary clone provider
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);

        // Delete the inserted row through clone provider
        int deleteResult = mCloneContactsActor.resolver.delete(
                ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                null);

        // Check results, no rows should have been affected
        assertEquals(0, deleteResult);

        // Check that contact is present in the primary CP2 database
        Cursor cursor = mActor.resolver.query(ContentUris.withAppendedId(
                        ContactsContract.RawContacts.CONTENT_URI, rawContactId),
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        assertTrue(cursor.moveToFirst());
        assertEqualContentValues(inputContentValues, cursor);
    }

    public void testCloneContactsProviderBulkInsert() {
        int bulkInsertResult =
                mCloneContactsActor.resolver.bulkInsert(ContactsContract.RawContacts.CONTENT_URI,
                new ContentValues[]{ getSampleContentValues() });

        // Check results, no rows should have been affected
        assertEquals(0, bulkInsertResult);
        assertContactsProviderEmpty(mCloneContactsActor);
    }

    public void testCloneContactsApplyBatch()
            throws RemoteException, OperationApplicationException {
        ArrayList<ContentProviderOperation> ops = new ArrayList<>();

        ops.add(ContentProviderOperation.newInsert(ContactsContract.RawContacts.CONTENT_URI)
                .withValue(ContactsContract.RawContacts.ACCOUNT_TYPE, null /* value */)
                .withValue(ContactsContract.RawContacts.ACCOUNT_NAME, null /* value */).build());

        // Phone Number
        ops.add(ContentProviderOperation
                .newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE,
                        ContactsContract.CommonDataKinds.Phone.CONTENT_ITEM_TYPE)
                .withValue(ContactsContract.CommonDataKinds.Phone.NUMBER, "7XXXXXXXXXX")
                .withValue(ContactsContract.Data.MIMETYPE,
                        ContactsContract.CommonDataKinds.Phone.CONTENT_ITEM_TYPE)
                .withValue(ContactsContract.CommonDataKinds.Phone.TYPE, "1").build());

        // Display name/Contact name
        ops.add(ContentProviderOperation
                .newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE,
                        ContactsContract.CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)
                .withValue(ContactsContract.CommonDataKinds.StructuredName.DISPLAY_NAME, "Name")
                .build());

        // Check results, fake uris should be returned for each of the insert operation
        ContentProviderResult[] res = mCloneContactsActor.resolver.applyBatch(
                    ContactsContract.AUTHORITY, ops);
        assertRejectedApplyBatchResults(res, ops);

        // No contacts should be present in both clone and primary providers
        assertContactsProviderEmpty(mCloneContactsActor);
        assertContactsProviderEmpty(mActor);
    }

    public void testCloneContactsCallOperation() {
        // Query Account Operation
        Bundle response = mCloneContactsActor.resolver.call(ContactsContract.AUTHORITY_URI,
                ContactsContract.Settings.QUERY_DEFAULT_ACCOUNT_METHOD, null /* arg */,
                null /* extras */);
        assertNotNull(response);
        assertEquals(Bundle.EMPTY, response);

        // Set account operation
        Bundle bundle = new Bundle();
        bundle.putString(ContactsContract.Settings.ACCOUNT_NAME, "test@test.com");
        bundle.putString(ContactsContract.Settings.ACCOUNT_TYPE, "test.com");
        Bundle setAccountResponse =
                mCloneContactsActor.resolver.call(ContactsContract.AUTHORITY_URI,
                ContactsContract.Settings.SET_DEFAULT_ACCOUNT_METHOD, null /* arg */, bundle);
        assertNotNull(setAccountResponse);
        assertEquals(Bundle.EMPTY, response);

        // Authorization URI
        Uri testUri = ContentUris.withAppendedId(ContactsContract.Contacts.CONTENT_URI, 1);
        final Bundle uriBundle = new Bundle();
        uriBundle.putParcelable(ContactsContract.Authorization.KEY_URI_TO_AUTHORIZE, testUri);
        final Bundle authResponse = mCloneContactsActor.resolver.call(
                ContactsContract.AUTHORITY_URI,
                ContactsContract.Authorization.AUTHORIZATION_METHOD,
                null /* arg */,
                uriBundle);
        assertNotNull(authResponse);
        assertEquals(Bundle.EMPTY, authResponse);
    }
}
