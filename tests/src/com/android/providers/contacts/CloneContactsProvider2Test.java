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
import static com.android.providers.contacts.ContactsActor.MockUserManager.PRIMARY_USER;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;

import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.OperationApplicationException;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.os.Build;
import android.os.Bundle;
import android.os.RemoteException;
import android.provider.CallLog;
import android.provider.ContactsContract;
import android.util.SparseArray;

import androidx.test.filters.MediumTest;
import androidx.test.filters.SdkSuppress;

import org.junit.Assert;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

@MediumTest
@SdkSuppress(minSdkVersion = Build.VERSION_CODES.UPSIDE_DOWN_CAKE, codeName = "UpsideDownCake")
public class CloneContactsProvider2Test extends BaseContactsProvider2Test {

    private ContactsActor mCloneContactsActor;
    private SynchronousContactsProvider2 mCloneContactsProvider;

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
        mCloneContactsProvider = spy(getCloneContactsProvider());
        mCloneContactsProvider.wipeData();
    }

    private ContentValues getSampleContentValues() {
        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.ACCOUNT_NAME, "test@test.com");
        values.put(ContactsContract.RawContacts.ACCOUNT_TYPE, "test.com");
        values.put(ContactsContract.RawContacts.CUSTOM_RINGTONE, "custom");
        values.put(ContactsContract.RawContacts.STARRED, "1");
        return values;
    }

    private void getCloneContactsProviderWithMockedCallToParent(Uri uri) {
        Cursor primaryProfileCursor = mActor.provider.query(uri,
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(primaryProfileCursor);
        doReturn(primaryProfileCursor).when(mCloneContactsProvider)
                .queryContactsProviderForUser(eq(uri), any(), any(), any(), any(),
                        any(), eq(PRIMARY_USER));
    }

    private void getCloneContactsProviderWithMockedOpenAssetFileCall(Uri uri)
            throws FileNotFoundException {
        AssetFileDescriptor fileDescriptor = mActor.provider.openAssetFile(uri, "r");
        doReturn(fileDescriptor).when(mCloneContactsProvider)
                .openAssetFileThroughParentProvider(eq(uri), eq("r"));
    }

    private String getCursorValue(Cursor c, String columnName) {
        return c.getString(c.getColumnIndex(columnName));
    }

    private void assertEqualContentValues(ContentValues contentValues, Cursor cursor) {
        for (String key: contentValues.getValues().keySet()) {
            assertEquals(contentValues.get(key), getCursorValue(cursor, key));
        }
    }

    private void assertRawContactsCursorEquals(Cursor expectedCursor, Cursor actualCursor,
            Set<String> columnNames) {
        assertNotNull(actualCursor);
        assertEquals(expectedCursor.getCount(), actualCursor.getCount());
        while (actualCursor.moveToNext()) {
            expectedCursor.moveToNext();
            for (String key: columnNames) {
                assertEquals(getCursorValue(expectedCursor, key),
                        getCursorValue(actualCursor, key));
            }
        }
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
     * Asserts that no contacts are returned when queried by the given contacts provider
     */
    private void assertContactsProviderEmpty(ContactsProvider2 contactsProvider2) {
        Cursor cursor = contactsProvider2.query(ContactsContract.RawContacts.CONTENT_URI,
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
        // No contacts should be present in both clone and primary providers
        assertContactsProviderEmpty(getContactsProvider());
        doReturn(false)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());
        assertContactsProviderEmpty(mCloneContactsProvider);

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
        // No contacts should be present in both clone and primary providers
        assertContactsProviderEmpty(getContactsProvider());
        doReturn(false)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());
        assertContactsProviderEmpty(mCloneContactsProvider);
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
        assertContactsProviderEmpty(getContactsProvider());
        doReturn(false)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());
        assertContactsProviderEmpty(mCloneContactsProvider);
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

    public void testCloneContactsProviderReads_callerNotInAllowlist() {
        // Insert raw contact through the primary clone provider
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);

        // Mock call to parent profile contacts provider to return the correct result containing all
        // contacts in the parent profile.
        getCloneContactsProviderWithMockedCallToParent(uri);

        // Mock call to ensure the caller package is not in the app-cloning allowlist
        doReturn(false)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());

        // Test clone contacts provider read with the uri of the contact added above
        mCloneContactsProvider.query(uri,
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);

        // Check that the call passed through to the local query instead of redirecting to the
        // parent provider
        verify(mCloneContactsProvider, times(1))
                .queryDirectoryIfNecessary(any(), any(), any(), any(), any(), any());
    }

    public void testContactsProviderReads_callerInAllowlist() {
        // Insert raw contact through the primary clone provider
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);

        // Mock call to parent profile contacts provider to return the correct result containing all
        // contacts in the parent profile.
        getCloneContactsProviderWithMockedCallToParent(uri);

        // Mock call to ensure the caller package is in the app-cloning allowlist
        doReturn(true)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());

        // Test clone contacts provider read with the uri of the contact added above
        Cursor cursor = mCloneContactsProvider.query(uri,
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);

        // Check that the call did not pass through to the local query and instead redirected to the
        // parent provider
        verify(mCloneContactsProvider, times(0))
                .queryDirectoryIfNecessary(any(), any(), any(), any(), any(), any());
        assertNotNull(cursor);
        Cursor primaryProfileCursor = mActor.provider.query(uri,
                null /* projection */, null /* queryArgs */, null /* cancellationSignal */);
        assertNotNull(primaryProfileCursor);
        assertRawContactsCursorEquals(primaryProfileCursor, cursor,
                inputContentValues.getValues().keySet());
    }

    public void testQueryPrimaryProfileProvider_callingFromParentUser() {
        ContentValues inputContentValues = getSampleContentValues();
        long rawContactId = insertRawContactsThroughPrimaryProvider(inputContentValues);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);

        // Fetch primary contacts provider and call method to redirect to parent provider
        final ContactsProvider2 primaryCP2 = (ContactsProvider2) getProvider();
        Cursor cursor = primaryCP2.queryParentProfileContactsProvider(uri,
                null /* projection */, null /* selection */, null /* selectionArgs */,
                null /* sortOrder */, null /* cancellationSignal */);

        // Assert that empty cursor is returned
        assertNotNull(cursor);
        assertEquals(0, cursor.getCount());
    }

    public void testQueryPrimaryProfileProvider_incorrectAuthority() {
        ContentValues inputContentValues = getSampleContentValues();
        insertRawContactsThroughPrimaryProvider(inputContentValues);

        Assert.assertThrows(IllegalArgumentException.class, () ->
                mCloneContactsProvider.queryParentProfileContactsProvider(CallLog.CONTENT_URI,
                null /* projection */, null /* selection */, null /* selectionArgs */,
                null /* sortOrder */, null /* cancellationSignal */));
    }

    public void testOpenAssetFileMultiVCard() throws IOException {
        final VCardTestUriCreator contacts = createVCardTestContacts();

        // Mock call to parent profile contacts provider to return the correct asset file
        getCloneContactsProviderWithMockedOpenAssetFileCall(contacts.getCombinedUri());

        // Mock call to ensure the caller package is in the app-cloning allowlist
        doReturn(true)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());

        final AssetFileDescriptor descriptor =
                mCloneContactsProvider.openAssetFile(contacts.getCombinedUri(), "r");
        final FileInputStream inputStream = descriptor.createInputStream();
        String data = readToEnd(inputStream);
        inputStream.close();
        descriptor.close();

        // Ensure that the resulting VCard has both contacts
        assertTrue(data.contains("N:Doe;John;;;"));
        assertTrue(data.contains("N:Doh;Jane;;;"));
    }

    public void testOpenAssetFileMultiVCard_callerNotInAllowlist() throws IOException {
        final VCardTestUriCreator contacts = createVCardTestContacts();

        // Mock call to parent profile contacts provider to return the correct asset file
        getCloneContactsProviderWithMockedOpenAssetFileCall(contacts.getCombinedUri());

        // Mock call to ensure the caller package is not in the app-cloning allowlist
        doReturn(false)
                .when(mCloneContactsProvider).isAppAllowedToUseParentUsersContacts(any());

        final AssetFileDescriptor descriptor =
                mCloneContactsProvider.openAssetFile(contacts.getCombinedUri(), "r");

        // Check that the call passed through to the local call instead of redirecting to the
        // parent provider
        verify(mCloneContactsProvider, times(1))
                .openAssetFile(eq(contacts.getCombinedUri()), any());
    }

    public void testIsAppAllowedToUseParentUsersContacts_AppInAllowlistCacheEmpty()
            throws InterruptedException {
        String testPackageName = mCloneContactsActor.packageName;
        int processUid = Binder.getCallingUid();
        doReturn(true)
                .when(mCloneContactsProvider)
                .doesPackageHaveALauncherActivity(eq(testPackageName), any());

        SparseArray<ContactsProvider2.LaunchableCloneAppsCacheEntry> launchableCloneAppsCache =
                mCloneContactsProvider.getLaunchableCloneAppsCacheForTesting();
        launchableCloneAppsCache.clear();
        boolean appAllowedToUseParentUsersContacts =
                mCloneContactsProvider.isAppAllowedToUseParentUsersContacts(testPackageName);
        assertTrue(appAllowedToUseParentUsersContacts);

        // Check that the cache has been updated with an entry corresponding to current app uid
        ContactsProvider2.LaunchableCloneAppsCacheEntry cacheEntry =
                launchableCloneAppsCache.get(processUid);
        assertNotNull(cacheEntry);
        assertEquals(1, launchableCloneAppsCache.size());
        assertTrue(cacheEntry.doesAppHaveLaunchableActivity);
    }

    public void testIsAppAllowedToUseParentUsersContacts_AppNotInAllowlistCacheEmtpy() {
        String testPackageName = mCloneContactsActor.packageName;
        int processUid = Binder.getCallingUid();

        SparseArray<ContactsProvider2.LaunchableCloneAppsCacheEntry> launchableCloneAppsCache =
                mCloneContactsProvider.getLaunchableCloneAppsCacheForTesting();
        launchableCloneAppsCache.clear();
        assertFalse(mCloneContactsProvider.isAppAllowedToUseParentUsersContacts(testPackageName));

        // Check that the cache has been updated with an entry corresponding to current app uid
        ContactsProvider2.LaunchableCloneAppsCacheEntry cacheEntry =
                launchableCloneAppsCache.get(processUid);
        assertNotNull(cacheEntry);
        assertEquals(1, launchableCloneAppsCache.size());
        assertFalse(cacheEntry.doesAppHaveLaunchableActivity);
    }
}
