/*
 * Copyright (C) 2010 The Android Open Source Project
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

import com.android.providers.contacts.ContactsDatabaseHelper.AggregationExceptionColumns;
import com.google.android.collect.Lists;

import android.accounts.Account;
import android.content.ContentValues;
import android.content.Context;
import android.content.pm.PackageInfo;
import android.content.pm.ProviderInfo;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.Bundle;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.RawContacts;
import android.test.mock.MockContentProvider;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link ContactDirectoryManager}. Run the test like this:
 * <code>
 *   adb shell am instrument -e class com.android.providers.contacts.ContactDirectoryManagerTest
 *       -w com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactDirectoryManagerTest extends BaseContactsProvider2Test {

    private ContactsMockPackageManager mPackageManager;

    private ContactsProvider2 mProvider;

    private ContactDirectoryManager mDirectoryManager;

    public static class MockContactDirectoryProvider extends MockContentProvider {

        private String mAuthority;

        private MatrixCursor mResponse;

        @Override
        public void attachInfo(Context context, ProviderInfo info) {
            mAuthority = info.authority;
        }

        public MatrixCursor createResponseCursor() {
            mResponse = new MatrixCursor(
                    new String[] { Directory.ACCOUNT_NAME, Directory.ACCOUNT_TYPE,
                            Directory.DISPLAY_NAME, Directory.TYPE_RESOURCE_ID,
                            Directory.EXPORT_SUPPORT, Directory.SHORTCUT_SUPPORT, });

            return mResponse;
        }

        @Override
        public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                String sortOrder) {

            if (uri.toString().equals("content://" + mAuthority + "/directories")) {
                return mResponse;
            } else if (uri.toString().startsWith("content://" + mAuthority + "/contacts")) {
                MatrixCursor cursor = new MatrixCursor(
                        new String[] { "projection", "selection", "selectionArgs", "sortOrder",
                                "accountName", "accountType"});
                cursor.addRow(new Object[] {
                    Lists.newArrayList(projection).toString(),
                    selection,
                    Lists.newArrayList(selectionArgs).toString(),
                    sortOrder,
                    uri.getQueryParameter(RawContacts.ACCOUNT_NAME),
                    uri.getQueryParameter(RawContacts.ACCOUNT_TYPE),
                });
                return cursor;
            } else if (uri.toString().startsWith(
                    "content://" + mAuthority + "/aggregation_exceptions")) {
                return new MatrixCursor(projection);
            }

            fail("Unexpected uri: " + uri);
            return null;
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        mProvider = (ContactsProvider2) getProvider();
        mDirectoryManager = mProvider.getContactDirectoryManager();

        mPackageManager = (ContactsMockPackageManager) getProvider()
                .getContext().getPackageManager();
    }

    public void testScanAllProviders() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(
                        createProviderPackage("test.package1", "authority1"),
                        createProviderPackage("test.package2", "authority2")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);
        addDirectoryRow(response1, "account-name2", "account-type2", "display-name2", 2,
                Directory.EXPORT_SUPPORT_ANY_ACCOUNT, Directory.SHORTCUT_SUPPORT_DATA_ITEMS_ONLY);

        MockContactDirectoryProvider provider2 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority2");

        MatrixCursor response2 = provider2.createResponseCursor();
        addDirectoryRow(response2, "account-name3", "account-type3", "display-name3", 3,
                Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY, Directory.SHORTCUT_SUPPORT_FULL);

        mDirectoryManager.scanAllPackages();

        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(5, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name1", "account-type1",
                "display-name1", 1, Directory.EXPORT_SUPPORT_NONE);

        cursor.moveToNext();
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name2", "account-type2",
                "display-name2", 2, Directory.EXPORT_SUPPORT_ANY_ACCOUNT);

        cursor.moveToNext();
        assertDirectoryRow(cursor, "test.package2", "authority2", "account-name3", "account-type3",
                "display-name3", 3, Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY);

        cursor.close();
    }

    public void testPackageInstalled() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(createProviderPackage("test.package1", "authority1")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        mDirectoryManager.scanAllPackages();

        // At this point the manager has discovered a single directory (plus two
        // standard ones).
        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(3, cursor.getCount());
        cursor.close();

        // Pretend to install another package
        MockContactDirectoryProvider provider2 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority2");

        MatrixCursor response2 = provider2.createResponseCursor();
        addDirectoryRow(response2, "account-name3", "account-type3", "display-name3", 3,
                Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY, Directory.SHORTCUT_SUPPORT_FULL);

        mPackageManager.getInstalledPackages(0).add(
                createProviderPackage("test.package2", "authority2"));

        mProvider.onPackageChanged("test.package2");

        cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(4, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name1", "account-type1",
                "display-name1", 1, Directory.EXPORT_SUPPORT_NONE);

        cursor.moveToNext();
        assertDirectoryRow(cursor, "test.package2", "authority2", "account-name3", "account-type3",
                "display-name3", 3, Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY);

        cursor.close();
    }

    public void testPackageUninstalled() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(
                        createProviderPackage("test.package1", "authority1"),
                        createProviderPackage("test.package2", "authority2")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        MockContactDirectoryProvider provider2 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority2");

        MatrixCursor response2 = provider2.createResponseCursor();
        addDirectoryRow(response2, "account-name3", "account-type3", "display-name3", 3,
                Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY, Directory.SHORTCUT_SUPPORT_FULL);

        mDirectoryManager.scanAllPackages();

        // At this point the manager has discovered two custom directories.
        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(4, cursor.getCount());
        cursor.close();

        // Pretend to uninstall one of the packages
        mPackageManager.getInstalledPackages(0).remove(1);

        mProvider.onPackageChanged("test.package2");

        cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(3, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name1", "account-type1",
                "display-name1", 1, Directory.EXPORT_SUPPORT_NONE);

        cursor.close();
    }

    public void testPackageReplaced() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(
                        createProviderPackage("test.package1", "authority1"),
                        createProviderPackage("test.package2", "authority2")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        MockContactDirectoryProvider provider2 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority2");

        MatrixCursor response2 = provider2.createResponseCursor();
        addDirectoryRow(response2, "account-name3", "account-type3", "display-name3", 3,
                Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY, Directory.SHORTCUT_SUPPORT_FULL);

        mDirectoryManager.scanAllPackages();

        // At this point the manager has discovered two custom directories.
        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(4, cursor.getCount());
        cursor.close();

        // Pretend to replace the package with a different provider inside
        MatrixCursor response3 = provider2.createResponseCursor();
        addDirectoryRow(response3, "account-name4", "account-type4", "display-name4", 4,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        mProvider.onPackageChanged("test.package2");

        cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(4, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name1", "account-type1",
                "display-name1", 1, Directory.EXPORT_SUPPORT_NONE);

        cursor.moveToNext();
        assertDirectoryRow(cursor, "test.package2", "authority2", "account-name4", "account-type4",
                "display-name4", 4, Directory.EXPORT_SUPPORT_NONE);

        cursor.close();
    }

    public void testAccountRemoval() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(
                        createProviderPackage("test.package1", "authority1"),
                        createProviderPackage("test.package2", "authority2")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        ((ContactsProvider2)getProvider()).onAccountsUpdated(
                new Account[]{
                        new Account("account-name1", "account-type1"),
                        new Account("account-name2", "account-type2")});

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);
        addDirectoryRow(response1, "account-name2", "account-type2", "display-name2", 2,
                Directory.EXPORT_SUPPORT_ANY_ACCOUNT, Directory.SHORTCUT_SUPPORT_DATA_ITEMS_ONLY);

        mDirectoryManager.scanAllPackages();

        ((ContactsProvider2)getProvider()).onAccountsUpdated(
                new Account[]{new Account("account-name1", "account-type1")});

        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(3, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name1", "account-type1",
                "display-name1", 1, Directory.EXPORT_SUPPORT_NONE);

        cursor.close();
    }

    public void testNotifyDirectoryChange() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(createProviderPackage("test.package1", "authority1")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        mDirectoryManager.scanAllPackages();

        // Pretend to replace the package with a different provider inside
        MatrixCursor response2 = provider1.createResponseCursor();
        addDirectoryRow(response2, "account-name2", "account-type2", "display-name2", 2,
                Directory.EXPORT_SUPPORT_ANY_ACCOUNT, Directory.SHORTCUT_SUPPORT_FULL);

        ContactsContract.Directory.notifyDirectoryChange(mResolver);

        Cursor cursor = mResolver.query(Directory.CONTENT_URI, null, null, null, null);
        assertEquals(3, cursor.getCount());

        cursor.moveToPosition(2);
        assertDirectoryRow(cursor, "test.package1", "authority1", "account-name2", "account-type2",
                "display-name2", 2, Directory.EXPORT_SUPPORT_ANY_ACCOUNT);

        cursor.close();
    }

    public void testForwardingToDirectoryProvider() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(createProviderPackage("test.package1", "authority1")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        mDirectoryManager.scanAllPackages();

        Cursor cursor = mResolver.query(
                Directory.CONTENT_URI, new String[] { Directory._ID }, null, null, null);
        cursor.moveToPosition(2);
        long directoryId = cursor.getLong(0);
        cursor.close();

        Uri contentUri = Contacts.CONTENT_URI.buildUpon().appendQueryParameter(
                ContactsContract.DIRECTORY_PARAM_KEY, String.valueOf(directoryId)).build();

        // The request should be forwarded to TestProvider, which will simply
        // package arguments and return them to us for verification
        cursor = mResolver.query(contentUri,
                new String[]{"f1", "f2"}, "query", new String[]{"s1", "s2"}, "so");
        assertNotNull(cursor);
        assertEquals(1, cursor.getCount());
        cursor.moveToFirst();
        assertEquals("[f1, f2]", cursor.getString(cursor.getColumnIndex("projection")));
        assertEquals("query", cursor.getString(cursor.getColumnIndex("selection")));
        assertEquals("[s1, s2]", cursor.getString(cursor.getColumnIndex("selectionArgs")));
        assertEquals("so", cursor.getString(cursor.getColumnIndex("sortOrder")));
        assertEquals("account-name1", cursor.getString(cursor.getColumnIndex("accountName")));
        assertEquals("account-type1", cursor.getString(cursor.getColumnIndex("accountType")));
        cursor.close();
    }

    public void testProjectionPopulated() throws Exception {
        mPackageManager.setInstalledPackages(
                Lists.newArrayList(createProviderPackage("test.package1", "authority1")));

        MockContactDirectoryProvider provider1 = (MockContactDirectoryProvider) addProvider(
                MockContactDirectoryProvider.class, "authority1");

        MatrixCursor response1 = provider1.createResponseCursor();
        addDirectoryRow(response1, "account-name1", "account-type1", "display-name1", 1,
                Directory.EXPORT_SUPPORT_NONE, Directory.SHORTCUT_SUPPORT_NONE);

        mDirectoryManager.scanAllPackages();

        Cursor cursor = mResolver.query(
                Directory.CONTENT_URI, new String[] { Directory._ID }, null, null, null);
        cursor.moveToPosition(2);
        long directoryId = cursor.getLong(0);
        cursor.close();

        Uri contentUri = AggregationExceptions.CONTENT_URI.buildUpon().appendQueryParameter(
                ContactsContract.DIRECTORY_PARAM_KEY, String.valueOf(directoryId)).build();

        // The request should be forwarded to TestProvider, which will return an empty cursor
        // but the projection should be correctly populated by ContactProvider
        assertProjection(contentUri, new String[]{
                AggregationExceptionColumns._ID,
                AggregationExceptions.TYPE,
                AggregationExceptions.RAW_CONTACT_ID1,
                AggregationExceptions.RAW_CONTACT_ID2,
        });
    }

    protected PackageInfo createProviderPackage(String packageName, String authority) {
        PackageInfo providerPackage = new PackageInfo();
        providerPackage.packageName = packageName;
        ProviderInfo providerInfo = new ProviderInfo();
        providerInfo.packageName = providerPackage.packageName;
        providerInfo.authority = authority;
        providerInfo.metaData = new Bundle();
        providerInfo.metaData.putBoolean("android.content.ContactDirectory", true);
        providerPackage.providers = new ProviderInfo[] { providerInfo };
        return providerPackage;
    }

    protected void addDirectoryRow(MatrixCursor cursor, String accountName, String accountType,
            String displayName, int typeResourceId, int exportSupport, int shortcutSupport) {
        Object[] row = new Object[cursor.getColumnCount()];
        row[cursor.getColumnIndex(Directory.ACCOUNT_NAME)] = accountName;
        row[cursor.getColumnIndex(Directory.ACCOUNT_TYPE)] = accountType;
        row[cursor.getColumnIndex(Directory.DISPLAY_NAME)] = displayName;
        row[cursor.getColumnIndex(Directory.TYPE_RESOURCE_ID)] = typeResourceId;
        row[cursor.getColumnIndex(Directory.EXPORT_SUPPORT)] = exportSupport;
        row[cursor.getColumnIndex(Directory.SHORTCUT_SUPPORT)] = shortcutSupport;
        cursor.addRow(row);
    }

    protected void assertDirectoryRow(Cursor cursor, String packageName, String authority,
            String accountName, String accountType, String displayName, int typeResourceId,
            int exportSupport) {
        ContentValues values = new ContentValues();
        values.put(Directory.PACKAGE_NAME, packageName);
        values.put(Directory.DIRECTORY_AUTHORITY, authority);
        values.put(Directory.ACCOUNT_NAME, accountName);
        values.put(Directory.ACCOUNT_TYPE, accountType);
        values.put(Directory.DISPLAY_NAME, displayName);
        values.put(Directory.TYPE_RESOURCE_ID, typeResourceId);
        values.put(Directory.EXPORT_SUPPORT, exportSupport);

        // TODO: uncomment once shortcut support is implemented
        // values.put(Directory.SHORTCUT_SUPPORT,
        // Directory.SHORTCUT_SUPPORT_DATA_ITEMS_ONLY);

        assertCursorValues(cursor, values);
    }
}
