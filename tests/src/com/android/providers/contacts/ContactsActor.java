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

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.res.Configuration;
import android.content.res.Resources;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.test.IsolatedContext;
import android.test.RenamingDelegatingContext;
import android.test.mock.MockContentResolver;
import android.test.mock.MockContext;
import android.test.mock.MockPackageManager;
import android.test.mock.MockResources;
import android.util.TypedValue;

import java.util.HashMap;
import java.util.Locale;

/**
 * Helper class that encapsulates an "actor" which is owned by a specific
 * package name. It correctly maintains a wrapped {@link Context} and an
 * attached {@link MockContentResolver}. Multiple actors can be used to test
 * security scenarios between multiple packages.
 */
public class ContactsActor {
    private static final String FILENAME_PREFIX = "test.";

    public static final String PACKAGE_GREY = "edu.example.grey";
    public static final String PACKAGE_RED = "net.example.red";
    public static final String PACKAGE_GREEN = "com.example.green";
    public static final String PACKAGE_BLUE = "org.example.blue";

    public Context context;
    public String packageName;
    public MockContentResolver resolver;
    public ContentProvider provider;

    private IsolatedContext mProviderContext;

    /**
     * Create an "actor" using the given parent {@link Context} and the specific
     * package name. Internally, all {@link Context} method calls are passed to
     * a new instance of {@link RestrictionMockContext}, which stubs out the
     * security infrastructure.
     */
    public ContactsActor(Context overallContext, String packageName,
            Class<? extends ContentProvider> providerClass, String authority) throws Exception {
        resolver = new MockContentResolver();
        context = new RestrictionMockContext(overallContext, packageName, resolver);
        this.packageName = packageName;

        RenamingDelegatingContext targetContextWrapper = new RenamingDelegatingContext(context,
                overallContext, FILENAME_PREFIX);
        mProviderContext = new IsolatedContext(resolver, targetContextWrapper);
        provider = addProvider(providerClass, authority);
    }

    public void addAuthority(String authority) {
        resolver.addProvider(authority, provider);
    }

    public ContentProvider addProvider(Class<? extends ContentProvider> providerClass,
            String authority) throws Exception {
        ContentProvider provider = providerClass.newInstance();
        provider.attachInfo(mProviderContext, null);
        resolver.addProvider(authority, provider);
        return provider;
    }

    /**
     * Mock {@link Context} that reports specific well-known values for testing
     * data protection. The creator can override the owner package name, and
     * force the {@link PackageManager} to always return a well-known package
     * list for any call to {@link PackageManager#getPackagesForUid(int)}.
     * <p>
     * For example, the creator could request that the {@link Context} lives in
     * package name "com.example.red", and also cause the {@link PackageManager}
     * to report that no UID contains that package name.
     */
    private static class RestrictionMockContext extends MockContext {
        private final Context mOverallContext;
        private final String mReportedPackageName;
        private final RestrictionMockPackageManager mPackageManager;
        private final ContentResolver mResolver;
        private final Resources mRes;

        /**
         * Create a {@link Context} under the given package name.
         */
        public RestrictionMockContext(Context overallContext, String reportedPackageName,
                ContentResolver resolver) {
            mOverallContext = overallContext;
            mReportedPackageName = reportedPackageName;
            mResolver = resolver;

            mPackageManager = new RestrictionMockPackageManager();
            mPackageManager.addPackage(1000, PACKAGE_GREY);
            mPackageManager.addPackage(2000, PACKAGE_RED);
            mPackageManager.addPackage(3000, PACKAGE_GREEN);
            mPackageManager.addPackage(4000, PACKAGE_BLUE);

            Resources resources = overallContext.getResources();
            Configuration configuration = new Configuration(resources.getConfiguration());
            configuration.locale = Locale.US;
            resources.updateConfiguration(configuration, resources.getDisplayMetrics());
            mRes = new RestrictionMockResources(resources);
        }

        @Override
        public String getPackageName() {
            return mReportedPackageName;
        }

        @Override
        public PackageManager getPackageManager() {
            return mPackageManager;
        }

        @Override
        public Resources getResources() {
            return mRes;
        }

        @Override
        public ContentResolver getContentResolver() {
            return mResolver;
        }
    }

    private static class RestrictionMockResources extends MockResources {
        private static final String UNRESTRICTED = "unrestricted_packages";
        private static final int UNRESTRICTED_ID = 1024;

        private static final String[] UNRESTRICTED_LIST = new String[] {
            PACKAGE_GREY
        };

        private final Resources mRes;

        public RestrictionMockResources(Resources res) {
            mRes = res;
        }

        @Override
        public int getIdentifier(String name, String defType, String defPackage) {
            if (UNRESTRICTED.equals(name)) {
                return UNRESTRICTED_ID;
            } else {
                return mRes.getIdentifier(name, defType, defPackage);
            }
        }

        @Override
        public String[] getStringArray(int id) throws NotFoundException {
            if (id == UNRESTRICTED_ID) {
                return UNRESTRICTED_LIST;
            } else {
                return mRes.getStringArray(id);
            }
        }

        @Override
        public void getValue(int id, TypedValue outValue, boolean resolveRefs)
                throws NotFoundException {
            mRes.getValue(id, outValue, resolveRefs);
        }

        @Override
        public String getString(int id) throws NotFoundException {
            return mRes.getString(id);
        }

        @Override
        public String getString(int id, Object... formatArgs) throws NotFoundException {
            return mRes.getString(id, formatArgs);
        }

        @Override
        public CharSequence getText(int id) throws NotFoundException {
            return mRes.getText(id);
        }
    }

    private static String sCallingPackage = null;

    void ensureCallingPackage() {
        sCallingPackage = this.packageName;
    }

    /**
     * Mock {@link PackageManager} that knows about a specific set of packages
     * to help test security models. Because {@link Binder#getCallingUid()}
     * can't be mocked, you'll have to find your mock-UID manually using your
     * {@link Context#getPackageName()}.
     */
    private static class RestrictionMockPackageManager extends MockPackageManager {
        private final HashMap<Integer, String> mForward = new HashMap<Integer, String>();
        private final HashMap<String, Integer> mReverse = new HashMap<String, Integer>();

        public RestrictionMockPackageManager() {
        }

        /**
         * Add a UID-to-package mapping, which is then stored internally.
         */
        public void addPackage(int packageUid, String packageName) {
            mForward.put(packageUid, packageName);
            mReverse.put(packageName, packageUid);
        }

        @Override
        public String getNameForUid(int uid) {
            return "name-for-uid";
        }

        @Override
        public String[] getPackagesForUid(int uid) {
            return new String[] { sCallingPackage };
        }

        @Override
        public ApplicationInfo getApplicationInfo(String packageName, int flags) {
            ApplicationInfo info = new ApplicationInfo();
            Integer uid = mReverse.get(packageName);
            info.uid = (uid != null) ? uid : -1;
            return info;
        }
    }

    public long createRawContact(boolean isRestricted, String name) {
        ensureCallingPackage();
        long rawContactId = createRawContact(isRestricted);
        createName(rawContactId, name);
        return rawContactId;
    }

    public long createRawContact(boolean isRestricted) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        if (isRestricted) {
            values.put(RawContacts.IS_RESTRICTED, 1);
        }

        Uri rawContactUri = resolver.insert(RawContacts.CONTENT_URI, values);
        return ContentUris.parseId(rawContactUri);
    }

    public long createRawContactWithStatus(boolean isRestricted, String name, String address,
            String status) {
        final long rawContactId = createRawContact(isRestricted, name);
        final long dataId = createEmail(rawContactId, address);
        createStatus(dataId, status);
        return rawContactId;
    }

    public long createName(long contactId, String name) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, contactId);
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.MIMETYPE, CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE);
        values.put(CommonDataKinds.StructuredName.FAMILY_NAME, name);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                contactId), RawContacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    public long createPhone(long contactId, String phoneNumber) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, contactId);
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values.put(ContactsContract.CommonDataKinds.Phone.TYPE,
                ContactsContract.CommonDataKinds.Phone.TYPE_HOME);
        values.put(ContactsContract.CommonDataKinds.Phone.NUMBER, phoneNumber);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                contactId), RawContacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    public long createEmail(long contactId, String address) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, contactId);
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        values.put(Email.TYPE, Email.TYPE_HOME);
        values.put(Email.DATA, address);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                contactId), RawContacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    public long createStatus(long dataId, String status) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(StatusUpdates.DATA_ID, dataId);
        values.put(StatusUpdates.STATUS, status);
        Uri dataUri = resolver.insert(StatusUpdates.CONTENT_URI, values);
        return ContentUris.parseId(dataUri);
    }

    public void updateException(String packageProvider, String packageClient, boolean allowAccess) {
        throw new UnsupportedOperationException("RestrictionExceptions are hard-coded");
    }

    public long getContactForRawContact(long rawContactId) {
        ensureCallingPackage();
        Uri contactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        final Cursor cursor = resolver.query(contactUri, Projections.PROJ_RAW_CONTACTS, null,
                null, null);
        if (!cursor.moveToFirst()) {
            cursor.close();
            throw new RuntimeException("Contact didn't have an aggregate");
        }
        final long aggId = cursor.getLong(Projections.COL_CONTACTS_ID);
        cursor.close();
        return aggId;
    }

    public int getDataCountForContact(long contactId) {
        ensureCallingPackage();
        Uri contactUri = Uri.withAppendedPath(ContentUris.withAppendedId(Contacts.CONTENT_URI,
                contactId), Contacts.Data.CONTENT_DIRECTORY);
        final Cursor cursor = resolver.query(contactUri, Projections.PROJ_ID, null, null,
                null);
        final int count = cursor.getCount();
        cursor.close();
        return count;
    }

    public int getDataCountForRawContact(long rawContactId) {
        ensureCallingPackage();
        Uri contactUri = Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                rawContactId), Contacts.Data.CONTENT_DIRECTORY);
        final Cursor cursor = resolver.query(contactUri, Projections.PROJ_ID, null, null,
                null);
        final int count = cursor.getCount();
        cursor.close();
        return count;
    }

    public void setSuperPrimaryPhone(long dataId) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        Uri updateUri = ContentUris.withAppendedId(Data.CONTENT_URI, dataId);
        resolver.update(updateUri, values, null, null);
    }

    public long createGroup(String groupName) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(ContactsContract.Groups.RES_PACKAGE, packageName);
        values.put(ContactsContract.Groups.TITLE, groupName);
        Uri groupUri = resolver.insert(ContactsContract.Groups.CONTENT_URI, values);
        return ContentUris.parseId(groupUri);
    }

    public long createGroupMembership(long rawContactId, long groupId) {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        values.put(Data.MIMETYPE, CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE);
        values.put(CommonDataKinds.GroupMembership.GROUP_ROW_ID, groupId);
        Uri insertUri = Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI,
                rawContactId), RawContacts.Data.CONTENT_DIRECTORY);
        Uri dataUri = resolver.insert(insertUri, values);
        return ContentUris.parseId(dataUri);
    }

    protected void setAggregationException(int type, long rawContactId1, long rawContactId2) {
        ContentValues values = new ContentValues();
        values.put(AggregationExceptions.RAW_CONTACT_ID1, rawContactId1);
        values.put(AggregationExceptions.RAW_CONTACT_ID2, rawContactId2);
        values.put(AggregationExceptions.TYPE, type);
        resolver.update(AggregationExceptions.CONTENT_URI, values, null, null);
    }

    /**
     * Various internal database projections.
     */
    private interface Projections {
        static final String[] PROJ_ID = new String[] {
                BaseColumns._ID,
        };

        static final int COL_ID = 0;

        static final String[] PROJ_RAW_CONTACTS = new String[] {
                RawContacts.CONTACT_ID
        };

        static final int COL_CONTACTS_ID = 0;
    }
}
