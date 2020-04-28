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

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AccountManagerCallback;
import android.accounts.AccountManagerFuture;
import android.accounts.AuthenticatorException;
import android.accounts.OnAccountsUpdateListener;
import android.accounts.OperationCanceledException;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.ContextWrapper;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.ProviderInfo;
import android.content.pm.UserInfo;
import android.content.res.Configuration;
import android.content.res.Resources;
import android.database.Cursor;
import android.location.Country;
import android.location.CountryDetector;
import android.location.CountryListener;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.UserHandle;
import android.os.UserManager;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.StatusUpdates;
import android.telephony.TelephonyManager;
import android.test.IsolatedContext;
import android.test.mock.MockContentResolver;
import android.test.mock.MockContext;
import android.text.TextUtils;

import com.android.providers.contacts.util.ContactsPermissions;
import com.android.providers.contacts.util.MockSharedPreferences;

import com.google.android.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
    private Country mMockCountry = new Country("us", 0);

    private Account[] mAccounts = new Account[0];

    private Set<String> mGrantedPermissions = Sets.newHashSet();
    private final Set<Uri> mGrantedUriPermissions = Sets.newHashSet();
    private boolean mHasCarrierPrivileges;

    private List<ContentProvider> mAllProviders = new ArrayList<>();

    private CountryDetector mMockCountryDetector = new CountryDetector(null){
        @Override
        public Country detectCountry() {
            return mMockCountry;
        }

        @Override
        public void addCountryListener(CountryListener listener, Looper looper) {
        }
    };

    private AccountManager mMockAccountManager;

    private class MockAccountManager extends AccountManager {
        public MockAccountManager(Context conteact) {
            super(context, null, null);
        }

        @Override
        public void addOnAccountsUpdatedListener(OnAccountsUpdateListener listener,
                Handler handler, boolean updateImmediately) {
            // do nothing
        }

        @Override
        public Account[] getAccounts() {
            return mAccounts;
        }

        @Override
        public AccountManagerFuture<Account[]> getAccountsByTypeAndFeatures(
                final String type, final String[] features,
                AccountManagerCallback<Account[]> callback, Handler handler) {
            return null;
        }

        @Override
        public String blockingGetAuthToken(Account account, String authTokenType,
                boolean notifyAuthFailure)
                throws OperationCanceledException, IOException, AuthenticatorException {
            return null;
        }
    }

    public MockUserManager mockUserManager;

    public static class MockUserManager extends UserManager {
        public static UserInfo createUserInfo(String name, int id, int groupId, int flags) {
            final UserInfo ui = new UserInfo();
            ui.name = name;
            ui.id = id;
            ui.profileGroupId = groupId;
            ui.flags = flags | UserInfo.FLAG_INITIALIZED;
            return ui;
        }

        public static final UserInfo PRIMARY_USER = createUserInfo("primary", 0, 0,
                UserInfo.FLAG_PRIMARY | UserInfo.FLAG_ADMIN);
        public static final UserInfo CORP_USER = createUserInfo("corp", 10, 0,
                UserInfo.FLAG_MANAGED_PROFILE);
        public static final UserInfo SECONDARY_USER = createUserInfo("2nd", 11, 11, 0);

        /** "My" user.  Set it to change the current user. */
        public int myUser = 0;

        private ArrayList<UserInfo> mUsers = new ArrayList<>();

        public MockUserManager(Context context) {
            super(context, /* IUserManager */ null);

            mUsers.add(PRIMARY_USER); // Add the primary user.
        }

        /** Replaces users. */
        public void setUsers(UserInfo... users) {
            mUsers.clear();
            for (UserInfo ui : users) {
                mUsers.add(ui);
            }
        }

        @Override
        public int getUserHandle() {
            return myUser;
        }

        @Override
        public UserInfo getUserInfo(int userHandle) {
            for (UserInfo ui : mUsers) {
                if (ui.id == userHandle) {
                    return ui;
                }
            }
            return null;
        }

        @Override
        public UserInfo getProfileParent(int userHandle) {
            final UserInfo child = getUserInfo(userHandle);
            if (child == null) {
                return null;
            }
            for (UserInfo ui : mUsers) {
                if (ui.id != userHandle && ui.id == child.profileGroupId) {
                    return ui;
                }
            }
            return null;
        }

        @Override
        public List<UserInfo> getUsers() {
            return mUsers;
        }

        @Override
        public Bundle getUserRestrictions(UserHandle userHandle) {
            return new Bundle();
        }

        @Override
        public boolean hasUserRestriction(String restrictionKey) {
            return false;
        }

        @Override
        public boolean hasUserRestriction(String restrictionKey, UserHandle userHandle) {
            return false;
        }

        @Override
        public boolean isSameProfileGroup(int userId, int otherUserId) {
            return getUserInfo(userId).profileGroupId == getUserInfo(otherUserId).profileGroupId;
        }

        @Override
        public boolean isUserUnlocked(int userId) {
            return true; // Just make it always unlocked for now.
        }
    }

    private MockTelephonyManager mMockTelephonyManager;

    private class MockTelephonyManager extends TelephonyManager {
        public MockTelephonyManager(Context context) {
            super(context);
        }

        @Override
        public int checkCarrierPrivilegesForPackageAnyPhone(String packageName) {
            if (TextUtils.equals(packageName, ContactsActor.this.packageName)
                    && mHasCarrierPrivileges) {
                return TelephonyManager.CARRIER_PRIVILEGE_STATUS_HAS_ACCESS;
            }
            return TelephonyManager.CARRIER_PRIVILEGE_STATUS_NO_ACCESS;
        }

        @Override
        public List<String> getPackagesWithCarrierPrivileges() {
            if (!mHasCarrierPrivileges) {
                return Collections.emptyList();
            }
            return Collections.singletonList(packageName);
        }
    }

    /**
     * A context wrapper that reports a different user id.
     *
     * TODO This should override getSystemService() and returns a UserManager that returns the
     * same, altered user ID too.
     */
    public static class AlteringUserContext extends ContextWrapper {
        private final int mUserId;

        public AlteringUserContext(Context base, int userId) {
            super(base);
            mUserId = userId;
        }

        @Override
        public int getUserId() {
            return mUserId;
        }
    }

    private IsolatedContext mProviderContext;

    /**
     * Create an "actor" using the given parent {@link Context} and the specific
     * package name. Internally, all {@link Context} method calls are passed to
     * a new instance of {@link RestrictionMockContext}, which stubs out the
     * security infrastructure.
     */
    public ContactsActor(final Context overallContext, String packageName,
            Class<? extends ContentProvider> providerClass, String authority) throws Exception {

        // Force permission check even when called by self.
        ContactsPermissions.ALLOW_SELF_CALL = false;

        resolver = new MockContentResolver();
        context = new RestrictionMockContext(overallContext, packageName, resolver,
                mGrantedPermissions, mGrantedUriPermissions) {
            @Override
            public Object getSystemService(String name) {
                if (Context.COUNTRY_DETECTOR.equals(name)) {
                    return mMockCountryDetector;
                }
                if (Context.ACCOUNT_SERVICE.equals(name)) {
                    return mMockAccountManager;
                }
                if (Context.USER_SERVICE.equals(name)) {
                    return mockUserManager;
                }
                if (Context.TELEPHONY_SERVICE.equals(name)) {
                    return mMockTelephonyManager;
                }
                // Use overallContext here; super.getSystemService() somehow won't return
                // DevicePolicyManager.
                return overallContext.getSystemService(name);
            }



            @Override
            public String getSystemServiceName(Class<?> serviceClass) {
                return overallContext.getSystemServiceName(serviceClass);
            }
        };
        this.packageName = packageName;

        // Let the Secure class initialize the settings provider, which is done when we first
        // tries to get any setting.  Because our mock context/content resolver doesn't have the
        // settings provider, we need to do this with an actual context, before other classes
        // try to do this with a mock context.
        // (Otherwise ContactsProvider2.initialzie() will crash trying to get a setting with
        // a mock context.)
        android.provider.Settings.Secure.getString(overallContext.getContentResolver(), "dummy");

        RenamingDelegatingContext targetContextWrapper = new RenamingDelegatingContext(context,
                overallContext, FILENAME_PREFIX);
        mProviderContext = new IsolatedContext(resolver, targetContextWrapper) {
            private final MockSharedPreferences mPrefs = new MockSharedPreferences();

            @Override
            public File getFilesDir() {
                // TODO: Need to figure out something more graceful than this.
                return new File("/data/data/com.android.providers.contacts.tests/files");
            }

            @Override
            public Object getSystemService(String name) {
                if (Context.COUNTRY_DETECTOR.equals(name)) {
                    return mMockCountryDetector;
                }
                if (Context.ACCOUNT_SERVICE.equals(name)) {
                    return mMockAccountManager;
                }
                if (Context.USER_SERVICE.equals(name)) {
                    return mockUserManager;
                }
                if (Context.TELEPHONY_SERVICE.equals(name)) {
                    return mMockTelephonyManager;
                }
                // Use overallContext here; super.getSystemService() somehow won't return
                // DevicePolicyManager.
                return overallContext.getSystemService(name);
            }

            @Override
            public String getSystemServiceName(Class<?> serviceClass) {
                return overallContext.getSystemServiceName(serviceClass);
            }

            @Override
            public SharedPreferences getSharedPreferences(String name, int mode) {
                return mPrefs;
            }

            @Override
            public int getUserId() {
                return mockUserManager.getUserHandle();
            }

            @Override
            public void sendBroadcast(Intent intent, String receiverPermission) {
                // Ignore.
            }

            @Override
            public Context getApplicationContext() {
                return this;
            }
        };

        mMockAccountManager = new MockAccountManager(mProviderContext);
        mockUserManager = new MockUserManager(mProviderContext);
        mMockTelephonyManager = new MockTelephonyManager(mProviderContext);
        provider = addProvider(providerClass, authority);
    }

    public Context getProviderContext() {
        return mProviderContext;
    }

    public <T extends ContentProvider> T addProvider(Class<T> providerClass,
            String authority) throws Exception {
        return addProvider(providerClass, authority, mProviderContext);
    }

    public <T extends ContentProvider> T addProvider(Class<T> providerClass,
            String authority, Context providerContext) throws Exception {
        return addProvider(providerClass.newInstance(), authority, providerContext);
    }

    public <T extends ContentProvider> T addProvider(T provider,
            String authority, Context providerContext) throws Exception {
        ProviderInfo info = new ProviderInfo();

        // Here, authority can have "user-id@".  We want to use it for addProvider, but provider
        // info shouldn't have it.
        info.authority = stripOutUserIdFromAuthority(authority);
        provider.attachInfoForTesting(providerContext, info);

        // In case of LegacyTest, "authority" here is actually multiple authorities.
        // Register all authority here.
        for (String a : authority.split(";")) {
            resolver.addProvider(a, provider);
            resolver.addProvider("0@" + a, provider);
        }
        mAllProviders.add(provider);
        return provider;
    }

    /**
     * Takes an provider authority. If it has "userid@", then remove it.
     */
    private String stripOutUserIdFromAuthority(String authority) {
        final int pos = authority.indexOf('@');
        return pos < 0 ? authority : authority.substring(pos + 1);
    }

    public void addPermissions(String... permissions) {
        mGrantedPermissions.addAll(Arrays.asList(permissions));
    }

    public void removePermissions(String... permissions) {
        mGrantedPermissions.removeAll(Arrays.asList(permissions));
    }

    public void addUriPermissions(Uri... uris) {
        mGrantedUriPermissions.addAll(Arrays.asList(uris));
    }

    public void removeUriPermissions(Uri... uris) {
        mGrantedUriPermissions.removeAll(Arrays.asList(uris));
    }

    public void grantCarrierPrivileges() {
        mHasCarrierPrivileges = true;
    }

    public void revokeCarrierPrivileges() {
        mHasCarrierPrivileges = false;
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
        private final ContactsMockPackageManager mPackageManager;
        private final ContentResolver mResolver;
        private final Resources mRes;
        private final Set<String> mGrantedPermissions;
        private final Set<Uri> mGrantedUriPermissions;

        /**
         * Create a {@link Context} under the given package name.
         */
        public RestrictionMockContext(Context overallContext, String reportedPackageName,
                ContentResolver resolver, Set<String> grantedPermissions,
                Set<Uri> grantedUriPermissions) {
            mOverallContext = overallContext;
            mReportedPackageName = reportedPackageName;
            mResolver = resolver;
            mGrantedPermissions = grantedPermissions;
            mGrantedUriPermissions = grantedUriPermissions;

            mPackageManager = new ContactsMockPackageManager(overallContext);
            mPackageManager.addPackage(1000, PACKAGE_GREY);
            mPackageManager.addPackage(2000, PACKAGE_RED);
            mPackageManager.addPackage(3000, PACKAGE_GREEN);
            mPackageManager.addPackage(4000, PACKAGE_BLUE);

            Resources resources = overallContext.getResources();
            Configuration configuration = new Configuration(resources.getConfiguration());
            configuration.locale = Locale.US;
            resources.updateConfiguration(configuration, resources.getDisplayMetrics());
            mRes = resources;
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

        @Override
        public ApplicationInfo getApplicationInfo() {
            ApplicationInfo ai = new ApplicationInfo();
            ai.packageName = "contactsTestPackage";
            return ai;
        }

        // All permission checks are implemented to simply check against the granted permission set.

        @Override
        public int checkPermission(String permission, int pid, int uid) {
            return checkCallingPermission(permission);
        }

        @Override
        public int checkCallingPermission(String permission) {
            if (mGrantedPermissions.contains(permission)) {
                return PackageManager.PERMISSION_GRANTED;
            } else {
                return PackageManager.PERMISSION_DENIED;
            }
        }

        @Override
        public int checkUriPermission(Uri uri, int pid, int uid, int modeFlags) {
            return checkCallingUriPermission(uri, modeFlags);
        }

        @Override
        public int checkCallingUriPermission(Uri uri, int modeFlags) {
            if (mGrantedUriPermissions.contains(uri)) {
                return PackageManager.PERMISSION_GRANTED;
            } else {
                return PackageManager.PERMISSION_DENIED;
            }
        }

        @Override
        public int checkCallingOrSelfPermission(String permission) {
            return checkCallingPermission(permission);
        }

        @Override
        public void enforcePermission(String permission, int pid, int uid, String message) {
            enforceCallingPermission(permission, message);
        }

        @Override
        public void enforceCallingPermission(String permission, String message) {
            if (!mGrantedPermissions.contains(permission)) {
                throw new SecurityException(message);
            }
        }

        @Override
        public void enforceCallingOrSelfPermission(String permission, String message) {
            enforceCallingPermission(permission, message);
        }

        @Override
        public void sendBroadcast(Intent intent) {
            mOverallContext.sendBroadcast(intent);
        }

        @Override
        public void sendBroadcast(Intent intent, String receiverPermission) {
            mOverallContext.sendBroadcast(intent, receiverPermission);
        }
    }

    static String sCallingPackage = null;

    void ensureCallingPackage() {
        sCallingPackage = this.packageName;
    }

    public long createRawContact(String name) {
        ensureCallingPackage();
        long rawContactId = createRawContact();
        createName(rawContactId, name);
        return rawContactId;
    }

    public long createRawContact() {
        ensureCallingPackage();
        final ContentValues values = new ContentValues();

        Uri rawContactUri = resolver.insert(RawContacts.CONTENT_URI, values);
        return ContentUris.parseId(rawContactUri);
    }

    public long createRawContactWithStatus(String name, String address,
            String status) {
        final long rawContactId = createRawContact(name);
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

    public void setAccounts(Account[] accounts) {
        mAccounts = accounts;
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

    public void shutdown() {
        for (ContentProvider provider : mAllProviders) {
            provider.shutdown();
        }
    }
}
