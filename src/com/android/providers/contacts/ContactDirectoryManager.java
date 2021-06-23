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
 * limitations under the License
 */

package com.android.providers.contacts;

import android.annotation.NonNull;
import android.content.ContentValues;
import android.content.Context;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.pm.ProviderInfo;
import android.content.res.Resources;
import android.content.res.Resources.NotFoundException;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.Bundle;
import android.os.SystemClock;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Directory;
import android.sysprop.ContactsProperties;
import android.text.TextUtils;
import android.util.Log;

import com.android.providers.contacts.ContactsDatabaseHelper.DbProperties;
import com.android.providers.contacts.ContactsDatabaseHelper.DirectoryColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.google.android.collect.Lists;
import com.google.android.collect.Sets;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Manages the contents of the {@link Directory} table.
 */
public class ContactDirectoryManager {

    private static final String TAG = "ContactDirectoryManager";
    private static final boolean DEBUG = AbstractContactsProvider.VERBOSE_LOGGING;

    public static final String CONTACT_DIRECTORY_META_DATA = "android.content.ContactDirectory";

    public static class DirectoryInfo {
        long id;
        String packageName;
        String authority;
        String accountName;
        String accountType;
        String displayName;
        int typeResourceId;
        int exportSupport = Directory.EXPORT_SUPPORT_NONE;
        int shortcutSupport = Directory.SHORTCUT_SUPPORT_NONE;
        int photoSupport = Directory.PHOTO_SUPPORT_NONE;
        @Override
        public String toString() {
            return "DirectoryInfo:"
                    + "id=" + id
                    + " packageName=" + accountType
                    + " authority=" + authority
                    + " accountName=***"
                    + " accountType=" + accountType;
        }
    }

    private final static class DirectoryQuery {
        public static final String[] PROJECTION = {
            Directory.ACCOUNT_NAME,
            Directory.ACCOUNT_TYPE,
            Directory.DISPLAY_NAME,
            Directory.TYPE_RESOURCE_ID,
            Directory.EXPORT_SUPPORT,
            Directory.SHORTCUT_SUPPORT,
            Directory.PHOTO_SUPPORT,
        };

        public static final int ACCOUNT_NAME = 0;
        public static final int ACCOUNT_TYPE = 1;
        public static final int DISPLAY_NAME = 2;
        public static final int TYPE_RESOURCE_ID = 3;
        public static final int EXPORT_SUPPORT = 4;
        public static final int SHORTCUT_SUPPORT = 5;
        public static final int PHOTO_SUPPORT = 6;
    }

    private final ContactsProvider2 mContactsProvider;
    private final Context mContext;
    private final PackageManager mPackageManager;

    private volatile boolean mDirectoriesForceUpdated = false;

    public ContactDirectoryManager(ContactsProvider2 contactsProvider) {
        mContactsProvider = contactsProvider;
        mContext = contactsProvider.getContext();
        mPackageManager = mContext.getPackageManager();
    }

    public ContactsDatabaseHelper getDbHelper() {
        return (ContactsDatabaseHelper) mContactsProvider.getDatabaseHelper();
    }

    public void setDirectoriesForceUpdated(boolean updated) {
        mDirectoriesForceUpdated = updated;
    }

    /**
     * Scans through existing directories to see if the cached resource IDs still
     * match their original resource names.  If not - plays it safe by refreshing all directories.
     *
     * @return true if all resource IDs were found valid
     */
    private boolean areTypeResourceIdsValid() {
        SQLiteDatabase db = getDbHelper().getReadableDatabase();

        final Cursor cursor = db.rawQuery("SELECT DISTINCT "
                + Directory.TYPE_RESOURCE_ID + ","
                + Directory.PACKAGE_NAME + ","
                + DirectoryColumns.TYPE_RESOURCE_NAME
                + " FROM " + Tables.DIRECTORIES, null);
        try {
            while (cursor.moveToNext()) {
                int resourceId = cursor.getInt(0);
                if (resourceId != 0) {
                    String packageName = cursor.getString(1);
                    String storedResourceName = cursor.getString(2);
                    String resourceName = getResourceNameById(packageName, resourceId);
                    if (!TextUtils.equals(storedResourceName, resourceName)) {
                        if (DEBUG) {
                            Log.d(TAG, "areTypeResourceIdsValid:"
                                    + " resourceId=" + resourceId
                                    + " packageName=" + packageName
                                    + " storedResourceName=" + storedResourceName
                                    + " resourceName=" + resourceName);
                        }
                        return false;
                    }
                }
            }
        } finally {
            cursor.close();
        }

        return true;
    }

    /**
     * Given a resource ID, returns the corresponding resource name or null if the package name /
     * resource ID combination is invalid.
     */
    private String getResourceNameById(String packageName, int resourceId) {
        try {
            Resources resources = mPackageManager.getResourcesForApplication(packageName);
            return resources.getResourceName(resourceId);
        } catch (NameNotFoundException e) {
            return null;
        } catch (NotFoundException e) {
            return null;
        }
    }

    private void saveKnownDirectoryProviders(Set<String> packages) {
        getDbHelper().setProperty(DbProperties.KNOWN_DIRECTORY_PACKAGES,
                TextUtils.join(",", packages));
    }

    private boolean haveKnownDirectoryProvidersChanged(Set<String> packages) {
        final String directoryPackages = TextUtils.join(",", packages);
        final String prev = getDbHelper().getProperty(DbProperties.KNOWN_DIRECTORY_PACKAGES, "");

        final boolean changed = !Objects.equals(directoryPackages, prev);
        if (DEBUG) {
            Log.d(TAG, "haveKnownDirectoryProvidersChanged=" + changed + "\nprev=" + prev
                    + " current=" + directoryPackages);
        }
        return changed;
    }

    @VisibleForTesting
    boolean isRescanNeeded() {
        if (ContactsProperties.debug_scan_all_packages().orElse(false)) {
            Log.w(TAG, "debug.cp2.scan_all_packages set to 1.");
            return true; // For debugging.
        }
        final String scanComplete =
                getDbHelper().getProperty(DbProperties.DIRECTORY_SCAN_COMPLETE, "0");
        if (!"1".equals(scanComplete)) {
            if (DEBUG) {
                Log.d(TAG, "DIRECTORY_SCAN_COMPLETE is 0.");
            }
            return true;
        }
        if (haveKnownDirectoryProvidersChanged(getDirectoryProviderPackages(mPackageManager))) {
            Log.i(TAG, "Directory provider packages have changed.");
            return true;
        }
        return false;
    }

    /**
     * Scans all packages for directory content providers.
     */
    public int scanAllPackages(boolean rescan) {
        if (!areTypeResourceIdsValid()) {
            rescan = true;
            Log.i(TAG, "!areTypeResourceIdsValid.");
        }
        if (rescan) {
            getDbHelper().forceDirectoryRescan();
        }

        return scanAllPackagesIfNeeded();
    }

    private int scanAllPackagesIfNeeded() {
        if (!isRescanNeeded()) {
            return 0;
        }
        if (DEBUG) {
            Log.d(TAG, "scanAllPackagesIfNeeded()");
        }
        final long start = SystemClock.elapsedRealtime();
        // Reset directory updated flag to false. If it's changed to true
        // then we need to rescan directories.
        mDirectoriesForceUpdated = false;
        final int count = scanAllPackages();
        getDbHelper().setProperty(DbProperties.DIRECTORY_SCAN_COMPLETE, "1");
        final long end = SystemClock.elapsedRealtime();
        Log.i(TAG, "Discovered " + count + " contact directories in " + (end - start) + "ms");

        // Announce the change to listeners of the contacts authority
        mContactsProvider.notifyChange(/* syncToNetwork =*/false);

        // We schedule a rescan if update(DIRECTORIES) is called while we're scanning all packages.
        if (mDirectoriesForceUpdated) {
            mDirectoriesForceUpdated = false;
            mContactsProvider.scheduleRescanDirectories();
        }

        return count;
    }

    @VisibleForTesting
    static boolean isDirectoryProvider(ProviderInfo provider) {
        if (provider == null) return false;
        Bundle metaData = provider.metaData;
        if (metaData == null) return false;

        Object trueFalse = metaData.get(CONTACT_DIRECTORY_META_DATA);
        return trueFalse != null && Boolean.TRUE.equals(trueFalse);
    }

    @NonNull
    static private List<ProviderInfo> getDirectoryProviderInfos(PackageManager pm) {
        return pm.queryContentProviders(null, 0, 0, CONTACT_DIRECTORY_META_DATA);
    }

    /**
     * @return List of packages that contain a directory provider.
     */
    @VisibleForTesting
    @NonNull
    static Set<String> getDirectoryProviderPackages(PackageManager pm) {
        final Set<String> ret = Sets.newHashSet();

        if (DEBUG) {
            Log.d(TAG, "Listing directory provider packages...");
        }

        for (ProviderInfo provider : getDirectoryProviderInfos(pm)) {
            ret.add(provider.packageName);
        }
        if (DEBUG) {
            Log.d(TAG, "Found " + ret.size() + " directory provider packages");
        }

        return ret;
    }

    private int scanAllPackages() {
        SQLiteDatabase db = getDbHelper().getWritableDatabase();
        insertDefaultDirectory(db);
        insertLocalInvisibleDirectory(db);

        int count = 0;

        // Prepare query strings for removing stale rows which don't correspond to existing
        // directories.
        StringBuilder deleteWhereBuilder = new StringBuilder();
        ArrayList<String> deleteWhereArgs = new ArrayList<String>();
        deleteWhereBuilder.append("NOT (" + Directory._ID + "=? OR " + Directory._ID + "=?");
        deleteWhereArgs.add(String.valueOf(Directory.DEFAULT));
        deleteWhereArgs.add(String.valueOf(Directory.LOCAL_INVISIBLE));
        final String wherePart = "(" + Directory.PACKAGE_NAME + "=? AND "
                + Directory.DIRECTORY_AUTHORITY + "=? AND "
                + Directory.ACCOUNT_NAME + "=? AND "
                + Directory.ACCOUNT_TYPE + "=?)";

        final Set<String> directoryProviderPackages = getDirectoryProviderPackages(mPackageManager);
        for (String packageName : directoryProviderPackages) {
            if (DEBUG) Log.d(TAG, "package=" + packageName);

            // getDirectoryProviderPackages() shouldn't return the contacts provider package
            // because it doesn't have CONTACT_DIRECTORY_META_DATA, but just to make sure...
            if (mContext.getPackageName().equals(packageName)) {
                Log.w(TAG, "  skipping self");
                continue;
            }

            final PackageInfo packageInfo;
            try {
                packageInfo = mPackageManager.getPackageInfo(packageName,
                        PackageManager.GET_PROVIDERS | PackageManager.GET_META_DATA);
                if (packageInfo == null) continue;  // Just in case...
            } catch (NameNotFoundException nnfe) {
                continue; // Application just removed?
            }

            List<DirectoryInfo> directories = updateDirectoriesForPackage(packageInfo, true);
            if (directories != null && !directories.isEmpty()) {
                count += directories.size();

                // We shouldn't delete rows for existing directories.
                for (DirectoryInfo info : directories) {
                    if (DEBUG) Log.d(TAG, "  directory=" + info);
                    deleteWhereBuilder.append(" OR ");
                    deleteWhereBuilder.append(wherePart);
                    deleteWhereArgs.add(info.packageName);
                    deleteWhereArgs.add(info.authority);
                    deleteWhereArgs.add(info.accountName);
                    deleteWhereArgs.add(info.accountType);
                }
            }
        }

        deleteWhereBuilder.append(")");  // Close "NOT ("

        int deletedRows = db.delete(Tables.DIRECTORIES, deleteWhereBuilder.toString(),
                deleteWhereArgs.toArray(new String[0]));

        saveKnownDirectoryProviders(directoryProviderPackages);

        Log.i(TAG, "deleted " + deletedRows
                + " stale rows which don't have any relevant directory");
        return count;
    }

    private void insertDefaultDirectory(SQLiteDatabase db) {
        ContentValues values = new ContentValues();
        values.put(Directory._ID, Directory.DEFAULT);
        values.put(Directory.PACKAGE_NAME, mContext.getPackageName());
        values.put(Directory.DIRECTORY_AUTHORITY, ContactsContract.AUTHORITY);
        values.put(Directory.TYPE_RESOURCE_ID, R.string.default_directory);
        values.put(DirectoryColumns.TYPE_RESOURCE_NAME,
                mContext.getResources().getResourceName(R.string.default_directory));
        values.put(Directory.EXPORT_SUPPORT, Directory.EXPORT_SUPPORT_NONE);
        values.put(Directory.SHORTCUT_SUPPORT, Directory.SHORTCUT_SUPPORT_FULL);
        values.put(Directory.PHOTO_SUPPORT, Directory.PHOTO_SUPPORT_FULL);
        db.replace(Tables.DIRECTORIES, null, values);
    }

    private void insertLocalInvisibleDirectory(SQLiteDatabase db) {
        ContentValues values = new ContentValues();
        values.put(Directory._ID, Directory.LOCAL_INVISIBLE);
        values.put(Directory.PACKAGE_NAME, mContext.getPackageName());
        values.put(Directory.DIRECTORY_AUTHORITY, ContactsContract.AUTHORITY);
        values.put(Directory.TYPE_RESOURCE_ID, R.string.local_invisible_directory);
        values.put(DirectoryColumns.TYPE_RESOURCE_NAME,
                mContext.getResources().getResourceName(R.string.local_invisible_directory));
        values.put(Directory.EXPORT_SUPPORT, Directory.EXPORT_SUPPORT_NONE);
        values.put(Directory.SHORTCUT_SUPPORT, Directory.SHORTCUT_SUPPORT_FULL);
        values.put(Directory.PHOTO_SUPPORT, Directory.PHOTO_SUPPORT_FULL);
        db.replace(Tables.DIRECTORIES, null, values);
    }

    /**
     * Scans the specified package for content directories.  The package may have
     * already been removed, so packageName does not necessarily correspond to
     * an installed package.
     */
    public void onPackageChanged(String packageName) {
        PackageInfo packageInfo = null;

        try {
            packageInfo = mPackageManager.getPackageInfo(packageName,
                    PackageManager.GET_PROVIDERS | PackageManager.GET_META_DATA);
        } catch (NameNotFoundException e) {
            // The package got removed
            packageInfo = new PackageInfo();
            packageInfo.packageName = packageName;
        }

        if (mContext.getPackageName().equals(packageInfo.packageName)) {
            if (DEBUG) Log.d(TAG, "Ignoring onPackageChanged for self");
            return;
        }
        updateDirectoriesForPackage(packageInfo, false);
    }


    /**
     * Scans the specified package for content directories and updates the {@link Directory}
     * table accordingly.
     */
    private List<DirectoryInfo> updateDirectoriesForPackage(
            PackageInfo packageInfo, boolean initialScan) {
        if (DEBUG) {
            Log.d(TAG, "updateDirectoriesForPackage  packageName=" + packageInfo.packageName
                    + " initialScan=" + initialScan);
        }

        ArrayList<DirectoryInfo> directories = Lists.newArrayList();

        ProviderInfo[] providers = packageInfo.providers;
        if (providers != null) {
            for (ProviderInfo provider : providers) {
                if (isDirectoryProvider(provider)) {
                    queryDirectoriesForAuthority(directories, provider);
                }
            }
        }

        if (directories.size() == 0 && initialScan) {
            return null;
        }

        SQLiteDatabase db = getDbHelper().getWritableDatabase();
        db.beginTransaction();
        try {
            updateDirectories(db, directories);
            // Clear out directories that are no longer present
            StringBuilder sb = new StringBuilder(Directory.PACKAGE_NAME + "=?");
            if (!directories.isEmpty()) {
                sb.append(" AND " + Directory._ID + " NOT IN(");
                for (DirectoryInfo info: directories) {
                    sb.append(info.id).append(",");
                }
                sb.setLength(sb.length() - 1);  // Remove the extra comma
                sb.append(")");
            }
            final int numDeleted = db.delete(Tables.DIRECTORIES, sb.toString(),
                    new String[] { packageInfo.packageName });
            if (DEBUG) {
                Log.d(TAG, "  deleted " + numDeleted + " stale rows");
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }

        mContactsProvider.resetDirectoryCache();
        return directories;
    }

    /**
     * Sends a {@link Directory#CONTENT_URI} request to a specific contact directory
     * provider and appends all discovered directories to the directoryInfo list.
     */
    protected void queryDirectoriesForAuthority(
            ArrayList<DirectoryInfo> directoryInfo, ProviderInfo provider) {
        Uri uri = new Uri.Builder().scheme("content")
                .authority(provider.authority).appendPath("directories").build();
        Cursor cursor = null;
        try {
            cursor = mContext.getContentResolver().query(
                    uri, DirectoryQuery.PROJECTION, null, null, null);
            if (cursor == null) {
                Log.i(TAG, providerDescription(provider) + " returned a NULL cursor.");
            } else {
                while (cursor.moveToNext()) {
                    DirectoryInfo info = new DirectoryInfo();
                    info.packageName = provider.packageName;
                    info.authority = provider.authority;
                    info.accountName = cursor.getString(DirectoryQuery.ACCOUNT_NAME);
                    info.accountType = cursor.getString(DirectoryQuery.ACCOUNT_TYPE);
                    info.displayName = cursor.getString(DirectoryQuery.DISPLAY_NAME);
                    if (!cursor.isNull(DirectoryQuery.TYPE_RESOURCE_ID)) {
                        info.typeResourceId = cursor.getInt(DirectoryQuery.TYPE_RESOURCE_ID);
                    }
                    if (!cursor.isNull(DirectoryQuery.EXPORT_SUPPORT)) {
                        int exportSupport = cursor.getInt(DirectoryQuery.EXPORT_SUPPORT);
                        switch (exportSupport) {
                            case Directory.EXPORT_SUPPORT_NONE:
                            case Directory.EXPORT_SUPPORT_SAME_ACCOUNT_ONLY:
                            case Directory.EXPORT_SUPPORT_ANY_ACCOUNT:
                                info.exportSupport = exportSupport;
                                break;
                            default:
                                Log.e(TAG, providerDescription(provider)
                                        + " - invalid export support flag: " + exportSupport);
                        }
                    }
                    if (!cursor.isNull(DirectoryQuery.SHORTCUT_SUPPORT)) {
                        int shortcutSupport = cursor.getInt(DirectoryQuery.SHORTCUT_SUPPORT);
                        switch (shortcutSupport) {
                            case Directory.SHORTCUT_SUPPORT_NONE:
                            case Directory.SHORTCUT_SUPPORT_DATA_ITEMS_ONLY:
                            case Directory.SHORTCUT_SUPPORT_FULL:
                                info.shortcutSupport = shortcutSupport;
                                break;
                            default:
                                Log.e(TAG, providerDescription(provider)
                                        + " - invalid shortcut support flag: " + shortcutSupport);
                        }
                    }
                    if (!cursor.isNull(DirectoryQuery.PHOTO_SUPPORT)) {
                        int photoSupport = cursor.getInt(DirectoryQuery.PHOTO_SUPPORT);
                        switch (photoSupport) {
                            case Directory.PHOTO_SUPPORT_NONE:
                            case Directory.PHOTO_SUPPORT_THUMBNAIL_ONLY:
                            case Directory.PHOTO_SUPPORT_FULL_SIZE_ONLY:
                            case Directory.PHOTO_SUPPORT_FULL:
                                info.photoSupport = photoSupport;
                                break;
                            default:
                                Log.e(TAG, providerDescription(provider)
                                        + " - invalid photo support flag: " + photoSupport);
                        }
                    }
                    directoryInfo.add(info);
                }
            }
        } catch (Throwable t) {
            Log.e(TAG, providerDescription(provider) + " exception", t);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Updates the directories tables in the database to match the info received
     * from directory providers.
     */
    private void updateDirectories(SQLiteDatabase db, ArrayList<DirectoryInfo> directoryInfo) {
        // Insert or replace existing directories.
        // This happens so infrequently that we can use a less-then-optimal one-a-time approach
        for (DirectoryInfo info : directoryInfo) {
            ContentValues values = new ContentValues();
            values.put(Directory.PACKAGE_NAME, info.packageName);
            values.put(Directory.DIRECTORY_AUTHORITY, info.authority);
            values.put(Directory.ACCOUNT_NAME, info.accountName);
            values.put(Directory.ACCOUNT_TYPE, info.accountType);
            values.put(Directory.TYPE_RESOURCE_ID, info.typeResourceId);
            values.put(Directory.DISPLAY_NAME, info.displayName);
            values.put(Directory.EXPORT_SUPPORT, info.exportSupport);
            values.put(Directory.SHORTCUT_SUPPORT, info.shortcutSupport);
            values.put(Directory.PHOTO_SUPPORT, info.photoSupport);

            if (info.typeResourceId != 0) {
                String resourceName = getResourceNameById(info.packageName, info.typeResourceId);
                values.put(DirectoryColumns.TYPE_RESOURCE_NAME, resourceName);
            }

            Cursor cursor = db.query(Tables.DIRECTORIES, new String[] { Directory._ID },
                    Directory.PACKAGE_NAME + "=? AND " + Directory.DIRECTORY_AUTHORITY + "=? AND "
                            + Directory.ACCOUNT_NAME + "=? AND " + Directory.ACCOUNT_TYPE + "=?",
                    new String[] {
                            info.packageName, info.authority, info.accountName, info.accountType },
                    null, null, null);
            try {
                long id;
                if (cursor.moveToFirst()) {
                    id = cursor.getLong(0);
                    db.update(Tables.DIRECTORIES, values, Directory._ID + "=?",
                            new String[] { String.valueOf(id) });
                } else {
                    id = db.insert(Tables.DIRECTORIES, null, values);
                }
                info.id = id;
            } finally {
                cursor.close();
            }
        }
    }

    protected String providerDescription(ProviderInfo provider) {
        return "Directory provider " + provider.packageName + "(" + provider.authority + ")";
    }
}
