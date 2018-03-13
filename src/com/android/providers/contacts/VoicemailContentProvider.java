/*
 * Copyright (C) 2011 The Android Open Source Project
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

import static android.app.AppOpsManager.MODE_ALLOWED;
import static android.provider.VoicemailContract.SOURCE_PACKAGE_FIELD;

import static com.android.providers.contacts.util.DbQueryUtils.concatenateClauses;
import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

import android.annotation.NonNull;
import android.app.AppOpsManager;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.os.IBinder;
import android.os.ParcelFileDescriptor;
import android.provider.BaseColumns;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.util.ArraySet;
import android.util.Log;

import com.android.providers.contacts.CallLogDatabaseHelper.Tables;
import com.android.providers.contacts.util.ContactsPermissions;
import com.android.providers.contacts.util.PackageUtils;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.TypedUriMatcherImpl;
import com.android.providers.contacts.util.UserUtils;

import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

/**
 * An implementation of the Voicemail content provider. This class in the entry point for both
 * voicemail content ('calls') table and 'voicemail_status' table. This class performs all common
 * permission checks and then delegates database level operations to respective table delegate
 * objects.
 */
public class VoicemailContentProvider extends ContentProvider
        implements VoicemailTable.DelegateHelper {
    private static final String TAG = "VoicemailProvider";

    public static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    private static final int BACKGROUND_TASK_SCAN_STALE_PACKAGES = 0;

    private ContactsTaskScheduler mTaskScheduler;

    private VoicemailPermissions mVoicemailPermissions;
    private VoicemailTable.Delegate mVoicemailContentTable;
    private VoicemailTable.Delegate mVoicemailStatusTable;

    @Override
    public boolean onCreate() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "onCreate: " + this.getClass().getSimpleName()
                    + " user=" + android.os.Process.myUserHandle().getIdentifier());
        }
        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.INFO)) {
            Log.i(Constants.PERFORMANCE_TAG, "VoicemailContentProvider.onCreate start");
        }
        Context context = context();

        // Read and write permission requires ADD_VOICEMAIL or carrier privileges. We can't declare
        // any permission entries in the manifest because carrier-privileged apps without
        // ADD_VOICEMAIL would be blocked by the platform without even reaching our custom
        // enforce{Read,Write}PermissionInner functions. These overrides are what allow carrier-
        // privileged apps to bypass these runtime-configured permissions.
        // TODO(b/74245334): See if these can be removed since individual operations perform their
        // own checks.
        setReadPermission(android.Manifest.permission.ADD_VOICEMAIL);
        setWritePermission(android.Manifest.permission.ADD_VOICEMAIL);
        setAppOps(AppOpsManager.OP_ADD_VOICEMAIL, AppOpsManager.OP_ADD_VOICEMAIL);

        mVoicemailPermissions = new VoicemailPermissions(context);
        mVoicemailContentTable = new VoicemailContentTable(Tables.CALLS, context,
                getDatabaseHelper(context), this, createCallLogInsertionHelper(context));
        mVoicemailStatusTable = new VoicemailStatusTable(Tables.VOICEMAIL_STATUS, context,
                getDatabaseHelper(context), this);

        mTaskScheduler = new ContactsTaskScheduler(getClass().getSimpleName()) {
            @Override
            public void onPerformTask(int taskId, Object arg) {
                performBackgroundTask(taskId, arg);
            }
        };

        scheduleScanStalePackages();

        ContactsPackageMonitor.start(getContext());

        if (Log.isLoggable(Constants.PERFORMANCE_TAG, Log.INFO)) {
            Log.i(Constants.PERFORMANCE_TAG, "VoicemailContentProvider.onCreate finish");
        }
        return true;
    }

    @Override
    protected int enforceReadPermissionInner(Uri uri, String callingPkg, IBinder callerToken)
            throws SecurityException {
        // Permit carrier-privileged apps regardless of ADD_VOICEMAIL permission state.
        if (mVoicemailPermissions.callerHasCarrierPrivileges()) {
            return MODE_ALLOWED;
        }
        return super.enforceReadPermissionInner(uri, callingPkg, callerToken);
    }


    @Override
    protected int enforceWritePermissionInner(Uri uri, String callingPkg, IBinder callerToken)
            throws SecurityException {
        // Permit carrier-privileged apps regardless of ADD_VOICEMAIL permission state.
        if (mVoicemailPermissions.callerHasCarrierPrivileges()) {
            return MODE_ALLOWED;
        }
        return super.enforceWritePermissionInner(uri, callingPkg, callerToken);
    }

    @VisibleForTesting
    void scheduleScanStalePackages() {
        scheduleTask(BACKGROUND_TASK_SCAN_STALE_PACKAGES, null);
    }

    @VisibleForTesting
    void scheduleTask(int taskId, Object arg) {
        mTaskScheduler.scheduleTask(taskId, arg);
    }

    @VisibleForTesting
    /*package*/ CallLogInsertionHelper createCallLogInsertionHelper(Context context) {
        return DefaultCallLogInsertionHelper.getInstance(context);
    }

    @VisibleForTesting
    /*package*/ CallLogDatabaseHelper getDatabaseHelper(Context context) {
        return CallLogDatabaseHelper.getInstance(context);
    }

    @VisibleForTesting
    /*package*/ Context context() {
        return getContext();
    }

    @Override
    public String getType(Uri uri) {
        UriData uriData = null;
        try {
            uriData = UriData.createUriData(uri);
        } catch (IllegalArgumentException ignored) {
            // Special case: for illegal URIs, we return null rather than thrown an exception.
            return null;
        }
        return getTableDelegate(uriData).getType(uriData);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insert: uri=" + uri + "  values=[" + values + "]" +
                    " CPID=" + Binder.getCallingPid());
        }
        UriData uriData = checkPermissionsAndCreateUriDataForWrite(uri, values);
        return getTableDelegate(uriData).insert(uriData, values);
    }

    @Override
    public int bulkInsert(@NonNull Uri uri, @NonNull ContentValues[] values) {
        UriData uriData = checkPermissionsAndCreateUriDataForWrite(uri, values);
        return getTableDelegate(uriData).bulkInsert(uriData, values);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: uri=" + uri + "  projection=" + Arrays.toString(projection) +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  order=[" + sortOrder + "] CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        UriData uriData = checkPermissionsAndCreateUriDataForRead(uri);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause(true/*isQuery*/));
        return getTableDelegate(uriData).query(uriData, projection, selectionBuilder.build(),
                selectionArgs, sortOrder);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "update: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    "  values=[" + values + "] CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        UriData uriData = checkPermissionsAndCreateUriDataForWrite(uri, values);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause(false/*isQuery*/));
        return getTableDelegate(uriData).update(uriData, values, selectionBuilder.build(),
                selectionArgs);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "delete: uri=" + uri +
                    "  selection=[" + selection + "]  args=" + Arrays.toString(selectionArgs) +
                    " CPID=" + Binder.getCallingPid() +
                    " User=" + UserUtils.getCurrentUserHandle(getContext()));
        }
        UriData uriData = checkPermissionsAndCreateUriDataForWrite(uri);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause(false/*isQuery*/));
        return getTableDelegate(uriData).delete(uriData, selectionBuilder.build(), selectionArgs);
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        boolean success = false;
        try {
            UriData uriData = null;
            if (mode.equals("r")) {
                uriData = checkPermissionsAndCreateUriDataForRead(uri);
            } else {
                uriData = checkPermissionsAndCreateUriDataForWrite(uri);
            }
            // openFileHelper() relies on "_data" column to be populated with the file path.
            final ParcelFileDescriptor ret = getTableDelegate(uriData).openFile(uriData, mode);
            success = true;
            return ret;
        } finally {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "openFile uri=" + uri + " mode=" + mode + " success=" + success +
                        " CPID=" + Binder.getCallingPid() +
                        " User=" + UserUtils.getCurrentUserHandle(getContext()));
            }
        }
    }

    /** Returns the correct table delegate object that can handle this URI. */
    private VoicemailTable.Delegate getTableDelegate(UriData uriData) {
        switch (uriData.getUriType()) {
            case STATUS:
            case STATUS_ID:
                return mVoicemailStatusTable;
            case VOICEMAILS:
            case VOICEMAILS_ID:
                return mVoicemailContentTable;
            case NO_MATCH:
                throw new IllegalStateException("Invalid uri type for uri: " + uriData.getUri());
            default:
                throw new IllegalStateException("Impossible, all cases are covered.");
        }
    }

    /**
     * Decorates a URI by providing methods to get various properties from the URI.
     */
    public static class UriData {
        private final Uri mUri;
        private final String mId;
        private final String mSourcePackage;
        private final VoicemailUriType mUriType;

        private UriData(Uri uri, VoicemailUriType uriType, String id, String sourcePackage) {
            mUriType = uriType;
            mUri = uri;
            mId = id;
            mSourcePackage = sourcePackage;
        }

        /** Gets the original URI to which this {@link UriData} corresponds. */
        public final Uri getUri() {
            return mUri;
        }

        /** Tells us if our URI has an individual voicemail id. */
        public final boolean hasId() {
            return mId != null;
        }

        /** Gets the ID for the voicemail. */
        public final String getId() {
            return mId;
        }

        /** Tells us if our URI has a source package string. */
        public final boolean hasSourcePackage() {
            return mSourcePackage != null;
        }

        /** Gets the source package. */
        public final String getSourcePackage() {
            return mSourcePackage;
        }

        /** Gets the Voicemail URI type. */
        public final VoicemailUriType getUriType() {
            return mUriType;
        }

        /** Builds a where clause from the URI data. */
        public final String getWhereClause() {
            return concatenateClauses(
                    (hasId() ? getEqualityClause(BaseColumns._ID, getId()) : null),
                    (hasSourcePackage() ? getEqualityClause(SOURCE_PACKAGE_FIELD,
                            getSourcePackage()) : null));
        }

        /** Create a {@link UriData} corresponding to a given uri. */
        public static UriData createUriData(Uri uri) {
            String sourcePackage = uri.getQueryParameter(
                    VoicemailContract.PARAM_KEY_SOURCE_PACKAGE);
            List<String> segments = uri.getPathSegments();
            VoicemailUriType uriType = createUriMatcher().match(uri);
            switch (uriType) {
                case VOICEMAILS:
                case STATUS:
                    return new UriData(uri, uriType, null, sourcePackage);
                case VOICEMAILS_ID:
                case STATUS_ID:
                    return new UriData(uri, uriType, segments.get(1), sourcePackage);
                case NO_MATCH:
                    throw new IllegalArgumentException("Invalid URI: " + uri);
                default:
                    throw new IllegalStateException("Impossible, all cases are covered");
            }
        }

        private static TypedUriMatcherImpl<VoicemailUriType> createUriMatcher() {
            return new TypedUriMatcherImpl<VoicemailUriType>(
                    VoicemailContract.AUTHORITY, VoicemailUriType.values());
        }
    }

    @Override
    // VoicemailTable.DelegateHelper interface.
    public void checkAndAddSourcePackageIntoValues(UriData uriData, ContentValues values) {
        // If content values don't contain the provider, calculate the right provider to use.
        if (!values.containsKey(SOURCE_PACKAGE_FIELD)) {
            String provider = uriData.hasSourcePackage() ?
                    uriData.getSourcePackage() : getInjectedCallingPackage();
            values.put(SOURCE_PACKAGE_FIELD, provider);
        }

        // You must have access to the provider given in values.
        if (!mVoicemailPermissions.callerHasWriteAccess(getCallingPackage())) {
            checkPackagesMatch(getInjectedCallingPackage(),
                    values.getAsString(VoicemailContract.SOURCE_PACKAGE_FIELD),
                    uriData.getUri());
        }
    }

    /**
     * Checks that the source_package field is same in uriData and ContentValues, if it happens
     * to be set in both.
     */
    private void checkSourcePackageSameIfSet(UriData uriData, ContentValues values) {
        if (uriData.hasSourcePackage() && values.containsKey(SOURCE_PACKAGE_FIELD)) {
            if (!uriData.getSourcePackage().equals(values.get(SOURCE_PACKAGE_FIELD))) {
                throw new SecurityException(
                        "source_package in URI was " + uriData.getSourcePackage() +
                        " but doesn't match source_package in ContentValues which was "
                        + values.get(SOURCE_PACKAGE_FIELD));
            }
        }
    }

    @Override
    /** Implementation of  {@link VoicemailTable.DelegateHelper#openDataFile(UriData, String)} */
    public ParcelFileDescriptor openDataFile(UriData uriData, String mode)
            throws FileNotFoundException {
        return openFileHelper(uriData.getUri(), mode);
    }

    /**
     * Ensures that the caller has the permissions to perform a query/read operation, and
     * then returns the structured representation {@link UriData} of the supplied uri.
     */
    private UriData checkPermissionsAndCreateUriDataForRead(Uri uri) {
        // If the caller has been explicitly granted read permission to this URI then no need to
        // check further.
        if (ContactsPermissions.hasCallerUriPermission(
                getContext(), uri, Intent.FLAG_GRANT_READ_URI_PERMISSION)) {
            return UriData.createUriData(uri);
        }

        if (mVoicemailPermissions.callerHasReadAccess(getCallingPackage())) {
            return UriData.createUriData(uri);
        }

        return checkPermissionsAndCreateUriData(uri, true);
    }

    /**
     * Performs necessary voicemail permission checks common to all operations and returns
     * the structured representation, {@link UriData}, of the supplied uri.
     */
    private UriData checkPermissionsAndCreateUriData(Uri uri, boolean read) {
        UriData uriData = UriData.createUriData(uri);
        if (!hasReadWritePermission(read)) {
            mVoicemailPermissions.checkCallerHasOwnVoicemailAccess();
            checkPackagePermission(uriData);
        }
        return uriData;
    }

    /**
     * Ensures that the caller has the permissions to perform an update/delete operation, and
     * then returns the structured representation {@link UriData} of the supplied uri.
     * Also does a permission check on the ContentValues.
     */
    private UriData checkPermissionsAndCreateUriDataForWrite(Uri uri, ContentValues... valuesArray) {
        UriData uriData = checkPermissionsAndCreateUriData(uri, false);
        for (ContentValues values : valuesArray) {
            checkSourcePackageSameIfSet(uriData, values);
        }
        return uriData;
    }

    /**
     * Checks that the callingPackage is same as voicemailSourcePackage. Throws {@link
     * SecurityException} if they don't match.
     */
    private final void checkPackagesMatch(String callingPackage, String voicemailSourcePackage,
            Uri uri) {
        if (!voicemailSourcePackage.equals(callingPackage)) {
            String errorMsg = String.format("Permission denied for URI: %s\n. " +
                    "Package %s cannot perform this operation for %s. Requires %s permission.",
                    uri, callingPackage, voicemailSourcePackage,
                    android.Manifest.permission.WRITE_VOICEMAIL);
            throw new SecurityException(errorMsg);
        }
    }

    /**
     * Checks that either the caller has the MANAGE_VOICEMAIL permission,
     * or has the ADD_VOICEMAIL permission and is using a URI that matches
     * /voicemail/?source_package=[source-package] where [source-package] is the same as the calling
     * package.
     *
     * @throws SecurityException if the check fails.
     */
    private void checkPackagePermission(UriData uriData) {
        if (!mVoicemailPermissions.callerHasWriteAccess(getCallingPackage())) {
            if (!uriData.hasSourcePackage()) {
                // You cannot have a match if this is not a provider URI.
                throw new SecurityException(String.format(
                        "Provider %s does not have %s permission." +
                                "\nPlease set query parameter '%s' in the URI.\nURI: %s",
                        getInjectedCallingPackage(), android.Manifest.permission.WRITE_VOICEMAIL,
                        VoicemailContract.PARAM_KEY_SOURCE_PACKAGE, uriData.getUri()));
            }
            checkPackagesMatch(getInjectedCallingPackage(), uriData.getSourcePackage(),
                    uriData.getUri());
        }
    }

    @VisibleForTesting
    String getInjectedCallingPackage() {
        return super.getCallingPackage();
    }

    /**
     * Creates a clause to restrict the selection to the calling provider or null if the caller has
     * access to all data.
     */
    private String getPackageRestrictionClause(boolean isQuery) {
        if (hasReadWritePermission(isQuery)) {
            return null;
        }
        return getEqualityClause(Voicemails.SOURCE_PACKAGE, getInjectedCallingPackage());
    }

    /**
     * Whether or not the calling package has the appropriate read/write permission. The user
     * selected default and/or system dialers are always allowed to read and write to the
     * VoicemailContentProvider.
     *
     * @param read Whether or not this operation is a read
     *
     * @return True if the package has the permission required to perform the read/write operation
     */
    private boolean hasReadWritePermission(boolean read) {
        return read ? mVoicemailPermissions.callerHasReadAccess(getCallingPackage()) :
            mVoicemailPermissions.callerHasWriteAccess(getCallingPackage());
    }

    /** Remove all records from a given source package. */
    public void removeBySourcePackage(String packageName) {
        delete(Voicemails.buildSourceUri(packageName), null, null);
        delete(Status.buildSourceUri(packageName), null, null);
    }

    @VisibleForTesting
    void performBackgroundTask(int task, Object arg) {
        switch (task) {
            case BACKGROUND_TASK_SCAN_STALE_PACKAGES:
                removeStalePackages();
                break;
        }
    }

    /**
     * Remove all records made by packages that no longer exist.
     */
    private void removeStalePackages() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "scanStalePackages start");
        }

        // Make sure all source tables still exists.

        // First, list all source packages.
        final ArraySet<String> packages = mVoicemailContentTable.getSourcePackages();
        packages.addAll(mVoicemailStatusTable.getSourcePackages());

        // Remove the ones that still exist.
        for (int i = packages.size() - 1; i >= 0; i--) {
            final String pkg = packages.valueAt(i);
            final boolean installed = PackageUtils.isPackageInstalled(getContext(), pkg);
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "  " + pkg + (installed ? " installed" : " removed"));
            }
            if (!installed) {
                removeBySourcePackage(pkg);
            }
        }

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "scanStalePackages finish");
        }
    }
}
