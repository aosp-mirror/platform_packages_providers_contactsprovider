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

import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.util.SelectionBuilder;
import com.android.providers.contacts.util.TypedUriMatcherImpl;

import android.content.ComponentName;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.content.pm.ResolveInfo;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.os.ParcelFileDescriptor;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Voicemails;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of the Voicemail content provider.
 */
public class VoicemailContentProvider extends ContentProvider
        implements VoicemailTable.DelegateHelper {
    private static final String TAG = "VoicemailContentProvider";
    private static final String VOICEMAILS_TABLE_NAME = Tables.CALLS;

    private ContentResolver mContentResolver;
    private VoicemailPermissions mVoicemailPermissions;
    private VoicemailTable.Delegate mVoicemailContentTable;

    @Override
    public boolean onCreate() {
        Context context = context();
        mContentResolver = context.getContentResolver();
        mVoicemailPermissions = new VoicemailPermissions(context);
        mVoicemailContentTable = new VoicemailContentTable(VOICEMAILS_TABLE_NAME, context,
                getDatabaseHelper(context), this);
        return true;
    }

    /*package for testing*/ ContactsDatabaseHelper getDatabaseHelper(Context context) {
        return ContactsDatabaseHelper.getInstance(context);
    }

    /*package for testing*/ Context context() {
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
        return mVoicemailContentTable.getType(uriData);
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] valuesArray) {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        return mVoicemailContentTable.bulkInsert(uriData, valuesArray);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        return mVoicemailContentTable.insert(uriData, values);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause());
        return mVoicemailContentTable.query(uriData, projection, selectionBuilder.build(),
                selectionArgs, sortOrder);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause());
        return mVoicemailContentTable.update(uriData, values, selectionBuilder.build(),
                selectionArgs);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        selectionBuilder.addClause(getPackageRestrictionClause());
        return mVoicemailContentTable.delete(uriData, selectionBuilder.build(), selectionArgs);
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        UriData uriData = checkPermissionsAndCreateUriData(uri);
        // openFileHelper() relies on "_data" column to be populated with the file path.
        return mVoicemailContentTable.openFile(uriData, mode, openFileHelper(uri, mode));
    }

    /**
     * Decorates a URI by providing methods to get various properties from the URI.
     */
    public static class UriData {
        private final Uri mUri;
        private final String mId;
        private final String mSourcePackage;

        public UriData(Uri uri, String id, String sourcePackage) {
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

        /** Create a {@link UriData} corresponding to a given uri. */
        public static UriData createUriData(Uri uri) {
            String sourcePackage = uri.getQueryParameter(
                    VoicemailContract.PARAM_KEY_SOURCE_PACKAGE);
            List<String> segments = uri.getPathSegments();
            switch (createUriMatcher().match(uri)) {
                case VOICEMAILS:
                    return new UriData(uri, null, sourcePackage);
                case VOICEMAILS_ID:
                    return new UriData(uri, segments.get(1), sourcePackage);
                case NO_MATCH:
                    throw new IllegalArgumentException("Invalid URI: " + uri);
                default:
                    throw new IllegalStateException("Impossible, all cases are covered");
            }
        }
    }

    @Override
    // VoicemailTable.DelegateHelper interface.
    public void notifyChange(Uri notificationUri, String... intentActions) {
        // Notify the observers.
        mContentResolver.notifyChange(notificationUri, null, true);
        String callingPackage = getCallingPackage();
        // Fire notification intents.
        for (String intentAction : intentActions) {
            // TODO: We can possibly be more intelligent here and send targeted intents based on
            // what voicemail permission the package has. If possible, here is what we would like to
            // do for a given broadcast intent -
            // 1) Send it to all packages that have READ_WRITE_ALL_VOICEMAIL permission.
            // 2) Send it to only the owner package that has just READ_WRITE_OWN_VOICEMAIL, if not
            // already sent in (1).
            for (ComponentName component :
                    getBroadcastReceiverComponents(intentAction, notificationUri)) {
                Intent intent = new Intent(intentAction, notificationUri);
                intent.setComponent(component);
                intent.putExtra(VoicemailContract.EXTRA_SELF_CHANGE,
                        callingPackage.equals(component.getPackageName()));
                context().sendBroadcast(intent, Manifest.permission.READ_WRITE_OWN_VOICEMAIL);
            }
        }
    }

    @Override
    // VoicemailTable.DelegateHelper interface.
    public void checkAndAddSourcePackageIntoValues(UriData uriData, ContentValues values) {
        // If content values don't contain the provider, calculate the right provider to use.
        if (!values.containsKey(VoicemailContract.SOURCE_PACKAGE_FIELD)) {
            String provider = uriData.hasSourcePackage() ?
                    uriData.getSourcePackage() : getCallingPackage();
            values.put(VoicemailContract.SOURCE_PACKAGE_FIELD, provider);
        }
        // If you put a provider in the URI and in the values, they must match.
        if (uriData.hasSourcePackage() &&
                values.containsKey(VoicemailContract.SOURCE_PACKAGE_FIELD)) {
            if (!uriData.getSourcePackage().equals(
                    values.get(VoicemailContract.SOURCE_PACKAGE_FIELD))) {
                throw new SecurityException(
                        "Provider in URI was " + uriData.getSourcePackage() +
                        " but doesn't match provider in ContentValues which was "
                        + values.get(VoicemailContract.SOURCE_PACKAGE_FIELD));
            }
        }
        // You must have access to the provider given in values.
        if (!mVoicemailPermissions.callerHasFullAccess()) {
            checkPackagesMatch(getCallingPackage(),
                    values.getAsString(VoicemailContract.SOURCE_PACKAGE_FIELD),
                    uriData.getUri());
        }
    }

    private static TypedUriMatcherImpl<VoicemailUriType> createUriMatcher() {
        return new TypedUriMatcherImpl<VoicemailUriType>(
                VoicemailContract.AUTHORITY, VoicemailUriType.values());
    }

    /**
     * Performs necessary voicemail permission checks common to all operations and returns
     * the structured representation, {@link UriData}, of the supplied uri.
     */
    private UriData checkPermissionsAndCreateUriData(Uri uri) {
        mVoicemailPermissions.checkCallerHasOwnVoicemailAccess();
        UriData uriData = UriData.createUriData(uri);
        checkPackagePermission(uriData);
        return uriData;
    }

    /**
     * Checks that the callingProvider is same as voicemailProvider. Throws {@link
     * SecurityException} if they don't match.
     */
    private final void checkPackagesMatch(String callingProvider, String voicemailProvider,
            Uri uri) {
        if (!voicemailProvider.equals(callingProvider)) {
            String errorMsg = String.format("Permission denied for URI: %s\n. " +
                    "Provider %s cannot perform this operation for %s. Requires %s permission.",
                    uri, callingProvider, voicemailProvider,
                    Manifest.permission.READ_WRITE_ALL_VOICEMAIL);
            throw new SecurityException(errorMsg);
        }
    }

    /**
     * Checks that either the caller has READ_WRITE_ALL_VOICEMAIL permission, or has the
     * READ_WRITE_OWN_VOICEMAIL permission and is using a URI that matches
     * /voicemail/source/[source-package] where [source-package] is the same as the calling
     * package.
     *
     * @throws SecurityException if the check fails.
     */
    private void checkPackagePermission(UriData uriData) {
        if (!mVoicemailPermissions.callerHasFullAccess()) {
            if (!uriData.hasSourcePackage()) {
                // You cannot have a match if this is not a provider uri.
                throw new SecurityException(String.format(
                        "Provider %s does not have %s permission." +
                                "\nPlease set query parameter '%s' in the URI.\nURI: %s",
                        getCallingPackage(), Manifest.permission.READ_WRITE_ALL_VOICEMAIL,
                        VoicemailContract.PARAM_KEY_SOURCE_PACKAGE, uriData.getUri()));
            }
            checkPackagesMatch(getCallingPackage(), uriData.getSourcePackage(), uriData.getUri());
        }
    }

    /**
     * Gets the name of the calling package.
     * <p>
     * It's possible (though unlikely) for there to be more than one calling package (requires that
     * your manifest say you want to share process ids) in which case we will return an arbitrary
     * package name. It's also possible (though very unlikely) for us to be unable to work out what
     * your calling package is, in which case we will return null.
     */
    /* package for test */String getCallingPackage() {
        int caller = Binder.getCallingUid();
        if (caller == 0) {
            return null;
        }
        String[] callerPackages = context().getPackageManager().getPackagesForUid(caller);
        if (callerPackages == null || callerPackages.length == 0) {
            return null;
        }
        if (callerPackages.length == 1) {
            return callerPackages[0];
        }
        // If we have more than one caller package, which is very unlikely, let's return the one
        // with the highest permissions. If more than one has the same permission, we don't care
        // which one we return.
        String bestSoFar = callerPackages[0];
        for (String callerPackage : callerPackages) {
            if (mVoicemailPermissions.packageHasFullAccess(callerPackage)) {
                // Full always wins, we can return early.
                return callerPackage;
            }
            if (mVoicemailPermissions.packageHasOwnVoicemailAccess(callerPackage)) {
                bestSoFar = callerPackage;
            }
        }
        return bestSoFar;
    }

    /**
     * Creates a clause to restrict the selection to the calling provider or null if the caller has
     * access to all data.
     */
    private String getPackageRestrictionClause() {
        if (mVoicemailPermissions.callerHasFullAccess()) {
            return null;
        }
        return getEqualityClause(Voicemails.SOURCE_PACKAGE, getCallingPackage());
    }

    /** Determines the components that can possibly receive the specified intent. */
    protected List<ComponentName> getBroadcastReceiverComponents(String intentAction, Uri uri) {
        Intent intent = new Intent(intentAction, uri);
        List<ComponentName> receiverComponents = new ArrayList<ComponentName>();
        // For broadcast receivers ResolveInfo.activityInfo is the one that is populated.
        for (ResolveInfo resolveInfo :
                context().getPackageManager().queryBroadcastReceivers(intent, 0)) {
            ActivityInfo activityInfo = resolveInfo.activityInfo;
            receiverComponents.add(new ComponentName(activityInfo.packageName, activityInfo.name));
        }
        return receiverComponents;
    }
}
