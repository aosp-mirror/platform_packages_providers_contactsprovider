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

import static com.android.providers.contacts.util.DbQueryUtils.concatenateClauses;
import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Binder;
import android.os.ParcelFileDescriptor;
import android.provider.CallLog.Calls;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Voicemails;
import android.util.Log;

import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.ContactsDatabaseHelper.Views;
import com.android.providers.contacts.util.CloseUtils;
import com.android.providers.contacts.util.TypedUriMatcherImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Restrict access to only voicemail columns (i.e no access to call_log
// specific fields)
// TODO: Port unit tests from perforce.
/**
 * An implementation of the Voicemail content provider.
 */
public class VoicemailContentProvider extends ContentProvider {
    private static final String TAG = "VoicemailContentProvider";

    /** The private directory in which to store the data associated with the voicemail. */
    private static final String DATA_DIRECTORY = "voicemail-data";

    private static final String[] MIME_TYPE_ONLY_PROJECTION = new String[] { Voicemails.MIME_TYPE };
    private static final String[] FILENAME_ONLY_PROJECTION = new String[] { Voicemails._DATA };
    private static final String VOICEMAILS_TABLE_NAME = Tables.CALLS;

    // Voicemail projection map
    private static final ProjectionMap sVoicemailProjectionMap = new ProjectionMap.Builder()
            .add(Voicemails._ID)
            .add(Voicemails.NUMBER)
            .add(Voicemails.DATE)
            .add(Voicemails.DURATION)
            .add(Voicemails.NEW)
            .add(Voicemails.STATE)
            .add(Voicemails.SOURCE_DATA)
            .add(Voicemails.SOURCE_PACKAGE)
            .add(Voicemails.HAS_CONTENT)
            .add(Voicemails.MIME_TYPE)
            .add(Voicemails._DATA)
            .build();
    private ContentResolver mContentResolver;
    private ContactsDatabaseHelper mDbHelper;

    @Override
    public boolean onCreate() {
        Context context = context();

        mContentResolver = context.getContentResolver();
        mDbHelper = ContactsDatabaseHelper.getInstance(context);

        return true;
    }

    /*package for testing*/ Context context() {
        return getContext();
    }

    @Override
    public String getType(Uri uri) {
        UriData uriData = null;
        try {
            uriData = createUriData(uri);
        } catch (IllegalArgumentException ignored) {
            // Special case: for illegal URIs, we return null rather than thrown an exception.
            return null;
        }
        // TODO: DB lookup for the mime type may cause strict mode exception for the callers of
        // getType(). See if this could be avoided.
        if (uriData.hasId()) {
            // An individual voicemail - so lookup the MIME type in the db.
            return lookupMimeType(uriData);
        }
        // Not an individual voicemail - must be a directory listing type.
        return VoicemailContract.DIR_TYPE;
    }

    /** Query the db for the MIME type of the given URI, called only from {@link #getType(Uri)}. */
    private String lookupMimeType(UriData uriData) {
        Cursor cursor = null;
        try {
            // Use queryInternal, bypassing provider permission check. This is needed because
            // getType() can be called from any application context (even without voicemail
            // permissions) to know the MIME type of the URI. There is no security issue here as we
            // do not expose any sensitive data through this interface.
            cursor = queryInternal(uriData, MIME_TYPE_ONLY_PROJECTION, null, null, null);
            if (cursor.moveToFirst()) {
                return cursor.getString(cursor.getColumnIndex(Voicemails.MIME_TYPE));
            }
        } finally {
            CloseUtils.closeQuietly(cursor);
        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        checkHasOwnPermission();
        UriData uriData = createUriData(uri);
        checkPackagePermission(uriData);
        return queryInternal(uriData, projection,
                concatenateClauses(selection, getPackageRestrictionClause()), selectionArgs,
                sortOrder);
    }

    /**
     * Internal version of query(), that does not apply any provider restriction and lets the query
     * flow through without such checks.
     * <p>
     * This is useful for internal queries when we do not worry about access permissions.
     */
    private Cursor queryInternal(UriData uriData, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Tables.CALLS);
        qb.setProjectionMap(sVoicemailProjectionMap);
        qb.setStrict(true);

        String combinedClause = concatenateClauses(selection, getWhereClause(uriData),
                getCallTypeClause());
        SQLiteDatabase db = mDbHelper.getReadableDatabase();
        Cursor c = qb.query(db, projection, combinedClause, selectionArgs, null, null, sortOrder);
        if (c != null) {
            c.setNotificationUri(mContentResolver, VoicemailContract.CONTENT_URI);
        }
        return c;
    }

    private String getWhereClause(UriData uriData) {
        return concatenateClauses(
                (uriData.hasId() ?
                        getEqualityClause(Voicemails._ID, uriData.getId())
                        : null),
                (uriData.hasSourcePackage() ?
                        getEqualityClause(Voicemails.SOURCE_PACKAGE, uriData.getSourcePackage())
                        : null));
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] valuesArray) {
        checkHasOwnPermission();
        // TODO: There is scope to optimize this method further. At the least we can avoid doing the
        // extra work related to the calling provider and checking permissions.
        UriData uriData = createUriData(uri);
        int numInserted = 0;
        for (ContentValues values : valuesArray) {
            if (insertInternal(uriData, values, false) != null) {
                numInserted++;
            }
        }
        if (numInserted > 0) {
            notifyChange(uri, Intent.ACTION_PROVIDER_CHANGED);
        }
        return numInserted;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        checkHasOwnPermission();
        return insertInternal(createUriData(uri), values, true);
    }

    private Uri insertInternal(UriData uriData, ContentValues values,
            boolean sendProviderChangedNotification) {
        checkInsertSupported(uriData);
        checkAndAddSourcePackageIntoValues(uriData, values);

        // "_data" column is used by base ContentProvider's openFileHelper() to determine filename
        // when Input/Output stream is requested to be opened.
        values.put(Voicemails._DATA, generateDataFile());

        // call type is always voicemail.
        values.put(Calls.TYPE, Calls.VOICEMAIL_TYPE);

        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        long rowId = db.insert(VOICEMAILS_TABLE_NAME, null, values);
        if (rowId > 0) {
            Uri newUri = ContentUris.withAppendedId(
                    Uri.withAppendedPath(VoicemailContract.CONTENT_URI_SOURCE,
                            values.getAsString(Voicemails.SOURCE_PACKAGE)), rowId);

            if (sendProviderChangedNotification) {
                notifyChange(newUri, VoicemailContract.ACTION_NEW_VOICEMAIL,
                        Intent.ACTION_PROVIDER_CHANGED);
            } else {
                notifyChange(newUri, VoicemailContract.ACTION_NEW_VOICEMAIL);
            }
            // Populate the 'voicemail_uri' field to be used by the call_log provider.
            updateVoicemailUri(newUri);
            return newUri;
        }
        return null;
    }

    private void updateVoicemailUri(Uri newUri) {
        ContentValues values = new ContentValues();
        values.put(Calls.VOICEMAIL_URI, newUri.toString());
        update(newUri, values, null, null);
    }

    private void checkAndAddSourcePackageIntoValues(UriData uriData, ContentValues values) {
        // If content values don't contain the provider, calculate the right provider to use.
        if (!values.containsKey(Voicemails.SOURCE_PACKAGE)) {
            String provider = uriData.hasSourcePackage() ?
                    uriData.getSourcePackage() : getCallingPackage();
            values.put(Voicemails.SOURCE_PACKAGE, provider);
        }
        // If you put a provider in the URI and in the values, they must match.
        if (uriData.hasSourcePackage() && values.containsKey(Voicemails.SOURCE_PACKAGE)) {
            if (!uriData.getSourcePackage().equals(values.get(Voicemails.SOURCE_PACKAGE))) {
                throw new IllegalArgumentException(
                        "Provider in URI was " + uriData.getSourcePackage() +
                        " but doesn't match provider in ContentValues which was "
                        + values.get(Voicemails.SOURCE_PACKAGE));
            }
        }
        // You must have access to the provider given in values.
        if (!hasFullPermission(getCallingPackage())) {
            checkPackagesMatch(getCallingPackage(), values.getAsString(Voicemails.SOURCE_PACKAGE),
                    uriData.getUri());
        }
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

    private void checkInsertSupported(UriData uriData) {
        if (uriData.hasId()) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot insert URI: %s. Inserted URIs should not contain an id.",
                    uriData.getUri()));
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        checkHasOwnPermission();
        UriData uriData = createUriData(uri);
        checkUpdateSupported(uriData);
        checkPackagePermission(uriData);
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        // TODO: This implementation does not allow bulk update because it only accepts
        // URI that include message Id. I think we do want to support bulk update.
        String combinedClause = concatenateClauses(selection, getPackageRestrictionClause(),
                getWhereClause(uriData), getCallTypeClause());
        int count = db.update(VOICEMAILS_TABLE_NAME, values, combinedClause, selectionArgs);
        if (count > 0) {
            notifyChange(uri, Intent.ACTION_PROVIDER_CHANGED);
        }
        return count;
    }

    private void checkUpdateSupported(UriData uriData) {
        if (!uriData.hasId()) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot update URI: %s.  Bulk update not supported", uriData.getUri()));
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        checkHasOwnPermission();
        UriData uriData = createUriData(uri);
        checkPackagePermission(uriData);
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        String combinedClause = concatenateClauses(selection, getPackageRestrictionClause(),
                getWhereClause(uriData), getCallTypeClause());

        // Delete all the files associated with this query.  Once we've deleted the rows, there will
        // be no way left to get hold of the files.
        Cursor cursor = null;
        try {
            cursor = queryInternal(uriData, FILENAME_ONLY_PROJECTION, selection, selectionArgs,
                    null);
            while (cursor.moveToNext()) {
                File file = new File(cursor.getString(0));
                if (file.exists()) {
                    boolean success = file.delete();
                    if (!success) {
                        Log.e(TAG, "Failed to delete file: " + file.getAbsolutePath());
                    }
                }
            }
        } finally {
            CloseUtils.closeQuietly(cursor);
        }

        // Now delete the rows themselves.
        int count = db.delete(VOICEMAILS_TABLE_NAME, combinedClause, selectionArgs);
        if (count > 0) {
            notifyChange(uri, Intent.ACTION_PROVIDER_CHANGED);
        }
        return count;
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        checkHasOwnPermission();
        UriData uriData = createUriData(uri);
        checkPackagePermission(uriData);

        // This relies on "_data" column to be populated with the file path.
        ParcelFileDescriptor openFileHelper = openFileHelper(uri, mode);

        // If the open succeeded, then update the file exists bit in the table.
        if (mode.contains("w")) {
            ContentValues contentValues = new ContentValues();
            contentValues.put(Voicemails.HAS_CONTENT, 1);
            update(uri, contentValues, null, null);
        }

        return openFileHelper;
    }

    /**
     * Notifies the content resolver and fires required broadcast intent(s) to notify about the
     * change.
     *
     * @param notificationUri The URI that got impacted due to the change. This is the URI that is
     *            included in content resolver and broadcast intent notification.
     * @param intentActions List of intent actions that needs to be fired. A separate intent is
     *            fired for each intent action.
     */
    private void notifyChange(Uri notificationUri, String... intentActions) {
        // Notify the observers.
        mContentResolver.notifyChange(notificationUri, null, true);
        // Fire notification intents.
        for (String intentAction : intentActions) {
            // TODO: We can possibly be more intelligent here and send targeted intents based on
            // what voicemail permission the package has. If possible, here is what we would like to
            // do for a given broadcast intent -
            // 1) Send it to all packages that have READ_WRITE_ALL_VOICEMAIL permission.
            // 2) Send it to only the owner package that has just READ_WRITE_OWN_VOICEMAIL, if not
            // already sent in (1).
            Intent intent = new Intent(intentAction, notificationUri);
            intent.putExtra(VoicemailContract.EXTRA_CHANGED_BY, getCallingPackage());
            context().sendOrderedBroadcast(intent, Manifest.permission.READ_WRITE_OWN_VOICEMAIL);
        }
    }

    /** Generates a random file for storing audio data. */
    private String generateDataFile() {
        try {
            File dataDirectory = context().getDir(DATA_DIRECTORY, Context.MODE_PRIVATE);
            File voicemailFile = File.createTempFile("voicemail", "", dataDirectory);
            return voicemailFile.getAbsolutePath();
        } catch (IOException e) {
            // If we are unable to create a temporary file, something went horribly wrong.
            throw new RuntimeException("unable to create temp file", e);
        }
    }

    /**
     * Decorates a URI by providing methods to get various properties from the URI.
     */
    private static class UriData {
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
        if (!hasFullPermission(getCallingPackage())) {
            if (!uriData.hasSourcePackage()) {
                // You cannot have a match if this is not a provider uri.
                throw new SecurityException(String.format(
                        "Provider %s does not have %s permission." +
                                "\nPlease use /voicemail/provider/ query path instead.\nURI: %s",
                        getCallingPackage(), Manifest.permission.READ_WRITE_ALL_VOICEMAIL,
                        uriData.getUri()));
            }
            checkPackagesMatch(getCallingPackage(), uriData.getSourcePackage(), uriData.getUri());
        }
    }

    private static TypedUriMatcherImpl<VoicemailUriType> createUriMatcher() {
        return new TypedUriMatcherImpl<VoicemailUriType>(
                VoicemailContract.AUTHORITY, VoicemailUriType.values());
    }

    /** Get a {@link UriData} corresponding to a given uri. */
    private UriData createUriData(Uri uri) {
        List<String> segments = uri.getPathSegments();
        switch (createUriMatcher().match(uri)) {
            case VOICEMAILS:
                return new UriData(uri, null, null);
            case VOICEMAILS_ID:
                return new UriData(uri, segments.get(1), null);
            case VOICEMAILS_SOURCE:
                return new UriData(uri, null, segments.get(2));
            case VOICEMAILS_SOURCE_ID:
                return new UriData(uri, segments.get(3), segments.get(2));
            case NO_MATCH:
                throw new IllegalArgumentException("Invalid URI: " + uri);
            default:
                throw new IllegalStateException("Impossible, all cases are covered");
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
            if (hasFullPermission(callerPackage)) {
                // Full always wins, we can return early.
                return callerPackage;
            }
            if (hasOwnPermission(callerPackage)) {
                bestSoFar = callerPackage;
            }
        }
        return bestSoFar;
    }

    /**
     * This check is made once only at every entry-point into this class from outside.
     *
     * @throws SecurityException if the caller does not have the voicemail source permission.
     */
    private void checkHasOwnPermission() {
        if (!hasOwnPermission(getCallingPackage())) {
            throw new SecurityException("The caller must have permission: " +
                    Manifest.permission.READ_WRITE_OWN_VOICEMAIL);
        }
    }

    /** Tells us if the given package has the source permission. */
    private boolean hasOwnPermission(String packageName) {
        return hasPermission(packageName, Manifest.permission.READ_WRITE_OWN_VOICEMAIL);
    }

    /**
     * Tells us if the given package has the full permission and the source
     * permission.
     */
    private boolean hasFullPermission(String packageName) {
        return hasOwnPermission(packageName) &&
                hasPermission(packageName, Manifest.permission.READ_WRITE_ALL_VOICEMAIL);
    }

    /** Tells us if the given package has the given permission. */
    /* package for test */boolean hasPermission(String packageName, String permission) {
        return context().getPackageManager().checkPermission(permission, packageName)
                == PackageManager.PERMISSION_GRANTED;
    }

    /**
     * Creates a clause to restrict the selection to the calling provider or null if the caller has
     * access to all data.
     */
    private String getPackageRestrictionClause() {
        if (hasFullPermission(getCallingPackage())) {
            return null;
        }
        return getEqualityClause(Voicemails.SOURCE_PACKAGE, getCallingPackage());
    }


    /** Creates a clause to restrict the selection to only voicemail call type.*/
    private String getCallTypeClause() {
        return getEqualityClause(Calls.TYPE, String.valueOf(Calls.VOICEMAIL_TYPE));
    }

}
