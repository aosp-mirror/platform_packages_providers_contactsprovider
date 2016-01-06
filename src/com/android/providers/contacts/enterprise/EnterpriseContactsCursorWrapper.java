/*
 * Copyright (C) 2016 The Android Open Source Project
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

package com.android.providers.contacts.enterprise;

import android.annotation.Nullable;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.CursorWrapper;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.MediaStore;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.PhoneLookup;
import android.text.TextUtils;
import android.util.Log;

import com.android.providers.contacts.ContactsProvider2;

/**
 * Wrap cursor returned from work-side ContactsProvider in order to rewrite values in some colums
 */
public class EnterpriseContactsCursorWrapper extends CursorWrapper {

    private static final String TAG = "EnterpriseCursorWrapper";
    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    private static final UriMatcher sUriMatcher = ContactsProvider2.sUriMatcher;

    // As some of the columns like PHOTO_URI requires contact id, but original projection may not
    // have it, so caller may use a work projection instead of original project to make the
    // query. Hence, we need also to restore the cursor to the origianl projection.
    private final int contactIdIndex;
    private final boolean isContactIdAppended;

    // Derived Fields
    private final Long mDirectoryId;
    private final boolean mIsDirectoryRemote;
    private final String[] originalColumnNames;

    public EnterpriseContactsCursorWrapper(Cursor cursor, String[] originalColumnNames,
            int contactIdIndex, boolean isContactIdAppended, @Nullable Long directoryId) {
        super(cursor);
        this.contactIdIndex = contactIdIndex;
        this.isContactIdAppended = isContactIdAppended;
        this.originalColumnNames = originalColumnNames;
        this.mDirectoryId = directoryId;
        this.mIsDirectoryRemote = directoryId != null
                && Directory.isRemoteDirectory(directoryId);
    }

    @Override
    public int getColumnCount() {
        return originalColumnNames.length;
    }

    @Override
    public String[] getColumnNames() {
        return originalColumnNames;
    }

    @Override
    public String getString(int columnIndex) {
        final String result = super.getString(columnIndex);
        final String columnName = super.getColumnName(columnIndex);
        final long contactId = super.getLong(contactIdIndex);
        switch (columnName) {
            case Contacts.PHOTO_THUMBNAIL_URI:
                if(mIsDirectoryRemote) {
                    return getRemoteDirectoryFileUri(result);
                } else {
                    return getCorpThumbnailUri(contactId, getWrappedCursor());
                }
            case Contacts.PHOTO_URI:
                if(mIsDirectoryRemote) {
                    return getRemoteDirectoryFileUri(result);
                } else {
                    return getCorpDisplayPhotoUri(contactId, getWrappedCursor());
                }
            case Data.PHOTO_FILE_ID:
            case Data.PHOTO_ID:
                return null;
            case Data.CUSTOM_RINGTONE:
                String ringtoneUri = super.getString(columnIndex);
                // TODO: Remove this conditional block once accessing sounds in corp
                // profile becomes possible.
                if (ringtoneUri != null
                        && !Uri.parse(ringtoneUri).isPathPrefixMatch(
                                MediaStore.Audio.Media.INTERNAL_CONTENT_URI)) {
                    ringtoneUri = null;
                }
                return ringtoneUri;
            case Contacts.LOOKUP_KEY:
                final String lookupKey = super.getString(columnIndex);
                if (TextUtils.isEmpty(lookupKey)) {
                    return null;
                } else {
                    return Contacts.ENTERPRISE_CONTACT_LOOKUP_PREFIX + lookupKey;
                }
            default:
                return result;
        }
    }

    @Override
    public int getInt(int column) {
        return (int) getLong(column);
    }

    @Override
    public long getLong(int column) {
        long result = super.getLong(column);
        if (column == contactIdIndex) {
            return result + Contacts.ENTERPRISE_CONTACT_ID_BASE;
        } else {
            final String columnName = getColumnName(column);
            switch (columnName) {
                case Data.PHOTO_FILE_ID:
                case Data.PHOTO_ID:
                    return 0;
                default:
                    return result;
            }
        }
    }

    private String getRemoteDirectoryFileUri(final String photoUriString) {
        if (photoUriString == null) {
            return null;
        }

        // Assume that the authority of photoUri is directoryInfo.authority first
        // TODO: Validate the authority of photoUri is directoryInfo.authority
        Uri.Builder builder = Directory.ENTERPRISE_FILE_URI.buildUpon();
        builder.appendPath(photoUriString);
        builder.appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY, Long.toString(mDirectoryId));
        final String outputUri = builder.build().toString();
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "getCorpDirectoryFileUri: output URI=" + outputUri);
        }

        return outputUri;
    }

    /**
     * Generate a photo URI for {@link PhoneLookup#PHOTO_THUMBNAIL_URI}.
     *
     * Example: "content://com.android.contacts/contacts_corp/ID/photo"
     *
     * {@link ContentProvider#openAssetFile} knows how to fetch from this URI.
     */
    private static String getCorpThumbnailUri(long contactId, Cursor originalCursor) {
        // First, check if the contact has a thumbnail.
        if (originalCursor.isNull(
                originalCursor.getColumnIndex(Contacts.PHOTO_THUMBNAIL_URI))) {
            // No thumbnail.  Just return null.
            return null;
        }
        return ContentUris.appendId(Contacts.CORP_CONTENT_URI.buildUpon(), contactId)
                .appendPath(Contacts.Photo.CONTENT_DIRECTORY).build().toString();
    }

    /**
     * Generate a photo URI for {@link PhoneLookup#PHOTO_URI}.
     *
     * Example 1: "content://com.android.contacts/contacts_corp/ID/display_photo"
     * Example 2: "content://com.android.contacts/contacts_corp/ID/photo"
     *
     * {@link ContentProvider#openAssetFile} knows how to fetch from this URI.
     */
    private static String getCorpDisplayPhotoUri(long contactId, Cursor originalCursor) {
        final int photoUriIndex = originalCursor.getColumnIndex(Contacts.PHOTO_URI);
        final String photoUri = originalCursor.getString(photoUriIndex);
        if (photoUri == null) {
            return null;
        }

        final int uriCode = sUriMatcher.match(Uri.parse(photoUri));
        if (uriCode == ContactsProvider2.CONTACTS_ID_PHOTO) {
            return ContentUris.appendId(Contacts.CORP_CONTENT_URI.buildUpon(), contactId)
                    .appendPath(Contacts.Photo.CONTENT_DIRECTORY).build().toString();
        } else {
            return ContentUris.appendId(Contacts.CORP_CONTENT_URI.buildUpon(), contactId)
                    .appendPath(Contacts.Photo.DISPLAY_PHOTO).build().toString();
        }
    }
}