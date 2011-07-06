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

import com.android.providers.contacts.VoicemailContentProvider.UriData;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.ParcelFileDescriptor;

/**
 * Defines interfaces for communication between voicemail content provider and voicemail table
 * implementations.
 */
public interface VoicemailTable {
    /**
     * Interface that the voicemail content provider uses to delegate database level operations
     * to the appropriate voicemail table implementation.
     */
    public interface Delegate {
        public Uri insert(UriData uriData, ContentValues values);
        public int bulkInsert(UriData uriData, ContentValues[] valuesArray);
        public int delete(UriData uriData, String selection, String[] selectionArgs);
        public Cursor query(UriData uriData, String[] projection, String selection,
                String[] selectionArgs, String sortOrder);
        public int update(UriData uriData, ContentValues values, String selection,
                String[] selectionArgs);
        public String getType(UriData uriData);
        public ParcelFileDescriptor openFile(UriData uriData, String mode,
                ParcelFileDescriptor openFileHelper);
    }

    /**
     * A helper interface that an implementation of {@link Delegate} uses to access common
     * functionality across different voicemail tables.
     */
    public interface DelegateHelper {
        /**
         * Notifies the content resolver and fires required broadcast intent(s) to notify about the
         * change.
         *
         * @param notificationUri The URI that got impacted due to the change. This is the URI that
         *            is included in content resolver and broadcast intent notification.
         * @param intentActions List of intent actions that needs to be fired. A separate intent is
         *            fired for each intent action.
         */
        public void notifyChange(Uri notificationUri, String... intentActions);
        /**
         * Inserts source_package field into ContentValues. Used in insert operations.
         */
        public void checkAndAddSourcePackageIntoValues(UriData uriData, ContentValues values);
    }
}