/*
 * Copyright (C) 2018 The Android Open Source Project
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
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

/**
 * An empty provider to support RawContacts change notification.
 */
public class RawContactsChangeNotificationProvider extends ContentProvider {
    public static final String AUTHORITY = "com.android.contacts.raw_contacts";

    @Override
    public boolean onCreate() {
        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // Not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // Not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // Not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public String getType(Uri uri) {
        // Not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor query(Uri uri, String[] inProjection, String selection, String[] selectionArgs,
            String sortOrder) {
        // Not needed.
        throw new UnsupportedOperationException();
    }
}
