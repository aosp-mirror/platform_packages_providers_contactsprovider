/*
 * Copyright (C) 2021 The Android Open Source Project
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

import static com.android.providers.contacts.util.DbQueryUtils.getEqualityClause;

import android.Manifest;
import android.annotation.NonNull;
import android.annotation.Nullable;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Binder;
import android.os.Process;
import android.provider.CallLog;
import android.telecom.TelecomManager;
import android.text.TextUtils;
import android.util.Log;


import com.android.providers.contacts.util.SelectionBuilder;

import java.util.Objects;

public class CallComposerLocationProvider extends ContentProvider {
    private static final String TAG = CallComposerLocationProvider.class.getSimpleName();
    private static final String DB_NAME = "call_composer_locations.db";
    private static final String TABLE_NAME = "locations";
    private static final int VERSION = 1;

    private static final int LOCATION = 1;
    private static final int LOCATION_ID = 2;

    private static class OpenHelper extends SQLiteOpenHelper {
        public OpenHelper(@Nullable Context context, @Nullable String name,
                @Nullable SQLiteDatabase.CursorFactory factory, int version) {
            super(context, name, factory, version);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE " + TABLE_NAME+ " (" +
                    CallLog.Locations._ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    CallLog.Locations.LATITUDE + " REAL, " +
                    CallLog.Locations.LONGITUDE + " REAL" +
                    ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // Nothing to do here, still on version 1.
        }
    }

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);
    static {
        sURIMatcher.addURI(CallLog.Locations.AUTHORITY, "", LOCATION);
        sURIMatcher.addURI(CallLog.Locations.AUTHORITY, "/#", LOCATION_ID);
    }

    private OpenHelper mOpenHelper;

    @Override
    public boolean onCreate() {
        mOpenHelper = new OpenHelper(getContext(), DB_NAME, null, VERSION);
        return true;
    }

    @Nullable
    @Override
    public Cursor query(@NonNull Uri uri, @Nullable String[] projection, @Nullable String selection,
            @Nullable String[] selectionArgs, @Nullable String sortOrder) {
        enforceAccessRestrictions();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);
        qb.setStrict(true);
        qb.setStrictGrammar(true);
        final int match = sURIMatcher.match(uri);

        final SelectionBuilder selectionBuilder = new SelectionBuilder(selection);
        switch (match) {
            case LOCATION_ID: {
                selectionBuilder.addClause(getEqualityClause(CallLog.Locations._ID,
                        parseLocationIdFromUri(uri)));
                break;
            }
            default:
                throw new IllegalArgumentException("Provided URI is not supported for query.");
        }

        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        return qb.query(db, projection, selectionBuilder.build(), selectionArgs, null,
                null, sortOrder, null);
    }

    @Nullable
    @Override
    public String getType(@NonNull Uri uri) {
        final int match = sURIMatcher.match(uri);
        switch (match) {
            case LOCATION_ID:
                return CallLog.Locations.CONTENT_ITEM_TYPE;
            case LOCATION:
                return CallLog.Locations.CONTENT_TYPE;
            default:
                return null;
        }
    }

    @Nullable
    @Override
    public Uri insert(@NonNull Uri uri, @Nullable ContentValues values) {
        enforceAccessRestrictions();
        long id = mOpenHelper.getWritableDatabase().insert(TABLE_NAME, null, values);
        return ContentUris.withAppendedId(CallLog.Locations.CONTENT_URI, id);
    }

    @Override
    public int delete(@NonNull Uri uri, @Nullable String selection,
            @Nullable String[] selectionArgs) {
        enforceAccessRestrictions();
        final int match = sURIMatcher.match(uri);
        switch (match) {
            case LOCATION_ID:
                long id = parseLocationIdFromUri(uri);
                return mOpenHelper.getWritableDatabase().delete(TABLE_NAME,
                        CallLog.Locations._ID + " = ?", new String[] {String.valueOf(id)});
            case LOCATION:
                Log.w(TAG, "Deleting entire location table!");
                return mOpenHelper.getWritableDatabase().delete(TABLE_NAME, "1", null);
            default:
                throw new IllegalArgumentException("delete() on the locations"
                        + " does not support the uri " + uri.toString());
        }
    }

    @Override
    public int update(@NonNull Uri uri, @Nullable ContentValues values, @Nullable String selection,
            @Nullable String[] selectionArgs) {
        enforceAccessRestrictions();
        throw new UnsupportedOperationException(
                "Call composer location db does not support updates");
    }

    private long parseLocationIdFromUri(Uri uri) {
        try {
            return Long.parseLong(uri.getPathSegments().get(0));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid location id in uri: " + uri, e);
        }
    }

    private void enforceAccessRestrictions() {
        int uid = Binder.getCallingUid();
        if (uid == Process.SYSTEM_UID || uid == Process.myUid() || uid == Process.PHONE_UID) {
            return;
        }
        String defaultDialerPackageName = getContext().getSystemService(TelecomManager.class)
                .getDefaultDialerPackage();
        if (TextUtils.isEmpty(defaultDialerPackageName)) {
            throw new SecurityException("Access to call composer locations is only allowed for the"
                    + " default dialer, but the default dialer is unset");
        }
        String[] callingPackageCandidates = getContext().getPackageManager().getPackagesForUid(uid);
        for (String packageCandidate : callingPackageCandidates) {
            if (Objects.equals(packageCandidate, defaultDialerPackageName)) {
                return;
            }
        }
        throw new SecurityException("Access to call composer locations is only allowed for the "
                + "default dialer: " + defaultDialerPackageName);
    }
}
