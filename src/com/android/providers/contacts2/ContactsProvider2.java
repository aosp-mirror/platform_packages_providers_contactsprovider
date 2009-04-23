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
 * limitations under the License
 */

package com.android.providers.contacts2;

import com.android.providers.contacts2.ContactsContract.Contacts;
import com.android.providers.contacts2.ContactsContract.Data;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import java.util.HashMap;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends ContentProvider {
    private static final String TAG = "~~~~~~~~~~~~~"; // TODO: set to class name

    private static final int DATABASE_VERSION = 4;
    private static final String DATABASE_NAME = "contacts2.db";

    private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int CONTACTS = 1000;
    private static final int CONTACTS_ID = 1001;

    private static final int DATA = 2000;
    private static final int DATA_ID = 2001;

    private static final HashMap<String, String> sContactsProjectionMap;
    private static final HashMap<String, String> sDataProjectionMap;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sURIMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);

        HashMap<String, String> columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.GIVEN_NAME, Contacts.GIVEN_NAME);
        columns.put(Contacts.PHONETIC_GIVEN_NAME, Contacts.PHONETIC_GIVEN_NAME);
        columns.put(Contacts.FAMILY_NAME, Contacts.FAMILY_NAME);
        columns.put(Contacts.PHONETIC_FAMILY_NAME, Contacts.PHONETIC_FAMILY_NAME);
        columns.put(Contacts.DISPLAY_NAME,
                Contacts.GIVEN_NAME + " || ' ' || " + Contacts.FAMILY_NAME +
                " AS " + Contacts.DISPLAY_NAME);
        sContactsProjectionMap = columns;

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, "data._id AS _id");
        columns.put(Data.CONTACT_ID, Data.CONTACT_ID);
        columns.put(Data.PACKAGE, Data.PACKAGE);
        columns.put(Data.KIND, Data.KIND);
        columns.put(Data.DATA1, Data.DATA1);
        columns.put(Data.DATA2, Data.DATA2);
        columns.put(Data.DATA3, Data.DATA3);
        columns.put(Data.DATA4, Data.DATA4);
        columns.put(Data.DATA5, Data.DATA5);
        columns.put(Data.DATA6, Data.DATA6);
        columns.put(Data.DATA7, Data.DATA7);
        columns.put(Data.DATA8, Data.DATA8);
        columns.put(Data.DATA9, Data.DATA9);
        columns.put(Data.DATA10, Data.DATA10);
        sDataProjectionMap = columns;
    }

    private OpenHelper mOpenHelper;

    private class OpenHelper extends SQLiteOpenHelper {
        public OpenHelper() {
            super(getContext(), DATABASE_NAME, null, DATABASE_VERSION);
            Log.i(TAG, "Creating OpenHelper");
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            Log.i(TAG, "Bootstrapping database");

            db.execSQL("CREATE TABLE contacts (" +
                    Contacts._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Contacts.GIVEN_NAME + " TEXT," +
                    Contacts.FAMILY_NAME + " TEXT," +
                    Contacts.TIMES_CONTACTED + " INTEGER," +
                    Contacts.LAST_TIME_CONTACTED + " INTEGER," +
                    Contacts.CUSTOM_RINGTONE + " TEXT," +
                    Contacts.SEND_TO_VOICEMAIL + " INTEGER," +
                    Contacts.STARRED + " INTEGER" +
            ");");

            db.execSQL("CREATE TABLE data (" +
                    Data._ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                    Data.CONTACT_ID + " INTEGER NOT NULL," +
                    Data.PACKAGE + " TEXT NOT NULL," +
                    Data.KIND + " INTEGER NOT NULL," +
                    Data.DATA1 + " TEXT," +
                    Data.DATA2 + " TEXT," +
                    Data.DATA3 + " TEXT," +
                    Data.DATA4 + " TEXT," +
                    Data.DATA5 + " TEXT," +
                    Data.DATA6 + " TEXT," +
                    Data.DATA7 + " TEXT," +
                    Data.DATA8 + " TEXT," +
                    Data.DATA9 + " TEXT," +
                    Data.DATA10 + " TEXT" +
            ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.i(TAG, "Upgraing from version " + oldVersion + " to " + newVersion
                    + ", data will be lost!");

            db.execSQL("DROP TABLE IF EXISTS contacts;");
            db.execSQL("DROP TABLE IF EXISTS data;");

            onCreate(db);
        }
    }

    @Override
    public boolean onCreate() {
        mOpenHelper = new OpenHelper();

        // TODO remove this, it's here to force opening the database on boot for testing
        mOpenHelper.getReadableDatabase();

        return true;
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        final int match = sURIMatcher.match(uri);
        switch (match) {
            case CONTACTS: {
                qb.setTables("contacts");
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                qb.setTables("contacts");
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            case DATA: {
                qb.setTables("data");
                qb.setProjectionMap(sDataProjectionMap);
                break;
            }

            case DATA_ID: {
                qb.setTables("data");
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("_id = " + uri.getLastPathSegment());
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs, null, null, sortOrder);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    @Override
    public String getType(Uri uri) {
        final int match = sURIMatcher.match(uri);
        switch (match) {
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
        }
        return null;
    }
}
