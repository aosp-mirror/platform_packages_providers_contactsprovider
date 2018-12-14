/*
 * Copyright (C) 2012 The Android Open Source Project
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

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.FileUtils;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Profile;
import android.provider.ContactsContract.RawContacts;
import android.util.Log;

import androidx.annotation.Nullable;

import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import junit.framework.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TestUtils {
    private static final String TAG = "ContactsTestUtils";

    private TestUtils() {
    }

    /**
     * We normally use in-memory DBs in unit tests, because that's faster, but it's impossible to
     * look at intermediate databases when something is failing.  When this flag is set to true,
     * we'll switch to file-based DBs, so we can call {@link #createDatabaseSnapshot}
     * , pull the snapshot DBs and take a look at them.
     */
    public static final boolean ENABLE_DATABASE_SNAPSHOT = false; // DO NOT SUBMIT WITH TRUE.

    private static final Object sDatabasePathLock = new Object();
    private static File sDatabasePath = null;

    private static String getDatabaseFile(Context context, @Nullable String name) {
        if (!ENABLE_DATABASE_SNAPSHOT) {
            return null; // Use the in-memory DB.
        }
        synchronized (sDatabasePathLock) {
            if (sDatabasePath == null) {
                final File path = new File(context.getCacheDir(), "test-db");
                if (path.exists()) {
                    Assert.assertTrue("Unable to delete directory: " + path,
                            FileUtils.deleteContents(path));
                } else {
                    Assert.assertTrue("Unable to create directory: " + path, path.mkdirs());
                }
                Log.i(TAG, "Test DB directory: " + path);

                sDatabasePath = path;
            }
            final File ret;
            if (name == null) {
                ret = sDatabasePath;
            } else {
                ret = new File(sDatabasePath, name);
                Log.i(TAG, "Test DB file: " + ret);
            }
            return ret.getAbsolutePath();
        }
    }

    public static String getContactsDatabaseFilename(Context context) {
        return getContactsDatabaseFilename(context, "");
    }

    public static String getContactsDatabaseFilename(Context context, String suffix) {
        return getDatabaseFile(context, "contacts2" + suffix + ".db");
    }

    public static String getProfileDatabaseFilename(Context context) {
        return getProfileDatabaseFilename(context, "");
    }

    public static String getProfileDatabaseFilename(Context context, String suffix) {
        return getDatabaseFile(context, "profile.db" + suffix + ".db");
    }

    public static void createDatabaseSnapshot(Context context, String name) {
        Assert.assertTrue(
                "ENABLE_DATABASE_SNAPSHOT must be set to true to create database snapshot",
                ENABLE_DATABASE_SNAPSHOT);

        final File fromDir = new File(getDatabaseFile(context, null));
        final File toDir = new File(context.getCacheDir(), "snapshot-" + name);
        if (toDir.exists()) {
            Assert.assertTrue("Unable to delete directory: " + toDir,
                    FileUtils.deleteContents(toDir));
        } else {
            Assert.assertTrue("Unable to create directory: " + toDir, toDir.mkdirs());
        }

        Log.w(TAG, "Copying database files from '" + fromDir + "' into '" + toDir + "'...");

        for (File file : fromDir.listFiles()) {
            try {
                final File to = new File(toDir, file.getName());
                FileUtils.copyFileOrThrow(file, to);
                Log.i(TAG, "Created: " + to);
            } catch (IOException e) {
                Assert.fail("Failed to copy file: " + e.toString());
            }
        }
    }

    /** Convenient method to create a ContentValues */
    public static ContentValues cv(Object... namesAndValues) {
        Assert.assertTrue((namesAndValues.length % 2) == 0);

        final ContentValues ret = new ContentValues();
        for (int i = 1; i < namesAndValues.length; i += 2) {
            final String name = namesAndValues[i - 1].toString();
            final Object value =  namesAndValues[i];
            if (value == null) {
                ret.putNull(name);
            } else if (value instanceof String) {
                ret.put(name, (String) value);
            } else if (value instanceof Integer) {
                ret.put(name, (Integer) value);
            } else if (value instanceof Long) {
                ret.put(name, (Long) value);
            } else {
                Assert.fail("Unsupported type: " + value.getClass().getSimpleName());
            }
        }
        return ret;
    }

    /**
     * Writes the content of a cursor to the log.
     */
    public static final void dumpCursor(Cursor c) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.getColumnCount(); i++) {
            if (i > 0) sb.append("|");
            sb.append(c.getColumnName(i));
        }
        Log.i(TAG, sb.toString());

        final int pos = c.getPosition();

        c.moveToPosition(-1);
        while (c.moveToNext()) {
            sb.setLength(0);
            for (int i = 0; i < c.getColumnCount(); i++) {
                if (i > 0) sb.append("|");

                if (c.getType(i) == Cursor.FIELD_TYPE_BLOB) {
                    byte[] blob = c.getBlob(i);
                    sb.append("([blob] ");
                    sb.append(blob == null ? "null" : blob.length + "b");
                    sb.append(")");
                } else {
                    sb.append(c.getString(i));
                }
            }
            Log.i(TAG, sb.toString());
        }

        c.moveToPosition(pos);
    }

    public static void dumpTable(SQLiteDatabase db, String name) {
        Log.i(TAG, "Dumping table: " + name);
        try (Cursor c = db.rawQuery(String.format("SELECT * FROM %s", name), null)) {
            dumpCursor(c);
        }
    }

    public static void dumpUri(Context context, Uri uri) {
        Log.i(TAG, "Dumping URI: " + uri);
        try (Cursor c = context.getContentResolver().query(uri, null, null, null, null)) {
            dumpCursor(c);
        }
    }

    public static void dumpUri(ContentResolver resolver, Uri uri) {
        Log.i(TAG, "Dumping URI: " + uri);
        try (Cursor c = resolver.query(uri, null, null, null, null)) {
            dumpCursor(c);
        }
    }

    /**
     * Writes an arbitrary byte array to the test apk's cache directory.
     */
    public static final String dumpToCacheDir(Context context, String prefix, String suffix,
            byte[] data) {
        try {
            File file = File.createTempFile(prefix, suffix, context.getCacheDir());
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(data);
            fos.close();
            return file.getAbsolutePath();
        } catch (IOException e) {
            return "[Failed to write to file: " + e.getMessage() + "]";
        }
    }

    public static Uri insertRawContact(
            ContentResolver resolver, ContactsDatabaseHelper dbh, ContentValues values) {
        return insertRawContact(RawContacts.CONTENT_URI, resolver, dbh, values);
    }

    public static Uri insertProfileRawContact(
            ContentResolver resolver, ContactsDatabaseHelper dbh, ContentValues values) {
        return insertRawContact(Profile.CONTENT_RAW_CONTACTS_URI, resolver, dbh, values);
    }

    private static Uri insertRawContact(Uri tableUri,
            ContentResolver resolver, ContactsDatabaseHelper dbh, ContentValues values) {
        final SQLiteDatabase db = dbh.getWritableDatabase();

        final Uri rowUri = resolver.insert(tableUri, values);
        Long timesContacted = values.getAsLong(RawContacts.LR_TIMES_CONTACTED);
        if (timesContacted != null) {
            // TIMES_CONTACTED is no longer modifiable via resolver, so we update the DB directly.
            final long rid = Long.parseLong(rowUri.getLastPathSegment());

            final String[] args = {String.valueOf(rid)};

            db.update(Tables.RAW_CONTACTS,
                    cv(RawContacts.RAW_TIMES_CONTACTED, (long) timesContacted),
                    "_id=?", args);

            // Then propagate it to contacts too.
            db.execSQL("UPDATE " + Tables.CONTACTS
                    + " SET " + Contacts.RAW_TIMES_CONTACTED + " = ("
                    + " SELECT sum(" + RawContacts.RAW_TIMES_CONTACTED + ") FROM "
                    + Tables.RAW_CONTACTS + " AS r "
                    + " WHERE " + Tables.CONTACTS + "._id = r." + RawContacts.CONTACT_ID
                    + " GROUP BY r." + RawContacts.CONTACT_ID + ")");
        }
        return rowUri;
    }

    public static void executeSqlFromAssetFile(
            Context context, SQLiteDatabase db, String assetName) {
        try (InputStream input = context.getAssets().open(assetName);) {
            BufferedReader r = new BufferedReader(new InputStreamReader(input));
            String query;
            while ((query = r.readLine()) != null) {
                if (query.trim().length() == 0 || query.startsWith("--")) {
                    continue;
                }
                db.execSQL(query);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.toString());
        }
    }
}
