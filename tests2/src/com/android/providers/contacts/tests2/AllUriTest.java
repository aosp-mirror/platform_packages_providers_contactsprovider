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
package com.android.providers.contacts.tests2;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.CancellationSignal;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.SyncState;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

import junit.framework.AssertionFailedError;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

/*
 * TODO The following operations would fail, not because they're not supported, but because of
 * missing parameters.  Fix them.
insert for 'content://com.android.contacts/contacts' failed: Aggregate contacts are created automatically
insert for 'content://com.android.contacts/raw_contacts/1/data' failed: mimetype is required
update for 'content://com.android.contacts/raw_contacts/1/stream_items/1' failed: Empty values
insert for 'content://com.android.contacts/data' failed: raw_contact_id is required
insert for 'content://com.android.contacts/settings' failed: Must specify both or neither of ACCOUNT_NAME and ACCOUNT_TYPE; URI: content://com.android.contacts/settings?account_type=1, calling user: com.android.providers.contacts.tests2, calling package:com.android.providers.contacts.tests2
insert for 'content://com.android.contacts/status_updates' failed: PROTOCOL and IM_HANDLE are required
insert for 'content://com.android.contacts/profile' failed: The profile contact is created automatically
insert for 'content://com.android.contacts/profile/data' failed: raw_contact_id is required
insert for 'content://com.android.contacts/profile/raw_contacts/1/data' failed: mimetype is required
insert for 'content://com.android.contacts/profile/status_updates' failed: PROTOCOL and IM_HANDLE are required


openInputStream for 'content://com.android.contacts/contacts/as_multi_vcard/XXX' failed: Caught Exception: Invalid lookup id: XXX
openInputStream for 'content://com.android.contacts/directory_file_enterprise/XXX?directory=0' failed: Caught Exception: java.lang.IllegalArgumentException: Directory is not a remote directory: content://com.android.contacts/directory_file_enterprise/XXX?directory=0
openOutputStream for 'content://com.android.contacts/directory_file_enterprise/XXX?directory=0' failed: Caught Exception: java.lang.IllegalArgumentException: Directory is not a remote directory: content://com.android.contacts/directory_file_enterprise/XXX?directory=0
*/

/**
 * TODO Add test for delete/update/insert too.
 * TODO Copy it to CTS
 */
@LargeTest
public class AllUriTest extends AndroidTestCase {
    private static final String TAG = "AllUrlTest";

    // "-" : Query not supported.
    // "!" : Can't query because it requires the cross-user permission.
    // The following markers are planned, but not implemented and the definition below is not all
    // correct yet.
    // "d" : supports delete.
    // "u" : supports update.
    // "i" : supports insert.
    // "r" : supports read.
    // "w" : supports write.
    // "s" : has x_times_contacted and x_last_time_contacted.
    // "t" : has x_times_used and x_last_time_used.
    private static final String[][] URIs = {
            {"content://com.android.contacts/contacts", "sud"},
            {"content://com.android.contacts/contacts/1", "sud"},
            {"content://com.android.contacts/contacts/1/data", "t"},
            {"content://com.android.contacts/contacts/1/entities", "t"},
            {"content://com.android.contacts/contacts/1/suggestions"},
            {"content://com.android.contacts/contacts/1/suggestions/XXX"},
            {"content://com.android.contacts/contacts/1/photo", "r"},
            {"content://com.android.contacts/contacts/1/display_photo", "-r"},
            {"content://com.android.contacts/contacts_corp/1/photo", "-r"},
            {"content://com.android.contacts/contacts_corp/1/display_photo", "-r"},

            {"content://com.android.contacts/contacts/filter", "s"},
            {"content://com.android.contacts/contacts/filter/XXX", "s"},

            {"content://com.android.contacts/contacts/lookup/nlookup", "sud"},
            {"content://com.android.contacts/contacts/lookup/nlookup/data", "t"},
            {"content://com.android.contacts/contacts/lookup/nlookup/photo", "tr"},

            {"content://com.android.contacts/contacts/lookup/nlookup/1", "sud"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/data"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/photo", "r"},
            {"content://com.android.contacts/contacts/lookup/nlookup/display_photo", "-r"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/display_photo", "-r"},
            {"content://com.android.contacts/contacts/lookup/nlookup/entities"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/entities"},

            {"content://com.android.contacts/contacts/as_vcard/nlookup", "r"},
            {"content://com.android.contacts/contacts/as_multi_vcard/XXX"},

            {"content://com.android.contacts/contacts/strequent/", "s"},
            {"content://com.android.contacts/contacts/strequent/filter/XXX", "s"},

            {"content://com.android.contacts/contacts/group/XXX"},

            {"content://com.android.contacts/contacts/frequent", "s"},
            {"content://com.android.contacts/contacts/delete_usage", "-d"},
            {"content://com.android.contacts/contacts/filter_enterprise?directory=0", "s"},
            {"content://com.android.contacts/contacts/filter_enterprise/XXX?directory=0", "s"},

            {"content://com.android.contacts/raw_contacts", "siud"},
            {"content://com.android.contacts/raw_contacts/1", "sud"},
            {"content://com.android.contacts/raw_contacts/1/data", "tu"},
            {"content://com.android.contacts/raw_contacts/1/display_photo", "-rw"},
            {"content://com.android.contacts/raw_contacts/1/entity"},

            {"content://com.android.contacts/raw_contact_entities"},
            {"content://com.android.contacts/raw_contact_entities_corp", "!"},

            {"content://com.android.contacts/data", "tud"},
            {"content://com.android.contacts/data/1", "tudr"},
            {"content://com.android.contacts/data/phones", "t"},
            {"content://com.android.contacts/data_enterprise/phones", "!"},
            {"content://com.android.contacts/data/phones/1", "tud"},
            {"content://com.android.contacts/data/phones/filter", "t"},
            {"content://com.android.contacts/data/phones/filter/XXX", "t"},

            {"content://com.android.contacts/data/phones/filter_enterprise?directory=0", "t"},
            {"content://com.android.contacts/data/phones/filter_enterprise/XXX?directory=0", "t"},

            {"content://com.android.contacts/data/emails", "t"},
            {"content://com.android.contacts/data/emails/1", "tud"},
            {"content://com.android.contacts/data/emails/lookup", "t"},
            {"content://com.android.contacts/data/emails/lookup/XXX", "t"},
            {"content://com.android.contacts/data/emails/filter", "t"},
            {"content://com.android.contacts/data/emails/filter/XXX", "t"},
            {"content://com.android.contacts/data/emails/filter_enterprise?directory=0", "t"},
            {"content://com.android.contacts/data/emails/filter_enterprise/XXX?directory=0", "t"},
            {"content://com.android.contacts/data/emails/lookup_enterprise", "t"},
            {"content://com.android.contacts/data/emails/lookup_enterprise/XXX", "t"},
            {"content://com.android.contacts/data/postals", "t"},
            {"content://com.android.contacts/data/postals/1", "tud"},
            {"content://com.android.contacts/data/usagefeedback/1,2,3", "-u"},
            {"content://com.android.contacts/data/callables/", "t"},
            {"content://com.android.contacts/data/callables/1", "tud"},
            {"content://com.android.contacts/data/callables/filter", "t"},
            {"content://com.android.contacts/data/callables/filter/XXX", "t"},
            {"content://com.android.contacts/data/callables/filter_enterprise?directory=0", "t"},
            {"content://com.android.contacts/data/callables/filter_enterprise/XXX?directory=0",
                    "t"},
            {"content://com.android.contacts/data/contactables/", "t"},
            {"content://com.android.contacts/data/contactables/filter", "t"},
            {"content://com.android.contacts/data/contactables/filter/XXX", "t"},

            {"content://com.android.contacts/groups", "iud"},
            {"content://com.android.contacts/groups/1", "ud"},
            {"content://com.android.contacts/groups_summary"},
            {"content://com.android.contacts/syncstate", "iud"},
            {"content://com.android.contacts/syncstate/1", "-ud"},
            {"content://com.android.contacts/profile/syncstate", "iud"},
            {"content://com.android.contacts/phone_lookup/XXX"},
            {"content://com.android.contacts/phone_lookup_enterprise/XXX"},
            {"content://com.android.contacts/aggregation_exceptions", "u"},
            {"content://com.android.contacts/settings", "ud"},
            {"content://com.android.contacts/status_updates", "ud"},
            {"content://com.android.contacts/status_updates/1"},
            {"content://com.android.contacts/search_suggest_query"},
            {"content://com.android.contacts/search_suggest_query/XXX"},
            {"content://com.android.contacts/search_suggest_shortcut/XXX"},
            {"content://com.android.contacts/provider_status"},
            {"content://com.android.contacts/directories", "u"},
            {"content://com.android.contacts/directories/1"},
            {"content://com.android.contacts/directories_enterprise"},
            {"content://com.android.contacts/directories_enterprise/1"},
            {"content://com.android.contacts/complete_name"},
            {"content://com.android.contacts/profile", "su"},
            {"content://com.android.contacts/profile/entities", "s"},
            {"content://com.android.contacts/profile/data", "tud"},
            {"content://com.android.contacts/profile/data/1", "td"},
            {"content://com.android.contacts/profile/photo", "t"},
            {"content://com.android.contacts/profile/display_photo", "-r"},
            {"content://com.android.contacts/profile/as_vcard", "r"},
            {"content://com.android.contacts/profile/raw_contacts", "siud"},

            // Note this should have supported update... Too late to add.
            {"content://com.android.contacts/profile/raw_contacts/1", "sd"},
            {"content://com.android.contacts/profile/raw_contacts/1/data", "tu"},
            {"content://com.android.contacts/profile/raw_contacts/1/entity"},
            {"content://com.android.contacts/profile/status_updates", "ud"},
            {"content://com.android.contacts/profile/raw_contact_entities"},
            {"content://com.android.contacts/display_photo/1", "-r"},
            {"content://com.android.contacts/photo_dimensions"},
            {"content://com.android.contacts/deleted_contacts"},
            {"content://com.android.contacts/deleted_contacts/1"},
            {"content://com.android.contacts/directory_file_enterprise/XXX?directory=0", "-"},
    };

    private static final String[] ARG1 = {"-1"};

    private ContentResolver mResolver;

    private ArrayList<String> mFailures;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mFailures = new ArrayList<>();
        mResolver = getContext().getContentResolver();
    }

    @Override
    protected void tearDown() throws Exception {
        if (mFailures != null) {
            fail("mFailures is not null.  Did you forget to call failIfFailed()?");
        }

        super.tearDown();
    }

    private void addFailure(String message, Throwable th) {
        Log.e(TAG, "Failed: " + message, th);

        final int MAX = 100;
        if (mFailures.size() == MAX) {
            mFailures.add("Too many failures.");
        } else if (mFailures.size() > MAX) {
            // Too many failures already...
        } else {
            mFailures.add(message);
        }
    }

    private void failIfFailed() {
        if (mFailures == null) {
            fail("mFailures is null.  Maybe called failIfFailed() twice?");
        }
        if (mFailures.size() > 0) {
            StringBuilder message = new StringBuilder();

            if (mFailures.size() > 0) {
                Log.e(TAG, "Something went wrong:");
                for (String s : mFailures) {
                    Log.e(TAG, s);
                    if (message.length() > 0) {
                        message.append("\n");
                    }
                    message.append(s);
                }
            }
            mFailures = null;
            fail("Following test(s) failed:\n" + message);
        }
        mFailures = null;
    }

    private static Uri getUri(String[] path) {
        return Uri.parse(path[0]);
    }

    private static boolean supportsQuery(String[] path) {
        if (path.length == 1) {
            return true; // supports query by default.
        }
        return !(path[1].contains("-") || path[1].contains("!"));
    }

    private static boolean supportsInsert(String[] path) {
        return (path.length) >= 2 && path[1].contains("i");
    }

    private static boolean supportsUpdate(String[] path) {
        return (path.length) >= 2 && path[1].contains("u");
    }

    private static boolean supportsDelete(String[] path) {
        return (path.length) >= 2 && path[1].contains("d");
    }

    private static boolean supportsRead(String[] path) {
        return (path.length) >= 2 && path[1].contains("r");
    }

    private static boolean supportsWrite(String[] path) {
        return (path.length) >= 2 && path[1].contains("w");
    }

    private String[] getColumns(Uri uri) {
        try (Cursor c = mResolver.query(uri,
                null, // projection
                "1=2", // selection
                null, // selection args
                null // sort order
                )) {
            return c.getColumnNames();
        }
    }

    private void checkQueryExecutable(Uri uri,
            String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        try {
            try (Cursor c = mResolver.query(uri, projection, selection,
                    selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query failed: URI=" + uri + " Message=" + th.getMessage(), th);
        }
        try {
            // With CancellationSignal.
            try (Cursor c = mResolver.query(uri, projection, selection,
                    selectionArgs, sortOrder, new CancellationSignal())) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query with cancel failed: URI=" + uri + " Message=" + th.getMessage(), th);
        }
        try {
            // With limit.
            try (Cursor c = mResolver.query(
                    uri.buildUpon().appendQueryParameter(
                            ContactsContract.LIMIT_PARAM_KEY, "0").build(),
                    projection, selection, selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query with limit failed: URI=" + uri + " Message=" + th.getMessage(), th);
        }

        try {
            // With account.
            try (Cursor c = mResolver.query(
                    uri.buildUpon()
                            .appendQueryParameter(RawContacts.ACCOUNT_NAME, "a")
                            .appendQueryParameter(RawContacts.ACCOUNT_TYPE, "b")
                            .appendQueryParameter(RawContacts.DATA_SET, "c")
                            .build(),
                    projection, selection, selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query with limit failed: URI=" + uri + " Message=" + th.getMessage(), th);
        }
    }

    private void checkQueryNotExecutable(Uri uri,
            String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        try {
            try (Cursor c = mResolver.query(uri, projection, selection,
                    selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            // pass.
            return;
        }
        addFailure("Query on " + uri + " expected to fail, but succeeded.", null);
    }

    /**
     * Make sure all URLs are accessible with all arguments = null.
     */
    public void testSelect() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            try {
                final Uri uri = getUri(path);

                checkQueryExecutable(uri, // uri
                        null, // projection
                        null, // selection
                        null, // selection args
                        null // sort order
                        );
            } catch (Throwable th) {
                addFailure("Failed: URI=" + path[0] + " Message=" + th.getMessage(), th);
            }
        }
        failIfFailed();
    }

    public void testNoHiddenColumns() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            try {
                final Uri uri = getUri(path);

                for (String column : getColumns(uri)) {
                    if (column.toLowerCase().startsWith(ContactsContract.HIDDEN_COLUMN_PREFIX)) {
                        addFailure("Uri " + uri + " returned hidden column " + column, null);
                    }
                }
            } catch (Throwable th) {
                addFailure("Failed: URI=" + path[0] + " Message=" + th.getMessage(), th);
            }
        }
        failIfFailed();
    }

// Temporarily disabled due to taking too much time.
//    /**
//     * Make sure all URLs are accessible with a projection.
//     */
//    public void testSelectWithProjection() {
//        for (String[] path : URIs) {
//            if (!supportsQuery(path)) continue;
//            final Uri uri = getUri(path);
//
//            for (String column : getColumns(uri)) {
//                // Some columns are not selectable alone due to bugs, and we don't want to fix them
//                // in order to avoid expanding the differences between versions, so here're some
//                // hacks to make it work...
//
//                String[] projection = {column};
//
//                final String u = path[0];
//                if ((u.startsWith("content://com.android.contacts/status_updates")
//                        || u.startsWith("content://com.android.contacts/profile/status_updates"))
//                        && ("im_handle".equals(column)
//                        || "im_account".equals(column)
//                        || "protocol".equals(column)
//                        || "custom_protocol".equals(column)
//                        || "presence_raw_contact_id".equals(column)
//                        )) {
//                    // These columns only show up when the projection contains certain columns.
//
//                    projection = new String[]{"mode", column};
//                } else if ((u.startsWith("content://com.android.contacts/search_suggest_query")
//                        || u.startsWith("content://contacts/search_suggest_query"))
//                        && "suggest_intent_action".equals(column)) {
//                    // Can't be included in the projection due to a bug in GlobalSearchSupport.
//                    continue;
//                } else if (RawContacts.BACKUP_ID.equals(column)) {
//                    // Some URIs don't support a projection with BAKCUP_ID only.
//                    projection = new String[]{RawContacts.BACKUP_ID, RawContacts.SOURCE_ID};
//                }
//
//                checkQueryExecutable(uri,
//                        projection, // projection
//                        null, // selection
//                        null, // selection args
//                        null // sort order
//                );
//            }
//        }
//        failIfFailed();
//    }

    /**
     * Make sure all URLs are accessible with a selection.
     */
    public void testSelectWithSelection() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            checkQueryExecutable(uri,
                    null, // projection
                    "1=?", // selection
                    ARG1, // , // selection args
                    null // sort order
            );
        }
        failIfFailed();
    }

//    /**
//     * Make sure all URLs are accessible with a selection.
//     */
//    public void testSelectWithSelectionUsingColumns() {
//        for (String[] path : URIs) {
//            if (!supportsQuery(path)) continue;
//            final Uri uri = getUri(path);
//
//            for (String column : getColumns(uri)) {
//                checkQueryExecutable(uri,
//                        null, // projection
//                        column + "=?", // selection
//                        ARG1, // , //  selection args
//                        null // sort order
//                );
//            }
//        }
//        failIfFailed();
//    }

// Temporarily disabled due to taking too much time.
//    /**
//     * Make sure all URLs are accessible with an order-by.
//     */
//    public void testSelectWithSortOrder() {
//        for (String[] path : URIs) {
//            if (!supportsQuery(path)) continue;
//            final Uri uri = getUri(path);
//
//            for (String column : getColumns(uri)) {
//                checkQueryExecutable(uri,
//                        null, // projection
//                        "1=2", // selection
//                        null, // , // selection args
//                        column // sort order
//                );
//            }
//        }
//        failIfFailed();
//    }

    /**
     * Make sure all URLs are accessible with all arguments.
     */
    public void testSelectWithAllArgs() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            final String[] projection = {getColumns(uri)[0]};

            checkQueryExecutable(uri,
                    projection, // projection
                    "1=?", // selection
                    ARG1, // , // selection args
                    getColumns(uri)[0] // sort order
            );
        }
        failIfFailed();
    }

    public void testNonSelect() {
        for (String[] path : URIs) {
            if (supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            checkQueryNotExecutable(uri, // uri
                    null, // projection
                    null, // selection
                    null, // selection args
                    null // sort order
            );
        }
        failIfFailed();
    }

    private static boolean supportsTimesContacted(String[] path) {
        return path.length > 1 && path[1].contains("s");
    }

    private static boolean supportsTimesUsed(String[] path) {
        return path.length > 1 && path[1].contains("t");
    }

    private void checkColumnAccessible(Uri uri, String column) {
        try {
            try (Cursor c = mResolver.query(
                    uri, new String[]{column}, column + "=0", null, column
            )) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query failed: URI=" + uri + " Message=" + th.getMessage(), th);
        }
    }

    /** Test for {@link #checkColumnAccessible} */
    public void testCheckColumnAccessible() {
        checkColumnAccessible(Contacts.CONTENT_URI, "x_times_contacted");
        try {
            failIfFailed();
        } catch (AssertionFailedError expected) {
            return; // expected.
        }
        fail("Failed to detect issue.");
    }

    private void checkColumnNotAccessibleInner(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        try {
            try (Cursor c = mResolver.query(uri, projection, selection,
                    selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (IllegalArgumentException th) {
            // pass.
            return;
        }
        addFailure("Query on " + uri +
                " expected to throw IllegalArgumentException, but succeeded.", null);
    }

    private void checkColumnNotAccessible(Uri uri, String column) {
        checkColumnNotAccessibleInner(uri, new String[] {column}, null, null, null);
        checkColumnNotAccessibleInner(uri, null, column + "=1", null, null);
        checkColumnNotAccessibleInner(uri, null, null, null, /* order by */ column);
    }

    /** Test for {@link #checkColumnNotAccessible} */
    public void testCheckColumnNotAccessible() {
        checkColumnNotAccessible(Contacts.CONTENT_URI, "times_contacted");
        try {
            failIfFailed();
        } catch (AssertionFailedError expected) {
            return; // expected.
        }
        fail("Failed to detect issue.");
    }

    /**
     * Make sure the x_ columns are not accessible.
     */
    public void testProhibitedColumns() {
        for (String[] path : URIs) {
            final Uri uri = getUri(path);
            if (supportsTimesContacted(path)) {
                checkColumnAccessible(uri, "times_contacted");
                checkColumnAccessible(uri, "last_time_contacted");

                checkColumnNotAccessible(uri, "X_times_contacted");
                checkColumnNotAccessible(uri, "X_slast_time_contacted");
            }
            if (supportsTimesUsed(path)) {
                checkColumnAccessible(uri, "times_used");
                checkColumnAccessible(uri, "last_time_used");

                checkColumnNotAccessible(uri, "X_times_used");
                checkColumnNotAccessible(uri, "X_last_time_used");
            }
        }
        failIfFailed();
    }

    private void checkExecutable(String operation, Uri uri, boolean shouldWork, Runnable r) {
        if (shouldWork) {
            try {
                r.run();
            } catch (Exception e) {
                addFailure(operation + " for '" + uri + "' failed: " + e.getMessage(), e);
            }
        } else {
            try {
                r.run();
                addFailure(operation + " for '" + uri + "' NOT failed.", null);
            } catch (Exception expected) {
            }
        }
    }

    public void testAllOperations() {
        final ContentValues cv = new ContentValues();

        for (String[] path : URIs) {
            final Uri uri = getUri(path);

            try {
                cv.clear();
                if (supportsQuery(path)) {
                    cv.put(getColumns(uri)[0], 1);
                } else {
                    cv.put("_id", 1);
                }
                if (uri.toString().contains("syncstate")) {
                    cv.put(SyncState.ACCOUNT_NAME, "abc");
                    cv.put(SyncState.ACCOUNT_TYPE, "def");
                }

                checkExecutable("insert", uri, supportsInsert(path), () -> {
                    final Uri newUri = mResolver.insert(uri, cv);
                    if (newUri == null) {
                        addFailure("Insert for '" + uri + "' returned null.", null);
                    } else {
                        // "profile/raw_contacts/#" is missing update support.  too late to add, so
                        // just skip.
                        if (!newUri.toString().startsWith(
                                "content://com.android.contacts/profile/raw_contacts/")) {
                            checkExecutable("insert -> update", newUri, true, () -> {
                                mResolver.update(newUri, cv, null, null);
                            });
                        }
                        checkExecutable("insert -> delete", newUri, true, () -> {
                            mResolver.delete(newUri, null, null);
                        });
                    }
                });
                checkExecutable("update", uri, supportsUpdate(path), () -> {
                    mResolver.update(uri, cv, "1=2", null);
                });
                checkExecutable("delete", uri, supportsDelete(path), () -> {
                    mResolver.delete(uri, "1=2", null);
                });
            } catch (Throwable th) {
                addFailure("Failed: URI=" + uri + " Message=" + th.getMessage(), th);
            }
        }
        failIfFailed();
    }

    public void testAllFileOperations() {
        for (String[] path : URIs) {
            final Uri uri = getUri(path);

            checkExecutable("openInputStream", uri, supportsRead(path), () -> {
                try (InputStream st = mResolver.openInputStream(uri)) {
                } catch (FileNotFoundException e) {
                    // TODO This happens because we try to read nonexistent photos.  Ideally
                    // we should actually check it's readable.
                    if (e.getMessage().contains("Stream I/O not supported")) {
                        throw new RuntimeException("Caught Exception: " + e.toString(), e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Caught Exception: " + e.toString(), e);
                }
            });
            checkExecutable("openOutputStream", uri, supportsWrite(path), () -> {
                try (OutputStream st = mResolver.openOutputStream(uri)) {
                } catch (Exception e) {
                    throw new RuntimeException("Caught Exception: " + e.toString(), e);
                }
            });
        }
        failIfFailed();
    }
}


