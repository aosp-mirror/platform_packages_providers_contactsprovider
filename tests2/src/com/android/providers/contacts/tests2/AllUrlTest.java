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

import android.database.Cursor;
import android.net.Uri;
import android.os.CancellationSignal;
import android.provider.ContactsContract;
import android.provider.ContactsContract.RawContacts;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

import java.util.ArrayList;

/**
 * TODO Add test for delete/update/insert too.
 * TODO Copy it to CTS
 */
@LargeTest
public class AllUrlTest extends AndroidTestCase {
    private static final String TAG = "AllUrlTest";

    // "-" : Query not supported.
    // "!" : Can't query because it requires the cross-user permission.
    // The following markers are planned, but not implemented and the definition below is not all
    // correct yet.
    // "d" : supports delete.
    // "u" : supports delete.
    // "i" : supports insert.
    // "r" : supports read.
    // "w" : supports write.
    private static final String[][] URIs = {
            {"content://com.android.contacts/contacts"},
            {"content://com.android.contacts/contacts/1"},
            {"content://com.android.contacts/contacts/1/data"},
            {"content://com.android.contacts/contacts/1/entities"},
            {"content://com.android.contacts/contacts/1/suggestions"},
            {"content://com.android.contacts/contacts/1/suggestions/XXX"},
            {"content://com.android.contacts/contacts/1/photo"},
            {"content://com.android.contacts/contacts/1/display_photo", "-rw"},
            {"content://com.android.contacts/contacts_corp/1/photo", "-rw"},
            {"content://com.android.contacts/contacts_corp/1/display_photo", "-rw"},
            {"content://com.android.contacts/contacts/1/stream_items"},
            {"content://com.android.contacts/contacts/filter"},
            {"content://com.android.contacts/contacts/filter/XXX"},
            {"content://com.android.contacts/contacts/lookup/nlookup"},
            {"content://com.android.contacts/contacts/lookup/nlookup/data"},
            {"content://com.android.contacts/contacts/lookup/nlookup/photo"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/data"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/photo"},
            {"content://com.android.contacts/contacts/lookup/nlookup/display_photo", "-rw"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/display_photo", "-rw"},
            {"content://com.android.contacts/contacts/lookup/nlookup/entities"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/entities"},
            {"content://com.android.contacts/contacts/lookup/nlookup/stream_items"},
            {"content://com.android.contacts/contacts/lookup/nlookup/1/stream_items"},
            {"content://com.android.contacts/contacts/as_vcard/nlookup"},
            {"content://com.android.contacts/contacts/as_multi_vcard/XXX"},
            {"content://com.android.contacts/contacts/strequent/"},
            {"content://com.android.contacts/contacts/strequent/filter/XXX"},
            {"content://com.android.contacts/contacts/group/XXX"},
            {"content://com.android.contacts/contacts/frequent"},
            {"content://com.android.contacts/contacts/delete_usage", "-d"},
            {"content://com.android.contacts/contacts/filter_enterprise?directory=0"},
            {"content://com.android.contacts/contacts/filter_enterprise/XXX?directory=0"},
            {"content://com.android.contacts/raw_contacts"},
            {"content://com.android.contacts/raw_contacts/1"},
            {"content://com.android.contacts/raw_contacts/1/data"},
            {"content://com.android.contacts/raw_contacts/1/display_photo", "-rw"},
            {"content://com.android.contacts/raw_contacts/1/entity"},
            {"content://com.android.contacts/raw_contacts/1/stream_items"},
            {"content://com.android.contacts/raw_contacts/1/stream_items/1"},
            {"content://com.android.contacts/raw_contact_entities"},
            {"content://com.android.contacts/raw_contact_entities_corp", "!"},
            {"content://com.android.contacts/data"},
            {"content://com.android.contacts/data/1"},
            {"content://com.android.contacts/data/phones"},
            {"content://com.android.contacts/data_enterprise/phones", "!"},
            {"content://com.android.contacts/data/phones/1"},
            {"content://com.android.contacts/data/phones/filter"},
            {"content://com.android.contacts/data/phones/filter/XXX"},
            {"content://com.android.contacts/data/phones/filter_enterprise?directory=0"},
            {"content://com.android.contacts/data/phones/filter_enterprise/XXX?directory=0"},
            {"content://com.android.contacts/data/emails"},
            {"content://com.android.contacts/data/emails/1"},
            {"content://com.android.contacts/data/emails/lookup"},
            {"content://com.android.contacts/data/emails/lookup/XXX"},
            {"content://com.android.contacts/data/emails/filter"},
            {"content://com.android.contacts/data/emails/filter/XXX"},
            {"content://com.android.contacts/data/emails/filter_enterprise?directory=0"},
            {"content://com.android.contacts/data/emails/filter_enterprise/XXX?directory=0"},
            {"content://com.android.contacts/data/emails/lookup_enterprise"},
            {"content://com.android.contacts/data/emails/lookup_enterprise/XXX"},
            {"content://com.android.contacts/data/postals"},
            {"content://com.android.contacts/data/postals/1"},
            {"content://com.android.contacts/data/usagefeedback/XXX", "-u"},
            {"content://com.android.contacts/data/callables/"},
            {"content://com.android.contacts/data/callables/1"},
            {"content://com.android.contacts/data/callables/filter"},
            {"content://com.android.contacts/data/callables/filter/XXX"},
            {"content://com.android.contacts/data/callables/filter_enterprise?directory=0"},
            {"content://com.android.contacts/data/callables/filter_enterprise/XXX?directory=0"},
            {"content://com.android.contacts/data/contactables/"},
            {"content://com.android.contacts/data/contactables/filter"},
            {"content://com.android.contacts/data/contactables/filter/XXX"},
            {"content://com.android.contacts/groups"},
            {"content://com.android.contacts/groups/1"},
            {"content://com.android.contacts/groups_summary"},
            {"content://com.android.contacts/syncstate"},
            {"content://com.android.contacts/syncstate/1", "-du"},
            {"content://com.android.contacts/profile/syncstate"},
            {"content://com.android.contacts/phone_lookup/XXX"},
            {"content://com.android.contacts/phone_lookup_enterprise/XXX"},
            {"content://com.android.contacts/aggregation_exceptions"},
            {"content://com.android.contacts/settings"},
            {"content://com.android.contacts/status_updates"},
            {"content://com.android.contacts/status_updates/1"},
            {"content://com.android.contacts/search_suggest_query"},
            {"content://com.android.contacts/search_suggest_query/XXX"},
            {"content://com.android.contacts/search_suggest_shortcut/XXX"},
            {"content://com.android.contacts/provider_status"},
            {"content://com.android.contacts/directories"},
            {"content://com.android.contacts/directories/1"},
            {"content://com.android.contacts/directories_enterprise"},
            {"content://com.android.contacts/directories_enterprise/1"},
            {"content://com.android.contacts/complete_name"},
            {"content://com.android.contacts/profile"},
            {"content://com.android.contacts/profile/entities"},
            {"content://com.android.contacts/profile/data"},
            {"content://com.android.contacts/profile/data/1"},
            {"content://com.android.contacts/profile/photo"},
            {"content://com.android.contacts/profile/display_photo", "-rw"},
            {"content://com.android.contacts/profile/as_vcard"},
            {"content://com.android.contacts/profile/raw_contacts"},
            {"content://com.android.contacts/profile/raw_contacts/1"},
            {"content://com.android.contacts/profile/raw_contacts/1/data"},
            {"content://com.android.contacts/profile/raw_contacts/1/entity"},
            {"content://com.android.contacts/profile/status_updates"},
            {"content://com.android.contacts/profile/raw_contact_entities"},
            {"content://com.android.contacts/stream_items"},
            {"content://com.android.contacts/stream_items/photo"},
            {"content://com.android.contacts/stream_items/1"},
            {"content://com.android.contacts/stream_items/1/photo"},
            {"content://com.android.contacts/stream_items/1/photo/1"},
            {"content://com.android.contacts/stream_items_limit"},
            {"content://com.android.contacts/display_photo/1", "-rw"},
            {"content://com.android.contacts/photo_dimensions"},
            {"content://com.android.contacts/deleted_contacts"},
            {"content://com.android.contacts/deleted_contacts/1"},
            {"content://com.android.contacts/directory_file_enterprise/XXX", "-rw"},

            {"content://contacts/extensions"},
            {"content://contacts/extensions/1"},
            {"content://contacts/groups"},
            {"content://contacts/groups/1"},
            {"content://contacts/groups/name/XXX/members"},
            {"content://contacts/groups/system_id/XXX/members"},
            {"content://contacts/groupmembership"},
            {"content://contacts/groupmembership/1"},
            {"content://contacts/people"},
            {"content://contacts/people/filter/XXX"},
            {"content://contacts/people/1"},
            {"content://contacts/people/1/extensions"},
            {"content://contacts/people/1/extensions/1"},
            {"content://contacts/people/1/phones"},
            {"content://contacts/people/1/phones/1"},
            {"content://contacts/people/1/photo"},
            {"content://contacts/people/1/contact_methods"},
            {"content://contacts/people/1/contact_methods/1"},
            {"content://contacts/people/1/organizations"},
            {"content://contacts/people/1/organizations/1"},
            {"content://contacts/people/1/groupmembership"},
            {"content://contacts/people/1/groupmembership/1"},
            {"content://contacts/people/1/update_contact_time", "-u"},
            {"content://contacts/deleted_people", "-"},
            {"content://contacts/deleted_groups", "-"},
            {"content://contacts/phones"},
            {"content://contacts/phones/filter/XXX"},
            {"content://contacts/phones/1"},
            {"content://contacts/photos"},
            {"content://contacts/photos/1"},
            {"content://contacts/contact_methods"},
            {"content://contacts/contact_methods/email"},
            {"content://contacts/contact_methods/1"},
            {"content://contacts/organizations"},
            {"content://contacts/organizations/1"},
            {"content://contacts/search_suggest_query"},
            {"content://contacts/search_suggest_query/XXX"},
            {"content://contacts/search_suggest_shortcut/XXX"},
            {"content://contacts/settings"},
    };

    private static final String[] ARG1 = {"-1"};


    private ArrayList<String> mFailures;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mFailures = new ArrayList<>();
    }

    @Override
    protected void tearDown() throws Exception {
        if (mFailures != null) {
            fail("mFailures is not null.  Did you forget to call failIfFailed()?");
        }

        super.tearDown();
    }

    private void addFailure(String message) {
        mFailures.add(message);
        Log.e(TAG, "Failed: " + message);
        if (mFailures.size() > 100) {
            // Too many failures...
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
            fail("Following test(s) failed:\n" + message);
        }
        mFailures = null;
    }

    private static boolean supportsQuery(String[] path) {
        if (path.length == 1) {
            return true; // supports query by default.
        }
        return !(path[1].contains("-") || path[1].contains("!"));
    }

    private static Uri getUri(String[] path) {
        return Uri.parse(path[0]);
    }

    private String[] getColumns(Uri uri) {
        try (Cursor c = getContext().getContentResolver().query(uri,
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
            try (Cursor c = getContext().getContentResolver().query(uri, projection, selection,
                    selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query failed: URI=" + uri + " Message=" + th.getMessage());
        }
        try {
            // With CancellationSignal.
            try (Cursor c = getContext().getContentResolver().query(uri, projection, selection,
                    selectionArgs, sortOrder, new CancellationSignal())) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query with cancel failed: URI=" + uri + " Message=" + th.getMessage());
        }
        try {
            // With limit.
            try (Cursor c = getContext().getContentResolver().query(
                    uri.buildUpon().appendQueryParameter(
                            ContactsContract.LIMIT_PARAM_KEY, "0").build(),
                    projection, selection, selectionArgs, sortOrder)) {
                c.moveToFirst();
            }
        } catch (Throwable th) {
            addFailure("Query with limit failed: URI=" + uri + " Message=" + th.getMessage());
        }
    }

    private void checkQueryNotExecutable(Uri uri,
            String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        try {
            try (Cursor c = getContext().getContentResolver().query(uri, projection, selection,
                    selectionArgs, sortOrder)) {
            }
        } catch (Throwable th) {
            // pass.
            return;
        }
        addFailure("Query on " + uri + " expeced to fail, but succeeded.");
    }

    /**
     * Make sure all URLs are accessible with all arguments = null.
     */
    public void testSelect() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            checkQueryExecutable(uri, // uri
                    null, // projection
                    null, // selection
                    null, // selection args
                    null // sort order
                    );
        }
        failIfFailed();
    }

    public void testNoHiddenColumns() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            for (String column : getColumns(uri)) {
                if (column.toLowerCase().startsWith(ContactsContract.HIDDEN_COLUMN_PREFIX)) {  // doesn't seem to be working
                    addFailure("Uri " + uri + " returned hidden column " + column);
                }
            }
        }
        failIfFailed();
    }

    /**
     * Make sure all URLs are accessible with a projection.
     */
    public void testSelectWithProjection() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            for (String column : getColumns(uri)) {
                // Some columns are not selectable alone due to bugs, and we don't want to fix them
                // in order to avoid expanding the differences between versions, so here're some
                // hacks to make it work...

                String[] projection = {column};

                final String u = path[0];
                if ((u.startsWith("content://com.android.contacts/status_updates")
                        || u.startsWith("content://com.android.contacts/profile/status_updates"))
                        && ("im_handle".equals(column)
                        || "im_account".equals(column)
                        || "protocol".equals(column)
                        || "custom_protocol".equals(column)
                        || "presence_raw_contact_id".equals(column)
                        )) {
                    // These columns only show up when the projection contains certain columns.

                    projection = new String[]{"mode", column};
                } else if ((u.startsWith("content://com.android.contacts/search_suggest_query")
                        || u.startsWith("content://contacts/search_suggest_query"))
                        && "suggest_intent_action".equals(column)) {
                    // Can't be included in the projection due to a bug in GlobalSearchSupport.
                    continue;
                } else if (RawContacts.BACKUP_ID.equals(column)) {
                    // Some URIs don't support a projection with BAKCUP_ID only.
                    projection = new String[]{RawContacts.BACKUP_ID, RawContacts.SOURCE_ID};
                }

                checkQueryExecutable(uri,
                        projection, // projection
                        null, // selection
                        null, // selection args
                        null // sort order
                );
            }
        }
        failIfFailed();
    }

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
//                        ARG1, // , // selection args
//                        null // sort order
//                );
//            }
//        }
//        failIfFailed();
//    }

    /**
     * Make sure all URLs are accessible with an order-by.
     */
    public void testSelectWithSortOrder() {
        for (String[] path : URIs) {
            if (!supportsQuery(path)) continue;
            final Uri uri = getUri(path);

            for (String column : getColumns(uri)) {
                checkQueryExecutable(uri,
                        null, // projection
                        "1=2", // selection
                        null, // , // selection args
                        column // sort order
                );
            }
        }
        failIfFailed();
    }

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
}
