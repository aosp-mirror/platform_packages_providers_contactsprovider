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

package com.android.contacts2;

import com.android.providers.contacts2.ContactsContract.CommonDataKinds;
import com.android.providers.contacts2.ContactsContract.Contacts;
import com.android.providers.contacts2.ContactsContract.PhoneLookup;
import com.android.providers.contacts2.ContactsContract.CommonDataKinds.Phone;
import com.android.providers.contacts2.ContactsContract.Contacts.Data;

import android.app.ListActivity;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Intent;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.ListView;
import android.widget.SimpleCursorAdapter;

public class Contacts2 extends ListActivity {
    static final String[] PROJECTION = {
        Contacts._ID, // 0
        Contacts.DISPLAY_NAME, // 1
    };
    static final int COLUMN_INDEX_ID = 0;
    static final int COLUMN_INDEX_DISPLAY_NAME = 1;

    SimpleCursorAdapter mAdapter;

    public class QueryTask extends AsyncTask<Void, Void, Cursor> {
        @Override
        protected Cursor doInBackground(Void... params) {
            final Cursor c = getContentResolver().query(Contacts.CONTENT_URI, PROJECTION, null,
                    null, Contacts.DISPLAY_NAME + " ASC");
            if (c != null) {
                c.getCount();
            }
            return c;
        }

        @Override
        protected void onPostExecute(Cursor c) {
            if (isFinishing()) {
                if (c != null) {
                    c.close();
                }
                return;
            }

            mAdapter.changeCursor(c);
        }
    }

    @Override
    public void onCreate(Bundle savedState) {
        super.onCreate(savedState);

        mAdapter = new SimpleCursorAdapter(this, android.R.layout.simple_list_item_1, null,
                new String[] { Contacts.DISPLAY_NAME }, new int[] { android.R.id.text1 });
        setListAdapter(mAdapter);
    }

    @Override
    public void onResume() {
        super.onResume();

        new QueryTask().execute((Void[]) null);
    }

    @Override
    protected void onListItemClick(ListView l, View v, int position, long id) {
        final Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, id);
        final Intent intent = new Intent(Intent.ACTION_VIEW, uri);
        intent.setClass(this, ContactDetails2.class);
        startActivity(intent);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        menu.add(0, 42, 0, "Add data");
        menu.add(0, 43, 0, "Phone lookup");
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case 42: {
                final ContentResolver resolver = getContentResolver();
                final ContentValues values = new ContentValues();
                values.put(Contacts.GIVEN_NAME, "Bob");
                values.put(Contacts.FAMILY_NAME, "Smith");
                final Uri contactUri = resolver.insert(Contacts.CONTENT_URI, values);

                final Uri dataUri = Uri.withAppendedPath(contactUri, Data.CONTENT_DIRECTORY);
                values.clear();
                values.put(Phone.PACKAGE, CommonDataKinds.PACKAGE_COMMON);
                values.put(Phone.KIND, Phone.KIND_PHONE);
                values.put(Phone.NUMBER, "512-555-1212");
                values.put(Phone.TYPE, Phone.TYPE_MOBILE);
                resolver.insert(dataUri, values);

                return true;
            }

            case 43: {
                final ContentResolver resolver = getContentResolver();
                final Uri uri = Uri.withAppendedPath(PhoneLookup.CONTENT_URI, "555-1212");
                final Cursor c = resolver.query(uri, null, null, null, null);
                final StringBuilder sb = new StringBuilder();
                DatabaseUtils.dumpCursor(c, sb);
                Log.i("!!!!!!!!", sb.toString());
            }
        }
        return false;
    }
}
