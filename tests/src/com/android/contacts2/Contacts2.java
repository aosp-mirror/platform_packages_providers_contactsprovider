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

import com.android.providers.contacts2.ContactsContract.Contacts;

import android.app.ListActivity;
import android.database.Cursor;
import android.os.AsyncTask;
import android.os.Bundle;
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
}
