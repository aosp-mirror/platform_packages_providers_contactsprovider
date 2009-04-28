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
import com.android.providers.contacts2.ContactsContract.Data;

import android.app.ListActivity;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.View;
import android.widget.ResourceCursorAdapter;
import android.widget.TextView;

/**
 * Simple test activity to display the data entries for a contact in the contacts2 provider.
 */
public class ContactDetails2 extends ListActivity {

    static final String[] PROJECTION = new String[] {
        Data.PACKAGE, // 0
        Data.MIMETYPE, // 1
        Data.DATA1, // 2
        Data.DATA2, // 3
        Data.DATA3, // 4
        Data.DATA4, // 5
        Data.DATA5, // 6
        Data.DATA6, // 7
        Data.DATA7, // 8
        Data.DATA8, // 9
        Data.DATA9, // 10
        Data.DATA10, // 11
        Data._ID,
    };
    static final int COLUMN_INDEX_PACKAGE = 0;
    static final int COLUMN_INDEX_MIMETYPE = 1;
    static final int COLUMN_INDEX_DATA1 = 2;
    static final int COLUMN_INDEX_DATA2 = 3;
    static final int COLUMN_INDEX_DATA3 = 4;
    static final int COLUMN_INDEX_DATA4 = 5;
    static final int COLUMN_INDEX_DATA5 = 6;
    static final int COLUMN_INDEX_DATA6 = 7;
    static final int COLUMN_INDEX_DATA7 = 8;
    static final int COLUMN_INDEX_DATA8 = 9;
    static final int COLUMN_INDEX_DATA9 = 10;
    static final int COLUMN_INDEX_DATA10 = 11;

    DetailsAdapter mAdapter;

    /**
     * Simple task for doing an async query.
     */
    final class QueryTask extends AsyncTask<Uri, Void, Cursor> {
        @Override
        protected Cursor doInBackground(Uri... params) {
            final Cursor c = getContentResolver().query(params[0], PROJECTION, null,
                    null, null);
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

    /**
     * Simple list adapter to display the data rows.
     */
    final class DetailsAdapter extends ResourceCursorAdapter {
        public DetailsAdapter() {
            super(ContactDetails2.this, android.R.layout.simple_list_item_1, null);
        }

        @Override
        public void bindView(View view, Context context, Cursor cursor) {
            final StringBuilder text = new StringBuilder();

            text.append("Package: ");
            text.append(cursor.getString(COLUMN_INDEX_PACKAGE));
            text.append("\nMime-type: ");
            text.append(cursor.getLong(COLUMN_INDEX_MIMETYPE));
            text.append("\nData1: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA1));
            text.append("\nData2: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA2));
            text.append("\nData3: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA3));
            text.append("\nData4: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA4));
            text.append("\nData5: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA5));
            text.append("\nData6: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA6));
            text.append("\nData7: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA7));
            text.append("\nData8: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA8));
            text.append("\nData9: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA9));
            text.append("\nData10: ");
            text.append(cursor.getString(COLUMN_INDEX_DATA10));

            ((TextView) view).setText(text);
        }
    }

    @Override
    public void onCreate(Bundle savedState) {
        super.onCreate(savedState);

        mAdapter = new DetailsAdapter();
        setListAdapter(mAdapter);
    }

    @Override
    public void onResume() {
        super.onResume();

        new QueryTask().execute(Uri.withAppendedPath(getIntent().getData(),
                Contacts.Data.CONTENT_DIRECTORY));
    }
}
