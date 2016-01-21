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
 * limitations under the License.
 */

package com.android.providers.contacts;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.provider.ContactsContract.PhoneLookup;
import android.test.AndroidTestCase;
import android.test.MoreAsserts;
import android.test.suitebuilder.annotation.SmallTest;

import com.android.providers.contacts.enterprise.EnterpriseContactsCursorWrapper;


@SmallTest
public class EnterpriseContactsCursorWrapperTest extends AndroidTestCase {

    public void testWrappedResults() {
        // 19 columns
        final String[] projection = new String[] {
                /* column 0 */ PhoneLookup._ID,
                /* column 1 */ PhoneLookup.LOOKUP_KEY,
                /* column 2 */ PhoneLookup.DISPLAY_NAME,
                /* column 3 */ PhoneLookup.LAST_TIME_CONTACTED,
                /* column 4 */ PhoneLookup.TIMES_CONTACTED,
                /* column 5 */ PhoneLookup.STARRED,
                /* column 6 */ PhoneLookup.IN_DEFAULT_DIRECTORY,
                /* column 7 */ PhoneLookup.IN_VISIBLE_GROUP,
                /* column 8 */ PhoneLookup.PHOTO_FILE_ID,
                /* column 9 */ PhoneLookup.PHOTO_ID,
                /* column 10 */ PhoneLookup.PHOTO_URI,
                /* column 11 */ PhoneLookup.PHOTO_THUMBNAIL_URI,
                /* column 12 */ PhoneLookup.CUSTOM_RINGTONE,
                /* column 13 */ PhoneLookup.HAS_PHONE_NUMBER,
                /* column 14 */ PhoneLookup.SEND_TO_VOICEMAIL,
                /* column 15 */ PhoneLookup.NUMBER,
                /* column 16 */ PhoneLookup.TYPE,
                /* column 17 */ PhoneLookup.LABEL,
                /* column 18 */ PhoneLookup.NORMALIZED_NUMBER
        };
        final MatrixCursor c = new MatrixCursor(projection);

        // First, convert and make sure it returns an empty cursor.
        Cursor rewritten = new EnterpriseContactsCursorWrapper(c, projection, 0, false, null);

        assertEquals(0, rewritten.getCount());
        assertEquals(19, rewritten.getColumnCount());

        c.addRow(new Object[] {
                1L, // PhoneLookup._ID,
                null, // PhoneLookup.LOOKUP_KEY,
                null, // PhoneLookup.DISPLAY_NAME,
                null, // PhoneLookup.LAST_TIME_CONTACTED,
                null, // PhoneLookup.TIMES_CONTACTED,
                null, // PhoneLookup.STARRED,
                null, // PhoneLookup.IN_DEFAULT_DIRECTORY,
                null, // PhoneLookup.IN_VISIBLE_GROUP,
                null, // PhoneLookup.PHOTO_FILE_ID,
                null, // PhoneLookup.PHOTO_ID,
                null, // PhoneLookup.PHOTO_URI,
                null, // PhoneLookup.PHOTO_THUMBNAIL_URI,
                null, // PhoneLookup.CUSTOM_RINGTONE,
                null, // PhoneLookup.HAS_PHONE_NUMBER,
                null, // PhoneLookup.SEND_TO_VOICEMAIL,
                null, // PhoneLookup.NUMBER,
                null, // PhoneLookup.TYPE,
                null, // PhoneLookup.LABEL,
                null, // PhoneLookup.NORMALIZED_NUMBER
        });

        c.addRow(new Object[] {
                10L, // PhoneLookup._ID,
                "key", // PhoneLookup.LOOKUP_KEY,
                "name", // PhoneLookup.DISPLAY_NAME,
                123, // PhoneLookup.LAST_TIME_CONTACTED,
                456, // PhoneLookup.TIMES_CONTACTED,
                1, // PhoneLookup.STARRED,
                1, // PhoneLookup.IN_DEFAULT_DIRECTORY,
                1, // PhoneLookup.IN_VISIBLE_GROUP,
                1001, // PhoneLookup.PHOTO_FILE_ID,
                1002, // PhoneLookup.PHOTO_ID,
                "content://a/a", // PhoneLookup.PHOTO_URI,
                "content://a/b", // PhoneLookup.PHOTO_THUMBNAIL_URI,
                "content://a/c", // PhoneLookup.CUSTOM_RINGTONE,
                1, // PhoneLookup.HAS_PHONE_NUMBER,
                1, // PhoneLookup.SEND_TO_VOICEMAIL,
                "1234", // PhoneLookup.NUMBER,
                1, // PhoneLookup.TYPE,
                "label", // PhoneLookup.LABEL,
                "+1234", // PhoneLookup.NORMALIZED_NUMBER
        });
        rewritten = new EnterpriseContactsCursorWrapper(c, projection, 0, false, null);
        assertEquals(2, rewritten.getCount());
        assertEquals(19, rewritten.getColumnCount());

        rewritten.moveToPosition(0);
        int column = 0;
        assertEquals(1000000001L, rewritten.getLong(column++)); // We offset ID for corp contacts.
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++));


        rewritten.moveToNext();
        column = 0;
        assertEquals(1000000010L, rewritten.getLong(column++)); // With offset.
        assertEquals("c-key", rewritten.getString(column++));
        assertEquals("name", rewritten.getString(column++));
        assertEquals(123, rewritten.getInt(column++));
        assertEquals(456, rewritten.getInt(column++));
        assertEquals(1, rewritten.getInt(column++));
        assertEquals(1, rewritten.getInt(column++));
        assertEquals(1, rewritten.getInt(column++));
        assertEquals(null, rewritten.getString(column++)); // photo file id
        assertEquals(null, rewritten.getString(column++)); // photo id
        assertEquals("content://com.android.contacts/contacts_corp/10/display_photo",
                rewritten.getString(column++));
        assertEquals("content://com.android.contacts/contacts_corp/10/photo",
                rewritten.getString(column++));
        assertEquals(null, rewritten.getString(column++)); // ringtone
        assertEquals(1, rewritten.getInt(column++));
        assertEquals(1, rewritten.getInt(column++));
        assertEquals("1234", rewritten.getString(column++));
        assertEquals(1, rewritten.getInt(column++));
        assertEquals("label", rewritten.getString(column++));
        assertEquals("+1234", rewritten.getString(column++));


        rewritten = new EnterpriseContactsCursorWrapper(c, projection, 0, false, null);

        assertEquals(2, rewritten.getCount());
        assertEquals(19, rewritten.getColumnCount());

        rewritten.moveToPosition(0);

        assertEquals(null, rewritten.getString(10));
        assertEquals(null, rewritten.getString(11));


        rewritten.moveToNext();
        column = 0;
        assertEquals("content://com.android.contacts/contacts_corp/10/display_photo",
                rewritten.getString(10));
        assertEquals("content://com.android.contacts/contacts_corp/10/photo",
                rewritten.getString(11));
    }
}

