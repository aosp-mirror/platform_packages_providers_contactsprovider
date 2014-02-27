/*
 * Copyright (C) 2014 The Android Open Source Project
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
package com.android.providers.contacts;

import com.android.internal.annotations.VisibleForTesting;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.provider.ContactsContract.PhoneLookup;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Log;

/**
 * Helper class for PHONE_LOOKUP's that involve numbers with "*" prefixes.
 */
/* package-protected */ final class PhoneLookupWithStarPrefix {
    private static final String TAG = "PhoneLookupWSP";

    /**
     * Returns a cursor with a subset of the rows passed into this function. If {@param number}
     * starts with a "*" then only rows from {@param cursor} that have a number equal to
     * {@param number} will be returned. If {@param number} doesn't start with a "*", then
     * only rows from {@param cursor} that have numbers without starting "*" characters
     * will be returned.
     *
     * This function is used to resolve b/13195334.
     *
     * @param number unnormalized phone number.
     * @param cursor this function takes ownership of the cursor. The calling scope MUST NOT
     * use or close() the cursor passed into this function. The cursor must contain
     * PhoneLookup.NUMBER.
     *
     * @return a cursor that the calling context owns
     */
    public static Cursor removeNonStarMatchesFromCursor(String number, Cursor cursor) {

        // Close cursors that we don't return.
        Cursor unreturnedCursor = cursor;

        try {
            if (TextUtils.isEmpty(number)) {
                unreturnedCursor = null;
                return cursor;
            }

            final String queryPhoneNumberNormalized = normalizeNumberWithStar(number);
            if (!queryPhoneNumberNormalized.startsWith("*")
                    && !matchingNumberStartsWithStar(cursor)) {
                cursor.moveToPosition(-1);
                unreturnedCursor = null;
                return cursor;
            }

            final MatrixCursor matrixCursor = new MatrixCursor(cursor.getColumnNames());

            // Close cursors that we don't return.
            Cursor unreturnedMatrixCursor = matrixCursor;

            try {
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    final int numberIndex = cursor.getColumnIndex(PhoneLookup.NUMBER);
                    final String matchingNumberNormalized
                            = normalizeNumberWithStar(cursor.getString(numberIndex));
                    if (!matchingNumberNormalized.startsWith("*")
                            && !queryPhoneNumberNormalized.startsWith("*")
                            || matchingNumberNormalized.equals(queryPhoneNumberNormalized)) {
                        // Copy row from cursor into matrixCursor
                        final MatrixCursor.RowBuilder b = matrixCursor.newRow();
                        for (int column = 0; column < cursor.getColumnCount(); column++) {
                            b.add(cursor.getColumnName(column), cursorValue(cursor, column));
                        }
                    }
                }
                unreturnedMatrixCursor = null;
                return matrixCursor;
            } finally {
                if (unreturnedMatrixCursor != null) {
                    unreturnedMatrixCursor.close();
                }
            }
        } finally {
            if (unreturnedCursor != null) {
                unreturnedCursor.close();
            }
        }
    }

    @VisibleForTesting
    static String normalizeNumberWithStar(String phoneNumber) {
        if (TextUtils.isEmpty(phoneNumber)) {
            return phoneNumber;
        }
        if (phoneNumber.startsWith("*")) {
            // Use PhoneNumberUtils.normalizeNumber() to normalize the rest of the number after
            // the leading "*". Strip out the "+" since "+"s are only allowed as leading
            // characters. NOTE: This statement has poor performance. Fortunately, it won't be
            // called very often.
            return "*" + PhoneNumberUtils.normalizeNumber(
                    phoneNumber.substring(1).replace("+", ""));
        }
        return PhoneNumberUtils.normalizeNumber(phoneNumber);
    }

    /**
     * @return whether {@param cursor} contain any numbers that start with "*"
     */
    private static boolean matchingNumberStartsWithStar(Cursor cursor) {
        cursor.moveToPosition(-1);
        while (cursor.moveToNext()) {
            final int numberIndex = cursor.getColumnIndex(PhoneLookup.NUMBER);
            final String phoneNumber = normalizeNumberWithStar(cursor.getString(numberIndex));
            if (phoneNumber.startsWith("*")) {
                return true;
            }
        }
        return false;
    }

    private static Object cursorValue(Cursor cursor, int column) {
        switch(cursor.getType(column)) {
            case Cursor.FIELD_TYPE_BLOB:
                return cursor.getBlob(column);
            case Cursor.FIELD_TYPE_INTEGER:
                return cursor.getInt(column);
            case Cursor.FIELD_TYPE_FLOAT:
                return cursor.getFloat(column);
            case Cursor.FIELD_TYPE_STRING:
                return cursor.getString(column);
            case Cursor.FIELD_TYPE_NULL:
                return null;
            default:
                Log.d(TAG, "Invalid value in cursor: " + cursor.getType(column));
                return null;
        }
    }
}
