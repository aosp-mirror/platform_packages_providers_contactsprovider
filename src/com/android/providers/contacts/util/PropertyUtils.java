/*
 * Copyright (C) 2015 The Android Open Source Project
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
package com.android.providers.contacts.util;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

/**
 * Utilities related to "database properties", which are similar to shard preferences, but
 * are transaction-aware.
 */
public class PropertyUtils {
    private PropertyUtils() {
    }

    public interface Tables {
        String PROPERTIES = "properties";
    }

    public interface PropertiesColumns {
        String PROPERTY_KEY = "property_key";
        String PROPERTY_VALUE = "property_value";
    }

    /**
     * Creates the properties table.
     */
    public static void createPropertiesTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + Tables.PROPERTIES + " (" +
                PropertiesColumns.PROPERTY_KEY + " TEXT PRIMARY KEY, " +
                PropertiesColumns.PROPERTY_VALUE + " TEXT " +
                ");");

    }

    /**
     * Gets a property.
     */
    public static String getProperty(SQLiteDatabase db, String key, String defaultValue) {
        final Cursor cursor = db.query(Tables.PROPERTIES,
                new String[] {PropertiesColumns.PROPERTY_VALUE},
                PropertiesColumns.PROPERTY_KEY + "=?",
                new String[] {key}, null, null, null);
        String value = null;
        try {
            if (cursor.moveToFirst()) {
                value = cursor.getString(0);
            }
        } finally {
            cursor.close();
        }

        return value != null ? value : defaultValue;
    }

    /**
     * Sets a property.
     */
    public static void setProperty(SQLiteDatabase db, String key, String value) {
        ContentValues values = new ContentValues();
        values.put(PropertiesColumns.PROPERTY_KEY, key);
        values.put(PropertiesColumns.PROPERTY_VALUE, value);
        db.replace(Tables.PROPERTIES, null, values);
    }
}
