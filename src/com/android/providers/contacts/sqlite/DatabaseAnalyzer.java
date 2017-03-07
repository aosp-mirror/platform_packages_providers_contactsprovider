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

package com.android.providers.contacts.sqlite;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import com.android.providers.contacts.AbstractContactsProvider;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to extract table/view/column names from databases.
 */
@VisibleForTesting
public class DatabaseAnalyzer {
    private static final String TAG = "DatabaseAnalyzer";

    private static final boolean VERBOSE_LOGGING = AbstractContactsProvider.VERBOSE_LOGGING;

    private DatabaseAnalyzer() {
    }

    /**
     * Find and return all table/view names in a db.
     */
    private static List<String> findTablesAndViews(SQLiteDatabase db) {
        final List<String> ret = new ArrayList<>();
        try (final Cursor c = db.rawQuery(
                "SELECT name FROM sqlite_master WHERE type in (\"table\", \"view\")", null)) {
            while (c.moveToNext()) {
                ret.add(c.getString(0).toLowerCase());
            }
        }
        return ret;
    }

    /**
     * Find all columns in a table/view.
     */
    private static List<String> findColumns(SQLiteDatabase db, String table) {
        final List<String> ret = new ArrayList<>();

        // Open the table/view but requests 0 rows.
        final Cursor c = db.rawQuery("SELECT * FROM " + table + " WHERE 0 LIMIT 0", null);
        try {
            // Collect the column names.
            for (int i = 0; i < c.getColumnCount(); i++) {
                ret.add(c.getColumnName(i).toLowerCase());
            }
        } finally {
            c.close();
        }
        return ret;
    }

    /**
     * Return all table/view names that clients shouldn't use in their queries -- basically the
     * result contains all table/view names, except for the names that are column names of any
     * tables.
     */
    @VisibleForTesting
    public static List<String> findTableViewsAllowingColumns(SQLiteDatabase db) {
        final List<String> tables = findTablesAndViews(db);
        if (VERBOSE_LOGGING) {
            Log.d(TAG, "Tables and views:");
        }
        final List<String> ret = new ArrayList<>(tables); // Start with the table/view list.
        for (String name : tables) {
            if (VERBOSE_LOGGING) {
                Log.d(TAG, "  " + name);
            }
            final List<String> columns = findColumns(db, name);
            if (VERBOSE_LOGGING) {
                Log.d(TAG, "    Columns: " + columns);
            }
            for (String c : columns) {
                if (ret.remove(c)) {
                    Log.d(TAG, "Removing [" + c + "] from disallow list");
                }
            }
        }
        return ret;
    }
}
