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
package com.android.providers.contacts;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import junit.framework.AssertionFailedError;

import java.util.HashMap;

/**
 * Unit tests for database create/upgrade operations.
 *
 * Run the test like this: <code> runtest -c com.android.providers.contacts.BaseDatabaseHelperUpgradeTest
 * contactsprov </code>
 */
public abstract class BaseDatabaseHelperUpgradeTest extends FixedAndroidTestCase {

    protected static final String INTEGER = "INTEGER";
    protected static final String TEXT = "TEXT";
    protected static final String STRING = "STRING";
    protected static final String BLOB = "BLOB";

    protected SQLiteDatabase mDb;

    /**
     * The column info returned by PRAGMA table_info()
     */
    protected static class TableColumn {

        public int cid;
        public String name;
        public String type;
        public boolean notnull;
        // default value
        public String dflt_value;
        // primary key. Not tested.
        public int pk;

        public TableColumn() {

        }

        public TableColumn(String name, String type, boolean notnull, String defaultValue) {
            this.name = name;
            this.type = type;
            this.notnull = notnull;
            this.dflt_value = defaultValue;
        }
    }

    protected static class TableStructure {

        private final HashMap<String, TableColumn> mColumns = new HashMap<String, TableColumn>();
        private final String mName;

        public TableStructure(SQLiteDatabase db, String tableName) {
            mName = tableName;
            try (final Cursor cursor = db.rawQuery("PRAGMA table_info(" + tableName + ");", null)) {
                final int cidIndex = cursor.getColumnIndex("cid");
                final int nameIndex = cursor.getColumnIndex("name");
                final int typeIndex = cursor.getColumnIndex("type");
                final int notNullIndex = cursor.getColumnIndex("notnull");
                final int dfltValueIndex = cursor.getColumnIndex("dflt_value");
                final int pkIndex = cursor.getColumnIndex("pk");
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    TableColumn column = new TableColumn();
                    column.cid = cursor.getInt(cidIndex);
                    column.name = cursor.getString(nameIndex);
                    column.type = cursor.getString(typeIndex);
                    column.notnull = cursor.getInt(notNullIndex) != 0;
                    column.dflt_value = cursor.getString(dfltValueIndex);
                    column.pk = cursor.getInt(pkIndex);

                    addColumn(column);
                }
            }
        }

        private TableStructure() {
            mName = "";
        }

        private void addColumn(TableColumn column) {
            mColumns.put(column.name, column);
        }

        public void assertHasColumn(String name, String type, boolean notnull,
                String defaultValue) {
            final TableColumn column = mColumns.get(name);
            if (column == null) {
                throw new AssertionFailedError("Table " + mName + ": Column missing: " + name);
            }
            if (!type.equals(column.type)) {
                throw new AssertionFailedError("Table " + mName + ": Column " + name + " type:"
                        + column.type + ", " + type + " expected");
            }
            if (!notnull == column.notnull) {
                throw new AssertionFailedError("Table " + mName + ": Column " + name + " notnull:"
                        + column.notnull + ", " + notnull + " expected");
            }
            if (defaultValue == null) {
                if (column.dflt_value != null) {
                    throw new AssertionFailedError("Table " + mName + ": Column " + name
                            + " defaultValue: " + column.dflt_value + ", null expected");
                }
            } else if (!defaultValue.equals(column.dflt_value)) {
                throw new AssertionFailedError("Table " + mName + ": Column " + name
                        + " defaultValue:" + column.dflt_value + ", " + defaultValue + " expected");
            }
        }


        public void assertHasColumns(TableColumn[] columns) {
            for (final TableColumn column : columns) {
                assertHasColumn(column.name, column.type, column.notnull, column.dflt_value);
            }
        }

        /**
         * Assert the TableStructure has every column in @param columns, and nothing else.
         */
        public void assertSame(TableColumn[] columns) {
            assertHasColumns(columns);
            if (columns.length != mColumns.size()) {
                throw new RuntimeException("column count mismatch");
            }
        }

    }

    /**
     * Used to store a tables' name and its' current structure in a array.
     */
    protected static class TableListEntry {

        public final String name;
        public final TableColumn[] columns;
        public final boolean shouldBeInNewDb;

        public TableListEntry(String name, TableColumn[] columns) {
            this(name, columns, /* shouldBeInNewDb = */ true);
        }

        public TableListEntry(String name, TableColumn[] columns, boolean shouldBeInNewDb) {
            this.name = name;
            this.columns = columns;
            this.shouldBeInNewDb = shouldBeInNewDb;
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        final String filename = getDatabaseFilename();
        if (filename == null) {
            mDb = SQLiteDatabase.create(null);
        } else {
            getContext().deleteDatabase(filename);
            mDb = SQLiteDatabase.openOrCreateDatabase(filename, null);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        mDb.close();
        super.tearDown();
    }

    protected abstract String getDatabaseFilename();

    protected void assertDatabaseStructureSameAsList(TableListEntry[] list, boolean isNewDatabase) {
        for (TableListEntry entry : list) {
            if (!entry.shouldBeInNewDb) {
                if (isNewDatabase) {
                    continue;
                }
            }
            TableStructure structure = new TableStructure(mDb, entry.name);
            structure.assertSame(entry.columns);
        }
    }

    public void testAssertHasColumn_Match() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, null);
        table.assertHasColumn("foo", INTEGER, false, null);
    }

    public void testAssertHasColumn_Empty() {
        TableStructure table = new TableStructure();

        try {
            table.assertHasColumn("bar", INTEGER, false, null);
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_ColumnNotExist() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, null);

        try {
            table.assertHasColumn("bar", INTEGER, false, null);
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_TypeMismatch() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, null);

        try {
            table.assertHasColumn("foo", TEXT, false, null);
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_NotNullMismatch() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, null);

        try {
            table.assertHasColumn("foo", INTEGER, true, null);
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_DefaultMatch() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, "baz");
        table.assertHasColumn("foo", INTEGER, false, "baz");
    }

    public void testAssertHasColumn_DefaultMismatch() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, "bar");

        try {
            table.assertHasColumn("foo", INTEGER, false, "baz");
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_DefaultMismatch_Null1() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, null);

        try {
            table.assertHasColumn("foo", INTEGER, false, "baz");
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    public void testAssertHasColumn_DefaultMismatch_Null2() {
        TableStructure table = createOneColumnTable("foo", INTEGER, false, "baz");

        try {
            table.assertHasColumn("foo", INTEGER, false, null);
            throw new AssertionError("Assert should fail");
        } catch (AssertionFailedError e) {
            // Should fail
        }
    }

    private TableStructure createOneColumnTable(String name, String type, boolean notnull,
            String defaultValue) {
        TableStructure table = new TableStructure();
        table.addColumn(new TableColumn(name, type, notnull, defaultValue));
        return table;
    }

}
