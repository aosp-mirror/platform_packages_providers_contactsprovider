/*
 * Copyright (C) 2011 The Android Open Source Project
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

import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentValues;
import android.content.Context;
import android.content.OperationApplicationException;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteTransactionListener;
import android.net.Uri;
import android.os.Binder;
import android.os.SystemClock;
import android.provider.BaseColumns;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.util.Log;

import com.android.internal.util.ProviderAccessStats;
import com.android.providers.contacts.ContactsDatabaseHelper.AccountsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.RawContactsColumns;
import com.android.providers.contacts.ContactsDatabaseHelper.Tables;

import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * A common base class for the contacts and profile providers.  This handles much of the same
 * logic that SQLiteContentProvider does (i.e. starting transactions on the appropriate database),
 * but exposes awareness of batch operations to the subclass so that cross-database operations
 * can be supported.
 */
public abstract class AbstractContactsProvider extends ContentProvider
        implements SQLiteTransactionListener {

    public static final String TAG = "ContactsProvider";

    public static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    /** Set true to enable detailed transaction logging. */
    public static final boolean ENABLE_TRANSACTION_LOG = false; // Don't submit with true.

    /**
     * Duration in ms to sleep after successfully yielding the lock during a batch operation.
     */
    protected static final int SLEEP_AFTER_YIELD_DELAY = 4000;

    /**
     * Maximum number of operations allowed in a batch between yield points.
     */
    private static final int MAX_OPERATIONS_PER_YIELD_POINT = 500;

    /**
     * Number of inserts performed in bulk to allow before yielding the transaction.
     */
    private static final int BULK_INSERTS_PER_YIELD_POINT = 50;

    /**
     * The contacts transaction that is active in this thread.
     */
    private ThreadLocal<ContactsTransaction> mTransactionHolder;

    /**
     * The DB helper to use for this content provider.
     */
    private ContactsDatabaseHelper mDbHelper;

    /**
     * The database helper to serialize all transactions on.  If non-null, any new transaction
     * created by this provider will automatically retrieve a writable database from this helper
     * and initiate a transaction on that database.  This should be used to ensure that operations
     * across multiple databases are all blocked on a single DB lock (to prevent deadlock cases).
     *
     * Hint: It's always {@link ContactsDatabaseHelper}.
     *
     * TODO Change the structure to make it obvious that it's actually always set, and is the
     * {@link ContactsDatabaseHelper}.
     */
    private SQLiteOpenHelper mSerializeOnDbHelper;

    /**
     * The tag corresponding to the database used for serializing transactions.
     *
     * Hint: It's always the contacts db helper tag.
     *
     * See also the TODO on {@link #mSerializeOnDbHelper}.
     */
    private String mSerializeDbTag;

    /**
     * The transaction listener used with {@link #mSerializeOnDbHelper}.
     *
     * Hint: It's always {@link ContactsProvider2}.
     *
     * See also the TODO on {@link #mSerializeOnDbHelper}.
     */
    private SQLiteTransactionListener mSerializedDbTransactionListener;


    protected final ProviderAccessStats mStats = new ProviderAccessStats();

    @Override
    public boolean onCreate() {
        Context context = getContext();
        mDbHelper = newDatabaseHelper(context);
        mTransactionHolder = getTransactionHolder();
        return true;
    }

    public ContactsDatabaseHelper getDatabaseHelper() {
        return mDbHelper;
    }

    /**
     * Specifies a database helper (and corresponding tag) to serialize all transactions on.
     *
     * See also the TODO on {@link #mSerializeOnDbHelper}.
     */
    public void setDbHelperToSerializeOn(SQLiteOpenHelper serializeOnDbHelper, String tag,
            SQLiteTransactionListener listener) {
        mSerializeOnDbHelper = serializeOnDbHelper;
        mSerializeDbTag = tag;
        mSerializedDbTransactionListener = listener;
    }

    public ContactsTransaction getCurrentTransaction() {
        return mTransactionHolder.get();
    }

    private boolean isInBatch() {
        final ContactsTransaction t = mTransactionHolder.get();
        return t != null && t.isBatch();
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementInsertStats(callingUid, isInBatch());
        try {
            ContactsTransaction transaction = startTransaction(false);
            try {
                Uri result = insertInTransaction(uri, values);
                if (result != null) {
                    transaction.markDirty();
                }
                transaction.markSuccessful(false);
                return result;
            } finally {
                endTransaction(false);
            }
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementDeleteStats(callingUid, isInBatch());
        try {
            ContactsTransaction transaction = startTransaction(false);
            try {
                int deleted = deleteInTransaction(uri, selection, selectionArgs);
                if (deleted > 0) {
                    transaction.markDirty();
                }
                transaction.markSuccessful(false);
                return deleted;
            } finally {
                endTransaction(false);
            }
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementUpdateStats(callingUid, isInBatch());
        try {
            ContactsTransaction transaction = startTransaction(false);
            try {
                int updated = updateInTransaction(uri, values, selection, selectionArgs);
                if (updated > 0) {
                    transaction.markDirty();
                }
                transaction.markSuccessful(false);
                return updated;
            } finally {
                endTransaction(false);
            }
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values) {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementBatchStats(callingUid);
        try {
            ContactsTransaction transaction = startTransaction(true);
            int numValues = values.length;
            int opCount = 0;
            try {
                for (int i = 0; i < numValues; i++) {
                    insert(uri, values[i]);
                    if (++opCount >= BULK_INSERTS_PER_YIELD_POINT) {
                        opCount = 0;
                        try {
                            this.yield(transaction);
                        } catch (RuntimeException re) {
                            transaction.markYieldFailed();
                            throw re;
                        }
                    }
                }
                transaction.markSuccessful(true);
            } finally {
                endTransaction(true);
            }
            return numValues;
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    @Override
    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {
        final int callingUid = Binder.getCallingUid();
        mStats.incrementBatchStats(callingUid);
        try {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "applyBatch: " + operations.size() + " ops");
            }
            int ypCount = 0;
            int opCount = 0;
            ContactsTransaction transaction = startTransaction(true);
            try {
                final int numOperations = operations.size();
                final ContentProviderResult[] results = new ContentProviderResult[numOperations];
                for (int i = 0; i < numOperations; i++) {
                    if (++opCount >= MAX_OPERATIONS_PER_YIELD_POINT) {
                        throw new OperationApplicationException(
                                "Too many content provider operations between yield points. "
                                        + "The maximum number of operations per yield point is "
                                        + MAX_OPERATIONS_PER_YIELD_POINT, ypCount);
                    }
                    final ContentProviderOperation operation = operations.get(i);
                    if (i > 0 && operation.isYieldAllowed()) {
                        if (VERBOSE_LOGGING) {
                            Log.v(TAG, "applyBatch: " + opCount + " ops finished; about to yield...");
                        }
                        opCount = 0;
                        try {
                            if (this.yield(transaction)) {
                                ypCount++;
                            }
                        } catch (RuntimeException re) {
                            transaction.markYieldFailed();
                            throw re;
                        }
                    }

                    results[i] = operation.apply(this, results, i);
                }
                transaction.markSuccessful(true);
                return results;
            } finally {
                endTransaction(true);
            }
        } finally {
            mStats.finishOperation(callingUid);
        }
    }

    /**
     * If we are not yet already in a transaction, this starts one (on the DB to serialize on, if
     * present) and sets the thread-local transaction variable for tracking.  If we are already in
     * a transaction, this returns that transaction, and the batch parameter is ignored.
     * @param callerIsBatch Whether the caller is operating in batch mode.
     */
    private ContactsTransaction startTransaction(boolean callerIsBatch) {
        if (ENABLE_TRANSACTION_LOG) {
            Log.i(TAG, "startTransaction " + getClass().getSimpleName() +
                    "  callerIsBatch=" + callerIsBatch, new RuntimeException("startTransaction"));
        }
        ContactsTransaction transaction = mTransactionHolder.get();
        if (transaction == null) {
            transaction = new ContactsTransaction(callerIsBatch);
            if (mSerializeOnDbHelper != null) {
                transaction.startTransactionForDb(mSerializeOnDbHelper.getWritableDatabase(),
                        mSerializeDbTag, mSerializedDbTransactionListener);
            }
            mTransactionHolder.set(transaction);
        }
        return transaction;
    }

    /**
     * Ends the current transaction and clears out the member variable.  This does not set the
     * transaction as being successful.
     * @param callerIsBatch Whether the caller is operating in batch mode.
     */
    private void endTransaction(boolean callerIsBatch) {
        if (ENABLE_TRANSACTION_LOG) {
            Log.i(TAG, "endTransaction " + getClass().getSimpleName() +
                    "  callerIsBatch=" + callerIsBatch, new RuntimeException("endTransaction"));
        }
        ContactsTransaction transaction = mTransactionHolder.get();
        if (transaction != null && (!transaction.isBatch() || callerIsBatch)) {
            boolean notify = false;
            try {
                if (transaction.isDirty()) {
                    notify = true;
                }
                transaction.finish(callerIsBatch);
                if (notify) {
                    notifyChange();
                }
            } finally {
                // No matter what, make sure we clear out the thread-local transaction reference.
                mTransactionHolder.set(null);
            }
        }
    }

    /**
     * Gets the database helper for this contacts provider.  This is called once, during onCreate().
     * Do not call in other places.
     */
    protected abstract ContactsDatabaseHelper newDatabaseHelper(Context context);

    /**
     * Gets the thread-local transaction holder to use for keeping track of the transaction.  This
     * is called once, in onCreate().  If multiple classes are inheriting from this class that need
     * to be kept in sync on the same transaction, they must all return the same thread-local.
     */
    protected abstract ThreadLocal<ContactsTransaction> getTransactionHolder();

    protected abstract Uri insertInTransaction(Uri uri, ContentValues values);

    protected abstract int deleteInTransaction(Uri uri, String selection, String[] selectionArgs);

    protected abstract int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs);

    protected abstract boolean yield(ContactsTransaction transaction);

    protected abstract void notifyChange();

    private static final String ACCOUNTS_QUERY =
            "SELECT * FROM " + Tables.ACCOUNTS + " ORDER BY " + BaseColumns._ID;

    private static final String NUM_INVISIBLE_CONTACTS_QUERY =
            "SELECT count(*) FROM " + Tables.CONTACTS;

    private static final String NUM_VISIBLE_CONTACTS_QUERY =
            "SELECT count(*) FROM " + Tables.DEFAULT_DIRECTORY;

    private static final String NUM_RAW_CONTACTS_PER_CONTACT =
            "SELECT _id, count(*) as c FROM " + Tables.RAW_CONTACTS
                    + " GROUP BY " + RawContacts.CONTACT_ID;

    private static final String MAX_RAW_CONTACTS_PER_CONTACT =
            "SELECT max(c) FROM (" + NUM_RAW_CONTACTS_PER_CONTACT + ")";

    private static final String AVG_RAW_CONTACTS_PER_CONTACT =
            "SELECT avg(c) FROM (" + NUM_RAW_CONTACTS_PER_CONTACT + ")";

    private static final String NUM_RAW_CONTACT_PER_ACCOUNT_PER_CONTACT =
            "SELECT " + RawContactsColumns.ACCOUNT_ID + " AS aid"
                    + ", " + RawContacts.CONTACT_ID + " AS cid"
                    + ", count(*) AS c"
                    + " FROM " + Tables.RAW_CONTACTS
                    + " GROUP BY aid, cid";

    private static final String RAW_CONTACTS_PER_ACCOUNT_PER_CONTACT =
            "SELECT aid, sum(c) AS s, max(c) AS m, avg(c) AS a"
                    + " FROM (" + NUM_RAW_CONTACT_PER_ACCOUNT_PER_CONTACT + ")"
                    + " GROUP BY aid";

    private static final String DATA_WITH_ACCOUNT =
            "SELECT d._id AS did"
            + ", d." + Data.RAW_CONTACT_ID + " AS rid"
            + ", r." + RawContactsColumns.ACCOUNT_ID + " AS aid"
            + " FROM " + Tables.DATA + " AS d JOIN " + Tables.RAW_CONTACTS + " AS r"
            + " ON d." + Data.RAW_CONTACT_ID + "=r._id";

    private static final String NUM_DATA_PER_ACCOUNT_PER_RAW_CONTACT =
            "SELECT aid, rid, count(*) AS c"
                    + " FROM (" + DATA_WITH_ACCOUNT + ")"
                    + " GROUP BY aid, rid";

    private static final String DATA_PER_ACCOUNT_PER_RAW_CONTACT =
            "SELECT aid, sum(c) AS s, max(c) AS m, avg(c) AS a"
                    + " FROM (" + NUM_DATA_PER_ACCOUNT_PER_RAW_CONTACT + ")"
                    + " GROUP BY aid";

    protected void dump(PrintWriter pw, String dbName) {
        pw.print("Database: ");
        pw.println(dbName);

        mStats.dump(pw, "  ");

        if (mDbHelper == null) {
            pw.println("mDbHelper is null");
            return;
        }
        try {
            pw.println();
            pw.println("  Accounts:");
            final SQLiteDatabase db = mDbHelper.getReadableDatabase();

            try (Cursor c = db.rawQuery(ACCOUNTS_QUERY, null)) {
                c.moveToPosition(-1);
                while (c.moveToNext()) {
                    pw.print("    ");
                    dumpLongColumn(pw, c, BaseColumns._ID);
                    pw.print(" ");
                    dumpStringColumn(pw, c, AccountsColumns.ACCOUNT_NAME);
                    pw.print(" ");
                    dumpStringColumn(pw, c, AccountsColumns.ACCOUNT_TYPE);
                    pw.print(" ");
                    dumpStringColumn(pw, c, AccountsColumns.DATA_SET);
                    pw.println();
                }
            }

            pw.println();
            pw.println("  Contacts:");
            pw.print("    # of visible: ");
            pw.print(longForQuery(db, NUM_VISIBLE_CONTACTS_QUERY));
            pw.println();

            pw.print("    # of invisible: ");
            pw.print(longForQuery(db, NUM_INVISIBLE_CONTACTS_QUERY));
            pw.println();

            pw.print("    Max # of raw contacts: ");
            pw.print(longForQuery(db, MAX_RAW_CONTACTS_PER_CONTACT));
            pw.println();

            pw.print("    Avg # of raw contacts: ");
            pw.print(doubleForQuery(db, AVG_RAW_CONTACTS_PER_CONTACT));
            pw.println();

            pw.println();
            pw.println("  Raw contacts (per account):");
            try (Cursor c = db.rawQuery(RAW_CONTACTS_PER_ACCOUNT_PER_CONTACT, null)) {
                c.moveToPosition(-1);
                while (c.moveToNext()) {
                    pw.print("    ");
                    dumpLongColumn(pw, c, "aid");
                    pw.print(" total # of raw contacts: ");
                    dumpStringColumn(pw, c, "s");
                    pw.print(", max # per contact: ");
                    dumpLongColumn(pw, c, "m");
                    pw.print(", avg # per contact: ");
                    dumpDoubleColumn(pw, c, "a");
                    pw.println();
                }
            }

            pw.println();
            pw.println("  Data (per account):");
            try (Cursor c = db.rawQuery(DATA_PER_ACCOUNT_PER_RAW_CONTACT, null)) {
                c.moveToPosition(-1);
                while (c.moveToNext()) {
                    pw.print("    ");
                    dumpLongColumn(pw, c, "aid");
                    pw.print(" total # of data:");
                    dumpLongColumn(pw, c, "s");
                    pw.print(", max # per raw contact: ");
                    dumpLongColumn(pw, c, "m");
                    pw.print(", avg # per raw contact: ");
                    dumpDoubleColumn(pw, c, "a");
                    pw.println();
                }
            }
        } catch (Exception e) {
            pw.println("Error: " + e);
        }
    }

    private static void dumpStringColumn(PrintWriter pw, Cursor c, String column) {
        final int index = c.getColumnIndex(column);
        if (index == -1) {
            pw.println("Column not found: " + column);
            return;
        }
        final String value = c.getString(index);
        if (value == null) {
            pw.print("(null)");
        } else if (value.length() == 0) {
            pw.print("\"\"");
        } else {
            pw.print(value);
        }
    }

    private static void dumpLongColumn(PrintWriter pw, Cursor c, String column) {
        final int index = c.getColumnIndex(column);
        if (index == -1) {
            pw.println("Column not found: " + column);
            return;
        }
        if (c.isNull(index)) {
            pw.print("(null)");
        } else {
            pw.print(c.getLong(index));
        }
    }

    private static void dumpDoubleColumn(PrintWriter pw, Cursor c, String column) {
        final int index = c.getColumnIndex(column);
        if (index == -1) {
            pw.println("Column not found: " + column);
            return;
        }
        if (c.isNull(index)) {
            pw.print("(null)");
        } else {
            pw.print(c.getDouble(index));
        }
    }

    private static long longForQuery(SQLiteDatabase db, String query) {
        return DatabaseUtils.longForQuery(db, query, null);
    }

    private static double doubleForQuery(SQLiteDatabase db, String query) {
        try (final Cursor c = db.rawQuery(query, null)) {
            if (!c.moveToFirst()) {
                return -1;
            }
            return c.getDouble(0);
        }
    }
}
