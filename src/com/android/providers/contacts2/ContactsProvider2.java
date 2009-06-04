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

package com.android.providers.contacts2;

import com.android.providers.contacts2.OpenHelper.DataColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupColumns;
import com.android.providers.contacts2.OpenHelper.NameLookupType;
import com.android.providers.contacts2.OpenHelper.PhoneLookupColumns;
import com.android.providers.contacts2.OpenHelper.Tables;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.ContentProviderResult;
import android.content.ContentProviderOperation;
import android.content.OperationApplicationException;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Accounts;
import android.provider.Contacts.ContactMethods;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.CommonDataKinds;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.telephony.PhoneNumberUtils;
import android.text.TextUtils;
import android.util.Log;
import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.OnAccountsUpdatedListener;
import android.os.RemoteException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Contacts content provider. The contract between this provider and applications
 * is defined in {@link ContactsContract}.
 */
public class ContactsProvider2 extends ContentProvider implements OnAccountsUpdatedListener {
    // TODO: clean up debug tag and rename this class
    private static final String TAG = "ContactsProvider ~~~~";

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);

    private static final int AGGREGATES = 1000;
    private static final int AGGREGATES_ID = 1001;
    private static final int AGGREGATES_DATA = 1002;
    private static final int AGGREGATES_PRIMARY_PHONE = 1003;

    private static final int CONTACTS = 2002;
    private static final int CONTACTS_ID = 2003;
    private static final int CONTACTS_DATA = 2004;
    private static final int CONTACTS_FILTER_EMAIL = 2005;

    private static final int DATA = 3000;
    private static final int DATA_ID = 3001;

    private static final int PHONE_LOOKUP = 4000;

    private static final int ACCOUNTS = 5000;
    private static final int ACCOUNTS_ID = 5001;

    /** Contains just the contacts columns */
    private static final HashMap<String, String> sAggregatesProjectionMap;
    /** Contains the aggregate columns along with primary phone */
    private static final HashMap<String, String> sAggregatesPrimaryPhoneProjectionMap;
    /** Contains the data, contacts, and aggregate columns, for joined tables. */
    private static final HashMap<String, String> sDataContactsAggregateProjectionMap;
    /** Contains just the contacts columns */
    private static final HashMap<String, String> sContactsProjectionMap;
    /** Contains just the data columns */
    private static final HashMap<String, String> sDataProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsProjectionMap;
    /** Contains the data and contacts columns, for joined tables */
    private static final HashMap<String, String> sDataContactsAccountsProjectionMap;
    /** Contains just the key and value columns */
    private static final HashMap<String, String> sAccountsProjectionMap;

    private static final HashMap<Account, Long> sAccountsToIdMap = new HashMap<Account, Long>();
    private static final HashMap<Long, Account> sIdToAccountsMap = new HashMap<Long, Account>();

    /** Sql select statement that returns the contact id associated with a data record. */
    private static final String sNestedContactIdSelect;
    /** Sql select statement that returns the mimetype id associated with a data record. */
    private static final String sNestedMimetypeSelect;
    /** Sql select statement that returns the aggregate id associated with a contact record. */
    private static final String sNestedAggregateIdSelect;
    /** Sql select statement that returns a list of contact ids associated with an aggregate record. */
    private static final String sNestedContactIdListSelect;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "primary" is selected.*/
    private static final String sSetPrimaryWhere;
    /** Sql where statement used to match all the data records that need to be updated when a new
     * "super primary" is selected.*/
    private static final String sSetSuperPrimaryWhere;
    /** Precompiled sql statement for setting a data record to the primary. */
    private SQLiteStatement mSetPrimaryStatement;
    /** Precomipled sql statement for setting a data record to the super primary. */
    private SQLiteStatement mSetSuperPrimaryStatement;

    static {
        // Contacts URI matching table
        final UriMatcher matcher = sUriMatcher;
        matcher.addURI(ContactsContract.AUTHORITY, "accounts", ACCOUNTS);
        matcher.addURI(ContactsContract.AUTHORITY, "accounts/#", ACCOUNTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates", AGGREGATES);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#", AGGREGATES_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates/#/data", AGGREGATES_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "aggregates_primary_phone/*",
                AGGREGATES_PRIMARY_PHONE);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts", CONTACTS);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#", CONTACTS_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/#/data", CONTACTS_DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "contacts/filter_email/*", CONTACTS_FILTER_EMAIL);
        matcher.addURI(ContactsContract.AUTHORITY, "data", DATA);
        matcher.addURI(ContactsContract.AUTHORITY, "data/#", DATA_ID);
        matcher.addURI(ContactsContract.AUTHORITY, "phone_lookup/*", PHONE_LOOKUP);

        HashMap<String, String> columns;

        // Accounts projection map
        columns = new HashMap<String, String>();
        columns.put(Accounts._ID, "accounts._id AS _id");
        columns.put(Accounts.NAME, Accounts.NAME);
        columns.put(Accounts.TYPE, Accounts.TYPE);
        columns.put(Accounts.DATA1, Accounts.DATA1);
        columns.put(Accounts.DATA2, Accounts.DATA2);
        columns.put(Accounts.DATA3, Accounts.DATA3);
        columns.put(Accounts.DATA4, Accounts.DATA4);
        columns.put(Accounts.DATA5, Accounts.DATA5);
        sAccountsProjectionMap = columns;

        // Aggregates projection map
        columns = new HashMap<String, String>();
        columns.put(Aggregates._ID, "aggregates._id AS _id");
        columns.put(Aggregates.DISPLAY_NAME, Aggregates.DISPLAY_NAME);
        columns.put(Aggregates.LAST_TIME_CONTACTED, Aggregates.LAST_TIME_CONTACTED);
        columns.put(Aggregates.STARRED, Aggregates.STARRED);
        columns.put(Aggregates.PRIMARY_PHONE_ID, Aggregates.PRIMARY_PHONE_ID);
        columns.put(Aggregates.PRIMARY_EMAIL_ID, Aggregates.PRIMARY_EMAIL_ID);
        sAggregatesProjectionMap = columns;

        // Aggregates primaries projection map
        columns = new HashMap<String, String>(sAggregatesProjectionMap);
        columns.put(CommonDataKinds.Phone.TYPE, CommonDataKinds.Phone.TYPE);
        columns.put(CommonDataKinds.Phone.LABEL, CommonDataKinds.Phone.LABEL);
        columns.put(CommonDataKinds.Phone.NUMBER, CommonDataKinds.Phone.NUMBER);
        sAggregatesPrimaryPhoneProjectionMap = columns;

        // Contacts projection map
        columns = new HashMap<String, String>();
        columns.put(Contacts._ID, "contacts._id AS _id");
        columns.put(Contacts.AGGREGATE_ID, Contacts.AGGREGATE_ID);
        columns.put(Contacts.ACCOUNTS_ID, Contacts.ACCOUNTS_ID);
        columns.put(Contacts.SOURCE_ID, Contacts.SOURCE_ID);
        columns.put(Contacts.VERSION, Contacts.VERSION);
        columns.put(Contacts.DIRTY, Contacts.DIRTY);
        sContactsProjectionMap = columns;

        // Data projection map
        columns = new HashMap<String, String>();
        columns.put(Data._ID, "data._id AS _id");
        columns.put(Data.CONTACT_ID, Data.CONTACT_ID);
        columns.put(Data.PACKAGE, Data.PACKAGE);
        columns.put(Data.MIMETYPE, Data.MIMETYPE);
        columns.put(Data.IS_PRIMARY, Data.IS_PRIMARY);
        columns.put(Data.IS_SUPER_PRIMARY, Data.IS_SUPER_PRIMARY);
        columns.put(Data.DATA1, "data.data1 as data1");
        columns.put(Data.DATA2, "data.data2 as data2");
        columns.put(Data.DATA3, "data.data3 as data3");
        columns.put(Data.DATA4, "data.data4 as data4");
        columns.put(Data.DATA5, "data.data5 as data5");
        columns.put(Data.DATA6, "data.data6 as data6");
        columns.put(Data.DATA7, "data.data7 as data7");
        columns.put(Data.DATA8, "data.data8 as data8");
        columns.put(Data.DATA9, "data.data9 as data9");
        columns.put(Data.DATA10, "data.data10 as data10");
        sDataProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sContactsProjectionMap);
        columns.putAll(sDataProjectionMap); // _id will be replaced with the one from data
        columns.put(Data.CONTACT_ID, "data.contact_id");
        sDataContactsProjectionMap = columns;

        columns = new HashMap<String, String>();
        columns.put(Accounts.NAME, Accounts.NAME);
        columns.put(Accounts.TYPE, Accounts.TYPE);
        columns.putAll(sDataContactsProjectionMap);
        sDataContactsAccountsProjectionMap = columns;

        // Data and contacts projection map for joins. _id comes from the data table
        columns = new HashMap<String, String>();
        columns.putAll(sAggregatesProjectionMap);
        columns.putAll(sContactsProjectionMap); //
        columns.putAll(sDataProjectionMap); // _id will be replaced with the one from data
        columns.put(Data.CONTACT_ID, "data.contact_id");
        sDataContactsAggregateProjectionMap = columns;

        sNestedContactIdSelect = "SELECT " + Data.CONTACT_ID + " FROM " + Tables.DATA + " WHERE "
                + Data._ID + "=?";
        sNestedMimetypeSelect = "SELECT " + DataColumns.MIMETYPE_ID + " FROM " + Tables.DATA
                + " WHERE " + Data._ID + "=?";
        sNestedAggregateIdSelect = "SELECT " + Contacts.AGGREGATE_ID + " FROM " + Tables.CONTACTS
                + " WHERE " + Contacts._ID + "=(" + sNestedContactIdSelect + ")";
        sNestedContactIdListSelect = "SELECT " + Contacts._ID + " FROM " + Tables.CONTACTS
                + " WHERE " + Contacts.AGGREGATE_ID + "=(" + sNestedAggregateIdSelect + ")";
        sSetPrimaryWhere = Data.CONTACT_ID + "=(" + sNestedContactIdSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
        sSetSuperPrimaryWhere  = Data.CONTACT_ID + " IN (" + sNestedContactIdListSelect + ") AND "
                + DataColumns.MIMETYPE_ID + "=(" + sNestedMimetypeSelect + ")";
    }

    private final boolean mAsynchronous;
    private OpenHelper mOpenHelper;
    private static final AccountComparator sAccountComparator = new AccountComparator();
    private ContactAggregator mContactAggregator;

    public ContactsProvider2() {
        this(true);
    }

    /**
     * Constructor for testing.
     */
    /* package */ ContactsProvider2(boolean asynchronous) {
        mAsynchronous = asynchronous;
    }

    @Override
    public boolean onCreate() {
        final Context context = getContext();
        mOpenHelper = OpenHelper.getInstance(context);

        loadAccountsMaps();

        mContactAggregator = new ContactAggregator(context, mAsynchronous);

        // TODO remove this, it's here to force opening the database on boot for testing
        SQLiteDatabase db = mOpenHelper.getReadableDatabase();

        mSetPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_PRIMARY
                + "=(_id=?) WHERE " + sSetPrimaryWhere);
        mSetSuperPrimaryStatement = db.compileStatement(
                "UPDATE " + Tables.DATA + " SET " + Data.IS_SUPER_PRIMARY
                + "=(_id=?) WHERE " + sSetSuperPrimaryWhere);

        return true;
    }

    @Override
    protected void finalize() throws Throwable {
        if (mContactAggregator != null) {
            mContactAggregator.quit();
        }

        super.finalize();
    }

    /**
     * Wipes all data from the contacts database.
     */
    /* package */ void wipeData() {
        mOpenHelper.wipeData();
    }

    /**
     * Read the rows from the accounts table and populate the in-memory accounts maps.
     */
    private void loadAccountsMaps() {
        synchronized (sAccountsToIdMap) {
            sAccountsToIdMap.clear();
            sIdToAccountsMap.clear();
            Cursor c = mOpenHelper.getReadableDatabase().query(Tables.ACCOUNTS,
                    new String[]{Accounts._ID, Accounts.NAME, Accounts.TYPE},
                    null, null, null, null, null);
            try {
                while (c.moveToNext()) {
                    addToAccountsMaps(c.getLong(0), new Account(c.getString(1), c.getString(2)));
                }
            } finally {
                c.close();
            }
        }
    }

    /**
     * Return the Accounts rowId that matches the account that is passed in or null if
     * no match exists. If refreshIfNotFound is set then if the account cannot be found in the
     * map then the AccountManager will be queried synchronously for the current set of
     * accounts.
     */
    private Long readAccountByName(Account account, boolean refreshIfNotFound) {
        synchronized (sAccountsToIdMap) {
            Long id = sAccountsToIdMap.get(account);
            if (id == null && refreshIfNotFound) {
                onAccountsUpdated(AccountManager.get(getContext()).blockingGetAccounts());
                id = sAccountsToIdMap.get(account);
            }
            return id;
        }
    }

    /**
     * Return the Account that has the specified rowId or null if it does not exist.
     */
    private Account readAccountById(long id) {
        synchronized (sAccountsToIdMap) {
            return sIdToAccountsMap.get(id);
        }
    }

    /**
     * Add the contents from the Accounts row to the accounts maps.
     */
    private void addToAccountsMaps(long id, Account account) {
        synchronized (sAccountsToIdMap) {
            sAccountsToIdMap.put(account, id);
            sIdToAccountsMap.put(id, account);
        }
    }

    /**
     * Reads the current set of accounts from the AccountManager and makes the local
     * Accounts table and the in-memory accounts maps consistent with it.
     */
    public void onAccountsUpdated(Account[] accounts) {
        synchronized (sAccountsToIdMap) {
            Arrays.sort(accounts);

            // if there is an account in the array that we don't know about yet add it to our
            // cache and our database copy of accounts
            final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
            for (Account account : accounts) {
                if (readAccountByName(account, false /* refreshIfNotFound */) == null) {
                    // add this account
                    ContentValues values = new ContentValues();
                    values.put(Accounts.NAME, account.mName);
                    values.put(Accounts.TYPE, account.mType);
                    long id = db.insert(Tables.ACCOUNTS, Accounts.NAME, values);
                    if (id < 0) {
                        throw new IllegalStateException("error inserting account in db");
                    }
                    addToAccountsMaps(id, account);
                }
            }

            ArrayList<Account> accountsToRemove = new ArrayList<Account>();
            // now check our list of accounts and remove any that are not in the array
            for (Account account : sAccountsToIdMap.keySet()) {
                if (Arrays.binarySearch(accounts, account, sAccountComparator) < 0) {
                    accountsToRemove.add(account);
                }
            }

            for (Account account : accountsToRemove) {
                final Long id = sAccountsToIdMap.remove(account);
                sIdToAccountsMap.remove(id);
                db.delete(Tables.ACCOUNTS, Accounts._ID + "=" + id, null);
            }
        }
    }

    private static class AccountComparator implements Comparator<Account> {
        public int compare(Account object1, Account object2) {
            if (object1 == object2) {
                return 0;
            }
            int result = object1.mType.compareTo(object2.mType);
            if (result != 0) {
                return result;
            }
            return object1.mName.compareTo(object2.mName);
        }
    }

    /**
     * Called when a change has been made.
     *
     * @param uri the uri that the change was made to
     */
    private void onChange(Uri uri) {
        getContext().getContentResolver().notifyChange(ContactsContract.AUTHORITY_URI, null);
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        final int match = sUriMatcher.match(uri);
        long id = 0;
        switch (match) {
            case ACCOUNTS: {
                id = insertAccountData(values);
                break;
            }

            case AGGREGATES: {
                id = insertAggregate(values);
                break;
            }

            case CONTACTS: {
                id = insertContact(values);
                break;
            }

            case CONTACTS_DATA: {
                values.put(Data.CONTACT_ID, uri.getPathSegments().get(1));
                id = insertData(values);
                break;
            }

            case DATA: {
                id = insertData(values);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if (id < 0) {
            return null;
        }

        final Uri result = ContentUris.withAppendedId(uri, id);
        onChange(result);
        return result;
    }

    /**
     * Inserts an item in the accounts table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertAccountData(ContentValues values) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        return db.insert(Tables.ACCOUNTS, Accounts.DATA1, values);
    }

    /**
     * Inserts an item in the aggregates table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertAggregate(ContentValues values) {
        throw new UnsupportedOperationException("Aggregates are created automatically");
    }

    /**
     * Inserts an item in the contacts table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertContact(ContentValues values) {
        /*
         * The contact record is inserted in the contacts table, but it needs to
         * be processed by the aggregator before it will be returned by the
         * "aggregates" queries.
         */
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        ContentValues overriddenValues = new ContentValues(values);
        overriddenValues.putNull(Contacts.AGGREGATE_ID);
        if (!resolveAccount(overriddenValues)) {
            return -1;
        }

        long rowId = db.insert(Tables.CONTACTS, Contacts.AGGREGATE_ID, overriddenValues);

        mContactAggregator.schedule();

        return rowId;
    }

    /**
     * If an account name or type is specified in values then create an Account from it
     * and look up the Accounts rowId that corresponds to the Account. Then insert
     * the Accounts rowId into the values with key {@link Contacts#ACCOUNTS_ID}. Remove any
     * value for {@link Accounts#NAME} or {@link Accounts#TYPE} from the values.
     * @param values the ContentValues to read from and update
     * @return false if an account was present in the values that is not in the Accounts table
     */
    private boolean resolveAccount(ContentValues values) {
        // If an account name and type is specified then resolve it into an accounts_id.
        // If either is specified then both must be specified.
        final String accountName = values.getAsString(Accounts.NAME);
        final String accountType = values.getAsString(Accounts.TYPE);
        if (!TextUtils.isEmpty(accountName) || !TextUtils.isEmpty(accountType)) {
            final Account account = new Account(accountName, accountType);
            final Long accountId = readAccountByName(account, true /* refreshIfNotFound */);
            if (accountId == null) {
                // an invalid account was passed in or the account was deleted after this
                // request was made. fail this request.
                return false;
            }
            values.put(Contacts.ACCOUNTS_ID, accountId);
        }
        values.remove(Accounts.NAME);
        values.remove(Accounts.TYPE);
        return true;
    }

    /**
     * Inserts an item in the data table
     *
     * @param values the values for the new row
     * @return the row ID of the newly created row
     */
    private long insertData(ContentValues values) {
        boolean success = false;

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long id = 0;
        db.beginTransaction();
        try {
            // TODO: Consider enforcing Binder.getCallingUid() for package name
            // requested by this insert.

            long contactId = values.getAsLong(Data.CONTACT_ID);

            // Replace package name and mime-type with internal mappings
            final String packageName = values.getAsString(Data.PACKAGE);
            values.put(DataColumns.PACKAGE_ID, mOpenHelper.getPackageId(packageName));
            values.remove(Data.PACKAGE);

            final String mimeType = values.getAsString(Data.MIMETYPE);
            values.put(DataColumns.MIMETYPE_ID, mOpenHelper.getMimeTypeId(mimeType));
            values.remove(Data.MIMETYPE);

            // Insert the data row itself
            id = db.insert(Tables.DATA, Data.DATA1, values);

            // If it's a phone number add the normalized version to the lookup table
            if (CommonDataKinds.Phone.CONTENT_ITEM_TYPE.equals(mimeType)) {
                final ContentValues phoneValues = new ContentValues();
                final String number = values.getAsString(Phone.NUMBER);
                phoneValues.put(PhoneLookupColumns.NORMALIZED_NUMBER,
                        PhoneNumberUtils.getStrippedReversed(number));
                phoneValues.put(PhoneLookupColumns.DATA_ID, id);
                phoneValues.put(PhoneLookupColumns.CONTACT_ID, contactId);
                db.insert(Tables.PHONE_LOOKUP, null, phoneValues);
            }

            if (CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE.equals(mimeType)) {
                insertStructuredNameLookup(db, id, contactId, values);
            }

            markContactForAggregation(db, contactId);

            db.setTransactionSuccessful();
            success = true;
        } finally {
            db.endTransaction();
        }

        if (success) {
            mContactAggregator.schedule();
        }

        return id;
    }

    /**
     * Marks the specified contact for (re)aggregation.
     *
     * @param db a writable database with an open transaction
     * @param contactId contact ID that needs to be (re)aggregated
     */
    private void markContactForAggregation(final SQLiteDatabase db, long contactId) {
        ContentValues values = new ContentValues(1);
        values.putNull(Contacts.AGGREGATE_ID);
        db.update(Tables.CONTACTS, values, Contacts._ID + "=" + contactId, null);
    }

    /**
     * Inserts various permutations of the contact name into a helper table (name_lookup) to
     * facilitate aggregation of contacts with slightly different names (e.g. first name and last
     * name swapped or concatenated.)
     */
    private void insertStructuredNameLookup(SQLiteDatabase db, long id, long contactId,
            ContentValues values) {

        String givenName = values.getAsString(StructuredName.GIVEN_NAME);
        String familyName = values.getAsString(StructuredName.FAMILY_NAME);

        if (TextUtils.isEmpty(givenName)) {
            if (TextUtils.isEmpty(familyName)) {

                // Nothing specified - nothing to insert in the lookup table
                return;
            }

            insertFamilyNameLookup(db, id, contactId, familyName);
        } else if (TextUtils.isEmpty(familyName)) {
            insertGivenNameLookup(db, id, contactId, givenName);
        } else {
            insertFullNameLookup(db, id, contactId, givenName, familyName);
        }
    }

    /**
     * Populates the name_lookup table when only the first name is specified.
     */
    private void insertGivenNameLookup(SQLiteDatabase db, long id, Long contactId,
            String givenName) {
        final String givenNameLc = givenName.toLowerCase();
        insertNameLookup(db, id, contactId, givenNameLc,
                NameLookupType.GIVEN_NAME_ONLY);
    }

    /**
     * Populates the name_lookup table when only the last name is specified.
     */
    private void insertFamilyNameLookup(SQLiteDatabase db, long id, Long contactId,
            String familyName) {
        final String familyNameLc = familyName.toLowerCase();
        insertNameLookup(db, id, contactId, familyNameLc,
                NameLookupType.FAMILY_NAME_ONLY);
    }

    /**
     * Populates the name_lookup table when both the first and last names are specified.
     */
    private void insertFullNameLookup(SQLiteDatabase db, long id, Long contactId, String givenName,
            String familyName) {
        final String givenNameLc = givenName.toLowerCase();
        final String familyNameLc = familyName.toLowerCase();

        insertNameLookup(db, id, contactId, givenNameLc + "." + familyNameLc,
                NameLookupType.FULL_NAME);
        insertNameLookup(db, id, contactId, familyNameLc + "." + givenNameLc,
                NameLookupType.FULL_NAME_REVERSE);
        insertNameLookup(db, id, contactId, givenNameLc + familyNameLc,
                NameLookupType.FULL_NAME_CONCATENATED);
        insertNameLookup(db, id, contactId, familyNameLc + givenNameLc,
                NameLookupType.FULL_NAME_REVERSE_CONCATENATED);
    }

    /**
     * Inserts a single name permutation into the name_lookup table.
     */
    private void insertNameLookup(SQLiteDatabase db, long id, Long contactId, String name,
            int nameType) {
        ContentValues values = new ContentValues(4);
        values.put(NameLookupColumns.DATA_ID, id);
        values.put(NameLookupColumns.CONTACT_ID, contactId);
        values.put(NameLookupColumns.NORMALIZED_NAME, name);
        values.put(NameLookupColumns.NAME_TYPE, nameType);
        db.insert(Tables.NAME_LOOKUP, NameLookupColumns._ID, values);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case AGGREGATES_ID: {
                long aggregateId = ContentUris.parseId(uri);

                // Remove references to the aggregate first
                ContentValues values = new ContentValues();
                values.putNull(Contacts.AGGREGATE_ID);
                db.update(Tables.CONTACTS, values, Contacts.AGGREGATE_ID + "=" + aggregateId, null);

                return db.delete(Tables.AGGREGATES, BaseColumns._ID + "=" + aggregateId, null);
            }

            case ACCOUNTS_ID: {
                long accountId = ContentUris.parseId(uri);

                return db.delete(Tables.ACCOUNTS, BaseColumns._ID + "=" + accountId, null);
            }

            case CONTACTS_ID: {
                long contactId = ContentUris.parseId(uri);
                int contactsDeleted = db.delete(Tables.CONTACTS, Contacts._ID + "=" + contactId, null);
                int dataDeleted = db.delete(Tables.DATA, Data.CONTACT_ID + "=" + contactId, null);
                return contactsDeleted + dataDeleted;
            }

            case DATA_ID: {
                long dataId = ContentUris.parseId(uri);
                return db.delete("data", Data._ID + "=" + dataId, null);
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        int count = 0;
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();

        final int match = sUriMatcher.match(uri);
        switch(match) {
            case ACCOUNTS: {
                final String accountName = uri.getQueryParameter(Accounts.NAME);
                final String accountType = uri.getQueryParameter(Accounts.TYPE);
                if (TextUtils.isEmpty(accountName) || TextUtils.isEmpty(accountType)) {
                    return 0;
                }
                final Long accountId = readAccountByName(
                        new Account(accountName, accountType), true /* refreshIfNotFound */);
                if (accountId == null) {
                    return 0;
                }
                String selectionWithId = (Accounts._ID + " = " + accountId + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.ACCOUNTS, values, selectionWithId, selectionArgs);
                break;
            }

            case ACCOUNTS_ID: {
                String selectionWithId = (Accounts._ID + " = " + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.ACCOUNTS, values, selectionWithId, selectionArgs);
                Log.i(TAG, "Selection is: " + selectionWithId);
                break;
            }

            // TODO(emillar): We will want to disallow editing the aggregates table at some point.
            case AGGREGATES: {
                count = db.update(Tables.AGGREGATES, values, selection, selectionArgs);
                break;
            }

            case AGGREGATES_ID: {
                String selectionWithId = (Aggregates._ID + " = " + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.AGGREGATES, values, selectionWithId, selectionArgs);
                break;
            }

            case DATA_ID: {
                boolean containsIsSuperPrimary = values.containsKey(Data.IS_SUPER_PRIMARY);
                boolean containsIsPrimary = values.containsKey(Data.IS_PRIMARY);
                int isSuperPrimary = values.getAsInteger(Data.IS_SUPER_PRIMARY);
                int isPrimary = values.getAsInteger(Data.IS_PRIMARY);
                final long id = ContentUris.parseId(uri);

                // Remove primary or super primary values being set to 0. This is disallowed by the
                // content provider.
                if (containsIsSuperPrimary && isSuperPrimary == 0) {
                    containsIsSuperPrimary = false;
                    values.remove(Data.IS_SUPER_PRIMARY);
                }
                if (containsIsPrimary && isPrimary == 0) {
                    containsIsPrimary = false;
                    values.remove(Data.IS_PRIMARY);
                }

                if (containsIsSuperPrimary) {
                    setIsSuperPrimary(id);
                    setIsPrimary(id);

                    // Now that we've taken care of setting these, remove them from "values".
                    values.remove(Data.IS_SUPER_PRIMARY);
                    if (containsIsPrimary) {
                        values.remove(Data.IS_PRIMARY);
                    }
                } else if (containsIsPrimary) {
                    setIsPrimary(id);

                    // Now that we've taken care of setting this, remove it from "values".
                    values.remove(Data.IS_PRIMARY);
                }

                if (values.size() > 0) {
                    String selectionWithId = (Data._ID + " = " + ContentUris.parseId(uri) + " ")
                    + (selection == null ? "" : " AND " + selection);
                    count = db.update(Tables.DATA, values, selectionWithId, selectionArgs);
                }
                break;
            }

            case CONTACTS: {
                count = db.update(Tables.CONTACTS, values, selection, selectionArgs);
                break;
            }

            case CONTACTS_ID: {
                String selectionWithId = (Contacts._ID + " = " + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND " + selection);
                count = db.update(Tables.CONTACTS, values, selectionWithId, selectionArgs);
                Log.i(TAG, "Selection is: " + selectionWithId);
                break;
            }

            case DATA: {
                count = db.update(Tables.DATA, values, selection, selectionArgs);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if (count > 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return count;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACCOUNTS: {
                qb.setTables(Tables.ACCOUNTS);
                qb.setProjectionMap(sAccountsProjectionMap);
                break;
            }

            case ACCOUNTS_ID: {
                qb.setTables(Tables.ACCOUNTS);
                qb.setProjectionMap(sAccountsProjectionMap);
                qb.appendWhere(BaseColumns._ID + " = " + ContentUris.parseId(uri));
                break;
            }

            case AGGREGATES: {
                qb.setTables(Tables.AGGREGATES);
                qb.setProjectionMap(sAggregatesProjectionMap);
                break;
            }

            case AGGREGATES_ID: {
                qb.setTables(Tables.AGGREGATES);
                qb.setProjectionMap(sAggregatesProjectionMap);
                qb.appendWhere(BaseColumns._ID + " = " + ContentUris.parseId(uri));
                break;
            }

            case AGGREGATES_DATA: {
                qb.setTables(Tables.DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataContactsAggregateProjectionMap);
                qb.appendWhere(Contacts.AGGREGATE_ID + " = " + uri.getPathSegments().get(1));
                break;
            }

            case AGGREGATES_PRIMARY_PHONE: {
                qb.setTables(Tables.AGGREGATES_JOIN_PRIMARY_PHONE_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sAggregatesPrimaryPhoneProjectionMap);
                break;
            }

            case CONTACTS: {
                qb.setTables(Tables.CONTACTS_JOIN_ACCOUNTS);
                qb.setProjectionMap(sContactsProjectionMap);
                break;
            }

            case CONTACTS_ID: {
                qb.setTables(Tables.CONTACTS);
                qb.setProjectionMap(sContactsProjectionMap);
                qb.appendWhere("_id = " + ContentUris.parseId(uri));
                break;
            }

            case CONTACTS_DATA: {
                qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataContactsProjectionMap);
                qb.appendWhere("contact_id = " + uri.getPathSegments().get(1));
                break;
            }

            case CONTACTS_FILTER_EMAIL: {
                qb.setTables(Tables.DATA_JOIN_AGGREGATES_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataContactsProjectionMap);
                qb.appendWhere(Data.MIMETYPE + "='" + CommonDataKinds.Email.CONTENT_ITEM_TYPE + "'");
                qb.appendWhere(" AND " + CommonDataKinds.Email.DATA + "=");
                qb.appendWhereEscapeString(uri.getPathSegments().get(2));
                break;
            }

            case DATA: {
                // TODO: enforce that caller has read access to this data
                // if the caller only has READ_{account type} permission then they must specify the
                // account type of the account they are interested in here and we will be sure to
                // limit the rows that are returned to that account type.
                final String accountName = uri.getQueryParameter(Accounts.NAME);
                final String accountType = uri.getQueryParameter(Accounts.TYPE);
                if (!TextUtils.isEmpty(accountName)) {
                    Account account = new Account(accountName, accountType);
                    Long accountId = readAccountByName(account, true /* refreshIfNotFound */);
                    if (accountId == null) {
                        // use -1 as the account to ensure that no rows are returned
                        accountId = (long) -1;
                    }
                    qb.appendWhere(Contacts.ACCOUNTS_ID + "=" + accountId);
                    qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE_CONTACTS);
                } else {
                    qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                }
                qb.setProjectionMap(sDataProjectionMap);
                break;
            }

            case DATA_ID: {
                // TODO: enforce that caller has read access to this data
                qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setProjectionMap(sDataProjectionMap);
                qb.appendWhere("data._id = " + ContentUris.parseId(uri));
                break;
            }

            case PHONE_LOOKUP: {
                if (TextUtils.isEmpty(sortOrder)) {
                    // Default the sort order to something reasonable so we get consistent
                    // results when callers don't request an ordering
                    sortOrder = Data.CONTACT_ID;
                }

                final String number = uri.getLastPathSegment();
                final String normalizedNumber = PhoneNumberUtils.toCallerIDMinMatch(number);
                final StringBuilder tables = new StringBuilder();
                tables.append("contacts, (SELECT data_id FROM phone_lookup WHERE (phone_lookup.normalized_number GLOB '");
                tables.append(normalizedNumber);
                tables.append("*')) AS lookup, " + Tables.DATA_JOIN_PACKAGE_MIMETYPE);
                qb.setTables(tables.toString());
                qb.appendWhere("lookup.data_id=data._id AND data.contact_id=contacts._id AND ");
                qb.appendWhere("PHONE_NUMBERS_EQUAL(data." + Phone.NUMBER + ", ");
                qb.appendWhereEscapeString(number);
                qb.appendWhere(")");
                qb.setProjectionMap(sDataContactsProjectionMap);
                break;
            }

            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        // Perform the query and set the notification uri
        final Cursor c = qb.query(db, projection, selection, selectionArgs, null, null, sortOrder);
        if (c != null) {
            c.setNotificationUri(getContext().getContentResolver(), ContactsContract.AUTHORITY_URI);
        }
        return c;
    }

    /**
     * An implementation of EntityIterator that joins the contacts and data tables
     * and consumes all the data rows for a contact in order to build the Entity for a contact.
     */
    private static class ContactsEntityIterator implements EntityIterator {
        private final Cursor mEntityCursor;
        private volatile boolean mIsClosed;
        private final Account mAccount;

        private static final String[] DATA_KEYS = new String[]{
                "data1",
                "data2",
                "data3",
                "data4",
                "data5",
                "data6",
                "data7",
                "data8",
                "data9",
                "data10"};

        private static final String[] PROJECTION = new String[]{
            Contacts.ACCOUNTS_ID,
            Contacts.SOURCE_ID,
            Contacts.VERSION,
            Contacts.DIRTY,
            Contacts.Data._ID,
            Contacts.Data.PACKAGE,
            Contacts.Data.MIMETYPE,
            Contacts.Data.DATA1,
            Contacts.Data.DATA2,
            Contacts.Data.DATA4,
            Contacts.Data.DATA5,
            Contacts.Data.DATA6,
            Contacts.Data.DATA7,
            Contacts.Data.DATA8,
            Contacts.Data.DATA9,
            Contacts.Data.DATA10,
            Contacts.Data.CONTACT_ID};

        private static final int COLUMN_SOURCE_ID = 1;
        private static final int COLUMN_DIRTY = 3;
        private static final int COLUMN_DATA_ID = 4;
        private static final int COLUMN_PACKAGE = 5;
        private static final int COLUMN_MIMETYPE = 6;
        private static final int COLUMN_DATA1 = 7;
        private static final int COLUMN_CONTACT_ID = 16;


        public ContactsEntityIterator(ContactsProvider2 provider, String contactsIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;

            final String accountName = uri.getQueryParameter(Accounts.NAME);
            final String accountType = uri.getQueryParameter(Accounts.TYPE);
            if (TextUtils.isEmpty(accountName) || TextUtils.isEmpty(accountType)) {
                throw new IllegalArgumentException("the account name and type must be "
                        + "specified in the query params of the uri");
            }
            mAccount = new Account(accountName, accountType);
            final Long accountId = provider.readAccountByName(mAccount,
                    true /* refreshIfNotFound */);
            if (accountId == null) {
                throw new IllegalArgumentException("the specified account does not exist");
            }

            final String updatedSortOrder = (sortOrder == null)
                    ? Contacts.Data.CONTACT_ID
                    : (Contacts.Data.CONTACT_ID + "," + sortOrder);

            final SQLiteDatabase db = provider.mOpenHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(Tables.DATA_JOIN_PACKAGE_MIMETYPE_CONTACTS);
            qb.setProjectionMap(sDataContactsAccountsProjectionMap);
            if (contactsIdString != null) {
                qb.appendWhere(Data.CONTACT_ID + "=" + contactsIdString);
            }
            qb.appendWhere(Contacts.ACCOUNTS_ID + "=" + accountId);
            mEntityCursor = qb.query(db, PROJECTION, selection, selectionArgs,
                    null, null, updatedSortOrder);
            mEntityCursor.moveToFirst();
        }

        public void close() {
            if (mIsClosed) {
                throw new IllegalStateException("closing when already closed");
            }
            mIsClosed = true;
            mEntityCursor.close();
        }

        public boolean hasNext() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling hasNext() when the iterator is closed");
            }

            return !mEntityCursor.isAfterLast();
        }

        public Entity next() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling next() when the iterator is closed");
            }
            if (!hasNext()) {
                throw new IllegalStateException("you may only call next() if hasNext() is true");
            }

            final SQLiteCursor c = (SQLiteCursor) mEntityCursor;

            final long contactId = c.getLong(COLUMN_CONTACT_ID);

            // we expect the cursor is already at the row we need to read from
            ContentValues contactValues = new ContentValues();
            contactValues.put(Accounts.NAME, mAccount.mName);
            contactValues.put(Accounts.TYPE, mAccount.mType);
            contactValues.put(Contacts._ID, contactId);
            contactValues.put(Contacts.DIRTY, c.getLong(COLUMN_DIRTY));
            contactValues.put(Contacts.SOURCE_ID, c.getString(COLUMN_SOURCE_ID));
            Entity contact = new Entity(contactValues);

            // read data rows until the contact id changes
            do {
                if (contactId != c.getLong(COLUMN_CONTACT_ID)) {
                    break;
                }
                // add the data to to the contact
                ContentValues dataValues = new ContentValues();
                dataValues.put(Contacts.Data._ID, c.getString(COLUMN_DATA_ID));
                dataValues.put(Contacts.Data.PACKAGE, c.getString(COLUMN_PACKAGE));
                dataValues.put(Contacts.Data.MIMETYPE, c.getLong(COLUMN_MIMETYPE));
                for (int i = 0; i < 10; i++) {
                    final int columnIndex = i + COLUMN_DATA1;
                    String key = DATA_KEYS[i];
                    if (c.isNull(columnIndex)) {
                        // don't put anything
                    } else if (c.isLong(columnIndex)) {
                        dataValues.put(key, c.getLong(columnIndex));
                    } else if (c.isFloat(columnIndex)) {
                        dataValues.put(key, c.getFloat(columnIndex));
                    } else if (c.isString(columnIndex)) {
                        dataValues.put(key, c.getString(columnIndex));
                    } else if (c.isBlob(columnIndex)) {
                        dataValues.put(key, c.getBlob(columnIndex));
                    }
                }
                contact.addSubValue(Data.CONTENT_URI, dataValues);
            } while (mEntityCursor.moveToNext());

            return contact;
        }
    }

    @Override
    public EntityIterator queryEntities(Uri uri, String selection, String[] selectionArgs,
            String sortOrder) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case CONTACTS:
            case CONTACTS_ID:
                String contactsIdString = null;
                if (match == CONTACTS_ID) {
                    contactsIdString = uri.getPathSegments().get(1);
                }

                return new ContactsEntityIterator(this, contactsIdString,
                        uri, selection, selectionArgs, sortOrder);
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public String getType(Uri uri) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case ACCOUNTS: return Accounts.CONTENT_TYPE;
            case ACCOUNTS_ID: return Accounts.CONTENT_ITEM_TYPE;
            case AGGREGATES: return Aggregates.CONTENT_TYPE;
            case AGGREGATES_ID: return Aggregates.CONTENT_ITEM_TYPE;
            case CONTACTS: return Contacts.CONTENT_TYPE;
            case CONTACTS_ID: return Contacts.CONTENT_ITEM_TYPE;
            case DATA_ID:
                final SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                long dataId = ContentUris.parseId(uri);
                return mOpenHelper.getDataMimeType(dataId);
        }
        throw new UnsupportedOperationException("Unknown uri: " + uri);
    }

    public ContentProviderResult[] applyBatch(ArrayList<ContentProviderOperation> operations)
            throws OperationApplicationException {

        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            ContentProviderResult[] results = super.applyBatch(operations);
            db.setTransactionSuccessful();
            return results;
        } finally {
            db.endTransaction();
        }
    }

    /*
     * Sets the given dataId record in the "data" table to primary, and resets all data records of
     * the same mimetype and under the same contact to not be primary.
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsPrimary(long dataId) {
        mSetPrimaryStatement.bindLong(1, dataId);
        mSetPrimaryStatement.bindLong(2, dataId);
        mSetPrimaryStatement.bindLong(3, dataId);
        mSetPrimaryStatement.execute();
    }

    /*
     * Sets the given dataId record in the "data" table to "super primary", and resets all data
     * records of the same mimetype and under the same aggregate to not be "super primary".
     *
     * @param dataId the id of the data record to be set to primary.
     */
    private void setIsSuperPrimary(long dataId) {
        mSetSuperPrimaryStatement.bindLong(1, dataId);
        mSetSuperPrimaryStatement.bindLong(2, dataId);
        mSetSuperPrimaryStatement.bindLong(3, dataId);
        mSetSuperPrimaryStatement.execute();
    }

}
