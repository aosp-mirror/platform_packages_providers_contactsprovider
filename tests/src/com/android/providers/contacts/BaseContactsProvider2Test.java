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
 * limitations under the License.
 */
package com.android.providers.contacts;

import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;

import com.android.providers.contacts.ContactsActor;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Entity;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Nickname;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.test.AndroidTestCase;
import android.test.mock.MockContentResolver;
import android.test.suitebuilder.annotation.LargeTest;
import android.accounts.Account;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * A common superclass for {@link ContactsProvider2}-related tests.
 */
@LargeTest
public abstract class BaseContactsProvider2Test extends AndroidTestCase {

    protected static final String PACKAGE = "ContactsProvider2Test";

    protected ContactsActor mActor;
    protected MockContentResolver mResolver;
    protected Account mAccount = new Account("account1", "account type1");

    protected final static Long NO_LONG = new Long(0);
    protected final static String NO_STRING = new String("");
    protected final static Account NO_ACCOUNT = new Account("a", "b");

    protected Class<? extends ContentProvider> getProviderClass() {
        return SynchronousContactsProvider2.class;
    }

    protected String getAuthority() {
        return ContactsContract.AUTHORITY;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mActor = new ContactsActor(getContext(), PACKAGE_GREY, getProviderClass(), getAuthority());
        mResolver = mActor.resolver;
        if (mActor.provider instanceof SynchronousContactsProvider2) {
            ((SynchronousContactsProvider2) mActor.provider)
                    .getOpenHelper(mActor.context).wipeData();
        }
    }

    protected Uri maybeAddAccountQueryParameters(Uri uri, Account account) {
        if (account == null) {
            return uri;
        }
        return uri.buildUpon()
                .appendQueryParameter(RawContacts.ACCOUNT_NAME, account.mName)
                .appendQueryParameter(RawContacts.ACCOUNT_TYPE, account.mType)
                .build();
    }

    protected long createContact() {
        ContentValues values = new ContentValues();
        Uri contactUri = mResolver.insert(RawContacts.CONTENT_URI, values);
        return ContentUris.parseId(contactUri);
    }

    protected long createContact(Account account) {
        ContentValues values = new ContentValues();
        final Uri uri = maybeAddAccountQueryParameters(RawContacts.CONTENT_URI, account);
        Uri contactUri = mResolver.insert(uri, values);
        return ContentUris.parseId(contactUri);
    }

    protected long createGroup(Account account, String sourceId, String title) {
        ContentValues values = new ContentValues();
        values.put(Groups.SOURCE_ID, sourceId);
        values.put(Groups.TITLE, title);
        final Uri uri = maybeAddAccountQueryParameters(Groups.CONTENT_URI, account);
        return ContentUris.parseId(mResolver.insert(uri, values));
    }

    protected Uri insertStructuredName(long contactId, String givenName, String familyName) {
        ContentValues values = new ContentValues();
        StringBuilder sb = new StringBuilder();
        if (givenName != null) {
            sb.append(givenName);
        }
        if (givenName != null && familyName != null) {
            sb.append(" ");
        }
        if (familyName != null) {
            sb.append(familyName);
        }
        values.put(StructuredName.DISPLAY_NAME, sb.toString());
        values.put(StructuredName.GIVEN_NAME, givenName);
        values.put(StructuredName.FAMILY_NAME, familyName);

        return insertStructuredName(contactId, values);
    }

    protected Uri insertStructuredName(long contactId, ContentValues values) {
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);
        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertPhoneNumber(long contactId, String phoneNumber) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
        values.put(Phone.NUMBER, phoneNumber);
        values.put(Phone.TYPE, Phone.TYPE_HOME);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertEmail(long contactId, String email) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
        values.put(Email.DATA, email);
        values.put(Email.TYPE, Email.TYPE_HOME);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertNickname(long contactId, String nickname) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Nickname.CONTENT_ITEM_TYPE);
        values.put(Nickname.NAME, nickname);
        values.put(Nickname.TYPE, Nickname.TYPE_OTHER_NAME);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertPhoto(long contactId) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertGroupMembership(long contactId, String sourceId) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
        values.put(GroupMembership.GROUP_SOURCE_ID, sourceId);
        return mResolver.insert(Data.CONTENT_URI, values);
    }

    protected Uri insertGroupMembership(long contactId, Long groupId) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
        values.put(GroupMembership.GROUP_ROW_ID, groupId);
        return mResolver.insert(Data.CONTENT_URI, values);
    }

    protected Uri insertPresence(int protocol, String handle, int presence) {
        ContentValues values = new ContentValues();
        values.put(Presence.IM_PROTOCOL, protocol);
        values.put(Presence.IM_HANDLE, handle);
        values.put(Presence.PRESENCE_STATUS, presence);

        Uri resultUri = mResolver.insert(Presence.CONTENT_URI, values);
        return resultUri;
    }

    protected Uri insertImHandle(long contactId, int protocol, String handle) {
        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, Im.CONTENT_ITEM_TYPE);
        values.put(Im.PROTOCOL, protocol);
        values.put(Im.DATA, handle);
        values.put(Im.TYPE, Im.TYPE_HOME);

        Uri resultUri = mResolver.insert(Data.CONTENT_URI, values);
        return resultUri;
    }

    protected void setContactAccountName(long contactId, String accountName) {
        ContentValues values = new ContentValues();
        values.put(RawContacts.ACCOUNT_NAME, accountName);

        mResolver.update(ContentUris.withAppendedId(
                RawContacts.CONTENT_URI, contactId), values, null, null);
    }

    protected void setAggregationException(int type, long aggregateId, long contactId) {
        ContentValues values = new ContentValues();
        values.put(AggregationExceptions.AGGREGATE_ID, aggregateId);
        values.put(AggregationExceptions.CONTACT_ID, contactId);
        values.put(AggregationExceptions.TYPE, type);
        mResolver.update(AggregationExceptions.CONTENT_URI, values, null, null);
    }

    protected Cursor queryContact(long contactId) {
        return mResolver.query(ContentUris.withAppendedId(RawContacts.CONTENT_URI, contactId), null,
                null, null, null);
    }

    protected Cursor queryAggregate(long aggregateId) {
        return mResolver.query(ContentUris.withAppendedId(Aggregates.CONTENT_URI, aggregateId),
                null, null, null, null);
    }

    protected Cursor queryAggregateSummary(long aggregateId, String[] projection) {
        return mResolver.query(ContentUris.withAppendedId(Aggregates.CONTENT_SUMMARY_URI,
                aggregateId), projection, null, null, null);
    }

    protected Cursor queryAggregateSummary() {
        return mResolver.query(Aggregates.CONTENT_SUMMARY_URI, null, null, null, null);
    }

    protected long queryAggregateId(long contactId) {
        Cursor c = queryContact(contactId);
        assertTrue(c.moveToFirst());
        long aggregateId = c.getLong(c.getColumnIndex(RawContacts.AGGREGATE_ID));
        c.close();
        return aggregateId;
    }

    protected long queryPhotoId(long aggregateId) {
        Cursor c = queryAggregate(aggregateId);
        assertTrue(c.moveToFirst());
        long photoId = c.getInt(c.getColumnIndex(Aggregates.PHOTO_ID));
        c.close();
        return photoId;
    }

    protected String queryDisplayName(long aggregateId) {
        Cursor c = queryAggregate(aggregateId);
        assertTrue(c.moveToFirst());
        String displayName = c.getString(c.getColumnIndex(Aggregates.DISPLAY_NAME));
        c.close();
        return displayName;
    }

    protected void assertAggregated(long contactId1, long contactId2) {
        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 == aggregateId2);
    }

    protected void assertAggregated(long contactId1, long contactId2, String expectedDisplayName) {
        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 == aggregateId2);

        String displayName = queryDisplayName(aggregateId1);
        assertEquals(expectedDisplayName, displayName);
    }

    protected void assertNotAggregated(long contactId1, long contactId2) {
        long aggregateId1 = queryAggregateId(contactId1);
        long aggregateId2 = queryAggregateId(contactId2);
        assertTrue(aggregateId1 != aggregateId2);
    }

    protected void assertStructuredName(long contactId, String prefix, String givenName,
            String middleName, String familyName, String suffix) {
        Uri uri =
                Uri.withAppendedPath(ContentUris.withAppendedId(RawContacts.CONTENT_URI, contactId),
                RawContacts.Data.CONTENT_DIRECTORY);

        final String[] projection = new String[] {
                StructuredName.PREFIX, StructuredName.GIVEN_NAME, StructuredName.MIDDLE_NAME,
                StructuredName.FAMILY_NAME, StructuredName.SUFFIX
        };

        Cursor c = mResolver.query(uri, projection, Data.MIMETYPE + "='"
                + StructuredName.CONTENT_ITEM_TYPE + "'", null, null);

        assertTrue(c.moveToFirst());
        assertEquals(prefix, c.getString(0));
        assertEquals(givenName, c.getString(1));
        assertEquals(middleName, c.getString(2));
        assertEquals(familyName, c.getString(3));
        assertEquals(suffix, c.getString(4));
        c.close();
    }

    protected long assertSingleGroup(Long rowId, Account account, String sourceId, String title) {
        Cursor c = mResolver.query(Groups.CONTENT_URI, null, null, null, null);
        try {
            assertTrue(c.moveToNext());
            long actualRowId = assertGroup(c, rowId, account, sourceId, title);
            assertFalse(c.moveToNext());
            return actualRowId;
        } finally {
            c.close();
        }
    }

    protected long assertSingleGroupMembership(Long rowId, Long contactId, Long groupRowId,
            String sourceId) {
        Cursor c = mResolver.query(ContactsContract.Data.CONTENT_URI, null, null, null, null);
        try {
            assertTrue(c.moveToNext());
            long actualRowId = assertGroupMembership(c, rowId, contactId, groupRowId, sourceId);
            assertFalse(c.moveToNext());
            return actualRowId;
        } finally {
            c.close();
        }
    }

    protected long assertGroupMembership(Cursor c, Long rowId, Long contactId, Long groupRowId,
            String sourceId) {
        assertNullOrEquals(c, rowId, Data._ID);
        assertNullOrEquals(c, contactId, GroupMembership.CONTACT_ID);
        assertNullOrEquals(c, groupRowId, GroupMembership.GROUP_ROW_ID);
        assertNullOrEquals(c, sourceId, GroupMembership.GROUP_SOURCE_ID);
        return c.getLong(c.getColumnIndexOrThrow("_id"));
    }

    protected long assertGroup(Cursor c, Long rowId, Account account, String sourceId, String title) {
        assertNullOrEquals(c, rowId, Groups._ID);
        assertNullOrEquals(c, account);
        assertNullOrEquals(c, sourceId, Groups.SOURCE_ID);
        assertNullOrEquals(c, title, Groups.TITLE);
        return c.getLong(c.getColumnIndexOrThrow("_id"));
    }

    private void assertNullOrEquals(Cursor c, Account account) {
        if (account == NO_ACCOUNT) {
            return;
        }
        if (account == null) {
            assertTrue(c.isNull(c.getColumnIndexOrThrow(Groups.ACCOUNT_NAME)));
            assertTrue(c.isNull(c.getColumnIndexOrThrow(Groups.ACCOUNT_TYPE)));
        } else {
            assertEquals(account.mName, c.getString(c.getColumnIndexOrThrow(Groups.ACCOUNT_NAME)));
            assertEquals(account.mType, c.getString(c.getColumnIndexOrThrow(Groups.ACCOUNT_TYPE)));
        }
    }

    private void assertNullOrEquals(Cursor c, Long value, String columnName) {
        if (value != NO_LONG) {
            if (value == null) assertTrue(c.isNull(c.getColumnIndexOrThrow(columnName)));
            else assertEquals((long) value, c.getLong(c.getColumnIndexOrThrow(columnName)));
        }
    }

    private void assertNullOrEquals(Cursor c, String value, String columnName) {
        if (value != NO_STRING) {
            if (value == null) assertTrue(c.isNull(c.getColumnIndexOrThrow(columnName)));
            else assertEquals(value, c.getString(c.getColumnIndexOrThrow(columnName)));
        }
    }

    protected void assertDataRow(ContentValues actual, String expectedMimetype,
            Object... expectedArguments) {
        assertEquals(actual.toString(), expectedMimetype, actual.getAsString(Data.MIMETYPE));
        for (int i = 0; i < expectedArguments.length; i += 2) {
            String columnName = (String) expectedArguments[i];
            Object expectedValue = expectedArguments[i + 1];
            if (expectedValue instanceof Uri) {
                expectedValue = ContentUris.parseId((Uri) expectedValue);
            }
            if (expectedValue == null) {
                assertNull(actual.toString(), actual.get(columnName));
            }
            if (expectedValue instanceof Long) {
                assertEquals(actual.toString(), expectedValue, actual.getAsLong(columnName));
            } else if (expectedValue instanceof Integer) {
                assertEquals(actual.toString(), expectedValue, actual.getAsInteger(columnName));
            } else if (expectedValue instanceof String) {
                assertEquals(actual.toString(), expectedValue, actual.getAsString(columnName));
            } else {
                assertEquals(actual.toString(), expectedValue, actual.get(columnName));
            }
        }
    }

    protected static class IdComparator implements Comparator<ContentValues> {
        public int compare(ContentValues o1, ContentValues o2) {
            long id1 = o1.getAsLong(ContactsContract.Data._ID);
            long id2 = o2.getAsLong(ContactsContract.Data._ID);
            if (id1 == id2) return 0;
            return (id1 < id2) ? -1 : 1;
        }
    }

    protected ContentValues[] asSortedContentValuesArray(
            ArrayList<Entity.NamedContentValues> subValues) {
        ContentValues[] result = new ContentValues[subValues.size()];
        int i = 0;
        for (Entity.NamedContentValues subValue : subValues) {
            result[i] = subValue.values;
            i++;
        }
        Arrays.sort(result, new IdComparator());
        return result;
    }

    protected void assertDirty(Uri uri, boolean state) {
        Cursor c = mResolver.query(uri, new String[]{"dirty"}, null, null, null);
        assertTrue(c.moveToNext());
        assertEquals(state, c.getLong(0) != 0);
        assertFalse(c.moveToNext());
    }

    protected long getVersion(Uri uri) {
        Cursor c = mResolver.query(uri, new String[]{"version"}, null, null, null);
        assertTrue(c.moveToNext());
        long version = c.getLong(0);
        assertFalse(c.moveToNext());
        return version;
    }

    protected void clearDirty(Uri uri) {
        ContentValues values = new ContentValues();
        values.put("dirty", 0);
        mResolver.update(uri, values, null, null);
    }

    protected void assertStoredValues(Uri rowUri, String column, String expectedValue) {
        String value = getStoredValue(rowUri, column);
        assertEquals("Column value " + column, expectedValue, value);
    }

    protected String getStoredValue(Uri rowUri, String column) {
        String value;
        Cursor c = mResolver.query(rowUri, new String[] { column }, null, null, null);
        try {
            c.moveToFirst();
            value = c.getString(c.getColumnIndex(column));
        } finally {
            c.close();
        }
        return value;
    }

    protected void assertStoredValues(Uri rowUri, ContentValues expectedValues) {
        Cursor c = mResolver.query(rowUri, null, null, null, null);
        try {
            assertEquals("Record count", 1, c.getCount());
            c.moveToFirst();
            assertCursorValues(c, expectedValues);
        } finally {
            c.close();
        }
    }

    /**
     * Constructs a selection (where clause) out of all supplied values, uses it
     * to query the provider and verifies that a single row is returned and it
     * has the same values as requested.
     */
    protected void assertSelection(Uri uri, ContentValues values, String idColumn, long id) {
        StringBuilder sb = new StringBuilder();
        ArrayList<String> selectionArgs = new ArrayList<String>(values.size());
        sb.append(idColumn).append("=").append(id);
        Set<Map.Entry<String, Object>> entries = values.valueSet();
        for (Map.Entry<String, Object> entry : entries) {
            String column = entry.getKey();
            Object value = entry.getValue();
            sb.append(" AND ").append(column);
            if (value == null) {
                sb.append(" IS NULL");
            } else {
                sb.append("=?");
                selectionArgs.add(String.valueOf(value));
            }
        }

        Cursor c = mResolver.query(uri, null, sb.toString(), selectionArgs.toArray(new String[0]),
                null);
        try {
            assertEquals("Record count", 1, c.getCount());
            c.moveToFirst();
            assertCursorValues(c, values);
        } finally {
            c.close();
        }
    }

    protected void assertCursorValues(Cursor cursor, ContentValues expectedValues) {
        Set<Map.Entry<String, Object>> entries = expectedValues.valueSet();
        for (Map.Entry<String, Object> entry : entries) {
            String column = entry.getKey();
            int index = cursor.getColumnIndex(column);
            assertTrue("No such column: " + column, index != -1);
            Object expectedValue = expectedValues.get(column);
            String value;
            if (expectedValue instanceof byte[]) {
                expectedValue = Hex.encodeHex((byte[])expectedValue, false);
                value = Hex.encodeHex(cursor.getBlob(index), false);
            } else {
                expectedValue = expectedValues.getAsString(column);
                value = cursor.getString(index);
            }
            assertEquals("Column value " + column, expectedValue, value);
        }
    }

    protected int getCount(Uri uri, String selection, String[] selectionArgs) {
        Cursor c = mResolver.query(uri, null, selection, selectionArgs, null);
        try {
            return c.getCount();
        } finally {
            c.close();
        }
    }
}
