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

import android.accounts.Account;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.platform.test.annotations.EnableFlags;
import android.platform.test.flag.junit.SetFlagsRule;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;

import androidx.test.filters.MediumTest;

import com.android.providers.contacts.flags.Flags;
import com.android.providers.contacts.testutil.DataUtil;
import com.android.providers.contacts.testutil.RawContactUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link ContactsProvider2} Move API.
 *
 * Run the test like this:
 * <code>
   adb shell am instrument -e class com.android.providers.contacts.MoveRawContactsTest -w \
           com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@MediumTest
@RunWith(JUnit4.class)
public class MoveRawContactsTest extends BaseContactsProvider2Test {
    @ClassRule public static final SetFlagsRule.ClassRule mClassRule = new SetFlagsRule.ClassRule();

    @Rule public final SetFlagsRule mSetFlagsRule = mClassRule.createSetFlagsRule();

    static final Account SOURCE_ACCOUNT = new Account("sourceName", "sourceType");
    static final Account DEST_ACCOUNT = new Account("destName", "destType");
    static final Account DEST_ACCOUNT_WITH_SOURCE_TYPE = new Account("destName", "sourceType");

    static final String SOURCE_ID = "uniqueSourceId";

    static final String NON_PORTABLE_MIMETYPE = "test/mimetype";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertMovedContactIsDeleted(long rawContactId,
            AccountWithDataSet account) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(RawContacts._ID, rawContactId);
        contentValues.put(RawContacts.DELETED, 1);
        contentValues.put(RawContacts.ACCOUNT_NAME, account.getAccountName());
        contentValues.put(RawContacts.ACCOUNT_TYPE, account.getAccountType());
        assertStoredValues(RawContacts.CONTENT_URI,
                RawContacts._ID + " = ?",
                new String[]{String.valueOf(rawContactId)},
                contentValues);
    }

    private void assertMovedRawContact(long rawContactId, AccountWithDataSet account,
            boolean isStarred) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(RawContacts._ID, rawContactId);
        contentValues.put(RawContacts.DELETED, 0);
        contentValues.put(RawContacts.STARRED, isStarred ? 1 : 0);
        contentValues.putNull(RawContacts.SOURCE_ID);
        contentValues.put(RawContacts.ACCOUNT_NAME, account.getAccountName());
        contentValues.put(RawContacts.ACCOUNT_TYPE, account.getAccountType());
        assertStoredValues(RawContacts.CONTENT_URI,
                RawContacts._ID + " = ?",
                new String[]{String.valueOf(rawContactId)},
                contentValues);
    }

    private void assertMoveStubExists(long rawContactId, String sourceId,
            AccountWithDataSet account) {
        assertEquals(1, getCount(RawContacts.CONTENT_URI,
                RawContacts._ID + " <> ? and " + RawContacts.SOURCE_ID + " = ? and "
                        + RawContacts.DELETED + " = 1 and " + RawContacts.ACCOUNT_NAME + " = ? and "
                        + RawContacts.ACCOUNT_TYPE + " = ?",
                new String[] {
                        Long.toString(rawContactId),
                        sourceId,
                        account.getAccountName(),
                        account.getAccountType()
                }));
    }

    private void assertMoveStubDoesNotExist(long rawContactId, AccountWithDataSet account) {
        assertEquals(0, getCount(RawContacts.CONTENT_URI,
                RawContacts._ID + " <> ? and "
                        + RawContacts.DELETED + " = 1 and " + RawContacts.ACCOUNT_NAME + " = ? and "
                        + RawContacts.ACCOUNT_TYPE + " = ?",
                new String[] {
                        Long.toString(rawContactId),
                        account.getAccountName(),
                        account.getAccountType()
                }));
    }

    private long createStarredRawContactForMove(String firstName, String lastName, String sourceId,
            Account account) {
        long rawContactId = RawContactUtil.createRawContactWithName(
                mResolver, firstName, lastName, account);
        ContentValues rawContactValues = new ContentValues();
        rawContactValues.put(RawContacts.SOURCE_ID, sourceId);
        rawContactValues.put(RawContacts.STARRED, 1);

        if (account == null) {
            rawContactValues.putNull(RawContacts.ACCOUNT_NAME);
            rawContactValues.putNull(RawContacts.ACCOUNT_TYPE);
        } else {
            rawContactValues.put(RawContacts.ACCOUNT_NAME, account.name);
            rawContactValues.put(RawContacts.ACCOUNT_TYPE, account.type);
        }

        RawContactUtil.update(mResolver, rawContactId, rawContactValues);
        return rawContactId;
    }

    private void insertNonPortableData(
            ContentResolver resolver, long rawContactId, String data1) {
        ContentValues values = new ContentValues();
        values.put(Data.DATA1, data1);
        values.put(Data.MIMETYPE, NON_PORTABLE_MIMETYPE);
        values.put(Data.RAW_CONTACT_ID, rawContactId);
        resolver.insert(Data.CONTENT_URI, values);
    }

    private void assertData(long rawContactId, String mimetype, String data1, int expectedCount) {
        assertEquals(expectedCount, getCount(Data.CONTENT_URI,
                Data.RAW_CONTACT_ID + " == ? AND "
                        + Data.MIMETYPE + " = ? AND "
                        + Data.DATA1 + " = ?",
                new String[] {
                        Long.toString(rawContactId),
                        mimetype,
                        data1
                }));
    }

    private void assertDataExists(long rawContactId, String mimetype, String data1) {
        assertData(rawContactId, mimetype, data1, 1);
    }

    private void assertDataDoesNotExist(long rawContactId, String mimetype, String data1) {
        assertData(rawContactId, mimetype, data1, 0);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveDuplicateRawContacts() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a duplicate pair of contacts
        long sourceDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT);

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedContactIsDeleted(sourceDupeRawContactId, source);

        // verify the duplicate destination contact is unaffected
        assertMovedRawContact(destDupeRawContactId, dest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsWithDataRows() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId1 = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        long destRawContactId2 = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        // create a combination of data rows
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstA", "lastA");
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstB", "lastB");
        DataUtil.insertStructuredName(mResolver, destRawContactId1, "firstA", "lastA");
        DataUtil.insertStructuredName(mResolver, destRawContactId2, "firstB", "lastB");

        // trigger the move
        cp.moveRawContacts(source, dest);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, source);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(sourceRawContactId, dest, false);

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId1, dest, false);
        assertMovedRawContact(destRawContactId2, dest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContacts() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a near duplicate in the destination account
        long destContactId = RawContactUtil.createRawContactWithName(
                mResolver, "Foo", "Bar", DEST_ACCOUNT);

        // create a near duplicate, unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify a stub has been written for the unique raw contact in the source account
        assertMoveStubExists(uniqueContactId, SOURCE_ID, source);

        // verify the original near duplicate contact remains unchanged (still not starred)
        assertMovedRawContact(destContactId, dest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsFromNullAccount() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(null, null, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a near duplicate in the destination account
        long destContactId = RawContactUtil.createRawContactWithName(
                mResolver, "Foo", "Bar", DEST_ACCOUNT);

        // create a near duplicate, unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, /* account= */ null);

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify we didn't write a stub since null accounts don't need them (they're not synced)
        assertEquals(0, getCount(RawContacts.CONTENT_URI,
                RawContacts._ID + " <> ? and " + RawContacts.SOURCE_ID + " = ? and "
                        + RawContacts.DELETED + " = 1 and " + RawContacts.ACCOUNT_NAME + " IS NULL"
                        + " and " + RawContacts.ACCOUNT_TYPE + " IS NULL",
                new String[] {
                        Long.toString(uniqueContactId),
                        SOURCE_ID
                }));

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destContactId, dest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsFromNullAccountToEmptyDestination() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(null, null, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, /* account= */ null);

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify we didn't write a stub since null accounts don't need them (they're not synced)
        assertEquals(0, getCount(RawContacts.CONTENT_URI,
                RawContacts._ID + " <> ? and " + RawContacts.SOURCE_ID + " = ? and "
                        + RawContacts.DELETED + " = 1 and " + RawContacts.ACCOUNT_NAME + " IS NULL"
                        + " and " + RawContacts.ACCOUNT_TYPE + " IS NULL",
                new String[] {
                        Long.toString(uniqueContactId),
                        SOURCE_ID
                }));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsToNullAccount() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(null, null, null);

        // create a unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify a stub has been written for the unique raw contact in the source account
        assertMoveStubExists(uniqueContactId, SOURCE_ID, source);
    }

    /**
     * Move a contact between source and dest where both account have different account types.
     * The contact is unique because of a non-portable data row, because the account types don't
     * match, the non-portable data row will be deleted before matching the contacts and the contact
     * will be deleted as a duplicate.
     */
    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactWithNonPortableDataRows() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);

        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        // create a combination of data rows
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstA", "lastA");
        insertNonPortableData(mResolver, sourceRawContactId, "foo");
        DataUtil.insertStructuredName(mResolver, destRawContactId, "firstA", "lastA");

        // trigger the move
        cp.moveRawContacts(source, dest);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, source);

        // verify the unique raw contact has been deleted as a duplicate
        assertMovedContactIsDeleted(sourceRawContactId, source);
        assertDataDoesNotExist(sourceRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataDoesNotExist(
                sourceRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");


        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId, dest, false);
        // the non portable data should still not exist on the destination account
        assertDataDoesNotExist(destRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        // the existing data row in the destination account should be unaffected
        assertDataExists(destRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");
    }

    /**
     * Moves a contact between source and dest where both accounts have the same account type.
    *  The contact is unique because of a non-portable data row. Because the account types match,
    *  the non-portable data row will be considered while matching the contacts and the contact will
    *  be treated as unique.
     */
    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsWithNonPortableDataRowsAccountTypesMatch() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT_WITH_SOURCE_TYPE});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT_WITH_SOURCE_TYPE.name,
                        DEST_ACCOUNT_WITH_SOURCE_TYPE.type, null);

        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT_WITH_SOURCE_TYPE);
        // create a combination of data rows
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstA", "lastA");
        insertNonPortableData(mResolver, sourceRawContactId, "foo");
        DataUtil.insertStructuredName(mResolver, destRawContactId, "firstA", "lastA");

        // trigger the move
        cp.moveRawContacts(source, dest);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, source);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(sourceRawContactId, dest, false);
        // all data rows should have moved with the source
        assertDataExists(sourceRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataExists(sourceRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId, dest, false);
        // the non portable data should still not exist on the destination account
        assertDataDoesNotExist(destRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        // the existing data row in the destination account should be unaffected
        assertDataExists(destRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");
    }

    /**
     * Moves a contact between source and dest where both accounts have the same account type.
     * The contact is unique because of a non-portable data row. Because the account types match,
     * the non-portable data row will be considered while matching the contacts and the contact will
     * be treated as a duplicate.
     */
    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveDuplicateRawContactsWithNonPortableDataRowsAccountTypesMatch() {
        ContactsProvider2 cp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT_WITH_SOURCE_TYPE});
        AccountWithDataSet source =
                AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        AccountWithDataSet dest =
                AccountWithDataSet.get(DEST_ACCOUNT_WITH_SOURCE_TYPE.name,
                        DEST_ACCOUNT_WITH_SOURCE_TYPE.type, null);

        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT_WITH_SOURCE_TYPE);
        // create a combination of data rows
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstA", "lastA");
        insertNonPortableData(mResolver, sourceRawContactId, "foo");
        DataUtil.insertStructuredName(mResolver, destRawContactId, "firstA", "lastA");
        insertNonPortableData(mResolver, destRawContactId, "foo");

        // trigger the move
        cp.moveRawContacts(source, dest);

        // verify the duplicate contact has been deleted
        assertMovedContactIsDeleted(sourceRawContactId, source);
        assertDataDoesNotExist(sourceRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataDoesNotExist(
                sourceRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId, dest, false);
        assertDataExists(destRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataExists(destRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");
    }
}
