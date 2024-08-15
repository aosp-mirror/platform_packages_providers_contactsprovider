/*
 * Copyright (C) 2024 The Android Open Source Project
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
import android.database.Cursor;
import android.platform.test.annotations.EnableFlags;
import android.platform.test.flag.junit.SetFlagsRule;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Groups;
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

import java.util.List;
import java.util.Set;

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

    static final String RES_PACKAGE = "testpackage";

    ContactsProvider2 mCp;
    AccountWithDataSet mSource;
    AccountWithDataSet mDest;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        mCp = (ContactsProvider2) getProvider();
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT});
        mSource = AccountWithDataSet.get(SOURCE_ACCOUNT.name, SOURCE_ACCOUNT.type, null);
        mDest = AccountWithDataSet.get(DEST_ACCOUNT.name, DEST_ACCOUNT.type, null);
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
        contentValues.put(RawContacts.DIRTY, 1);
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
                        + RawContacts.ACCOUNT_TYPE + " = ? and " + RawContacts.DIRTY + " = 1",
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

    private Long createGroupWithMembers(AccountWithDataSet account,
            String title, String titleRes, List<Long> memberIds) {
        ContentValues values = new ContentValues();
        values.put(Groups.TITLE, title);
        values.put(Groups.TITLE_RES, titleRes);
        values.put(Groups.RES_PACKAGE, RES_PACKAGE);
        values.put(Groups.ACCOUNT_NAME, account.getAccountName());
        values.put(Groups.ACCOUNT_TYPE, account.getAccountType());
        values.put(Groups.DATA_SET, account.getDataSet());
        mResolver.insert(Groups.CONTENT_URI, values);
        Long groupId = getGroupWithName(account, title, titleRes);

        for (Long rawContactId: memberIds) {
            values = new ContentValues();
            values.put(GroupMembership.GROUP_ROW_ID, groupId);
            values.put(GroupMembership.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
            mResolver.insert(Data.CONTENT_URI, values);
        }
        return groupId;
    }

    private void promoteToSystemGroup(Long groupId, String systemId, boolean isReadOnly) {
        ContentValues values = new ContentValues();
        values.put(Groups.SYSTEM_ID, systemId);
        values.put(Groups.GROUP_IS_READ_ONLY, isReadOnly ? 1 : 0);
        mResolver.update(Groups.CONTENT_URI, values,
                Groups._ID + " = ?",
                new String[]{
                        groupId.toString()
                });
    }

    private void setGroupSourceId(Long groupId, String sourceId) {
        ContentValues values = new ContentValues();
        values.put(Groups.SOURCE_ID, sourceId);
        mResolver.update(Groups.CONTENT_URI, values,
                Groups._ID + " = ?",
                new String[]{
                        groupId.toString()
                });
    }

    private void assertInGroup(Long rawContactId, Long groupId) {
        assertEquals(1, getCount(Data.CONTENT_URI,
                GroupMembership.GROUP_ROW_ID + " == ? AND "
                        + Data.MIMETYPE + " = ? AND "
                        + GroupMembership.RAW_CONTACT_ID + " = ?",
                new String[] {
                        Long.toString(groupId),
                        GroupMembership.CONTENT_ITEM_TYPE,
                        Long.toString(rawContactId)
                }));
    }

    private void assertGroupState(Long groupId, AccountWithDataSet account, Set<Long> members,
            boolean isDeleted) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(Groups._ID, groupId);
        contentValues.put(Groups.DELETED, isDeleted ? 1 : 0);
        contentValues.put(Groups.ACCOUNT_NAME, account.getAccountName());
        contentValues.put(Groups.ACCOUNT_TYPE, account.getAccountType());
        contentValues.put(Groups.RES_PACKAGE, RES_PACKAGE);
        contentValues.put(Groups.DIRTY, 1);
        assertStoredValues(Groups.CONTENT_URI,
                Groups._ID + " = ?",
                new String[]{String.valueOf(groupId)},
                contentValues);

        assertEquals(members.size(), getCount(Data.CONTENT_URI,
                GroupMembership.GROUP_ROW_ID + " == ? AND "
                        + Data.MIMETYPE + " = ?",
                new String[] {
                        Long.toString(groupId),
                        GroupMembership.CONTENT_ITEM_TYPE
                }));

        for (Long member: members) {
            assertInGroup(member, groupId);
        }
    }

    private void assertGroup(Long groupId, AccountWithDataSet account, Set<Long> members) {
        assertGroupState(groupId, account, members, /* isDeleted= */ false);
    }

    private void assertGroupDeleted(Long groupId, AccountWithDataSet account) {
        assertGroupState(groupId, account, Set.of(), /* isDeleted= */ true);
    }

    private void assertGroupMoveStubExists(long groupId, String sourceId,
            AccountWithDataSet account) {
        assertEquals(1, getCount(Groups.CONTENT_URI,
                Groups._ID + " <> ? and " + Groups.SOURCE_ID + " = ? and "
                        + Groups.DELETED + " = 1 and " + Groups.ACCOUNT_NAME + " = ? and "
                        + Groups.ACCOUNT_TYPE + " = ? and " + Groups.DIRTY + " = 1",
                new String[] {
                        Long.toString(groupId),
                        sourceId,
                        account.getAccountName(),
                        account.getAccountType()
                }));
    }

    private Long getGroupWithName(AccountWithDataSet account, String title, String titleRes) {
        try (Cursor c = mResolver.query(Groups.CONTENT_URI,
                new String[] { Groups._ID, },
                Groups.ACCOUNT_NAME + " = ? AND "
                        + Groups.ACCOUNT_TYPE + " = ? AND "
                        + Groups.TITLE + " = ? AND "
                        + Groups.TITLE_RES + " = ?",
                new String[] {
                        account.getAccountName(),
                        account.getAccountType(),
                        title,
                        titleRes
                },
                null)) {
            assertNotNull(c);
            c.moveToFirst();
            return c.getLong(0);
        }
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveDuplicateRawContacts() {
        // create a duplicate pair of contacts
        long sourceDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT);

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedContactIsDeleted(sourceDupeRawContactId, mSource);

        // verify the duplicate destination contact is unaffected
        assertMovedRawContact(destDupeRawContactId, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsWithDataRows() {
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
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, mSource);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(sourceRawContactId, mDest, false);

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId1, mDest, false);
        assertMovedRawContact(destRawContactId2, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContacts() {
        // create a near duplicate in the destination account
        long destContactId = RawContactUtil.createRawContactWithName(
                mResolver, "Foo", "Bar", DEST_ACCOUNT);

        // create a near duplicate, unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, mDest, true);

        // verify a stub has been written for the unique raw contact in the source account
        assertMoveStubExists(uniqueContactId, SOURCE_ID, mSource);

        // verify the original near duplicate contact remains unchanged (still not starred)
        assertMovedRawContact(destContactId, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsStubDisabled() {
        // create a near duplicate in the destination account
        long destContactId = RawContactUtil.createRawContactWithName(
                mResolver, "Foo", "Bar", DEST_ACCOUNT);

        // create a near duplicate, unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ false);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, mDest, true);

        // verify a stub has been written for the unique raw contact in the source account
        assertMoveStubDoesNotExist(uniqueContactId, mSource);

        // verify no stub was created (since we've disabled stub creation)
        assertMovedRawContact(destContactId, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsFromNullAccount() {
        mActor.setAccounts(new Account[]{DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(null, null, null);

        // create a near duplicate in the destination account
        long destContactId = RawContactUtil.createRawContactWithName(
                mResolver, "Foo", "Bar", DEST_ACCOUNT);

        // create a near duplicate, unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, /* account= */ null);

        // trigger the move
        mCp.moveRawContacts(source, mDest, /* insertSyncStubs= */ true);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, mDest, true);

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
        assertMovedRawContact(destContactId, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsFromNullAccountToEmptyDestination() {
        mActor.setAccounts(new Account[]{DEST_ACCOUNT});
        AccountWithDataSet source =
                AccountWithDataSet.get(null, null, null);

        // create a unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, /* account= */ null);

        // trigger the move
        mCp.moveRawContacts(source, mDest, /* insertSyncStubs= */ true);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, mDest, true);

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
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT});
        AccountWithDataSet dest =
                AccountWithDataSet.get(null, null, null);

        // create a unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        mCp.moveRawContacts(mSource, dest, /* insertSyncStubs= */ true);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify a stub has been written for the unique raw contact in the source account
        assertMoveStubExists(uniqueContactId, SOURCE_ID, mSource);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsToNullAccountStubDisabled() {
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT});
        AccountWithDataSet dest =
                AccountWithDataSet.get(null, null, null);

        // create a unique contact in the source account
        long uniqueContactId = createStarredRawContactForMove(
                "Foo", "Bar", SOURCE_ID, SOURCE_ACCOUNT);

        // trigger the move
        mCp.moveRawContacts(mSource, dest, /* insertSyncStubs= */ false);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(uniqueContactId, dest, true);

        // verify no stub was created (since stub creation is disabled)
        assertMoveStubDoesNotExist(uniqueContactId, mSource);
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
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        // create a combination of data rows
        DataUtil.insertStructuredName(mResolver, sourceRawContactId, "firstA", "lastA");
        insertNonPortableData(mResolver, sourceRawContactId, "foo");
        DataUtil.insertStructuredName(mResolver, destRawContactId, "firstA", "lastA");

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, mSource);

        // verify the unique raw contact has been deleted as a duplicate
        assertMovedContactIsDeleted(sourceRawContactId, mSource);
        assertDataDoesNotExist(sourceRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataDoesNotExist(
                sourceRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");


        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId, mDest, false);
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
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT_WITH_SOURCE_TYPE});
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
        mCp.moveRawContacts(mSource, dest, /* insertSyncStubs= */ true);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, mSource);

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
        mActor.setAccounts(new Account[]{SOURCE_ACCOUNT, DEST_ACCOUNT_WITH_SOURCE_TYPE});
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
        mCp.moveRawContacts(mSource, dest, /* insertSyncStubs= */ true);

        // verify the duplicate contact has been deleted
        assertMovedContactIsDeleted(sourceRawContactId, mSource);
        assertDataDoesNotExist(sourceRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataDoesNotExist(
                sourceRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId, dest, false);
        assertDataExists(destRawContactId, NON_PORTABLE_MIMETYPE, "foo");
        assertDataExists(destRawContactId, StructuredName.CONTENT_ITEM_TYPE, "firstA lastA");
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveDuplicateNonSystemGroup() {
        // create a duplicate pair of contacts
        long sourceDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceDupeRawContactId));
        setGroupSourceId(sourceGroup, SOURCE_ID);
        long destGroup = createGroupWithMembers(mDest, "groupTitle",
                "groupTitleRes", List.of(destDupeRawContactId));

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify the duplicate raw contact in dest has been deleted in place instead of creating
        // a stub (because this is a duplicate non-system group, we delete in-place even if there's
        // a source ID)
        assertMovedContactIsDeleted(sourceDupeRawContactId, mSource);

        // since sourceGroup was a duplicate of destGroup, it was deleted in place
        assertGroupDeleted(sourceGroup, mSource);

        // verify the duplicate destination contact is unaffected
        assertMovedRawContact(destDupeRawContactId, mDest, false);
        assertGroup(destGroup, mDest, Set.of(destDupeRawContactId));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueNonSystemGroup() {
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceRawContactId));


        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify group and contact have been moved from the source account to the dest account
        assertMovedRawContact(sourceRawContactId, mDest, false);
        assertGroup(sourceGroup, mDest, Set.of(sourceRawContactId));

        // check that the only group in source got moved and no stub was written
        assertEquals(0, getCount(Groups.CONTENT_URI,
                Groups.ACCOUNT_NAME + " = ? AND "
                        + Groups.ACCOUNT_TYPE + " = ?",
                new String[] {
                        mSource.getAccountName(),
                        mSource.getAccountType()
                }));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueNonSystemGroupWithSourceId() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceRawContactId));
        setGroupSourceId(sourceGroup, SOURCE_ID);


        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify group and contact have been moved from the source account to the dest account
        assertMovedRawContact(sourceRawContactId, mDest, false);
        assertGroup(sourceGroup, mDest, Set.of(sourceRawContactId));

        // verify we created a move stub (since this was a unique non-system group with a source ID)
        assertGroupMoveStubExists(sourceGroup, SOURCE_ID, mSource);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueNonSystemGroupWithSourceIdStubsDisabled() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceRawContactId));
        setGroupSourceId(sourceGroup, SOURCE_ID);


        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ false);

        // verify group and contact have been moved from the source account to the dest account
        assertMovedRawContact(sourceRawContactId, mDest, false);
        assertGroup(sourceGroup, mDest, Set.of(sourceRawContactId));

        // check that the only group in source got moved and no stub was written (because we
        // disabled stub creation)
        assertEquals(0, getCount(Groups.CONTENT_URI,
                Groups.ACCOUNT_NAME + " = ? AND "
                        + Groups.ACCOUNT_TYPE + " = ?",
                new String[] {
                        mSource.getAccountName(),
                        mSource.getAccountType()
                }));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueRawContactsWithGroups() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destRawContactId1 = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        long destRawContactId2 = RawContactUtil.createRawContactWithName(mResolver, DEST_ACCOUNT);
        // create a combination of data rows
        long sourceGroup1 = createGroupWithMembers(
                mSource, "group1Title", "group1TitleRes",
                List.of(sourceRawContactId));
        long sourceGroup2 = createGroupWithMembers(
                mSource, "group2Title", "group2TitleRes",
                List.of(sourceRawContactId));
        promoteToSystemGroup(sourceGroup2, null, true);
        long destGroup1 = createGroupWithMembers(
                mDest, "group1Title", "group1TitleRes",
                List.of(destRawContactId1));
        long destGroup2 = createGroupWithMembers(
                mDest, "group2Title", "group2TitleRes",
                List.of(destRawContactId2));

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // Verify no stub was written since no source ID existed
        assertMoveStubDoesNotExist(sourceRawContactId, mSource);

        // verify the unique raw contact has been moved from the old -> new account
        assertMovedRawContact(sourceRawContactId, mDest, false);

        // check the source contact got moved into the new group
        assertGroup(destGroup1, mDest, Set.of(sourceRawContactId, destRawContactId1));
        assertGroup(destGroup2, mDest, Set.of(sourceRawContactId, destRawContactId2));
        assertGroupDeleted(sourceGroup1, mSource);
        assertGroup(sourceGroup2, mSource, Set.of());

        // verify the original near duplicate contact remains unchanged
        assertMovedRawContact(destRawContactId1, mDest, false);
        assertMovedRawContact(destRawContactId2, mDest, false);
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveDuplicateSystemGroup() {
        // create a duplicate pair of contacts
        long sourceDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long destDupeRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                DEST_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceDupeRawContactId));
        promoteToSystemGroup(sourceGroup, null, true);
        setGroupSourceId(sourceGroup, SOURCE_ID);
        long destGroup = createGroupWithMembers(mDest, "groupTitle",
                "groupTitleRes", List.of(destDupeRawContactId));

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedContactIsDeleted(sourceDupeRawContactId, mSource);

        // Source group is a system group so it shouldn't get deleted
        assertGroup(sourceGroup, mSource, Set.of());

        // verify the duplicate destination contact is unaffected
        assertMovedRawContact(destDupeRawContactId, mDest, false);

        // The destination contact is the only one in destGroup since the source and destination
        // contacts were true duplicates
        assertGroup(destGroup, mDest, Set.of(destDupeRawContactId));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testMoveUniqueSystemGroup() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceRawContactId));
        promoteToSystemGroup(sourceGroup, null, true);
        setGroupSourceId(sourceGroup, SOURCE_ID);

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedRawContact(sourceRawContactId, mDest, false);

        // since sourceGroup is a system group, it cannot be deleted
        assertGroup(sourceGroup, mSource, Set.of());

        // verify that a copied group exists in dest now
        long newGroup = getGroupWithName(mDest, "groupTitle", "groupTitleRes");
        assertGroup(newGroup, mDest, Set.of(sourceRawContactId));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testDoNotMoveEmptyUniqueSystemGroup() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of());
        promoteToSystemGroup(sourceGroup, null, true);

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // since sourceGroup is a system group, it cannot be deleted
        assertGroup(sourceGroup, mSource, Set.of());

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedRawContact(sourceRawContactId, mDest, false);

        // check that we did not create a copy of the empty group in dest
        assertEquals(0, getCount(Groups.CONTENT_URI,
                Groups.ACCOUNT_NAME + " = ? AND "
                        + Groups.ACCOUNT_TYPE + " = ? AND "
                        + Groups.TITLE + " = ? AND "
                        + Groups.TITLE_RES + " = ?",
                new String[] {
                        mDest.getAccountName(),
                        mDest.getAccountType(),
                        "groupTitle",
                        "groupTitleRes"
                }));
    }

    @Test
    @EnableFlags({Flags.FLAG_CP2_ACCOUNT_MOVE_FLAG})
    public void testDoNotMoveAutoAddSystemGroup() {
        // create a duplicate pair of contacts
        long sourceRawContactId = RawContactUtil.createRawContactWithName(mResolver,
                SOURCE_ACCOUNT);
        long sourceGroup = createGroupWithMembers(mSource, "groupTitle",
                "groupTitleRes", List.of(sourceRawContactId));
        promoteToSystemGroup(sourceGroup, null, true);
        ContentValues values = new ContentValues();
        values.put(Groups.AUTO_ADD, 1);
        mResolver.update(Groups.CONTENT_URI, values,
                Groups._ID + " = ?",
                new String[]{
                        Long.toString(sourceGroup)
                });

        // trigger the move
        mCp.moveRawContacts(mSource, mDest, /* insertSyncStubs= */ true);

        // since sourceGroup is a system group, it cannot be deleted
        assertGroup(sourceGroup, mSource, Set.of());

        // verify the duplicate raw contact in dest has been deleted in place
        assertMovedRawContact(sourceRawContactId, mDest, false);

        // check that we did not create a copy of the AUTO_ADD group in dest
        assertEquals(0, getCount(Groups.CONTENT_URI,
                Groups.ACCOUNT_NAME + " = ? AND "
                        + Groups.ACCOUNT_TYPE + " = ? AND "
                        + Groups.TITLE + " = ? AND "
                        + Groups.TITLE_RES + " = ?",
                new String[] {
                        mDest.getAccountName(),
                        mDest.getAccountType(),
                        "groupTitle",
                        "groupTitleRes"
                }));
    }
}
