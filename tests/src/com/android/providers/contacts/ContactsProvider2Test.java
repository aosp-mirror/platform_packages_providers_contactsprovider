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

import com.android.internal.util.ArrayUtils;
import com.android.providers.contacts.BaseContactsProvider2Test;

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.EntityIterator;
import android.content.Entity;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.test.suitebuilder.annotation.LargeTest;
import android.os.RemoteException;

/**
 * Unit tests for {@link ContactsProvider2}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactsProvider2Test extends BaseContactsProvider2Test {

    public void testDisplayNameParsingWhenPartsUnspecified() {
        long contactId = createContact();
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        insertStructuredName(contactId, values);

        assertStructuredName(contactId, "Mr", "John", "Kevin", "von Smith", "Jr");
    }

    public void testDisplayNameParsingWhenPartsSpecified() {
        long contactId = createContact();
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        values.put(StructuredName.FAMILY_NAME, "Johnson");
        insertStructuredName(contactId, values);

        assertStructuredName(contactId, null, null, null, "Johnson", null);
    }

    public void testSendToVoicemailDefault() {
        long contactId = createContact();
        long aggregateId = queryAggregateId(contactId);

        Cursor c = queryAggregate(aggregateId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Aggregates.SEND_TO_VOICEMAIL));
        assertEquals(0, sendToVoicemail);
        c.close();
    }

    public void testSetSendToVoicemailAndRingtone() {
        long contactId = createContact();
        long aggregateId = queryAggregateId(contactId);

        updateSendToVoicemailAndRingtone(aggregateId, true, "foo");
        assertSendToVoicemailAndRingtone(aggregateId, true, "foo");
    }

    public void testSendToVoicemailAndRingtoneAfterAggregation() {
        long contactId1 = createContact();
        long aggregateId1 = queryAggregateId(contactId1);
        updateSendToVoicemailAndRingtone(aggregateId1, true, "foo");

        long contactId2 = createContact();
        long aggregateId2 = queryAggregateId(contactId2);
        updateSendToVoicemailAndRingtone(aggregateId2, true, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, aggregateId1, contactId2);

        // Both contacts had "send to VM", the aggregate now has the same value
        assertSendToVoicemailAndRingtone(aggregateId1, true, "foo,bar"); // Either foo or bar
    }

    public void testDoNotSendToVoicemailAfterAggregation() {
        long contactId1 = createContact();
        long aggregateId1 = queryAggregateId(contactId1);
        updateSendToVoicemailAndRingtone(aggregateId1, true, null);

        long contactId2 = createContact();
        long aggregateId2 = queryAggregateId(contactId2);
        updateSendToVoicemailAndRingtone(aggregateId2, false, null);

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, aggregateId1, contactId2);

        // Since one of the contacts had "don't send to VM" that setting wins for the aggregate
        assertSendToVoicemailAndRingtone(aggregateId1, false, null);
    }

    public void testSetSendToVoicemailAndRingtonePreservedAfterJoinAndSplit() {
        long contactId1 = createContact();
        long aggregateId1 = queryAggregateId(contactId1);
        updateSendToVoicemailAndRingtone(aggregateId1, true, "foo");

        long contactId2 = createContact();
        long aggregateId2 = queryAggregateId(contactId2);
        updateSendToVoicemailAndRingtone(aggregateId2, false, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, aggregateId1, contactId2);

        // Split them
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, aggregateId1, contactId2);

        assertSendToVoicemailAndRingtone(aggregateId1, true, "foo");
        assertSendToVoicemailAndRingtone(queryAggregateId(contactId2), false, "bar");
    }

    public void testSinglePresenceRowPerAggregate() {
        int protocol1 = Im.PROTOCOL_GOOGLE_TALK;
        String handle1 = "test@gmail.com";

        long contactId1 = createContact();
        insertImHandle(contactId1, protocol1, handle1);

        insertPresence(protocol1, handle1, Presence.AVAILABLE);
        insertPresence(protocol1, handle1, Presence.AWAY);
        insertPresence(protocol1, handle1, Presence.INVISIBLE);

        Cursor c = queryAggregateSummary(queryAggregateId(contactId1),
                new String[] {Presence.PRESENCE_STATUS});
        assertEquals(c.getCount(), 1);

        c.moveToFirst();
        assertEquals(c.getInt(0), Presence.AVAILABLE);

    }

    private void updateSendToVoicemailAndRingtone(long aggregateId, boolean sendToVoicemail,
            String ringtone) {
        ContentValues values = new ContentValues();
        values.put(Aggregates.SEND_TO_VOICEMAIL, sendToVoicemail);
        if (ringtone != null) {
            values.put(Aggregates.CUSTOM_RINGTONE, ringtone);
        }

        final Uri uri = ContentUris.withAppendedId(Aggregates.CONTENT_URI, aggregateId);
        int count = mResolver.update(uri, values, null, null);
        assertEquals(1, count);
    }

    private void assertSendToVoicemailAndRingtone(long aggregateId, boolean expectedSendToVoicemail,
            String expectedRingtone) {
        Cursor c = queryAggregate(aggregateId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Aggregates.SEND_TO_VOICEMAIL));
        assertEquals(expectedSendToVoicemail ? 1 : 0, sendToVoicemail);
        String ringtone = c.getString(c.getColumnIndex(Aggregates.CUSTOM_RINGTONE));
        if (expectedRingtone == null) {
            assertNull(ringtone);
        } else {
            assertTrue(ArrayUtils.contains(expectedRingtone.split(","), ringtone));
        }
        c.close();
    }

    public void testGroupCreationAfterMembershipInsert() {
        long contactId1 = createContact(mAccount);
        Uri groupMembershipUri = insertGroupMembership(contactId1, "gsid1");

        long groupId = assertSingleGroup(NO_LONG, mAccount, "gsid1", null);
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                contactId1, groupId, "gsid1");
    }

    public void testGroupReuseAfterMembershipInsert() {
        long contactId1 = createContact(mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");
        Uri groupMembershipUri = insertGroupMembership(contactId1, "gsid1");

        assertSingleGroup(groupId1, mAccount, "gsid1", "title1");
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                contactId1, groupId1, "gsid1");
    }

    public void testGroupInsertFailureOnGroupIdConflict() {
        long contactId1 = createContact(mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");

        ContentValues values = new ContentValues();
        values.put(GroupMembership.CONTACT_ID, contactId1);
        values.put(GroupMembership.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
        values.put(GroupMembership.GROUP_SOURCE_ID, "gsid1");
        values.put(GroupMembership.GROUP_ROW_ID, groupId1);
        try {
            mResolver.insert(Data.CONTENT_URI, values);
            fail("the insert was expected to fail, but it succeeded");
        } catch (IllegalArgumentException e) {
            // this was expected
        }
    }

    public void testContentEntityIterator() throws RemoteException {
        // create multiple contacts and check that the selected ones are returned
        long id;

        long groupId1 = createGroup(mAccount, "gsid1", "title1");
        long groupId2 = createGroup(mAccount, "gsid2", "title2");

        long c0 = id = createContact(mAccount);
        Uri id_0_0 = insertGroupMembership(id, "gsid1");
        Uri id_0_1 = insertEmail(id, "c0@email.com");
        Uri id_0_2 = insertPhoneNumber(id, "5551212c0");

        long c1 = id = createContact(mAccount);
        Uri id_1_0 = insertGroupMembership(id, "gsid1");
        Uri id_1_1 = insertGroupMembership(id, "gsid2");
        Uri id_1_2 = insertEmail(id, "c1@email.com");
        Uri id_1_3 = insertPhoneNumber(id, "5551212c1");

        long c2 = id = createContact(mAccount);
        Uri id_2_0 = insertGroupMembership(id, "gsid1");
        Uri id_2_1 = insertEmail(id, "c2@email.com");
        Uri id_2_2 = insertPhoneNumber(id, "5551212c2");

        long c3 = id = createContact(mAccount);
        Uri id_3_0 = insertGroupMembership(id, groupId2);
        Uri id_3_1 = insertEmail(id, "c3@email.com");
        Uri id_3_2 = insertPhoneNumber(id, "5551212c3");

        EntityIterator iterator = mResolver.queryEntities(Contacts.CONTENT_URI,
                "contact_id in (" + c1 + "," + c2 + "," + c3 + ")", null, null);
        Entity entity;
        ContentValues[] subValues;
        entity = iterator.next();
        assertEquals(c1, (long) entity.getEntityValues().getAsLong(Contacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(4, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_1_0,
                GroupMembership.GROUP_ROW_ID, groupId1,
                GroupMembership.GROUP_SOURCE_ID, "gsid1");
        assertDataRow(subValues[1], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_1_1,
                GroupMembership.GROUP_ROW_ID, groupId2,
                GroupMembership.GROUP_SOURCE_ID, "gsid2");
        assertDataRow(subValues[2], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_1_2,
                Email.DATA, "c1@email.com");
        assertDataRow(subValues[3], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_1_3,
                Email.DATA, "5551212c1");

        entity = iterator.next();
        assertEquals(c2, (long) entity.getEntityValues().getAsLong(Contacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(3, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_2_0,
                GroupMembership.GROUP_ROW_ID, groupId1,
                GroupMembership.GROUP_SOURCE_ID, "gsid1");
        assertDataRow(subValues[1], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_2_1,
                Email.DATA, "c2@email.com");
        assertDataRow(subValues[2], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_2_2,
                Email.DATA, "5551212c2");

        entity = iterator.next();
        assertEquals(c3, (long) entity.getEntityValues().getAsLong(Contacts._ID));
        subValues = asSortedContentValuesArray(entity.getSubValues());
        assertEquals(3, subValues.length);
        assertDataRow(subValues[0], GroupMembership.CONTENT_ITEM_TYPE,
                Data._ID, id_3_0,
                GroupMembership.GROUP_ROW_ID, groupId2,
                GroupMembership.GROUP_SOURCE_ID, "gsid2");
        assertDataRow(subValues[1], Email.CONTENT_ITEM_TYPE,
                Data._ID, id_3_1,
                Email.DATA, "c3@email.com");
        assertDataRow(subValues[2], Phone.CONTENT_ITEM_TYPE,
                Data._ID, id_3_2,
                Email.DATA, "5551212c3");

        assertFalse(iterator.hasNext());
    }

    public void testDataCreateUpdateDeleteByMimeType() throws Exception {
        long contactId = createContact();

        ContentValues values = new ContentValues();
        values.put(Data.CONTACT_ID, contactId);
        values.put(Data.MIMETYPE, "testmimetype");
        values.put(Data.RES_PACKAGE, "oldpackage");
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        values.put(Data.DATA1, "old1");
        values.put(Data.DATA2, "old2");
        values.put(Data.DATA3, "old3");
        values.put(Data.DATA4, "old4");
        values.put(Data.DATA5, "old5");
        values.put(Data.DATA6, "old6");
        values.put(Data.DATA7, "old7");
        values.put(Data.DATA8, "old8");
        values.put(Data.DATA9, "old9");
        values.put(Data.DATA10, "old10");
        values.put(Data.DATA11, "old11");
        values.put(Data.DATA12, "old12");
        values.put(Data.DATA13, "old13");
        values.put(Data.DATA14, "old14");
        values.put(Data.DATA15, "old15");
        Uri uri = mResolver.insert(Data.CONTENT_URI, values);
        assertStoredValues(uri, values);

        values.clear();
        values.put(Data.RES_PACKAGE, "newpackage");
        values.put(Data.IS_PRIMARY, 0);
        values.put(Data.IS_SUPER_PRIMARY, 0);
        values.put(Data.DATA1, "new1");
        values.put(Data.DATA2, "new2");
        values.put(Data.DATA3, "new3");
        values.put(Data.DATA4, "new4");
        values.put(Data.DATA5, "new5");
        values.put(Data.DATA6, "new6");
        values.put(Data.DATA7, "new7");
        values.put(Data.DATA8, "new8");
        values.put(Data.DATA9, "new9");
        values.put(Data.DATA10, "new10");
        values.put(Data.DATA11, "new11");
        values.put(Data.DATA12, "new12");
        values.put(Data.DATA13, "new13");
        values.put(Data.DATA14, "new14");
        values.put(Data.DATA15, "new15");
        mResolver.update(Data.CONTENT_URI, values, Data.CONTACT_ID + "=" + contactId +
                " AND " + Data.MIMETYPE + "='testmimetype'", null);
        assertStoredValues(uri, values);

        int count = mResolver.delete(Data.CONTENT_URI, Data.CONTACT_ID + "=" + contactId
                + " AND " + Data.MIMETYPE + "='testmimetype'", null);
        assertEquals(1, count);

        Cursor c = mResolver.query(Data.CONTENT_URI, null, Data.CONTACT_ID + "=" + contactId
                + " AND " + Data.MIMETYPE + "='testmimetype'", null, null);
        try {
            assertEquals(0, c.getCount());
        } finally {
            c.close();
        }
    }
}

