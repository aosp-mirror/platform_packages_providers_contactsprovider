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

import android.app.SearchManager;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.RemoteException;
import android.provider.ContactsContract;
import android.provider.Contacts.Intents;
import android.provider.ContactsContract.AggregationExceptions;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.Presence;
import android.provider.ContactsContract.RawContacts;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

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
        long rawContactId = createRawContact();
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        insertStructuredName(rawContactId, values);

        assertStructuredName(rawContactId, "Mr", "John", "Kevin", "von Smith", "Jr");
    }

    public void testDisplayNameParsingWhenPartsSpecified() {
        long rawContactId = createRawContact();
        ContentValues values = new ContentValues();
        values.put(StructuredName.DISPLAY_NAME, "Mr.John Kevin von Smith, Jr.");
        values.put(StructuredName.FAMILY_NAME, "Johnson");
        insertStructuredName(rawContactId, values);

        assertStructuredName(rawContactId, null, null, null, "Johnson", null);
    }

    public void testSendToVoicemailDefault() {
        long rawContactId = createRawContactWithName();
        long contactId = queryContactId(rawContactId);

        Cursor c = queryContact(contactId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Contacts.SEND_TO_VOICEMAIL));
        assertEquals(0, sendToVoicemail);
        c.close();
    }

    public void testSetSendToVoicemailAndRingtone() {
        long rawContactId = createRawContactWithName();
        long contactId = queryContactId(rawContactId);

        updateSendToVoicemailAndRingtone(contactId, true, "foo");
        assertSendToVoicemailAndRingtone(contactId, true, "foo");
    }

    public void testSendToVoicemailAndRingtoneAfterAggregation() {
        long rawContactId1 = createRawContactWithName();
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, "foo");

        long rawContactId2 = createRawContactWithName();
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, true, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, contactId1, rawContactId2);

        // Both contacts had "send to VM", the contact now has the same value
        assertSendToVoicemailAndRingtone(contactId1, true, "foo,bar"); // Either foo or bar
    }

    public void testDoNotSendToVoicemailAfterAggregation() {
        long rawContactId1 = createRawContactWithName();
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, null);

        long rawContactId2 = createRawContactWithName();
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, false, null);

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, contactId1, rawContactId2);

        // Since one of the contacts had "don't send to VM" that setting wins for the aggregate
        assertSendToVoicemailAndRingtone(contactId1, false, null);
    }

    public void testSetSendToVoicemailAndRingtonePreservedAfterJoinAndSplit() {
        long rawContactId1 = createRawContactWithName();
        long contactId1 = queryContactId(rawContactId1);
        updateSendToVoicemailAndRingtone(contactId1, true, "foo");

        long rawContactId2 = createRawContactWithName();
        long contactId2 = queryContactId(rawContactId2);
        updateSendToVoicemailAndRingtone(contactId2, false, "bar");

        // Aggregate them
        setAggregationException(AggregationExceptions.TYPE_KEEP_IN, contactId1, rawContactId2);

        // Split them
        setAggregationException(AggregationExceptions.TYPE_KEEP_OUT, contactId1, rawContactId2);

        assertSendToVoicemailAndRingtone(contactId1, true, "foo");
        assertSendToVoicemailAndRingtone(queryContactId(rawContactId2), false, "bar");
    }

    public void testSinglePresenceRowPerContact() {
        int protocol1 = Im.PROTOCOL_GOOGLE_TALK;
        String handle1 = "test@gmail.com";

        long rawContactId1 = createRawContact();
        insertImHandle(rawContactId1, protocol1, handle1);

        insertPresence(protocol1, handle1, Presence.AVAILABLE);
        insertPresence(protocol1, handle1, Presence.AWAY);
        insertPresence(protocol1, handle1, Presence.INVISIBLE);

        Cursor c = queryContactSummary(queryContactId(rawContactId1),
                new String[] {Presence.PRESENCE_STATUS});
        assertEquals(c.getCount(), 1);

        c.moveToFirst();
        assertEquals(c.getInt(0), Presence.AVAILABLE);

    }

    private void updateSendToVoicemailAndRingtone(long contactId, boolean sendToVoicemail,
            String ringtone) {
        ContentValues values = new ContentValues();
        values.put(Contacts.SEND_TO_VOICEMAIL, sendToVoicemail);
        if (ringtone != null) {
            values.put(Contacts.CUSTOM_RINGTONE, ringtone);
        }

        final Uri uri = ContentUris.withAppendedId(Contacts.CONTENT_URI, contactId);
        int count = mResolver.update(uri, values, null, null);
        assertEquals(1, count);
    }

    private void assertSendToVoicemailAndRingtone(long contactId, boolean expectedSendToVoicemail,
            String expectedRingtone) {
        Cursor c = queryContact(contactId);
        assertTrue(c.moveToNext());
        int sendToVoicemail = c.getInt(c.getColumnIndex(Contacts.SEND_TO_VOICEMAIL));
        assertEquals(expectedSendToVoicemail ? 1 : 0, sendToVoicemail);
        String ringtone = c.getString(c.getColumnIndex(Contacts.CUSTOM_RINGTONE));
        if (expectedRingtone == null) {
            assertNull(ringtone);
        } else {
            assertTrue(ArrayUtils.contains(expectedRingtone.split(","), ringtone));
        }
        c.close();
    }

    public void testGroupCreationAfterMembershipInsert() {
        long rawContactId1 = createRawContact(mAccount);
        Uri groupMembershipUri = insertGroupMembership(rawContactId1, "gsid1");

        long groupId = assertSingleGroup(NO_LONG, mAccount, "gsid1", null);
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                rawContactId1, groupId, "gsid1");
    }

    public void testGroupReuseAfterMembershipInsert() {
        long rawContactId1 = createRawContact(mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");
        Uri groupMembershipUri = insertGroupMembership(rawContactId1, "gsid1");

        assertSingleGroup(groupId1, mAccount, "gsid1", "title1");
        assertSingleGroupMembership(ContentUris.parseId(groupMembershipUri),
                rawContactId1, groupId1, "gsid1");
    }

    public void testGroupInsertFailureOnGroupIdConflict() {
        long rawContactId1 = createRawContact(mAccount);
        long groupId1 = createGroup(mAccount, "gsid1", "title1");

        ContentValues values = new ContentValues();
        values.put(GroupMembership.RAW_CONTACT_ID, rawContactId1);
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

        long c0 = id = createRawContact(mAccount, RawContacts.SOURCE_ID, "c0");
        Uri id_0_0 = insertGroupMembership(id, "gsid1");
        Uri id_0_1 = insertEmail(id, "c0@email.com");
        Uri id_0_2 = insertPhoneNumber(id, "5551212c0");

        long c1 = id = createRawContact(mAccount, RawContacts.SOURCE_ID, "c1");
        Uri id_1_0 = insertGroupMembership(id, "gsid1");
        Uri id_1_1 = insertGroupMembership(id, "gsid2");
        Uri id_1_2 = insertEmail(id, "c1@email.com");
        Uri id_1_3 = insertPhoneNumber(id, "5551212c1");

        long c2 = id = createRawContact(mAccount, RawContacts.SOURCE_ID, "c2");
        Uri id_2_0 = insertGroupMembership(id, "gsid1");
        Uri id_2_1 = insertEmail(id, "c2@email.com");
        Uri id_2_2 = insertPhoneNumber(id, "5551212c2");

        long c3 = id = createRawContact(mAccount);
        Uri id_3_0 = insertGroupMembership(id, groupId2);
        Uri id_3_1 = insertEmail(id, "c3@email.com");
        Uri id_3_2 = insertPhoneNumber(id, "5551212c3");

        EntityIterator iterator = mResolver.queryEntities(
                maybeAddAccountQueryParameters(RawContacts.CONTENT_URI, mAccount),
                RawContacts.SOURCE_ID + " in ('c1', 'c2', 'c3')", null, null);
        Entity entity;
        ContentValues[] subValues;
        entity = iterator.next();
        assertEquals(c1, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
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
        assertEquals(c2, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
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
        assertEquals(c3, (long) entity.getEntityValues().getAsLong(RawContacts._ID));
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
        long rawContactId = createRawContact();

        ContentValues values = new ContentValues();
        values.put(Data.RAW_CONTACT_ID, rawContactId);
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
        mResolver.update(Data.CONTENT_URI, values, Data.RAW_CONTACT_ID + "=" + rawContactId +
                " AND " + Data.MIMETYPE + "='testmimetype'", null);

        // Should not be able to change IS_PRIMARY and IS_SUPER_PRIMARY by the above update
        values.put(Data.IS_PRIMARY, 1);
        values.put(Data.IS_SUPER_PRIMARY, 1);
        assertStoredValues(uri, values);

        int count = mResolver.delete(Data.CONTENT_URI, Data.RAW_CONTACT_ID + "=" + rawContactId
                + " AND " + Data.MIMETYPE + "='testmimetype'", null);
        assertEquals(1, count);
        assertEquals(0, getCount(Data.CONTENT_URI, Data.RAW_CONTACT_ID + "=" + rawContactId
                        + " AND " + Data.MIMETYPE + "='testmimetype'", null));
    }

    public void testRawContactDeletion() {
        long rawContactId = createRawContact();
        Uri uri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);

        insertImHandle(rawContactId, Im.PROTOCOL_GOOGLE_TALK, "deleteme@android.com");
        insertPresence(Im.PROTOCOL_GOOGLE_TALK, "deleteme@android.com", Presence.AVAILABLE);
        assertEquals(1, getCount(Uri.withAppendedPath(uri, RawContacts.Data.CONTENT_DIRECTORY),
                null, null));
        assertEquals(1, getCount(Presence.CONTENT_URI, Presence.RAW_CONTACT_ID + "=" + rawContactId,
                null));

        mResolver.delete(uri, null, null);

        assertStoredValues(uri, RawContacts.DELETED, "1");

        Uri permanentDeletionUri = uri.buildUpon().appendQueryParameter(
                RawContacts.DELETE_PERMANENTLY, "true").build();
        mResolver.delete(permanentDeletionUri, null, null);
        assertEquals(0, getCount(uri, null, null));
        assertEquals(0, getCount(Uri.withAppendedPath(uri, RawContacts.Data.CONTENT_DIRECTORY),
                null, null));
        assertEquals(0, getCount(Presence.CONTENT_URI, Presence.RAW_CONTACT_ID + "=" + rawContactId,
                null));
    }

    public void testRawContactDirtySetOnChange() {
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                createRawContact(mAccount));
        assertDirty(uri, true);
        clearDirty(uri);
        assertDirty(uri, false);
    }

    public void testRawContactDirtyAndVersion() {
        final long rawContactId = createRawContact(mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI, rawContactId);
        assertDirty(uri, true);
        long version = getVersion(uri);

        ContentValues values = new ContentValues();
        values.put(ContactsContract.RawContacts.DIRTY, 0);
        values.put(ContactsContract.RawContacts.SEND_TO_VOICEMAIL, 1);
        values.put(ContactsContract.RawContacts.AGGREGATION_MODE,
                RawContacts.AGGREGATION_MODE_IMMEDITATE);
        values.put(ContactsContract.RawContacts.STARRED, 1);
        assertEquals(1, mResolver.update(uri, values, null, null));
        assertEquals(version, getVersion(uri));

        assertDirty(uri, false);

        Uri emailUri = insertEmail(rawContactId, "goo@woo.com");
        assertDirty(uri, true);
        ++version;
        assertEquals(version, getVersion(uri));
        clearDirty(uri);

        values = new ContentValues();
        values.put(Email.DATA, "goo@hoo.com");
        mResolver.update(emailUri, values, null, null);
        assertDirty(uri, true);
        ++version;
        assertEquals(version, getVersion(uri));
        clearDirty(uri);

        mResolver.delete(emailUri, null, null);
        assertDirty(uri, true);
        ++version;
        assertEquals(version, getVersion(uri));
    }

    public void testRawContactClearDirty() {
        final long rawContactId = createRawContact(mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);
        long version = getVersion(uri);
        insertEmail(rawContactId, "goo@woo.com");
        assertDirty(uri, true);
        version++;
        assertEquals(version, getVersion(uri));

        clearDirty(uri);
        assertDirty(uri, false);
        assertEquals(version, getVersion(uri));
    }

    public void testRawContactDeletionSetsDirty() {
        final long rawContactId = createRawContact(mAccount);
        Uri uri = ContentUris.withAppendedId(ContactsContract.RawContacts.CONTENT_URI,
                rawContactId);
        long version = getVersion(uri);
        clearDirty(uri);
        assertDirty(uri, false);

        mResolver.delete(uri, null, null);
        assertStoredValues(uri, RawContacts.DELETED, "1");
        assertDirty(uri, true);
        version++;
        assertEquals(version, getVersion(uri));
    }

    // TODO fix and enable test
    public void __testSearchSuggestionsNotInMyContacts() throws Exception {

        long rawContactId = createRawContact();
        insertStructuredName(rawContactId, "Deer", "Dough");

        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath("D").build();

        // If the contact is not in the "my contacts" group, nothing should be found
        Cursor c = mResolver.query(searchUri, null, null, null, null);
        assertEquals(0, c.getCount());
        c.close();
    }

    public void testSearchSuggestionsByName() throws Exception {
        assertSearchSuggestion(
                true,  // name
                true,  // photo
                false, // organization
                false, // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", null);

        assertSearchSuggestion(
                true,  // name
                true,  // photo
                true,  // organization
                false, // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", "Google");

        assertSearchSuggestion(
                true,  // name
                true,  // photo
                false, // organization
                true,  // phone
                false, // email
                "D",   // query
                true,  // expect icon URI
                null, "Deer Dough", "1-800-4664-411");

        assertSearchSuggestion(
                true,  // name
                true,  // photo
                false, // organization
                false, // phone
                true,  // email
                "D",   // query
                true,  // expect icon URI
                String.valueOf(Presence.getPresenceIconResourceId(Presence.OFFLINE)),
                "Deer Dough", "foo@acme.com");

        assertSearchSuggestion(
                true,  // name
                false, // photo
                true,  // organization
                false, // phone
                false, // email
                "D",   // query
                false, // expect icon URI
                null, "Deer Dough", "Google");
    }

    private void assertSearchSuggestion(boolean name, boolean photo, boolean organization,
            boolean phone, boolean email, String query, boolean expectIcon1Uri, String expectedIcon2,
            String expectedText1, String expectedText2) throws IOException {
        ContentValues values = new ContentValues();

        long rawContactId = createRawContact();

        if (name) {
            insertStructuredName(rawContactId, "Deer", "Dough");
        }


        final Uri rawContactUri = ContentUris.withAppendedId(RawContacts.CONTENT_URI, rawContactId);
        if (photo) {
            values.clear();
            byte[] photoData = loadTestPhoto();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
            values.put(Photo.PHOTO, photoData);
            mResolver.insert(Data.CONTENT_URI, values);
        }

        if (organization) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE);
            values.put(Organization.TYPE, Organization.TYPE_WORK);
            values.put(Organization.COMPANY, "Google");
            mResolver.insert(Data.CONTENT_URI, values);
        }

        if (email) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);
            values.put(Email.TYPE, Email.TYPE_WORK);
            values.put(Email.DATA, "foo@acme.com");
            mResolver.insert(Data.CONTENT_URI, values);

            String protocol = Im.encodePredefinedImProtocol(Im.PROTOCOL_GOOGLE_TALK);

            values.clear();
            values.put(Presence.IM_PROTOCOL, protocol);
            values.put(Presence.IM_HANDLE, "foo@acme.com");
            values.put(Presence.IM_ACCOUNT, "foo");
            values.put(Presence.PRESENCE_STATUS, Presence.OFFLINE);
            values.put(Presence.PRESENCE_CUSTOM_STATUS, "Coding for Android");
            mResolver.insert(Presence.CONTENT_URI, values);
        }

        if (phone) {
            values.clear();
            values.put(Data.RAW_CONTACT_ID, rawContactId);
            values.put(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);
            values.put(Data.IS_PRIMARY, 1);
            values.put(Phone.TYPE, Phone.TYPE_HOME);
            values.put(Phone.NUMBER, "1-800-4664-411");
            mResolver.insert(Data.CONTENT_URI, values);
        }

        long contactId = queryContactId(rawContactId);
        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath(query).build();

        Cursor c = mResolver.query(searchUri, null, null, null, null);
        DatabaseUtils.dumpCursor(c);
        assertEquals(1, c.getCount());
        c.moveToFirst();
        values.clear();

        // SearchManager does not declare a constant for _id
        values.put("_id", contactId);
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, expectedText1);
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, expectedText2);

        String icon1 = c.getString(c.getColumnIndex(SearchManager.SUGGEST_COLUMN_ICON_1));
        if (expectIcon1Uri) {
            assertTrue(icon1.startsWith("content:"));
        } else {
            assertEquals(String.valueOf(com.android.internal.R.drawable.ic_contact_picture), icon1);
        }

        values.put(SearchManager.SUGGEST_COLUMN_ICON_2, expectedIcon2);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA_ID, contactId);
        values.put(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID, contactId);
        assertCursorValues(c, values);
        c.close();

        // Cleanup
        mResolver.delete(rawContactUri, null, null);
    }

    public void testSearchSuggestionsByPhoneNumber() throws Exception {
        ContentValues values = new ContentValues();

        Uri searchUri = new Uri.Builder().scheme("content").authority(ContactsContract.AUTHORITY)
                .appendPath(SearchManager.SUGGEST_URI_PATH_QUERY).appendPath("12345").build();

        Cursor c = mResolver.query(searchUri, null, null, null, null);
        DatabaseUtils.dumpCursor(c);
        assertEquals(2, c.getCount());
        c.moveToFirst();

        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, "Dial number");
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, "using 12345");
        values.put(SearchManager.SUGGEST_COLUMN_ICON_1,
                String.valueOf(com.android.internal.R.drawable.call_contact));
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
                Intents.SEARCH_SUGGESTION_DIAL_NUMBER_CLICKED);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA, "tel:12345");
        values.putNull(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID);
        assertCursorValues(c, values);

        c.moveToNext();
        values.clear();
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_1, "Create contact");
        values.put(SearchManager.SUGGEST_COLUMN_TEXT_2, "using 12345");
        values.put(SearchManager.SUGGEST_COLUMN_ICON_1,
                String.valueOf(com.android.internal.R.drawable.create_contact));
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_ACTION,
                Intents.SEARCH_SUGGESTION_CREATE_CONTACT_CLICKED);
        values.put(SearchManager.SUGGEST_COLUMN_INTENT_DATA, "tel:12345");
        values.put(SearchManager.SUGGEST_COLUMN_SHORTCUT_ID,
                SearchManager.SUGGEST_NEVER_MAKE_SHORTCUT);
        assertCursorValues(c, values);
        c.close();
    }

    private byte[] loadTestPhoto() throws IOException {
        final Resources resources = getContext().getResources();
        InputStream is =
                resources.openRawResource(com.android.internal.R.drawable.ic_contact_picture);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1000];
        int count;
        while((count = is.read(buffer)) != -1) {
            os.write(buffer, 0, count);
        }
        return os.toByteArray();
    }
}

