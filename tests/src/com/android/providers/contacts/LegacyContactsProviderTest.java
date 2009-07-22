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

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.res.Resources;
import android.database.Cursor;
import android.net.Uri;
import android.provider.Contacts;
import android.provider.Contacts.ContactMethods;
import android.provider.Contacts.Extensions;
import android.provider.Contacts.GroupMembership;
import android.provider.Contacts.Groups;
import android.provider.Contacts.Organizations;
import android.provider.Contacts.People;
import android.provider.Contacts.Phones;
import android.provider.Contacts.Photos;
import android.provider.Contacts.Presence;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * Tests for legacy contacts APIs.
 */
@LargeTest
public class LegacyContactsProviderTest extends BaseContactsProvider2Test {

    private static final boolean USE_LEGACY_PROVIDER = false;

    @Override
    protected Class<? extends ContentProvider> getProviderClass() {
       return USE_LEGACY_PROVIDER ? ContactsProvider.class : SynchronousContactsProvider2.class;
    }

    @Override
    protected String getAuthority() {
        return Contacts.AUTHORITY;
    }

    public void testPeopleInsert() {
        ContentValues values = new ContentValues();
        putContactValues(values);

        Uri uri = mResolver.insert(People.CONTENT_URI, values);
        assertStoredValues(uri, values);
    }

    // TODO fix and reenable the test
    public void __testPeopleDelete() {
        ContentValues values = new ContentValues();
        values.put(People.NAME, "John Doe");
        Uri personId = mResolver.insert(People.CONTENT_URI, values);
        mResolver.delete(personId, null, null);

        Cursor c = mResolver.query(personId, null, People.NAME + "='John Doe'" , null, null);
        assertEquals("Record count after deletion", 0, c.getCount());
        c.close();

        try {
            mResolver.query(People.DELETED_CONTENT_URI, null, null, null, null);
        } catch (UnsupportedOperationException e) {
            // Expected exception
        }
    }

    // TODO fix and reenable the test
    public void __testPeopleFilter() {
        ContentValues values = new ContentValues();
        values.put(People.NAME, "Deer Doe");
        mResolver.insert(People.CONTENT_URI, values);

        values.clear();
        values.put(People.NAME, "Dear Dough");
        mResolver.insert(People.CONTENT_URI, values);

        values.clear();
        values.put(People.NAME, "D.R. Dauwe");
        mResolver.insert(People.CONTENT_URI, values);

        assertFilteredContacts("d", "Deer Doe", "Dear Dough", "D.R. Dauwe");
        assertFilteredContacts("de", "Deer Doe", "Dear Dough");
        assertFilteredContacts("dee", "Deer Doe");
        assertFilteredContacts("der");
    }

    public void testDefaultDisplayName() {
        ContentValues values = new ContentValues();
        values.put(People.NAME, "John Doe");
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        assertStoredValues(personUri, People.DISPLAY_NAME, "John Doe");
    }

    public void testPrimaryOrganization() {
        ContentValues values = new ContentValues();
        final Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        // Primary
        values.clear();
        values.put(Organizations.ISPRIMARY, 1);
        values.put(Organizations.COMPANY, "Google");
        values.put(Organizations.TYPE, Organizations.TYPE_WORK);
        values.put(Organizations.PERSON_ID, personId);
        Uri orgUri1 = mResolver.insert(Organizations.CONTENT_URI, values);

        // Non-primary
        values.clear();
        values.put(Organizations.COMPANY, "Acme");
        values.put(Organizations.TYPE, Organizations.TYPE_WORK);
        values.put(Organizations.PERSON_ID, personId);
        Uri orgUri2 = mResolver.insert(Organizations.CONTENT_URI, values);

        values.clear();
        values.put(People.PRIMARY_ORGANIZATION_ID, ContentUris.parseId(orgUri1));
        values.put(People.DISPLAY_NAME, "Google");
        assertStoredValues(personUri, values);

        // Remove the original primary organization
        mResolver.delete(orgUri1, null, null);

        values.clear();
        values.put(People.PRIMARY_ORGANIZATION_ID, ContentUris.parseId(orgUri2));
        values.put(People.DISPLAY_NAME, "Acme");
        assertStoredValues(personUri, values);

        // Remove the remaining organization
        mResolver.delete(orgUri2, null, null);

        values.clear();
        values.putNull(People.PRIMARY_ORGANIZATION_ID);
        values.putNull(People.DISPLAY_NAME);
        assertStoredValues(personUri, values);
    }

    public void testPrimaryPhone() {
        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        // Primary
        values.clear();
        values.put(Phones.ISPRIMARY, 1);
        values.put(Phones.TYPE, Phones.TYPE_WORK);
        values.put(Phones.PERSON_ID, personId);
        values.put(Phones.NUMBER, "12345");
        Uri phoneUri1 = mResolver.insert(Phones.CONTENT_URI, values);

        // Non-primary
        values.clear();
        values.put(Phones.TYPE, Phones.TYPE_WORK);
        values.put(Phones.PERSON_ID, personId);
        values.put(Phones.NUMBER, "67890");
        Uri phoneUri2 = mResolver.insert(Phones.CONTENT_URI, values);

        values.clear();
        values.put(People.PRIMARY_PHONE_ID, ContentUris.parseId(phoneUri1));
        values.put(People.DISPLAY_NAME, "12345");
        assertStoredValues(personUri, values);

        // Remove the primary phone number
        mResolver.delete(phoneUri1, null, null);

        values.clear();
        values.put(People.PRIMARY_PHONE_ID, ContentUris.parseId(phoneUri2));
        values.put(People.DISPLAY_NAME, "67890");
        assertStoredValues(personUri, values);

        // Remove the remaining phone number
        mResolver.delete(phoneUri2, null, null);

        values.clear();
        values.putNull(People.PRIMARY_PHONE_ID);
        values.putNull(People.DISPLAY_NAME);
        assertStoredValues(personUri, values);
    }

    public void testPrimaryEmail() {
        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        // Primary
        values.clear();
        values.put(ContactMethods.PERSON_ID, personId);
        values.put(ContactMethods.KIND, Contacts.KIND_EMAIL);
        values.put(ContactMethods.TYPE, ContactMethods.TYPE_HOME);
        values.put(ContactMethods.DATA, "foo@acme.com");
        values.put(ContactMethods.ISPRIMARY, 1);
        Uri emailUri1 = mResolver.insert(ContactMethods.CONTENT_URI, values);

        // Non-primary
        values.clear();
        values.put(ContactMethods.PERSON_ID, personId);
        values.put(ContactMethods.KIND, Contacts.KIND_EMAIL);
        values.put(ContactMethods.TYPE, ContactMethods.TYPE_WORK);
        values.put(ContactMethods.DATA, "bar@acme.com");
        Uri emailUri2 = mResolver.insert(ContactMethods.CONTENT_URI, values);

        values.clear();
        values.put(People.PRIMARY_EMAIL_ID, ContentUris.parseId(emailUri1));
        values.put(People.DISPLAY_NAME, "foo@acme.com");
        assertStoredValues(personUri, values);

        // Remove the primary email
        mResolver.delete(emailUri1, null, null);

        values.clear();
        values.put(People.PRIMARY_EMAIL_ID, ContentUris.parseId(emailUri2));
        values.put(People.DISPLAY_NAME, "bar@acme.com");
        assertStoredValues(personUri, values);

        // Remove the remaining email
        mResolver.delete(emailUri2, null, null);

        values.clear();
        values.putNull(People.PRIMARY_EMAIL_ID);
        values.putNull(People.DISPLAY_NAME);
        assertStoredValues(personUri, values);
    }

    public void testMarkAsContacted() {
        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        int timesContactedBefore =
                Integer.parseInt(getStoredValue(personUri, People.TIMES_CONTACTED));
        long timeBefore = System.currentTimeMillis();
        People.markAsContacted(mResolver, personId);
        long timeAfter = System.currentTimeMillis();

        long lastContacted = Long.parseLong(getStoredValue(personUri, People.LAST_TIME_CONTACTED));
        int timesContactedAfter =
            Integer.parseInt(getStoredValue(personUri, People.TIMES_CONTACTED));

        assertTrue(lastContacted >= timeBefore);
        assertTrue(lastContacted <= timeAfter);
        assertEquals(timesContactedAfter, timesContactedBefore + 1);
    }

    public void testOrganizationsInsert() {
        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        values.clear();
        values.put(Organizations.COMPANY, "Sierra");
        values.put(Organizations.PERSON_ID, personId);
        values.put(Organizations.TYPE, Organizations.TYPE_CUSTOM);
        values.put(Organizations.LABEL, "Club");
        values.put(Organizations.TITLE, "Member");
        values.put(Organizations.ISPRIMARY, 1);

        Uri uri = mResolver.insert(Organizations.CONTENT_URI, values);
        assertStoredValues(uri, values);

        assertPersonIdConstraint(Organizations.CONTENT_URI, Organizations.TYPE,
                Organizations.TYPE_WORK);

        assertTypeAndLabelConstraints(Organizations.CONTENT_URI, Organizations.PERSON_ID, personId,
                Organizations.TYPE, Organizations.TYPE_CUSTOM, Organizations.TYPE_OTHER,
                Organizations.LABEL);
    }

    public void testPhonesInsert() {
        ContentValues values = new ContentValues();
        putContactValues(values);
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        values.clear();
        values.put(Phones.PERSON_ID, personId);
        values.put(Phones.TYPE, Phones.TYPE_CUSTOM);
        values.put(Phones.LABEL, "Directory");
        values.put(Phones.NUMBER, "1-800-4664-411");
        values.put(Phones.ISPRIMARY, 1);

        Uri uri = mResolver.insert(Phones.CONTENT_URI, values);

        // Adding another value to assert
        values.put(Phones.NUMBER_KEY, "11446640081");

        // The result is joined with People
        putContactValues(values);
        assertStoredValues(uri, values);

        // Access the phone through People
        Uri twigUri = Uri.withAppendedPath(personUri, People.Phones.CONTENT_DIRECTORY);
        assertStoredValues(twigUri, values);

        // Now the person should be joined with Phone
        values.clear();
        putContactValues(values);
        values.put(People.TYPE, Phones.TYPE_CUSTOM);
        values.put(People.LABEL, "Directory");
        values.put(People.NUMBER, "1-800-4664-411");
        assertStoredValues(personUri, values);

        assertPersonIdConstraint(Phones.CONTENT_URI, Phones.TYPE, Phones.TYPE_WORK);

        assertTypeAndLabelConstraints(Phones.CONTENT_URI, Phones.PERSON_ID, personId, Phones.TYPE,
                Phones.TYPE_CUSTOM, Phones.TYPE_OTHER, Phones.LABEL);
    }

    public void testEmailInsert() {
        assertContactMethodInsert(Contacts.KIND_EMAIL, ContactMethods.TYPE_CUSTOM,
                "Some other way", "foo@acme.com", null, true);
    }

    public void testImInsert() {
        assertContactMethodInsert(Contacts.KIND_IM, ContactMethods.TYPE_CUSTOM, "Some other way",
                "Foo", "Bar", true);
    }

    public void testPostalInsert() {
        assertContactMethodInsert(Contacts.KIND_POSTAL, ContactMethods.TYPE_CUSTOM,
                "Some other way", "Foo", "Bar", true);
    }

    private void assertContactMethodInsert(int kind, int type, String label, String data,
            String auxData, boolean primary) {
        ContentValues values = new ContentValues();
        putContactValues(values);
        final Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        values.clear();
        values.put(ContactMethods.PERSON_ID, personId);
        values.put(ContactMethods.KIND, kind);
        values.put(ContactMethods.TYPE, type);
        values.put(ContactMethods.LABEL, label);
        values.put(ContactMethods.DATA, data);
        values.put(ContactMethods.AUX_DATA, auxData);
        values.put(ContactMethods.ISPRIMARY, primary ? 1 : 0);

        Uri uri = mResolver.insert(ContactMethods.CONTENT_URI, values);

        // The result is joined with People
        putContactValues(values);
        assertStoredValues(uri, values);

        // Access the contact method through People
        Uri twigUri = Uri.withAppendedPath(personUri, People.ContactMethods.CONTENT_DIRECTORY);
        assertStoredValues(twigUri, values);

        assertPersonIdConstraint(ContactMethods.CONTENT_URI, ContactMethods.TYPE,
                ContactMethods.TYPE_WORK);

        assertTypeAndLabelConstraints(ContactMethods.CONTENT_URI, ContactMethods.PERSON_ID,
                personId, ContactMethods.TYPE, ContactMethods.TYPE_CUSTOM,
                ContactMethods.TYPE_OTHER, ContactMethods.LABEL);
    }

    public void testExtensionsInsert() {
        ContentValues values = new ContentValues();
        final Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        values.clear();
        values.put(Extensions.PERSON_ID, personId);
        values.put(Extensions.NAME, "Foo");
        values.put(Extensions.VALUE, "Bar");

        Uri uri = mResolver.insert(Extensions.CONTENT_URI, values);
        assertStoredValues(uri, values);

        // Access the extensions through People
        Uri twigUri = Uri.withAppendedPath(personUri, People.Extensions.CONTENT_DIRECTORY);
        assertStoredValues(twigUri, values);
    }

    public void testGroupsInsert() {
        ContentValues values = new ContentValues();
        values.put(Groups.NAME, "Galois");
        values.put(Groups.NOTES, "Abel");
        values.put(Groups.SYSTEM_ID, "12345");

        Uri groupUri = mResolver.insert(Groups.CONTENT_URI, values);
        assertStoredValues(groupUri, values);
    }

    public void testGroupMembershipsInsert() {
        ContentValues values = new ContentValues();
        values.put(Groups.NAME, "Galois");
        values.put(Groups.NOTES, "Abel");
        Uri groupUri = mResolver.insert(Groups.CONTENT_URI, values);

        values.clear();
        values.put(People.NAME, "Klein");
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);

        long groupId = ContentUris.parseId(groupUri);
        long personId = ContentUris.parseId(personUri);

        ContentValues values1 = new ContentValues();
        values1.put(GroupMembership.GROUP_ID, groupId);
        values1.put(GroupMembership.PERSON_ID, personId);
        Uri membershipUri = mResolver.insert(GroupMembership.CONTENT_URI, values1);
        assertStoredValues(membershipUri, values1);

        Uri personsGroupsUri = Uri.withAppendedPath(personUri, GroupMembership.CONTENT_DIRECTORY);
        assertStoredValues(personsGroupsUri, values1);
    }

    // TODO fix and reenable the test
    public void __testAddToGroup() {
        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        values.clear();
        values.put(Groups.NAME, "Galois");
        Uri groupUri = mResolver.insert(Groups.CONTENT_URI, values);

        People.addToGroup(mResolver, personId, "Galois");

        values.clear();
        values.put(GroupMembership.GROUP_ID, ContentUris.parseId(groupUri));
        values.put(GroupMembership.PERSON_ID, personId);

        Uri personsGroupsUri = Uri.withAppendedPath(personUri, GroupMembership.CONTENT_DIRECTORY);
        assertStoredValues(personsGroupsUri, values);
    }

    /**
     * We will not support presence compatibility mode.  Leaving the test for now in case we change
     * our mind.
     */
    public void __testPresenceInsertMatchOnHandle() {
        ContentValues values = new ContentValues();
        putContactValues(values);
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        String encodedProtocol =
                ContactMethods.encodePredefinedImProtocol(ContactMethods.PROTOCOL_GOOGLE_TALK);

        values.clear();
        values.put(ContactMethods.PERSON_ID, personId);
        values.put(ContactMethods.KIND, Contacts.KIND_IM);
        values.put(ContactMethods.TYPE, ContactMethods.TYPE_HOME);
        values.put(ContactMethods.DATA, "Android");
        values.put(ContactMethods.AUX_DATA, encodedProtocol);
        mResolver.insert(ContactMethods.CONTENT_URI, values);

        values.clear();
        values.put(Presence.IM_PROTOCOL, encodedProtocol);
        values.put(Presence.IM_HANDLE, "Android");
        values.put(Presence.IM_ACCOUNT, "foo");

        Uri presenceUri = mResolver.insert(Presence.CONTENT_URI, values);

        values.put(Presence.PERSON_ID, personId);

        // Presence is joined with People
        putContactValues(values);
        assertStoredValues(presenceUri, values);
    }

    /**
     * We will not support presence compatibility mode.  Leaving the test for now in case we change
     * our mind.
     */
    public void __testPresenceInsertMatchOnEmail() {
        ContentValues values = new ContentValues();
        putContactValues(values);
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);
        long personId = ContentUris.parseId(personUri);

        String protocol =
            ContactMethods.encodePredefinedImProtocol(ContactMethods.PROTOCOL_GOOGLE_TALK);

        values.clear();
        values.put(ContactMethods.PERSON_ID, personId);
        values.put(ContactMethods.KIND, Contacts.KIND_EMAIL);
        values.put(ContactMethods.TYPE, ContactMethods.TYPE_HOME);
        values.put(ContactMethods.DATA, "Android@android.com");
        mResolver.insert(ContactMethods.CONTENT_URI, values);

        values.clear();
        values.put(Presence.IM_PROTOCOL, protocol);
        values.put(Presence.IM_HANDLE, "Android@android.com");
        values.put(Presence.IM_ACCOUNT, "foo");
        values.put(Presence.PRESENCE_STATUS, Presence.OFFLINE);
        values.put(Presence.PRESENCE_CUSTOM_STATUS, "Coding for Android");

        Uri presenceUri = mResolver.insert(Presence.CONTENT_URI, values);

        values.put(Presence.PERSON_ID, personId);
        assertStoredValues(presenceUri, values);

        // Now the person should be joined with Presence
        values.clear();
        putContactValues(values);
        values.put(People.IM_PROTOCOL, protocol);
        values.put(People.IM_HANDLE, "Android@android.com");
        values.put(People.IM_ACCOUNT, "foo");
        values.put(People.PRESENCE_STATUS, Presence.OFFLINE);
        values.put(People.PRESENCE_CUSTOM_STATUS, "Coding for Android");
        assertStoredValues(personUri, values);
    }

    // TODO fix and reenable the test
    public void __testPhotoUpdate() throws Exception {
        byte[] photo = loadTestPhoto();

        ContentValues values = new ContentValues();
        Uri personUri = mResolver.insert(People.CONTENT_URI, values);

        values.clear();
        values.put(Photos.DATA, photo);

        Uri photoUri = Uri.withAppendedPath(personUri, Photos.CONTENT_DIRECTORY);
        mResolver.update(photoUri, values, null, null);
        assertStoredValues(photoUri, values);
    }

    private void putContactValues(ContentValues values) {
        // Populating only unhidden columns
        values.put(People.NAME, "Deer Dough");
        values.put(People.PHONETIC_NAME, "Dear Doe");
        values.put(People.NOTES, "Cash Cow");
        values.put(People.TIMES_CONTACTED, 3);
        values.put(People.LAST_TIME_CONTACTED, 10);
        values.put(People.CUSTOM_RINGTONE, "ringtone");
        values.put(People.SEND_TO_VOICEMAIL, 1);
        values.put(People.STARRED, 1);
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

    private void assertFilteredContacts(String filter, String... expectedNames) {
        Uri filterUri = Uri.withAppendedPath(People.CONTENT_FILTER_URI, filter);
        Cursor c = mResolver.query(filterUri, null, null, null, null);
        try {
            assertEquals("Record count", expectedNames.length, c.getCount());
            int column = c.getColumnIndex(People.NAME);
            for (int i = 0; i < expectedNames.length; i++) {
                c.moveToNext();
                assertEquals(expectedNames[i], c.getString(column));
            }
        } finally {
            c.close();
        }
    }

    private void assertPersonIdConstraint(Uri uri, String typeColumn, int defaultType) {
        ContentValues values = new ContentValues();
        values.put(typeColumn, defaultType);
        try {
            mResolver.insert(uri, values);
            fail("Inserted row without person ID");
        } catch (Exception e) {
            // Exception expected
        }
    }

    private void assertTypeAndLabelConstraints(Uri uri, String personIdColumn, long personId,
            String typeColumn, int defaultType, int otherType, String labelColumn) {
        ContentValues values = new ContentValues();
        values.put(personIdColumn, personId);
        values.put(typeColumn, defaultType);
        try {
            mResolver.insert(uri, values);
            fail("Inserted row with custom type but without label");
        } catch (Exception e) {
            // Exception expected
        }

        values.clear();
        values.put(personIdColumn, personId);
        try {
            mResolver.insert(uri, values);
            fail("Inserted row without either type or label");
        } catch (Exception e) {
            // Exception expected
        }

        values.clear();
        values.put(personIdColumn, personId);
        values.put(typeColumn, otherType);
        values.put(labelColumn, "Foo");
        try {
            mResolver.insert(uri, values);
            fail("Inserted row with both type and label");
        } catch (Exception e) {
            // Exception expected
        }
    }

    private void assertStoredValues(Uri rowUri, String column, String expectedValue) {
        String value = getStoredValue(rowUri, column);
        assertEquals("Column value " + column, expectedValue, value);
    }

    private String getStoredValue(Uri rowUri, String column) {
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

    private void assertStoredValues(Uri rowUri, ContentValues expectedValues) {
        Cursor c = mResolver.query(rowUri, null, null, null, null);
        try {
            assertEquals("Record count", 1, c.getCount());
            c.moveToFirst();
            assertCursorValues(c, expectedValues);
        } finally {
            c.close();
        }
    }

    private void assertCursorValues(Cursor cursor, ContentValues expectedValues) {
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
}
