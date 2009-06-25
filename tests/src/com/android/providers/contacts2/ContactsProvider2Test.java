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
package com.android.providers.contacts2;

import com.android.internal.util.ArrayUtils;

import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Aggregates;
import android.provider.ContactsContract.AggregationExceptions;
import android.test.suitebuilder.annotation.LargeTest;

/**
 * Unit tests for {@link ContactsProvider2}.
 *
 * Run the test like this:
 * <code>
 * adb shell am instrument -w \
 *         com.android.providers.contacts2.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactsProvider2Test extends BaseContactsProvider2Test {

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
}

