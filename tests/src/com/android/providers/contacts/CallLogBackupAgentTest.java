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
 * limitations under the License.
 */

package com.android.providers.contacts;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

import android.app.backup.BackupDataOutput;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;

import com.android.providers.contacts.CallLogBackupAgent.Call;
import com.android.providers.contacts.CallLogBackupAgent.CallLogBackupState;

import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Test cases for {@link com.android.providers.contacts.CallLogBackupAgent}
 */
@SmallTest
public class CallLogBackupAgentTest extends AndroidTestCase {

    @Mock DataInput mDataInput;
    @Mock DataOutput mDataOutput;
    @Mock BackupDataOutput mBackupDataOutput;

    CallLogBackupAgent mCallLogBackupAgent;

    MockitoHelper mMockitoHelper = new MockitoHelper();

    @Override
    public void setUp() throws Exception {
        super.setUp();

        mMockitoHelper.setUp(getClass());
        // Since we're testing a system app, AppDataDirGuesser doesn't find our
        // cache dir, so set it explicitly.
        System.setProperty("dexmaker.dexcache", getContext().getCacheDir().toString());

        MockitoAnnotations.initMocks(this);

        mCallLogBackupAgent = new CallLogBackupAgent();
    }

    @Override
    public void tearDown() throws Exception {
        mMockitoHelper.tearDown();
    }

    public void testReadState_NoCall() throws Exception {
        when(mDataInput.readInt()).thenThrow(new EOFException());

        CallLogBackupState state = mCallLogBackupAgent.readState(mDataInput);

        assertEquals(state.version, CallLogBackupAgent.VERSION_NO_PREVIOUS_STATE);
        assertEquals(state.callIds.size(), 0);
    }

    public void testReadState_OneCall() throws Exception {
        when(mDataInput.readInt()).thenReturn(
                1 /* version */,
                1 /* size */,
                101 /* call-ID */ );

        CallLogBackupState state = mCallLogBackupAgent.readState(mDataInput);

        assertEquals(1, state.version);
        assertEquals(1, state.callIds.size());
        assertTrue(state.callIds.contains(101));
    }

    public void testReadState_MultipleCalls() throws Exception {
        when(mDataInput.readInt()).thenReturn(
                1 /* version */,
                2 /* size */,
                101 /* call-ID */,
                102 /* call-ID */);

        CallLogBackupState state = mCallLogBackupAgent.readState(mDataInput);

        assertEquals(1, state.version);
        assertEquals(2, state.callIds.size());
        assertTrue(state.callIds.contains(101));
        assertTrue(state.callIds.contains(102));
    }

    public void testWriteState_NoCalls() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();

        mCallLogBackupAgent.writeState(mDataOutput, state);

        InOrder inOrder = Mockito.inOrder(mDataOutput);
        inOrder.verify(mDataOutput).writeInt(CallLogBackupAgent.VERSION);
        inOrder.verify(mDataOutput).writeInt(0 /* size */);
    }

    public void testWriteState_OneCall() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        state.callIds.add(101);

        mCallLogBackupAgent.writeState(mDataOutput, state);

        InOrder inOrder = Mockito.inOrder(mDataOutput);
        inOrder.verify(mDataOutput).writeInt(CallLogBackupAgent.VERSION);
        inOrder.verify(mDataOutput).writeInt(1);
        inOrder.verify(mDataOutput).writeInt(101 /* call-ID */);
    }

    public void testWriteState_MultipleCalls() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        state.callIds.add(101);
        state.callIds.add(102);
        state.callIds.add(103);

        mCallLogBackupAgent.writeState(mDataOutput, state);

        InOrder inOrder = Mockito.inOrder(mDataOutput);
        inOrder.verify(mDataOutput).writeInt(CallLogBackupAgent.VERSION);
        inOrder.verify(mDataOutput).writeInt(3 /* size */);
        inOrder.verify(mDataOutput).writeInt(101 /* call-ID */);
        inOrder.verify(mDataOutput).writeInt(102 /* call-ID */);
        inOrder.verify(mDataOutput).writeInt(103 /* call-ID */);
    }

    public void testRunBackup_NoCalls() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        List<Call> calls = new LinkedList<>();

        mCallLogBackupAgent.runBackup(state, mBackupDataOutput, calls);

        Mockito.verifyNoMoreInteractions(mBackupDataOutput);
    }

    public void testRunBackup_OneNewCall() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        List<Call> calls = new LinkedList<>();
        calls.add(makeCall(101, 0L, 0L, "555-5555"));
        mCallLogBackupAgent.runBackup(state, mBackupDataOutput, calls);

        verify(mBackupDataOutput).writeEntityHeader(eq("101"), Matchers.anyInt());
        verify(mBackupDataOutput).writeEntityData((byte[]) Matchers.any(), Matchers.anyInt());
    }

    public void testRunBackup_MultipleCall() throws Exception {
        CallLogBackupState state = new CallLogBackupState();
        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        List<Call> calls = new LinkedList<>();
        calls.add(makeCall(101, 0L, 0L, "555-1234"));
        calls.add(makeCall(102, 0L, 0L, "555-5555"));

        mCallLogBackupAgent.runBackup(state, mBackupDataOutput, calls);

        InOrder inOrder = Mockito.inOrder(mBackupDataOutput);
        inOrder.verify(mBackupDataOutput).writeEntityHeader(eq("101"), Matchers.anyInt());
        inOrder.verify(mBackupDataOutput).
                writeEntityData((byte[]) Matchers.any(), Matchers.anyInt());
        inOrder.verify(mBackupDataOutput).writeEntityHeader(eq("102"), Matchers.anyInt());
        inOrder.verify(mBackupDataOutput).
                writeEntityData((byte[]) Matchers.any(), Matchers.anyInt());
    }

    public void testRunBackup_PartialMultipleCall() throws Exception {
        CallLogBackupState state = new CallLogBackupState();

        state.version = CallLogBackupAgent.VERSION;
        state.callIds = new TreeSet<>();
        state.callIds.add(101);

        List<Call> calls = new LinkedList<>();
        calls.add(makeCall(101, 0L, 0L, "555-1234"));
        calls.add(makeCall(102, 0L, 0L, "555-5555"));

        mCallLogBackupAgent.runBackup(state, mBackupDataOutput, calls);

        InOrder inOrder = Mockito.inOrder(mBackupDataOutput);
        inOrder.verify(mBackupDataOutput).writeEntityHeader(eq("102"), Matchers.anyInt());
        inOrder.verify(mBackupDataOutput).
                writeEntityData((byte[]) Matchers.any(), Matchers.anyInt());
    }

    private static Call makeCall(int id, long date, long duration, String number) {
        Call c = new Call();
        c.id = id;
        c.date = date;
        c.duration = duration;
        c.number = number;
        c.accountComponentName = "account-component";
        c.accountId = "account-id";
        return c;
    }

}
