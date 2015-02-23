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
 * limitations under the License
 */

package com.android.providers.contacts;

import android.app.backup.BackupAgent;
import android.app.backup.BackupDataInput;
import android.app.backup.BackupDataOutput;
import android.content.ComponentName;
import android.content.ContentResolver;
import android.database.Cursor;
import android.os.ParcelFileDescriptor;
import android.provider.CallLog;
import android.provider.CallLog.Calls;
import android.telecom.PhoneAccountHandle;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Call log backup agent.
 */
public class CallLogBackupAgent extends BackupAgent {

    @VisibleForTesting
    static class CallLogBackupState {
        int version;
        SortedSet<Integer> callIds;
    }

    private static class Call {
        int id;
        long date;
        long duration;
        String number;
        int type;
        int numberPresentation;
        String accountComponentName;
        String accountId;
        String accountAddress;
        Long dataUsage;
        int features;

        @Override
        public String toString() {
            if (isDebug()) {
                return  "[" + id + ", account: [" + accountComponentName + " : " + accountId +
                    "]," + number + ", " + date + "]";
            } else {
                return "[" + id + "]";
            }
        }
    }

    private static final String TAG = "CallLogBackupAgent";

    /** Current version of CallLogBackup. Used to track the backup format. */
    private static final int VERSION = 1;
    /** Version indicating that there exists no previous backup entry. */
    @VisibleForTesting
    static final int VERSION_NO_PREVIOUS_STATE = 0;

    private static final String[] CALL_LOG_PROJECTION = new String[] {
        CallLog.Calls._ID,
        CallLog.Calls.DATE,
        CallLog.Calls.DURATION,
        CallLog.Calls.NUMBER,
        CallLog.Calls.TYPE,
        CallLog.Calls.COUNTRY_ISO,
        CallLog.Calls.GEOCODED_LOCATION,
        CallLog.Calls.NUMBER_PRESENTATION,
        CallLog.Calls.PHONE_ACCOUNT_COMPONENT_NAME,
        CallLog.Calls.PHONE_ACCOUNT_ID,
        CallLog.Calls.PHONE_ACCOUNT_ADDRESS,
        CallLog.Calls.DATA_USAGE,
        CallLog.Calls.FEATURES
    };

    /** ${inheritDoc} */
    @Override
    public void onBackup(ParcelFileDescriptor oldStateDescriptor, BackupDataOutput data,
            ParcelFileDescriptor newStateDescriptor) throws IOException {

        // Get the list of the previous calls IDs which were backed up.
        DataInputStream dataInput = new DataInputStream(
                new FileInputStream(oldStateDescriptor.getFileDescriptor()));
        final CallLogBackupState state;
        try {
            state = readState(dataInput);
        } finally {
            dataInput.close();
        }

        // Run the actual backup of data
        runBackup(state, data);

        // Rewrite the backup state.
        DataOutputStream dataOutput = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(newStateDescriptor.getFileDescriptor())));
        try {
            writeState(dataOutput, state);
        } finally {
            dataOutput.close();
        }
    }

    /** ${inheritDoc} */
    @Override
    public void onRestore(BackupDataInput data, int appVersionCode, ParcelFileDescriptor newState)
            throws IOException {
        if (isDebug()) {
            Log.d(TAG, "Performing Restore");
        }

        while (data.readNextHeader()) {
            Call call = readCallFromData(data);
            if (call != null) {
                writeCallToProvider(call);
                if (isDebug()) {
                    Log.d(TAG, "Restored call: " + call);
                }
            }
        }
    }

    @VisibleForTesting
    void runBackup(CallLogBackupState state, BackupDataOutput data) {
        SortedSet<Integer> callsToRemove = new TreeSet<>(state.callIds);

        // Get all the existing call log entries.
        Cursor cursor = getAllCallLogEntries();
        if (cursor == null) {
            return;
        }

        try {
            // Loop through all the call log entries to identify:
            // (1) new calls
            // (2) calls which have been deleted.
            while (cursor.moveToNext()) {
                Call call = readCallFromCursor(cursor);

                if (!state.callIds.contains(call.id)) {

                    if (isDebug()) {
                        Log.d(TAG, "Adding call to backup: " + call);
                    }

                    // This call new (not in our list from the last backup), lets back it up.
                    addCallToBackup(data, call);
                    state.callIds.add(call.id);
                } else {
                    // This call still exists in the current call log so delete it from the
                    // "callsToRemove" set since we want to keep it.
                    callsToRemove.remove(call.id);
                }
            }

            // Remove calls which no longer exist in the set.
            for (Integer i : callsToRemove) {
                if (isDebug()) {
                    Log.d(TAG, "Removing call from backup: " + i);
                }

                removeCallFromBackup(data, i);
                state.callIds.remove(i);
            }

        } finally {
            cursor.close();
        }
    }

    private Cursor getAllCallLogEntries() {
        // We use the API here instead of querying ContactsDatabaseHelper directly because
        // CallLogProvider has special locks in place for sychronizing when to read.  Using the APIs
        // gives us that for free.
        ContentResolver resolver = getContentResolver();
        return resolver.query(CallLog.Calls.CONTENT_URI, CALL_LOG_PROJECTION, null, null, null);
    }

    private void writeCallToProvider(Call call) {
        Long dataUsage = call.dataUsage == 0 ? null : call.dataUsage;

        PhoneAccountHandle handle = new PhoneAccountHandle(
                ComponentName.unflattenFromString(call.accountComponentName), call.accountId);
        Calls.addCall(null /* CallerInfo */, this, call.number, call.numberPresentation, call.type,
                call.features, handle, call.date, (int) call.duration,
                dataUsage, true /* addForAllUsers */);
    }

    @VisibleForTesting
    CallLogBackupState readState(DataInput dataInput) throws IOException {
        CallLogBackupState state = new CallLogBackupState();
        state.callIds = new TreeSet<>();

        try {
            // Read the version.
            state.version = dataInput.readInt();

            if (state.version >= 1) {
                // Read the size.
                int size = dataInput.readInt();

                // Read all of the call IDs.
                for (int i = 0; i < size; i++) {
                    state.callIds.add(dataInput.readInt());
                }
            }
        } catch (EOFException e) {
            state.version = VERSION_NO_PREVIOUS_STATE;
        }

        return state;
    }

    @VisibleForTesting
    void writeState(DataOutput dataOutput, CallLogBackupState state)
            throws IOException {
        // Write version first of all
        dataOutput.writeInt(VERSION);

        // [Version 1]
        // size + callIds
        dataOutput.writeInt(state.callIds.size());
        for (Integer i : state.callIds) {
            dataOutput.writeInt(i);
        }
    }

    @VisibleForTesting
    Call readCallFromData(BackupDataInput data) {
        final int callId;
        try {
            callId = Integer.parseInt(data.getKey());
        } catch (NumberFormatException e) {
            Log.e(TAG, "Unexpected key found in restore: " + data.getKey());
            return null;
        }

        try {
            byte [] byteArray = new byte[data.getDataSize()];
            data.readEntityData(byteArray, 0, byteArray.length);
            DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(byteArray));

            Call call = new Call();
            call.id = callId;

            int version = dataInput.readInt();
            if (version >= 1) {
                call.date = dataInput.readLong();
                call.duration = dataInput.readLong();
                call.number = dataInput.readUTF();
                call.type = dataInput.readInt();
                call.numberPresentation = dataInput.readInt();
                call.accountComponentName = dataInput.readUTF();
                call.accountId = dataInput.readUTF();
                call.accountAddress = dataInput.readUTF();
                call.dataUsage = dataInput.readLong();
                call.features = dataInput.readInt();
            }

            return call;
        } catch (IOException e) {
            Log.e(TAG, "Error reading call data for " + callId, e);
            return null;
        }
    }

    private Call readCallFromCursor(Cursor cursor) {
        Call call = new Call();
        call.id = cursor.getInt(cursor.getColumnIndex(CallLog.Calls._ID));
        call.date = cursor.getLong(cursor.getColumnIndex(CallLog.Calls.DATE));
        call.duration = cursor.getLong(cursor.getColumnIndex(CallLog.Calls.DURATION));
        call.number = cursor.getString(cursor.getColumnIndex(CallLog.Calls.NUMBER));
        call.type = cursor.getInt(cursor.getColumnIndex(CallLog.Calls.TYPE));
        call.numberPresentation =
                cursor.getInt(cursor.getColumnIndex(CallLog.Calls.NUMBER_PRESENTATION));
        call.accountComponentName =
                cursor.getString(cursor.getColumnIndex(CallLog.Calls.PHONE_ACCOUNT_COMPONENT_NAME));
        call.accountId =
                cursor.getString(cursor.getColumnIndex(CallLog.Calls.PHONE_ACCOUNT_ID));
        call.accountAddress =
                cursor.getString(cursor.getColumnIndex(CallLog.Calls.PHONE_ACCOUNT_ADDRESS));
        call.dataUsage = cursor.getLong(cursor.getColumnIndex(CallLog.Calls.DATA_USAGE));
        call.features = cursor.getInt(cursor.getColumnIndex(CallLog.Calls.FEATURES));
        return call;
    }

    private void addCallToBackup(BackupDataOutput output, Call call) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(baos);

        try {
            data.writeInt(VERSION);
            data.writeLong(call.date);
            data.writeLong(call.duration);
            data.writeUTF(call.number);
            data.writeInt(call.type);
            data.writeInt(call.numberPresentation);
            data.writeUTF(call.accountComponentName);
            data.writeUTF(call.accountId);
            data.writeUTF(call.accountAddress);
            data.writeLong(call.dataUsage);
            data.writeInt(call.features);
            data.flush();

            output.writeEntityHeader(Integer.toString(call.id), baos.size());
            output.writeEntityData(baos.toByteArray(), baos.size());

            if (isDebug()) {
                Log.d(TAG, "Wrote call to backup: " + call + " with byte array: " + baos);
            }
        } catch (IOException e) {
            Log.e(TAG, "Failed to backup call: " + call, e);
        }
    }

    private void removeCallFromBackup(BackupDataOutput output, int callId) {
        try {
            output.writeEntityHeader(Integer.toString(callId), -1);
        } catch (IOException e) {
            Log.e(TAG, "Failed to remove call: " + callId, e);
        }
    }

    private static boolean isDebug() {
        return Log.isLoggable(TAG, Log.DEBUG);
    }
}
