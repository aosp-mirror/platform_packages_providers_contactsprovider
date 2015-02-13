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
import android.content.ContentResolver;
import android.database.Cursor;
import android.os.ParcelFileDescriptor;
import android.provider.CallLog;
import android.util.Log;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
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

    private static class CallLogBackupState {
        int version;
        SortedSet<Integer> callIds;
    }

    private static class Call {
        int id;

        @Override
        public String toString() {
            return "[" + id + "]";
        }
    }

    private static final String TAG = "CallLogBackupAgent";
    private static final boolean DEBUG = Log.isLoggable(TAG, Log.DEBUG);

    /** Current version of CallLogBackup. Used to track the backup format. */
    private static final int VERSION = 1;
    /** Version indicating that there exists no previous backup entry. */
    private static final int VERSION_NO_PREVIOUS_STATE = 0;

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
        CallLog.Calls.PHONE_ACCOUNT_ADDRESS
    };

    /** ${inheritDoc} */
    @Override
    public void onBackup(ParcelFileDescriptor oldStateDescriptor, BackupDataOutput data,
            ParcelFileDescriptor newStateDescriptor) throws IOException {

        // Get the list of the previous calls IDs which were backed up.
        CallLogBackupState state = readState(oldStateDescriptor);
        SortedSet<Integer> callsToRemove = new TreeSet<>(state.callIds);

        // Get all the existing call log entries.
        Cursor cursor = getAllCallLogEntries();
        if (DEBUG) {
            Log.d(TAG, "Starting debug - state: " + state + ", cursor: " + cursor);
        }
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

                    if (DEBUG) {
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
                if (DEBUG) {
                    Log.d(TAG, "Removing call from backup: " + i);
                }

                removeCallFromBackup(data, i);
                state.callIds.remove(i);
            }

            // Rewrite the backup state.
            writeState(newStateDescriptor, state);
        } finally {
            cursor.close();
        }
    }

    /** ${inheritDoc} */
    @Override
    public void onRestore(BackupDataInput data, int appVersionCode, ParcelFileDescriptor newState)
            throws IOException {
        if (DEBUG) {
            Log.d(TAG, "Performing Restore");
        }
    }

    private Cursor getAllCallLogEntries() {
        // We use the API here instead of querying ContactsDatabaseHelper directly because
        // CallLogProvider has special locks in place for sychronizing when to read.  Using the APIs
        // gives us that for free.
        ContentResolver resolver = getContentResolver();
        return resolver.query(CallLog.Calls.CONTENT_URI, CALL_LOG_PROJECTION, null, null, null);
    }

    private CallLogBackupState readState(ParcelFileDescriptor oldState) throws IOException {
        DataInputStream dataInput = new DataInputStream(
                new FileInputStream(oldState.getFileDescriptor()));
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
        } finally {
            dataInput.close();
        }

        return state;
    }

    private void writeState(ParcelFileDescriptor descriptor, CallLogBackupState state)
            throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(descriptor.getFileDescriptor())));

        // Write version first of all
        dataOutput.writeInt(VERSION);

        // [Version 1]
        // size + callIds
        dataOutput.writeInt(state.callIds.size());
        for (Integer i : state.callIds) {
            dataOutput.writeInt(i);
        }

        // Done!
        dataOutput.close();
    }

    private Call readCallFromCursor(Cursor cursor) {
        Call call = new Call();
        call.id = cursor.getInt(cursor.getColumnIndex(CallLog.Calls._ID));
        // TODO: Rest of call data.
        return call;
    }

    private void addCallToBackup(BackupDataOutput output, Call call) {
        // TODO: Write the code
    }

    private void removeCallFromBackup(BackupDataOutput output, int callId) {
        // TODO: Write the code
    }
}
