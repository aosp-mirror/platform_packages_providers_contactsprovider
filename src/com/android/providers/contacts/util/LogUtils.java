/*
 * Copyright (C) 2020 The Android Open Source Project
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

package com.android.providers.contacts.util;

import android.os.SystemClock;
import android.util.StatsEvent;
import android.util.StatsLog;

public class LogUtils {
    // Keep in sync with ContactsProviderStatus#ResultType in
    // frameworks/proto_logging/stats/atoms.proto file.
    public interface ResultType {
        int SUCCESS = 1;
        int FAIL = 2;
        int ILLEGAL_ARGUMENT = 3;
        int UNSUPPORTED_OPERATION = 4;
    }

    // Keep in sync with ContactsProviderStatus#ApiType in
    // frameworks/proto_logging/stats/atoms.proto file.
    public interface ApiType {
        int QUERY = 1;
        int INSERT = 2;
        int UPDATE = 3;
        int DELETE = 4;
        int CALL = 5;
        int GAL_CALL = 6;
    }

    // Keep in sync with ContactsProviderStatus#TaskType in
    // frameworks/proto_logging/stats/atoms.proto file.
    public interface TaskType {
        int DANGLING_CONTACTS_CLEANUP_TASK = 1;
    }

    // Keep in sync with ContactsProviderStatus#CallerType in
    // frameworks/proto_logging/stats/atoms.proto file.
    public interface CallerType {
        int CALLER_IS_SYNC_ADAPTER = 1;
        int CALLER_IS_NOT_SYNC_ADAPTER = 2;
    }

    private static final int STATSD_LOG_ATOM_ID = 301;

    // The write methods must be called in the same order as the order of fields in the
    // atom (frameworks/proto_logging/stats/atoms.proto) definition.
    public static void log(LogFields logFields) {
        StatsLog.write(StatsEvent.newBuilder()
                .setAtomId(STATSD_LOG_ATOM_ID)
                .writeInt(logFields.getApiType())
                .writeInt(logFields.getUriType())
                .writeInt(getCallerType(logFields.isCallerIsSyncAdapter()))
                .writeInt(getResultType(logFields.getException()))
                .writeInt(logFields.getResultCount())
                .writeLong(getLatencyMicros(logFields.getStartNanos()))
                .writeInt(logFields.getTaskType())
                .writeInt(0) // Not used yet.
                .writeInt(logFields.getUid())
                .usePooledBuffer()
                .build());
    }

    private static int getCallerType(boolean callerIsSyncAdapter) {
        return callerIsSyncAdapter
                ? CallerType.CALLER_IS_SYNC_ADAPTER : CallerType.CALLER_IS_NOT_SYNC_ADAPTER;
    }

    private static int getResultType(Exception exception) {
        if (exception == null) {
            return ResultType.SUCCESS;
        } else if (exception instanceof IllegalArgumentException) {
            return ResultType.ILLEGAL_ARGUMENT;
        } else if (exception instanceof UnsupportedOperationException) {
            return ResultType.UNSUPPORTED_OPERATION;
        } else {
            return ResultType.FAIL;
        }
    }

    private static long getLatencyMicros(long startNanos) {
        return (SystemClock.elapsedRealtimeNanos() - startNanos) / 1000;
    }
}
