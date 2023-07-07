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

import android.net.Uri;

public final class LogFields {

    private final int apiType;

    private final int uriType;

    // The type is from LogUtils.TaskType
    private final int taskType;

    private final boolean callerIsSyncAdapter;

    private final long startNanos;

    private Exception exception;

    private Uri resultUri;

    private int resultCount;

    private int uid;

    public LogFields(
            int apiType, int uriType, int taskType, boolean callerIsSyncAdapter, long startNanos) {
        this.apiType = apiType;
        this.uriType = uriType;
        this.taskType = taskType;
        this.callerIsSyncAdapter = callerIsSyncAdapter;
        this.startNanos = startNanos;
    }

    public int getApiType() {
        return apiType;
    }

    public int getUriType() {
        return uriType;
    }

    public int getTaskType() {
        return taskType;
    }

    public boolean isCallerIsSyncAdapter() {
        return callerIsSyncAdapter;
    }

    public long getStartNanos() {
        return startNanos;
    }

    public Exception getException() {
        return exception;
    }

    public Uri getResultUri() {
        return resultUri;
    }

    public int getResultCount() {
        return resultCount;
    }

    public int getUid() {
        return uid;
    }

    public static final class Builder {
        private int apiType;
        private int uriType;
        private int taskType;
        private boolean callerIsSyncAdapter;
        private long startNanos;
        private Exception exception;
        private Uri resultUri;
        private int resultCount;

        private int uid;

        private Builder() {
        }

        public static Builder aLogFields() {
            return new Builder();
        }

        public Builder setApiType(int apiType) {
            this.apiType = apiType;
            return this;
        }

        public Builder setUriType(int uriType) {
            this.uriType = uriType;
            return this;
        }

        public Builder setTaskType(int taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder setCallerIsSyncAdapter(boolean callerIsSyncAdapter) {
            this.callerIsSyncAdapter = callerIsSyncAdapter;
            return this;
        }

        public Builder setStartNanos(long startNanos) {
            this.startNanos = startNanos;
            return this;
        }

        public Builder setException(Exception exception) {
            this.exception = exception;
            return this;
        }

        public Builder setResultUri(Uri resultUri) {
            this.resultUri = resultUri;
            return this;
        }

        public Builder setResultCount(int resultCount) {
            this.resultCount = resultCount;
            return this;
        }

        public Builder setUid(int uid) {
            this.uid = uid;
            return this;
        }

        public LogFields build() {
            LogFields logFields =
                    new LogFields(apiType, uriType, taskType, callerIsSyncAdapter, startNanos);
            logFields.resultCount = this.resultCount;
            logFields.exception = this.exception;
            logFields.resultUri = this.resultUri;
            logFields.uid = this.uid;
            return logFields;
        }
    }
}
