/*
 * Copyright (C) 2011 The Android Open Source Project
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

import com.google.android.collect.Lists;

import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import java.util.List;

/**
 * Cursor wrapper that handles tracking time taken before query result came back and how long
 * the cursor was open before it was closed.
 */
public class InstrumentedCursorWrapper extends CrossProcessCursorWrapper {

    /**
     * Static list of active cursors.
     */
    private static List<InstrumentedCursorWrapper> mActiveCursors = Lists.newArrayList();

    /**
     * Time (ms since epoch) when the cursor was created.
     */
    private long mCreationTime;

    /**
     * Milliseconds after creation at which the query completed (triggered by a getCount or
     * any method that moves the cursor).
     */
    private long mTimeToQuery;

    /**
     * The URI being queried in this cursor.
     */
    private Uri mUri;

    /**
     * Log tag to use.
     */
    private String mTag;

    public InstrumentedCursorWrapper(Cursor cursor, Uri uri, String tag) {
        super(cursor);
        mCreationTime = System.currentTimeMillis();
        mUri = uri;
        mTag = tag;
        mActiveCursors.add(this);
    }

    @Override
    public int getCount() {
        int count = super.getCount();
        logQueryTime();
        return count;
    }

    @Override
    public boolean moveToFirst() {
        boolean result = super.moveToFirst();
        logQueryTime();
        return result;
    }

    @Override
    public boolean moveToLast() {
        boolean result = super.moveToLast();
        logQueryTime();
        return result;
    }

    @Override
    public boolean move(int offset) {
        boolean result = super.move(offset);
        logQueryTime();
        return result;
    }

    @Override
    public boolean moveToPosition(int position) {
        boolean result = super.moveToPosition(position);
        logQueryTime();
        return result;
    }

    @Override
    public boolean moveToNext() {
        boolean result = super.moveToNext();
        logQueryTime();
        return result;
    }

    @Override
    public boolean moveToPrevious() {
        boolean result = super.moveToPrevious();
        logQueryTime();
        return result;
    }

    @Override
    public void close() {
        super.close();
        long timeToClose = System.currentTimeMillis() - mCreationTime;
        Log.v(mTag, timeToClose + "ms to close for URI " + mUri
                + " (" + (timeToClose - mTimeToQuery) + "ms since query complete)");
        mActiveCursors.remove(this);
        Log.v(mTag, mActiveCursors.size() + " cursors still open");
    }

    private void logQueryTime() {
        if (mTimeToQuery == 0) {
            mTimeToQuery = System.currentTimeMillis() - mCreationTime;
            Log.v(mTag, mTimeToQuery + "ms to query URI " + mUri);
        }
    }
}
