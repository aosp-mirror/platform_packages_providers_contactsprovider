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
 * limitations under the License
 */
package com.android.providers.contacts;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.Process;
import android.util.Log;

/**
 * A scheduler for asynchronous aggregation of contacts. Aggregation will start after
 * a short delay after it is scheduled, unless it is scheduled again, in which case the
 * aggregation pass is delayed.  There is an upper boundary on how long aggregation can
 * be delayed.
 */
public class ContactAggregationScheduler {

    private static final String TAG = "ContactAggregator";

    public interface Aggregator {

        /**
         * Performs an aggregation run.
         */
        void run();

        /**
         * Interrupts aggregation.
         */
        void interrupt();
    }

    // Message ID used for communication with the aggregator
    private static final int START_AGGREGATION_MESSAGE_ID = 1;

    // Aggregation is delayed by this many milliseconds to allow changes to accumulate
    public static final int AGGREGATION_DELAY = 1000;

    // Maximum delay of aggregation from the initial aggregation request
    public static final int MAX_AGGREGATION_DELAY = 10000;

    // Minimum gap between requests that should cause a delay of aggregation
    public static final int DELAYED_EXECUTION_TIMEOUT = 500;

    public static final int STATUS_STAND_BY = 0;
    public static final int STATUS_SCHEDULED = 1;
    public static final int STATUS_RUNNING = 2;
    public static final int STATUS_INTERRUPTED = 3;


    private Aggregator mAggregator;

    // Aggregation status
    private int mStatus = STATUS_STAND_BY;

    // If true, we need to automatically reschedule aggregation after the current pass is done
    private boolean mRescheduleWhenComplete;

    // The time when aggregation was requested for the first time.
    // Reset when aggregation is completed
    private long mInitialRequestTimestamp;

    // Last time aggregation was requested
    private long mLastAggregationEndedTimestamp;

    private HandlerThread mHandlerThread;
    private Handler mMessageHandler;

    public void setAggregator(Aggregator aggregator) {
        mAggregator = aggregator;
    }

    public void start() {
        mHandlerThread = new HandlerThread("ContactAggregator", Process.THREAD_PRIORITY_BACKGROUND);
        mHandlerThread.start();
        mMessageHandler = new Handler(mHandlerThread.getLooper()) {

            @Override
            public void handleMessage(Message msg) {
                switch (msg.what) {
                    case START_AGGREGATION_MESSAGE_ID:
                        run();
                        break;

                    default:
                        throw new IllegalStateException("Unhandled message: " + msg.what);
                }
            }
        };
    }

    public void stop() {
        mAggregator.interrupt();
        Looper looper = mHandlerThread.getLooper();
        if (looper != null) {
            looper.quit();
        }
    }

    /**
     * Schedules an aggregation pass after a short delay.
     */
    public synchronized void schedule() {

        switch (mStatus) {
            case STATUS_STAND_BY: {

                mInitialRequestTimestamp = currentTime();
                mStatus = STATUS_SCHEDULED;
                if (mInitialRequestTimestamp - mLastAggregationEndedTimestamp <
                        DELAYED_EXECUTION_TIMEOUT) {
                    runDelayed();
                } else {
                    runNow();
                }
                break;
            }

            case STATUS_INTERRUPTED: {

                // If the previous aggregation run was interrupted, do not reset
                // the initial request timestamp - we don't want a continuous string
                // of interrupted runs
                mStatus = STATUS_SCHEDULED;
                runDelayed();
                break;
            }

            case STATUS_SCHEDULED: {

                // If it has been less than MAX_AGGREGATION_DELAY millis since the initial request,
                // reschedule the request.
                if (currentTime() - mInitialRequestTimestamp < MAX_AGGREGATION_DELAY) {
                    runDelayed();
                }
                break;
            }

            case STATUS_RUNNING: {

                // If it has been less than MAX_AGGREGATION_DELAY millis since the initial request,
                // interrupt the current pass and reschedule the request.
                if (currentTime() - mInitialRequestTimestamp < MAX_AGGREGATION_DELAY) {
                    mAggregator.interrupt();
                    mStatus = STATUS_INTERRUPTED;
                }

                mRescheduleWhenComplete = true;
                break;
            }
        }
    }

    /**
     * Called just before an aggregation pass begins.
     */
    public void run() {
        synchronized (this) {
            mStatus = STATUS_RUNNING;
            mRescheduleWhenComplete = false;
        }
        try {
            mAggregator.run();
        } finally {
            mLastAggregationEndedTimestamp = currentTime();
            synchronized (this) {
                if (mStatus == STATUS_RUNNING) {
                    mStatus = STATUS_STAND_BY;
                }
                if (mRescheduleWhenComplete) {
                    mRescheduleWhenComplete = false;
                    schedule();
                } else {
                    Log.w(TAG, "No more aggregation requests");
                }
            }
        }
    }

    /* package */ void runNow() {

        // If aggregation has already been requested, cancel the previous request
        mMessageHandler.removeMessages(START_AGGREGATION_MESSAGE_ID);

        // Schedule aggregation for right now
        mMessageHandler.sendEmptyMessage(START_AGGREGATION_MESSAGE_ID);
    }

    /* package */ void runDelayed() {

        // If aggregation has already been requested, cancel the previous request
        mMessageHandler.removeMessages(START_AGGREGATION_MESSAGE_ID);

        // Schedule aggregation for AGGREGATION_DELAY milliseconds from now
        mMessageHandler.sendEmptyMessageDelayed(
                START_AGGREGATION_MESSAGE_ID, AGGREGATION_DELAY);
    }

    /* package */ long currentTime() {
        return System.currentTimeMillis();
    }
}
