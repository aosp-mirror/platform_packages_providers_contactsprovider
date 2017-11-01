/*
 * Copyright (C) 2017 The Android Open Source Project
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
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;

/**
 * Runs tasks in a worker thread, which is created on-demand and shuts down after a timeout.
 */
public abstract class ContactsTaskScheduler {
    private static final String TAG = "ContactsTaskScheduler";

    public static final boolean VERBOSE_LOGGING = AbstractContactsProvider.VERBOSE_LOGGING;

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 60;

    private final AtomicInteger mThreadSequenceNumber = new AtomicInteger();

    private final Object mLock = new Object();

    /**
     * Name of this scheduler for logging.
     */
    private final String mName;

    @GuardedBy("mLock")
    private HandlerThread mThread;

    @GuardedBy("mLock")
    private MyHandler mHandler;

    private final int mShutdownTimeoutSeconds;

    public ContactsTaskScheduler(String name) {
        this(name, SHUTDOWN_TIMEOUT_SECONDS);
    }

    /** With explicit timeout seconds, for testing. */
    protected ContactsTaskScheduler(String name, int shutdownTimeoutSeconds) {
        mName = name;
        mShutdownTimeoutSeconds = shutdownTimeoutSeconds;
    }

    private class MyHandler extends Handler {
        public MyHandler(Looper looper) {
            super(looper);
        }

        @Override
        public void handleMessage(Message msg) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "[" + mName + "] " + mThread + " dispatching " + msg.what);
            }
            onPerformTask(msg.what, msg.obj);
        }
    }

    private final Runnable mQuitter = () -> {
        synchronized (mLock) {
            stopThread(/* joinOnlyForTest=*/ false);
        }
    };

    private boolean isRunning() {
        synchronized (mLock) {
            return mThread != null;
        }
    }

    /** Schedule a task with no arguments. */
    @VisibleForTesting
    public void scheduleTask(int taskId) {
        scheduleTask(taskId, null);
    }

    /** Schedule a task with an argument. */
    @VisibleForTesting
    public void scheduleTask(int taskId, Object arg) {
        synchronized (mLock) {
            if (!isRunning()) {
                mThread = new HandlerThread("Worker-" + mThreadSequenceNumber.incrementAndGet());
                mThread.start();
                mHandler = new MyHandler(mThread.getLooper());

                if (VERBOSE_LOGGING) {
                    Log.v(TAG, "[" + mName + "] " + mThread + " started.");
                }
            }
            if (arg == null) {
                mHandler.sendEmptyMessage(taskId);
            } else {
                mHandler.sendMessage(mHandler.obtainMessage(taskId, arg));
            }

            // Schedule thread shutdown.
            mHandler.removeCallbacks(mQuitter);
            mHandler.postDelayed(mQuitter, mShutdownTimeoutSeconds * 1000);
        }
    }

    public abstract void onPerformTask(int taskId, Object arg);

    @VisibleForTesting
    public void shutdownForTest() {
        stopThread(/* joinOnlyForTest=*/ true);
    }

    private void stopThread(boolean joinOnlyForTest) {
        synchronized (mLock) {
            if (VERBOSE_LOGGING) {
                Log.v(TAG, "[" + mName + "] " + mThread + " stopping...");
            }
            if (mThread != null) {
                mThread.quit();
                if (joinOnlyForTest) {
                    try {
                        mThread.join();
                    } catch (InterruptedException ignore) {
                    }
                }
            }
            mThread = null;
            mHandler = null;
        }
    }

    @VisibleForTesting
    public int getThreadSequenceNumber() {
        return mThreadSequenceNumber.get();
    }

    @VisibleForTesting
    public boolean isRunningForTest() {
        return isRunning();
    }
}
