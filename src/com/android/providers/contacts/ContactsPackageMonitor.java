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

import android.content.BroadcastReceiver;
import android.content.BroadcastReceiver.PendingResult;
import android.content.ContentProvider;
import android.content.Context;
import android.content.IContentProvider;
import android.content.Intent;
import android.content.IntentFilter;
import android.provider.ContactsContract;
import android.provider.VoicemailContract;
import android.text.TextUtils;
import android.util.Log;
import android.util.Slog;

import com.android.providers.contacts.util.PackageUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * - Handles package related broadcasts.
 * - Also scan changed packages while the process wasn't running using PM.getChangedPackages().
 */
public class ContactsPackageMonitor {
    private static final String TAG = "ContactsPackageMonitor";

    private static final boolean VERBOSE_LOGGING = AbstractContactsProvider.VERBOSE_LOGGING;

    private static final int BACKGROUND_TASK_PACKAGE_EVENT = 0;

    private static ContactsPackageMonitor sInstance;

    private Context mContext;

    /** We run all BG tasks on this thread/handler sequentially. */
    private final ContactsTaskScheduler mTaskScheduler;

    private static class PackageEventArg {
        final String packageName;
        final PendingResult broadcastPendingResult;

        private PackageEventArg(String packageName, PendingResult broadcastPendingResult) {
            this.packageName = packageName;
            this.broadcastPendingResult = broadcastPendingResult;
        }
    }

    private ContactsPackageMonitor(Context context) {
        mContext = context; // Can't use the app context due to a bug with shared process.

        // Start the BG thread and register the receiver.
        mTaskScheduler = new ContactsTaskScheduler(getClass().getSimpleName()) {
            @Override
            public void onPerformTask(int taskId, Object arg) {
                switch (taskId) {
                    case BACKGROUND_TASK_PACKAGE_EVENT:
                        onPackageChanged((PackageEventArg) arg);
                        break;
                }
            }
        };
    }

    private void start() {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "Starting... user="
                    + android.os.Process.myUserHandle().getIdentifier());
        }

        registerReceiver();
    }

    public static synchronized void start(Context context) {
        if (sInstance == null) {
            sInstance = new ContactsPackageMonitor(context);
            sInstance.start();
        }
    }

    private void registerReceiver() {
        final IntentFilter filter = new IntentFilter();

        filter.addAction(Intent.ACTION_PACKAGE_ADDED);
        filter.addAction(Intent.ACTION_PACKAGE_REMOVED);
        filter.addAction(Intent.ACTION_PACKAGE_REPLACED);
        filter.addAction(Intent.ACTION_PACKAGE_CHANGED);
        filter.addDataScheme("package");

        mContext.registerReceiver(new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                if (intent.getData() == null) {
                    return; // Shouldn't happen.
                }
                final String changedPackage = intent.getData().getSchemeSpecificPart();
                final PendingResult result = goAsync();

                mTaskScheduler.scheduleTask(BACKGROUND_TASK_PACKAGE_EVENT,
                        new PackageEventArg(changedPackage, result));
            }
        }, filter);
    }

    private void onPackageChanged(PackageEventArg arg) {
        try {
            final String packageName = arg.packageName;
            if (TextUtils.isEmpty(packageName)) {
                Log.w(TAG, "Empty package name detected.");
                return;
            }
            if (VERBOSE_LOGGING) Log.d(TAG, "onPackageChanged: Scanning package: " + packageName);

            // First, tell CP2.
            final ContactsProvider2 provider = getProvider(mContext, ContactsContract.AUTHORITY);
            if (provider != null) {
                provider.onPackageChanged(packageName);
            }

            // Next, if the package is gone, clean up the voicemail.
            cleanupVoicemail(mContext, packageName);
        } finally {
            if (VERBOSE_LOGGING) Log.v(TAG, "Calling PendingResult.finish()...");
            arg.broadcastPendingResult.finish();
        }
    }

    @VisibleForTesting
    static void cleanupVoicemail(Context context, String packageName) {
        if (PackageUtils.isPackageInstalled(context, packageName)) {
            return; // Still installed.
        }
        if (VERBOSE_LOGGING) Log.d(TAG, "Cleaning up data for package: " + packageName);

        // Delete both voicemail content and voicemail status entries for this package.
        final VoicemailContentProvider provider = getProvider(context, VoicemailContract.AUTHORITY);
        if (provider != null) {
            provider.removeBySourcePackage(packageName);
        }
    }

    private static <T extends ContentProvider> T getProvider(Context context, String authority) {
        final IContentProvider iprovider = context.getContentResolver().acquireProvider(authority);
        final ContentProvider provider = ContentProvider.coerceToLocalContentProvider(iprovider);
        if (provider != null) {
            return (T) provider;
        }
        Slog.wtf(TAG, "Provider for " + authority + " not found");
        return null;
    }
}
