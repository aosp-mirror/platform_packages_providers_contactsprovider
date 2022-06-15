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

import android.content.BroadcastReceiver;
import android.content.ContentProvider;
import android.content.Context;
import android.content.IContentProvider;
import android.content.Intent;
import android.provider.CallLog;
import android.telecom.PhoneAccountHandle;
import android.telecom.TelecomManager;

/**
 * This will be launched when a new phone account is registered in telecom. It is used by the call
 * log to un-hide any entries which were previously hidden after a backup-restore until it's
 * associated phone-account is registered with telecom.
 *
 * IOW, after a restore, we hide call log entries until the user inserts the corresponding SIM,
 * registers the corresponding SIP account, or registers a corresponding alternative phone-account.
 */
public class PhoneAccountRegistrationReceiver extends BroadcastReceiver {
    static final String TAG = "PhoneAccountReceiver";

    @Override
    public void onReceive(Context context, Intent intent) {
        // We are now running with the system up, but no apps started,
        // so can do whatever cleanup after an upgrade that we want.
        if (TelecomManager.ACTION_PHONE_ACCOUNT_REGISTERED.equals(intent.getAction())) {

            PhoneAccountHandle handle = (PhoneAccountHandle) intent.getParcelableExtra(
                    TelecomManager.EXTRA_PHONE_ACCOUNT_HANDLE);

            IContentProvider iprovider =
                    context.getContentResolver().acquireProvider(CallLog.AUTHORITY);
            ContentProvider provider = ContentProvider.coerceToLocalContentProvider(iprovider);
            if (provider instanceof CallLogProvider) {
                ((CallLogProvider) provider).adjustForNewPhoneAccount(handle);
            }
        }
    }
}
