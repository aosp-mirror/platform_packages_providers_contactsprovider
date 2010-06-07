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

import android.content.BroadcastReceiver;
import android.content.ContentProvider;
import android.content.Context;
import android.content.IContentProvider;
import android.content.Intent;
import android.net.Uri;
import android.provider.ContactsContract;

/**
 * Package uninstall receiver whose job is to remove orphaned contact directories.
 */
public class PackageUninstallReceiver extends BroadcastReceiver {

    @Override
    public void onReceive(Context context, Intent intent) {
        if (Intent.ACTION_PACKAGE_REMOVED.equals(intent.getAction())) {
            // TODO: do the work in an IntentService
            Uri packageUri = intent.getData();
            String packageName = packageUri.getSchemeSpecificPart();
            IContentProvider iprovider =
                    context.getContentResolver().acquireProvider(ContactsContract.AUTHORITY);
            ContentProvider provider = ContentProvider.coerceToLocalContentProvider(iprovider);
            if (provider instanceof ContactsProvider2) {
                ((ContactsProvider2)provider).onPackageUninstalled(packageName);
            }
        }
    }
}
