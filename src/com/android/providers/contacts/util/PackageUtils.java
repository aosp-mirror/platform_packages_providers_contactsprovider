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
package com.android.providers.contacts.util;

import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;

public class PackageUtils {
    private PackageUtils() {
    }

    /**
     * @return TRUE if the given package is installed for this user.
     */
    public static boolean isPackageInstalled(Context context, String packageName) {
        try {
            // Need to pass MATCH_UNINSTALLED_PACKAGES to fetch it even if the package is
            // being updated.  Then use FLAG_INSTALLED to see if it's actually installed for this
            // user.
            final ApplicationInfo ai = context.getPackageManager().getApplicationInfo(packageName,
                    PackageManager.MATCH_DIRECT_BOOT_AWARE
                            | PackageManager.MATCH_DIRECT_BOOT_UNAWARE
                            | PackageManager.MATCH_UNINSTALLED_PACKAGES);
            return (ai != null) && ((ai.flags & ApplicationInfo.FLAG_INSTALLED) != 0);
        } catch (NameNotFoundException e) {
            return false;
        }
    }
}
