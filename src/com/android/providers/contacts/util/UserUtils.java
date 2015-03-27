/*
 * Copyright (C) 2014 The Android Open Source Project
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

import com.android.providers.contacts.ContactsProvider2;

import android.app.admin.DevicePolicyManager;
import android.content.Context;
import android.content.pm.UserInfo;
import android.os.UserHandle;
import android.os.UserManager;
import android.util.Log;

public final class UserUtils {
    public static final String TAG = ContactsProvider2.TAG;

    public static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE);

    private UserUtils() {
    }

    public static UserManager getUserManager(Context context) {
        return (UserManager) context.getSystemService(Context.USER_SERVICE);
    }

    private static DevicePolicyManager getDevicePolicyManager(Context context) {
        return (DevicePolicyManager) context.getSystemService(Context.DEVICE_POLICY_SERVICE);
    }

    public static int getCurrentUserHandle(Context context) {
        return getUserManager(context).getUserHandle();
    }

    /**
     * @param enforceCallerIdCheck True if we want to enforce cross profile
     *            caller-id device policy.
     * @return the user ID of the corp user that is linked to the current user,
     *         if any. If there's no such user or cross-user contacts access is
     *         disallowed by policy, returns -1.
     */
    public static int getCorpUserId(Context context, boolean enforceCallerIdCheck) {
        final UserManager um = getUserManager(context);
        if (um == null) {
            Log.e(TAG, "No user manager service found");
            return -1;
        }

        final int myUser = um.getUserHandle();

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "getCorpUserId: myUser=" + myUser);
        }

        // Check each user.
        for (UserInfo ui : um.getUsers()) {
            if (!ui.isManagedProfile()) {
                continue; // Not a managed user.
            }
            final UserInfo parent = um.getProfileParent(ui.id);
            if (parent == null) {
                continue; // No parent.
            }
            // Check if it's linked to the current user.
            if (parent.id == myUser) {
                // Check if profile is blocking calling id.
                // TODO DevicePolicyManager is not mockable -- the constructor is private.
                // Test it somehow.
                if (enforceCallerIdCheck
                        && getDevicePolicyManager(context)
                        .getCrossProfileCallerIdDisabled(ui.getUserHandle())) {
                    if (VERBOSE_LOGGING) {
                        Log.v(TAG, "Enterprise caller-id disabled for user " + ui.id);
                    }
                    return -1;
                } else {
                    if (VERBOSE_LOGGING) {
                        Log.v(TAG, "Corp user=" + ui.id);
                    }
                    return ui.id;
                }
            }
        }
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "Corp user not found.");
        }
        return -1;
    }
}
