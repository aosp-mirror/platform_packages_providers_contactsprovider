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
     * @param context Context
     * @return {@link UserInfo} of the corp user that is linked to the current user,
     *         if any. If there's no such user or cross-user contacts access is
     *         disallowed by policy, returns null.
     */
    private static UserInfo getCorpUserInfo(Context context) {
        final UserManager um = getUserManager(context);
        final int myUser = um.getUserHandle();

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
                return ui;
            }
        }
        return null;
    }

    /**
     * @return the user ID of the corp user that is linked to the current user,
     *         if any. If there's no such user returns -1.
     */
    public static int getCorpUserId(Context context) {
        final UserInfo ui = getCorpUserInfo(context);
        return ui == null ? -1 : ui.id;
    }
}
