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
package com.android.providers.contacts.util;

import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Binder;
import android.os.Process;
import android.util.Log;

public class ContactsPermissions {
    private static final String TAG = "ContactsPermissions";

    private static final boolean DEBUG = false; // DO NOT submit with true

    // Normally, we allow calls from self, *except* in unit tests, where we clear this flag
    // to emulate calls from other apps.
    public static boolean ALLOW_SELF_CALL = true;

    private ContactsPermissions() {
    }

    public static boolean hasCallerOrSelfPermission(Context context, String permission) {
        boolean ok = false;

        if (ALLOW_SELF_CALL && Binder.getCallingPid() == Process.myPid()) {
            ok = true; // Called by self; always allow.
        } else {
            ok = context.checkCallingOrSelfPermission(permission)
                    == PackageManager.PERMISSION_GRANTED;
        }
        if (DEBUG) {
            Log.d(TAG, "hasCallerOrSelfPermission: "
                    + " perm=" + permission
                    + " caller=" + Binder.getCallingPid()
                    + " self=" + Process.myPid()
                    + " ok=" + ok);
        }
        return ok;
    }

    public static void enforceCallingOrSelfPermission(Context context, String permission) {
        final boolean ok = hasCallerOrSelfPermission(context, permission);
        if (!ok) {
            throw new SecurityException(String.format("The caller must have the %s permission.",
                    permission));
        }
    }

    public static boolean hasPackagePermission(Context context, String permission, String pkg) {
        boolean ok = false;
        if (ALLOW_SELF_CALL && context.getPackageName().equals(pkg)) {
            ok =  true; // Called by self; always allow.
        } else {
            ok = context.getPackageManager().checkPermission(permission, pkg)
                    == PackageManager.PERMISSION_GRANTED;
        }
        if (DEBUG) {
            Log.d(TAG, "hasCallerOrSelfPermission: "
                    + " perm=" + permission
                    + " pkg=" + pkg
                    + " self=" + context.getPackageName()
                    + " ok=" + ok);
        }
        return ok;
    }

    public static boolean hasCallerUriPermission(Context context, Uri uri, int modeFlags) {
        boolean ok = false;
        if (ALLOW_SELF_CALL && Binder.getCallingPid() == Process.myPid()) {
            ok =  true; // Called by self; always allow.
        } else {
            ok = context.checkCallingUriPermission(uri, modeFlags)
                    == PackageManager.PERMISSION_GRANTED;
        }
        if (DEBUG) {
            Log.d(TAG, "hasCallerUriPermission: "
                    + " uri=" + uri
                    + " caller=" + Binder.getCallingPid()
                    + " self=" + Process.myPid()
                    + " ok=" + ok);
        }
        return ok;
    }
}
