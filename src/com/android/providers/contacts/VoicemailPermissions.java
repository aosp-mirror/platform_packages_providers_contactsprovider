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

import android.content.Context;
import android.os.Binder;
import android.telecom.DefaultDialerManager;
import android.telephony.TelephonyManager;

import com.android.providers.contacts.util.ContactsPermissions;

/**
 * Provides method related to check various voicemail permissions under the
 * specified context.
 * <p> This is an immutable object.
 */
public class VoicemailPermissions {
    private final Context mContext;

    public VoicemailPermissions(Context context) {
        mContext = context;
    }

    /** Determines if the calling process has access to its own voicemails. */
    public boolean callerHasOwnVoicemailAccess() {
        return callerHasPermission(android.Manifest.permission.ADD_VOICEMAIL)
                || callerHasCarrierPrivileges();
    }

    /** Determine if the calling process has full read access to all voicemails. */
    public boolean callerHasReadAccess(String callingPackage) {
        if (DefaultDialerManager.isDefaultOrSystemDialer(mContext, callingPackage)) {
            return true;
        }
        return callerHasPermission(android.Manifest.permission.READ_VOICEMAIL);
    }

    /** Determine if the calling process has the permission required to update and remove all
     * voicemails */
    public boolean callerHasWriteAccess(String callingPackage) {
        if (DefaultDialerManager.isDefaultOrSystemDialer(mContext, callingPackage)) {
            return true;
        }
        return callerHasPermission(android.Manifest.permission.WRITE_VOICEMAIL);
    }

    /**
     * Checks that the caller has permissions to access its own voicemails.
     *
     * @throws SecurityException if the caller does not have the voicemail source permission.
     */
    public void checkCallerHasOwnVoicemailAccess() {
        if (!callerHasOwnVoicemailAccess()) {
            throw new SecurityException("The caller must have permission: " +
                    android.Manifest.permission.ADD_VOICEMAIL + " or carrier privileges");
        }
    }

    /**
     * Checks that the caller has permissions to read ALL voicemails.
     *
     * @throws SecurityException if the caller does not have the voicemail source permission.
     */
    public void checkCallerHasReadAccess(String callingPackage) {
        if (!callerHasReadAccess(callingPackage)) {
            throw new SecurityException(String.format("The caller must be the default or system "
                    + "dialer, or have the system-only %s permission: ",
                            android.Manifest.permission.READ_VOICEMAIL));
        }
    }

    public void checkCallerHasWriteAccess(String callingPackage) {
        if (!callerHasWriteAccess(callingPackage)) {
            throw new SecurityException(String.format("The caller must be the default or system "
                    + "dialer, or have the system-only %s permission: ",
                            android.Manifest.permission.WRITE_VOICEMAIL));
        }
    }

    /** Determines if the given package has access to its own voicemails. */
    public boolean packageHasOwnVoicemailAccess(String packageName) {
        return packageHasPermission(packageName,
                android.Manifest.permission.ADD_VOICEMAIL)
                || packageHasCarrierPrivileges(packageName);
    }

    /** Determines if the given package has read access. */
    public boolean packageHasReadAccess(String packageName) {
        return packageHasPermission(packageName, android.Manifest.permission.READ_VOICEMAIL);
    }

    /** Determines if the given package has write access. */
    public boolean packageHasWriteAccess(String packageName) {
        return packageHasPermission(packageName, android.Manifest.permission.WRITE_VOICEMAIL);
    }

    /** Determines if the given package has the given permission. */
    private boolean packageHasPermission(String packageName, String permission) {
        return ContactsPermissions.hasPackagePermission(mContext, permission, packageName);
    }

    /** Determines if the calling process has the given permission. */
    private boolean callerHasPermission(String permission) {
        return ContactsPermissions.hasCallerOrSelfPermission(mContext, permission);
    }

    /** Determines if the calling process has carrier privileges. */
    public boolean callerHasCarrierPrivileges() {
        TelephonyManager tm =
                (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
        String[] packages = mContext.getPackageManager().getPackagesForUid(Binder.getCallingUid());
        for (String packageName : packages) {
            if (tm.checkCarrierPrivilegesForPackageAnyPhone(packageName)
                    == TelephonyManager.CARRIER_PRIVILEGE_STATUS_HAS_ACCESS) {
                return true;
            }
        }
        return false;
    }

    /** Determines if the given package has carrier privileges. */
    private boolean packageHasCarrierPrivileges(String packageName) {
        TelephonyManager tm =
                (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
        return tm.getPackagesWithCarrierPrivileges().contains(packageName);
    }
}
