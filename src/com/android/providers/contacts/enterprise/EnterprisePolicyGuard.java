/*
 * Copyright (C) 2016 The Android Open Source Project
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

package com.android.providers.contacts.enterprise;

import android.annotation.NonNull;
import android.app.admin.DevicePolicyManager;
import android.content.Context;
import android.net.Uri;
import android.os.UserHandle;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Directory;
import android.provider.Settings;
import android.util.Log;

import com.android.providers.contacts.ContactsProvider2;
import com.android.providers.contacts.ProfileAwareUriMatcher;
import com.android.providers.contacts.util.UserUtils;
import com.google.common.annotations.VisibleForTesting;

import static android.provider.Settings.Secure.MANAGED_PROFILE_CONTACT_REMOTE_SEARCH;

/**
 * Digest contacts policy from DevcicePolicyManager and guard enterprise uri
 */
public class EnterprisePolicyGuard {
    private static final boolean VERBOSE_LOGGING = ContactsProvider2.VERBOSE_LOGGING;
    private static final String TAG = ContactsProvider2.TAG;

    private static final ProfileAwareUriMatcher sUriMatcher = ContactsProvider2.sUriMatcher;

    final private Context mContext;
    final private DevicePolicyManager mDpm;

    public EnterprisePolicyGuard(Context context) {
        mContext = context;
        mDpm = context.getSystemService(DevicePolicyManager.class);
    }

    /**
     * Check if cross profile query is allowed for the given uri
     *
     * @param uri Uri that we want to check.
     * @return True if cross profile query is allowed for this uri
     */
    public boolean isCrossProfileAllowed(@NonNull Uri uri) {
        final int uriCode = sUriMatcher.match(uri);
        final UserHandle currentHandle = new UserHandle(UserUtils.getCurrentUserHandle(mContext));
        if (uriCode == -1 || currentHandle == null) {
            return false;
        }

        if (isUriWhitelisted(uriCode)) {
            return true;
        }

        final boolean isCallerIdEnabled = !mDpm.getCrossProfileCallerIdDisabled(currentHandle);
        final boolean isContactsSearchPolicyEnabled =
                !mDpm.getCrossProfileContactsSearchDisabled(currentHandle);
        final boolean isBluetoothContactSharingEnabled =
                !mDpm.getBluetoothContactSharingDisabled(currentHandle);
        final boolean isContactRemoteSearchUserEnabled = isContactRemoteSearchUserSettingEnabled();

        final String directory = uri.getQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY);

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "isCallerIdEnabled: " + isCallerIdEnabled);
            Log.v(TAG, "isContactsSearchPolicyEnabled: " + isContactsSearchPolicyEnabled);
            Log.v(TAG, "isBluetoothContactSharingEnabled: " + isBluetoothContactSharingEnabled);
            Log.v(TAG, "isContactRemoteSearchUserEnabled: " + isContactRemoteSearchUserEnabled);
        }

        // If it is a remote directory, it is allowed only when
        // (i) The uri supports directory
        // (ii) User enables it in settings
        if (directory != null) {
            final long directoryId = Long.parseLong(directory);
            if (Directory.isRemoteDirectoryId(directoryId)
                    && !(isCrossProfileDirectorySupported(uri)
                    && isContactRemoteSearchUserEnabled)) {
                return false;
            }
        }

        // If either guard policy allows access, return true.
        return (isCallerIdGuarded(uriCode) && isCallerIdEnabled)
                || (isContactsSearchGuarded(uriCode) && isContactsSearchPolicyEnabled)
                || (isBluetoothContactSharing(uriCode) && isBluetoothContactSharingEnabled);
    }

    private boolean isUriWhitelisted(int uriCode) {
        switch (uriCode) {
            case ContactsProvider2.PROFILE_AS_VCARD:
            case ContactsProvider2.CONTACTS_AS_VCARD:
            case ContactsProvider2.CONTACTS_AS_MULTI_VCARD:
                return true;
            default:
                return false;
        }
    }

    /**
     * Check if uri is a cross profile query with directory param supported.
     *
     * @param uri Uri that we want to check.
     * @return True if it is an enterprise uri.
     */
    @VisibleForTesting
    protected boolean isCrossProfileDirectorySupported(@NonNull Uri uri) {
        final int uriCode = sUriMatcher.match(uri);
        return isDirectorySupported(uriCode);
    }

    public boolean isValidEnterpriseUri(@NonNull Uri uri) {
        final int uriCode = sUriMatcher.match(uri);
        return isValidEnterpriseUri(uriCode);
    }

    private static boolean isDirectorySupported(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.PHONE_LOOKUP:
            case ContactsProvider2.EMAILS_LOOKUP:
            case ContactsProvider2.CONTACTS_FILTER:
            case ContactsProvider2.PHONES_FILTER:
            case ContactsProvider2.CALLABLES_FILTER:
            case ContactsProvider2.EMAILS_FILTER:
            case ContactsProvider2.DIRECTORY_FILE_ENTERPRISE:
                return true;
            default:
                return false;
        }
    }

    private static boolean isValidEnterpriseUri(int uriCode) {
        switch (uriCode) {
            case ContactsProvider2.PHONE_LOOKUP_ENTERPRISE:
            case ContactsProvider2.EMAILS_LOOKUP_ENTERPRISE:
            case ContactsProvider2.CONTACTS_FILTER_ENTERPRISE:
            case ContactsProvider2.PHONES_FILTER_ENTERPRISE:
            case ContactsProvider2.CALLABLES_FILTER_ENTERPRISE:
            case ContactsProvider2.EMAILS_FILTER_ENTERPRISE:
            case ContactsProvider2.DIRECTORIES_ENTERPRISE:
            case ContactsProvider2.DIRECTORIES_ID_ENTERPRISE:
            case ContactsProvider2.DIRECTORY_FILE_ENTERPRISE:
                return true;
            default:
                return false;
        }
    }

    private static boolean isCallerIdGuarded(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.DIRECTORIES:
            case ContactsProvider2.DIRECTORIES_ID:
            case ContactsProvider2.PHONE_LOOKUP:
            case ContactsProvider2.EMAILS_LOOKUP:
            case ContactsProvider2.CONTACTS_ID_PHOTO:
            case ContactsProvider2.CONTACTS_ID_DISPLAY_PHOTO:
            case ContactsProvider2.DIRECTORY_FILE_ENTERPRISE:
                return true;
            default:
                return false;
        }
    }

    private static boolean isContactsSearchGuarded(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.DIRECTORIES:
            case ContactsProvider2.DIRECTORIES_ID:
            case ContactsProvider2.CONTACTS_FILTER:
            case ContactsProvider2.CALLABLES_FILTER:
            case ContactsProvider2.PHONES_FILTER:
            case ContactsProvider2.EMAILS_FILTER:
            case ContactsProvider2.CONTACTS_ID_PHOTO:
            case ContactsProvider2.CONTACTS_ID_DISPLAY_PHOTO:
            case ContactsProvider2.DIRECTORY_FILE_ENTERPRISE:
                return true;
            default:
                return false;
        }
    }

    private static boolean isBluetoothContactSharing(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.PHONES:
            case ContactsProvider2.RAW_CONTACT_ENTITIES:
                return true;
            default:
                return false;
        }
    }

    protected boolean isContactRemoteSearchUserSettingEnabled() {
        return Settings.Secure.getInt(
                mContext.getContentResolver(),
                MANAGED_PROFILE_CONTACT_REMOTE_SEARCH, 0) == 1;
    }
}
