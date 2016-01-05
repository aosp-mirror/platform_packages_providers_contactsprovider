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
import android.content.UriMatcher;
import android.net.Uri;
import android.os.UserHandle;
import android.provider.ContactsContract;
import android.util.Log;

import com.android.providers.contacts.ContactsProvider2;
import com.android.providers.contacts.ProfileAwareUriMatcher;
import com.android.providers.contacts.util.UserUtils;

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
        final UserHandle workUserHandle = UserUtils.getCorpUserHandle(mContext);
        if (uriCode == -1 || workUserHandle == null) {
            return false;
        }

        final boolean isCallerIdEnabled = !mDpm.getCrossProfileCallerIdDisabled(workUserHandle);
        final boolean isContactsSearchEnabled =
                !mDpm.getCrossProfileContactsSearchDisabled(workUserHandle);

        if (VERBOSE_LOGGING) {
            Log.v(TAG, "isCallerIdEnabled: " + isCallerIdEnabled);
            Log.v(TAG, "isContactsSearchEnabled: " + isContactsSearchEnabled);
        }

        // If either guard policy allows access, return true.
        return (isCallerIdGuarded(uriCode) && isCallerIdEnabled)
                || (isContactsSearchGuarded(uriCode) && isContactsSearchEnabled);
    }

    /**
     * Check if uri is an enterprise uri with directory param supported.
     *
     * @param uri Uri that we want to check.
     * @return True if it is an enterprise uri.
     */
    public boolean isEnterpriseUriWithDirectorySupport(@NonNull Uri uri) {
        final int uriCode = sUriMatcher.match(uri);
        return isDirectorySupported(uriCode);
    }


    /** Private methods **/
    private static boolean isDirectorySupported(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.PHONE_LOOKUP_ENTERPRISE:
            case ContactsProvider2.EMAILS_LOOKUP_ENTERPRISE:
            case ContactsProvider2.CONTACTS_FILTER_ENTERPRISE:
            case ContactsProvider2.PHONES_FILTER_ENTERPRISE:
            case ContactsProvider2.CALLABLES_FILTER_ENTERPRISE:
            case ContactsProvider2.EMAILS_FILTER_ENTERPRISE:
                return true;
            default:
                return false;
        }
    }

    private static boolean isCallerIdGuarded(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.DIRECTORIES_ENTERPRISE:
            case ContactsProvider2.DIRECTORIES_ID_ENTERPRISE:
            case ContactsProvider2.PHONE_LOOKUP_ENTERPRISE:
            case ContactsProvider2.EMAILS_LOOKUP_ENTERPRISE:
            case ContactsProvider2.CONTACTS_ID_PHOTO_CORP:
            case ContactsProvider2.CONTACTS_ID_DISPLAY_PHOTO_CORP:
                return true;
            default:
                return false;
        }
    }

    private static boolean isContactsSearchGuarded(int uriCode) {
        switch(uriCode) {
            case ContactsProvider2.DIRECTORIES_ENTERPRISE:
            case ContactsProvider2.DIRECTORIES_ID_ENTERPRISE:
            case ContactsProvider2.CONTACTS_FILTER_ENTERPRISE:
            case ContactsProvider2.CALLABLES_FILTER_ENTERPRISE:
            case ContactsProvider2.PHONES_FILTER_ENTERPRISE:
            case ContactsProvider2.EMAILS_FILTER_ENTERPRISE:
            case ContactsProvider2.CONTACTS_ID_PHOTO_CORP:
            case ContactsProvider2.CONTACTS_ID_DISPLAY_PHOTO_CORP:
                return true;
            default:
                return false;
        }
    }

}
