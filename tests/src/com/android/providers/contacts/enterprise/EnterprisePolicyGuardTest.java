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

import android.app.admin.DevicePolicyManager;
import android.content.ContentResolver;
import android.content.Context;
import android.content.pm.UserInfo;
import android.net.Uri;
import android.os.UserHandle;
import android.os.UserManager;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Directory;
import android.test.mock.MockContext;
import android.test.suitebuilder.annotation.SmallTest;

import org.mockito.Matchers;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.android.providers.contacts.FixedAndroidTestCase;


/**
 * Unit tests for {@link EnterprisePolicyGuard}.
 */
@SmallTest
public class EnterprisePolicyGuardTest extends FixedAndroidTestCase {
    private static final String SYSTEM_PROPERTY_DEXMAKER_DEXCACHE = "dexmaker.dexcache";

    private static final int CONTACT_ID = 10;
    private static final String CONTACT_NAME = "david";
    private static final String CONTACT_EMAIL = "david.green@android.com";
    private static final String CONTACT_PHONE = "+1234567890";
    private static final long DIRECTORY_ID = Directory.ENTERPRISE_DEFAULT;

    private static final Uri URI_CONTACTS_ID_PHOTO =
            Uri.parse("content://com.android.contacts/contacts/" + CONTACT_ID + "/photo");
    private static final Uri URI_CONTACTS_ID_DISPLAY_PHOTO = Uri
            .parse("content://com.android.contacts/contacts/" + CONTACT_ID + "/display_photo");
    private static final Uri URI_CONTACTS_FILTER =
            Uri.parse("content://com.android.contacts/contacts/filter/" + CONTACT_NAME);
    private static final Uri URI_PHONES_FILTER = Uri
            .parse("content://com.android.contacts/data/phones/filter/" + CONTACT_NAME);
    private static final Uri URI_CALLABLES_FILTER = Uri.parse(
            "content://com.android.contacts/data/callables/filter/" + CONTACT_NAME);
    private static final Uri URI_EMAILS_FILTER = Uri
            .parse("content://com.android.contacts/data/emails/filter/" + CONTACT_NAME);
    private static final Uri URI_EMAILS_LOOKUP =
            Uri.parse("content://com.android.contacts/data/emails/lookup/"
                    + Uri.encode(CONTACT_EMAIL));
    private static final Uri URI_PHONE_LOOKUP = Uri.parse(
            "content://com.android.contacts/phone_lookup/" + Uri.encode(CONTACT_PHONE));
    private static final Uri URI_DIRECTORIES =
            Uri.parse("content://com.android.contacts/directories");
    private static final Uri URI_DIRECTORIES_ID =
            Uri.parse("content://com.android.contacts/directories/" + DIRECTORY_ID);
    private static final Uri URI_DIRECTORY_FILE =
            Uri.parse("content://com.android.contacts/directory_file_enterprise/content%3A%2F%2F"
                    + "com.google.contacts.gal.provider%2Fphoto%2F?directory=1000000002");

    private static final Uri URI_OTHER =
            Uri.parse("content://com.android.contacts/contacts/" + CONTACT_ID);

    // Please notice that the directory id should be < ENTERPRISE_BASE because the id should be
    // substracted before passing to enterprise side.
    private static final int REMOTE_DIRECTORY_ID = 10;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        System.setProperty(SYSTEM_PROPERTY_DEXMAKER_DEXCACHE, getContext().getCacheDir().getPath());
        Thread.currentThread().setContextClassLoader(EnterprisePolicyGuard.class.getClassLoader());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        System.clearProperty(SYSTEM_PROPERTY_DEXMAKER_DEXCACHE);
    }

    public void testDirectorySupport() {
        EnterprisePolicyGuard guard = new EnterprisePolicyGuardTestable(getContext(), true);
        checkDirectorySupport(guard, URI_PHONE_LOOKUP, true);
        checkDirectorySupport(guard, URI_EMAILS_LOOKUP, true);
        checkDirectorySupport(guard, URI_CONTACTS_FILTER, true);
        checkDirectorySupport(guard, URI_PHONES_FILTER, true);
        checkDirectorySupport(guard, URI_CALLABLES_FILTER, true);
        checkDirectorySupport(guard, URI_EMAILS_FILTER, true);
        checkDirectorySupport(guard, URI_DIRECTORY_FILE, true);
        checkDirectorySupport(guard, URI_DIRECTORIES, false);
        checkDirectorySupport(guard, URI_DIRECTORIES_ID, false);
        checkDirectorySupport(guard, URI_CONTACTS_ID_PHOTO, false);
        checkDirectorySupport(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, false);
        checkDirectorySupport(guard, URI_OTHER, false);
    }


    public void testCrossProfile_userSettingOn() {
        Context context;
        EnterprisePolicyGuard guard;
        // All enabled.
        context = getMockContext(true, true);
        guard = new EnterprisePolicyGuardTestable(context, true);
        checkCrossProfile(guard, URI_PHONE_LOOKUP, true);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP, true);
        checkCrossProfile(guard, URI_CONTACTS_FILTER, true);
        checkCrossProfile(guard, URI_PHONES_FILTER, true);
        checkCrossProfile(guard, URI_CALLABLES_FILTER, true);
        checkCrossProfile(guard, URI_EMAILS_FILTER, true);
        checkCrossProfile(guard, URI_DIRECTORY_FILE, true);
        checkCrossProfile(guard, URI_DIRECTORIES, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // Only ContactsSearch is disabled
        context = getMockContext(true, /* isContactsSearchEnabled= */ false);
        guard = new EnterprisePolicyGuardTestable(context, true);
        checkCrossProfile(guard, URI_PHONE_LOOKUP, true);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP, true);
        checkCrossProfile(guard, URI_CONTACTS_FILTER, false);
        checkCrossProfile(guard, URI_PHONES_FILTER, false);
        checkCrossProfile(guard, URI_CALLABLES_FILTER, false);
        checkCrossProfile(guard, URI_EMAILS_FILTER, false);
        checkCrossProfile(guard, URI_DIRECTORY_FILE, true);
        checkCrossProfile(guard, URI_DIRECTORIES, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // Only CallerId is disabled
        context = getMockContext(/* isCallerIdEnabled= */ false, true);
        guard = new EnterprisePolicyGuardTestable(context, true);
        checkCrossProfile(guard, URI_PHONE_LOOKUP, false);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP, false);
        checkCrossProfile(guard, URI_CONTACTS_FILTER, true);
        checkCrossProfile(guard, URI_PHONES_FILTER, true);
        checkCrossProfile(guard, URI_CALLABLES_FILTER, true);
        checkCrossProfile(guard, URI_EMAILS_FILTER, true);
        checkCrossProfile(guard, URI_DIRECTORY_FILE, true);
        checkCrossProfile(guard, URI_DIRECTORIES, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // CallerId and ContactsSearch are disabled
        context = getMockContext(/* isCallerIdEnabled= */ false,
            /* isContactsSearchEnabled= */ false);
        guard = new EnterprisePolicyGuardTestable(context, true);
        checkCrossProfile(guard, URI_PHONE_LOOKUP, false);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP, false);
        checkCrossProfile(guard, URI_CONTACTS_FILTER, false);
        checkCrossProfile(guard, URI_PHONES_FILTER, false);
        checkCrossProfile(guard, URI_CALLABLES_FILTER, false);
        checkCrossProfile(guard, URI_EMAILS_FILTER, false);
        checkCrossProfile(guard, URI_DIRECTORY_FILE, false);
        checkCrossProfile(guard, URI_DIRECTORIES, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ID, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, false);
        checkCrossProfile(guard, URI_OTHER, false);
    }

    public void testCrossProfile_userSettingOff() {
        // All enabled.
        Context context = getMockContext(true, true);
        EnterprisePolicyGuard guard = new EnterprisePolicyGuardTestable(context, false);
        // All directory supported Uris with remote directory id should not allowed.
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_PHONE_LOOKUP), false);
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_EMAILS_LOOKUP), false);
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_CONTACTS_FILTER), false);
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_PHONES_FILTER), false);
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_CALLABLES_FILTER), false);
        checkCrossProfile(guard, appendRemoteDirectoryId(URI_EMAILS_FILTER), false);
        checkCrossProfile(guard, URI_DIRECTORY_FILE, false);

        // Always allow uri with no directory support.
        checkCrossProfile(guard, URI_DIRECTORIES, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // Always allow uri with no remote directory id.
        checkCrossProfile(guard, URI_PHONE_LOOKUP, true);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP, true);
        checkCrossProfile(guard, URI_CONTACTS_FILTER, true);
        checkCrossProfile(guard, URI_PHONES_FILTER, true);
        checkCrossProfile(guard, URI_CALLABLES_FILTER, true);
        checkCrossProfile(guard, URI_EMAILS_FILTER, true);
    }

    private static Uri appendRemoteDirectoryId(Uri uri) {
        return appendDirectoryId(uri, REMOTE_DIRECTORY_ID);
    }

    private static Uri appendDirectoryId(Uri uri, int directoryId) {
        return uri.buildUpon().appendQueryParameter(ContactsContract.DIRECTORY_PARAM_KEY,
                String.valueOf(directoryId)).build();
    }


    private static void checkDirectorySupport(EnterprisePolicyGuard guard, Uri uri,
            boolean expected) {
        if (expected) {
            assertTrue("Expected true but got false for uri: " + uri,
                    guard.isCrossProfileDirectorySupported(uri));
        } else {
            assertFalse("Expected false but got true for uri: " + uri,
                    guard.isCrossProfileDirectorySupported(uri));
        }
    }

    private static void checkCrossProfile(EnterprisePolicyGuard guard, Uri uri, boolean expected) {
        if (expected) {
            assertTrue("Expected true but got false for uri: " + uri,
                    guard.isCrossProfileAllowed(uri));
        } else {
            assertFalse("Expected false but got true for uri: " + uri,
                    guard.isCrossProfileAllowed(uri));
        }
    }

    private static final int CURRENT_USER_ID = 0;
    private static final int WORK_USER_ID = 10;
    private static final UserInfo CURRENT_USER_INFO =
            new UserInfo(CURRENT_USER_ID, "user0", CURRENT_USER_ID);
    private static final UserInfo WORK_USER_INFO =
            new UserInfo(WORK_USER_ID, "user10", UserInfo.FLAG_MANAGED_PROFILE);

    private static final List<UserInfo> NORMAL_USERINFO_LIST =
            Arrays.asList(new UserInfo[] {CURRENT_USER_INFO});

    private static final List<UserInfo> MANAGED_USERINFO_LIST =
            Arrays.asList(new UserInfo[] {CURRENT_USER_INFO, WORK_USER_INFO});


    private Context getMockContext(boolean isCallerIdEnabled, boolean isContactsSearchEnabled) {
        DevicePolicyManager mockDpm = mock(DevicePolicyManager.class);
        when(mockDpm.getCrossProfileCallerIdDisabled(Matchers.<UserHandle>any()))
                .thenReturn(!isCallerIdEnabled);
        when(mockDpm.getCrossProfileContactsSearchDisabled(Matchers.<UserHandle>any()))
                .thenReturn(!isContactsSearchEnabled);

        List<UserInfo> userInfos = MANAGED_USERINFO_LIST;
        UserManager mockUm = mock(UserManager.class);
        when(mockUm.getUserHandle()).thenReturn(CURRENT_USER_ID);
        when(mockUm.getUsers()).thenReturn(userInfos);
        when(mockUm.getProfiles(Matchers.anyInt())).thenReturn(userInfos);
        when(mockUm.getProfileParent(WORK_USER_ID)).thenReturn(CURRENT_USER_INFO);

        Context mockContext = new TestMockContext(getContext(), mockDpm, mockUm);

        return mockContext;
    }

    private static final class TestMockContext extends MockContext {
        private Context mRealContext;
        private DevicePolicyManager mDpm;
        private UserManager mUm;

        public TestMockContext(Context realContext, DevicePolicyManager dpm, UserManager um) {
            mRealContext = realContext;
            mDpm = dpm;
            mUm = um;
        }

        public Object getSystemService(String name) {
            if (Context.DEVICE_POLICY_SERVICE.equals(name)) {
                return mDpm;
            } else if (Context.USER_SERVICE.equals(name)) {
                return mUm;
            } else {
                return super.getSystemService(name);
            }
        }

        public String getSystemServiceName(Class<?> serviceClass) {
            return mRealContext.getSystemServiceName(serviceClass);
        }
    }

    private static class EnterprisePolicyGuardTestable extends EnterprisePolicyGuard {
        private boolean mIsContactRemoteSearchUserSettingEnabled;

        public EnterprisePolicyGuardTestable(Context context,
                boolean isContactRemoteSearchUserSettingEnabled) {
            super(context);
            mIsContactRemoteSearchUserSettingEnabled = isContactRemoteSearchUserSettingEnabled;
        }

        @Override
        protected boolean isContactRemoteSearchUserSettingEnabled() {
            return mIsContactRemoteSearchUserSettingEnabled;
        }
    }

}
