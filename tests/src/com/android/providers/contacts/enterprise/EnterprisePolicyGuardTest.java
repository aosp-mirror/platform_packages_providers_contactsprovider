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
import android.content.Context;
import android.content.pm.UserInfo;
import android.net.Uri;
import android.os.UserHandle;
import android.os.UserManager;
import android.provider.ContactsContract.Directory;
import android.test.AndroidTestCase;
import android.test.mock.MockContext;
import android.test.suitebuilder.annotation.SmallTest;

import java.util.Arrays;
import java.util.List;

import org.mockito.Matchers;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


/**
 * Unit tests for {@link EnterprisePolicyGuard}.
 */
@SmallTest
public class EnterprisePolicyGuardTest extends AndroidTestCase {
    private static final String SYSTEM_PROPERTY_DEXMAKER_DEXCACHE = "dexmaker.dexcache";

    private static final int CONTACT_ID = 10;
    private static final String CONTACT_NAME = "david";
    private static final String CONTACT_EMAIL = "david.green@android.com";
    private static final String CONTACT_PHONE = "+1234567890";
    private static final long DIRECTORY_ID = Directory.ENTERPRISE_DEFAULT;

    private static final Uri URI_CONTACTS_ID_PHOTO_CORP =
            Uri.parse("content://com.android.contacts/contacts_corp/" + CONTACT_ID + "/photo");
    private static final Uri URI_CONTACTS_ID_DISPLAY_PHOTO_CORP = Uri
            .parse("content://com.android.contacts/contacts_corp/" + CONTACT_ID + "/display_photo");
    private static final Uri URI_CONTACTS_FILTER_ENTERPRISE =
            Uri.parse("content://com.android.contacts/contacts/filter_enterprise/" + CONTACT_NAME);
    private static final Uri URI_PHONES_FILTER_ENTERPRISE = Uri
            .parse("content://com.android.contacts/data/phones/filter_enterprise/" + CONTACT_NAME);
    private static final Uri URI_CALLABLES_FILTER_ENTERPRISE = Uri.parse(
            "content://com.android.contacts/data/callables/filter_enterprise/" + CONTACT_NAME);
    private static final Uri URI_EMAILS_FILTER_ENTERPRISE = Uri
            .parse("content://com.android.contacts/data/emails/filter_enterprise/" + CONTACT_NAME);
    private static final Uri URI_EMAILS_LOOKUP_ENTERPRISE =
            Uri.parse("content://com.android.contacts/data/emails/lookup_enterprise/"
                    + Uri.encode(CONTACT_EMAIL));
    private static final Uri URI_PHONE_LOOKUP_ENTERPRISE = Uri.parse(
            "content://com.android.contacts/phone_lookup_enterprise/" + Uri.encode(CONTACT_PHONE));
    private static final Uri URI_DIRECTORIES_ENTERPRISE =
            Uri.parse("content://com.android.contacts/directories_enterprise");
    private static final Uri URI_DIRECTORIES_ID_ENTERPRISE =
            Uri.parse("content://com.android.contacts/directories_enterprise/" + DIRECTORY_ID);

    private static final Uri URI_OTHER =
            Uri.parse("content://com.android.contacts/contacts/" + CONTACT_ID);

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
        EnterprisePolicyGuard guard = new EnterprisePolicyGuard(getContext());
        checkDirectorySupport(guard, URI_PHONE_LOOKUP_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_EMAILS_LOOKUP_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_CONTACTS_FILTER_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_PHONES_FILTER_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_CALLABLES_FILTER_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_EMAILS_FILTER_ENTERPRISE, true);
        checkDirectorySupport(guard, URI_DIRECTORIES_ENTERPRISE, false);
        checkDirectorySupport(guard, URI_DIRECTORIES_ID_ENTERPRISE, false);
        checkDirectorySupport(guard, URI_CONTACTS_ID_PHOTO_CORP, false);
        checkDirectorySupport(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, false);
        checkDirectorySupport(guard, URI_OTHER, false);
    }


    public void testCrossProfile() {
        Context context;
        EnterprisePolicyGuard guard;

        // No corp user
        context = getMockContext(/* managedProfileExists= */ false, true, true);
        guard = new EnterprisePolicyGuard(context);
        checkCrossProfile(guard, URI_PHONE_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CONTACTS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_PHONES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CALLABLES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ENTERPRISE, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ID_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO_CORP, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, false);
        checkCrossProfile(guard, URI_OTHER, false);

        // All enabled. Corp user enabled
        context = getMockContext(/* managedProfileExists= */ true, true, true);
        guard = new EnterprisePolicyGuard(context);
        checkCrossProfile(guard, URI_PHONE_LOOKUP_ENTERPRISE, true);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CONTACTS_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_PHONES_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CALLABLES_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_EMAILS_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ENTERPRISE, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // Only ContactsSearch is disabled
        context = getMockContext(true, true, /* isContactsSearchEnabled= */ false);
        guard = new EnterprisePolicyGuard(context);
        checkCrossProfile(guard, URI_PHONE_LOOKUP_ENTERPRISE, true);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CONTACTS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_PHONES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CALLABLES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ENTERPRISE, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // Only CallerId is disabled
        context = getMockContext(true, /* isCallerIdEnabled= */ false, true);
        guard = new EnterprisePolicyGuard(context);
        checkCrossProfile(guard, URI_PHONE_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CONTACTS_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_PHONES_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CALLABLES_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_EMAILS_FILTER_ENTERPRISE, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ENTERPRISE, true);
        checkCrossProfile(guard, URI_DIRECTORIES_ID_ENTERPRISE, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, true);
        checkCrossProfile(guard, URI_OTHER, false);

        // CallerId and ContactsSearch are disabled
        context = getMockContext(true, /* isCallerIdEnabled= */ false,
                /* isContactsSearchEnabled= */ false);
        guard = new EnterprisePolicyGuard(context);
        checkCrossProfile(guard, URI_PHONE_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_LOOKUP_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CONTACTS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_PHONES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CALLABLES_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_EMAILS_FILTER_ENTERPRISE, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ENTERPRISE, false);
        checkCrossProfile(guard, URI_DIRECTORIES_ID_ENTERPRISE, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_PHOTO_CORP, false);
        checkCrossProfile(guard, URI_CONTACTS_ID_DISPLAY_PHOTO_CORP, false);
        checkCrossProfile(guard, URI_OTHER, false);
    }

    private static void checkDirectorySupport(EnterprisePolicyGuard guard, Uri uri,
            boolean expected) {
        if (expected) {
            assertTrue("Expected true but got false for uri: " + uri,
                    guard.isEnterpriseUriWithDirectorySupport(uri));
        } else {
            assertFalse("Expected false but got true for uri: " + uri,
                    guard.isEnterpriseUriWithDirectorySupport(uri));

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


    private Context getMockContext(boolean managedProfileExists, boolean isCallerIdEnabled,
            boolean isContactsSearchEnabled) {
        DevicePolicyManager mockDpm = mock(DevicePolicyManager.class);
        when(mockDpm.getCrossProfileCallerIdDisabled(Matchers.<UserHandle>any()))
                .thenReturn(!isCallerIdEnabled);
        when(mockDpm.getCrossProfileContactsSearchDisabled(Matchers.<UserHandle>any()))
                .thenReturn(!isContactsSearchEnabled);

        List<UserInfo> userInfos =
                managedProfileExists ? MANAGED_USERINFO_LIST : NORMAL_USERINFO_LIST;
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

}
