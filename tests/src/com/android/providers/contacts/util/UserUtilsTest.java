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

import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;

import android.content.Context;
import android.os.UserHandle;
import android.provider.ContactsContract;
import android.test.suitebuilder.annotation.SmallTest;

import com.android.providers.contacts.ContactsActor;
import com.android.providers.contacts.ContactsActor.MockUserManager;
import com.android.providers.contacts.FixedAndroidTestCase;
import com.android.providers.contacts.SynchronousContactsProvider2;

@SmallTest
public class UserUtilsTest extends FixedAndroidTestCase {

    protected ContactsActor mActor;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mActor = new ContactsActor(getContext(), PACKAGE_GREY,
                SynchronousContactsProvider2.class,
                ContactsContract.AUTHORITY);
    }

    public void testGetCorpUser() {
        final Context c = mActor.getProviderContext();
        final MockUserManager um = mActor.mockUserManager;

        // No corp user.  Primary only.
        assertEquals(-1, UserUtils.getCorpUserId(c));

        // Primary + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(MockUserManager.CORP_USER.id, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        // Primary + secondary + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER,
                MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(MockUserManager.CORP_USER.id, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.SECONDARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        // Primary + secondary
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.SECONDARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        // Primary + clone + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CLONE_PROFILE_USER,
                MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(MockUserManager.CORP_USER.id, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.CLONE_PROFILE_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c));
    }

    public void testShouldUseParentsContacts() {
        final Context c = mActor.getProviderContext();
        final MockUserManager um = mActor.mockUserManager;

        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER,
                MockUserManager.CLONE_PROFILE_USER, MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertFalse(UserUtils.shouldUseParentsContacts(c));
        assertFalse(UserUtils.shouldUseParentsContacts(c,
                MockUserManager.PRIMARY_USER.getUserHandle()));

        um.myUser = MockUserManager.SECONDARY_USER.id;
        assertFalse(UserUtils.shouldUseParentsContacts(c));
        assertFalse(UserUtils.shouldUseParentsContacts(c,
                MockUserManager.SECONDARY_USER.getUserHandle()));

        um.myUser = MockUserManager.CORP_USER.id;
        assertFalse(UserUtils.shouldUseParentsContacts(c));
        assertFalse(UserUtils.shouldUseParentsContacts(c,
                MockUserManager.CORP_USER.getUserHandle()));

        um.myUser = MockUserManager.CLONE_PROFILE_USER.id;
        assertTrue(UserUtils.shouldUseParentsContacts(c));
        assertTrue(UserUtils.shouldUseParentsContacts(c,
                MockUserManager.CLONE_PROFILE_USER.getUserHandle()));

    }

    public void testIsParentUser() {
        final Context c = mActor.getProviderContext();
        final MockUserManager um = mActor.mockUserManager;
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER,
                MockUserManager.CLONE_PROFILE_USER, MockUserManager.CORP_USER);

        UserHandle primaryProfileUserHandle = MockUserManager.PRIMARY_USER.getUserHandle();
        UserHandle cloneUserHandle = MockUserManager.CLONE_PROFILE_USER.getUserHandle();
        UserHandle corpUserHandle = MockUserManager.CORP_USER.getUserHandle();

        assertTrue(UserUtils.isParentUser(c, primaryProfileUserHandle, cloneUserHandle));
        assertTrue(UserUtils.isParentUser(c, primaryProfileUserHandle, corpUserHandle));
        assertFalse(UserUtils.isParentUser(c, primaryProfileUserHandle, primaryProfileUserHandle));
        assertFalse(UserUtils.isParentUser(c, cloneUserHandle, cloneUserHandle));
        assertFalse(UserUtils.isParentUser(c, cloneUserHandle, primaryProfileUserHandle));
        assertFalse(UserUtils.isParentUser(c, corpUserHandle, primaryProfileUserHandle));
    }

    public void testGetProfileParent() {
        final Context c = mActor.getProviderContext();
        final MockUserManager um = mActor.mockUserManager;

        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER,
                MockUserManager.CLONE_PROFILE_USER, MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertNull(UserUtils.getProfileParentUser(c));

        um.myUser = MockUserManager.CLONE_PROFILE_USER.id;
        assertEquals(MockUserManager.PRIMARY_USER, UserUtils.getProfileParentUser(c));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(MockUserManager.PRIMARY_USER, UserUtils.getProfileParentUser(c));
    }
}
