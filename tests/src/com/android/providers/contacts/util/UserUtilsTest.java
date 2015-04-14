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

import com.android.providers.contacts.ContactsActor;
import com.android.providers.contacts.ContactsActor.MockUserManager;
import com.android.providers.contacts.SynchronousContactsProvider2;

import android.content.ContentProvider;
import android.content.Context;
import android.os.UserManager;
import android.provider.ContactsContract;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;

import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;

@SmallTest
public class UserUtilsTest extends AndroidTestCase {

    protected ContactsActor mActor;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mActor = new ContactsActor(getContext(), PACKAGE_GREY,
                SynchronousContactsProvider2.class,
                ContactsContract.AUTHORITY);
    }

    public void testGetCorpUserId() {
        final Context c = mActor.getProviderContext();
        final MockUserManager um = mActor.mockUserManager;

        // No corp user.  Primary only.
        assertEquals(-1, UserUtils.getCorpUserId(c, false));

        // Primary + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(MockUserManager.CORP_USER.id, UserUtils.getCorpUserId(c, false));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c, false));

        // Primary + secondary + corp
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER,
                MockUserManager.CORP_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(MockUserManager.CORP_USER.id, UserUtils.getCorpUserId(c, false));

        um.myUser = MockUserManager.CORP_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c, false));

        um.myUser = MockUserManager.SECONDARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c, false));

        // Primary + secondary
        um.setUsers(MockUserManager.PRIMARY_USER, MockUserManager.SECONDARY_USER);

        um.myUser = MockUserManager.PRIMARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c, false));

        um.myUser = MockUserManager.SECONDARY_USER.id;
        assertEquals(-1, UserUtils.getCorpUserId(c, false));
    }
}
