/*
 * Copyright (C) 2017 The Android Open Source Project
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

import android.content.ContentValues;
import android.database.Cursor;
import android.provider.VoicemailContract;
import android.provider.VoicemailContract.Status;
import android.provider.VoicemailContract.Voicemails;
import android.test.suitebuilder.annotation.SmallTest;

import com.android.providers.contacts.testutil.TestUtil;

/**
 * Tests for {@link VoicemailCleanupTest}.
 */
@SmallTest
public class VoicemailCleanupTest extends BaseVoicemailProviderTest {
    private static final String TEST_PACKAGE_1 = "package1";
    private static final String TEST_PACKAGE_2 = "package2";
    private static final String TEST_PACKAGE_3 = "package3";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setUpForFullPermission();
    }

    /**
     * Test for {@link ContactsPackageMonitor#cleanupVoicemail}.
     */
    public void testCleanupVoicemail() {
        insertDataForPackage(TEST_PACKAGE_1);
        insertDataForPackage(TEST_PACKAGE_2);

        mPackageManager.addPackage(123456, TEST_PACKAGE_2);

        checkDataExistsForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);

        ContactsPackageMonitor.cleanupVoicemail(getMockContext(), TEST_PACKAGE_1);

        checkDataDoesNotExistForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);

        // Call for TEST_PACKAGE_2, which is still installed.
        ContactsPackageMonitor.cleanupVoicemail(getMockContext(), TEST_PACKAGE_2);

        checkDataDoesNotExistForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);

        // Uninstall the package and try again.
        mPackageManager.removePackage(123456);
        ContactsPackageMonitor.cleanupVoicemail(getMockContext(), TEST_PACKAGE_2);

        checkDataDoesNotExistForPackage(TEST_PACKAGE_1);
        checkDataDoesNotExistForPackage(TEST_PACKAGE_2);
    }

    private void insertDataForPackage(String sourcePackage) {
        ContentValues values = new ContentValues();
        values.put(VoicemailContract.SOURCE_PACKAGE_FIELD, sourcePackage);
        mResolver.insert(Voicemails.buildSourceUri(sourcePackage), values);
        mResolver.insert(Status.buildSourceUri(sourcePackage), values);
    }

    private static void assertBigger(int smaller, int actual) {
        if (smaller >= actual) {
            fail("Expected to be bigger than " + smaller + ", but was " + actual);
        }
    }

    private void checkDataExistsForPackage(String sourcePackage) {
        Cursor cursor = mResolver.query(
                Voicemails.buildSourceUri(sourcePackage), null, null, null, null);
        assertBigger(0, cursor.getCount());
        cursor = mResolver.query(
                Status.buildSourceUri(sourcePackage), null, null, null, null);
        assertBigger(0, cursor.getCount());
    }

    private void checkDataDoesNotExistForPackage(String sourcePackage) {
        Cursor cursor = mResolver.query(
                Voicemails.buildSourceUri(sourcePackage), null,
                "(ifnull(" + Voicemails.DELETED + ",0)==0)", null, null);
        assertEquals(0, cursor.getCount());
        cursor = mResolver.query(
                Status.buildSourceUri(sourcePackage), null, null, null, null);
        assertEquals(0, cursor.getCount());
    }

    public void testRemoveStalePackagesAtStartUp() {
        insertDataForPackage(TEST_PACKAGE_1);
        insertDataForPackage(TEST_PACKAGE_2);
        insertDataForPackage(TEST_PACKAGE_3);

        mPackageManager.addPackage(10001, TEST_PACKAGE_1);
        mPackageManager.addPackage(10002, TEST_PACKAGE_2);

        checkDataExistsForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);
        checkDataExistsForPackage(TEST_PACKAGE_3);

        final VoicemailContentProvider provider =
                (VoicemailContentProvider) mActor.provider;

        // In unit tests, BG tasks are synchronous.
        provider.scheduleScanStalePackages();

        checkDataExistsForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);
        checkDataDoesNotExistForPackage(TEST_PACKAGE_3);

        mPackageManager.removePackage(10001);

        provider.scheduleScanStalePackages();

        checkDataDoesNotExistForPackage(TEST_PACKAGE_1);
        checkDataExistsForPackage(TEST_PACKAGE_2);
        checkDataDoesNotExistForPackage(TEST_PACKAGE_3);
    }
}
