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
 * limitations under the License.
 */

package com.android.providers.contacts;

import com.android.providers.contacts.ContactsDatabaseHelper.Tables;
import com.android.providers.contacts.tests.R;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.provider.ContactsContract;
import android.provider.ContactsContract.PhotoFiles;
import android.test.mock.MockContentResolver;
import android.test.suitebuilder.annotation.LargeTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.android.providers.contacts.ContactsActor.PACKAGE_GREY;

/**
 * Tests for {@link PhotoStore}.
 */
@LargeTest
public class PhotoStoreTest extends PhotoLoadingTestCase {

    private ContactsActor mActor;
    private SynchronousContactsProvider2 mProvider;
    private SQLiteDatabase mDb;

    // The object under test.
    private PhotoStore mPhotoStore;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mActor = new ContactsActor(getContext(), PACKAGE_GREY, SynchronousContactsProvider2.class,
                ContactsContract.AUTHORITY);
        mProvider = ((SynchronousContactsProvider2) mActor.provider);
        mPhotoStore = mProvider.getPhotoStore();
        mProvider.wipeData();
        mDb = mProvider.getDatabaseHelper(getContext()).getReadableDatabase();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        mPhotoStore.clear();
    }

    public void testStoreThumbnailPhoto() throws IOException {
        byte[] photo = loadPhotoFromResource(R.drawable.earth_small, PhotoSize.ORIGINAL);

        // Since the photo is already thumbnail-sized, no file will be stored.
        assertEquals(0, mPhotoStore.insert(new PhotoProcessor(photo, 256, 96)));
    }

    public void testStoreMediumPhoto() throws IOException {
        runStorageTestForResource(R.drawable.earth_normal);
    }

    public void testStoreLargePhoto() throws IOException {
        runStorageTestForResource(R.drawable.earth_large);
    }

    public void testStoreHugePhoto() throws IOException {
        runStorageTestForResource(R.drawable.earth_huge);
    }

    /**
     * Runs the following steps:
     * - Loads the given photo resource.
     * - Inserts it into the photo store.
     * - Checks that the photo has a photo file ID.
     * - Loads the expected display photo for the resource.
     * - Gets the photo entry from the photo store.
     * - Loads the photo entry's file content from disk.
     * - Compares the expected photo content to the disk content.
     * - Queries the contacts provider for the photo file entry, checks for its
     *   existence, and matches it up against the expected metadata.
     * - Checks that the total storage taken up by the photo store is equal to
     *   the size of the photo.
     * @param resourceId The resource ID of the photo file to test.
     */
    public void runStorageTestForResource(int resourceId) throws IOException {
        byte[] photo = loadPhotoFromResource(resourceId, PhotoSize.ORIGINAL);
        long photoFileId = mPhotoStore.insert(new PhotoProcessor(photo, 256, 96));
        assertTrue(photoFileId != 0);

        byte[] expectedStoredVersion = loadPhotoFromResource(resourceId, PhotoSize.DISPLAY_PHOTO);
        File storedFile = new File(mPhotoStore.get(photoFileId).path);
        assertTrue(storedFile.exists());
        byte[] storedVersion = readInputStreamFully(new FileInputStream(storedFile));
        assertEquals(Hex.encodeHex(expectedStoredVersion, false),
                Hex.encodeHex(storedVersion, false));

        Cursor c = mDb.query(Tables.PHOTO_FILES,
                new String[]{PhotoFiles.HEIGHT, PhotoFiles.WIDTH, PhotoFiles.FILESIZE},
                PhotoFiles._ID + "=?", new String[]{String.valueOf(photoFileId)}, null, null, null);
        try {
            assertEquals(1, c.getCount());
            c.moveToFirst();
            assertEquals(256, c.getInt(0));
            assertEquals(256, c.getInt(1));
            assertEquals(expectedStoredVersion.length, c.getInt(2));
        } finally {
            c.close();
        }

        assertEquals(expectedStoredVersion.length, mPhotoStore.getTotalSize());
    }

    public void testRemoveEntry() throws IOException {
        byte[] photo = loadPhotoFromResource(R.drawable.earth_normal, PhotoSize.ORIGINAL);
        long photoFileId = mPhotoStore.insert(new PhotoProcessor(photo, 256, 96));
        PhotoStore.Entry entry = mPhotoStore.get(photoFileId);
        assertTrue(new File(entry.path).exists());

        mPhotoStore.remove(photoFileId);

        // Check that the file has been deleted.
        assertFalse(new File(entry.path).exists());

        // Check that the database record has also been removed.
        Cursor c = mDb.query(Tables.PHOTO_FILES, new String[]{PhotoFiles._ID},
                PhotoFiles._ID + "=?", new String[]{String.valueOf(photoFileId)}, null, null, null);
        try {
            assertEquals(0, c.getCount());
        } finally {
            c.close();
        }
    }

    public void testCleanup() throws IOException {
        // Load some photos into the store.
        Set<Long> photoFileIds = new HashSet<Long>();
        Map<Integer, Long> resourceIdToPhotoMap = new HashMap<Integer, Long>();
        int[] resourceIds = new int[] {
                R.drawable.earth_normal, R.drawable.earth_large, R.drawable.earth_huge
        };
        for (int resourceId : resourceIds) {
            long photoFileId = mPhotoStore.insert(
                    new PhotoProcessor(loadPhotoFromResource(resourceId, PhotoSize.ORIGINAL),
                            256, 96));
            resourceIdToPhotoMap.put(resourceId, photoFileId);
            photoFileIds.add(photoFileId);
        }
        assertFalse(photoFileIds.contains(0L));
        assertEquals(3, photoFileIds.size());

        // Run cleanup with the indication that only the large and huge photos are in use, along
        // with a bogus photo file ID that isn't in the photo store.
        long bogusPhotoFileId = 42;
        Set<Long> photoFileIdsInUse = new HashSet<Long>();
        photoFileIdsInUse.add(resourceIdToPhotoMap.get(R.drawable.earth_large));
        photoFileIdsInUse.add(resourceIdToPhotoMap.get(R.drawable.earth_huge));
        photoFileIdsInUse.add(bogusPhotoFileId);

        Set<Long> photoIdsToCleanup = mPhotoStore.cleanup(photoFileIdsInUse);

        // The set of photo IDs to clean up should consist of the bogus photo file ID.
        assertEquals(1, photoIdsToCleanup.size());
        assertTrue(photoIdsToCleanup.contains(bogusPhotoFileId));

        // The entry for the normal-sized photo should have been cleaned up, since it isn't being
        // used.
        long normalPhotoId = resourceIdToPhotoMap.get(R.drawable.earth_normal);
        assertNull(mPhotoStore.get(normalPhotoId));

        // Check that the database record has also been removed.
        Cursor c = mDb.query(Tables.PHOTO_FILES, new String[]{PhotoFiles._ID},
                PhotoFiles._ID + "=?", new String[]{String.valueOf(normalPhotoId)},
                null, null, null);
        try {
            assertEquals(0, c.getCount());
        } finally {
            c.close();
        }
    }
}
