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

import com.android.vcard.VCardComposer;
import com.android.vcard.VCardComposer.OneEntryHandler;
import com.android.vcard.VCardConfig;

import android.content.ContentResolver;
import android.content.Context;
import android.text.TextUtils;
import android.util.Log;

/**
 * Tests (or integration tests) verifying if vCard library works well with {@link ContentResolver}.
 *
 * Unit tests for vCard itself should be availabel in vCard library.
 */
public class VCardTest extends BaseContactsProvider2Test {
    private static final String TAG = "VCardTest";
    private static final boolean DEBUG = false;

    private class CustomHandler implements OneEntryHandler {
        private String mVCard;

        @Override
        public boolean onInit(Context context) {
            return true;
        }

        @Override
        public boolean onEntryCreated(String vcard) {
            if (DEBUG) Log.d(TAG, "vcard:" + vcard);
            assertTrue(TextUtils.isEmpty(mVCard));
            mVCard = vcard;
            return true;
        }

        @Override
        public void onTerminate() {
        }

        public String getVCard() {
            return mVCard;
        }
    }

    /**
     * Confirms the app fetches a stored contact from resolver and output the name as part of
     * a vCard string.
     */
    public void testCompose() {
        createRawContactWithName("John", "Doe");
        final VCardComposer composer = new VCardComposer(
                getContext(), mResolver, VCardConfig.VCARD_TYPE_DEFAULT, null, true);
        CustomHandler handler = new CustomHandler();
        composer.addHandler(handler);
        assertTrue(composer.init());
        int total = composer.getCount();
        assertEquals(1, total);
        assertFalse(composer.isAfterLast());
        assertTrue(composer.createOneEntry());
        assertTrue(composer.isAfterLast());
        String vcard = handler.getVCard();
        assertNotNull(vcard);

        // Check vCard very roughly.
        assertTrue(vcard.contains("John"));
        assertTrue(vcard.contains("Doe"));
    }
}
