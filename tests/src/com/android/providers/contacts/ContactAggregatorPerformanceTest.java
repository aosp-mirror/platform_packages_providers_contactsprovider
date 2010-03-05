/*
 * Copyright (C) 2009 The Android Open Source Project
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

import android.content.Context;
import android.content.res.Resources;
import android.os.Debug;
import android.provider.ContactsContract;
import android.test.AndroidTestCase;
import android.test.IsolatedContext;
import android.test.RenamingDelegatingContext;
import android.test.mock.MockContentResolver;
import android.test.mock.MockContext;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

/**
 * Performance test for {@link ContactAggregator}.
 *
 * Run the test like this:
 * <code>
 * adb push <large contacts2.db> \
 *         data/data/com.android.providers.contacts/databases/perf.contacts2.db
 * adb shell am instrument \
 *         -e class com.android.providers.contacts.ContactAggregatorPerformanceTest \
 *         -w com.android.providers.contacts.tests/android.test.InstrumentationTestRunner
 * </code>
 */
@LargeTest
public class ContactAggregatorPerformanceTest extends AndroidTestCase {

    private static final String TAG = "ContactAggregatorPerformanceTest";
    private static final boolean TRACE = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        SynchronousContactsProvider2.resetOpenHelper();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        SynchronousContactsProvider2.resetOpenHelper();
    }

    public void testPerformance() {
        final Context targetContext = getContext();
        MockContentResolver resolver = new MockContentResolver();
        MockContext context = new MockContext() {
            @Override
            public Resources getResources() {
                return targetContext.getResources();
            }

            @Override
            public String getPackageName() {
                return "no.package";
            }
        };
        RenamingDelegatingContext targetContextWrapper =
                new RenamingDelegatingContext(context, targetContext, "perf.");
        targetContextWrapper.makeExistingFilesAndDbsAccessible();
        IsolatedContext providerContext = new IsolatedContext(resolver, targetContextWrapper);
        SynchronousContactsProvider2 provider = new SynchronousContactsProvider2();
        provider.setDataWipeEnabled(false);
        provider.attachInfo(providerContext, null);
        resolver.addProvider(ContactsContract.AUTHORITY, provider);

        long rawContactCount = provider.getRawContactCount();
        if (rawContactCount == 0) {
            Log.w(TAG, "The test has not been set up. Use this command to copy a contact db"
                    + " to the device:\nadb push <large contacts2.db> "
                    + "data/data/com.android.providers.contacts/databases/perf.contacts2.db");
            return;
        }

        provider.prepareForFullAggregation(500);
        rawContactCount = provider.getRawContactCount();
        long start = System.currentTimeMillis();
        if (TRACE) {
            Debug.startMethodTracing("aggregation");
        }

        // TODO
//        provider.aggregate();

        if (TRACE) {
            Debug.stopMethodTracing();
        }
        long end = System.currentTimeMillis();
        long contactCount = provider.getContactCount();

        Log.i(TAG, String.format("Aggregated contacts in %d ms.\n" +
                "Raw contacts: %d\n" +
                "Aggregated contacts: %d\n" +
                "Per raw contact: %.3f",
                end-start,
                rawContactCount,
                contactCount,
                ((double)(end-start)/rawContactCount)));

        provider.getDatabaseHelper().close();
    }
}

