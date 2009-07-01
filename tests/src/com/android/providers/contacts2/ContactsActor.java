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

package com.android.providers.contacts2;

import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.content.res.Resources;
import android.os.Binder;
import android.provider.ContactsContract;
import android.test.IsolatedContext;
import android.test.RenamingDelegatingContext;
import android.test.mock.MockContentResolver;
import android.test.mock.MockContext;
import android.test.mock.MockPackageManager;
import android.util.Log;

import java.util.HashMap;

/**
 * Helper class that encapsulates an "actor" which is owned by a specific
 * package name. It correctly maintains a wrapped {@link Context} and an
 * attached {@link MockContentResolver}. Multiple actors can be used to test
 * security scenarios between multiple packages.
 */
public class ContactsActor {
    private static final String FILENAME_PREFIX = "test.";

    public static final String PACKAGE_GREY = "edu.example.grey";
    public static final String PACKAGE_RED = "net.example.red";
    public static final String PACKAGE_GREEN = "com.example.green";
    public static final String PACKAGE_BLUE = "org.example.blue";

    public Context context;
    public String packageName;
    public MockContentResolver resolver;
    public SynchronousContactsProvider2 provider;

    /**
     * Create an "actor" using the given parent {@link Context} and the specific
     * package name. Internally, all {@link Context} method calls are passed to
     * a new instance of {@link RestrictionMockContext}, which stubs out the
     * security infrastructure.
     */
    public ContactsActor(Context overallContext, String packageName) {
        context = new RestrictionMockContext(overallContext, packageName);
        this.packageName = packageName;
        resolver = new MockContentResolver();

        RenamingDelegatingContext targetContextWrapper = new RenamingDelegatingContext(
                context, overallContext, FILENAME_PREFIX);
        Context providerContext = new IsolatedContext(resolver, targetContextWrapper);

        provider = new SynchronousContactsProvider2();
        provider.attachInfo(providerContext, null);
        resolver.addProvider(ContactsContract.AUTHORITY, provider);
    }

    /**
     * Mock {@link Context} that reports specific well-known values for testing
     * data protection. The creator can override the owner package name, and
     * force the {@link PackageManager} to always return a well-known package
     * list for any call to {@link PackageManager#getPackagesForUid(int)}.
     * <p>
     * For example, the creator could request that the {@link Context} lives in
     * package name "com.example.red", and also cause the {@link PackageManager}
     * to report that no UID contains that package name.
     */
    private static class RestrictionMockContext extends MockContext {
        private final Context mOverallContext;
        private final String mReportedPackageName;
        private final RestrictionMockPackageManager mPackageManager;

        /**
         * Create a {@link Context} under the given package name.
         */
        public RestrictionMockContext(Context overallContext, String reportedPackageName) {
            mOverallContext = overallContext;
            mReportedPackageName = reportedPackageName;
            mPackageManager = new RestrictionMockPackageManager();
            mPackageManager.addPackage(1000, PACKAGE_GREY);
            mPackageManager.addPackage(2000, PACKAGE_RED);
            mPackageManager.addPackage(3000, PACKAGE_GREEN);
            mPackageManager.addPackage(4000, PACKAGE_BLUE);
        }

        @Override
        public String getPackageName() {
            return mReportedPackageName;
        }

        @Override
        public PackageManager getPackageManager() {
            return mPackageManager;
        }

        @Override
        public Resources getResources() {
            return mOverallContext.getResources();
        }
    }

    /**
     * Mock {@link PackageManager} that knows about a specific set of packages
     * to help test security models. Because {@link Binder#getCallingUid()}
     * can't be mocked, you'll have to find your mock-UID manually using your
     * {@link Context#getPackageName()}.
     */
    private static class RestrictionMockPackageManager extends MockPackageManager {
        private final HashMap<Integer, String> mForward = new HashMap<Integer, String>();
        private final HashMap<String, Integer> mReverse = new HashMap<String, Integer>();

        /**
         * Add a UID-to-package mapping, which is then stored internally.
         */
        public void addPackage(int packageUid, String packageName) {
            mForward.put(packageUid, packageName);
            mReverse.put(packageName, packageUid);
        }

        @Override
        public String[] getPackagesForUid(int uid) {
            return new String[] { mForward.get(uid) };
        }

        @Override
        public ApplicationInfo getApplicationInfo(String packageName, int flags) {
            ApplicationInfo info = new ApplicationInfo();
            Integer uid = mReverse.get(packageName);
            info.uid = (uid != null) ? uid : -1;
            return info;
        }
    }
}
