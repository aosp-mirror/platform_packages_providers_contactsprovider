/*
 * Copyright (C) 2010 The Android Open Source Project
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
import android.content.Intent;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.content.pm.ProviderInfo;
import android.content.pm.ResolveInfo;
import android.content.res.Resources;
import android.os.Binder;
import android.os.UserHandle;
import android.test.mock.MockPackageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Mock {@link PackageManager} that knows about a specific set of packages
 * to help test security models. Because {@link Binder#getCallingUid()}
 * can't be mocked, you'll have to find your mock-UID manually using your
 * {@link Context#getPackageName()}.
 */
public class ContactsMockPackageManager extends MockPackageManager {

    private final Context mRealContext;

    private final HashMap<Integer, String> mForward = new HashMap<Integer, String>();
    private final HashMap<String, Integer> mReverse = new HashMap<String, Integer>();
    private List<PackageInfo> mPackages;

    public ContactsMockPackageManager(Context realContext) {
        mRealContext = realContext;
    }

    /**
     * Add a UID-to-package mapping, which is then stored internally.
     */
    public void addPackage(int packageUid, String packageName) {
        mForward.put(packageUid, packageName);
        mReverse.put(packageName, packageUid);
    }

    public void removePackage(int packageUid) {
        final String packageName = mForward.remove(packageUid);
        if (packageName != null) {
            mReverse.remove(packageName);
        }
    }

    @Override
    public String getNameForUid(int uid) {
        return "name-for-uid";
    }

    @Override
    public String[] getPackagesForUid(int uid) {
        final String packageName = mForward.get(uid);
        if (packageName != null) {
            return new String[] {packageName};
        } else if (mPackages != null) {
            return new String[] { mPackages.get(0).packageName };
        } else {
            return new String[] {};
        }
    }

    @Override
    public ApplicationInfo getApplicationInfo(String packageName, int flags)
            throws NameNotFoundException {
        ApplicationInfo info = new ApplicationInfo();
        Integer uid = mReverse.get(packageName);
        if (uid == null) {
            throw new NameNotFoundException();
        }
        info.uid = (uid != null) ? uid : -1;
        info.flags = ApplicationInfo.FLAG_INSTALLED;
        return info;
    }

    public void setInstalledPackages(List<PackageInfo> packages) {
        this.mPackages = packages;
    }

    @Override
    public List<PackageInfo> getInstalledPackages(int flags) {
        return mPackages;
    }

    @Override
    public PackageInfo getPackageInfo(String packageName, int flags) throws NameNotFoundException {
        for (PackageInfo info : mPackages) {
            if (info.packageName.equals(packageName)) {
                return info;
            }
        }
        throw new NameNotFoundException();
    }

    @Override
    public List<ResolveInfo> queryBroadcastReceivers(Intent intent, int flags) {
        return new ArrayList<ResolveInfo>();
    }

    @Override
    public Resources getResourcesForApplication(String appPackageName) {
        if (mRealContext.getPackageName().equals(appPackageName)) {
            return mRealContext.getResources();
        }
        return new ContactsMockResources();
    }

    @Override
    public List<ProviderInfo> queryContentProviders(String processName, int uid, int flags,
            String metaDataKey) {
        final List<ProviderInfo> ret = new ArrayList<>();
        final List<PackageInfo> packages = getInstalledPackages(flags);
        if (packages == null) {
            return ret;
        }
        for (PackageInfo pkg : packages) {
            if (pkg.providers == null) {
                continue;
            }
            for (ProviderInfo proi : pkg.providers) {
                if (metaDataKey == null) {
                    ret.add(proi);
                } else {
                    if (proi.metaData != null && proi.metaData.containsKey(metaDataKey)) {
                        ret.add(proi);
                    }
                }
            }
        }
        return ret;
    }

    @Override
    public List<ResolveInfo> queryIntentActivitiesAsUser(Intent intent, ResolveInfoFlags flags,
            UserHandle user) {
        return new ArrayList<>();
    }

    @Override
    public int getPackageUid(String packageName, int flags) throws NameNotFoundException {
        return 123;
    }

    @Override
    public boolean isPackageStopped(String packageName) throws NameNotFoundException {
        PackageInfo packageInfo = getPackageInfo(packageName, 0);
        return packageInfo.applicationInfo != null
                && ((packageInfo.applicationInfo.flags & ApplicationInfo.FLAG_STOPPED) != 0);
    }
}
