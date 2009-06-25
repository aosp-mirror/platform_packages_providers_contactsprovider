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

/**
 * A version of {@link ContactsProvider2} class that performs aggregation
 * synchronously and wipes all data at construction time.
 */
public class SynchronousContactsProvider2 extends ContactsProvider2 {
    private static Boolean sDataWiped = false;
    private static OpenHelper mOpenHelper;

    public SynchronousContactsProvider2() {
        super(false);
    }

    @Override
    protected OpenHelper getOpenHelper(final Context context) {
        if (mOpenHelper == null) {
            mOpenHelper = new OpenHelper(context);
        }
        return mOpenHelper;
    }

    @Override
    public boolean onCreate() {
        boolean created = super.onCreate();
        synchronized (sDataWiped) {
            if (!sDataWiped) {
                sDataWiped = true;
                wipeData();
            }
        }
        return created;
    }
}
