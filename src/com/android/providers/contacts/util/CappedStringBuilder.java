/*
 * Copyright (C) 2018 The Android Open Source Project
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

import android.util.Log;

import com.android.providers.contacts.AbstractContactsProvider;

public class CappedStringBuilder {
    private final int mCapSize;
    private boolean mOver;
    private final StringBuilder mStringBuilder = new StringBuilder();

    public CappedStringBuilder(int capSize) {
        mCapSize = capSize;
    }

    public void clear() {
        mOver = false;
        mStringBuilder.setLength(0);
    }

    public int length() {
        return mStringBuilder.length();
    }

    @Override
    public String toString() {
        return mStringBuilder.toString();
    }

    public CappedStringBuilder append(char ch) {
        if (canAppend(mStringBuilder.length() + 1)) {
            mStringBuilder.append(ch);
        }
        return this;
    }

    public CappedStringBuilder append(String s) {
        if (canAppend(mStringBuilder.length() + s.length())) {
            mStringBuilder.append(s);
        }
        return this;
    }

    private boolean canAppend(int length) {
        if (mOver || length > mCapSize) {
            if (!mOver && AbstractContactsProvider.VERBOSE_LOGGING) {
                Log.w(AbstractContactsProvider.TAG, "String too long! new length=" + length);
            }
            mOver = true;
            return false;
        }
        return true;
    }
}
