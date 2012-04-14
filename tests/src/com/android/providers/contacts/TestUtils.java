/*
 * Copyright (C) 2012 The Android Open Source Project
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

import android.content.ContentValues;

import junit.framework.Assert;

public class TestUtils {
    private TestUtils() {
    }

    /** Convenient method to create a ContentValues */
    public static ContentValues cv(Object... namesAndValues) {
        Assert.assertTrue((namesAndValues.length % 2) == 0);

        final ContentValues ret = new ContentValues();
        for (int i = 1; i < namesAndValues.length; i += 2) {
            final String name = namesAndValues[i - 1].toString();
            final Object value =  namesAndValues[i];
            if (value == null) {
                ret.putNull(name);
            } else if (value instanceof String) {
                ret.put(name, (String) value);
            } else if (value instanceof Integer) {
                ret.put(name, (Integer) value);
            } else if (value instanceof Long) {
                ret.put(name, (Long) value);
            } else {
                Assert.fail("Unsupported type: " + value.getClass().getSimpleName());
            }
        }
        return ret;
    }
}
