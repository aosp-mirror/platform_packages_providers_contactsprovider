/*
 * Copyright (C) 2023 The Android Open Source Project
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

/**
 * A mock of the CountryMonitor class which always returns US
 * when country ISO code is requested to ensure that the tests
 * run successfully irrespective of the physical location of the
 * device.
 */
public class MockCountryMonitor extends CountryMonitor {

    public MockCountryMonitor(Context context) {
        super(context);
    }

    @Override
    public String getCountryIso() {
        return "US";
    }
}
