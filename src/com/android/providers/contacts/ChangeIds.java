/*
 * Copyright (C) 2024 The Android Open Source Project
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

import android.compat.annotation.ChangeId;
import android.compat.annotation.EnabledSince;
import android.os.Build;

/** All the {@link ChangeId} used for the Contacts Provider. */
public class ChangeIds {
    /**
     * Restricts contact creation in specific accounts, starting from
     * {@link android.os.Build.VERSION_CODES#BAKLAVA}.
     * <p>When enabled, this feature prevents the creation of contacts under local or SIM accounts
     * if the default account is associated with a cloud provider.
     */
    @ChangeId
    @EnabledSince(targetSdkVersion = Build.VERSION_CODES.BAKLAVA)
    public static final long RESTRICT_CONTACTS_CREATION_IN_ACCOUNTS = 352312780L;
}
