/*
 * Copyright (C) 2022 The Android Open Source Project
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

package com.android.providers.contacts.util;

import android.util.Log;

import java.io.File;
import java.io.IOException;

public final class FileUtilities {

    public static final String TAG = FileUtilities.class.getSimpleName();
    public static final String INVALID_CALL_LOG_PATH_EXCEPTION_MESSAGE =
            "Invalid [Call Log] path. Cannot operate on file:";

    /**
     * Checks, whether the child directory is the same as, or a sub-directory of the base
     * directory.
     */
    public static boolean isSameOrSubDirectory(File base, File child) {
        try {
            File basePath = base.getCanonicalFile();
            File currPath = child.getCanonicalFile();
            while (currPath != null) {
                if (basePath.equals(currPath)) {
                    return true;
                }
                currPath = currPath.getParentFile(); // pops sub-dir
            }
            return false;
        } catch (IOException ex) {
            Log.e(TAG, "Error while accessing file", ex);
            return false;
        }
    }
}
