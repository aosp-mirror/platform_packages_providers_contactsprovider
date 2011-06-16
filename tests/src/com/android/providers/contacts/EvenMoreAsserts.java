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

import junit.framework.Assert;

/**
 * Contains additional assertion methods not found in Junit or MoreAsserts.
 */
public final class EvenMoreAsserts {
    // Non instantiable.
    private EvenMoreAsserts() { }

    public static <T extends Exception> void assertThrows(Class<T> exception, Runnable r) {
        assertThrows(null, exception, r);
    }

    public static <T extends Exception> void assertThrows(String message, Class<T> exception,
            Runnable r) {
        try {
            r.run();
           // Cannot invoke Assert.fail() here because it will be caught by the try/catch below
           // and, if we are expecting an AssertionError or AssertionFailedError (depending on
           // the platform), we might incorrectly identify that as a success.
        } catch (Exception caught) {
            if (!exception.isInstance(caught)) {
                Assert.fail(appendUserMessage("Exception " + exception + " expected but " +
                        caught +" thrown.", message));
            }
            return;
        }
        Assert.fail(appendUserMessage(
                "Exception " + exception + " expected but no exception was thrown.",
                message));
    }

    private static String appendUserMessage(String errorMsg, String userMsg) {
        return userMsg == null ? errorMsg : errorMsg + userMsg;
    }
}
