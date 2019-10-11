/*
 * Copyright (C) 2017 The Android Open Source Project
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
package com.android.providers.contacts;

import android.test.suitebuilder.annotation.LargeTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@LargeTest
public class ContactsTaskSchedulerTest extends FixedAndroidTestCase {
    private static final int SHUTDOWN_SECONDS = 3;

    private static class MyContactsTaskScheduler extends ContactsTaskScheduler {
        final CountDownLatch latch;

        final List<String> executed = new ArrayList<>();

        public MyContactsTaskScheduler(int numExpectedTasks) {
            super("Test", SHUTDOWN_SECONDS);
            latch = new CountDownLatch(numExpectedTasks);
        }

        @Override
        public void onPerformTask(int taskId, Object arg) {
            executed.add("" + taskId + "," + arg);

            latch.countDown();
        }
    }

    public void testSimple() throws Exception {
        final MyContactsTaskScheduler scheduler = new MyContactsTaskScheduler(3);

        scheduler.scheduleTask(1);
        scheduler.scheduleTask(10);
        scheduler.scheduleTask(2, "arg");

        assertTrue(scheduler.latch.await(10, TimeUnit.SECONDS));

        assertEquals(Arrays.asList("1,null", "10,null", "2,arg"), scheduler.executed);

        // Only one thread has been created.
        assertEquals(1, scheduler.getThreadSequenceNumber());
    }

    public void testAutoShutdown() throws Exception {
        final MyContactsTaskScheduler scheduler = new MyContactsTaskScheduler(7);

        scheduler.scheduleTask(1);

        // Wait for 10 seconds and the thread should shut down.
        assertTrue(scheduler.isRunningForTest());
        Thread.sleep(10 * 1000);
        assertFalse(scheduler.isRunningForTest());

        scheduler.scheduleTask(2);
        assertTrue(scheduler.isRunningForTest());

        Thread.sleep(1 * 1000);
        scheduler.scheduleTask(3);

        Thread.sleep(1 * 1000);
        scheduler.scheduleTask(4);

        Thread.sleep(1 * 1000);
        scheduler.scheduleTask(5);

        Thread.sleep(1 * 1000);
        scheduler.scheduleTask(6);
        assertTrue(scheduler.isRunningForTest()); // Should still alive.

        // Wait for 10 seconds and the thread should shut down.
        Thread.sleep(10 * 1000);
        assertFalse(scheduler.isRunningForTest());

        scheduler.scheduleTask(7);
        assertTrue(scheduler.isRunningForTest());

        assertTrue(scheduler.latch.await(10, TimeUnit.SECONDS));
        assertEquals(7, scheduler.executed.size());

        // Only one thread has been created.
        assertEquals(3, scheduler.getThreadSequenceNumber());
    }
}
