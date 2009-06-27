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

import android.test.suitebuilder.annotation.SmallTest;

import junit.framework.TestCase;

/**
 * Tests from {@link ContactAggregationScheduler}.
 */
@SmallTest
public class ContactAggregationSchedulerTest extends TestCase {

    private TestContactAggregationScheduler mScheduler;

    @Override
    protected void setUp() throws Exception {
        mScheduler = new TestContactAggregationScheduler();
    }

    public void testScheduleInitial() {
        mScheduler.schedule();
        assertEquals(1, mScheduler.mRunDelayed);
    }

    public void testScheduleTwiceRapidly() {
        mScheduler.schedule();

        mScheduler.mTime += ContactAggregationScheduler.AGGREGATION_DELAY / 2;
        mScheduler.schedule();
        assertEquals(2, mScheduler.mRunDelayed);
    }

    public void testScheduleTwiceExceedingMaxDelay() {
        mScheduler.schedule();

        mScheduler.mTime += ContactAggregationScheduler.MAX_AGGREGATION_DELAY + 100;
        mScheduler.schedule();
        assertEquals(1, mScheduler.mRunDelayed);
    }

    public void testScheduleWhileRunning() {
        mScheduler.setAggregator(new ContactAggregationScheduler.Aggregator() {
            boolean mInterruptCalled;

            public void interrupt() {
                mInterruptCalled = true;
            }

            public void run() {
                mScheduler.schedule();
                assertTrue(mInterruptCalled);
            }
        });

        mScheduler.run();
        assertEquals(1, mScheduler.mRunDelayed);
    }

    public void testScheduleWhileRunningExceedingMaxDelay() {
        mScheduler.schedule();

        mScheduler.mTime += ContactAggregationScheduler.MAX_AGGREGATION_DELAY + 100;

        mScheduler.setAggregator(new ContactAggregationScheduler.Aggregator() {
            boolean mInterruptCalled;

            public void interrupt() {
                mInterruptCalled = true;
            }

            public void run() {
                mScheduler.schedule();
                assertFalse(mInterruptCalled);
            }
        });

        mScheduler.run();
        assertEquals(2, mScheduler.mRunDelayed);
    }

    private static class TestContactAggregationScheduler extends ContactAggregationScheduler {

        long mTime = 1000;
        int mRunDelayed;

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        long currentTime() {
            return mTime;
        }

        @Override
        void runDelayed() {
            mRunDelayed++;
        }
    }
}
