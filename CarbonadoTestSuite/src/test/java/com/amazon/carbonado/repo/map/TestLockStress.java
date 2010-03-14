/*
 * Copyright 2008-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.repo.map;

import java.util.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class TestLockStress {
    static long cSharedValue;

    static volatile boolean cStop;

    /**
     * @param args args[0]: readers, args[1]: upgraders, args[2]: writers, args[3]: run time
     */
    public static void main(String[] args) throws Exception {
        final int readers = Integer.parseInt(args[0]);
        final int upgraders = Integer.parseInt(args[1]);
        final int writers = Integer.parseInt(args[2]);
        final int runTimeSeconds = Integer.parseInt(args[3]);

        final UpgradableLock lock = new UpgradableLock();

        List<TestThread> threads = new ArrayList<TestThread>();

        for (int i=0; i<readers; i++) {
            TestThread t = new TestThread("reader " + i) {
                public void run() {
                    while (!cStop) {
                        lock.lockForRead(this);
                        try {
                            long value = cSharedValue;
                            // Do some "work"
                            String.valueOf(value);
                            long again = cSharedValue;
                            if (again != value) {
                                throw new AssertionError("" + again + " != " + value);
                            }
                            mReadCount++;
                        } finally {
                            lock.unlockFromRead(this);
                        }
                    }
                }
            };
            threads.add(t);
            t.start();
        }

        for (int i=0; i<upgraders; i++) {
            TestThread t = new TestThread("upgrader " + i) {
                public void run() {
                    while (!cStop) {
                        lock.lockForUpgrade(this);
                        try {
                            long value = cSharedValue;
                            // Do some "work"
                            String.valueOf(value);
                            assert(cSharedValue == value);
                            lock.lockForWrite(this);
                            assert(cSharedValue == value);
                            try {
                                cSharedValue = value + 1;
                            } finally {
                                lock.unlockFromWrite(this);
                            }
                            mAdjustCount++;
                        } finally {
                            lock.unlockFromUpgrade(this);
                        }
                    }
                }
            };
            threads.add(t);
            t.start();
        }

        for (int i=0; i<writers; i++) {
            TestThread t = new TestThread("writer " + i) {
                public void run() {
                    while (!cStop) {
                        lock.lockForWrite(this);
                        try {
                            long value = cSharedValue;
                            // Do some "work"
                            String.valueOf(value);
                            assert(cSharedValue == value);
                            cSharedValue = value + 1;
                            mAdjustCount++;
                        } finally {
                            lock.unlockFromWrite(this);
                        }
                    }
                }
            };
            threads.add(t);
            t.start();
        }

        Thread.sleep(1000L * runTimeSeconds);
        cStop = true;

        for (TestThread t : threads) {
            t.join();
        }

        long reads = 0;
        for (TestThread t : threads) {
            reads += t.mReadCount;
        }

        long expected = 0;
        for (TestThread t : threads) {
            expected += t.mAdjustCount;
        }

        System.out.println("Reads:    " + reads);
        System.out.println("Expected: " + expected);
        System.out.println("Actual:   " + cSharedValue);

        if (expected == cSharedValue) {
            System.out.println("SUCCESS");
        } else {
            System.out.println("FAILURE");
        }

        System.out.println();
        for (TestThread t : threads) {
            System.out.println(t.mReadCount + ", " + t.mAdjustCount);
        }
    }

    private abstract static class TestThread extends Thread {
        long mReadCount;
        long mAdjustCount;

        TestThread(String name) {
            super(name);
        }
    }
}
