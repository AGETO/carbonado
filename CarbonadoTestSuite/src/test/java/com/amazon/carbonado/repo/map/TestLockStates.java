/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestLockStates {
    private static void lockForRead(UpgradableLock lock, TestLockStates locker) {
        //System.out.println("read lock");
        lock.lockForRead(locker);
        //System.out.println(lock);
    }

    private static void unlockFromRead(UpgradableLock lock, TestLockStates locker) {
        //System.out.println("read unlock");
        lock.unlockFromRead(locker);
        //System.out.println(lock);
    }

    private static void lockForUpgrade(UpgradableLock lock, TestLockStates locker) {
        //System.out.println("upgrade lock");
        lock.lockForUpgrade(locker);
        //System.out.println(lock);
    }

    private static void unlockFromUpgrade(UpgradableLock lock, TestLockStates locker) {
        //System.out.println("upgrade unlock");
        lock.unlockFromUpgrade(locker);
        //System.out.println(lock);
    }

    private static void lockForWrite(UpgradableLock lock, TestLockStates locker)
        throws InterruptedException
    {
        //System.out.println("write lock");
        lock.lockForWriteInterruptibly(locker);
        //System.out.println(lock);
    }

    private static boolean tryLockForWrite(UpgradableLock lock, TestLockStates locker, int timeout)
        throws InterruptedException
    {
        //System.out.println("write lock");
        boolean result = lock.tryLockForWrite(locker, timeout, TimeUnit.MILLISECONDS);
        //System.out.println(lock);
        return result;
    }

    private static void unlockFromWrite(UpgradableLock lock, TestLockStates locker) {
        //System.out.println("write unlock");
        lock.unlockFromWrite(locker);
        //System.out.println(lock);
    }

    public static void main(String[] args) throws Exception {
        UpgradableLock lock = new UpgradableLock();
        TestLockStates locker = new TestLockStates();
        TestLockStates locker2 = new TestLockStates();

        /*
        final Thread main = Thread.currentThread();

        new Thread() {
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
                main.interrupt();
            }
        }.start();
        */

        System.out.println("0 ----------------------");
        System.out.println(lock);

        {
            // start with no locks

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("1 ----------------------");
        System.out.println(lock);

        {
            lockForRead(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            // should deadlock
            if (tryLockForWrite(lock, locker, 1000)) {
                System.out.println("***** did not deadlock!!!");
                unlockFromWrite(lock, locker);
            }

            unlockFromRead(lock, locker);
        }

        System.out.println("2 ----------------------");
        System.out.println(lock);

        {
            lockForRead(lock, locker);
            lockForUpgrade(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            // should deadlock
            if (tryLockForWrite(lock, locker, 1000)) {
                System.out.println("***** did not deadlock!!!");
                unlockFromWrite(lock, locker);
            }

            unlockFromRead(lock, locker);
            unlockFromUpgrade(lock, locker);
        }

        System.out.println("3 ----------------------");
        System.out.println(lock);

        {
            lockForUpgrade(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);

            unlockFromUpgrade(lock, locker);
        }

        System.out.println("4 ----------------------");
        System.out.println(lock);

        {
            lockForWrite(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);

            unlockFromWrite(lock, locker);
        }

        System.out.println("5 ----------------------");
        System.out.println(lock);

        {
            lockForWrite(lock, locker);
            lockForRead(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);

            unlockFromRead(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("6 ----------------------");
        System.out.println(lock);

        {
            lockForWrite(lock, locker);
            lockForUpgrade(lock, locker);
            lockForRead(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);

            unlockFromRead(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("7 ----------------------");
        System.out.println(lock);

        {
            lockForWrite(lock, locker);
            lockForUpgrade(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            lockForWrite(lock, locker);
            unlockFromWrite(lock, locker);

            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("8 ----------------------");
        System.out.println(lock);

        {
            lockForUpgrade(lock, locker);
            lockForWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("9 ----------------------");
        System.out.println(lock);

        {
            lockForUpgrade(lock, locker);
            lockForWrite(lock, locker);
            lockForUpgrade(lock, locker);
            lockForWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        System.out.println("10 ----------------------");
        System.out.println(lock);

        {
            lockForUpgrade(lock, locker);
            lockForUpgrade(lock, locker);
            lockForWrite(lock, locker);
            lockForWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);
            lockForUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
            unlockFromWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
        }

        System.out.println("11 ----------------------");
        System.out.println(lock);

        {
            lockForWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            try {
                unlockFromWrite(lock, locker);
                System.out.println("***** unlocked");
            } catch (IllegalMonitorStateException e) {
            }
        }

        System.out.println("12 ----------------------");
        System.out.println(lock);
     }

    public TestLockStates() {
    }
}
