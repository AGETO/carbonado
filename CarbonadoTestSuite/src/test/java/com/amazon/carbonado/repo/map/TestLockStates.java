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

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestLockStates extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestLockStates.class);
    }

    public TestLockStates(String s) {
        super(s);
    }

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
    }

    public void testStates() throws Exception {
        UpgradableLock<Object> lock = new UpgradableLock<Object>();
        Object locker = new Object();
        Object locker2 = new Object();

        assertTrue(lock.noLocksHeld());

        {
            lockForRead(lock, locker);
            assertFalse(lock.noLocksHeld());
            unlockFromRead(lock, locker);
            assertTrue(lock.noLocksHeld());

            lockForUpgrade(lock, locker);
            assertFalse(lock.noLocksHeld());
            unlockFromUpgrade(lock, locker);
            assertTrue(lock.noLocksHeld());

            lockForWrite(lock, locker);
            assertFalse(lock.noLocksHeld());
            unlockFromWrite(lock, locker);
            assertTrue(lock.noLocksHeld());
        }

        assertTrue(lock.noLocksHeld());

        {
            lockForRead(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            // should deadlock
            if (tryLockForWrite(lock, locker, 1000)) {
                fail();
                unlockFromWrite(lock, locker);
            }

            unlockFromRead(lock, locker);
        }

        assertTrue(lock.noLocksHeld());

        {
            lockForRead(lock, locker);
            lockForUpgrade(lock, locker);

            lockForRead(lock, locker);
            unlockFromRead(lock, locker);

            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);

            // should deadlock
            if (tryLockForWrite(lock, locker, 1000)) {
                fail();
                unlockFromWrite(lock, locker);
            }

            unlockFromRead(lock, locker);
            unlockFromUpgrade(lock, locker);
        }

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

        {
            lockForUpgrade(lock, locker);
            lockForWrite(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

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

        assertTrue(lock.noLocksHeld());

        {
            lockForWrite(lock, locker);
            // This is illegal usage which current implementation allows -- and ignores.
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
        }

        assertTrue(lock.noLocksHeld());

        {
            lockForWrite(lock, locker);
            lockForUpgrade(lock, locker);
            unlockFromUpgrade(lock, locker);
            unlockFromWrite(lock, locker);
            lockForUpgrade(lock, locker2);
            unlockFromUpgrade(lock, locker2);
        }

        assertTrue(lock.noLocksHeld());
    }

    private static void lockForRead(UpgradableLock<Object> lock, Object locker) {
        //System.out.println("read lock");
        lock.lockForRead(locker);
        //System.out.println(lock);
    }

    private static void unlockFromRead(UpgradableLock<Object> lock, Object locker) {
        //System.out.println("read unlock");
        lock.unlockFromRead(locker);
        //System.out.println(lock);
    }

    private static void lockForUpgrade(UpgradableLock<Object> lock, Object locker) {
        //System.out.println("upgrade lock");
        lock.lockForUpgrade(locker);
        //System.out.println(lock);
    }

    private static void unlockFromUpgrade(UpgradableLock<Object> lock, Object locker) {
        //System.out.println("upgrade unlock");
        lock.unlockFromUpgrade(locker);
        //System.out.println(lock);
    }

    private static void lockForWrite(UpgradableLock<Object> lock, Object locker)
        throws InterruptedException
    {
        //System.out.println("write lock");
        lock.lockForWriteInterruptibly(locker);
        //System.out.println(lock);
    }

    private static boolean tryLockForWrite(UpgradableLock<Object> lock, Object locker, int timeout)
        throws InterruptedException
    {
        //System.out.println("write lock");
        boolean result = lock.tryLockForWrite(locker, timeout, TimeUnit.MILLISECONDS);
        //System.out.println(lock);
        return result;
    }

    private static void unlockFromWrite(UpgradableLock<Object> lock, Object locker) {
        //System.out.println("write unlock");
        lock.unlockFromWrite(locker);
        //System.out.println(lock);
    }
}
