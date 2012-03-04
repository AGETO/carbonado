/*
 * Copyright 2009-2012 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.je;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;

import com.amazon.carbonado.repo.sleepycat.*;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestTransaction extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestTransaction.class);
    }

    public TestTransaction(String s) {
        super(s);
    }

    private Repository mRepo;

    protected void setUp() throws Exception {
        BDBRepositoryBuilder bob = new BDBRepositoryBuilder();
        bob.setProduct("JE");
        bob.setName("test");
        bob.setTransactionNoSync(true);
        bob.setCacheSize(100000);
        bob.setLogInMemory(true);
        bob.setEnvironmentHome(TestUtilities.makeTestDirectoryString("test"));
        bob.setLockTimeout(5.0);
        mRepo = bob.build();

    }

    protected void tearDown() throws Exception {
        if (mRepo != null) {
            mRepo.close();
        }
        mRepo = null;
    }

    public void testNestedRollback() throws Exception {
        Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);
        StorableTestBasic stb;

        Transaction txn = mRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(1);
            stb.setStringProp("hello");
            stb.setIntProp(3);
            stb.setLongProp(22);
            stb.setDoubleProp(234.2);
            stb.insert();

            StorableTestBasic stb2 = storage.prepare();
            stb2.setId(1);
            stb2.load();
            assertEquals("hello", stb2.getStringProp());

            Transaction txn2 = mRepo.enterTransaction();
            try {
                stb2.setStringProp("world");
                stb2.update();
                stb2.load();
                assertEquals("world", stb2.getStringProp());
            } finally {
                txn2.exit();
            }

            stb2.load();
            assertEquals("hello", stb2.getStringProp());
        } finally {
            txn.exit();
        }

        assertEquals(0, storage.query().count());

        stb.insert();

        assertEquals(1, storage.query().count());

        txn = mRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(1);
            stb.setStringProp("world");
            stb.update();

            StorableTestBasic stb2 = storage.prepare();
            stb2.setId(1);
            stb2.load();
            assertEquals("world", stb2.getStringProp());

            Transaction txn2 = mRepo.enterTransaction();
            try {
                stb2.setStringProp("everybody");
                stb2.update();
                stb2.load();
                assertEquals("everybody", stb2.getStringProp());

                StorableTestBasic stb3 = storage.prepare();
                stb3.setId(1);
                stb3.setStringProp("text");
                stb3.setIntProp(33);
                stb3.setLongProp(2);
                stb3.setDoubleProp(34.2);
                assertFalse(stb3.tryInsert());

                stb3.setId(2);
                stb3.insert();

                stb3.load();
                assertEquals("text", stb3.getStringProp());
            } finally {
                txn2.exit();
            }

            stb2.load();
            assertEquals("world", stb2.getStringProp());

            StorableTestBasic stb3 = storage.prepare();
            stb3.setId(2);
            assertFalse(stb3.tryLoad());
        } finally {
            txn.exit();
        }

        assertEquals(1, storage.query().count());

        stb = storage.prepare();
        stb.setId(1);
        stb.load();
        assertEquals("hello", stb.getStringProp());

        txn = mRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(1);
            stb.delete();

            StorableTestBasic stb2 = storage.prepare();
            stb2.setId(1);
            assertFalse(stb2.tryLoad());

            Transaction txn2 = mRepo.enterTransaction();
            try {
                StorableTestBasic stb3 = storage.prepare();
                stb3.setId(1);
                stb3.setStringProp("text");
                stb3.setIntProp(33);
                stb3.setLongProp(2);
                stb3.setDoubleProp(34.2);
                stb3.insert();

                assertTrue(stb3.tryLoad());
                assertEquals("text", stb3.getStringProp());
            } finally {
                txn2.exit();
            }

            assertFalse(stb2.tryLoad());
        } finally {
            txn.exit();
        }

        assertEquals(1, storage.query().count());

        stb = storage.prepare();
        stb.setId(1);
        stb.load();
        assertEquals("hello", stb.getStringProp());

        stb.delete();

        assertEquals(0, storage.query().count());
    }

    public void testNestedRollback2() throws Exception {
        Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);

        assertEquals(0, storage.query().count());

        Transaction outer = mRepo.enterTransaction();

        {
            Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
            StorableTestBasic stb = storage.prepare();
            stb.setId(1);
            stb.setStringProp("");
            stb.setIntProp(0);
            stb.setLongProp(0);
            stb.setDoubleProp(0);
            stb.insert();
 
            assertEquals(1, storage.query().count());

            txn.exit();

            assertEquals(0, storage.query().count());
        }

        {
            Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
            StorableTestBasic stb = storage.prepare();
            stb.setId(2);
            stb.setStringProp("");
            stb.setIntProp(0);
            stb.setLongProp(0);
            stb.setDoubleProp(0);
            stb.insert();
 
            assertEquals(1, storage.query().count());

            txn.exit();
 
            assertEquals(0, storage.query().count());
        }

        outer.commit();
        outer.exit();
    }

    public void testNestedTimeout() throws Exception {
        final Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);

        {
            StorableTestBasic stb = storage.prepare();
            stb.setId(100);
            stb.setStringProp("hello");
            stb.setIntProp(3);
            stb.setLongProp(22);
            stb.setDoubleProp(234.2);
            stb.insert();
        }

        class Locker extends Thread {
            volatile boolean locked;
            volatile boolean stop;

            public void run() {
                try {
                    Transaction txn = mRepo.enterTransaction();
                    try {
                        txn.setForUpdate(true);
                        StorableTestBasic stb = storage.prepare();
                        stb.setId(100);
                        stb.load();

                        locked = true;

                        while (!stop) {
                            Thread.sleep(100);
                        }
                    } finally {
                        txn.exit();
                        locked = false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        Locker locker = new Locker();
        locker.start();

        while (!locker.locked) {
            Thread.sleep(10);
        }

        StorableTestBasic stb = storage.prepare();
        stb.setId(100);

        try {
            stb.load();
        } catch (FetchException e) {
            assertTrue(e.toString().indexOf("timeoutMillis=5000") >= 0);
        }

        // JE kills the outer transaction after any timeout. This limits the
        // kinds of nested timeouts that can be peformed.

        Transaction txn = mRepo.enterTransaction();
        try {
            txn.setDesiredLockTimeout(1, TimeUnit.SECONDS);
            try {
                stb.load();
            } catch (FetchException e) {
                assertTrue(e.toString().indexOf("timeoutMillis=1000") >= 0);
            }
        } finally {
            txn.exit();
        }

        txn = mRepo.enterTransaction();
        try {
            Transaction inner = mRepo.enterTransaction();
            try {
                inner.setDesiredLockTimeout(1500, TimeUnit.MILLISECONDS);
                try {
                    stb.load();
                } catch (FetchException e) {
                    assertTrue(e.toString().indexOf("timeoutMillis=1500") >= 0);
                }
            } finally {
                inner.exit();
            }
        } finally {
            txn.exit();
        }

        txn = mRepo.enterTransaction();
        try {
            txn.setDesiredLockTimeout(1, TimeUnit.SECONDS);

            Transaction inner = mRepo.enterTransaction();
            try {
                inner.setDesiredLockTimeout(1500, TimeUnit.MILLISECONDS);
            } finally {
                inner.exit();
            }

            try {
                stb.load();
            } catch (FetchException e) {
                assertTrue(e.toString().indexOf("timeoutMillis=1000") >= 0);
            }
        } finally {
            txn.exit();
        }

        txn = mRepo.enterTransaction();
        try {
            txn.setDesiredLockTimeout(1, TimeUnit.SECONDS);

            Transaction inner = mRepo.enterTransaction();
            try {
                inner.setDesiredLockTimeout(1500, TimeUnit.MILLISECONDS);
                Transaction inner2 = mRepo.enterTransaction();
                try {
                    inner2.setDesiredLockTimeout(500, TimeUnit.MILLISECONDS);
                    try {
                        stb.load();
                    } catch (FetchException e) {
                        assertTrue(e.toString().indexOf("timeoutMillis=500") >= 0);
                    }
                } finally {
                    inner2.exit();
                }
            } finally {
                inner.exit();
            }
        } finally {
            txn.exit();
        }

        txn = mRepo.enterTransaction();
        try {
            txn.setDesiredLockTimeout(1, TimeUnit.SECONDS);

            Transaction inner = mRepo.enterTransaction();
            try {
                inner.setDesiredLockTimeout(1500, TimeUnit.MILLISECONDS);
                Transaction inner2 = mRepo.enterTransaction();
                try {
                    inner2.setDesiredLockTimeout(500, TimeUnit.MILLISECONDS);
                } finally {
                    inner2.exit();
                }
            } finally {
                inner.exit();
            }

            try {
                stb.load();
            } catch (FetchException e) {
                assertTrue(e.toString().indexOf("timeoutMillis=1000") >= 0);
            }
        } finally {
            txn.exit();
        }

        try {
            stb.load();
        } catch (FetchException e) {
            assertTrue(e.toString().indexOf("timeoutMillis=5000") >= 0);
        }

        locker.stop = true;

        while (locker.locked) {
            Thread.sleep(10);
        }
    }
}
