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

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;

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
        mRepo = MapRepositoryBuilder.newRepository();
    }

    protected void tearDown() throws Exception {
        mRepo = null;
    }

    public void testSimpleRollback() throws Exception {
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

    public void testDeadlock() throws Exception {
        // This test makes sure that transaction locker is not the current
        // thread. Top transactions will deadlock.

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

            Transaction txn2 = mRepo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
            try {
                stb = storage.prepare();
                stb.setId(2);
                stb.setStringProp("world");
                stb.setIntProp(3);
                stb.setLongProp(22);
                stb.setDoubleProp(234.2);
                try {
                    stb.insert();
                    fail();
                } catch (PersistTimeoutException e) {
                }
            } finally {
                txn2.exit();
            }

            txn.commit();
        } finally {
            txn.exit();
        }

        stb = storage.prepare();
        stb.setId(1);
        stb.load();
        assertEquals("hello", stb.getStringProp());
    }

    public void testIsolation() throws Exception {
        final Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);
        StorableTestBasic stb;

        stb = storage.prepare();
        stb.setId(1);
        stb.setStringProp("hello");
        stb.setIntProp(3);
        stb.setLongProp(22);
        stb.setDoubleProp(234.2);
        stb.insert();

        Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
        try {
            stb.load();
            assertEquals("hello", stb.getStringProp());

            Thread t = new Thread() {
                public void run() {
                    try {
                        StorableTestBasic stb = storage.prepare();
                        stb.setId(1);
                        assertTrue(stb.tryLoad());
                        stb.setStringProp("world");
                        stb.update();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            t.start();
            t.join();

            // Test that read is not repeatable.
            stb.load();
            assertEquals("world", stb.getStringProp());

            t = new Thread() {
                public void run() {
                    try {
                        Transaction txn = mRepo.enterTransaction();
                        try {
                            StorableTestBasic stb = storage.prepare();
                            stb.setId(1);
                            assertTrue(stb.tryLoad());
                            stb.setStringProp("world!!!");
                            stb.update();
                            txn.commit();
                        } finally {
                            txn.exit();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            t.start();
            t.join();

            // Test that read is still not repeatable.
            stb.load();
            assertEquals("world!!!", stb.getStringProp());

            txn.commit();
        } finally {
            txn.exit();
        }

        stb = storage.prepare();
        stb.setId(1);
        stb.load();
        assertEquals("world!!!", stb.getStringProp());

        stb.setStringProp("hello");
        stb.update();

        txn = mRepo.enterTransaction(IsolationLevel.REPEATABLE_READ);
        try {
            stb.load();
            assertEquals("hello", stb.getStringProp());

            Thread t = new Thread() {
                public void run() {
                    try {
                        StorableTestBasic stb = storage.prepare();
                        stb.setId(1);
                        assertTrue(stb.tryLoad());
                        stb.setStringProp("world");
                        stb.update();
                        fail();
                    } catch (PersistTimeoutException e) {
                        // expected
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            t.start();
            t.join();

            // Test that read is repeatable.
            stb.load();
            assertEquals("hello", stb.getStringProp());

            t = new Thread() {
                public void run() {
                    try {
                        Transaction txn = mRepo.enterTransaction();
                        try {
                            StorableTestBasic stb = storage.prepare();
                            stb.setId(1);
                            try {
                                assertTrue(stb.tryLoad());
                                fail();
                            } catch (FetchTimeoutException e) {
                                // expected
                            }
                            stb.setStringProp("world!!!");
                            stb.update();
                            fail();
                            txn.commit();
                            fail();
                        } finally {
                            txn.exit();
                        }
                    } catch (PersistTimeoutException e) {
                        // expected
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            t.start();
            t.join();

            // Test that read is still repeatable.
            stb.load();
            assertEquals("hello", stb.getStringProp());

            txn.commit();
        } finally {
            txn.exit();
        }
    }

    public void testForUpdate() throws Exception {
        // Tests that "for update" mode prevents deadlock.

        final Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);
        StorableTestBasic stb;

        stb = storage.prepare();
        stb.setId(1);
        stb.setStringProp("hello");
        stb.setIntProp(3);
        stb.setLongProp(22);
        stb.setDoubleProp(234.2);
        stb.insert();

        class Task extends Thread {
            private Boolean mLoaded;

            public void run() {
                try {
                    Transaction txn = mRepo.enterTransaction(IsolationLevel.REPEATABLE_READ);
                    try {
                        StorableTestBasic stb = storage.prepare();
                        stb.setId(1);
                        assertTrue(stb.tryLoad());
                        loaded(true);
                        Thread.sleep(1000);
                    } finally {
                        txn.exit();
                    }
                } catch (FetchTimeoutException e) {
                    // ok
                } catch (Exception e) {
                    e.printStackTrace();
                }
                loaded(false);
            }

            private synchronized void loaded(boolean b) {
                if (mLoaded == null) {
                    mLoaded = b;
                    notify();
                }
            }

            public synchronized boolean waitToLoad() throws InterruptedException {
                while (mLoaded == null) {
                    wait();
                }
                return mLoaded;
            }
        }

        Transaction txn = mRepo.enterTransaction();
        try {
            stb.load();

            Task t = new Task();

            t.start();
            assertTrue(t.waitToLoad());

            try {
                stb.load();
                fail();
            } catch (FetchTimeoutException e) {
                // expected
            }

            t.join();
        } finally {
            txn.exit();
        }

        txn = mRepo.enterTransaction();
        try {
            txn.setForUpdate(true);
            stb.load();

            Task t = new Task();

            t.start();
            assertFalse(t.waitToLoad());

            stb.load();
            stb.setStringProp("world");
            stb.update();

            t.join();

            txn.commit();
        } finally {
            txn.exit();
        }

        stb = storage.prepare();
        stb.setId(1);
        stb.load();
        assertEquals("world", stb.getStringProp());
    }
}
