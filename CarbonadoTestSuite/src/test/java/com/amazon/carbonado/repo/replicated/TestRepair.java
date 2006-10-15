/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.replicated;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.repo.replicated.ReplicatedRepository;
import com.amazon.carbonado.repo.replicated.ReplicatedRepositoryBuilder;

import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableVersioned;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestRepair extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestRepair.class);
    }

    private Repository mReplica;
    private Repository mMaster;
    private Repository mReplicated;

    public TestRepair(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        RepositoryBuilder replica = TestUtilities.newTempRepositoryBuilder("rr-replica");
        RepositoryBuilder master = TestUtilities.newTempRepositoryBuilder("rr-master");

        ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
        builder.setName("rr");
        builder.setReplicaRepositoryBuilder(replica);
        builder.setMasterRepositoryBuilder(master);

        ReplicatedRepository rr = (ReplicatedRepository) builder.build();

        mReplica = rr.getReplicaRepository();
        mMaster = rr.getMasterRepository();
        mReplicated = rr;
    }

    protected void tearDown() throws Exception {
        mReplicated.close();
        mReplica = null;
        mMaster = null;
        mReplicated = null;
    }

    public void testMissingEntry() throws Exception {
        // Insert an entry into master.
        {
            Storage<StorableTestBasic> storage = mMaster.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            stb.setStringProp("hello");
            stb.setIntProp(1);
            stb.setLongProp(1L);
            stb.setDoubleProp(1.0);
            stb.insert();
        }

        // Verify not available from rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            assertFalse(stb.tryLoad());
        }

        // Insert into rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            stb.setStringProp("world");
            stb.setIntProp(1);
            stb.setLongProp(1L);
            stb.setDoubleProp(1.0);
            try {
                stb.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify available from rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            assertTrue(stb.tryLoad());
            assertEquals("hello", stb.getStringProp());
        }
    }

    public void testMissingVersionedEntry() throws Exception {
        // Insert an entry into master.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Verify not available from rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            assertFalse(sv.tryLoad());
        }

        // Insert into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("world");
            try {
                sv.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify available from rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            assertTrue(sv.tryLoad());
            assertEquals("hello", sv.getValue());
            assertEquals(1, sv.getVersion());
        }
    }

    public void testStaleEntry() throws Exception {
        // Insert an entry into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Update master entry.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            sv.setValue("world");
            sv.update();
        }

        // Verify old version in replica.
        {
            Storage<StorableVersioned> storage = mReplica.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(1, sv.getVersion());
            assertEquals("hello", sv.getValue());
        }

        // Attempt to update rr entry.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(1, sv.getVersion());
            assertEquals("hello", sv.getValue());

            sv.setValue("ciao");
            try {
                sv.update();
                fail();
            } catch (OptimisticLockException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify new version in rr and update it.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(2, sv.getVersion());
            assertEquals("world", sv.getValue());

            sv.setValue("ciao");
            sv.update();
        }
    }

    public void testStaleEntryAndBackoff() throws Exception {
        // Insert an entry into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Update master entry.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            sv.setValue("world");
            sv.update();
        }

        // Attempt to update rr entry.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);

            int failCount = 0;

            for (int retryCount = 3;;) {
                try {
                    sv.load();
                    sv.setValue("ciao");
                    sv.update();
                    break;
                } catch (OptimisticLockException e) {
                    failCount++;
                    retryCount = e.backoff(e, retryCount, 1000);
                }
            }

            assertTrue(failCount > 0);
        }

        // Verify new version in rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(3, sv.getVersion());
            assertEquals("ciao", sv.getValue());
        }
    }
}
