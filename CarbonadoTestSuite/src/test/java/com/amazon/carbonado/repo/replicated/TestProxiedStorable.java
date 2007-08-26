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

import java.lang.reflect.Method;

import junit.framework.TestSuite;
import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.gen.StorableInterceptorFactory;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestBasicIdMunger;
import com.amazon.carbonado.TestStorables;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.TestStorableBase;

/*
 * TestProxiedStorable
 *
 * @author Don Schneider
 */
public class TestProxiedStorable extends TestStorableBase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestProxiedStorable.class);
    }

    public TestProxiedStorable() {
        super();
    }

    /**
     * Test copy storable properties interface
     */
    public void test_copyStorableProperties() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic storable = storage.prepare();

        TestStorables.InvocationTracker tracker = new TestStorables.InvocationTracker("tracker");
        storable.copyAllProperties(tracker);
        // unloaded, not dirty; nothing happens
        tracker.assertTrack(0);

        storable.setId(1);
        storable.setIntProp(1);
        storable.copyAllProperties(tracker);
        // setId, setIntProp
        tracker.assertTrack(0x1 + 0x10);
        tracker.clearTracks();

        setBasicProperties(storable);
        storable.copyAllProperties(tracker);
        // setXxx
        tracker.assertTrack(TestStorables.ALL_SET_METHODS);
        tracker.clearTracks();

        storable = storage.prepare();
        storable.copyPrimaryKeyProperties(tracker);
        tracker.assertTrack(0);
        setPrimaryKeyProperties(storable);
        storable.copyPrimaryKeyProperties(tracker);
        // setXxx
        tracker.assertTrack(TestStorables.ALL_PRIMARY_KEYS);
        tracker.clearTracks();

        storable = storage.prepare();
        storable.copyUnequalProperties(tracker);
        // Better not do anything since unloaded
        tracker.assertTrack(0);
        storable.setIntProp(0);  // this will now be dirty, and equal
        storable.copyUnequalProperties(tracker);
        // Should only examine one dirty property since still unloaded
        tracker.assertTrack(0x8);
        storable.setIntProp(1);  // this will now be dirty and not equal
        storable.copyUnequalProperties(tracker);
        tracker.assertTrack(0x8 + 0x10);

        // get a fresh one
        storable = storage.prepare();
        storable.setStringProp("hi");
        storable.setId(22);
        storable.copyPrimaryKeyProperties(tracker);
        storable.copyDirtyProperties(tracker);
        // setId, setStringProp
        tracker.assertTrack(0x1 + 0x4);
    }

    /**
     * Test Interceptor
     */
    public void test_Interceptor() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic storable = storage.prepare();

        StorableInterceptorFactory<StorableTestBasic> proxyFactory
                = StorableInterceptorFactory.getInstance(StorableTestBasicIdMunger.class,
                                                         StorableTestBasic.class,
                                                         false);
        StorableTestBasic proxy = proxyFactory.create(storable);

        proxy.setId(1);
        assertEquals(storable.getId(), 2<<8);

        storable.setId(5<<8);
        assertEquals(proxy.getId(), 4);

        proxy.setStringProp("passthrough");
        assertEquals("passthrough", storable.getStringProp());
    }

    /**
     * Test replicatingStorable
     */
    public void test_replicatingStorable() throws Exception {
        Repository altRepo = TestUtilities.buildTempRepository("alt");

        final Storage<StorableTestBasic> readage =
            getRepository().storageFor(StorableTestBasic.class);
        final Storage<StorableTestBasic> writage = altRepo.storageFor(StorableTestBasic.class);

        Storage<StorableTestBasic> wrappage =
            new ReplicatedStorage<StorableTestBasic>(getRepository(), readage, writage);

        StorableTestBasic replicator = wrappage.prepare();

        replicator.setId(1);
        setBasicProperties(replicator);
        replicator.insert();

        StorableTestBasic reader = load(readage, 1);
        StorableTestBasic writer = load(writage, 1);

        assertTrue(reader.equalProperties(writer));

        assertStorableEquivalenceById(1, readage, writage);

        replicator = wrappage.prepare();

        replicator.setId(1);
        replicator.setStringProp("updated");
        replicator.setLongProp(2342332);
        replicator.update();
        writer = load(writage, 1);
        reader = load(readage, 1);
        assertTrue(reader.equalProperties(writer));

        replicator.delete();


        try {
            reader.load();
            fail("successfully loaded deleted 'read' storable");
        }
        catch (FetchException e) {
            // expected
        }
        try {
            writer.load();
            fail("successfully loaded deleted 'write' storable");
        }
        catch (FetchException e) {
            // expected
        }

        StorableTestBasic replicator2 = wrappage.prepare();

        replicator2.setId(2);
        setBasicProperties(replicator2);
        replicator2.insert();

        // Now use the old replicator (which should be in "unloaded" state, since we've
        // just finished deleting something, to delete it.
        replicator.setId(2);
        replicator.delete();

        try {
            load(readage, 2);
            fail("successfully loaded deleted 'read' storable 2");
        }
        catch (FetchException e) {
            // expected
        }
        try {
            load(writage, 2);
            fail("successfully loaded deleted 'write' storable 2");
        }
        catch (FetchException e) {
            // expected
        }

        altRepo.close();
        altRepo = null;
    }

}
