/*
 * Copyright 2009-2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.sleepycat;

import java.io.File;

import java.net.InetAddress;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutCapability;
import com.amazon.carbonado.layout.Unevolvable;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.amazon.carbonado.TestUtilities;

/**
 * @author Olga Kuznetosova
 */
public class CompressionTest extends TestCase {
    protected Repository mRepository;
    protected Repository mRepository1;
    private File mDir;
    private File mDir1;
 
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(CompressionTest.class);
    }

    @Override
    protected void tearDown() throws Exception {
        if (mRepository != null) {
            mRepository.close();
            mRepository = null;
        }

        if (mDir != null) {
            TestUtilities.deleteTempDir(mDir);
            mDir = null;
        } 

        if (mRepository1 != null) {
            mRepository1.close();
            mRepository1 = null;
        }

        if (mDir1 != null) {
            TestUtilities.deleteTempDir(mDir1);
            mDir1 = null;
        }   
    }

    public void test_compression() throws Exception {
        mRepository = createRepository("uncompressed");
        assertNotNull(mRepository);
        Storage<StorableMessage1> storage = mRepository.storageFor(StorableMessage1.class);

        StorableMessage1 message;
        for (int i = 0; i < 1000; ++i) {
            message = storage.prepare();
            message.setKey(Integer.toString(i)+ "aaa");
            message.setValue("WORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORD");
            message.insert();
            message.setValue("BORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORD");
            message.update();
        }

        mRepository1 = createZippedRepository("compressed");
        assertNotNull(mRepository1);
        Storage<StorableMessage1> storage1 = mRepository1.storageFor(StorableMessage1.class);
        StorableMessage1 message2;
        for (int i = 0; i < 1000; ++i) {
            message2 = storage1.prepare();
            message2.setKey(Integer.toString(i)+ "aaa");
            message2.setValue("WORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORD");
            message2.insert();
            message2.setValue("BORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORDWORD");
            message2.update();
        }


        File[] files = mDir.listFiles();
        long total = 0;
        for (int i = 0; i < files.length; i++) {
            total += files[i].length();
        }


        files = mDir1.listFiles();
        long total1 = 0;
        for (int i = 0; i < files.length; i++) {
            total1 += files[i].length();
        }
        assertTrue(total > total1);

    }

    public void test_compression1() throws Exception {
        mRepository = createRepository("uncompressed");
        assertNotNull(mRepository);
        Storage<StorableMessage1> storage = mRepository.storageFor(StorableMessage1.class);

        StorableMessage1 message;
        for (int i = 0; i < 10000; ++i) {
            message = storage.prepare();
            message.setKey(Integer.toString(i));
            message.setValue("abababababababa");
            message.insert();
        }

        mRepository1 = createZippedRepository("compressed");
        assertNotNull(mRepository1);
        Storage<StorableMessage1> storage1 = mRepository1.storageFor(StorableMessage1.class);
        StorableMessage1 message2;
        for (int i = 0; i < 10000; ++i) {
            message2 = storage1.prepare();
            message2.setKey(Integer.toString(i));
            message2.setValue("abababababababa");
            message2.insert();
        }


        File[] files = mDir.listFiles();
        long total = 0;
        for (int i = 0; i < files.length; i++) {
            total += files[i].length();
        }


        files = mDir1.listFiles();
        long total1 = 0;
        for (int i = 0; i < files.length; i++) {
            total1 += files[i].length();
        }

        assertTrue(total > total1);
    }

    public void test_layout() throws Exception {
        // Verify that compressed and uncompressed records of same type can co-exist.

        mDir = TestUtilities.makeTempDir("comp-layout");

        BDBRepositoryBuilder bdb = new BDBRepositoryBuilder();
        bdb.setName("comp-layout");
        bdb.setEnvironmentHomeFile(mDir);
        bdb.setCacheSize(200000);
        bdb.setTransactionWriteNoSync(true);
        mRepository = bdb.build();

        Storage<StorableMessage1> storage = mRepository.storageFor(StorableMessage1.class);
        StorableMessage1 message = storage.prepare();
        message.setKey("lemon");
        message.setValue("lemon-lemon-lemon");
        message.insert();

        LayoutCapability cap = mRepository.getCapability(LayoutCapability.class);
        Layout layout = cap.layoutFor(StorableMessage1.class);
        assertEquals(0, layout.getGeneration());

        mRepository.close();

        bdb.setCompressor(StorableMessage1.class.getName(), "GZIP");
        mRepository = bdb.build();

        storage = mRepository.storageFor(StorableMessage1.class);

        message = storage.prepare();
        message.setKey("lemon1");
        message.setValue("lemon-lemon-lemon");
        message.insert();

        message.load();
        assertEquals("lemon-lemon-lemon", message.getValue());

        cap = mRepository.getCapability(LayoutCapability.class);
        layout = cap.layoutFor(StorableMessage1.class);
        assertEquals(1, layout.getGeneration());

        message = storage.prepare();
        message.setKey("lemon");
        message.load();
        assertEquals("lemon-lemon-lemon", message.getValue());

        // Finally, revert to original uncompressed mode and load mixed records.
        mRepository.close();

        bdb.setCompressor(StorableMessage1.class.getName(), "NONE");
        mRepository = bdb.build();

        storage = mRepository.storageFor(StorableMessage1.class);

        message = storage.prepare();
        message.setKey("lemon");
        message.load();
        assertEquals("lemon-lemon-lemon", message.getValue());

        message = storage.prepare();
        message.setKey("lemon1");
        message.load();
        assertEquals("lemon-lemon-lemon", message.getValue());

        cap = mRepository.getCapability(LayoutCapability.class);
        layout = cap.layoutFor(StorableMessage1.class);
        assertEquals(0, layout.getGeneration());
    }

    public void test_unevolvable() throws Exception {
        // Demonstrate that unevolvable storables cannot switch compression mode.

        mDir = TestUtilities.makeTempDir("comp-layout");

        BDBRepositoryBuilder bdb = new BDBRepositoryBuilder();
        bdb.setName("comp-layout");
        bdb.setEnvironmentHomeFile(mDir);
        bdb.setCacheSize(200000);
        bdb.setTransactionWriteNoSync(true);
        mRepository = bdb.build();

        Storage<Unevo> storage = mRepository.storageFor(Unevo.class);
        Unevo message = storage.prepare();
        message.setKey("lemon");
        message.setValue("lemon-lemon-lemon");
        message.insert();

        LayoutCapability cap = mRepository.getCapability(LayoutCapability.class);
        Layout layout = cap.layoutFor(Unevo.class);
        assertEquals(null, layout);

        mRepository.close();

        bdb.setCompressor(Unevo.class.getName(), "GZIP");
        mRepository = bdb.build();

        storage = mRepository.storageFor(Unevo.class);

        message = storage.prepare();
        message.setKey("lemon");
        try {
            message.load();
            fail();
        } catch (CorruptEncodingException e) {
        }
    }

    private Repository createRepository(String name) throws Exception {
        BDBRepositoryBuilder bdb = new BDBRepositoryBuilder();

        mDir = TestUtilities.makeTempDir(name);
        bdb.setName(name);
        bdb.setEnvironmentHomeFile(mDir);
        bdb.setCacheSize(200000);
        bdb.setTransactionWriteNoSync(true);
        return bdb.build();
    }

    private Repository createZippedRepository(String name) throws Exception {
        BDBRepositoryBuilder bdb = new BDBRepositoryBuilder();

        mDir1 = TestUtilities.makeTempDir(name);
        bdb.setName(name);
        bdb.setEnvironmentHomeFile(mDir1);
        bdb.setCompressor(StorableMessage1.class.getName(), "GZIP");
        bdb.setCacheSize(200000);
        bdb.setTransactionWriteNoSync(true);
        return bdb.build();
    }

    @PrimaryKey("key")
    public static abstract class Unevo extends StorableMessage1 implements Unevolvable {
    }
}
