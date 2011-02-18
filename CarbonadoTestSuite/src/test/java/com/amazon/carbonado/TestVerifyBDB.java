/*
 * Copyright 2011 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado;

import java.io.File;
import java.io.RandomAccessFile;

import java.util.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;
import com.amazon.carbonado.repo.sleepycat.CheckpointCapability;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestVerifyBDB extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestVerifyBDB.class);
        return suite;
    }

    private List<File> mTempDirs = new ArrayList<File>();

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (mTempDirs != null) {
            for (File dir : mTempDirs) {
                TestUtilities.deleteTempDir(dir);
            }
        }
    }

    public void testVerify() throws Exception {
        File dir = makeTempDir();
        BDBRepositoryBuilder bob = newBuilder(dir, "test");
        Repository repo = bob.build();

        // Fill up repository with some records.
        Storage<StorableTestBasic> g = repo.storageFor(StorableTestBasic.class);
        for (int i=0; i<1000; i++) {
            StorableTestBasic stb = g.prepare();
            stb.setId(i);
            stb.setStringProp("str-" + i);
            stb.setIntProp(i);
            stb.setLongProp(i);
            stb.setDoubleProp(i);
            stb.insert();
        }

        repo.getCapability(CheckpointCapability.class).forceCheckpoint();

        repo.close();

        boolean result = newBuilder(dir, null).verify(System.out);

        assertTrue(result);

        // Now corrupt one of the files.

        File victim = null;
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith("log.")) {
                continue;
            }
            if (file.getName().startsWith("_")) {
                continue;
            }
            if (file.length() <= (4096 * 4)) {
                continue;
            }
            victim = file;
            break;
        }

        System.out.println(victim);
        RandomAccessFile raf = new RandomAccessFile(victim, "rw");
        raf.seek(9000);
        for (int i=0; i<4000; i++) {
            raf.writeInt(1475716342);
        }
        raf.close();

        result = newBuilder(dir, null).verify(System.out);

        assertFalse(result);
    }

    private File makeTempDir() throws Exception {
        File dir = TestUtilities.makeTempDir("verify");
        mTempDirs.add(dir);
        return dir;
    }

    private BDBRepositoryBuilder newBuilder(File dir, String name) throws Exception {
        BDBRepositoryBuilder bob = new BDBRepositoryBuilder();
        bob.setProduct("DB");
        if (name != null) {
            bob.setName(name);
        }
        bob.setTransactionWriteNoSync(true);
        bob.setCacheSize(1000000);
        bob.setEnvironmentHomeFile(dir);
        bob.setPrivate(false);
        //bob.setChecksumEnabled(true);
        return bob;
    }
}
