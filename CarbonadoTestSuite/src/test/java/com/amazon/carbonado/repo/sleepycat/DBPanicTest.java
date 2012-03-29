/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
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

import java.io.FileWriter;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * Tests the panic handler with BDB-Core.
 * 
 * @author Jesse Morgan
 *
 */
public class DBPanicTest extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(DBPanicTest.class);
    }

    public DBPanicTest(String s) {
        super(s);
    }

    private Repository mRepo;
    private String mEnvHome;
    private volatile boolean mPanicked;
    private volatile boolean mTestDone;

    protected void setUp() throws Exception {
        BDBRepositoryBuilder bob = new BDBRepositoryBuilder();
        bob.setProduct("DB");
        bob.setName("test");
        bob.setTransactionNoSync(false);
        mEnvHome = TestUtilities.makeTestDirectoryString("test");
        bob.setEnvironmentHome(mEnvHome);
        bob.setLockTimeout(5.0);
        bob.setCacheSize(100000);
        bob.setPanicHandler(new TestPanicHandler());
        mRepo = bob.build();
        mPanicked = false;
        mTestDone = false;
    }

    protected void tearDown() throws Exception {
        if (mRepo != null) {
            try {
                mRepo.close();
                                
            } catch (Exception e) {
                                
            }
        }
        mRepo = null;
    }

    /**
     * Test the panic handler as follows:
     *  - Open the repository
     *  - In a new thread, write random data to the repository
     *  - In the main thread, perform reads and writes
     *  - Fail if 1000 reads/writes succeed
     *  - Succeed if panic handler fires
     */
    public void testPanicHandler() throws Exception {
        Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);
        StorableTestBasic stb;
        
        Thread destroyer = new Thread(new Runnable() {
                public void run() {
                    try {
                        FileWriter fwriter = new FileWriter(mEnvHome + "/__db.001");
                                        
                        while (!mPanicked && !mTestDone) {
                            fwriter.append('0');
                            fwriter.flush();
                        }
                                        
                        fwriter.close();
                                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        
        
        try {
            for (int i = 0; i < 1000; i++) {
                stb = storage.prepare();
                stb.initPropertiesRandomly(i);
                stb.insert();
            }
                
            destroyer.start();
            Thread.sleep(1000);
                
            Cursor<StorableTestBasic> c = storage.query().fetch();
                
            int i = 0;
            while (!mPanicked && c.hasNext()) {
                stb = c.next();
                assertEquals(stb.getId(), i);
                i++;
            }
                
        } catch (Exception e) {
            // We expect exceptions to occur
        }
        
        mTestDone = true;
        assertTrue("Panic Handler did not fire", mPanicked);
    }
        
    private class TestPanicHandler implements BDBPanicHandler {
        public void onPanic(Object environment, Exception exception) {
            mPanicked = true;
        }               
    }
}
