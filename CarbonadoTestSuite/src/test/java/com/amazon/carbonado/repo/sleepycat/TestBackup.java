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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;

import com.amazon.carbonado.repo.sleepycat.*;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * Test for HotBackupCapability
 *
 * @author Olga Kuznetsova
 */
public class TestBackup extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestBackup.class);
    }

    public TestBackup(String s) {
        super(s);
    }

    private Repository mRepo;
    private Repository mBackupRepo;

    private File mDir;
    private File mBackupDir;

    private String mProduct;

    protected void createBackup() throws Exception {
        BDBRepositoryBuilder bob = new BDBRepositoryBuilder();
        bob.setProduct(mProduct);
        bob.setName("test-backup-" + mProduct);
        bob.setTransactionWriteNoSync(true);
	bob.setKeepOldLogFiles(true);
	mDir = TestUtilities.makeTempDir("test-backup-" + mProduct);
        bob.setEnvironmentHomeFile(mDir);
        mRepo = bob.build();
    }

    protected void tearDown() throws Exception 
    {  
	if (mRepo != null) {
            mRepo.close();
	    mRepo = null;
        }
	if (mDir != null) {
	    TestUtilities.deleteTempDir(mDir);
	}

	if (mBackupRepo != null) {
            mBackupRepo.close();
	    mBackupRepo = null;
        }
	if (mBackupDir != null) {
	    TestUtilities.deleteTempDir(mBackupDir);
	}
    }

    public void testBackupJE() throws Exception {
	mProduct = "JE";
	createBackup();
	mBackupDir = TestUtilities.makeTempDir("test-backup-JE-backup");
	backup();
    }

    public void testBackupCore() throws Exception {
	mProduct = "DB";
	createBackup();
	mBackupDir = TestUtilities.makeTempDir("test-backup-DB-backup");
	backup();
    }
    
    public void backup() throws Exception {
        Storage<StorableTestBasic> storage = mRepo.storageFor(StorableTestBasic.class);
	StorableTestBasic stb;

	for (int i = 0; i < 100; ++i) {
	    stb = storage.prepare();
	    stb.setId(i);
            stb.setStringProp("oldhello");
            stb.setIntProp(3);
            stb.setLongProp(22);
            stb.setDoubleProp(234.2);
	    assertTrue(stb.tryInsert());
	}

	HotBackupCapability cap = mRepo.getCapability(HotBackupCapability.class);
	if (cap == null) {
	    fail();
	}

	try {
	    HotBackupCapability.Backup b = cap.startIncrementalBackup(-1);
	    fail();
	} catch (Exception e) {
	    // expected
	}

	HotBackupCapability.Backup b = cap.startBackup();

	if (b == null) {
	    fail();
	}

	File [] files = b.getFiles();
	int fullBackupLength = 0;
	for (File f : files) {
	    fullBackupLength+=f.length();
	}
	long lastLogNumber = b.getLastLogNumber();
	b.endBackup();
	
	HotBackupCapability.Backup incrementalB = cap.startIncrementalBackup(lastLogNumber);
	File[] files2 = incrementalB.getFiles();
	int numFiles = 0;
	int noChangesLength = 0;
	for (File f : files2) {
	    noChangesLength += f.length();
	    ++numFiles;
	} 
	assertEquals(1, numFiles);
	
	// move the backup files into a new directory
	OutputStream out;
	byte[] buf = new byte[1024];
	int len;
	InputStream in;
	for (File f : files) {
	    in = new FileInputStream(f);
	    File newFile = new File(mBackupDir, f.getName());
	    OutputStream outStream = new FileOutputStream(newFile);
	    int amt;
	    while((amt = in.read(buf)) > 0) {
		outStream.write(buf, 0, amt);
	    }
	    in.close();
	    outStream.close();
	}
	
	for (int i = 100; i < 110; ++i) {
	    stb = storage.prepare();
	    stb.setId(i);
            stb.setStringProp("hello");
            stb.setIntProp(3);
            stb.setLongProp(22);
            stb.setDoubleProp(234.2);
	    assertTrue(stb.tryInsert());
	}

	for (int i = 0; i < 10; ++i) {
	    stb = storage.prepare();
	    stb.setId(i);
	    assertTrue(stb.tryLoad());
	    stb.setStringProp("randomprop");
	    assertTrue(stb.tryUpdate()); // update to check log file order
	    stb.setStringProp("newprop");
	    assertTrue(stb.tryUpdate());
	}
	incrementalB.endBackup();

	incrementalB = cap.startIncrementalBackup(lastLogNumber, true);
	File [] files1 = incrementalB.getFiles();
	int moreChangesLength = 0;
	for (File f : files1) {
	    moreChangesLength += f.length();
	}

	// Now put the incremental backup files into the backup directory
	for (File f : files1) {
	    in = new FileInputStream(f);
	    File newFile = new File(mBackupDir, f.getName());
	    OutputStream outStream = new FileOutputStream(newFile);
	    int amt;
	    while((amt = in.read(buf)) > 0) {
		outStream.write(buf, 0, amt);
	    }
	    in.close();
	    outStream.close();
	    assertEquals(f.length(), newFile.length());
	}
	incrementalB.endBackup();

	BDBRepositoryBuilder bob = new BDBRepositoryBuilder();
        bob.setProduct(mProduct);
        bob.setName("test-backup" + mProduct);
        bob.setCacheSize(100000);
        bob.setEnvironmentHomeFile(mBackupDir);
	bob.setRunFullRecovery(true);
        mBackupRepo = bob.build();
	Storage<StorableTestBasic> backupStorage = mBackupRepo.storageFor(StorableTestBasic.class);
	for (int i = 0; i < 10; ++i) {
	    StorableTestBasic s = backupStorage.prepare();
	    s.setId(i);
	    assertTrue(s.tryLoad());
	    assertEquals("newprop", s.getStringProp());
	}

	for (int i = 100; i < 110; ++i) {
	    StorableTestBasic s = backupStorage.prepare();
	    s.setId(i);
	    assertTrue(s.tryLoad());
	}
    }
}
