/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

/**
 * TestUtilities
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class TestUtilities {
    public static final String FILE_PATH_KEY = "filepath";

    // Keep memory usage low to prevent spurious out-of-memory errors while running tests.
    private static final int DEFAULT_CAPACITY = 100000;

    private static final Random sRandom = new Random();

    private static final Set<File> cTempFiles = Collections.synchronizedSet(new HashSet<File>());

    public static String makeTestDirectoryString(String nameElement) {
        return makeTestDirectory(nameElement).getAbsolutePath();
    }

    public static File makeTestDirectory(String nameElement) {
        String dirPath = System.getProperty(FILE_PATH_KEY);
        File dir;
        if (dirPath == null) {
            dir = new File(new File(System.getProperty("java.io.tmpdir")), "carbonado-test");
        } else {
            dir = new File(dirPath);
        }
        String path = nameElement + '-' + System.currentTimeMillis() + '-' + sRandom.nextInt(1000);
        File file = new File(dir, path);
        try {
            // For better error reporting
            file = file.getCanonicalFile();
        } catch (IOException e) {
            // Ignore.
        }
        // BDBRepositoryBuilder will make directory if it doesn't exist.
        return file;
    }

    public static Repository buildTempRepository() {
        return buildTempRepository("test");
    }

    public static Repository buildTempRepository(boolean isMaster) {
        return buildTempRepository("test", isMaster);
    }

    public static Repository buildTempRepository(String name) {
        return buildTempRepository(name, DEFAULT_CAPACITY);
    }

    public static Repository buildTempRepository(String name, boolean isMaster) {
        return buildTempRepository(name, DEFAULT_CAPACITY, isMaster);
    }

    public static Repository buildTempRepository(String name, int capacity) {
        return buildTempRepository(name, capacity, true);
    }

    public static Repository buildTempRepository(String name, int capacity, boolean isMaster) {
        RepositoryBuilder builder = newTempRepositoryBuilder(name, capacity, isMaster);

        try {
            return builder.build();
        } catch (RepositoryException e) {
            throw new UnsupportedOperationException("Could not create repository", e);
        }
    }

    public static RepositoryBuilder newTempRepositoryBuilder() {
        return newTempRepositoryBuilder("test");
    }

    public static RepositoryBuilder newTempRepositoryBuilder(boolean isMaster) {
        return newTempRepositoryBuilder("test", isMaster);
    }

    public static RepositoryBuilder newTempRepositoryBuilder(String name) {
        return newTempRepositoryBuilder(name, DEFAULT_CAPACITY);
    }

    public static RepositoryBuilder newTempRepositoryBuilder(String name, boolean isMaster) {
        return newTempRepositoryBuilder(name, DEFAULT_CAPACITY, isMaster);
    }

    public static RepositoryBuilder newTempRepositoryBuilder(String name, int capacity) {
        return newTempRepositoryBuilder(name, capacity, true);
    }

    public static RepositoryBuilder newTempRepositoryBuilder(String name,
                                                             int capacity,
                                                             boolean isMaster)
    {
        BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
        builder.setProduct("JE");
        builder.setName(name);
        builder.setTransactionNoSync(true);
        builder.setCacheSize(capacity);
        builder.setLogInMemory(true);
        builder.setMaster(isMaster);
        builder.setEnvironmentHome(makeTestDirectoryString(name));
        // Makes it easier to get a thread dump during a deadlock.
        builder.setLockTimeout(10);

        return builder;
    }

    private static String sAlphabet = "abcdefghijklmnopqrstuvwxyz";
    private static Random sNumbers = new Random();

    public static void sAddRandomWord(StringBuffer buffer, int aCount) {
        sAddRandomWord(buffer, aCount, sNumbers);
    }

    public static void sAddRandomWord(StringBuffer buffer, int aCount, Random rnd) {
        int position = rnd.nextInt(sAlphabet.length());
        char c = sAlphabet.charAt(position);
        for (int i = 0; i < aCount; i++) {
            buffer.append(c);
        }
    }

    public static String sRandomText(int aMin, int aMax) {
        return sRandomText(aMin, aMax, sNumbers);
    }

    public static String sRandomText(int aMin, int aMax, Random rnd) {
        return sRandomText(rnd.nextInt(aMax-aMin) + aMin, rnd);
    }

    public static String sRandomText(int aCount) {
        return sRandomText(aCount, sNumbers);
    }

    public static String sRandomText(int aCount, Random rnd) {
        if (aCount <= 0) {
            return "";
        }

        final StringBuffer buffer = new StringBuffer(aCount);
        int filled = 0;
        while (filled<aCount) {
            int wordsize = rnd.nextInt(Math.min(10, aCount - filled)) + 1;
            sAddRandomWord(buffer, wordsize, rnd);
            filled += wordsize;
            if (filled<aCount) {
                buffer.append(' ');
                filled++;
            }
        }
        return buffer.toString();
    }

    public static File makeTempDir(String prefix) throws IOException {
        File temp;
        do {
            temp = new File(System.getProperty("java.io.tmpdir"),
                            prefix + '-' + UUID.randomUUID());
        } while (temp.exists());
        if (!temp.mkdir()) {
            throw new IOException("Couldn't create temp directory: " + temp);
        }
        cTempFiles.add(temp);
        return temp;
    }

    public static void deleteTempDir(File file) {
        if (!cTempFiles.remove(file)) {
            // Was not registered, so leave it alone.
            return;
        }
        recursiveDelete(file);
    }

    private static void recursiveDelete(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                recursiveDelete(f);
            }
        }
        if (!file.delete()) {
            System.err.println("Couldn't delete file: " + file);
        }
    }
}
