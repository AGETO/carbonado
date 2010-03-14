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

package com.amazon.carbonado.spi;

import java.io.*;

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Repository;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.ByteArrayBlob;

import com.amazon.carbonado.TestUtilities;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestLobEngine extends TestCase {
    private static final long SEED = 2358127411L;

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestLobEngine.class);
    }

    private Repository mRepo;
    private LobEngine mEngine;

    @Override
    protected void setUp() throws Exception {
        mRepo = TestUtilities.buildTempRepository();
        mEngine = new LobEngine(mRepo, mRepo);
    }

    @Override
    protected void tearDown() throws Exception {
        mRepo.close();
    }

    public void testBasic() throws Exception {
        final int blockSize = 1000;

        Blob blob = mEngine.createNewBlob(blockSize);
        long locator = mEngine.getLocator(blob);
        assertNull(mEngine.getBlobValue(0));
        Blob blob2 = mEngine.createNewBlob(blockSize);
        long locator2 = mEngine.getLocator(blob2);
        assertTrue(locator2 > locator);
        mEngine.deleteLob(locator);
        try {
            mEngine.getBlobValue(locator).getLength();
            fail();
        } catch (FetchNoneException e) {
        }
        Blob blob3 = mEngine.createNewBlob(blockSize);
        long locator3 = mEngine.getLocator(blob3);
        assertTrue(locator3 > locator2);

        try {
            mEngine.setBlobValue(locator, new ByteArrayBlob(new byte[1], 0));
            fail();
        } catch (PersistNoneException e) {
        }

        byte[] buf = new byte[100];

        blob = mEngine.createNewBlob(blockSize);
        locator = mEngine.getLocator(blob);

        InputStream in = blob.openInputStream();
        assertEquals(-1, in.read());
        in.close();

        in = blob.openInputStream();
        assertTrue(in.read(buf) <= 0);
        in.close();

        in = blob.openInputStream(100);
        assertEquals(-1, in.read());
        in.close();

        in = blob.openInputStream(100);
        assertTrue(in.read(buf) <= 0);
        in.close();

        in = blob.openInputStream(5001);
        assertEquals(-1, in.read());
        in.close();

        in = blob.openInputStream(5001);
        assertTrue(in.read(buf) <= 0);
        in.close();

        mEngine.deleteLob(locator);
        try {
            mEngine.getBlobValue(locator).getLength();
            fail();
        } catch (FetchNoneException e) {
        }
    }

    public void testShortLength() throws Exception {
        testShortLength(false);
    }

    public void testShortLengthBuffered() throws Exception {
        // Using buffered streams provides a simple means to test if the read
        // into byte[] methods work.
        testShortLength(true);
    }

    private void testShortLength(boolean buffered) throws Exception {
        final int blockSize = 1000;

        Blob blob = mEngine.createNewBlob(blockSize);
        long locator = mEngine.getLocator(blob);

        Random rnd = new Random(SEED);
        OutputStream out = blob.openOutputStream();
        if (buffered) {
            out = new BufferedOutputStream(out);
        }
        for (int i=0; i<100; i++) {
            out.write(rnd.nextInt());
        }
        out.flush();
        out.close();

        assertEquals(100, blob.getLength());

        rnd = new Random(SEED);
        InputStream in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        int b;
        while ((b = in.read()) >= 0) {
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        in.close();

        blob.setLength(50);
        assertEquals(50, blob.getLength());

        rnd = new Random(SEED);
        in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        for (int i=0; i<50; i++) {
            b = in.read();
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        assertEquals(-1, in.read());
        in.close();

        blob.setLength(200);
        rnd = new Random(SEED);
        in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        for (int i=0; i<50; i++) {
            b = in.read();
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        for (int i=50; i<200; i++) {
            b = in.read();
            assertEquals(0, b);
        }
        assertEquals(-1, in.read());
        in.close();

        mEngine.deleteLob(locator);

        try {
            blob.getLength();
            fail();
        } catch (FetchNoneException e) {
        }
    }

    public void testLongLength() throws Exception {
        testLongLength(false);
    }

    public void testLongLengthBuffered() throws Exception {
        // Using buffered streams provides a simple means to test if the read
        // into byte[] methods work.
        testLongLength(true);
    }

    private void testLongLength(boolean buffered) throws Exception {
        final int blockSize = 1000;

        Blob blob = mEngine.createNewBlob(blockSize);
        long locator = mEngine.getLocator(blob);

        Random rnd = new Random(SEED);
        OutputStream out = blob.openOutputStream();
        if (buffered) {
            out = new BufferedOutputStream(out);
        }
        for (int i=0; i<123456; i++) {
            out.write(rnd.nextInt());
        }
        out.flush();
        out.close();

        assertEquals(123456, blob.getLength());

        rnd = new Random(SEED);
        InputStream in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        int b;
        while ((b = in.read()) >= 0) {
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        in.close();

        blob.setLength(12345);
        assertEquals(12345, blob.getLength());

        rnd = new Random(SEED);
        in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        for (int i=0; i<12345; i++) {
            b = in.read();
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        assertEquals(-1, in.read());
        in.close();

        blob.setLength(23456);
        rnd = new Random(SEED);
        in = blob.openInputStream();
        if (buffered) {
            in = new BufferedInputStream(in);
        }
        for (int i=0; i<12345; i++) {
            b = in.read();
            assertEquals(rnd.nextInt() & 0xff, b);
        }
        for (int i=12345; i<23456; i++) {
            b = in.read();
            assertEquals(0, b);
        }
        assertEquals(-1, in.read());
        in.close();

        mEngine.deleteLob(locator);

        try {
            blob.getLength();
            fail();
        } catch (FetchNoneException e) {
        }

        {
            Cursor<?> cursor = mRepo.storageFor(StoredLob.class).query().fetch();
            assertFalse(cursor.hasNext());
        }

        {
            Cursor<?> cursor = mRepo.storageFor(StoredLob.Block.class).query().fetch();
            assertFalse(cursor.hasNext());
        }
    }

    public void testSetValue() throws Exception {
        byte[] data = new byte[12345];
        Random rnd = new Random(SEED);
        for (int i=0; i<data.length; i++) {
            data[i] = (byte) rnd.nextInt();
        }

        Blob blob = mEngine.createNewBlob(1000);
        long locator = mEngine.getLocator(blob);
        mEngine.setBlobValue(locator, new ByteArrayBlob(data));

        assertEquals(12345, blob.getLength());

        rnd = new Random(SEED);

        InputStream in = blob.openInputStream();
        for (int i=0; i<data.length; i++) {
            int b = in.read();
            assertEquals(data[i] & 0xff, b);
        }
        assertEquals(-1, in.read());
        in.close();
    }

    public void testChaos() throws Exception {
        byte[] buf = new byte[123456];
        int bufLen = 0;

        Blob blob = mEngine.createNewBlob(1000);
        long locator = mEngine.getLocator(blob);
        mEngine.setBlobValue(locator, new ByteArrayBlob(new byte[1], 0));

        assertEquals(buf, bufLen, blob);

        byte[] temp = new byte[2000];
        Random rnd = new Random(SEED);

        for (int i=0; i<1000; i++) {
            int op = rnd.nextInt(4);
            switch (op) {
            case 0: { // change length
                int newLen = rnd.nextInt(buf.length);
                if (newLen < bufLen) {
                    Arrays.fill(buf, newLen, bufLen, (byte) 0);
                }
                bufLen = newLen;
                blob.setLength(newLen);
                break;
            }

            case 1: case 2: case 3: { // write bytes
                int pos = rnd.nextInt(buf.length);
                int amt = rnd.nextInt(temp.length) + 1;
                if (pos + amt > buf.length) {
                    amt = buf.length - pos;
                }
                fillRandom(temp, amt, rnd);
                System.arraycopy(temp, 0, buf, pos, amt);
                if (pos + amt > bufLen) {
                    bufLen = pos + amt;
                }

                OutputStream out = blob.openOutputStream(pos);
                if (rnd.nextBoolean()) {
                    // Write chunk
                    out.write(temp, 0, amt);
                } else {
                    // Write one at a time
                    for (int j=0; j<amt; j++) {
                        out.write(temp[j]);
                    }
                }
                out.close();

                break;
            }

            default:
                fail();
            }

            assertEquals(buf, bufLen, blob);
        }
    }

    private void fillRandom(byte[] buf, int length, Random rnd) {
        for (int i=0; i<length; i++) {
            buf[i] = (byte) rnd.nextInt();
        }
    }

    private void assertEquals(byte[] buf, int bufLen, Blob blob) throws Exception {
        assertEquals(bufLen, blob.getLength());
        if (bufLen > 0) {
            InputStream in = blob.openInputStream();
            for (int i=0; i<bufLen; i++) {
                assertEquals(buf[i] & 0xff, in.read());
            }
            assertEquals(-1, in.read());
            in.close();
        }
    }

    private static void dump(Blob blob) throws Exception {
        dump(blob, 0);
    }

    private static void dump(Blob blob, long pos) throws Exception {
        InputStream in = blob.openInputStream(pos);
        in = new BufferedInputStream(in);
        int b;
        while ((b = in.read()) >= 0) {
            if (b != 10 && b != 13) {
                if (b < 32 || b > 126) {
                    b = '?';
                }
            }
            System.out.print((char) b);
        }
        in.close();
        System.out.println();
    }
}
