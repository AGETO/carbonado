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

package com.amazon.carbonado.gen;

import java.io.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;

import com.amazon.carbonado.repo.toy.ToyRepository;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestFinalMethods extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFinalMethods.class);
    }

    private Repository mRepository;

    public TestFinalMethods(String name) {
        super(name);
    }

    protected void setUp() {
        mRepository = new ToyRepository();
    }

    protected void tearDown() {
        mRepository.close();
        mRepository = null;
    }

    public void testLoad() throws Exception {
        Storage<Record> g = mRepository.storageFor(Record.class);
        Record rec = g.prepare();
        rec.setId(1);
        rec.insert();
        // Works because override was not declared final.
        rec.load();
    }

    public void testToString() throws Exception {
        Storage<Record> g = mRepository.storageFor(Record.class);
        Record rec = g.prepare();
        assertEquals("hello", rec.toString());
    }

    public void testWriteTo() throws Exception {
        Storage<Record> g = mRepository.storageFor(Record.class);
        Record rec = g.prepare();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        rec.writeTo(out);
        byte[] bytes = out.toByteArray();
        assertEquals("hello", new String(bytes));
    }

    @PrimaryKey("id")
    public static abstract class Record implements Storable {
        public abstract int getId();
        public abstract void setId(int id);

        @Override
        public void load() throws FetchException {
            throw null;
        }

        @Override
        public final void writeTo(OutputStream out) throws IOException {
            out.write("hello".getBytes());
        }

        @Override
        public final String toString() {
            return "hello";
        }
    }
}
