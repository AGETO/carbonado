/*
 * Copyright 2009 Amazon Technologies, Inc. or its affiliates.
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
public class TestNotSerializable extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestNotSerializable.class);
    }

    private Repository mRepository;

    public TestNotSerializable(String name) {
        super(name);
    }

    protected void setUp() {
        mRepository = new ToyRepository();
    }

    protected void tearDown() {
        mRepository.close();
        mRepository = null;
    }

    public void testWriteTo() throws Exception {
        Storage<Record> g = mRepository.storageFor(Record.class);
        Record rec = g.prepare();
        try {
            rec.writeTo(new ByteArrayOutputStream());
            fail();
        } catch (SupportException e) {
        }
    }

    public void testReadFrom() throws Exception {
        Storage<Record> g = mRepository.storageFor(Record.class);
        Record rec = g.prepare();
        try {
            rec.readFrom(new ByteArrayInputStream(new byte[0]));
            fail();
        } catch (SupportException e) {
        }
    }

    @PrimaryKey("id")
    public static abstract class Record implements Storable {
        public abstract int getId();
        public abstract void setId(int id);

        public abstract InputStream getInput();
        public abstract void setInput(InputStream in);
    }
}
