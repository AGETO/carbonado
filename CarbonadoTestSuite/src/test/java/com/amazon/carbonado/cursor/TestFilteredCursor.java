/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.cursor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.filter.*;

import com.amazon.carbonado.repo.toy.ToyRepository;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestFilteredCursor extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilteredCursor.class);
    }

    public TestFilteredCursor(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void testFloat() throws Exception {
        // Tests that float values are compared properly because the
        // implementation compares against float bits.

        Repository repo = new ToyRepository();
        Storage<FloatRecord> storage = repo.storageFor(FloatRecord.class);

        float[] floats = {
            0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f,
            0.0f/0.0f, 1.0f/0.0f, -1.0f/0.0f
        };

        for (int i=0; i<floats.length; i++) {
            FloatRecord rec = storage.prepare();
            rec.setID(i);
            rec.setFloatValue(floats[i]);
            rec.insert();
        }

        Filter<FloatRecord> filter = Filter.filterFor(FloatRecord.class, "floatValue < ?").bind();

        testFloatFilter(storage, filter, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.0f, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.1f, 5.0f, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 0.0f/0.0f,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, 1.0f/0.0f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 1.0f/0.0f,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -1.0f/0.0f);
    }

    private void testFloatFilter(Storage<FloatRecord> storage, Filter<FloatRecord> filter,
                                 float filterValue, float... expected)
        throws Exception
    {
        Cursor<FloatRecord> all = storage.query().fetch();

        Cursor<FloatRecord> filtered = FilteredCursor
            .applyFilter(filter, filter.initialFilterValues().with(filterValue), all);

        List<FloatRecord> records = filtered.toList();

        float[] actual = new float[records.size()];
        for (int i=0; i<actual.length; i++) {
            actual[i] = records.get(i).getFloatValue();
        }

        Arrays.sort(actual);

        for (float f : expected) {
            int result = Arrays.binarySearch(actual, f);
            if (result < 0) {
                fail("Expected " + f);
                return;
            }
        }

        assertEquals(expected.length, records.size());
    }

    public void testFloatObj() throws Exception {
        // Tests that float values are compared properly because the
        // implementation compares against float bits.

        Repository repo = new ToyRepository();
        Storage<FloatObjRecord> storage = repo.storageFor(FloatObjRecord.class);

        Float[] floats = {
            0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f,
            0.0f/0.0f, 1.0f/0.0f, -1.0f/0.0f, null
        };

        for (int i=0; i<floats.length; i++) {
            FloatObjRecord rec = storage.prepare();
            rec.setID(i);
            rec.setFloatValue(floats[i]);
            rec.insert();
        }

        Filter<FloatObjRecord> filter = Filter
            .filterFor(FloatObjRecord.class, "floatValue < ?").bind();

        testFloatFilter(storage, filter, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.0f, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.1f, 5.0f, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 0.0f/0.0f,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, 1.0f/0.0f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 1.0f/0.0f,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -1.0f/0.0f);
        testFloatFilter(storage, filter, null,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f,
                        0.0f/0.0f, 1.0f/0.0f, -1.0f/0.0f);

        filter = Filter.filterFor(FloatObjRecord.class, "floatValue <= ?").bind();

        testFloatFilter(storage, filter, 0.0f, 0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -0.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.0f, 0.0f, 5.0f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 5.1f,
                        5.0f, 0.0f, 5.1f, -0.0f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.1f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -10.2f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 0.0f/0.0f,
                        0.0f/0.0f, 0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, 1.0f/0.0f, -1.0f/0.0f);
        testFloatFilter(storage, filter, 1.0f/0.0f,
                        1.0f/0.0f, 0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f, -1.0f/0.0f);
        testFloatFilter(storage, filter, -1.0f/0.0f, -1.0f/0.0f);
        testFloatFilter(storage, filter, null,
                        0.0f, -0.0f, 5.0f, 5.1f, -10.1f, -10.2f,
                        0.0f/0.0f, 1.0f/0.0f, -1.0f/0.0f, null);
    }

    private void testFloatFilter(Storage<FloatObjRecord> storage, Filter<FloatObjRecord> filter,
                                 Float filterValue, Float... expected)
        throws Exception
    {
        Cursor<FloatObjRecord> all = storage.query().fetch();

        Cursor<FloatObjRecord> filtered = FilteredCursor
            .applyFilter(filter, filter.initialFilterValues().with(filterValue), all);

        List<FloatObjRecord> records = filtered.toList();
        int actualCount = records.size();

        boolean hadNull = false;
        Iterator<FloatObjRecord> it = records.iterator();
        while (it.hasNext()) {
            FloatObjRecord rec = it.next();
            if (rec.getFloatValue() == null) {
                it.remove();
                hadNull = true;
            }
        }

        Float[] actual = new Float[records.size()];
        for (int i=0; i<actual.length; i++) {
            actual[i] = records.get(i).getFloatValue();
        }

        Arrays.sort(actual);

        for (Float f : expected) {
            if (f == null) {
                if (!hadNull) {
                    fail("Expected " + f);
                    return;
                }
            } else {
                int result = Arrays.binarySearch(actual, f);
                if (result < 0) {
                    fail("Expected " + f);
                    return;
                }
            }
        }

        assertEquals(expected.length, actualCount);
    }

    public void testDouble() throws Exception {
        // Tests that double values are compared properly because the
        // implementation compares against double bits.

        Repository repo = new ToyRepository();
        Storage<DoubleRecord> storage = repo.storageFor(DoubleRecord.class);

        double[] doubles = {
            0.0, -0.0, 5.0, 5.1, -10.1, -10.2,
            0.0/0.0, 1.0/0.0, -1.0/0.0
        };

        for (int i=0; i<doubles.length; i++) {
            DoubleRecord rec = storage.prepare();
            rec.setID(i);
            rec.setDoubleValue(doubles[i]);
            rec.insert();
        }

        Filter<DoubleRecord> filter = Filter
            .filterFor(DoubleRecord.class, "doubleValue < ?").bind();

        testDoubleFilter(storage, filter, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.0, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.1, 5.0, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 0.0/0.0,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2, 1.0/0.0, -1.0/0.0);
        testDoubleFilter(storage, filter, 1.0/0.0,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -1.0/0.0);
    }

    private void testDoubleFilter(Storage<DoubleRecord> storage, Filter<DoubleRecord> filter,
                                  double filterValue, double... expected)
        throws Exception
    {
        Cursor<DoubleRecord> all = storage.query().fetch();

        Cursor<DoubleRecord> filtered = FilteredCursor
            .applyFilter(filter, filter.initialFilterValues().with(filterValue), all);

        List<DoubleRecord> records = filtered.toList();

        double[] actual = new double[records.size()];
        for (int i=0; i<actual.length; i++) {
            actual[i] = records.get(i).getDoubleValue();
        }

        Arrays.sort(actual);

        for (double f : expected) {
            int result = Arrays.binarySearch(actual, f);
            if (result < 0) {
                fail("Expected " + f);
                return;
            }
        }

        assertEquals(expected.length, records.size());
    }

    public void testDoubleObj() throws Exception {
        // Tests that double values are compared properly because the
        // implementation compares against double bits.

        Repository repo = new ToyRepository();
        Storage<DoubleObjRecord> storage = repo.storageFor(DoubleObjRecord.class);

        Double[] doubles = {
            0.0, -0.0, 5.0, 5.1, -10.1, -10.2,
            0.0/0.0, 1.0/0.0, -1.0/0.0, null
        };

        for (int i=0; i<doubles.length; i++) {
            DoubleObjRecord rec = storage.prepare();
            rec.setID(i);
            rec.setDoubleValue(doubles[i]);
            rec.insert();
        }

        Filter<DoubleObjRecord> filter = Filter
            .filterFor(DoubleObjRecord.class, "doubleValue < ?").bind();

        testDoubleFilter(storage, filter, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.0, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.1, 5.0, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 0.0/0.0,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2, 1.0/0.0, -1.0/0.0);
        testDoubleFilter(storage, filter, 1.0/0.0,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -1.0/0.0);
        testDoubleFilter(storage, filter, null,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2,
                         0.0/0.0, 1.0/0.0, -1.0/0.0);

        filter = Filter.filterFor(DoubleObjRecord.class, "doubleValue <= ?").bind();

        testDoubleFilter(storage, filter, 0.0, 0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -0.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.0, 0.0, 5.0, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 5.1, 5.0, 0.0, 5.1, -0.0, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.1, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -10.2, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, 0.0/0.0,
                         0.0/0.0, 0.0, -0.0, 5.0, 5.1, -10.1, -10.2, 1.0/0.0, -1.0/0.0);
        testDoubleFilter(storage, filter, 1.0/0.0,
                         1.0/0.0, 0.0, -0.0, 5.0, 5.1, -10.1, -10.2, -1.0/0.0);
        testDoubleFilter(storage, filter, -1.0/0.0, -1.0/0.0);
        testDoubleFilter(storage, filter, null,
                         0.0, -0.0, 5.0, 5.1, -10.1, -10.2,
                         0.0/0.0, 1.0/0.0, -1.0/0.0, null);
    }

    private void testDoubleFilter(Storage<DoubleObjRecord> storage, Filter<DoubleObjRecord> filter,
                                  Double filterValue, Double... expected)
        throws Exception
    {
        Cursor<DoubleObjRecord> all = storage.query().fetch();

        Cursor<DoubleObjRecord> filtered = FilteredCursor
            .applyFilter(filter, filter.initialFilterValues().with(filterValue), all);

        List<DoubleObjRecord> records = filtered.toList();
        int actualCount = records.size();

        boolean hadNull = false;
        Iterator<DoubleObjRecord> it = records.iterator();
        while (it.hasNext()) {
            DoubleObjRecord rec = it.next();
            if (rec.getDoubleValue() == null) {
                it.remove();
                hadNull = true;
            }
        }

        Double[] actual = new Double[records.size()];
        for (int i=0; i<actual.length; i++) {
            actual[i] = records.get(i).getDoubleValue();
        }

        Arrays.sort(actual);

        for (Double f : expected) {
            if (f == null) {
                if (!hadNull) {
                    fail("Expected " + f);
                    return;
                }
            } else {
                int result = Arrays.binarySearch(actual, f);
                if (result < 0) {
                    fail("Expected " + f);
                    return;
                }
            }
        }

        assertEquals(expected.length, actualCount);
    }

    @PrimaryKey("ID")
    public static interface FloatRecord extends Storable {
        int getID();
        void setID(int id);

        float getFloatValue();
        void setFloatValue(float value);
    }

    @PrimaryKey("ID")
    public static interface FloatObjRecord extends Storable {
        int getID();
        void setID(int id);

        @Nullable
        Float getFloatValue();
        void setFloatValue(Float value);
    }

    @PrimaryKey("ID")
    public static interface DoubleRecord extends Storable {
        int getID();
        void setID(int id);

        double getDoubleValue();
        void setDoubleValue(double value);
    }

    @PrimaryKey("ID")
    public static interface DoubleObjRecord extends Storable {
        int getID();
        void setID(int id);

        @Nullable
        Double getDoubleValue();
        void setDoubleValue(Double value);
    }
}
