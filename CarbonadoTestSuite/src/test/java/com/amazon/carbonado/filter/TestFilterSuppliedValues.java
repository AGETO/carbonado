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

package com.amazon.carbonado.filter;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.stored.Order;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestFilterSuppliedValues extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilterSuppliedValues.class);
    }

    public TestFilterSuppliedValues(String name) {
        super(name);
    }

    public void testSuppliedValuesFor() {
        Filter<Order> open = Filter.getOpenFilter(Order.class);
        Filter<Order> f1 = open.and("orderTotal", RelOp.EQ);
        Filter<Order> f2 = open.and("orderNumber", RelOp.EQ, "20");

        Filter<Order> f = f1.and(f2).bind();
        FilterValues<Order> fv = f.initialFilterValues();

        Object[] values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        values = fv.with(10).getSuppliedValuesFor(f);
        assertEquals(1, values.length);
        assertEquals(new Integer(10), values[0]);

        // Again, with reverse arrangement.
        f = f2.and(f1).bind();
        fv = f.initialFilterValues();

        values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        values = fv.with(10).getSuppliedValuesFor(f);
        assertEquals(1, values.length);
        assertEquals(new Integer(10), values[0]);

        // Now test with some values set and some blank.
        Filter<Order> f3 = open.and("orderComments", RelOp.GT);

        f = f1.and(f2).and(f3).bind();
        fv = f.initialFilterValues();

        values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        values = fv.with(10).getSuppliedValuesFor(f);
        assertEquals(1, values.length);
        assertEquals(new Integer(10), values[0]);

        values = fv.with(10).with("hello").getSuppliedValuesFor(f);
        assertEquals(2, values.length);
        assertEquals(new Integer(10), values[0]);
        assertEquals("hello", values[1]);

        // Arrange differently.
        f = f2.and(f1).and(f3).bind();
        fv = f.initialFilterValues();

        values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        values = fv.with(10).getSuppliedValuesFor(f);
        assertEquals(1, values.length);
        assertEquals(new Integer(10), values[0]);

        values = fv.with(10).with("hello").getSuppliedValuesFor(f);
        assertEquals(2, values.length);
        assertEquals(new Integer(10), values[0]);
        assertEquals("hello", values[1]);

        // Arrange differently again.
        f = f3.and(f1).and(f2).bind();
        fv = f.initialFilterValues();

        values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        values = fv.with("hello").getSuppliedValuesFor(f);
        assertEquals(1, values.length);
        assertEquals("hello", values[0]);

        values = fv.with("hello").with(5).getSuppliedValuesFor(f);
        assertEquals(2, values.length);
        assertEquals("hello", values[0]);
        assertEquals(new Integer(5), values[1]);
    }

    public void testSuppliedValuesFor_multipleReferences() {
        // Test with property filter duplication.
        Filter<Order> oneProp = Filter.filterFor(Order.class, "orderTotal != ?");
        oneProp = oneProp.bind();

        // Note that reduce is not called on the filter, as it would reduce the
        // filter back to one term.
        Filter<Order> f = oneProp.and(oneProp);

        // Note that values are only for oneProp.
        FilterValues<Order> fv = oneProp.initialFilterValues();

        Object[] values = fv.getSuppliedValuesFor(f);
        assertEquals(0, values.length);

        // Supply one actual value...
        values = fv.with(10).getSuppliedValuesFor(f);
        // ...which is duplicated as required by f.
        assertEquals(2, values.length);
        assertEquals(new Integer(10), values[0]);
        assertEquals(new Integer(10), values[1]);
    }
}
