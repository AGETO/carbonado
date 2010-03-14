/*
 * Copyright 2007-2010 Amazon Technologies, Inc. or its affiliates.
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

import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.stored.Address;
import com.amazon.carbonado.stored.Order;
import com.amazon.carbonado.stored.Shipment;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestFilterSplit extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilterSplit.class);
    }

    public TestFilterSplit(String name) {
        super(name);
    }

    public void testOpen() {
        Filter<Address> filter = Filter.getOpenFilter(Address.class);

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }
    }

    public void testClosed() {
        Filter<Address> filter = Filter.getClosedFilter(Address.class);

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }
    }

    public void testSimple() {
        Filter<Address> filter = Filter.filterFor(Address.class, "addressCity = ?");

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }
    }

    public void testSimpleOr() {
        Filter<Address> filter = Filter.filterFor
            (Address.class, "addressCity = ? | addressZip = ?").bind();

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor(Address.class, "addressCity = ?").bind(), split.get(0));
            assertEquals(Filter.filterFor(Address.class, "addressZip = ?").bind(), split.get(1));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }
    }

    public void testSimpleAnd() {
        Filter<Address> filter = Filter.filterFor
            (Address.class, "addressCity = ? & addressZip = ?").bind();

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(1, split.size());
            assertEquals(filter, split.get(0));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor(Address.class, "addressCity = ?").bind(), split.get(0));
            assertEquals(Filter.filterFor(Address.class, "addressZip = ?").bind(), split.get(1));
        }
    }

    public void testMixed() {
        Filter<Address> filter;

        filter = Filter.filterFor
            (Address.class, "(addressCity = ? & addressZip = ?) | addressState = ?").bind();

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor
                         (Address.class, "addressCity = ? & addressZip = ?").bind(),
                         split.get(0));
            assertEquals(Filter.filterFor(Address.class, "addressState = ?").bind(), split.get(1));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor
                         (Address.class, "addressCity = ? | addressState = ?").bind(),
                         split.get(0));
            assertEquals(Filter.filterFor
                         (Address.class, "addressZip = ? | addressState = ?").bind(),
                         split.get(1));
        }

        filter = Filter.filterFor
            (Address.class, "addressCity = ? & (addressZip = ? | addressState = ?)").bind();

        {
            List<Filter<Address>> split = filter.disjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor
                         (Address.class, "addressCity = ? & addressZip = ?").bind(),
                         split.get(0));
            assertEquals(Filter.filterFor
                         (Address.class, "addressCity = ? & addressState = ?").bind(),
                         split.get(1));
        }

        {
            List<Filter<Address>> split = filter.conjunctiveNormalFormSplit();
            assertEquals(2, split.size());
            assertEquals(Filter.filterFor(Address.class, "addressCity = ?").bind(), split.get(0));
            assertEquals(Filter.filterFor
                         (Address.class, "addressZip = ? | addressState = ?").bind(),
                         split.get(1));
        }
    }
}
