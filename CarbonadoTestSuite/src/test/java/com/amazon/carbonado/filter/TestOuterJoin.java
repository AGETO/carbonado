/*
 * Copyright 2007-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.MalformedFilterException;
import com.amazon.carbonado.stored.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestOuterJoin extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestOuterJoin.class);
    }

    public TestOuterJoin(String name) {
        super(name);
    }

    public void test_prime() {
        Filter<Order> filter = Filter.filterFor(Order.class, "orderID = ?");
        Filter<Order> filter2 = Filter.filterFor(Order.class, "orderID != ?");

        assertTrue(filter2 == filter.not());

        assertFalse(((PropertyFilter) filter).getChainedProperty().isOuterJoin(0));
        assertFalse(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(0));

        try {
            filter = Filter.filterFor(Order.class, "(orderID) = ?");
            fail();
        } catch (MalformedFilterException e) {
        }

        filter = Filter.filterFor(Order.class, "(orderID = ?)");
        filter2 = Filter.filterFor(Order.class, "(orderID != ?)");

        assertTrue(filter2 == filter.not());

        filter = Filter.filterFor(Order.class, "((orderID = ?))");
        assertTrue(filter2 == filter.not());

        try {
            filter = Filter.filterFor(Order.class, "((orderID) = ?)");
            fail();
        } catch (MalformedFilterException e) {
        }

        filter = Filter.filterFor(Order.class, "!orderID = ?");
        assertTrue(filter2 == filter);

        filter = Filter.filterFor(Order.class, "!(orderID = ?)");
        assertTrue(filter2 == filter);

        filter = Filter.filterFor(Order.class, "((((!((orderID = ?))))))");
        assertTrue(filter2 == filter);

        try {
            filter = Filter.filterFor(Order.class, "!(orderID) = ?");
            fail();
        } catch (MalformedFilterException e) {
        }
    }

    public void test_shortChain() {
        Filter<Order> filter = Filter.filterFor(Order.class, "address.addressCity > ?");
        Filter<Order> filter2 = Filter.filterFor(Order.class, "(address).addressCity <= ?");

        assertTrue(filter2 == filter.not());

        assertFalse(((PropertyFilter) filter).getChainedProperty().isOuterJoin(0));
        assertFalse(((PropertyFilter) filter).getChainedProperty().isOuterJoin(1));
        assertTrue(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(0));
        assertFalse(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(1));

        try {
            filter = Filter.filterFor(Order.class, "address.(addressCity) > ?");
            fail();
        } catch (MalformedFilterException e) {
        }

        try {
            filter = Filter.filterFor(Order.class, "(address).(addressCity) > ?");
            fail();
        } catch (MalformedFilterException e) {
        }

        filter = Filter.filterFor(Order.class, "(address.addressCity > ?)");
        assertTrue(filter2 == filter.not());

        filter = Filter.filterFor(Order.class, "((address.addressCity > ?))");
        assertTrue(filter2 == filter.not());

        filter2 = Filter.filterFor(Order.class, "((address).addressCity <= ?)");
        assertTrue(filter2 == filter.not());

        filter2 = Filter.filterFor(Order.class, "(((address).addressCity <= ?))");
        assertTrue(filter2 == filter.not());

        filter = Filter.filterFor(Order.class, "!(address).addressCity <= ?");
        assertTrue(filter2 == filter.not());

        filter = Filter.filterFor(Order.class, "!((address).addressCity <= ?)");
        assertTrue(filter2 == filter.not());
    }

    public void test_longChain() {
        Filter<OrderItem> filter = Filter.filterFor
            (OrderItem.class, "shipment.(order).(address).addressState = ?");
        Filter<OrderItem> filter2 = Filter.filterFor
            (OrderItem.class, "(shipment).order.address.addressState != ?");

        assertTrue(filter2 == filter.not());

        assertFalse(((PropertyFilter) filter).getChainedProperty().isOuterJoin(0));
        assertTrue(((PropertyFilter) filter).getChainedProperty().isOuterJoin(1));
        assertTrue(((PropertyFilter) filter).getChainedProperty().isOuterJoin(2));
        assertFalse(((PropertyFilter) filter).getChainedProperty().isOuterJoin(3));

        assertTrue(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(0));
        assertFalse(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(1));
        assertFalse(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(2));
        assertFalse(((PropertyFilter) filter2).getChainedProperty().isOuterJoin(3));
    }

    public void test_complex() {
        Filter<OrderItem> filter = Filter.filterFor
            (OrderItem.class,
             "shipment.(order).(address).addressState = ? & " +
             "(shipment).(order).address.addressState > ?");

        Filter<OrderItem> filter2 = Filter.filterFor
            (OrderItem.class,
             "(shipment).order.address.addressState != ? | " +
             "shipment.order.(address).addressState <= ?");

        assertTrue(filter2 == filter.not());
    }
}
