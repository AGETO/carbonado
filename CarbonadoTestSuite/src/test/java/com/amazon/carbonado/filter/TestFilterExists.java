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
public class TestFilterExists extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilterExists.class);
    }

    public TestFilterExists(String name) {
        super(name);
    }

    public void testParsing() {
        {
            Filter<Order> f1 = Filter.filterFor(Order.class, "shipments()");
            assertTrue(f1 instanceof ExistsFilter);
            assertEquals("shipments", ((ExistsFilter) f1).getChainedProperty().toString());
            assertTrue(((ExistsFilter) f1).getSubFilter().isOpen());
        }

        {
            Filter<Order> f1 = Filter.filterFor(Order.class, "orderTotal = ? & shipments()");
            assertTrue(f1 instanceof AndFilter);
            Filter left = ((AndFilter) f1).getLeftFilter();
            Filter right = ((AndFilter) f1).getRightFilter();
            assertTrue(Filter.filterFor(Order.class, "orderTotal = ?") == left);
            assertTrue(Filter.filterFor(Order.class, "shipments()") == right);
        }

        {
            Filter<Order> f1 = Filter.filterFor(Order.class, "shipments() | orderTotal = ?");
            assertTrue(f1 instanceof OrFilter);
            Filter left = ((OrFilter) f1).getLeftFilter();
            Filter right = ((OrFilter) f1).getRightFilter();
            assertTrue(Filter.filterFor(Order.class, "shipments()") == left);
            assertTrue(Filter.filterFor(Order.class, "orderTotal = ?") == right);
        }

        {
            Filter<Order> f1 = Filter.filterFor(Order.class, "shipments(shipmentNotes=?)");
            assertTrue(f1 instanceof ExistsFilter);
            assertEquals("shipments", ((ExistsFilter) f1).getChainedProperty().toString());
            assertTrue(Filter.filterFor(Shipment.class, "shipmentNotes = ?")
                       == ((ExistsFilter) f1).getSubFilter());
        }

        {
            Filter<Order> f1 = Filter.filterFor(Order.class, "shipments(orderItems(itemPrice=?))");
            assertTrue(f1 instanceof ExistsFilter);
            assertEquals("shipments", ((ExistsFilter) f1).getChainedProperty().toString());
            Filter sub = ((ExistsFilter) f1).getSubFilter();
            assertTrue(sub instanceof ExistsFilter);
            assertTrue(Filter.filterFor(OrderItem.class, "itemPrice = ?")
                       == ((ExistsFilter) sub).getSubFilter());
        }

        {
            Filter<Shipment> f1 = Filter.filterFor
                (Shipment.class, "order.shipments(orderItems(itemPrice=?))");
            assertTrue(f1 instanceof ExistsFilter);
            assertEquals("order.shipments", ((ExistsFilter) f1).getChainedProperty().toString());
            Filter sub = ((ExistsFilter) f1).getSubFilter();
            assertTrue(sub instanceof ExistsFilter);
            assertTrue(Filter.filterFor(OrderItem.class, "itemPrice = ?")
                       == ((ExistsFilter) sub).getSubFilter());
        }
    }

    public void testParseErrors() {
        try {
            Filter.filterFor(Order.class, "address()");
            fail();
        } catch (MalformedFilterException e) {
        }

        try {
            Filter.filterFor(Order.class, "shipments");
            fail();
        } catch (MalformedFilterException e) {
        }

        try {
            Filter.filterFor(Order.class, "shipments(");
            fail();
        } catch (MalformedFilterException e) {
        }

        try {
            Filter.filterFor(Order.class, "shipments(orderTotal=?)");
            fail();
        } catch (MalformedFilterException e) {
        }
    }

    public void testVisitor() {
        Filter<Order> f1 = Filter.filterFor
            (Order.class, "orderTotal = ? & shipments(orderItems(itemPrice=?)) | orderTotal < ?");
        Filter<Order> f2 = Filter.filterFor(Order.class, "shipments(orderItems(itemPrice=?))");

        class TestVisitor extends Visitor {
            ExistsFilter f;

            public Object visit(ExistsFilter f, Object param) {
                if (this.f != null) {
                    fail();
                }
                this.f = f;
                return null;
            }
        }

        TestVisitor tv = new TestVisitor();
        f1.accept(tv, null);

        assertTrue(f2 == tv.f);
    }

    public void testBindingAndValues() {
        Filter<Order> f1 = Filter.filterFor
            (Order.class,
             "orderComments != ? & " +
             "((shipments(orderItems(itemPrice=? & (itemPrice=? | itemPrice>?)) | " +
             "order.shipments(orderItems(itemPrice=?)))) | orderComments!=?)");

        assertTrue(f1 != f1.bind());
        assertTrue(f1.bind() != f1.disjunctiveNormalForm());

        FilterValues<Order> fv = f1.initialFilterValues();
        assertEquals(6, fv.getBlankParameterCount());

        fv = fv.with("comments1");
        assertEquals(5, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);

        fv = fv.with(100);
        assertEquals(4, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);
        assertEquals(100, fv.getSuppliedValues()[1]);

        fv = fv.with(200);
        assertEquals(3, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);
        assertEquals(100, fv.getSuppliedValues()[1]);
        assertEquals(200, fv.getSuppliedValues()[2]);

        fv = fv.with(300);
        assertEquals(2, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);
        assertEquals(100, fv.getSuppliedValues()[1]);
        assertEquals(200, fv.getSuppliedValues()[2]);
        assertEquals(300, fv.getSuppliedValues()[3]);

        fv = fv.with(400);
        assertEquals(1, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);
        assertEquals(100, fv.getSuppliedValues()[1]);
        assertEquals(200, fv.getSuppliedValues()[2]);
        assertEquals(300, fv.getSuppliedValues()[3]);
        assertEquals(400, fv.getSuppliedValues()[4]);

        fv = fv.with("comments2");
        assertEquals(0, fv.getBlankParameterCount());
        assertEquals("comments1", fv.getSuppliedValues()[0]);
        assertEquals(100, fv.getSuppliedValues()[1]);
        assertEquals(200, fv.getSuppliedValues()[2]);
        assertEquals(300, fv.getSuppliedValues()[3]);
        assertEquals(400, fv.getSuppliedValues()[4]);
        assertEquals("comments2", fv.getSuppliedValues()[5]);

        assertEquals("comments1", fv.getValues()[0]);
        assertEquals(100, fv.getValues()[1]);
        assertEquals(200, fv.getValues()[2]);
        assertEquals(300, fv.getValues()[3]);
        assertEquals(400, fv.getValues()[4]);
        assertEquals("comments2", fv.getValues()[5]);
    }
}
