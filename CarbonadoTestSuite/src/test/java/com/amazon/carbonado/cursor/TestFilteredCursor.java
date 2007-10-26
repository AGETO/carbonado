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

import org.joda.time.DateTime;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.filter.*;

import com.amazon.carbonado.repo.toy.ToyRepository;
import com.amazon.carbonado.stored.*;

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

    public void testExistsNoParams() throws Exception {
        Repository repo = new ToyRepository();
        Storage<Order> orders = repo.storageFor(Order.class);
        Storage<OrderItem> items = repo.storageFor(OrderItem.class);
        Storage<Shipment> shipments = repo.storageFor(Shipment.class);

        Order order = orders.prepare();
        order.setOrderID(1);
        order.setOrderNumber("one");
        order.setOrderTotal(100);
        order.setAddressID(0);
        order.insert();

        // Query for orders with any items.
        long count = orders.query("orderItems()").count();
        assertEquals(0, count);

        // Query for orders with no items.
        List<Order> matches = orders.query("!orderItems()").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(order, matches.get(0));

        // Query for orders with any shipments with any items.
        matches = orders.query("shipments(orderItems())").fetch().toList();
        assertEquals(0, matches.size());

        // Query for orders with no shipments with any items.
        matches = orders.query("!shipments(orderItems())").fetch().toList();
        assertEquals(1, matches.size());

        // Query for orders with any shipments with no items.
        matches = orders.query("shipments(!orderItems())").fetch().toList();
        assertEquals(0, matches.size());

        // Query for orders with no shipments with no items.
        matches = orders.query("!shipments(!orderItems())").fetch().toList();
        assertEquals(1, matches.size());

        // Insert more records (orders with items) and re-issue queries.

        order = orders.prepare();
        order.setOrderID(2);
        order.setOrderNumber("two");
        order.setOrderTotal(200);
        order.setAddressID(0);
        order.insert();

        OrderItem item = items.prepare();
        item.setOrderItemID(1);
        item.setOrderID(2);
        item.setItemDescription("desc one");
        item.setItemQuantity(1);
        item.setItemPrice(100);
        item.setShipmentID(0);
        item.insert();

        // Query for orders with any items.
        matches = orders.query("orderItems()").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(2, matches.get(0).getOrderID());

        // Query for orders with no items.
        matches = orders.query("!orderItems()").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(1, matches.get(0).getOrderID());

        // Query for orders with any shipments with any items.
        matches = orders.query("shipments(orderItems())").fetch().toList();
        assertEquals(0, matches.size());

        // Query for orders with no shipments with any items.
        matches = orders.query("!shipments(orderItems())").fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(2, matches.get(1).getOrderID());

        // Query for orders with any shipments with no items.
        matches = orders.query("shipments(!orderItems())").fetch().toList();
        assertEquals(0, matches.size());

        // Query for orders with no shipments with no items.
        matches = orders.query("!shipments(!orderItems())").fetch().toList();
        assertEquals(2, matches.size());

        // Insert more records (orders with shipments with items) and re-issue queries.

        order = orders.prepare();
        order.setOrderID(3);
        order.setOrderNumber("three");
        order.setOrderTotal(300);
        order.setAddressID(0);
        order.insert();

        Shipment shipment = shipments.prepare();
        shipment.setShipmentID(1);
        shipment.setShipmentNotes("notes");
        shipment.setShipmentDate(new DateTime());
        shipment.setOrderID(3);
        shipment.setShipperID(0);
        shipment.insert();

        item = items.prepare();
        item.setOrderItemID(2);
        item.setOrderID(3);
        item.setItemDescription("desc two");
        item.setItemQuantity(1);
        item.setItemPrice(500);
        item.setShipmentID(1);
        item.insert();

        // Query for orders with any items.
        matches = orders.query("orderItems()").fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(2, matches.get(0).getOrderID());
        assertEquals(3, matches.get(1).getOrderID());

        // Query for orders with no items.
        matches = orders.query("!orderItems()").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(1, matches.get(0).getOrderID());

        // Query for orders with any shipments with any items.
        matches = orders.query("shipments(orderItems())").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(3, matches.get(0).getOrderID());

        // Query for orders with no shipments with any items.
        matches = orders.query("!shipments(orderItems())").fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(2, matches.get(1).getOrderID());

        // Query for orders with any shipments with no items.
        matches = orders.query("shipments(!orderItems())").fetch().toList();
        assertEquals(0, matches.size());

        // Query for orders with no shipments with no items.
        matches = orders.query("!shipments(!orderItems())").fetch().toList();
        assertEquals(3, matches.size());

        // Insert more records to test for empty shipments.

        order = orders.prepare();
        order.setOrderID(4);
        order.setOrderNumber("four");
        order.setOrderTotal(400);
        order.setAddressID(0);
        order.insert();

        shipment = shipments.prepare();
        shipment.setShipmentID(2);
        shipment.setShipmentNotes("notes 2");
        shipment.setShipmentDate(new DateTime());
        shipment.setOrderID(4);
        shipment.setShipperID(0);
        shipment.insert();

        // Query for orders with any shipments with no items.
        matches = orders.query("shipments(!orderItems())").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(4, matches.get(0).getOrderID());

        // Query for orders with no shipments with no items.
        matches = orders.query("!shipments(!orderItems())").fetch().toList();
        assertEquals(3, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(2, matches.get(1).getOrderID());
        assertEquals(3, matches.get(2).getOrderID());
    }

    public void testExistsNoParamsManyToOne() throws Exception {
        Repository repo = new ToyRepository();
        Storage<Order> orders = repo.storageFor(Order.class);
        Storage<OrderItem> items = repo.storageFor(OrderItem.class);

        Order order = orders.prepare();
        order.setOrderID(1);
        order.setOrderNumber("one");
        order.setOrderTotal(100);
        order.setAddressID(0);
        order.insert();

        OrderItem item = items.prepare();
        item.setOrderItemID(1);
        item.setOrderID(1);
        item.setItemDescription("desc one");
        item.setItemQuantity(1);
        item.setItemPrice(100);
        item.setShipmentID(0);
        item.insert();

        item = items.prepare();
        item.setOrderItemID(2);
        item.setOrderID(1);
        item.setItemDescription("desc two");
        item.setItemQuantity(2);
        item.setItemPrice(5);
        item.setShipmentID(0);
        item.insert();

        item = items.prepare();
        item.setOrderItemID(3);
        item.setOrderID(0);
        item.setItemDescription("desc three");
        item.setItemQuantity(1);
        item.setItemPrice(5);
        item.setShipmentID(0);
        item.insert();

        // Query for items contained in an order.
        List<OrderItem> matches = items.query("order()").fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(1, matches.get(0).getOrderItemID());
        assertEquals(2, matches.get(1).getOrderItemID());

        // Query for items contained not in an order.
        matches = items.query("!order()").fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(3, matches.get(0).getOrderItemID());

        // Outer join.
        matches = items.query("itemPrice = ? & (order.orderTotal = ? | !order())")
            .with(5).with(100)
            .fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(2, matches.get(0).getOrderItemID());
        assertEquals(3, matches.get(1).getOrderItemID());

        // Outer join, done in the "correct" way.
        matches = items.query("itemPrice = ? & (order).orderTotal = ?")
            .with(5).with(100)
            .fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(2, matches.get(0).getOrderItemID());
        assertEquals(3, matches.get(1).getOrderItemID());
    }

    public void testExistsWithParams() throws Exception {
        Repository repo = new ToyRepository();
        Storage<Order> orders = repo.storageFor(Order.class);
        Storage<OrderItem> items = repo.storageFor(OrderItem.class);
        Storage<Shipment> shipments = repo.storageFor(Shipment.class);

        Order order = orders.prepare();
        order.setOrderID(1);
        order.setOrderNumber("one");
        order.setOrderTotal(100);
        order.setAddressID(0);
        order.insert();

        Shipment shipment = shipments.prepare();
        shipment.setShipmentID(1);
        shipment.setShipmentNotes("notes 1");
        shipment.setShipmentDate(new DateTime());
        shipment.setOrderID(1);
        shipment.setShipperID(0);
        shipment.insert();

        shipment = shipments.prepare();
        shipment.setShipmentID(2);
        shipment.setShipmentNotes("notes 2");
        shipment.setShipmentDate(new DateTime());
        shipment.setOrderID(1);
        shipment.setShipperID(0);
        shipment.insert();

        OrderItem item = items.prepare();
        item.setOrderItemID(1);
        item.setOrderID(1);
        item.setItemDescription("one one");
        item.setItemQuantity(1);
        item.setItemPrice(500);
        item.setShipmentID(1);
        item.insert();

        item = items.prepare();
        item.setOrderItemID(2);
        item.setOrderID(1);
        item.setItemDescription("one two");
        item.setItemQuantity(1);
        item.setItemPrice(500);
        item.setShipmentID(1);
        item.insert();

        item = items.prepare();
        item.setOrderItemID(3);
        item.setOrderID(1);
        item.setItemDescription("one three");
        item.setItemQuantity(2);
        item.setItemPrice(500);
        item.setShipmentID(2);
        item.insert();

        order = orders.prepare();
        order.setOrderID(2);
        order.setOrderNumber("two");
        order.setOrderTotal(20000);
        order.setAddressID(0);
        order.insert();

        order = orders.prepare();
        order.setOrderID(3);
        order.setOrderNumber("three");
        order.setOrderTotal(300);
        order.setAddressID(0);
        order.insert();

        item = items.prepare();
        item.setOrderItemID(4);
        item.setOrderID(3);
        item.setItemDescription("three one");
        item.setItemQuantity(20);
        item.setItemPrice(500);
        item.setShipmentID(2);
        item.insert();

        order = orders.prepare();
        order.setOrderID(4);
        order.setOrderNumber("four");
        order.setOrderTotal(0);
        order.setAddressID(0);
        order.insert();

        shipment = shipments.prepare();
        shipment.setShipmentID(3);
        shipment.setShipmentNotes("notes 3");
        shipment.setShipmentDate(new DateTime());
        shipment.setOrderID(4);
        shipment.setShipperID(0);
        shipment.insert();

        item = items.prepare();
        item.setOrderItemID(5);
        item.setOrderID(4);
        item.setItemDescription("four one");
        item.setItemQuantity(99);
        item.setItemPrice(500);
        item.setShipmentID(3);
        item.insert();

        // Query for orders which has an item with a specific quantity.

        List<Order> matches = orders.query("orderItems(itemQuantity >= ? & itemQuantity < ?)")
            .with(2).with(10).fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(1, matches.get(0).getOrderID());

        matches = orders.query("orderItems(itemQuantity >= ? & itemQuantity < ?)")
            .with(2).with(50).fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(3, matches.get(1).getOrderID());

        // Query for empty orders with a non-zero total.

        matches = orders.query("orderTotal != ? & !orderItems()")
            .with(0).fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(2, matches.get(0).getOrderID());
            
        matches = orders.query("!orderItems() & orderTotal != ?")
            .with(0).fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(2, matches.get(0).getOrderID());

        // Query for non-empty orders or those with a zero total.

        matches = orders.query("!(!orderItems() & orderTotal != ?)")
            .with(0).fetch().toList();
        assertEquals(3, matches.size());
        for (int i=0; i<matches.size(); i++) {
            assertTrue(matches.get(i).getOrderID() != 2);
        }

        // Query for orders in shipments which have an item with a high quantity.

        matches = orders.query("shipments(orderItems(itemQuantity > ?))")
            .with(10).fetch().toList();
        assertEquals(2, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(4, matches.get(1).getOrderID());

        // Query with a specific quantity.

        matches = orders.query("shipments(orderItems(itemQuantity = ?))")
            .with(99).fetch().toList();
        assertEquals(1, matches.size());
        assertEquals(4, matches.get(0).getOrderID());

        // Mix in some more stuff...

        matches = orders
            .query("orderNumber = ? | shipments(orderItems(itemQuantity = ?) | shipmentNotes = ?)")
            .with("three").with(99).with("notes 1").fetch().toList();
        assertEquals(3, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(3, matches.get(1).getOrderID());
        assertEquals(4, matches.get(2).getOrderID());

        matches = orders
            .query("orderNumber = ? | " +
                   "shipments(orderItems(itemQuantity = ?) | " +
                              "shipmentNotes = ? | " +
                              "orderItems(itemQuantity = ?))")
            .with("three").with(99).with("notes 1").with(25).fetch().toList();
        assertEquals(3, matches.size());
        assertEquals(1, matches.get(0).getOrderID());
        assertEquals(3, matches.get(1).getOrderID());
        assertEquals(4, matches.get(2).getOrderID());
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
