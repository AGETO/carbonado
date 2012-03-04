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

package com.amazon.carbonado.info;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.stored.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestChainedProperty extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestChainedProperty.class);
    }

    public TestChainedProperty(String name) {
        super(name);
    }

    public void test_parsePrime() {
        StorableInfo<Order> info = StorableIntrospector.examine(Order.class);

        ChainedProperty<Order> p, p2;

        p = ChainedProperty.parse(info, "orderID");
        assertEquals(info.getAllProperties().get("orderID"), p.getPrimeProperty());
        assertEquals(info.getAllProperties().get("orderID"), p.getLastProperty());
        assertEquals(long.class, p.getType());
        assertEquals(0, p.getChainCount());
        try {
            p.getChainedProperty(0);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertFalse(p.isOuterJoin(0));
        try {
            p.isOuterJoin(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("orderID", p.toString());

        p2 = ChainedProperty.parse(info, "    orderID");
        assertTrue(p == p2);

        p = ChainedProperty.parse(info, "(orderID)");
        assertEquals(info.getAllProperties().get("orderID"), p.getPrimeProperty());
        assertEquals(info.getAllProperties().get("orderID"), p.getLastProperty());
        assertEquals(long.class, p.getType());
        assertEquals(0, p.getChainCount());
        try {
            p.getChainedProperty(0);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertTrue(p.isOuterJoin(0));
        try {
            p.isOuterJoin(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("(orderID)", p.toString());

        p2 = ChainedProperty.parse(info, "    (orderID ) ");
        assertTrue(p == p2);
    }

    public void test_parseChained() {
        StorableInfo<Order> info = StorableIntrospector.examine(Order.class);
        StorableInfo<Address> info2 = StorableIntrospector.examine(Address.class);

        ChainedProperty<Order> p, p2;

        p = ChainedProperty.parse(info, "address.addressCity");
        assertEquals(info.getAllProperties().get("address"), p.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getLastProperty());
        assertEquals(String.class, p.getType());
        assertEquals(1, p.getChainCount());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getChainedProperty(0));
        try {
            p.getChainedProperty(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertFalse(p.isOuterJoin(0));
        assertFalse(p.isOuterJoin(1));
        try {
            p.isOuterJoin(2);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("address.addressCity", p.toString());

        p2 = ChainedProperty.parse(info, "address   .    addressCity  ");
        assertTrue(p == p2);

        p = ChainedProperty.parse(info, "(address).addressCity");
        assertEquals(info.getAllProperties().get("address"), p.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getLastProperty());
        assertEquals(String.class, p.getType());
        assertEquals(1, p.getChainCount());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getChainedProperty(0));
        try {
            p.getChainedProperty(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertTrue(p.isOuterJoin(0));
        assertFalse(p.isOuterJoin(1));
        try {
            p.isOuterJoin(2);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("(address).addressCity", p.toString());

        p2 = ChainedProperty.parse(info, "(address )   .    addressCity  ");
        assertTrue(p == p2);

        p = ChainedProperty.parse(info, "address.(addressCity)");
        assertEquals(info.getAllProperties().get("address"), p.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getLastProperty());
        assertEquals(String.class, p.getType());
        assertEquals(1, p.getChainCount());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getChainedProperty(0));
        try {
            p.getChainedProperty(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertFalse(p.isOuterJoin(0));
        assertTrue(p.isOuterJoin(1));
        try {
            p.isOuterJoin(2);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("address.(addressCity)", p.toString());

        p2 = ChainedProperty.parse(info, " address    .    (  addressCity )  ");
        assertTrue(p == p2);

        p = ChainedProperty.parse(info, "(address).(addressCity)");
        assertEquals(info.getAllProperties().get("address"), p.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getLastProperty());
        assertEquals(String.class, p.getType());
        assertEquals(1, p.getChainCount());
        assertEquals(info2.getAllProperties().get("addressCity"), p.getChainedProperty(0));
        try {
            p.getChainedProperty(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertTrue(p.isOuterJoin(0));
        assertTrue(p.isOuterJoin(1));
        try {
            p.isOuterJoin(2);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("(address).(addressCity)", p.toString());

        p2 = ChainedProperty.parse(info, " (address)    .    (  addressCity )  ");
        assertTrue(p == p2);

        StorableInfo<OrderItem> info3 = StorableIntrospector.examine(OrderItem.class);
        StorableInfo<Shipment> info4 = StorableIntrospector.examine(Shipment.class);

        ChainedProperty<OrderItem> p3 =
            ChainedProperty.parse(info3, "shipment.order.   address.addressState");
        assertEquals(info3.getAllProperties().get("shipment"), p3.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressState"), p3.getLastProperty());
        assertEquals(String.class, p3.getType());
        assertEquals(3, p3.getChainCount());
        assertEquals(info4.getAllProperties().get("order"), p3.getChainedProperty(0));
        assertEquals(info.getAllProperties().get("address"), p3.getChainedProperty(1));
        assertEquals(info2.getAllProperties().get("addressState"), p3.getChainedProperty(2));
        try {
            p3.getChainedProperty(3);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertFalse(p3.isOuterJoin(0));
        assertFalse(p3.isOuterJoin(1));
        assertFalse(p3.isOuterJoin(2));
        assertFalse(p3.isOuterJoin(3));
        try {
            p3.isOuterJoin(4);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("shipment.order.address.addressState", p3.toString());

        p3 = ChainedProperty.parse(info3, "shipment.(order).(address  ).addressState");
        assertEquals(info3.getAllProperties().get("shipment"), p3.getPrimeProperty());
        assertEquals(info2.getAllProperties().get("addressState"), p3.getLastProperty());
        assertEquals(String.class, p3.getType());
        assertEquals(3, p3.getChainCount());
        assertEquals(info4.getAllProperties().get("order"), p3.getChainedProperty(0));
        assertEquals(info.getAllProperties().get("address"), p3.getChainedProperty(1));
        assertEquals(info2.getAllProperties().get("addressState"), p3.getChainedProperty(2));
        try {
            p3.getChainedProperty(3);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertFalse(p3.isOuterJoin(0));
        assertTrue(p3.isOuterJoin(1));
        assertTrue(p3.isOuterJoin(2));
        assertFalse(p3.isOuterJoin(3));
        try {
            p3.isOuterJoin(4);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals("shipment.(order).(address).addressState", p3.toString());
    }

    public void test_append() {
        StorableInfo<OrderItem> info = StorableIntrospector.examine(OrderItem.class);
        StorableInfo<Shipment> info2 = StorableIntrospector.examine(Shipment.class);
        StorableInfo<Order> info3 = StorableIntrospector.examine(Order.class);
        StorableInfo<Address> info4 = StorableIntrospector.examine(Address.class);

        ChainedProperty<OrderItem> p, p2, p3, p4;

        p = ChainedProperty.get(info.getAllProperties().get("shipment"));
        p2 = p.append(info2.getAllProperties().get("order"), true);
        p3 = p2.append(info3.getAllProperties().get("address"), true);
        p4 = p3.append(info4.getAllProperties().get("addressState"), false);

        assertEquals("shipment", p.toString());
        assertEquals("shipment.(order)", p2.toString());
        assertEquals("shipment.(order).(address)", p3.toString());
        assertEquals("shipment.(order).(address).addressState", p4.toString());

        ChainedProperty<Order> p5 = ChainedProperty.parse(info3, "(address)");
        assertEquals(p3, p2.append(p5));

        p5 = ChainedProperty.parse(info3, "(address).addressState");
        assertEquals(p4, p2.append(p5));

        ChainedProperty<Shipment> p6 = ChainedProperty.parse(info2, "(order).(address)");
        assertEquals(p3, p.append(p6));
    }

    public void test_trim() {
        StorableInfo<OrderItem> info = StorableIntrospector.examine(OrderItem.class);

        ChainedProperty<OrderItem> p, p2, p3, p4;

        p = ChainedProperty.parse(info, "shipment.(order).(address).addressState");
        p2 = p.trim();
        p3 = p2.trim();
        p4 = p3.trim();

        assertEquals("shipment.(order).(address)", p2.toString());
        assertEquals("shipment.(order)", p3.toString());
        assertEquals("shipment", p4.toString());

        try {
            p4.trim();
            fail();
        } catch (IllegalStateException e) {
        }

        p = ChainedProperty.parse(info, "(shipment).order.address.(addressState)");
        p2 = p.trim();
        p3 = p2.trim();
        p4 = p3.trim();

        assertEquals("(shipment).order.address", p2.toString());
        assertEquals("(shipment).order", p3.toString());
        assertEquals("(shipment)", p4.toString());

        try {
            p4.trim();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    public void test_tail() {
        StorableInfo<OrderItem> info = StorableIntrospector.examine(OrderItem.class);

        ChainedProperty<OrderItem> p;
        ChainedProperty p2, p3, p4;

        p = ChainedProperty.parse(info, "shipment.(order).(address).addressState");
        p2 = p.tail();
        p3 = p2.tail();
        p4 = p3.tail();

        assertEquals("(order).(address).addressState", p2.toString());
        assertEquals("(address).addressState", p3.toString());
        assertEquals("addressState", p4.toString());

        try {
            p4.tail();
            fail();
        } catch (IllegalStateException e) {
        }

        p = ChainedProperty.parse(info, "(shipment).order.address.(addressState)");
        p2 = p.tail();
        p3 = p2.tail();
        p4 = p3.tail();

        assertEquals("order.address.(addressState)", p2.toString());
        assertEquals("address.(addressState)", p3.toString());
        assertEquals("(addressState)", p4.toString());

        try {
            p4.tail();
            fail();
        } catch (IllegalStateException e) {
        }
    }
}
