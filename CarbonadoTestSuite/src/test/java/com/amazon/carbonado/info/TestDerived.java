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

package com.amazon.carbonado.info;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.stored.*;

/**
 * Test cases for introspection of derived properties.
 *
 * @author Brian S O'Neill
 */
public class TestDerived extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestDerived.class);
    }

    public TestDerived(String name) {
        super(name);
    }

    public void test_simple() {
        StorableInfo<WithDerived> info = StorableIntrospector.examine(WithDerived.class);
        StorableProperty<WithDerived> prop = info.getAllProperties().get("upperCaseName");
        assertTrue(prop != null);
        assertTrue(prop.isDerived());
        assertEquals(1, prop.getDerivedFromProperties().length);
        assertEquals("name", prop.getDerivedFromProperties()[0].toString());
    }

    public void test_simpleComposites() {
        StorableInfo<WithDerived> info = StorableIntrospector.examine(WithDerived.class);

        {
            StorableProperty<WithDerived> prop = info.getAllProperties().get("IDAndName");
            assertTrue(prop != null);
            assertTrue(prop.isDerived());
            assertEquals(2, prop.getDerivedFromProperties().length);
            assertEquals("ID", prop.getDerivedFromProperties()[0].toString());
            assertEquals("name", prop.getDerivedFromProperties()[1].toString());
        }

        {
            StorableProperty<WithDerived> prop = info.getAllProperties().get("IDAndUpperCaseName");
            assertTrue(prop != null);
            assertTrue(prop.isDerived());
            assertEquals(3, prop.getDerivedFromProperties().length);
            assertEquals("ID", prop.getDerivedFromProperties()[0].toString());
            assertEquals("upperCaseName", prop.getDerivedFromProperties()[1].toString());
            assertEquals("name", prop.getDerivedFromProperties()[2].toString());
        }

        {
            StorableProperty<WithDerived> prop = info.getAllProperties().get("longKey");
            assertTrue(prop != null);
            assertTrue(prop.isDerived());
            assertEquals(4, prop.getDerivedFromProperties().length);
            assertEquals("IDAndUpperCaseName", prop.getDerivedFromProperties()[0].toString());
            assertEquals("ID", prop.getDerivedFromProperties()[1].toString());
            assertEquals("upperCaseName", prop.getDerivedFromProperties()[2].toString());
            assertEquals("name", prop.getDerivedFromProperties()[3].toString());
        }

        {
            StorableProperty<WithDerived> prop = info.getAllProperties().get("anotherLongKey");
            assertTrue(prop != null);
            assertTrue(prop.isDerived());
            assertEquals(5, prop.getDerivedFromProperties().length);
            assertEquals("longKey", prop.getDerivedFromProperties()[0].toString());
            assertEquals("IDAndUpperCaseName", prop.getDerivedFromProperties()[1].toString());
            assertEquals("ID", prop.getDerivedFromProperties()[2].toString());
            assertEquals("upperCaseName", prop.getDerivedFromProperties()[3].toString());
            assertEquals("name", prop.getDerivedFromProperties()[4].toString());
        }

        {
            StorableProperty<WithDerived> prop = info.getAllProperties().get("yetAnotherLongKey");
            assertTrue(prop != null);
            assertTrue(prop.isDerived());
            assertEquals(0, prop.getDerivedFromProperties().length);
        }
    }

    public void test_simpleCycle() {
        try {
            StorableIntrospector.examine(WithDerivedCycle.class);
            fail("Cycle not detected");
        } catch (MalformedTypeException e) {
        }
    }

    public void test_simpleCycle2() {
        try {
            StorableIntrospector.examine(WithDerivedCycle2.class);
            fail("Cycle not detected");
        } catch (MalformedTypeException e) {
        }
    }

    public void test_joinNotDoublyLinked() {
        try {
            StorableInfo<WithDerived2> info = StorableIntrospector.examine(WithDerived2.class);
            fail("Missing double link not detected");
        } catch (MalformedTypeException e) {
        }
    }

    public void test_joinChainA() {
        StorableInfo<WithDerivedChainA> info =
            StorableIntrospector.examine(WithDerivedChainA.class);

        StorableProperty<WithDerivedChainA> prop = info.getAllProperties().get("DName");
        assertTrue(prop != null);
        assertTrue(prop.isDerived());

        assertEquals(4, prop.getDerivedFromProperties().length);
        assertEquals("b.c.d.name", prop.getDerivedFromProperties()[0].toString());
        assertEquals("b.c.d", prop.getDerivedFromProperties()[1].toString());
        assertEquals("b.c", prop.getDerivedFromProperties()[2].toString());
        assertEquals("b", prop.getDerivedFromProperties()[3].toString());

        assertEquals(1, prop.getDerivedToProperties().length);
        assertEquals("upperDName", prop.getDerivedToProperties()[0].toString());

        prop = info.getAllProperties().get("upperDName");
        assertTrue(prop != null);
        assertTrue(prop.isDerived());

        assertEquals(5, prop.getDerivedFromProperties().length);
        assertEquals("DName", prop.getDerivedFromProperties()[0].toString());
        assertEquals("b.c.d.name", prop.getDerivedFromProperties()[1].toString());
        assertEquals("b.c.d", prop.getDerivedFromProperties()[2].toString());
        assertEquals("b.c", prop.getDerivedFromProperties()[3].toString());
        assertEquals("b", prop.getDerivedFromProperties()[4].toString());

        assertEquals(0, prop.getDerivedToProperties().length);
    }

    public void test_joinChainB() {
        StorableInfo<WithDerivedChainB> info =
            StorableIntrospector.examine(WithDerivedChainB.class);

        StorableProperty<WithDerivedChainB> prop = info.getAllProperties().get("c");
        assertTrue(prop != null);
        assertFalse(prop.isDerived());

        assertEquals(0, prop.getDerivedFromProperties().length);

        assertEquals(2, prop.getDerivedToProperties().length);
        assertEquals("DName.b", prop.getDerivedToProperties()[0].toString());
        assertEquals("upperDName.b", prop.getDerivedToProperties()[1].toString());

        prop = info.getAllProperties().get("name");
        assertTrue(prop != null);
        assertFalse(prop.isDerived());

        assertEquals(0, prop.getDerivedFromProperties().length);
        assertEquals(0, prop.getDerivedToProperties().length);
    }

    public void test_joinChainC() {
        StorableInfo<WithDerivedChainC> info =
            StorableIntrospector.examine(WithDerivedChainC.class);

        StorableProperty<WithDerivedChainC> prop = info.getAllProperties().get("d");
        assertTrue(prop != null);
        assertFalse(prop.isDerived());

        assertEquals(0, prop.getDerivedFromProperties().length);

        assertEquals(2, prop.getDerivedToProperties().length);
        assertEquals("DName.b.c", prop.getDerivedToProperties()[0].toString());
        assertEquals("upperDName.b.c", prop.getDerivedToProperties()[1].toString());

        prop = info.getAllProperties().get("name");
        assertTrue(prop != null);
        assertFalse(prop.isDerived());

        assertEquals(0, prop.getDerivedFromProperties().length);
        assertEquals(0, prop.getDerivedToProperties().length);
    }

    public void test_joinChainD() {
        StorableInfo<WithDerivedChainD> info =
            StorableIntrospector.examine(WithDerivedChainD.class);

        StorableProperty<WithDerivedChainD> prop = info.getAllProperties().get("name");
        assertTrue(prop != null);
        assertFalse(prop.isDerived());

        assertEquals(0, prop.getDerivedFromProperties().length);

        assertEquals(2, prop.getDerivedToProperties().length);
        assertEquals("DName.b.c.d", prop.getDerivedToProperties()[0].toString());
        assertEquals("upperDName.b.c.d", prop.getDerivedToProperties()[1].toString());
    }
}
