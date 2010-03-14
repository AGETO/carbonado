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

package com.amazon.carbonado.adapter;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author Brian S O'Neill
 */
public class TestDateTimeParser extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestDateTimeParser.class);
    }

    public TestDateTimeParser(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void testParse() throws Exception {
        DateTimeAdapter.Adapter adapter = 
            new DateTimeAdapter.Adapter(Object.class, "test", (DateTimeZone) null);

        DateTime dt;

        dt = adapter.adaptToDateTime("2007-08-23");
        assertEquals(new DateTime("2007-08-23"), dt);

        dt = adapter.adaptToDateTime("2007-08-23T12:34:56.123");
        assertEquals(new DateTime("2007-08-23T12:34:56.123"), dt);

        dt = adapter.adaptToDateTime("2007-08-23 12:34:56.123");
        assertEquals(new DateTime("2007-08-23T12:34:56.123"), dt);

        dt = adapter.adaptToDateTime("T12:34:56.123");
        assertEquals(new DateTime("1970-01-01T12:34:56.123"), dt);
    }

    public void testParseUTC() throws Exception {
        DateTimeAdapter.Adapter adapter = 
            new DateTimeAdapter.Adapter(Object.class, "test", DateTimeZone.UTC);

        DateTime dt;

        dt = adapter.adaptToDateTime("2007-08-23");
        assertEquals(new DateTime("2007-08-23", DateTimeZone.UTC), dt);

        dt = adapter.adaptToDateTime("2007-08-23T12:34:56.123");
        assertEquals(new DateTime("2007-08-23T12:34:56.123", DateTimeZone.UTC), dt);

        dt = adapter.adaptToDateTime("2007-08-23 12:34:56.123");
        assertEquals(new DateTime("2007-08-23T12:34:56.123", DateTimeZone.UTC), dt);

        dt = adapter.adaptToDateTime("T12:34:56.123");
        assertEquals(new DateTime("1970-01-01T12:34:56.123", DateTimeZone.UTC), dt);
    }
}
