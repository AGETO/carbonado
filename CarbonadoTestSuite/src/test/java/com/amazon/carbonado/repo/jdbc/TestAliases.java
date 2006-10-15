/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.jdbc;

import java.util.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Tests the auto-generated aliases for tables and columns.
 *
 * @author Brian S O'Neill
 */
public class TestAliases extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestAliases.class);
    }

    public TestAliases(String name) {
        super(name);
    }

    public void testSimpleAliases() {
        String[] aliases, expectedAliases;

        aliases = JDBCStorableIntrospector.generateAliases("bar");
        compareAliases(aliases, "bar", "Bar", "BAR");

        aliases = JDBCStorableIntrospector.generateAliases("Bar");
        compareAliases(aliases, "Bar", "bar", "BAR");

        aliases = JDBCStorableIntrospector.generateAliases("BAR");
        compareAliases(aliases, "BAR", "bar");

        aliases = JDBCStorableIntrospector.generateAliases("bAR");
        compareAliases(aliases, "bAR", "bar", "BAR");

        aliases = JDBCStorableIntrospector.generateAliases("_bAR");
        compareAliases(aliases, "_bAR", "_bar", "_BAR");

        aliases = JDBCStorableIntrospector.generateAliases("_BaR_");
        compareAliases(aliases, "_BaR_", "_Ba_R_", "_ba_r_", "_BA_R_");

        aliases = JDBCStorableIntrospector.generateAliases("URL");
        compareAliases(aliases, "URL", "url");
    }

    public void testCompoundAliases() {
        String[] aliases, expectedAliases;

        aliases = JDBCStorableIntrospector.generateAliases("lastName");
        compareAliases(aliases, "lastName", "last_name", "Last_Name", "LAST_NAME");

        aliases = JDBCStorableIntrospector.generateAliases("LastName");
        compareAliases(aliases, "LastName", "last_name", "Last_Name", "LAST_NAME");

        aliases = JDBCStorableIntrospector.generateAliases("Last_Name");
        compareAliases(aliases, "Last_Name", "last_name", "LAST_NAME");

        aliases = JDBCStorableIntrospector.generateAliases("Last__Name");
        compareAliases(aliases, "Last__Name", "last__name", "LAST__NAME");

        aliases = JDBCStorableIntrospector.generateAliases("_last_name");
        compareAliases(aliases, "_last_name", "_LAST_NAME");

        aliases = JDBCStorableIntrospector.generateAliases("resourceURL");
        compareAliases(aliases, "resourceURL", "resource_url", "Resource_URL", "RESOURCE_URL");
    }

    private void compareAliases(String[] aliases, String... expected) {
        Set<String> aliasesSet = new HashSet<String>();
        for (String alias : aliases) {
            aliasesSet.add(alias);
        }
        for (String alias : expected) {
            if (!aliasesSet.contains(alias)) {
                fail("Unable to find expected alias: " + alias);
            }
        }
    }
}
