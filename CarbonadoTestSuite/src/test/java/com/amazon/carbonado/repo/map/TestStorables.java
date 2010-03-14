/*
 * Copyright 2008-2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.map;

import junit.framework.TestSuite;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.stored.StorableDateIndex;
import com.amazon.carbonado.stored.StorableTestBasic;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestStorables extends com.amazon.carbonado.TestStorables {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestStorables.class);
        return suite;
    }

    public TestStorables(String name) {
        super(name);
    }

    @Override
    public void test_invalidStorables() {
        // Map repository has no problem with custom property types.
    }

    @Override
    public void test_dateTimeIndex() throws Exception {
        // Map repository does not use DateTimeAdapter.

        Storage<StorableDateIndex> storage = getRepository().storageFor(StorableDateIndex.class);

        DateTimeZone original = DateTimeZone.getDefault();
        // Set time zone different than defined in storable.
        DateTimeZone.setDefault(DateTimeZone.forID("America/Los_Angeles"));
        try {
            DateTime now = new DateTime();

            StorableDateIndex sdi = storage.prepare();
            sdi.setID(1);
            sdi.setOrderDate(now);
            sdi.insert();

            sdi.load();

            assertEquals(now.getMillis(), sdi.getOrderDate().getMillis());
            // Time zones are equal since there's no adapter.
            assertTrue(now.equals(sdi.getOrderDate()));

            Query<StorableDateIndex> query = storage.query("orderDate=?").with(now);
            StorableDateIndex sdi2 = query.tryLoadOne();
            assertNotNull(sdi2);
        } finally {
            DateTimeZone.setDefault(original);
        }
    }

    public void test_keyClone() throws Exception {
        // This test makes sure that map key is properly cloned before being
        // inserted into map.

        Repository repo = buildRepository(true);
        Storage<StorableTestBasic> storage = repo.storageFor(StorableTestBasic.class);

        StorableTestBasic s = storage.prepare();
        s.setId(1);
        s.setStringProp("a");
        s.setIntProp(1);
        s.setLongProp(1L);
        s.setDoubleProp(1.0);
        s.insert();

        s.markPropertiesDirty();
        s.setId(2);
        s.insert();
    }

    @Override
    protected Repository buildRepository(boolean isMaster) throws RepositoryException {
        MapRepositoryBuilder builder = new MapRepositoryBuilder();
        builder.setName("map");
        builder.setMaster(isMaster);
        return builder.build();
    }
}
