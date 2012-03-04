/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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
package com.amazon.carbonado.repo.replicated;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.ConfigurationException;

import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.STBContainer;

/**
 *
 * @author Don Schneider
 */
public class TestCursors extends TestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestCursors.class);
    }

    public TestCursors(String s) {
        super(s);
    }

    public TestCursors() {
        this("default");
    }

    /**
     * Test joins
     */
    public void test_joins() throws Exception {
        Repository repository = TestUtilities.buildTempRepository("test_joins");
        setup(repository);
        do_test_joins(repository);
        repository.close();
        repository = null;
    }

    public void do_test_joins(Repository repository) throws Exception {
        Storage<STBContainer> storage = repository.storageFor(STBContainer.class);
        STBContainer s = storage.prepare();
        s.setName("first");
        s.setCategory("imaString_1");
        s.setCount(1000001);

        Collection<Integer> ids = checkContents(s, 1);

        assertEquals(10, ids.size());

        LinkedHashMap<String, Collection<Integer>> allIdCollections =
                new LinkedHashMap<String, Collection<Integer>>();
        Cursor<STBContainer> cSTBC = storage.query().fetch();
        while (cSTBC.hasNext()) {
            STBContainer stbc = cSTBC.next();
            ids = checkContents(stbc, stbc.getCount());
            for (Map.Entry entry : allIdCollections.entrySet()) {
                if (entry.getKey().equals(stbc.getCategory())) {
                    assertTrue(ids.equals(entry.getValue()));
                } else {
                    assertFalse(ids.equals(entry.getValue()));
                }
            }
            allIdCollections.put(stbc.getCategory(), ids);
        }
     }

    /**
     * Test queries
     */
     public void test_queries() throws Exception {
        Repository repository = TestUtilities.buildTempRepository("test_queries");
        setup(repository);
        do_test_queries(repository);
        repository.close();
    }

    public void do_test_queries(Repository repository) throws Exception {
        Storage<StorableTestBasic> storage =
                repository.storageFor(StorableTestBasic.class);
        filterTest(storage, "id > ? & id < ?", 10, 20, 11, 20);
        filterTest(storage, "id >= ? & id < ?", 10, 20, 10, 20);
        filterTest(storage, "id > ? & id <= ?", 10, 20, 11, 21);
        filterTest(storage, "id >= ? & id <= ?", 10, 20, 10, 21);
     }

    /**
     * Test RRQueries
     * Includes an implicit test of the repository configuration
     */
    public void test_RRQueries() throws Exception {
        RepositoryBuilder replicaRepository = TestUtilities.newTempRepositoryBuilder();
        RepositoryBuilder masterRepository = TestUtilities.newTempRepositoryBuilder("alt");

        ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
        try {
            builder.assertReady();
            fail("unready builder passed assertReady()");
        }
        catch (ConfigurationException e) {
            // expected
        }
        builder.setName("test");
        try {
            builder.assertReady();
            fail("unready builder passed assertReady()");
        }
        catch (ConfigurationException e) {
            // expected
        }
        builder.setReplicaRepositoryBuilder(replicaRepository);
        try {
            builder.assertReady();
            fail("builder config passed assertReady()");
        }
        catch (ConfigurationException e) {
            // expected
        }
        builder.setMasterRepositoryBuilder(masterRepository);
        builder.assertReady();

        Repository RR = builder.build();

        setup(RR);

        do_test_joins(RR);
        do_test_queries(RR);

        RR.close();
    }

    private Collection<Integer> checkContents(STBContainer s, int count) throws FetchException {
        Query<StorableTestBasic> q = s.getContained();
        Cursor<StorableTestBasic> cSTB = q.fetch();
        Collection<Integer> ids = new HashSet<Integer>(15);
        while (cSTB.hasNext()) {
            StorableTestBasic stb = cSTB.next();
            assertEquals("imaString_" + count % 10, stb.getStringProp());
            Integer id = new Integer(stb.getId());
            assertFalse(ids.contains(id));
            ids.add(id);
        }
        return ids;
    }

    private void filterTest(Storage<StorableTestBasic> storage,
                            String queryString,
                            int min,
                            int max,
                            int first,
                            int last)
            throws Exception
    {
        Query<StorableTestBasic> q = storage.query(queryString).with(min).with(max);
        Cursor<StorableTestBasic> c = q.fetch();
        int i = first;
        while (c.hasNext()) {
            StorableTestBasic item = c.next();
            assertEquals(queryString, i, item.getId());
            i++;
        }
        assertEquals(queryString, last, i);
    }

    private void setup(Repository repository) throws RepositoryException {
        StorableTestBasic.insertBunches(repository, 100);
        STBContainer.insertBunches(repository, 30);
    }
}
