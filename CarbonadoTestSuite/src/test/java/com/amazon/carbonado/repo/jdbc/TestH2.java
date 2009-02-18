/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.io.*;

import java.sql.DriverManager;

import junit.framework.TestSuite;

import org.apache.commons.dbcp.BasicDataSource;

import com.amazon.carbonado.*;

import com.amazon.carbonado.lob.*;

import com.amazon.carbonado.repo.indexed.IndexedRepositoryBuilder;

import com.amazon.carbonado.TestUtilities;

import com.amazon.carbonado.stored.StorableWithLobs;
import com.amazon.carbonado.stored.WithPropertyOther;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestH2 extends com.amazon.carbonado.TestStorables {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestH2.class);
        return suite;
    }

    public TestH2(String name) {
        super(name);
    }

    public void test_propertyOther() throws Exception {
        Storage<WithPropertyOther> storage = getRepository().storageFor(WithPropertyOther.class);
        WithPropertyOther other = storage.prepare();
        other.setId(1);
        other.setObject("hello");
        other.insert();

        other.load();
        assertEquals("hello", other.getObject());
    }

    @Override
    public void test_derivedJoinIndex() throws Exception {
        // Needs to use custom indexing for this test to work.
        IndexedRepositoryBuilder builder = new IndexedRepositoryBuilder();
        builder.setWrappedRepository(jdbcBuilder(true));
        Repository repo = builder.build();
        test_derivedJoinIndex(repo);
    }

    /* FIXME: Deleting of derived index entries needs more thought.
    @Override
    public void test_basicDerivedJoinIndex() throws Exception {
        // Needs to use custom indexing for this test to work.
        IndexedRepositoryBuilder builder = new IndexedRepositoryBuilder();
        builder.setWrappedRepository(jdbcBuilder(true));
        Repository repo = builder.build();
        test_basicDerivedJoinIndex(repo);
    }
    */

    // Override because H2 does not fully support LOBs.
    @Override
    public void test_lobInsert() throws Exception {
        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        // Test null insert
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
            lobs.load();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
        }

        // Test content insert
        int id;
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world"));
            lobs.insert();
            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());
            lobs.load();
            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());
            id = lobs.getId();
        }

        // Test insert failure
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(id);

            Blob newBlob = new ByteArrayBlob("blob insert should fail".getBytes());
            Clob newClob = new StringClob("clob insert should fail");

            lobs.setBlobValue(newBlob);
            lobs.setClobValue(newClob);

            try {
                lobs.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }

            assertTrue(newBlob == lobs.getBlobValue());
            assertTrue(newClob == lobs.getClobValue());
        }
    }

    // Override because H2 does not fully support LOBs.
    @Override
    public void test_lobUpdate() throws Exception {
        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        // Test null replaces null
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();

            lobs.setBlobValue(null);
            lobs.setClobValue(null);

            lobs.update();

            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());

            lobs.load();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
        }

        // Test null replaces content and verify content deleted
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world!!!"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            assertEquals(5, blob.getLength());
            assertEquals(8, clob.getLength());

            lobs.setBlobValue(null);

            lobs.update();

            assertNull(lobs.getBlobValue());
            assertEquals(clob.asString(), lobs.getClobValue().asString());

            lobs.load();

            assertNull(lobs.getBlobValue());
            assertEquals(clob.asString(), lobs.getClobValue().asString());

            lobs.setClobValue(null);

            lobs.update();

            assertNull(lobs.getBlobValue());
            assertNull(lobs.getClobValue());

            lobs.load();

            assertNull(lobs.getBlobValue());
            assertNull(lobs.getClobValue());
        }

        // Test content replaces null
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();

            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world"));

            assertTrue(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertTrue(lobs.getClobValue() instanceof StringClob);

            lobs.update();

            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());

            assertFalse(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertFalse(lobs.getClobValue() instanceof StringClob);

            lobs.load();

            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());

            assertFalse(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertFalse(lobs.getClobValue() instanceof StringClob);
        }

        // Test content replaces content of same length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("12345".getBytes()));
            lobs.update();

            assertEquals(5, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("12345", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("123456"));
            lobs.update();

            assertEquals(5, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("12345", lobs.getBlobValue().asString());
            assertEquals("123456", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test content replaces content of longer length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("123".getBytes()));
            lobs.update();

            assertEquals(3, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("123", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("12"));
            lobs.update();

            assertEquals(3, lobs.getBlobValue().getLength());
            assertEquals(2, lobs.getClobValue().getLength());

            assertEquals("123", lobs.getBlobValue().asString());
            assertEquals("12", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test content replaces content of shorter length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("123456789".getBytes()));
            lobs.update();

            assertEquals(9, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("123456789", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("1234567890"));
            lobs.update();

            assertEquals(9, lobs.getBlobValue().getLength());
            assertEquals(10, lobs.getClobValue().getLength());

            assertEquals("123456789", lobs.getBlobValue().asString());
            assertEquals("1234567890", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test update failure
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(10000);

            Blob newBlob = new ByteArrayBlob("blob update should fail".getBytes());
            Clob newClob = new StringClob("clob update should fail");

            lobs.setBlobValue(newBlob);
            lobs.setClobValue(newClob);

            try {
                lobs.update();
                fail();
            } catch (PersistNoneException e) {
            }

            assertTrue(newBlob == lobs.getBlobValue());
            assertTrue(newClob == lobs.getClobValue());
        }
    }

    @Override
    public void test_insertLobBig() throws Exception {
        // Not a useful test.
    }

    @Override
    protected BigInteger expected(BigInteger bi) {
        // Used to detect that BigIntegerAdapter was selected.
        return bi.add(BigInteger.ONE);
    }

    @Override
    protected BigDecimal expected(BigDecimal bd) {
        return bd;
    }

    @Override
    protected Repository buildRepository(boolean isMaster) throws RepositoryException {
        return jdbcBuilder(isMaster).build();
    }

    private RepositoryBuilder jdbcBuilder(boolean isMaster) throws RepositoryException {
        JDBCRepositoryBuilder builder = new JDBCRepositoryBuilder();
        builder.setName("jdbc");
        builder.setAutoVersioningEnabled(true, null);
        builder.setMaster(isMaster);
        BasicDataSource ds = new BasicDataSource();
        builder.setDataSource(ds);

        builder.setSchemaResolver(new H2SchemaResolver());

        File dir = new File(TestUtilities.makeTestDirectory("jdbc"), "/h2");
        String url = "jdbc:h2:" + dir.getPath();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(url);
        ds.setUsername("sa");
        ds.setPassword("");

        return builder;
    }
}
