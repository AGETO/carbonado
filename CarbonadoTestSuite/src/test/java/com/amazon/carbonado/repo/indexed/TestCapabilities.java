/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.indexed;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;
import com.amazon.carbonado.info.Direction;

import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestBasicIndexed;
import com.amazon.carbonado.stored.StorableTestBasicCompoundIndexed;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestCapabilities extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestCapabilities.class);
    }

    private Repository mRepository;

    public TestCapabilities(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        mRepository = TestUtilities.buildTempRepository("indexed");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        mRepository.close();
        mRepository = null;
    }

    public void testStorableInfoCapability() throws Exception {
        StorableInfoCapability cap = mRepository.getCapability(StorableInfoCapability.class);
        assertNotNull(cap);

        String[] names = cap.getUserStorableTypeNames();
        assertEquals(0, names.length);

        mRepository.storageFor(StorableTestBasic.class);

        names = cap.getUserStorableTypeNames();
        assertEquals(1, names.length);
        assertEquals(StorableTestBasic.class.getName(), names[0]);

        mRepository.storageFor(StorableTestBasicIndexed.class);
        names = cap.getUserStorableTypeNames();
        assertEquals(2, names.length);
        List<String> list = Arrays.asList(names);
        assertTrue(list.contains(StorableTestBasic.class.getName()));
        assertTrue(list.contains(StorableTestBasicIndexed.class.getName()));

        mRepository.storageFor(StorableTestBasicCompoundIndexed.class);
        names = cap.getUserStorableTypeNames();
        assertEquals(3, names.length);
        list = Arrays.asList(names);
        assertTrue(list.contains(StorableTestBasic.class.getName()));
        assertTrue(list.contains(StorableTestBasicIndexed.class.getName()));
        assertTrue(list.contains(StorableTestBasicCompoundIndexed.class.getName()));
    }

    public void testIndexInfoCapability() throws Exception {
        IndexInfoCapability cap = mRepository.getCapability(IndexInfoCapability.class);
        assertNotNull(cap);

        // StorableTestBasic
        IndexInfo[] infos = cap.getIndexInfo(StorableTestBasic.class);
        assertEquals(1, infos.length);
        assertContainsIndex(infos,
                            StorableTestBasic.class.getName(),
                            true,
                            new String[] {"id"},
                            new Direction[] {Direction.ASCENDING});

        // StorableTestBasicIndexed
        infos = cap.getIndexInfo(StorableTestBasicIndexed.class);
        assertEquals(5, infos.length);
        assertContainsIndex(infos,
                            StorableTestBasicIndexed.class.getName(),
                            true,
                            new String[] {"id"},
                            new Direction[] {Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"stringProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"intProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"longProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"doubleProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING});

        // StorableTestBasicCompoundIndexed
        infos = cap.getIndexInfo(StorableTestBasicCompoundIndexed.class);
        assertEquals(5, infos.length);
        assertContainsIndex(infos,
                            StorableTestBasicCompoundIndexed.class.getName(),
                            true,
                            new String[] {"id"},
                            new Direction[] {Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"stringProp", "intProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"intProp", "stringProp", "id"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"doubleProp", "longProp", "id"},
                            new Direction[] {Direction.DESCENDING, Direction.ASCENDING, Direction.ASCENDING});
        assertContainsIndex(infos,
                            null,
                            true,
                            new String[] {"stringProp", "doubleProp"},
                            new Direction[] {Direction.ASCENDING, Direction.ASCENDING});
    }

    private void assertContainsIndex(IndexInfo[] infos,
                                     String name,
                                     boolean unique,
                                     String[] properties,
                                     Direction[] directions)
    {
        findIndex:
        for (IndexInfo info : infos) {
            if (name == null || name.equals(info.getName())) {
                if (name == null) {
                    if (unique != info.isUnique() ||
                        properties.length != info.getPropertyNames().length ||
                        directions.length != info.getPropertyDirections().length) {
                        continue findIndex;
                    }
                } else {
                    assertEquals(unique, info.isUnique());
                    assertEquals(properties.length, info.getPropertyNames().length);
                    assertEquals(directions.length, info.getPropertyDirections().length);
                }

                for (int i=0; i<properties.length; i++) {
                    String expectedProp = properties[i];
                    String[] actualProps = info.getPropertyNames();
                    int j;
                    findProp: {
                        for (j=0; j<actualProps.length; j++) {
                            if (expectedProp.equals(actualProps[j])) {
                                break findProp;
                            }
                        }
                        if (name == null) {
                            continue findIndex;
                        }
                        fail("Unable to find expected property: " + expectedProp);
                    }

                    assertEquals(directions[i], directions[j]);
                }
                return;
            }
        }

        fail("Index not found");
    }
}
