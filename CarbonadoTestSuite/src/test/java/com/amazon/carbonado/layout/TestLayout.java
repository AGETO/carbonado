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

package com.amazon.carbonado.layout;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.DateTime;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.attribute.Annotation;

import org.cojen.util.BeanPropertyAccessor;
import org.cojen.util.ClassInjector;

import com.amazon.carbonado.adapter.YesNoAdapter;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;

import com.amazon.carbonado.stored.FileInfo;
import com.amazon.carbonado.stored.StorableDateIndex;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestMinimal;

import com.amazon.carbonado.TestUtilities;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestLayout extends TestCase {
    private static final String TEST_STORABLE_NAME = "test.TheTestStorableType";
    private static final String TEST_STORABLE_NAME_2 = "test.TheOtherTestStorableType";

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestLayout.class);
    }

    private Repository mRepository;
    private LayoutFactory mFactory;

    public TestLayout(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        mRepository = TestUtilities.buildTempRepository();
        mFactory = new LayoutFactory(mRepository);
    }

    protected void tearDown() throws Exception {
        mRepository.close();
        mRepository = null;
    }

    public void testBasic() throws Exception {
        // Run test twice: First time, records get inserted. Second time, they are loaded.
        for (int i=0; i<2; i++) {
            Layout layout = mFactory.layoutFor(StorableTestBasic.class);
            
            assertEquals(StorableTestBasic.class.getName(), layout.getStorableTypeName());
            assertEquals(-8576805570777985009L, layout.getLayoutID());
            assertEquals(0, layout.getGeneration());
            assertTrue(layout.getCreationDateTime().getMillis() > 0);
            assertTrue(layout.getCreationDateTime().getMillis() <= System.currentTimeMillis());
            
            List<LayoutProperty> properties = layout.getAllProperties();
            
            assertEquals(6, properties.size());
            
            LayoutProperty property;
            
            property = properties.get(0);
            assertEquals("id", property.getPropertyName());
            assertEquals("I", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertTrue(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
            
            property = properties.get(1);
            assertEquals("date", property.getPropertyName());
            assertEquals("Lorg/joda/time/DateTime;", property.getPropertyTypeDescriptor());
            assertTrue(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
            
            property = properties.get(2);
            assertEquals("doubleProp", property.getPropertyName());
            assertEquals("D", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
            
            property = properties.get(3);
            assertEquals("intProp", property.getPropertyName());
            assertEquals("I", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
            
            property = properties.get(4);
            assertEquals("longProp", property.getPropertyName());
            assertEquals("J", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
            
            property = properties.get(5);
            assertEquals("stringProp", property.getPropertyName());
            assertEquals("Ljava/lang/String;", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(null, property.getAdapterTypeName());
            assertEquals(null, property.getAdapterParams());
        }
    }

    public void testAdapter() throws Exception {
        // Run test twice: First time, records get inserted. Second time, they are loaded.
        for (int i=0; i<2; i++) {
            Layout layout = mFactory.layoutFor(FileInfo.class);
            
            assertEquals(FileInfo.class.getName(), layout.getStorableTypeName());
            
            List<LayoutProperty> properties = layout.getAllProperties();

            // Should exclude joins.
            assertEquals(8, properties.size());

            LayoutProperty property;
            
            property = properties.get(1);
            assertEquals("directory", property.getPropertyName());
            assertEquals("Z", property.getPropertyTypeDescriptor());
            assertFalse(property.isNullable());
            assertFalse(property.isPrimaryKeyMember());
            assertEquals(YesNoAdapter.class.getName(), property.getAdapterTypeName());
            assertEquals("@Lcom/amazon/carbonado/adapter/YesNoAdapter;lenient=Z1;",
                         property.getAdapterParams());
        }
    }

    public void testAddProperty() throws Exception {
        for (int i=0; i<2; i++) {
            {
                // Zero extra properties.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(0, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(1, properties.size());
            }

            {
                // One extra property.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(1, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("I", properties.get(1).getPropertyTypeDescriptor());
            }
        }
    }

    public void testRemoveProperty() throws Exception {
        for (int i=0; i<2; i++) {
            {
                // One extra property.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(0, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("I", properties.get(1).getPropertyTypeDescriptor());
            }

            {
                // Zero extra properties.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(1, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(1, properties.size());
            }
        }
    }

    public void testChangePropertyType() throws Exception {
        for (int i=0; i<2; i++) {
            {
                // One extra property, type int.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(0, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("I", properties.get(1).getPropertyTypeDescriptor());
            }

            {
                // One extra property, type String.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.STRING);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(1, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("Ljava/lang/String;", properties.get(1).getPropertyTypeDescriptor());
            }
        }
    }

    public void testWithMulipleTypes() throws Exception {
        for (int i=0; i<10; i++) {
            {
                // One extra property, type int.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(0, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("I", properties.get(1).getPropertyTypeDescriptor());
            }

            {
                // One extra property, type int.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME_2, 1, TypeDesc.INT);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME_2, layout.getStorableTypeName());
                assertEquals(0, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("I", properties.get(1).getPropertyTypeDescriptor());
            }

            {
                // One extra property, type String.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.STRING);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME, layout.getStorableTypeName());
                assertEquals(1, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("Ljava/lang/String;", properties.get(1).getPropertyTypeDescriptor());
            }

            {
                // One extra property, type String.
                Class<? extends Storable> type =
                    defineStorable(TEST_STORABLE_NAME_2, 1, TypeDesc.STRING);
                Layout layout = mFactory.layoutFor(type);
                assertEquals(TEST_STORABLE_NAME_2, layout.getStorableTypeName());
                assertEquals(1, layout.getGeneration());
                List<LayoutProperty> properties = layout.getAllProperties();
                assertEquals(2, properties.size());
                assertEquals("prop0", properties.get(1).getPropertyName());
                assertEquals("Ljava/lang/String;", properties.get(1).getPropertyTypeDescriptor());
            }
        }
    }

    public void testReconstruct() throws Exception {
        // Run test twice: First time, records get inserted. Second time, they are loaded.
        for (int i=0; i<2; i++) {
            Layout layout = mFactory.layoutFor(StorableDateIndex.class);
            
            Class<? extends Storable> recon = layout.reconstruct();

            assertTrue(recon != StorableDateIndex.class);

            Layout reconLayout = mFactory.layoutFor(recon);
            assertTrue(layout == reconLayout);

            compareInfo(StorableIntrospector.examine(StorableDateIndex.class),
                        StorableIntrospector.examine(recon));
        }
    }

    public void testEvolution_addProperties() throws Exception {
        {
            // Zero extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.insert();
        }

        {
            // One extra property.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            bean.setPropertyValue(stm, "prop0", 200);
            stm.insert();

            // Now load old generation.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(0, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
        }

        {
            // Two extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 2, TypeDesc.INT);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(3);
            bean.setPropertyValue(stm, "prop0", 300);
            bean.setPropertyValue(stm, "prop1", 301);
            stm.insert();

            // Now load old generations.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(0, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
            assertEquals(0, ((Integer) bean.getPropertyValue(stm, "prop1")).intValue());

            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
            assertEquals(0, ((Integer) bean.getPropertyValue(stm, "prop1")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(300, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
            assertEquals(301, ((Integer) bean.getPropertyValue(stm, "prop1")).intValue());
        }

        {
            // Test incrementally with over 130 added properties, to ensure
            // generation is encoded in 1 and 4 bytes formats.
            for (int i=3; i<=130; i++) {
                Class<? extends StorableTestMinimal> type =
                    defineStorable(TEST_STORABLE_NAME, i, TypeDesc.INT);
                BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
                Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
                StorableTestMinimal stm = storage.prepare();
                stm.setId(i + 3);
                for (int j=0; j<i; j++) {
                    bean.setPropertyValue(stm, "prop" + j, 400 + j);
                }
                stm.insert();

                // Now load an old generation.
                stm = storage.prepare();
                stm.setId(3);
                stm.load();
                assertEquals(300, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
                assertEquals(301, ((Integer) bean.getPropertyValue(stm, "prop1")).intValue());
                assertEquals(0, ((Integer) bean.getPropertyValue(stm, "prop2")).intValue());
            }
        }
    }

    public void testEvolution_removeProperties() throws Exception {
        {
            // Two extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 2, TypeDesc.INT);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            bean.setPropertyValue(stm, "prop0", 100);
            bean.setPropertyValue(stm, "prop1", 101);
            stm.insert();
        }

        {
            // One extra property.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            bean.setPropertyValue(stm, "prop0", 200);
            stm.insert();

            // Now load old generation.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) bean.getPropertyValue(stm, "prop0")).intValue());
        }

        {
            // Zero extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(type);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(3);
            stm.insert();

            // Now load old generations.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();

            stm = storage.prepare();
            stm.setId(2);
            stm.load();

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
        }
    }

    public void testEvolution_changeProperties() throws Exception {
        // Define various generations.
        Class<? extends StorableTestMinimal> typeWithInt =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
        Class<? extends StorableTestMinimal> typeWithStr =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.STRING);
        Class<? extends StorableTestMinimal> typeWithLong =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.LONG);
        Class<? extends StorableTestMinimal> typeWithDate =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.forClass(DateTime.class));
        Class<? extends StorableTestMinimal> typeWithIntAndStr =
            defineStorable(TEST_STORABLE_NAME, TypeDesc.INT, TypeDesc.STRING);

        DateTime date = new DateTime();

        {
            // Insert int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithInt);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithInt);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            bean.setPropertyValue(stm, "prop0", 100);
            stm.insert();
        }

        {
            // Insert String prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithStr);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            bean.setPropertyValue(stm, "prop0", "hello");
            stm.insert();
        }

        {
            // Insert long prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithLong);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithLong);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(3);
            bean.setPropertyValue(stm, "prop0", 0x100000001L);
            stm.insert();
        }

        {
            // Insert date prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithDate);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithDate);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(4);
            bean.setPropertyValue(stm, "prop0", date);
            stm.insert();
        }

        {
            // Insert int prop0, String prop1.
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithIntAndStr);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithIntAndStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(5);
            bean.setPropertyValue(stm, "prop0", 500);
            bean.setPropertyValue(stm, "prop1", "world");
            stm.insert();
        }

        // Load using int property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithInt);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithInt);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, bean.getPropertyValue(stm, "prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0, bean.getPropertyValue(stm, "prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            // Cast of 0x100000001L to int yields 1.
            assertEquals(1, bean.getPropertyValue(stm, "prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0, bean.getPropertyValue(stm, "prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500, bean.getPropertyValue(stm, "prop0"));
        }

        // Load using String property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithStr);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals("hello", bean.getPropertyValue(stm, "prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));
        }

        // Load using long property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithLong);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithLong);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100L, bean.getPropertyValue(stm, "prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0L, bean.getPropertyValue(stm, "prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(0x100000001L, bean.getPropertyValue(stm, "prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0L, bean.getPropertyValue(stm, "prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500L, bean.getPropertyValue(stm, "prop0"));
        }

        // Load using date property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithDate);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithDate);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(date, bean.getPropertyValue(stm, "prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(null, bean.getPropertyValue(stm, "prop0"));
        }

        // Load using int and String property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithIntAndStr);
            BeanPropertyAccessor bean = BeanPropertyAccessor.forClass(typeWithIntAndStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, bean.getPropertyValue(stm, "prop0"));
            assertEquals(null, bean.getPropertyValue(stm, "prop1"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0, bean.getPropertyValue(stm, "prop0"));
            assertEquals(null, bean.getPropertyValue(stm, "prop1"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            // Cast of 0x100000001L to int yields 1.
            assertEquals(1, bean.getPropertyValue(stm, "prop0"));
            assertEquals(null, bean.getPropertyValue(stm, "prop1"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0, bean.getPropertyValue(stm, "prop0"));
            assertEquals(null, bean.getPropertyValue(stm, "prop1"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500, bean.getPropertyValue(stm, "prop0"));
            assertEquals("world", bean.getPropertyValue(stm, "prop1"));
        }
    }

    private void compareInfo(StorableInfo expected, StorableInfo actual) {
        Map<String, StorableProperty> expectedProps = expected.getAllProperties();
        Map<String, StorableProperty> actualProps = actual.getAllProperties();

        for (StorableProperty expectedProp : expectedProps.values()) {
            StorableProperty actualProp = actualProps.get(expectedProp.getName());
            if (actualProp == null) {
                fail("Missing property: " + expectedProp.getName());
            }
            assertEquals(expectedProp.getType(), actualProp.getType());

            assertEquals(expectedProp.isPrimaryKeyMember(), actualProp.isPrimaryKeyMember());

            StorablePropertyAdapter expectedAdapter = expectedProp.getAdapter();
            StorablePropertyAdapter actualAdapter = actualProp.getAdapter();

            if (expectedAdapter == null) {
                assertNull(actualAdapter);
            } else {
                assertNotNull(actualAdapter);
                assertEquals(expectedAdapter.getAnnotation().getAnnotation(),
                             actualAdapter.getAnnotation().getAnnotation());
            }
        }
    }

    private Class<? extends StorableTestMinimal> defineStorable(String name,
                                                                int extraPropCount,
                                                                TypeDesc propType)
    {
        ClassInjector ci = ClassInjector.createExplicit(name, null);
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.setTarget("1.5");
        cf.setModifiers(cf.getModifiers().toInterface(true));
        cf.addInterface(StorableTestMinimal.class);

        definePrimaryKey(cf);

        for (int i=0; i<extraPropCount; i++) {
            cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "getProp" + i, propType, null);
            cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "setProp" + i, null, new TypeDesc[]{propType});
        }

        return ci.defineClass(cf);
    }

    private Class<? extends StorableTestMinimal> defineStorable(String name,
                                                                TypeDesc propType,
                                                                TypeDesc propType2)
    {
        ClassInjector ci = ClassInjector.createExplicit(name, null);
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.setTarget("1.5");
        cf.setModifiers(cf.getModifiers().toInterface(true));
        cf.addInterface(StorableTestMinimal.class);

        definePrimaryKey(cf);

        int i = 0;
        cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "getProp" + i, propType, null);
        cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "setProp" + i, null, new TypeDesc[]{propType});

        i++;
        cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "getProp" + i, propType2, null);
        cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "setProp" + i, null, new TypeDesc[]{propType2});

        return ci.defineClass(cf);
    }

    private void definePrimaryKey(ClassFile cf) {
        // Add primary key on inherited "id" property.
        // @PrimaryKey(value={"id"})
        Annotation pk = cf.addRuntimeVisibleAnnotation(TypeDesc.forClass(PrimaryKey.class));
        Annotation.MemberValue[] props = new Annotation.MemberValue[1];
        props[0] = pk.makeMemberValue("id");
        pk.putMemberValue("value", props);
    }
}
