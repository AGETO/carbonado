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
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.attribute.Annotation;
import org.cojen.classfile.constant.ConstantUTFInfo;

import org.cojen.util.ClassInjector;

import com.amazon.carbonado.adapter.TextAdapter;
import com.amazon.carbonado.adapter.YesNoAdapter;
import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.amazon.carbonado.stored.FileInfo;
import com.amazon.carbonado.stored.SomeText;
import com.amazon.carbonado.stored.StorableDateIndex;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestMinimal;

import com.amazon.carbonado.util.AnnotationDescParser;

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
        mFactory = null;
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

    public void testAdapter2() throws Exception {
        // Run test twice: First time, records get inserted. Second time, they are loaded.
        for (int i=0; i<2; i++) {
            Layout layout = mFactory.layoutFor(SomeText.class);
            
            assertEquals(SomeText.class.getName(), layout.getStorableTypeName());
            
            List<LayoutProperty> properties = layout.getAllProperties();

            assertEquals(8, properties.size());

            LayoutProperty property;

            int charsetCount = 0;
            int altCharsetsCount = 0;

            for (int j=1; j<=7; j++) {
                property = properties.get(j);
                assertEquals("text" + j, property.getPropertyName());
                assertEquals("Ljava/lang/String;", property.getPropertyTypeDescriptor());
                assertTrue(property.isNullable());
                assertFalse(property.isPrimaryKeyMember());
                assertEquals(TextAdapter.class.getName(), property.getAdapterTypeName());

                String desc = property.getAdapterParams();
                assertTrue(desc.startsWith
                           ("@Lcom/amazon/carbonado/adapter/TextAdapter;altCharsets=["));

                ClassFile cf = new ClassFile("test");
                final MethodInfo mi = cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "test", null, null);

                Annotation ann = new AnnotationDescParser(desc) {
                    protected Annotation buildRootAnnotation(TypeDesc rootAnnotationType) {
                        return mi.addRuntimeVisibleAnnotation(rootAnnotationType);
                    }
                }.parse(null);

                assertEquals("Lcom/amazon/carbonado/adapter/TextAdapter;",
                             ann.getTypeConstant().getValue());

                Map<String, Annotation.MemberValue> mvMap = ann.getMemberValues();

                if (mvMap.containsKey("charset")) {
                    charsetCount++;
                    Annotation.MemberValue mv = mvMap.get("charset");
                    assertEquals(Annotation.MEMBER_TAG_STRING, mv.getTag());
                    ConstantUTFInfo value = (ConstantUTFInfo) mv.getValue();
                    assertEquals("UTF-8", value.getValue());
                }

                if (mvMap.containsKey("altCharsets")) {
                    altCharsetsCount++;
                    Annotation.MemberValue mv = mvMap.get("altCharsets");
                    assertEquals(Annotation.MEMBER_TAG_ARRAY, mv.getTag());
                    Annotation.MemberValue[] values = (Annotation.MemberValue[]) mv.getValue();
                    for (int k=0; k<values.length; k++) {
                        Annotation.MemberValue element = values[k];
                        assertEquals(Annotation.MEMBER_TAG_STRING, element.getTag());
                        ConstantUTFInfo value = (ConstantUTFInfo) element.getValue();
                        if (k == 0) {
                            assertEquals("ASCII", value.getValue());
                        } else if (k == 1) {
                            assertEquals("UTF-16", value.getValue());
                        }
                    }
                }
            }

            assertEquals(4, charsetCount);
            assertTrue(altCharsetsCount >= 6);
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
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            stm.setPropertyValue("prop0", 200);
            stm.insert();

            // Now load old generation.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(0, ((Integer) stm.getPropertyValue("prop0")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) stm.getPropertyValue("prop0")).intValue());

            // Load old generation and verify that new property value is cleared.
            stm.markAllPropertiesDirty();
            stm.setId(1);
            stm.load();
            assertEquals(0, ((Integer) stm.getPropertyValue("prop0")).intValue());
        }

        {
            // Two extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 2, TypeDesc.INT);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(3);
            stm.setPropertyValue("prop0", 300);
            stm.setPropertyValue("prop1", 301);
            stm.insert();

            // Now load old generations.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(0, ((Integer) stm.getPropertyValue("prop0")).intValue());
            assertEquals(0, ((Integer) stm.getPropertyValue("prop1")).intValue());

            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) stm.getPropertyValue("prop0")).intValue());
            assertEquals(0, ((Integer) stm.getPropertyValue("prop1")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(300, ((Integer) stm.getPropertyValue("prop0")).intValue());
            assertEquals(301, ((Integer) stm.getPropertyValue("prop1")).intValue());
        }

        {
            // Test incrementally with over 130 added properties, to ensure
            // generation is encoded in 1 and 4 bytes formats.
            for (int i=3; i<=130; i++) {
                Class<? extends StorableTestMinimal> type =
                    defineStorable(TEST_STORABLE_NAME, i, TypeDesc.INT);
                Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
                StorableTestMinimal stm = storage.prepare();
                stm.setId(i + 3);
                for (int j=0; j<i; j++) {
                    stm.setPropertyValue("prop" + j, 400 + j);
                }
                stm.insert();

                // Now load an old generation.
                stm = storage.prepare();
                stm.setId(3);
                stm.load();
                assertEquals(300, ((Integer) stm.getPropertyValue("prop0")).intValue());
                assertEquals(301, ((Integer) stm.getPropertyValue("prop1")).intValue());
                assertEquals(0, ((Integer) stm.getPropertyValue("prop2")).intValue());
            }
        }
    }

    public void testEvolution_removeProperties() throws Exception {
        {
            // Two extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 2, TypeDesc.INT);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.setPropertyValue("prop0", 100);
            stm.setPropertyValue("prop1", 101);
            stm.insert();
        }

        {
            // One extra property.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(type);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            stm.setPropertyValue("prop0", 200);
            stm.insert();

            // Now load old generation.
            stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, ((Integer) stm.getPropertyValue("prop0")).intValue());

            // Verify load of new generation.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(200, ((Integer) stm.getPropertyValue("prop0")).intValue());
        }

        {
            // Zero extra properties.
            Class<? extends StorableTestMinimal> type =
                defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);
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

        Class<? extends StorableTestMinimal> typeWithNullableInteger =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.forClass(Integer.class), true);

        DateTime date = new DateTime();

        {
            // Insert int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithInt);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.setPropertyValue("prop0", 100);
            stm.insert();
        }

        {
            // Insert String prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(2);
            stm.setPropertyValue("prop0", "hello");
            stm.insert();
        }

        {
            // Insert long prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithLong);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(3);
            stm.setPropertyValue("prop0", 0x100000001L);
            stm.insert();
        }

        {
            // Insert date prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithDate);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(4);
            stm.setPropertyValue("prop0", date);
            stm.insert();
        }

        {
            // Insert int prop0, String prop1.
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithIntAndStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(5);
            stm.setPropertyValue("prop0", 500);
            stm.setPropertyValue("prop1", "world");
            stm.insert();
        }

        {
            // Insert Nullable Integer prop0, value is non-null
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithNullableInteger);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(6);
            stm.setPropertyValue("prop0", new Integer(9876));
            stm.insert();
        }

        {
            // Insert Nullable Integer prop0, value is null
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithNullableInteger);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(7);
            stm.setPropertyValue("prop0", null);
            stm.insert();
        }

        // Load using int property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithInt);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, stm.getPropertyValue("prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            // Cast of 0x100000001L to int yields 1.
            assertEquals(1, stm.getPropertyValue("prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(9876, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));
        }

        // Load using String property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals("hello", stm.getPropertyValue("prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));
        }

        // Load using long property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithLong);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100L, stm.getPropertyValue("prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0L, stm.getPropertyValue("prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(0x100000001L, stm.getPropertyValue("prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0L, stm.getPropertyValue("prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500L, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(9876L, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(0L, stm.getPropertyValue("prop0"));
        }

        // Load using date property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(typeWithDate);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(date, stm.getPropertyValue("prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));
        }

        // Load using int and String property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithIntAndStr);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            // Cast of 0x100000001L to int yields 1.
            assertEquals(1, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500, stm.getPropertyValue("prop0"));
            assertEquals("world", stm.getPropertyValue("prop1"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(9876, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(0, stm.getPropertyValue("prop0"));
            assertEquals(null, stm.getPropertyValue("prop1"));
        }

        // Load using Nullable Integer property generation.
        {
            // Load against int prop0.
            Storage<? extends StorableTestMinimal> storage =
                mRepository.storageFor(typeWithNullableInteger);
            StorableTestMinimal stm = storage.prepare();
            stm.setId(1);
            stm.load();
            assertEquals(100, stm.getPropertyValue("prop0"));

            // Load against String prop0.
            stm = storage.prepare();
            stm.setId(2);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against long prop0.
            stm = storage.prepare();
            stm.setId(3);
            stm.load();
            // Cast of 0x100000001L to int yields 1.
            assertEquals(1, stm.getPropertyValue("prop0"));

            // Load against date prop0.
            stm = storage.prepare();
            stm.setId(4);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));

            // Load against int prop0, String prop1.
            stm = storage.prepare();
            stm.setId(5);
            stm.load();
            assertEquals(500, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value non-null.
            stm = storage.prepare();
            stm.setId(6);
            stm.load();
            assertEquals(9876, stm.getPropertyValue("prop0"));

            // Load against Integer prop0, value null.
            stm = storage.prepare();
            stm.setId(7);
            stm.load();
            assertEquals(null, stm.getPropertyValue("prop0"));
        }
    }

    public void testCorruption() throws Exception {
        // Forces corruption by deleting layout
        // record. CorruptEncodingException should include Storable.

        // Create a persistent repository.
        mRepository.close();
        BDBRepositoryBuilder builder =
            (BDBRepositoryBuilder) TestUtilities.newTempRepositoryBuilder();
        //System.out.println(builder.getEnvironmentHome());
        builder.setLogInMemory(false);
        mRepository = builder.build();

        Class<? extends StorableTestMinimal> type =
            defineStorable(TEST_STORABLE_NAME, 0, TypeDesc.INT);

        StorableTestMinimal test = mRepository.storageFor(type).prepare();
        test.setId(1);
        test.insert();

        Class<? extends StorableTestMinimal> type2 =
            defineStorable(TEST_STORABLE_NAME, 1, TypeDesc.INT);

        StorableTestMinimal test2 = mRepository.storageFor(type2).prepare();
        test2.setId(2);
        type2.getMethod("setProp0", int.class).invoke(test2, 10);
        test2.insert();

        // Close and re-open.
        mRepository.close();
        mRepository = builder.build();

        test = mRepository.storageFor(type).prepare();
        test.setId(1);
        test.load();

        // Blow away layout data.
        mRepository.storageFor(StoredLayout.class).query().deleteAll();

        test2 = mRepository.storageFor(type2).prepare();
        test2.setId(2);
        try {
            test2.load();
            fail();
        } catch (CorruptEncodingException e) {
            assertEquals(test2, e.getStorableWithPrimaryKey());
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

    /**
     * Defines a new Storable with a variable number of extra properties.
     *
     * @param name name of class to generate
     * @param extraPropCount number of properties named "propN" to add, where N
     * is the zero-based property number
     * @param propType type of each extra property
     */
    public static Class<? extends StorableTestMinimal> defineStorable(String name,
                                                                      int extraPropCount,
                                                                      TypeDesc propType)
    {
        return defineStorable(name, extraPropCount, propType, false);
    }

    /**
     * Defines a new Storable with a variable number of extra properties.
     *
     * @param name name of class to generate
     * @param extraPropCount number of properties named "propN" to add, where N
     * is the zero-based property number
     * @param propType type of each extra property
     * @param nullable properties should be annotated as Nullable
     */
    public static Class<? extends StorableTestMinimal> defineStorable(String name,
                                                                      int extraPropCount,
                                                                      TypeDesc propType,
                                                                      boolean nullable)
    {
        ClassInjector ci = ClassInjector.createExplicit(name, null);
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.setTarget("1.5");
        cf.setModifiers(cf.getModifiers().toInterface(true));
        cf.addInterface(StorableTestMinimal.class);

        definePrimaryKey(cf);

        for (int i=0; i<extraPropCount; i++) {
            MethodInfo getMethod = cf.addMethod
                (Modifiers.PUBLIC_ABSTRACT, "getProp" + i, propType, null);

            if (nullable) {
                getMethod.addRuntimeVisibleAnnotation(TypeDesc.forClass(Nullable.class));
            }

            cf.addMethod(Modifiers.PUBLIC_ABSTRACT, "setProp" + i, null, new TypeDesc[]{propType});
        }

        return ci.defineClass(cf);
    }

    /**
     * Defines a new Storable with two extra properties named "prop0" and "prop1".
     *
     * @param name name of class to generate
     * @param propType type of property 0
     * @param propType2 type of property 1
     */
    public static Class<? extends StorableTestMinimal> defineStorable(String name,
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

    private static void definePrimaryKey(ClassFile cf) {
        // Add primary key on inherited "id" property.
        // @PrimaryKey(value={"id"})
        Annotation pk = cf.addRuntimeVisibleAnnotation(TypeDesc.forClass(PrimaryKey.class));
        Annotation.MemberValue[] props = new Annotation.MemberValue[1];
        props[0] = pk.makeMemberValue("id");
        pk.putMemberValue("value", props);
    }
}
