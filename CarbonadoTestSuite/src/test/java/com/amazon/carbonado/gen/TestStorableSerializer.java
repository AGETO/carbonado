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

package com.amazon.carbonado.gen;

import java.io.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.lob.*;

import com.amazon.carbonado.repo.toy.ToyRepository;
import com.amazon.carbonado.stored.*;

/**
 * Test case for Storable serialization.
 *
 * @author Brian S O'Neill
 */
public class TestStorableSerializer extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestStorableSerializer.class);
    }

    private Repository mRepository;

    public TestStorableSerializer(String name) {
        super(name);
    }

    protected void setUp() {
        mRepository = new ToyRepository();
    }

    protected void tearDown() {
        mRepository.close();
        mRepository = null;
    }

    public void testReadAndWrite() throws Exception {
        Storage<StorableTestBasic> storage = mRepository.storageFor(StorableTestBasic.class);
        StorableTestBasic stb = storage.prepare();
        stb.setId(50);
        stb.setStringProp("hello");
        stb.setIntProp(100);
        stb.setLongProp(999);
        stb.setDoubleProp(2.718281828d);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        stb.writeTo(dout);
        dout.flush();

        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        StorableTestBasic stb2 = storage.prepare();
        stb2.readFrom(din);

        assertEquals(stb, stb2);
        assertEquals(stb.toString(), stb2.toString());
    }

    public void testReadAndWrite2() throws Exception {
        Storage<StorableDatePk> storage = mRepository.storageFor(StorableDatePk.class);
        StorableDatePk s = storage.prepare();
        s.setId(50);
        s.setOrderDate(new org.joda.time.DateTime());

        // This should not interfere with date property being set when deserialized.
        s.markAllPropertiesClean();

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        s.writeTo(bout);
        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        StorableDatePk s2 = storage.prepare();
        s2.readFrom(bin);

        assertEquals(s, s2);
        assertEquals(s.toString(), s2.toString());
    }

    public void testReadAndWrite3() throws Exception {
        // Regression test for bug introduced in GenericEncodingStrategy.
        // Definition of ManyProperties class must not change.

        Storage<ManyProperties> storage = mRepository.storageFor(ManyProperties.class);
        ManyProperties s = storage.prepare();

        s.setProp01(1);
        s.setProp02(null);
        s.setProp03("3");
        s.setProp04("hello");
        s.setProp06(6);
        s.setProp07(7);
        s.setProp08(8);
        s.markAllPropertiesClean();
        s.setProp09(9);
        s.setProp10(10);
        s.setProp11(11);
        s.setProp12(12);
        s.setProp13(13);
        s.setProp14(1.4);
        s.setProp16(16);
        s.setProp17(17);
        s.setProp18(18);
        s.setProp19(19);
        s.setProp20(20);
        s.setProp21(21);
        s.setProp22(22);
        s.setProp23(23);
        s.setProp24(24);
        s.setProp25(25);
        s.setProp26(26);
        s.setProp27(27);
        s.setProp28(28);
        s.setProp29(29);
        s.setProp30(30);
        s.setProp31(31);
        s.setProp32(32);
        s.setProp33(33);
        s.setProp34(34);
        s.setProp35(35);
        s.setProp37(37);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        s.writeTo(bout);
        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        ManyProperties s2 = storage.prepare();
        s2.readFrom(bin);

        assertEquals(s, s2);
        assertEquals(s.toString(), s2.toString());

        assertTrue(s2.isPropertyClean("prop01"));
        assertTrue(s2.isPropertyClean("prop02"));
        assertTrue(s2.isPropertyClean("prop03"));
        assertTrue(s2.isPropertyClean("prop04"));
        assertTrue(s2.isPropertyClean("prop06"));
        assertTrue(s2.isPropertyClean("prop07"));
        assertTrue(s2.isPropertyClean("prop08"));
        assertTrue(s2.isPropertyClean("prop15"));

        assertTrue(s2.isPropertyDirty("prop09"));
        assertTrue(s2.isPropertyDirty("prop10"));
        assertTrue(s2.isPropertyDirty("prop11"));
        assertTrue(s2.isPropertyDirty("prop12"));
        assertTrue(s2.isPropertyDirty("prop13"));
        assertTrue(s2.isPropertyDirty("prop14"));
        assertTrue(s2.isPropertyDirty("prop16"));
        assertTrue(s2.isPropertyDirty("prop17"));
        assertTrue(s2.isPropertyDirty("prop18"));
        assertTrue(s2.isPropertyDirty("prop19"));
        assertTrue(s2.isPropertyDirty("prop20"));
        assertTrue(s2.isPropertyDirty("prop21"));
        assertTrue(s2.isPropertyDirty("prop22"));
        assertTrue(s2.isPropertyDirty("prop23"));
        assertTrue(s2.isPropertyDirty("prop24"));
        assertTrue(s2.isPropertyDirty("prop25"));
        assertTrue(s2.isPropertyDirty("prop26"));
        assertTrue(s2.isPropertyDirty("prop27"));
        assertTrue(s2.isPropertyDirty("prop28"));
        assertTrue(s2.isPropertyDirty("prop29"));
        assertTrue(s2.isPropertyDirty("prop30"));
        assertTrue(s2.isPropertyDirty("prop31"));
        assertTrue(s2.isPropertyDirty("prop32"));
        assertTrue(s2.isPropertyDirty("prop33"));
        assertTrue(s2.isPropertyDirty("prop34"));
        assertTrue(s2.isPropertyDirty("prop35"));
        assertTrue(s2.isPropertyDirty("prop37"));
    }

    /*
    public void testReadAndWriteLobs() throws Exception {
        Storage<StorableWithLobs> storage = mRepository.storageFor(StorableWithLobs.class);
        StorableWithLobs s = storage.prepare();
        s.setBlobValue(new ByteArrayBlob("Hello Blob".getBytes()));
        s.setClobValue(new StringClob("Hello Clob"));

        StorableSerializer<StorableWithLobs> serializer = 
            StorableSerializer.forType(StorableWithLobs.class);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        serializer.write(s, (DataOutput) dout);
        dout.flush();

        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        StorableWithLobs s2 = serializer.read(storage, (DataInput) din);

        assertEquals(s, s2);
    }
    */

    @PrimaryKey("id")
    public static abstract class ManyProperties implements Storable {
        public abstract int getId();
        public abstract void setId(int id);

        public abstract int getProp01();
        public abstract void setProp01(int v);

        @Nullable
        public abstract Long getProp02();
        public abstract void setProp02(Long v);

        public abstract String getProp03();
        public abstract void setProp03(String v);

        @Nullable
        public abstract String getProp04();
        public abstract void setProp04(String v);

        @Join(internal="prop06", external="id")
        public abstract ManyProperties getProp05() throws FetchException;
        public abstract void setProp05(ManyProperties v);

        public abstract int getProp06();
        public abstract void setProp06(int v);

        public abstract int getProp07();
        public abstract void setProp07(int v);

        public abstract int getProp08();
        public abstract void setProp08(int v);

        @Derived
        public int getProp08_1() {
            return getProp08() + 1;
        }

        public abstract int getProp09();
        public abstract void setProp09(int v);

        public abstract int getProp10();
        public abstract void setProp10(int v);

        public abstract int getProp11();
        public abstract void setProp11(int v);

        public abstract double getProp12();
        public abstract void setProp12(double v);

        public abstract int getProp13();
        public abstract void setProp13(int v);

        public abstract Double getProp14();
        public abstract void setProp14(Double v);

        @Nullable
        public abstract Double getProp15();
        public abstract void setProp15(Double v);

        public abstract int getProp16();
        public abstract void setProp16(int v);

        public abstract int getProp17();
        public abstract void setProp17(int v);

        public abstract int getProp18();
        public abstract void setProp18(int v);

        public abstract int getProp19();
        public abstract void setProp19(int v);

        public abstract int getProp20();
        public abstract void setProp20(int v);

        public abstract int getProp21();
        public abstract void setProp21(int v);

        public abstract int getProp22();
        public abstract void setProp22(int v);

        public abstract int getProp23();
        public abstract void setProp23(int v);

        public abstract int getProp24();
        public abstract void setProp24(int v);

        public abstract int getProp25();
        public abstract void setProp25(int v);

        public abstract int getProp26();
        public abstract void setProp26(int v);

        public abstract int getProp27();
        public abstract void setProp27(int v);

        public abstract int getProp28();
        public abstract void setProp28(int v);

        public abstract int getProp29();
        public abstract void setProp29(int v);

        public abstract int getProp30();
        public abstract void setProp30(int v);

        public abstract int getProp31();
        public abstract void setProp31(int v);

        public abstract int getProp32();
        public abstract void setProp32(int v);

        public abstract int getProp33();
        public abstract void setProp33(int v);

        public abstract int getProp34();
        public abstract void setProp34(int v);

        public abstract int getProp35();
        public abstract void setProp35(int v);

        @Join(internal="id", external="prop06")
        public abstract Query<ManyProperties> getProp36() throws FetchException;

        public abstract int getProp37();
        public abstract void setProp37(int v);

        public abstract int getProp38();
        public abstract void setProp38(int v);

        public abstract int getProp39();
        public abstract void setProp39(int v);

        public abstract int getProp40();
        public abstract void setProp40(int v);

        public abstract int getProp41();
        public abstract void setProp41(int v);

        public abstract int getProp42();
        public abstract void setProp42(int v);

        public abstract int getProp43();
        public abstract void setProp43(int v);

        public abstract int getProp44();
        public abstract void setProp44(int v);

        public abstract int getProp45();
        public abstract void setProp45(int v);

        public abstract int getProp46();
        public abstract void setProp46(int v);

        public abstract int getProp47();
        public abstract void setProp47(int v);
    }
}
