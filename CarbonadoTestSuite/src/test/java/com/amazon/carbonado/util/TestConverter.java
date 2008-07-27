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

package com.amazon.carbonado.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestConverter extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestConverter.class);
    }

    public TestConverter(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void test_primitive() {
        Converter c = Converter.build(Converter.class);
        test_primitive(c);
        Converter c2 = Converter.build(Converter.class);
    }

    public void test_illegalPrimitive() {
        Converter c = Converter.build(Converter.class);
        test_illegalPrimitive(c);
    }

    public abstract static class ReplaceInt extends Converter {
        public int convertToInt(int value) {
            return value + 1;
        }
    }

    public void test_primitiveReplace() {
        ReplaceInt c = Converter.build(ReplaceInt.class);
        test_illegalPrimitive(c);

        {
            Integer value = c.convert((byte) 5, int.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert((byte) 5, Integer.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert(new Byte((byte) 5), int.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert(new Byte((byte) 5), Integer.class);
            assertEquals(6, value.intValue());
        }

        {
            Integer value = c.convert(5, int.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert(5, Integer.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert(new Integer(5), int.class);
            assertEquals(6, value.intValue());
        }
        {
            Integer value = c.convert(new Integer(5), Integer.class);
            assertEquals(6, value.intValue());
        }

        {
            Long value = c.convert(5, long.class);
            assertEquals(5, value.longValue());
        }
        {
            Long value = c.convert(5, Long.class);
            assertEquals(5, value.longValue());
        }
        {
            Long value = c.convert(new Long(5), long.class);
            assertEquals(5, value.longValue());
        }
        {
            Long value = c.convert(new Long(5), Long.class);
            assertEquals(5, value.longValue());
        }
    }

    public abstract static class ConvertBig extends Converter {
        public BigInteger convertToBigInteger(long value) {
            return BigInteger.valueOf(value);
        }

        public BigDecimal convertToBigDecimal(long value) {
            return BigDecimal.valueOf(value);
        }

        public BigDecimal convertToBigDecimal(double value) {
            return BigDecimal.valueOf(value);
        }
    }

    public void test_big() {
        ConvertBig c = Converter.build(ConvertBig.class);
        test_primitive(c);
        test_illegalPrimitive(c);

        {
            BigInteger big = c.convert(100, BigInteger.class);
            assertEquals(BigInteger.valueOf(100), big);
        }
        {
            BigInteger big = c.convert(new Integer(100), BigInteger.class);
            assertEquals(BigInteger.valueOf(100), big);
        }
        {
            BigInteger big = c.convert(null, BigInteger.class);
            assertEquals(null, big);
        }

        {
            BigDecimal big = c.convert(100, BigDecimal.class);
            assertEquals(BigDecimal.valueOf(100), big);
        }
        {
            BigDecimal big = c.convert(new Integer(100), BigDecimal.class);
            assertEquals(BigDecimal.valueOf(100), big);
        }
        {
            BigDecimal big = c.convert(null, BigDecimal.class);
            assertEquals(null, big);
        }

        {
            BigDecimal big = c.convert(100.2, BigDecimal.class);
            assertEquals(BigDecimal.valueOf(100.2), big);
        }
        {
            BigDecimal big = c.convert(new Float(100.2f), BigDecimal.class);
            assertEquals(BigDecimal.valueOf(100.2f), big);
        }
    }

    public abstract static class ConvertArray extends Converter {
        private final boolean mNegate;

        public ConvertArray(boolean negate) {
            mNegate = negate;
        }

        public byte convertToByte(byte value) {
            if (mNegate) {
                value = (byte) -value;
            }
            return value;
        }

        public int[] convertToIntArray(byte[] value) {
            int[] ia = new int[value.length];
            for (int i=0; i<value.length; i++) {
                ia[i] = convert(value[i], byte.class);
            }
            return ia;
        }
    }

    public void test_array() throws Exception {
        Class<? extends ConvertArray> clazz = Converter.buildClass(ConvertArray.class);

        ConvertArray c = clazz.getConstructor(boolean.class).newInstance(false);
        test_primitive(c);
        test_illegalPrimitive(c);

        int[] ia = c.convert(new byte[] {-1, 2}, int[].class);
        assertEquals(2, ia.length);
        assertEquals(-1, ia[0]);
        assertEquals(2, ia[1]);

        c = clazz.getConstructor(boolean.class).newInstance(true);

        ia = c.convert(new byte[] {-1, 2}, int[].class);
        assertEquals(2, ia.length);
        assertEquals(1, ia[0]);
        assertEquals(-2, ia[1]);
    }

    private void test_primitive(Converter c) {
        // from byte
        {
            {
                Byte obj = c.convert((byte) 10, byte.class);
                assertEquals(10, obj.byteValue());
            }

            {
                Byte obj = c.convert((byte) -10, Byte.class);
                assertEquals(-10, obj.byteValue());
            }

            {
                Short obj = c.convert((byte) -100, short.class);
                assertEquals(-100, obj.shortValue());
            }

            {
                Short obj = c.convert((byte) 100, Short.class);
                assertEquals(100, obj.shortValue());
            }

            {
                Integer obj = c.convert((byte) 100, int.class);
                assertEquals(100, obj.shortValue());
            }

            {
                Integer obj = c.convert((byte) 100, Integer.class);
                assertEquals(100, obj.intValue());
            }

            {
                Long obj = c.convert((byte) -100, long.class);
                assertEquals(-100L, obj.longValue());
            }

            {
                Long obj = c.convert((byte) 100, Long.class);
                assertEquals(100L, obj.longValue());
            }

            {
                Float obj = c.convert((byte) -100, float.class);
                assertEquals(-100.0f, obj.floatValue());
            }

            {
                Float obj = c.convert((byte) 100, Float.class);
                assertEquals(100.0f, obj.floatValue());
            }

            {
                Double obj = c.convert((byte) -100, double.class);
                assertEquals(-100.0, obj.doubleValue());
            }

            {
                Double obj = c.convert((byte) 100, Double.class);
                assertEquals(100.0, obj.doubleValue());
            }

            {
                Number obj = c.convert((byte) 10, Number.class);
                assertTrue(obj instanceof Byte);
                assertEquals(10, obj.byteValue());
            }

            {
                Object obj = c.convert((byte) 10, Object.class);
                assertTrue(obj instanceof Byte);
                assertEquals(10, ((Byte) obj).byteValue());
            }
        }

        // from Byte
        {
            {
                Byte obj = c.convert(new Byte((byte) 10), byte.class);
                assertEquals(10, obj.byteValue());
            }

            {
                Byte obj = c.convert(new Byte((byte) -10), Byte.class);
                assertEquals(-10, obj.byteValue());
            }

            {
                Short obj = c.convert(new Byte((byte) -100), short.class);
                assertEquals(-100, obj.shortValue());
            }

            {
                Short obj = c.convert(new Byte((byte) 100), Short.class);
                assertEquals(100, obj.shortValue());
            }

            {
                Integer obj = c.convert(new Byte((byte) 100), int.class);
                assertEquals(100, obj.shortValue());
            }

            {
                Integer obj = c.convert(new Byte((byte) 100), Integer.class);
                assertEquals(100, obj.intValue());
            }

            {
                Long obj = c.convert(new Byte((byte) -100), long.class);
                assertEquals(-100L, obj.longValue());
            }

            {
                Long obj = c.convert(new Byte((byte) 100), Long.class);
                assertEquals(100L, obj.longValue());
            }

            {
                Float obj = c.convert(new Byte((byte) -100), float.class);
                assertEquals(-100.0f, obj.floatValue());
            }

            {
                Float obj = c.convert(new Byte((byte) 100), Float.class);
                assertEquals(100.0f, obj.floatValue());
            }

            {
                Double obj = c.convert(new Byte((byte) -100), double.class);
                assertEquals(-100.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(new Byte((byte) 100), Double.class);
                assertEquals(100.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(new Byte((byte) 10), Number.class);
                assertTrue(obj instanceof Byte);
                assertEquals(10, obj.byteValue());
            }

            {
                Object obj = c.convert(new Byte((byte) 10), Object.class);
                assertTrue(obj instanceof Byte);
                assertEquals(10, ((Byte) obj).byteValue());
            }
        }

        // from short
        {
            {
                Short obj = c.convert((short) 1000, short.class);
                assertEquals(1000, obj.shortValue());
            }

            {
                Short obj = c.convert((short) 1000, Short.class);
                assertEquals(1000, obj.shortValue());
            }

            {
                Integer obj = c.convert((short) 1000, int.class);
                assertEquals(1000, obj.intValue());
            }

            {
                Integer obj = c.convert((short) 1000, Integer.class);
                assertEquals(1000, obj.intValue());
            }

            {
                Long obj = c.convert((short) 1000, long.class);
                assertEquals(1000L, obj.longValue());
            }

            {
                Long obj = c.convert((short) 1000, Long.class);
                assertEquals(1000L, obj.longValue());
            }

            {
                Float obj = c.convert((short) 1000, float.class);
                assertEquals(1000.0f, obj.floatValue());
            }

            {
                Float obj = c.convert((short) 1000, Float.class);
                assertEquals(1000.0f, obj.floatValue());
            }

            {
                Double obj = c.convert((short) 1000, double.class);
                assertEquals(1000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert((short) 1000, Double.class);
                assertEquals(1000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert((short) 1000, Number.class);
                assertTrue(obj instanceof Short);
                assertEquals(1000, obj.shortValue());
            }

            {
                Object obj = c.convert((short) 1000, Object.class);
                assertTrue(obj instanceof Short);
                assertEquals(1000, ((Short) obj).shortValue());
            }
        }

        // from Short
        {
            {
                Short obj = c.convert(new Short((short) 1000), short.class);
                assertEquals(1000, obj.shortValue());
            }

            {
                Short obj = c.convert(new Short((short) 1000), Short.class);
                assertEquals(1000, obj.shortValue());
            }

            {
                Integer obj = c.convert(new Short((short) 1000), int.class);
                assertEquals(1000, obj.intValue());
            }

            {
                Integer obj = c.convert(new Short((short) 1000), Integer.class);
                assertEquals(1000, obj.intValue());
            }

            {
                Long obj = c.convert(new Short((short) 1000), long.class);
                assertEquals(1000L, obj.longValue());
            }

            {
                Long obj = c.convert(new Short((short) 1000), Long.class);
                assertEquals(1000L, obj.longValue());
            }

            {
                Float obj = c.convert(new Short((short) 1000), float.class);
                assertEquals(1000.0f, obj.floatValue());
            }

            {
                Float obj = c.convert(new Short((short) 1000), Float.class);
                assertEquals(1000.0f, obj.floatValue());
            }

            {
                Double obj = c.convert(new Short((short) 1000), double.class);
                assertEquals(1000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(new Short((short) 1000), Double.class);
                assertEquals(1000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(new Short((short) 1000), Number.class);
                assertTrue(obj instanceof Short);
                assertEquals(1000, obj.shortValue());
            }

            {
                Object obj = c.convert(new Short((short) 1000), Object.class);
                assertTrue(obj instanceof Short);
                assertEquals(1000, ((Short) obj).shortValue());
            }
        }

        // from int
        {
            {
                Integer obj = c.convert(-100000, int.class);
                assertEquals(-100000, obj.intValue());
            }

            {
                Integer obj = c.convert(100000, Integer.class);
                assertEquals(100000, obj.intValue());
            }

            {
                Long obj = c.convert(-100000, long.class);
                assertEquals(-100000L, obj.longValue());
            }

            {
                Long obj = c.convert(100000, Long.class);
                assertEquals(100000L, obj.longValue());
            }

            {
                Double obj = c.convert(-100000, double.class);
                assertEquals(-100000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(100000, Double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(100000, Number.class);
                assertTrue(obj instanceof Integer);
                assertEquals(100000, obj.intValue());
            }

            {
                Object obj = c.convert(100000, Object.class);
                assertTrue(obj instanceof Integer);
                assertEquals(100000, ((Integer) obj).intValue());
            }
        }

        // from Integer
        {
            {
                Integer obj = c.convert(new Integer(-100000), int.class);
                assertEquals(-100000, obj.intValue());
            }

            {
                Integer obj = c.convert(new Integer(100000), Integer.class);
                assertEquals(100000, obj.intValue());
            }

            {
                Long obj = c.convert(new Integer(-100000), long.class);
                assertEquals(-100000L, obj.longValue());
            }

            {
                Long obj = c.convert(new Integer(100000), Long.class);
                assertEquals(100000L, obj.longValue());
            }

            {
                Double obj = c.convert(new Integer(-100000), double.class);
                assertEquals(-100000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(new Integer(100000), Double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(new Integer(100000), Number.class);
                assertTrue(obj instanceof Integer);
                assertEquals(100000, obj.intValue());
            }

            {
                Object obj = c.convert(new Integer(100000), Object.class);
                assertTrue(obj instanceof Integer);
                assertEquals(100000, ((Integer) obj).intValue());
            }
        }

        // from long
        {
            {
                Long obj = c.convert(10000000000L, long.class);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Long obj = c.convert(10000000000L, Long.class);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Number obj = c.convert(10000000000L, Number.class);
                assertTrue(obj instanceof Long);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Object obj = c.convert(10000000000L, Object.class);
                assertTrue(obj instanceof Long);
                assertEquals(10000000000L, ((Long) obj).longValue());
            }
        }

        // from Long
        {
            {
                Long obj = c.convert(new Long(10000000000L), long.class);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Long obj = c.convert(new Long(10000000000L), Long.class);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Number obj = c.convert(new Long(10000000000L), Number.class);
                assertTrue(obj instanceof Long);
                assertEquals(10000000000L, obj.longValue());
            }

            {
                Object obj = c.convert(new Long(10000000000L), Object.class);
                assertTrue(obj instanceof Long);
                assertEquals(10000000000L, ((Long) obj).longValue());
            }
        }

        // from float
        {
            {
                Float obj = c.convert(100000f, float.class);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Float obj = c.convert(100000f, Float.class);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Double obj = c.convert(100000f, double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(100000f, Double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(100000f, Number.class);
                assertTrue(obj instanceof Float);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Object obj = c.convert(100000f, Object.class);
                assertTrue(obj instanceof Float);
                assertEquals(100000f, ((Float) obj).floatValue());
            }
        }

        // from Float
        {
            {
                Float obj = c.convert(new Float(100000f), float.class);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Float obj = c.convert(new Float(100000f), Float.class);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Double obj = c.convert(new Float(100000f), double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Double obj = c.convert(new Float(100000f), Double.class);
                assertEquals(100000.0, obj.doubleValue());
            }

            {
                Number obj = c.convert(new Float(100000f), Number.class);
                assertTrue(obj instanceof Float);
                assertEquals(100000f, obj.floatValue());
            }

            {
                Object obj = c.convert(new Float(100000f), Object.class);
                assertTrue(obj instanceof Float);
                assertEquals(100000f, ((Float) obj).floatValue());
            }
        }

        // from double
        {
            {
                Double obj = c.convert(Math.PI, double.class);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Double obj = c.convert(Math.PI, Double.class);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Number obj = c.convert(Math.PI, Number.class);
                assertTrue(obj instanceof Double);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Object obj = c.convert(Math.PI, Object.class);
                assertTrue(obj instanceof Double);
                assertEquals(Math.PI, ((Double) obj).doubleValue());
            }
        }

        // from Double
        {
            {
                Double obj = c.convert(new Double(Math.PI), double.class);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Double obj = c.convert(new Double(Math.PI), Double.class);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Number obj = c.convert(new Double(Math.PI), Number.class);
                assertTrue(obj instanceof Double);
                assertEquals(Math.PI, obj.doubleValue());
            }

            {
                Object obj = c.convert(new Double(Math.PI), Object.class);
                assertTrue(obj instanceof Double);
                assertEquals(Math.PI, ((Double) obj).doubleValue());
            }
        }

        // from char
        {
            {
                Character obj = c.convert('a', char.class);
                assertEquals('a', obj.charValue());
            }

            {
                Character obj = c.convert('a', Character.class);
                assertEquals('a', obj.charValue());
            }

            {
                Object obj = c.convert('a', Object.class);
                assertTrue(obj instanceof Character);
                assertEquals('a', ((Character) obj).charValue());
            }
        }

        // from Character
        {
            {
                Character obj = c.convert(new Character('a'), char.class);
                assertEquals('a', obj.charValue());
            }

            {
                Character obj = c.convert(new Character('a'), Character.class);
                assertEquals('a', obj.charValue());
            }

            {
                Object obj = c.convert(new Character('a'), Object.class);
                assertTrue(obj instanceof Character);
                assertEquals('a', ((Character) obj).charValue());
            }
        }

        // from boolean
        {
            {
                Boolean obj = c.convert(true, boolean.class);
                assertEquals(true, obj.booleanValue());
                assertTrue(obj == Boolean.TRUE);
            }

            {
                Boolean obj = c.convert(true, Boolean.class);
                assertEquals(true, obj.booleanValue());
                assertTrue(obj == Boolean.TRUE);
            }

            {
                Object obj = c.convert(false, Object.class);
                assertTrue(obj instanceof Boolean);
                assertEquals(false, ((Boolean) obj).booleanValue());
                assertTrue(obj == Boolean.FALSE);
            }
        }

        // from Boolean
        {
            {
                Boolean obj = c.convert(Boolean.TRUE, boolean.class);
                assertEquals(true, obj.booleanValue());
                assertTrue(obj == Boolean.TRUE);
            }

            {
                Boolean obj = c.convert(Boolean.TRUE, Boolean.class);
                assertEquals(true, obj.booleanValue());
                assertTrue(obj == Boolean.TRUE);
            }

            {
                Object obj = c.convert(Boolean.FALSE, Object.class);
                assertTrue(obj instanceof Boolean);
                assertEquals(false, ((Boolean) obj).booleanValue());
                assertTrue(obj == Boolean.FALSE);
            }
        }

        // from null
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                double.class, Double.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                Object obj = c.convert(null, type);
                assertEquals(null, obj);
            }
        }
    }

    private void test_illegalPrimitive(Converter c) {
        // from byte and Byte
        {
            Class[] types = {
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert((byte) 10, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Byte((byte) 10), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from short and Short
        {
            Class[] types = {
                byte.class, Byte.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert((short) 1000, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Short((short) 1000), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from int and Integer
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                float.class, Float.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert(100000, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Integer(100000), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from long and Long
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                float.class, Float.class,
                double.class, Double.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert(10000000000L, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Long(10000000000L), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from float and Float
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert(10f, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Float(10f), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from double and Double
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                char.class, Character.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert(10.0, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Double(10.0), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from char and Character
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                double.class, Double.class,
                boolean.class, Boolean.class,
            };

            for (Class type : types) {
                try {
                    c.convert('a', type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(new Character('a'), type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }

        // from boolean and Boolean
        {
            Class[] types = {
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                double.class, Double.class,
                char.class, Character.class,
            };

            for (Class type : types) {
                try {
                    c.convert(true, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
                try {
                    c.convert(Boolean.TRUE, type);
                    fail();
                } catch (IllegalArgumentException e) {
                }
            }
        }
    }
}
