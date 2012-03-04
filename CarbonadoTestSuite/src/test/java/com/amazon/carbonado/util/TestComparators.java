/*
 * Copyright 2007-2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

import com.amazon.carbonado.stored.Dummy;
import com.amazon.carbonado.stored.StorableTestMinimal;

/**
 *
 * @author Brian S O'Neill
 */
public class TestComparators extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestComparators.class);
    }

    public TestComparators(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void testSignedByteArray() {
        Comparator<byte[]> c = Comparators.arrayComparator(byte[].class, false);

        {
            byte[] a = {};
            byte[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {1};
            byte[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {-1};
            byte[] b = {1};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            byte[] a = {Byte.MAX_VALUE};
            byte[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {1, 2, 4};
            byte[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testUnsignedByteArray() {
        Comparator<byte[]> c = Comparators.arrayComparator(byte[].class, true);

        {
            byte[] a = {};
            byte[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {1};
            byte[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {-1};
            byte[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[] a = {Byte.MAX_VALUE};
            byte[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[] a = {1, 2, 4};
            byte[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testSignedShortArray() {
        Comparator<short[]> c = Comparators.arrayComparator(short[].class, false);

        {
            short[] a = {};
            short[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {1};
            short[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {-1};
            short[] b = {1};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            short[] a = {Short.MAX_VALUE};
            short[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            short[] a = {1, 2, 3};
            short[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {1, 2, 4};
            short[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            short[] a = {1, 2, 3};
            short[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testUnsignedShortArray() {
        Comparator<short[]> c = Comparators.arrayComparator(short[].class, true);

        {
            short[] a = {};
            short[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {1};
            short[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {-1};
            short[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            short[] a = {Short.MAX_VALUE};
            short[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            short[] a = {1, 2, 3};
            short[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            short[] a = {1, 2, 4};
            short[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            short[] a = {1, 2, 3};
            short[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testSignedIntArray() {
        Comparator<int[]> c = Comparators.arrayComparator(int[].class, false);

        {
            int[] a = {};
            int[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {1};
            int[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {-1};
            int[] b = {1};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            int[] a = {Integer.MAX_VALUE};
            int[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {1, 2, 4};
            int[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testUnsignedIntArray() {
        Comparator<int[]> c = Comparators.arrayComparator(int[].class, true);

        {
            int[] a = {};
            int[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {1};
            int[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {-1};
            int[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            int[] a = {Integer.MAX_VALUE};
            int[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            int[] a = {1, 2, 4};
            int[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testSignedLongArray() {
        Comparator<long[]> c = Comparators.arrayComparator(long[].class, false);

        {
            long[] a = {};
            long[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {1};
            long[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {-1};
            long[] b = {1};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            long[] a = {Long.MAX_VALUE};
            long[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            long[] a = {1, 2, 3};
            long[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {1, 2, 4};
            long[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            long[] a = {1, 2, 3};
            long[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testUnsignedLongArray() {
        Comparator<long[]> c = Comparators.arrayComparator(long[].class, true);

        {
            long[] a = {};
            long[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {1};
            long[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {-1};
            long[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            long[] a = {Long.MAX_VALUE};
            long[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            long[] a = {1, 2, 3};
            long[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            long[] a = {1, 2, 4};
            long[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            long[] a = {1, 2, 3};
            long[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            long[] a = {Long.MAX_VALUE};
            long[] b = {Long.MAX_VALUE - 1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }
    }

    public void testBooleanArray() {
        Comparator<boolean[]> c = Comparators.arrayComparator(boolean[].class, false);

        {
            boolean[] a = {};
            boolean[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            boolean[] a = {true};
            boolean[] b = {true};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            boolean[] a = {false};
            boolean[] b = {true};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            boolean[] a = {false, true, false};
            boolean[] b = {false, true, false};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            boolean[] a = {false, true, true};
            boolean[] b = {false, true, false};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            boolean[] a = {false, true, false};
            boolean[] b = {false, true, false, false};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testCharArray() {
        Comparator<char[]> c = Comparators.arrayComparator(char[].class, false);

        {
            char[] a = {};
            char[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            char[] a = {1};
            char[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            char[] a = {65535};
            char[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            char[] a = {32767};
            char[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            char[] a = {1, 2, 3};
            char[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            char[] a = {1, 2, 4};
            char[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            char[] a = {1, 2, 3};
            char[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testFloatArray() {
        Comparator<float[]> c = Comparators.arrayComparator(float[].class, false);

        {
            float[] a = {};
            float[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            float[] a = {1};
            float[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            float[] a = {Float.POSITIVE_INFINITY};
            float[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            float[] a = {Float.NaN};
            float[] b = {Float.POSITIVE_INFINITY};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            float[] a = {1, 2, 3};
            float[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            float[] a = {1, 2, 4};
            float[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            float[] a = {1, 2, 3};
            float[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testDoubleArray() {
        Comparator<double[]> c = Comparators.arrayComparator(double[].class, false);

        {
            double[] a = {};
            double[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            double[] a = {1};
            double[] b = {1};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            double[] a = {Double.POSITIVE_INFINITY};
            double[] b = {1};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            double[] a = {Double.NaN};
            double[] b = {Double.POSITIVE_INFINITY};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            double[] a = {1, 2, 3};
            double[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            double[] a = {1, 2, 4};
            double[] b = {1, 2, 3};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            double[] a = {1, 2, 3};
            double[] b = {1, 2, 3, 4};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testComparableArray() {
        Comparator<String[]> c = Comparators.arrayComparator(String[].class, false);

        {
            String[] a = {};
            String[] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            String[] a = {"1"};
            String[] b = {"1"};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            String[] a = {null};
            String[] b = {"1"};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            String[] a = {"\uffff"};
            String[] b = {"1"};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            String[] a = {"a"};
            String[] b = {"B"};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            String[] a = {"1", "2", "3"};
            String[] b = {"1", "2", "3"};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            String[] a = {"1", "2", "4"};
            String[] b = {"1", "2", "3"};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            String[] a = {"1", "2", "3"};
            String[] b = {"1", "2", "3", "4"};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }

    public void testUnsignedByteArrayArray() {
        Comparator<byte[][]> c = Comparators.arrayComparator(byte[][].class, true);

        {
            byte[][] a = {};
            byte[][] b = {};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[][] a = {{}};
            byte[][] b = {{}};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[][] a = {null};
            byte[][] b = {{1}};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[][] a = {{1, 2, 3}, {3, 4, 5}};
            byte[][] b = {{1, 2, 3}, {3, 4, 5}};
            assertTrue(c.compare(a, b) == 0);
        }

        {
            byte[][] a = {{1, 2, 3}, {3, 4, 5}};
            byte[][] b = {{1, 2, 3}};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[][] a = {{1, 2, 4}, {3, 4, 5}};
            byte[][] b = {{1, 2, 3}, {3, 4, 5}};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[][] a = {{1, 2, 3}, {3, 4, 4}};
            byte[][] b = {{1, 2, 3}, {3, 4, 5}};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            byte[][] a = {{1, 2, 3}, {3, 4, 4}};
            byte[][] b = {{1, 2, 3}, {3, 4, 5, 1, 2, 3}};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }

        {
            byte[][] a = {{1, 2, 3, 4}, {3, 4, 4}};
            byte[][] b = {{1, 2, 3, 2}, {3, 4, 5, 1, 2, 3}};
            assertTrue(c.compare(a, b) > 0);
            assertTrue(c.compare(b, a) < 0);
        }

        {
            byte[][] a = {{1, 2, 3}, {3, 4, 4}};
            byte[][] b = {{1, 2, 3}, null};
            assertTrue(c.compare(a, b) < 0);
            assertTrue(c.compare(b, a) > 0);
        }
    }
}
