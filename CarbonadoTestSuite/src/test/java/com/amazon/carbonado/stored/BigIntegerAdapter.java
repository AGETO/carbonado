/*
 * Copyright 2008-2012 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.stored;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.amazon.carbonado.adapter.AdapterDefinition;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@AdapterDefinition(storageTypePreferences=BigDecimal.class)
public @interface BigIntegerAdapter {
    public static class Adapter {
        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with adapter
         * @param ann specific annotation that binds to this adapter class
         */
        public Adapter(Class<?> type, String propertyName, BigIntegerAdapter ann) {
        }

        public BigDecimal adaptToBigDecimal(BigInteger value) {
            if (value == null) {
                return null;
            }
            // Purposely destroy value in order for test case to detect if
            // adapter was selected or not.
            return new BigDecimal(value.add(BigInteger.ONE), 0);
        }

        public BigInteger adaptToBigInteger(BigDecimal value) {
            if (value == null) {
                return null;
            }
            value = value.stripTrailingZeros();
            if (value.scale() != 0) {
                throw new IllegalArgumentException("Cannot convert to BigInteger: " + value);
            }
            return value.unscaledValue();
        }
    }
}
