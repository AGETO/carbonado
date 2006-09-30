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

package com.amazon.carbonado.repo.sleepycat;

/**
 * Set of supported BDB product versions.
 *
 * @author Brian S O'Neill
 */
public enum BDBVersion {
    /** BDB version 4.1 */
    DB4_1,

    /** BDB version 4.3 and up */
    DB4_3,

    /** BDB version 4.4 and up */
    DB4_4,

    /** BDB version 4.5 and up */
    DB4_5,

    /** BDB version 4.4 and up with HA add-ons */
    DBHA4_4,

    /** BDB Java Edition version 2.0 and up */
    JE2_0;

    public static BDBVersion forString(String name) {
        name = name.toLowerCase();
        if (name.startsWith("db4.0")) {
            // Unsupported.
        } else if (name.startsWith("db4.1")) {
            return DB4_1;
        } else if (name.startsWith("db4.2")) {
            // Unsupported.
        } else if (name.startsWith("db4.3")) {
            return DB4_3;
        } else if (name.startsWith("db4.4")) {
            return DB4_4;
        } else if (name.startsWith("db4.")) {
            return DB4_5;
        } else if (name.startsWith("dbha4.")) {
            return DBHA4_4;
        } else if (name.startsWith("je2.")) {
            return JE2_0;
        }
        throw new IllegalArgumentException("Unsupported version: " + name);
    }

    public String toString() {
        switch (this) {
        case DB4_1:
            return "db4.1";
        case DB4_3:
            return "db4.3";
        case DB4_4:
            return "db4.4";
        case DBHA4_4:
            return "dbha4.4";
        case DB4_5:
            return "db4.5";
        case JE2_0: default:
            return "je2.0";
        }
    }
}
