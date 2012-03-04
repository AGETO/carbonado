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

package com.amazon.carbonado.stored;

import com.amazon.carbonado.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("aid")
@Indexes(@Index("DName"))
public abstract class WithDerivedChainA implements Storable {
    public abstract int getAid();
    public abstract void setAid(int id);

    public abstract int getBid();
    public abstract void setBid(int id);

    public abstract String getName();
    public abstract void setName(String name);

    @Join
    public abstract WithDerivedChainB getB() throws FetchException;
    public abstract void setB(WithDerivedChainB b);

    @Derived(from="b.c.d.name")
    public String getDName() throws FetchException {
        return getB().getC().getD().getName();
    }

    @Derived(from="DName")
    public String getUpperDName() throws FetchException {
        return getDName().toUpperCase();
    }
}
