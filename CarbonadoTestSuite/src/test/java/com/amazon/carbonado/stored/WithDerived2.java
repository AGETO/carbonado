/*
 * Copyright 2007-2010 Amazon Technologies, Inc. or its affiliates.
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
@PrimaryKey("key")
public abstract class WithDerived2 implements Storable {
    public abstract int getKey();
    public abstract void setKey(int id);

    public abstract int getId();
    public abstract void setId(int id);

    public abstract String getName();
    public abstract void setName(String name);

    @Join
    public abstract StorableTestBasic getBasic() throws FetchException;
    public abstract void setBasic(StorableTestBasic basic);

    // This is an error because "basic" isn't doubly joined.
    @Derived(from="basic.intProp")
    public int getIntProp() throws FetchException, Exception {
        return getBasic().getIntProp();
    }
}
