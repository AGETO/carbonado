/*
 * Copyright 2009 Amazon Technologies, Inc. or its affiliates.
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
@PrimaryKey("id")
@Indexes({@Index("intProp")})
public abstract class WithJoinIndex implements Storable {
    public static volatile int adjust;

    public abstract int getId();
    public abstract void setId(int id);

    public abstract int getBasicId();
    public abstract void setBasicId(int id);

    @Join(internal="basicId", external="id")
    public abstract Basic getBasic() throws FetchException;
    public abstract void setBasic(Basic basic);

    @Derived(from="basic.intProp")
    public int getIntProp() throws FetchException {
        return getBasic().getIntProp() + adjust;
    }

    @Alias("WITH_BASIC")
    @PrimaryKey("id")
    public static interface Basic extends Storable {
        int getId();
        void setId(int id);

        int getIntProp();
        void setIntProp(int anInt);

        @Join(internal="id", external="basicId")
        Query<WithJoinIndex> getParent() throws FetchException;
    }
}
