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
@PrimaryKey("ID")
@Indexes({@Index("average"), @Index("averageObject")})
public abstract class WithFunctionIndex implements Storable {
    public abstract int getID();
    public abstract void setID(int id);

    @Alias("THE_SUM")
    public abstract int getSum();
    public abstract void setSum(int sum);

    @Alias("THE_COUNT")
    public abstract int getCount();
    public abstract void setCount(int count);

    @Derived
    public double getAverage() {
        return ((double) getSum()) / getCount();
    }

    @Derived
    @Nullable
    public Double getAverageObject() {
        return ((double) getSum()) / getCount();
    }
}
