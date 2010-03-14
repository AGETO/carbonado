/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Random;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Join;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.FetchException;

/**
 * STBContainer
 *
 * @author Don Schneider
 */
@PrimaryKey("name")
public abstract class STBContainer implements Storable {
    public abstract String getName();
    public abstract void setName(String name);

    public abstract String getCategory();
    public abstract void setCategory(String cat);

    public abstract int getCount();
    public abstract void setCount(int count);

    @Join(internal="category", external="stringProp")
    public abstract Query<StorableTestBasic> getContained() throws FetchException;

    // Interesting join to same instance
    @Join
    public abstract STBContainer getSelf() throws FetchException;

    // This might fail sometimes
    public abstract void setSelf(STBContainer newSelf);

    public void initProperties(int count) {
        setName("STBContainer_" + count);
        setCount(count);
        setCategory("imaString_" + count % 10);
    }

    public static void insertBunches(Repository repository, int count)
            throws RepositoryException
    {
        Storage<STBContainer> storage =
                repository.storageFor(STBContainer.class);
        STBContainer s;

        // TODO: come up with an XML based mechanism to inject a boatload of data into an empty repo
        for (int i = 0; i < count; i ++) {
            s = storage.prepare();
            s.initProperties(i);
            s.insert();
        }
    }

}
