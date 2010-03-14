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

package com.amazon.carbonado.repo.toy;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.gen.DelegateStorableGenerator;
import com.amazon.carbonado.gen.DelegateSupport;
import com.amazon.carbonado.gen.MasterFeature;

import com.amazon.carbonado.util.QuickConstructorGenerator;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.QueryFactory;
import com.amazon.carbonado.qe.QueryHints;
import com.amazon.carbonado.qe.SortedQueryExecutor;
import com.amazon.carbonado.qe.FilteredQueryExecutor;
import com.amazon.carbonado.qe.IterableQueryExecutor;
import com.amazon.carbonado.qe.OrderingList;
import com.amazon.carbonado.qe.QueryExecutor;
import com.amazon.carbonado.qe.StandardQuery;

/**
 *
 * @author Brian S O'Neill
 */
public class ToyStorage<S extends Storable>
    implements Storage<S>, DelegateSupport<S>, QueryFactory<S>, QueryExecutorFactory<S>
{
    final ToyRepository mRepo;
    final Class<S> mType;

    final InstanceFactory mInstanceFactory;

    final Collection<S> mData;
    final Lock mDataLock;

    public ToyStorage(ToyRepository repo, Class<S> type) throws SupportException {
        StorableIntrospector.examine(type);
        mRepo = repo;
        mType = type;

        EnumSet<MasterFeature> features = EnumSet
            .of(MasterFeature.VERSIONING, MasterFeature.INSERT_SEQUENCES);

        Class<? extends S> delegateStorableClass =
            DelegateStorableGenerator.getDelegateClass(type, features);

        mInstanceFactory = QuickConstructorGenerator
            .getInstance(delegateStorableClass, InstanceFactory.class);

        mData = new LinkedList<S>();
        mDataLock = new ReentrantLock();
    }

    public Class<S> getStorableType() {
        return mType;
    }

    public S prepare() {
        return (S) mInstanceFactory.instantiate(this);
    }

    public Query<S> query() {
        return new ToyQuery(null, null, null);
    }

    public Query<S> query(String filter) {
        return query(Filter.filterFor(mType, filter));
    }

    public Query<S> query(Filter<S> filter) {
        return new ToyQuery(filter, null, null);
    }

    public Query<S> query(Filter<S> filter, FilterValues<S> values, OrderingList<S> ordering,
                          QueryHints hints)
    {
        return new ToyQuery(filter, values, ordering);
    }

    public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering,
                                     QueryHints hints)
    {
        QueryExecutor<S> executor = new IterableQueryExecutor<S>(mType, mData, mDataLock);

        if (filter != null) {
            executor = new FilteredQueryExecutor<S>(executor, filter);
        }

        if (ordering.size() > 0) {
            executor = new SortedQueryExecutor<S>(null, executor, null, ordering);
        }

        return executor;
    }

    public void truncate() {
        mDataLock.lock();
        try {
            mData.clear();
        } finally {
            mDataLock.unlock();
        }
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return false;
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return false;
    }

    public boolean doTryLoad(S storable) {
        mDataLock.lock();
        try {
            for (S existing : mData) {
                if (existing.equalPrimaryKeys(storable)) {
                    storable.markAllPropertiesDirty();
                    existing.copyAllProperties(storable);
                    storable.markAllPropertiesClean();
                    return true;
                }
            }
            return false;
        } finally {
            mDataLock.unlock();
        }
    }

    public boolean doTryInsert(S storable) {
        mDataLock.lock();
        try {
            for (S existing : mData) {
                if (existing.equalPrimaryKeys(storable)) {
                    return false;
                }
            }
            storable.markAllPropertiesClean();
            mData.add((S) storable.copy());
            return true;
        } finally {
            mDataLock.unlock();
        }
    }

    public boolean doTryUpdate(S storable) {
        mDataLock.lock();
        try {
            for (S existing : mData) {
                if (existing.equalPrimaryKeys(storable)) {
                    // Copy altered values to existing object.
                    existing.markAllPropertiesDirty();
                    storable.copyAllProperties(existing);
                    existing.markAllPropertiesClean();

                    // Copy all values to user object, to simulate a reload.
                    storable.markAllPropertiesDirty();
                    existing.copyAllProperties(storable);
                    storable.markAllPropertiesClean();

                    return true;
                }
            }
            return false;
        } finally {
            mDataLock.unlock();
        }
    }

    public boolean doTryDelete(S storable) {
        mDataLock.lock();
        try {
            Iterator<S> it = mData.iterator();
            while (it.hasNext()) {
                S existing = it.next();
                if (existing.equalPrimaryKeys(storable)) {
                    it.remove();
                    return true;
                }
            }
            return false;
        } finally {
            mDataLock.unlock();
        }
    }

    public Repository getRootRepository() {
        return mRepo;
    }

    public boolean isPropertySupported(String propertyName) {
        return StorableIntrospector.examine(mType)
            .getAllProperties().containsKey(propertyName);
    }

    public Trigger<? super S> getInsertTrigger() {
        return null;
    }

    public Trigger<? super S> getUpdateTrigger() {
        return null;
    }

    public Trigger<? super S> getDeleteTrigger() {
        return null;
    }

    public Trigger<? super S> getLoadTrigger() {
        return null;
    }

    public void locallyDisableLoadTrigger() {
    }

    public void locallyEnableLoadTrigger() {
    }

    public SequenceValueProducer getSequenceValueProducer(String name) throws PersistException {
        try {
            return mRepo.getSequenceValueProducer(name);
        } catch (RepositoryException e) {
            throw e.toPersistException();
        }
    }

    public static interface InstanceFactory {
        Storable instantiate(DelegateSupport support);
    }

    private class ToyQuery extends StandardQuery<S> {
        ToyQuery(Filter<S> filter,
                 FilterValues<S> values,
                 OrderingList<S> ordering)
        {
            super(filter, values, ordering, null);
        }

        protected Transaction enterTransaction(IsolationLevel level) {
            return mRepo.enterTransaction(level);
        }

        protected QueryFactory<S> queryFactory() {
            return ToyStorage.this;
        }

        protected QueryExecutorFactory<S> executorFactory() {
            return ToyStorage.this;
        }

        protected StandardQuery<S> newInstance(FilterValues<S> values,
                                               OrderingList<S> ordering,
                                               QueryHints hints)
        {
            return new ToyQuery(values.getFilter(), values, ordering);
        }
    }
}
