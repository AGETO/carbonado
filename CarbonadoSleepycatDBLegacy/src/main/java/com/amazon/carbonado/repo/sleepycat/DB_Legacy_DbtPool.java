/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.sleepycat.db.Dbt;

/**
 * Creating and finalizing Dbt instances is expensive due to finalization and
 * native malloc calls. Recycle instances here to improve performance and
 * reduce overall memory overhead.
 * <p>
 * This class is not needed with newer versions of BDB, as they do not rely on
 * finalization.
 *
 * @author Brian S O'Neill
 */
class DB_Legacy_DbtPool {
    // Only keep this many instances per thread. I don't expect more than about
    // 6 Dbt instances to ever be needed by a thread anyhow.
    private static final int POOL_SIZE = 20;

    private static ThreadLocal<DB_Legacy_DbtPool> cInstance = new ThreadLocal<DB_Legacy_DbtPool>();

    /**
     * Create an empty Dbt. Call recycleDbt when done with it.
     */
    static Dbt createDbt() {
        Dbt dbt = getPool().getDbt();
        dbt.set_offset(0);
        dbt.set_size(0);
        return dbt;
    }

    /**
     * Create a Dbt with the given value. Call recycleDbt when done with it.
     */
    static Dbt createDbt(byte[] value) {
        return createDbt(value, 0, value.length);
    }

    /**
     * Create a Dbt with the given value. Call recycleDbt when done with it.
     */
    static Dbt createDbt(byte[] value, int offset, int length) {
        Dbt dbt = getPool().getDbt();
        dbt.set_data(value);
        dbt.set_offset(offset);
        dbt.set_size(length);
        return dbt;
    }

    static void recycleDbt(Dbt dbt) {
        if (dbt != null) {
            getPool().putDbt(dbt);
        }
    }

    private static DB_Legacy_DbtPool getPool() {
        DB_Legacy_DbtPool pool = cInstance.get();
        if (pool == null) {
            pool = new DB_Legacy_DbtPool();
            cInstance.set(pool);
        }
        return pool;
    }

    private final Dbt[] mPool;
    private int mTop;

    private DB_Legacy_DbtPool() {
        mPool = new Dbt[POOL_SIZE];
    }

    private Dbt getDbt() {
        int top = mTop;
        if (top > 0) {
            mTop = --top;
            return mPool[top];
        }
        return new Dbt();
    }

    private void putDbt(Dbt dbt) {
        // Allow byte array to be garbage collected.
        dbt.set_data(null);

        Dbt[] pool = mPool;
        int top = mTop;
        if (top >= pool.length) {
            // Just let garbage collector take care of it. If the pool is
            // allowed to grow, then we'd need some mechanism to limit its
            // growth. It doesn't seem to be worth the effort.
            return;
        }

        // Restore back to defaults.
        dbt.set_ulen(0);
        dbt.set_dlen(0);
        dbt.set_doff(0);
        dbt.set_flags(0);

        pool[top] = dbt;
        mTop = top + 1;
    }
}
