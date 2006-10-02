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

import com.sleepycat.db.Db;
import com.sleepycat.db.Dbc;
import com.sleepycat.db.Dbt;
import com.sleepycat.db.DbException;
import com.sleepycat.db.DbTxn;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Storable;

/**
 * Cursor for a primary database.
 *
 * @author Brian S O'Neill
 */
class DB_Legacy_Cursor<S extends Storable> extends BDBCursor<DbTxn, S> {
    private final Db mDatabase;
    private final int mLockMode;
    private Dbt mSearchKey;
    private Dbt mData;

    private Dbc mCursor;

    /**
     * @param txnMgr
     * @param startBound specify the starting key for the cursor, or null if first
     * @param inclusiveStart true if start bound is inclusive
     * @param endBound specify the ending key for the cursor, or null if last
     * @param inclusiveEnd true if end bound is inclusive
     * @param maxPrefix maximum expected common initial bytes in start and end bound
     * @param reverse when true, iteration is reversed
     * @param storage
     * @param database primary database to use
     * @throws IllegalArgumentException if any bound is null but is not inclusive
     */
    DB_Legacy_Cursor(BDBTransactionManager<DbTxn> txnMgr,
                     byte[] startBound, boolean inclusiveStart,
                     byte[] endBound, boolean inclusiveEnd,
                     int maxPrefix,
                     boolean reverse,
                     DB_Legacy_Storage<S> storage,
                     Db database)
        throws DbException, FetchException
    {
        super(txnMgr, startBound, inclusiveStart, endBound, inclusiveEnd,
              maxPrefix, reverse, storage);

        mDatabase = database;
        mLockMode = txnMgr.isForUpdate() ? Db.DB_RMW : 0;

        mSearchKey = DB_Legacy_DbtPool.createDbt();
        mData = DB_Legacy_DbtPool.createDbt();
    }

    protected byte[] searchKey_getData() {
        return getData(mSearchKey.get_data(), mSearchKey.get_size());
    }

    protected byte[] searchKey_getDataCopy() {
        return getDataCopy(mSearchKey.get_data(), mSearchKey.get_size());
    }

    protected void searchKey_setData(byte[] data) {
        mSearchKey.set_data(data);
        mSearchKey.set_size(data.length);
    }

    protected void searchKey_setPartial(boolean partial) {
        mSearchKey.set_doff(0);
        mSearchKey.set_dlen(0);
        int flags = mSearchKey.get_flags();
        if (partial) {
            flags |= Db.DB_DBT_PARTIAL;
        } else {
            flags &= ~Db.DB_DBT_PARTIAL;
        }
        mSearchKey.set_flags(flags);
    }

    protected boolean searchKey_getPartial() {
        return (mSearchKey.get_flags() & Db.DB_DBT_PARTIAL) != 0;
    }

    protected byte[] data_getData() {
        return getData(mData.get_data(), mData.get_size());
    }

    protected byte[] data_getDataCopy() {
        return getDataCopy(mData.get_data(), mData.get_size());
    }

    protected void data_setPartial(boolean partial) {
        mData.set_doff(0);
        mData.set_dlen(0);
        int flags = mData.get_flags();
        if (partial) {
            flags |= Db.DB_DBT_PARTIAL;
        } else {
            flags &= ~Db.DB_DBT_PARTIAL;
        }
        mData.set_flags(flags);
    }

    protected boolean data_getPartial() {
        return (mData.get_flags() & Db.DB_DBT_PARTIAL) != 0;
    }

    protected byte[] primaryKey_getData() {
        // Search key is primary key.
        return getData(mSearchKey.get_data(), mSearchKey.get_size());
    }

    protected void cursor_open(DbTxn txn, IsolationLevel level) throws Exception {
        int flags = 0;
        if (level == IsolationLevel.READ_UNCOMMITTED) {
            flags |= Db.DB_DIRTY_READ;
        }
        mCursor = mDatabase.cursor(txn, flags);
    }

    protected void cursor_close() throws Exception {
        mCursor.close();
        mCursor = null;
        DB_Legacy_DbtPool.recycleDbt(mSearchKey);
        mSearchKey = null;
        DB_Legacy_DbtPool.recycleDbt(mData);
        mData = null;
    }

    protected boolean cursor_getCurrent() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_CURRENT | mLockMode) == 0;
    }

    protected boolean cursor_getFirst() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_FIRST | mLockMode) == 0;
    }

    protected boolean cursor_getLast() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_LAST | mLockMode) == 0;
    }

    protected boolean cursor_getSearchKeyRange() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_SET_RANGE | mLockMode) == 0;
    }

    protected boolean cursor_getNext() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_NEXT | mLockMode) == 0;
    }

    protected boolean cursor_getNextDup() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_NEXT_DUP | mLockMode) == 0;
    }

    protected boolean cursor_getPrev() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_PREV | mLockMode) == 0;
    }

    protected boolean cursor_getPrevNoDup() throws Exception {
        return mCursor.get(mSearchKey, mData, Db.DB_PREV_NODUP | mLockMode) == 0;
    }
}
