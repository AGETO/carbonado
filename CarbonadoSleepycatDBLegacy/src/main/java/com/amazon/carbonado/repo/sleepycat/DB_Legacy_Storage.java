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

import java.io.FileNotFoundException;

import com.sleepycat.db.Db;
import com.sleepycat.db.Dbc;
import com.sleepycat.db.Dbt;
import com.sleepycat.db.DbEnv;
import com.sleepycat.db.DbException;
import com.sleepycat.db.DbTxn;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * Storage implementation for DBRepository.
 *
 * @author Brian S O'Neill
 */
class DB_Legacy_Storage<S extends Storable> extends BDBStorage<DbTxn, S> {
    // Primary database of Storable instances
    private Db mDatabase;

    /**
     *
     * @param repository repository reference
     * @param storableFactory factory for emitting storables
     * @param db database for Storables
     * @throws DbException
     * @throws SupportException
     */
    DB_Legacy_Storage(DB_Legacy_Repository repository, Class<S> type)
        throws DbException, RepositoryException
    {
        super(repository, type);
        open(repository.mReadOnly);
    }

    protected boolean db_exists(DbTxn txn, byte[] key, boolean rmw) throws Exception {
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt(key);
        Dbt dataEntry = DB_Legacy_DbtPool.createDbt();

        // Set partial to retrieve no value.
        dataEntry.set_doff(0);
        dataEntry.set_dlen(0);
        dataEntry.set_flags(dataEntry.get_flags() | Db.DB_DBT_PARTIAL);

        try {
            int status = mDatabase.get(txn, keyEntry, dataEntry, rmw ? Db.DB_RMW : 0);
            return status != Db.DB_NOTFOUND;
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
            DB_Legacy_DbtPool.recycleDbt(dataEntry);
        }
    }

    protected byte[] db_get(DbTxn txn, byte[] key, boolean rmw) throws Exception {
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt(key);
        Dbt dataEntry = DB_Legacy_DbtPool.createDbt();
        try {
            int status = mDatabase.get(txn, keyEntry, dataEntry, rmw ? Db.DB_RMW : 0);
            if (status == Db.DB_NOTFOUND) {
                return NOT_FOUND;
            }
            return dataEntry.get_data();
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
            DB_Legacy_DbtPool.recycleDbt(dataEntry);
        }
    }

    protected Object db_putNoOverwrite(DbTxn txn, byte[] key, byte[] value)
        throws Exception
    {
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt(key);
        Dbt dataEntry = DB_Legacy_DbtPool.createDbt(value);
        int flags = Db.DB_NOOVERWRITE;
        if (txn == null) {
            flags |= Db.DB_AUTO_COMMIT;
        }
        try {
            int status = mDatabase.put(txn, keyEntry, dataEntry, flags);
            if (status == 0) {
                return SUCCESS;
            } else if (status == Db.DB_KEYEXIST) {
                return KEY_EXIST;
            } else {
                return NOT_FOUND;
            }
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
            DB_Legacy_DbtPool.recycleDbt(dataEntry);
        }
    }

    protected boolean db_put(DbTxn txn, byte[] key, byte[] value)
        throws Exception
    {
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt(key);
        Dbt dataEntry = DB_Legacy_DbtPool.createDbt(value);
        int flags = 0;
        if (txn == null) {
            flags |= Db.DB_AUTO_COMMIT;
        }
        try {
            return mDatabase.put(txn, keyEntry, dataEntry, flags) == 0;
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
            DB_Legacy_DbtPool.recycleDbt(dataEntry);
        }
    }

    protected boolean db_delete(DbTxn txn, byte[] key) throws Exception {
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt(key);
        int flags = 0;
        if (txn == null) {
            flags |= Db.DB_AUTO_COMMIT;
        }
        try {
            return mDatabase.del(txn, keyEntry, flags) == 0;
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
        }
    }

    protected void db_truncate(DbTxn txn) throws Exception {
        int flags = 0;
        if (txn == null) {
            flags |= Db.DB_AUTO_COMMIT;
        }
        mDatabase.truncate(txn, flags);
    }

    protected boolean db_isEmpty(DbTxn txn, Object database, boolean rmw) throws Exception {
        Dbc cursor = ((Db) database).cursor(txn, 0);
        Dbt keyEntry = DB_Legacy_DbtPool.createDbt();
        Dbt dataEntry = DB_Legacy_DbtPool.createDbt();
        try {
            int status = cursor.get(keyEntry, dataEntry, Db.DB_FIRST | (rmw ? Db.DB_RMW : 0));
            cursor.close();
            return status != 0;
        } finally {
            DB_Legacy_DbtPool.recycleDbt(keyEntry);
            DB_Legacy_DbtPool.recycleDbt(dataEntry);
        }
    }

    protected void db_close(Object database) throws Exception {
        ((Db) database).close(0);
    }

    protected Object env_openPrimaryDatabase(DbTxn txn, String name)
        throws Exception
    {
        return mDatabase = openDb(txn, name, false);
    }

    private Db openDb(DbTxn txn, String name, boolean allowDups) throws Exception {
        DB_Legacy_Repository dbRepository = (DB_Legacy_Repository) getRepository();
        DbEnv env = dbRepository.mEnv;

        Db database = new Db(env, 0);
        if (allowDups) {
            database.set_flags(Db.DB_DUPSORT);
        }

        Integer pageSize = dbRepository.getDatabasePageSize(getStorableType());
        if (pageSize != null) {
            database.set_pagesize(pageSize);
        }

        int flags = Db.DB_THREAD;

        if (txn == null) {
            flags |= Db.DB_AUTO_COMMIT;
        }
        if (dbRepository.mReadOnly) {
            flags |= Db.DB_RDONLY;
        }
        if (!dbRepository.mReadOnly) {
            flags |= Db.DB_CREATE;
        }

        runDatabasePrepareForOpeningHook(database);

        String fileName = dbRepository.getDatabaseFileName(name);
        String dbName = dbRepository.getDatabaseName(name);
        try {
            database.open(txn, fileName, dbName, Db.DB_BTREE, flags, 0);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(e.getMessage() + ": " + fileName);
        }

        return database;
    }

    protected void env_removeDatabase(DbTxn txn, String databaseName) throws Exception {
        DB_Legacy_Repository dbRepository = (DB_Legacy_Repository) getRepository();
        String fileName = dbRepository.getDatabaseFileName(databaseName);
        String dbName = dbRepository.getDatabaseName(databaseName);
        DbEnv env = dbRepository.mEnv;
        Db database = new Db(env, 0);
        database.remove(fileName, dbName, 0);
        database.close(0);
    }

    protected BDBCursor<DbTxn, S> openCursor
        (TransactionScope<DbTxn> scope,
         byte[] startBound, boolean inclusiveStart,
         byte[] endBound, boolean inclusiveEnd,
         int maxPrefix,
         boolean reverse,
         Object database)
        throws Exception
    {
        return new DB_Legacy_Cursor<S>
            (scope,
             startBound, inclusiveStart,
             endBound, inclusiveEnd,
             maxPrefix,
             reverse,
             this,
             (Db) database);
    }
}
