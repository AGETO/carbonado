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

import java.io.File;

import java.util.LinkedHashSet;
import java.util.Set;

import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.db.Db;
import com.sleepycat.db.DbException;
import com.sleepycat.db.DbEnv;
import com.sleepycat.db.DbTxn;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

/**
 * Repository implementation backed by a Berkeley DB. Data is encoded in the DB
 * in a specialized format, and so this repository should not be used to open
 * arbitrary Berkeley databases. DBRepository has total schema ownership, and
 * so it updates type definitions in the storage layer automatically.
 *
 * @author Brian S O'Neill
 */
class DB_Legacy_Repository extends BDBRepository<DbTxn> {

    // Default cache size, in bytes.
    private static final int DEFAULT_CACHE_SIZE = 60 * 1024 * 1024;

    final DbEnv mEnv;
    final boolean mReadOnly;

    /**
     * Open the repository using the given BDB repository configuration.
     *
     * @throws IllegalArgumentException if name or environment home is null
     * @throws RepositoryException if there is a problem opening the environment
     */
    DB_Legacy_Repository(AtomicReference<Repository> rootRef, BDBRepositoryBuilder builder)
        throws RepositoryException
    {
        super(rootRef, builder, DB_Legacy_ExceptionTransformer.getInstance());

        boolean readOnly = builder.getReadOnly();

        long lockTimeout = builder.getLockTimeoutInMicroseconds();
        long txnTimeout = builder.getTransactionTimeoutInMicroseconds();

        try {
            DbEnv env;
            try {
                env = (DbEnv) builder.getInitialEnvironmentConfig();
            } catch (ClassCastException e) {
                throw new ConfigurationException
                    ("Unsupported initial environment config. Must be instance of " +
                     DbEnv.class.getName(), e);
            }

            if (env == null) {
                env = new DbEnv(0);
            }

            mEnv = env;

            mEnv.set_lk_max_locks(10000);
            mEnv.set_lk_max_objects(10000);

            mEnv.set_timeout(lockTimeout, Db.DB_SET_LOCK_TIMEOUT);
            mEnv.set_timeout(txnTimeout, Db.DB_SET_TXN_TIMEOUT);

            Long cacheSize = builder.getCacheSize();
            if (cacheSize == null) {
                mEnv.set_cachesize(0, DEFAULT_CACHE_SIZE, 0);
            } else {
                int gbytes = (int) (cacheSize / (1024 * 1024 * 1024));
                int bytes = (int) (cacheSize - gbytes * (1024 * 1024 * 1024));
                mEnv.set_cachesize(gbytes, bytes, 0);
            }

            {
                int flags = 0;

                flags |= Db.DB_AUTO_COMMIT;

                if (builder.getTransactionNoSync()) {
                    flags |= Db.DB_TXN_NOSYNC;
                }
                if (builder.getTransactionWriteNoSync()) {
                    flags |= Db.DB_TXN_WRITE_NOSYNC;
                }

                if (builder.isPrivate()) {
                    flags |= Db.DB_PRIVATE;
                }

                mEnv.set_flags(flags, true);
            }

            {
                int flags = 0;

                flags |= Db.DB_INIT_LOCK;
                flags |= Db.DB_INIT_LOG;
                flags |= Db.DB_INIT_MPOOL;
                flags |= Db.DB_INIT_TXN;
                flags |= Db.DB_THREAD;

                if (!readOnly) {
                    flags |= Db.DB_CREATE;
                }

                mEnv.open(builder.getEnvironmentHome(), flags, 0);
            }
        } catch (DbException e) {
            throw DB_Legacy_ExceptionTransformer.getInstance().toRepositoryException(e);
        } catch (Throwable e) {
            String message = "Unable to open environment";
            if (e.getMessage() != null) {
                message += ": " + e.getMessage();
            }
            throw new RepositoryException(message, e);
        }

        if (!readOnly && !builder.getDataHomeFile().canWrite()) {
            // Allow environment to be created, but databases are read-only.
            // This is only significant if data home differs from environment home.
            readOnly = true;
        }

        mReadOnly = readOnly;

        long deadlockInterval = Math.min(lockTimeout, txnTimeout);
        // Make sure interval is no smaller than 0.5 seconds.
        deadlockInterval = Math.max(500000, deadlockInterval) / 1000;

        start(builder.getCheckpointInterval(), deadlockInterval);
    }

    public Object getEnvironment() {
        return mEnv;
    }

    @Override
    IsolationLevel selectIsolationLevel(com.amazon.carbonado.Transaction parent,
                                        IsolationLevel level)
    {
        if (level == null) {
            if (parent == null) {
                return IsolationLevel.REPEATABLE_READ;
            }
            return parent.getIsolationLevel();
        }

        if (level == IsolationLevel.READ_COMMITTED) {
            // Degree 2 isolation not supported, so promote.
            return IsolationLevel.REPEATABLE_READ;
        } else if (level == IsolationLevel.SNAPSHOT) {
            // Not supported.
            return null;
        } else if (level == IsolationLevel.SERIALIZABLE) {
            // Not supported.
            return null;
        }

        return level;
    }

    @Override
    protected DbTxn txn_begin(DbTxn parent, IsolationLevel level) throws Exception {
        int flags = 0;
        if (level == IsolationLevel.READ_UNCOMMITTED) {
            flags |= Db.DB_DIRTY_READ;
        }
        return mEnv.txn_begin(parent, flags);
    }

    @Override
    protected DbTxn txn_begin_nowait(DbTxn parent, IsolationLevel level) throws Exception {
        int flags = Db.DB_TXN_NOWAIT;
        if (level == IsolationLevel.READ_UNCOMMITTED) {
            flags |= Db.DB_DIRTY_READ;
        }
        return mEnv.txn_begin(parent, flags);
    }

    @Override
    protected void txn_commit(DbTxn txn) throws Exception {
        txn.commit(0);
    }

    @Override
    protected void txn_abort(DbTxn txn) throws Exception {
        txn.abort();
    }

    @Override
    protected void env_checkpoint() throws Exception {
        mEnv.txn_checkpoint(0, 0, Db.DB_FORCE);
        removeOldLogFiles();
    }

    @Override
    protected void env_checkpoint(int kBytes, int minutes) throws Exception {
        mEnv.txn_checkpoint(kBytes, minutes, 0);
        removeOldLogFiles();
    }

    private void removeOldLogFiles() throws Exception {
        synchronized (mBackupLock) {
            if (mBackupCount == 0) {
                String[] oldLogFiles = mEnv.log_archive(Db.DB_ARCH_ABS);
                if (oldLogFiles != null) {
                    for (String filename : oldLogFiles) {
                        new File(filename).delete();
                    }
                }
            }
        }
    }

    @Override
    protected void env_detectDeadlocks() throws Exception {
        mEnv.lock_detect(0, Db.DB_LOCK_DEFAULT);
    }

    @Override
    protected void env_close() throws Exception {
        if (mEnv != null) {
            mEnv.close(0);
        }
    }

    @Override
    protected <S extends Storable> BDBStorage<DbTxn, S> createBDBStorage(Class<S> type)
        throws Exception
    {
        return new DB_Legacy_Storage<S>(this, type);
    }

    @Override
    void enterBackupMode() throws Exception {
        // Nothing special to do.
    }

    @Override
    void exitBackupMode() throws Exception {
        // Nothing special to do.
    }

    @Override
    File[] backupFiles() throws Exception {
        Set<File> dbFileSet = new LinkedHashSet<File>();

        for (String dbName : getAllDatabaseNames()) {
            File file = new File(getDatabaseFileName(dbName));
            if (!file.isAbsolute()) {
                file = new File(mEnvHome, file.getPath());
            }
            if (!dbFileSet.contains(file) && file.exists()) {
                dbFileSet.add(file);
            }
        }

        for (String logName : mEnv.log_archive(Db.DB_ARCH_ABS | Db.DB_ARCH_LOG)) {
            File file = new File(logName);
            if (!dbFileSet.contains(file) && file.exists()) {
                dbFileSet.add(file);
            }
        }

        return dbFileSet.toArray(new File[dbFileSet.size()]);
    }
}
