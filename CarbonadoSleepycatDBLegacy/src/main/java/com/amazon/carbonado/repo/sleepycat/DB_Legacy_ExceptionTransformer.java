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

import com.sleepycat.db.DbException;
import com.sleepycat.db.DbDeadlockException;
import com.sleepycat.db.DbLockNotGrantedException;

import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistDeniedException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * Custom exception transform rules.
 *
 * @author Brian S O'Neill
 */
class DB_Legacy_ExceptionTransformer extends ExceptionTransformer {
    private static DB_Legacy_ExceptionTransformer cInstance;

    public static DB_Legacy_ExceptionTransformer getInstance() {
        if (cInstance == null) {
            cInstance = new DB_Legacy_ExceptionTransformer();
        }
        return cInstance;
    }

    DB_Legacy_ExceptionTransformer() {
    }

    @Override
    protected FetchException transformIntoFetchException(Throwable e) {
        FetchException fe = super.transformIntoFetchException(e);
        if (fe != null) {
            return fe;
        }
        if (e instanceof DbException) {
            if (e instanceof DbLockNotGrantedException) {
                return new FetchTimeoutException(e);
            }
            if (e instanceof DbDeadlockException) {
                return new FetchDeadlockException(e);
            }
            String message = e.getMessage();
            if (message != null) {
                message = message.toUpperCase();
                if (message.indexOf("DB_LOCK_NOTGRANTED") >= 0) {
                    return new FetchTimeoutException(e);
                }
                if (message.indexOf("DB_LOCK_DEADLOCK") >= 0) {
                    return new FetchDeadlockException(e);
                }
            }
        }
        return null;
    }

    @Override
    protected PersistException transformIntoPersistException(Throwable e) {
        PersistException pe = super.transformIntoPersistException(e);
        if (pe != null) {
            return pe;
        }
        if (e instanceof DbException) {
            if (e instanceof DbLockNotGrantedException) {
                return new PersistTimeoutException(e);
            }
            if (e instanceof DbDeadlockException) {
                return new PersistDeadlockException(e);
            }
            String message = e.getMessage();
            if (message != null) {
                message = message.toUpperCase();
                if (message.indexOf("READ ONLY") >= 0) {
                    return new PersistDeniedException(e);
                }
                if (message.indexOf("DB_LOCK_NOTGRANTED") >= 0) {
                    return new PersistTimeoutException(e);
                }
                if (message.indexOf("DB_LOCK_DEADLOCK") >= 0) {
                    return new PersistDeadlockException(e);
                }
            }
        }
        return null;
    }
}
