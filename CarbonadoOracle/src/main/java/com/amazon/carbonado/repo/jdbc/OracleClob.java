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

package com.amazon.carbonado.repo.jdbc;

import java.io.Reader;
import java.io.Writer;

import java.sql.SQLException;

import oracle.sql.CLOB;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleClob extends JDBCClob {
    OracleClob(JDBCRepository repo, CLOB clob, JDBCClobLoader loader) {
        super(repo, clob, loader);
    }

    public Reader openReader() throws FetchException {
        return openReader(0);
    }

    public Reader openReader(long pos) throws FetchException {
        try {
            return getOracleClobForFetch().getCharacterStream(pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public Reader openReader(long pos, int bufferSize) throws FetchException {
        return openReader(pos);
    }

    public long getLength() throws FetchException {
        try {
            return getOracleClobForFetch().length();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public Writer openWriter() throws PersistException {
        return openWriter(0);
    }

    public Writer openWriter(long pos) throws PersistException {
        try {
            return getOracleClobForPersist().getCharacterOutputStream(pos);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public Writer openWriter(long pos, int bufferSize) throws PersistException {
        return openWriter(pos);
    }

    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
        try {
            getOracleClobForPersist().trim(length);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    private CLOB getOracleClobForFetch() throws FetchException {
        return (CLOB) getInternalClobForFetch();
    }

    private CLOB getOracleClobForPersist() throws PersistException {
        return (CLOB) getInternalClobForPersist();
    }
}
