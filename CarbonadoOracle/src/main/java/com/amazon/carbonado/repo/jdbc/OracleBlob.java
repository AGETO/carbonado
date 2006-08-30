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

import java.io.InputStream;
import java.io.OutputStream;

import java.sql.SQLException;

import oracle.sql.BLOB;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleBlob extends JDBCBlob {
    OracleBlob(JDBCRepository repo, BLOB blob, JDBCBlobLoader loader) {
        super(repo, blob, loader);
    }

    public InputStream openInputStream() throws FetchException {
        return openInputStream(0);
    }

    public InputStream openInputStream(long pos) throws FetchException {
        try {
            return getOracleBlobForFetch().getBinaryStream(pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
        return openInputStream(pos);
    }

    public long getLength() throws FetchException {
        try {
            return getOracleBlobForFetch().length();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public OutputStream openOutputStream() throws PersistException {
        return openOutputStream(0);
    }

    public OutputStream openOutputStream(long pos) throws PersistException {
        try {
            return getOracleBlobForPersist().getBinaryOutputStream(pos);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
        return openOutputStream(pos);
    }

    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
        try {
            getOracleBlobForPersist().trim(length);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    private BLOB getOracleBlobForFetch() throws FetchException {
        return (BLOB) getInternalBlobForFetch();
    }

    private BLOB getOracleBlobForPersist() throws PersistException {
        return (BLOB) getInternalBlobForPersist();
    }
}
