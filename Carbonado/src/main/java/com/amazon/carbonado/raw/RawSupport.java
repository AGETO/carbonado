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

package com.amazon.carbonado.raw;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;

import com.amazon.carbonado.gen.MasterSupport;

/**
 * Provides runtime support for Storable classes generated by {@link RawStorableGenerator}.
 *
 * @author Brian S O'Neill
 */
public interface RawSupport<S extends Storable> extends MasterSupport<S> {
    /**
     * Try to load the entry referenced by the given key, but return null
     * if not found.
     *
     * @param key non-null key to search for
     * @return non-null value that was found, or null if not found
     */
    byte[] tryLoad(byte[] key) throws FetchException;

    /**
     * Try to insert the entry referenced by the given key with the given
     * value.
     *
     * @param storable storable object that key and value were derived from
     * @param key non-null key to insert
     * @param value non-null value to insert
     * @return false if unique constraint prevents insert
     */
    boolean tryInsert(S storable, byte[] key, byte[] value) throws PersistException;

    /**
     * Try to store the entry referenced by the given key with the given
     * value. If the entry does not exist, insert it. Otherwise, update it.
     *
     * @param storable storable object that key and value were derived from
     * @param key non-null key to store
     * @param value non-null value to store
     */
    void store(S storable, byte[] key, byte[] value) throws PersistException;

    /**
     * Try to delete the entry referenced by the given key.
     *
     * @param key non-null key to delete
     * @return true if entry existed and is now deleted
     */
    boolean tryDelete(byte[] key) throws PersistException;

    /**
     * Returns the Blob for the given locator, returning null if not found.
     *
     * @param storable storable that contains Blob
     * @param name name of Blob property
     * @param locator Blob locator
     */
    Blob getBlob(S storable, String name, long locator) throws FetchException;

    /**
     * Returns the locator for the given Blob, returning zero if null.
     *
     * @throws PersistException if blob is unrecognized
     */
    long getLocator(Blob blob) throws PersistException;

    /**
     * Returns the Clob for the given locator, returning null if not found.
     *
     * @param storable storable that contains Blob
     * @param name name of Clob property
     * @param locator Clob locator
     */
    Clob getClob(S storable, String name, long locator) throws FetchException;

    /**
     * Returns the locator for the given Clob, returning zero if null.
     *
     * @throws PersistException if blob is unrecognized
     */
    long getLocator(Clob clob) throws PersistException;
}
