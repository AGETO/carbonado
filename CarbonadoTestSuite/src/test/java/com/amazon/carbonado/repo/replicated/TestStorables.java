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

package com.amazon.carbonado.repo.replicated;

import junit.framework.TestSuite;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.TestUtilities;

import com.amazon.carbonado.repo.replicated.ReplicatedRepository;

import com.amazon.carbonado.sequence.StoredSequence;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestStorables extends com.amazon.carbonado.TestStorables {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestStorables.class);
        return suite;
    }

    public TestStorables(String name) {
        super(name);
    }

    @Override
    protected Repository buildRepository(boolean isMaster) throws RepositoryException {
        RepositoryBuilder replica = TestUtilities.newTempRepositoryBuilder("rr-replica");
        RepositoryBuilder master = TestUtilities.newTempRepositoryBuilder("rr-writer");
        ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
        builder.setMaster(isMaster);
        builder.setReplicaRepositoryBuilder(replica);
        builder.setMasterRepositoryBuilder(master);
        return builder.build();
    }

    public void testAuthoritative() throws Exception {
        // Make sure authoritative storable is not replicated.

        Storage<StoredSequence> storage = getRepository().storageFor(StoredSequence.class);

        StoredSequence seq = storage.prepare();
        seq.setName("foo");
        seq.setInitialValue(0);
        seq.setNextValue(1);
        seq.insert();

        Storage<StoredSequence> replica = ((ReplicatedRepository) getRepository())
            .getReplicaRepository().storageFor(StoredSequence.class);

        seq = replica.prepare();
        seq.setName("foo");
        assertFalse(seq.tryLoad());

        Storage<StoredSequence> master = ((ReplicatedRepository) getRepository())
            .getMasterRepository().storageFor(StoredSequence.class);

        seq = master.prepare();
        seq.setName("foo");
        assertTrue(seq.tryLoad());
    }
}
