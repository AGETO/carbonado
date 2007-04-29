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

package com.amazon.carbonado.gen;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.sequence.SequenceValueProducer;

/**
 * Provides runtime support for Storable classes generated by {@link MasterStorableGenerator}.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public interface MasterSupport<S extends Storable> extends TriggerSupport<S> {
    /**
     * Returns a sequence value producer by name, or throw PersistException if not found.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     */
    SequenceValueProducer getSequenceValueProducer(String name) throws PersistException;
}
