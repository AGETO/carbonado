/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.capability;

import java.io.Serializable;

import com.amazon.carbonado.Repository;

/**
 * Capability for gaining access to replication group members.
 *
 * @author Vishal Parakh
 */
public interface GroupAccessCapability extends Capability {
    /**
     * Return all replication group members, including this repository.
     * Returned instance remains the same unless group membership changes.
     */
    GroupMembers getGroupMembers();
}