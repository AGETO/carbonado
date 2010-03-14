/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado;

/**
 * Thrown if a persist operation fails because lock acquisition timed out.
 *
 * @author Brian S O'Neill
 */
public class PersistTimeoutException extends PersistException {
    private static final long serialVersionUID = 1L;

    public PersistTimeoutException() {
        super();
    }

    public PersistTimeoutException(String message) {
        super(message);
    }

    public PersistTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistTimeoutException(Throwable cause) {
        super(cause);
    }

    @Override
    protected FetchException makeFetchException(String message, Throwable cause) {
        return new FetchTimeoutException(message, cause);
    }
}
