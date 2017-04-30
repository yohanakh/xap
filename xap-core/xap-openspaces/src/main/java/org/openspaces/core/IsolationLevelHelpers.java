/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openspaces.core;

import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.IsolationLevelModifiers;
import com.gigaspaces.client.ReadModifiers;

import org.springframework.transaction.TransactionDefinition;

/**
 * Utility methods to handle conversions between different isolation level representations.
 *
 * @author Dan Kilman
 * @since 9.5
 */
public class IsolationLevelHelpers {

    /**
     * Converts a GigaSpaces XAP IsolationLevelModifiers to a Spring Isolation Level
     * @param modifiers GigaSpaces XAP IsolationLevelModifiers
     * @return Spring Isolation Level
     */
    public static int toSpringIsolationLevel(IsolationLevelModifiers modifiers) {
        if (modifiers == null)
            return TransactionDefinition.ISOLATION_DEFAULT;
        if (modifiers.getCode() == ReadModifiers.DIRTY_READ.getCode())
            return TransactionDefinition.ISOLATION_READ_UNCOMMITTED;
        if (modifiers.getCode() == ReadModifiers.READ_COMMITTED.getCode())
            return TransactionDefinition.ISOLATION_READ_COMMITTED;
        if (modifiers.getCode() == ReadModifiers.REPEATABLE_READ.getCode())
            return TransactionDefinition.ISOLATION_REPEATABLE_READ;
        throw new IllegalArgumentException("Unsupported isolation level [" + modifiers + "]");
    }

    /**
     * Converts a Spring Isolation Level to a GigaSpaces XAP IsolationLevelModifiers.
     * @param springIsolationLevel Spring Isolation Level
     * @return GigaSpaces XAP IsolationLevelModifiers
     */
    public static IsolationLevelModifiers fromSpringIsolationLevel(Integer springIsolationLevel) {
        if (springIsolationLevel == null)
            return null;
        switch (springIsolationLevel) {
            case TransactionDefinition.ISOLATION_DEFAULT: return null;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED: return ReadModifiers.DIRTY_READ;
            case TransactionDefinition.ISOLATION_READ_COMMITTED: return ReadModifiers.READ_COMMITTED;
            case TransactionDefinition.ISOLATION_REPEATABLE_READ: return ReadModifiers.REPEATABLE_READ;
            default: throw new IllegalArgumentException("Unsupported Spring isolation level [" + springIsolationLevel + "]");
        }
    }

    /**
     * @param springIsolationLevel the spring isolation level as defined in {@link
     *                             TransactionDefinition}.
     * @param defaultValue         The modifiers to use in case springIsolationLevel equals {@link
     *                             TransactionDefinition#ISOLATION_DEFAULT}
     * @return A matcing GigaSpaces isolation level modifier.
     * @see TransactionDefinition
     * @see ReadModifiers
     * @see CountModifiers
     */
    public static int convertSpringToSpaceIsolationLevel(int springIsolationLevel, int defaultValue) {
        IsolationLevelModifiers modifiers = fromSpringIsolationLevel(springIsolationLevel);
        return  modifiers != null ? modifiers.getCode() : defaultValue;
    }

    /**
     * @param isolationLevel The isolation level code number.
     * @return a {@link ReadModifiers} instance representing the isolationLevel parameter
     */
    public static ReadModifiers toReadModifiers(int isolationLevel) {
        if (isolationLevel == ReadModifiers.NONE.getCode()) {
            return ReadModifiers.NONE;
        } else if (isolationLevel == ReadModifiers.DIRTY_READ.getCode()) {
            return ReadModifiers.DIRTY_READ;
        } else if (isolationLevel == ReadModifiers.READ_COMMITTED.getCode()) {
            return ReadModifiers.READ_COMMITTED;
        } else if (isolationLevel == ReadModifiers.REPEATABLE_READ.getCode()) {
            return ReadModifiers.REPEATABLE_READ;
        } else {
            throw new IllegalArgumentException("GigaSpace does not support isolation level: " + isolationLevel);
        }
    }

    /**
     * @param isolationLevel The isolation level code number.
     * @return a {@link CountModifiers} instance representing the isolationLevel parameter
     */
    public static CountModifiers toCountModifiers(int isolationLevel) {
        if (isolationLevel == CountModifiers.NONE.getCode()) {
            return CountModifiers.NONE;
        } else if (isolationLevel == CountModifiers.DIRTY_READ.getCode()) {
            return CountModifiers.DIRTY_READ;
        } else if (isolationLevel == CountModifiers.READ_COMMITTED.getCode()) {
            return CountModifiers.READ_COMMITTED;
        } else if (isolationLevel == CountModifiers.REPEATABLE_READ.getCode()) {
            return CountModifiers.REPEATABLE_READ;
        } else {
            throw new IllegalArgumentException("GigaSpace does not support isolation level: " + isolationLevel);
        }
    }
}
