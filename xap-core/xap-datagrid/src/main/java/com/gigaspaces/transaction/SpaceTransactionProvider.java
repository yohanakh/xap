package com.gigaspaces.transaction;

import com.gigaspaces.client.IsolationLevelModifiers;

import java.io.Closeable;

/**
 * A transaction provider for space proxy.
 *
 * @author Niv Ingberg
 * @since 12.2
 */
public interface SpaceTransactionProvider extends Closeable {

    /**
     * Returns the currently running transaction isolation level.
     *
     * @return The transaction isolation level.
     */
    IsolationLevelModifiers getCurrentTransactionIsolationLevel();
}
