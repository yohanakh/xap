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

package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.logger.Constants;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps a {@link INonBatchRedoLogFileStorage} with a cache that keeps a constant size number of
 * packets in memory which were the last appended packet. Since the most frequent read access to the
 * redo log file are to the end of the file (or the start), keeping the latest packets in memory
 * will reduce the accesses to the storage which creates a much more serious bottleneck
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class CacheLastRedoLogFileStorageDecorator<T> implements INonBatchRedoLogFileStorage<T> {

    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _bufferCapacity;
    private final INonBatchRedoLogFileStorage<T> _storage;
    private final LinkedList<T> _buffer = new LinkedList<T>();
    private long _bufferWeight;


    public CacheLastRedoLogFileStorageDecorator(int bufferSize, INonBatchRedoLogFileStorage<T> storage) {
        this._bufferCapacity = bufferSize;
        this._storage = storage;

        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("CacheLastRedoLogFileStorageDecorator created:"
                    + "\n\tbufferSize = " + _bufferCapacity);
        }
        _bufferWeight = 0;
    }

    public void append(T replicationPacket)
            throws StorageException, StorageFullException {
        _buffer.addLast(replicationPacket);
        increaseBufferWeight(replicationPacket);
        flushOldest();

    }

    private void increaseBufferWeight(T replicationPacket) {
        _bufferWeight += ((IReplicationOrderedPacket) replicationPacket).getWeight();
    }

    private void decreaseBufferWeight(T replicationPacket) {
        _bufferWeight -= ((IReplicationOrderedPacket) replicationPacket).getWeight();
    }

    public void appendBatch(List<T> replicationPackets)
            throws StorageException, StorageFullException {
        for (T replicationPacket : replicationPackets) {
            increaseBufferWeight(replicationPacket);
        }
        _buffer.addAll(replicationPackets);
        flushOldest();
    }

    private void flushOldest() throws StorageException, StorageFullException {
        try {
            while (_bufferWeight > _bufferCapacity && _buffer.size() > 1){
                T packet = _buffer.removeFirst();
                _storage.append(packet);
                decreaseBufferWeight(packet);
            }
        } catch (StorageFullException e) {
            LinkedList newDeniedPackets = new LinkedList<T>();
            List deniedPackets = e.getDeniedPackets();
            for (int i = deniedPackets.size() - 1; i >= 0; --i) {
                _buffer.addFirst((T) deniedPackets.get(i));
                increaseBufferWeight((T) deniedPackets.get(i));
                T last = _buffer.removeLast();
                decreaseBufferWeight(last);
                newDeniedPackets.addFirst(last);
            }
            throw new StorageFullException(e.getMessage(), e.getCause(), newDeniedPackets);
        }
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _storage.validateIntegrity();
    }

    public void close() {
        _buffer.clear();
        _storage.close();
        _bufferWeight = 0;
    }

    @Override
    public long getWeight() {
        return _bufferWeight + _storage.getWeight();
    }

    public void deleteOldestPackets(long packetsCount) throws StorageException {
        long storageSize = _storage.size();
        _storage.deleteOldestPackets(packetsCount);
        int bufferSize = _buffer.size();
        for (long i = 0; i < Math.min(bufferSize, packetsCount - storageSize); ++i){
            T first = _buffer.removeFirst();
            decreaseBufferWeight(first);
        }
    }

    public boolean isEmpty() throws StorageException {
        return _buffer.isEmpty() && _storage.isEmpty();
    }

    public long getSpaceUsed() {
        return _storage.getSpaceUsed();
    }

    public long getExternalPacketsCount() {
        return _storage.getExternalPacketsCount();
    }

    public long getMemoryPacketsCount() {
        return _buffer.size() + _storage.getMemoryPacketsCount();
    }

    public StorageReadOnlyIterator<T> readOnlyIterator()
            throws StorageException {
        return new CacheReadOnlyIterator(_storage.readOnlyIterator());
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(
            long fromIndex) throws StorageException {
        long storageSize = _storage.size();

        if (fromIndex < storageSize)
            return new CacheReadOnlyIterator(_storage.readOnlyIterator(fromIndex));

        //Can safely cast to int because if reached here the buffer size cannot be more than int
        return new CacheReadOnlyIterator((int) (fromIndex - storageSize));
    }

    public WeightedBatch<T> removeFirstBatch(int batchSize) throws StorageException {
        WeightedBatch<T> batch = _storage.removeFirstBatch(batchSize);

        while (!_buffer.isEmpty() &&batch.getWeight() < batchSize && !batch.isLimitReached()){
            T first = _buffer.removeFirst();

            if(batch.size() > 0 && batch.getWeight() + ((IReplicationOrderedPacket) first).getWeight() > batchSize){
                batch.setLimitReached(true);
                break;
            }

            decreaseBufferWeight(first);
            batch.addToBatch(first);
        }
        if(batch.size() >= batchSize){
            batch.setLimitReached(true);
        }
        return batch;
    }

    public long size() throws StorageException {
        return _buffer.size() + _storage.size();
    }

    /**
     * A read only iterator which automatically starts iterating over the buffer once the external
     * storage is exhausted
     *
     * @author eitany
     * @since 7.1
     */
    private class CacheReadOnlyIterator implements StorageReadOnlyIterator<T> {

        private final StorageReadOnlyIterator<T> _externalIterator;
        private boolean _externalIteratorExhausted;
        private Iterator<T> _bufferIterator;

        /**
         * Create an iterator which stars iterating over the packets which reside in external
         * storage
         */
        public CacheReadOnlyIterator(StorageReadOnlyIterator<T> externalIterator) {
            this._externalIterator = externalIterator;
        }

        /**
         * Create an iterator which starts directly iterating over the buffer, thus skipping the
         * external storage.
         *
         * @param fromIndex offset index to start inside the buffer
         */
        public CacheReadOnlyIterator(int fromIndex) {
            _externalIteratorExhausted = true;
            _externalIterator = null;
            _bufferIterator = _buffer.listIterator(fromIndex);
        }

        public boolean hasNext() throws StorageException {
            if (!_externalIteratorExhausted) {
                _externalIteratorExhausted = !_externalIterator.hasNext();
                if (!_externalIteratorExhausted)
                    return true;
            }

            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            return _bufferIterator.hasNext();
        }

        public T next() throws StorageException {
            if (!_externalIteratorExhausted) {
                try {
                    return _externalIterator.next();
                } catch (NoSuchElementException e) {
                    _externalIteratorExhausted = true;
                }
            }
            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            return _bufferIterator.next();
        }

        public void close() throws StorageException {
            if (_externalIterator != null)
                _externalIterator.close();
        }

    }

}
