/*
 * Copyright (c) 2008-2017, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package com.j_spaces.kernel.pool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


/**
 * a resource pool that blocks clients when all resources are busy, and has soft limit and hard limit such that soft limit <= hard limit.
 * Once the soft Limit is reached the pool will create resource until the hard limit is reached and then blocked those resources between the limits will not be cached.
 *
 * @author Bar Orion Barak.
 * @author Moran Avigdor.
 * @since 12.3
 */
@com.gigaspaces.api.InternalApi
public class ElasticBlockingResourcePool<R extends IResource> extends ResourcePool<R> {

    private final Queue<Thread> waitingQueue;
    private final AtomicInteger unpooled = new AtomicInteger(0);

    private static int noWaitParkTime;

    static {
        try {
            LockSupport.parkUntil(-1);
            noWaitParkTime = -1;
        } catch (Exception e) {
            noWaitParkTime = 0;
        }
    }

    private final int hardLimit;

    /**
     * Creates a new Resources Pool with the specified resourceFactory, and max Resources.
     *
     * @param resourceFactory resource factory instance for new resources
     * @param minResources    resources to pre-allocate; can be zero
     * @param softLimit    upper bound on the number of resources
     */
    @SuppressWarnings("unused")
    public ElasticBlockingResourcePool(IResourceFactory<R> resourceFactory, int minResources, int softLimit, int hardLimit) {
        this(resourceFactory, minResources, softLimit, hardLimit,null);
    }

    /**
     * Creates a new Resources Pool with the specified resourceFactory, and max Resources.
     *
     * @param resourceFactory  resource factory instance for new resources
     * @param minResources     resources to pre-allocate; can be zero
     * @param softLimit        the soft upper bound on number of resources, after this number resources will be created until hardLimit but will not cached.
     * @param hardLimit        the max number of resources that can be track by this pool
     * @param initialResources initial array of resources to init the pool with
     */
    @SuppressWarnings("WeakerAccess")
    public ElasticBlockingResourcePool(IResourceFactory<R> resourceFactory, int minResources, int softLimit, int hardLimit, R[] initialResources) {
        super(resourceFactory, minResources, softLimit, initialResources);
        this.hardLimit = hardLimit;
        waitingQueue = new ConcurrentLinkedQueue<Thread>();
    }

    /**
     * Returns a Resource from the pool. If there is an un-used Resource in the pool, it is
     * returned; otherwise, a new Resource is created and added to the pool.
     *
     * @return free Resource allocated to this request
     */
    @Override
    public R getResource() {
        R resource = super.findFreeResource();

        /* retry on unsuccessful set - race condition on free resource. */
        if (resource == null) // free resource not found
        {
            // save in pool
            if (!_full) // we need this check so we won't get integer overflow on getAndIncrement()
            {
                // creates new peer
                resource = tryAllocateNewPooledResource();
                if (resource == null)
                    return handleFullPool();
                else {
                    resource.setAcquired(true); // to avoid stealing need to set before adding to pool

                    int newIndex = _nextFreeIndex.getAndIncrement();
                    if (newIndex < _maxResources) {
                        resource.setFromPool(true);
                        _resourcesPool[newIndex] = resource;
                    } else {
                        _full = true;
                        resource.setFromPool(false);
                        unpooled.incrementAndGet();
                    }
                }
            } else {
                resource = allocateUnpooledResource();
                if (resource == null){
                    // creates new peer
                    return handleFullPool();
                }
            }
        }

        return resource;
    }

    private boolean isBelowHardLimit() {
        while(true) {
            int unpooledResources = unpooled.get();
            if (unpooledResources < hardLimit - _maxResources) {
                if (unpooled.compareAndSet(unpooledResources, unpooledResources + 1)) {
                    return true;
                }
            }else{
                return false;
            }
        }
    }

    @Override
    protected R findFreeResource() {
        R ret = super.findFreeResource();
        if(ret == null){
            return allocateUnpooledResource();
        }
        return ret;
    }

    private R allocateUnpooledResource() {
        if(isBelowHardLimit()){
            R resource = _resourceFactory.allocate();
            if (resource == null) {
                unpooled.decrementAndGet();
            } else {
                resource.setFromPool(false);
                resource.setAcquired(true);
            }
            return resource;
        }else{
            return null;
        }
    }

    @Override
    protected R handleFullPool() {
        final Thread thread = Thread.currentThread();
        while (true) {
            waitingQueue.add(thread);
            R resource = findFreeResource();
            if (resource != null) {
                waitingQueue.remove(thread);

                // clears the "memory" of the thread's park state
                // in case some resource(s) was returned and "unparked"
                // the current thread.
                // state must be cleared so that next time park() will
                // be called the thread will actually park.
                // value of parkUntil() MUST be -1, for sun(like) JVMs(0 blocks forever!)
                // and 0 for BEA.
                LockSupport.parkUntil(noWaitParkTime);

                return resource;
            }

            LockSupport.park();
            resource = findFreeResource();
            if (resource != null) {
                return resource;
            }
        }
    }

    @Override
    public void freeResource(R resourceToFree) {
        if(resourceToFree.isFromPool()){
            super.freeResource(resourceToFree);
        }else{
            resourceToFree.release();
            unpooled.decrementAndGet();
        }
        Thread thread = waitingQueue.poll();
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

}
