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

//
package com.j_spaces.kernel.locks;
import com.gigaspaces.internal.server.space.SpaceConfigReader;

/**
 * Created by yechiel on 2/26/17.
 */
/*
handels locks in l.s. policy
 */
public class LayeredStorageLockManager <T extends ISelfLockingSubject>
        implements IBasicLockManager<T>
{
    private final BasicEvictableLockManager<T>  _evictableLockManager;
    private final OffHeapLockManager<T>    _offHeapLockManager;


    public LayeredStorageLockManager(SpaceConfigReader configReader)
    {
        _evictableLockManager = new BasicEvictableLockManager(configReader);
        _offHeapLockManager = new OffHeapLockManager();
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object)
     */
    public ILockObject getLockObject(T subject) {
        return getLockObject(subject, subject.getExternalLockObject() == null /*isEvictable*/);
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object, java.lang.Object, boolean)
     */
    public ILockObject getLockObject(T subject, boolean isEvictable) {
        return subject.getExternalLockObject() != null ? _offHeapLockManager.getLockObject(subject, isEvictable) :
                _evictableLockManager.getLockObject(subject, isEvictable);
    }

    /**
     * based only on subject's uid, return a lock object in order to lock the represented subject
     * this method is relevant only for evictable objects
     *
     * @return the lock object
     */
    public ILockObject getLockObject(String subjectUid) {
        return _evictableLockManager.getLockObject(subjectUid);
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#freeLockObject(com.j_spaces.kernel.locks.ILockObject)
     */
    public void freeLockObject(ILockObject lockObject) {
        return;
    }

    public boolean isPerLogicalSubjectLockObject(boolean isEvictable)
    {
        return isEvictable ? _evictableLockManager.isPerLogicalSubjectLockObject(isEvictable)
                : _offHeapLockManager.isPerLogicalSubjectLockObject(isEvictable);
    }





}
