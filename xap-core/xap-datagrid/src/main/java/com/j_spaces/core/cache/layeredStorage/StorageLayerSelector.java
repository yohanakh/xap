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
package com.j_spaces.core.cache.layeredStorage;
/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
//

import com.gigaspaces.internal.server.space.metadata.ServerTypeDesc;
import com.gigaspaces.internal.server.storage.EntryCreationReason;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.j_spaces.core.cache.CacheManager;

/**
 * Created by yechiel on 3/13/17.
 */
/*
    used in order to route an entry to its layer -wrapper of selection filters
 */

public class StorageLayerSelector {

//TBD
    private final CacheManager _cacheManager;

    public StorageLayerSelector(CacheManager cm)
    {
        _cacheManager = cm;
    }
    public EntryStorageLayer selectLayer(ITransactionalEntryData ed, String clazz, EntryCreationReason cr)
    {
        //scan the filters do match if none matches return the default
        return EntryStorageLayer.DB_BASED;
    }

}
