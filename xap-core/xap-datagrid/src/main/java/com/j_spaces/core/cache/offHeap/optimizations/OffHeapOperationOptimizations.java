package com.j_spaces.core.cache.offHeap.optimizations;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.LocalViewRegistrations;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class OffHeapOperationOptimizations {
    public static boolean isConsiderOptimizedForBlobstore(SpaceEngine spaceEngine, Context context, ITemplateHolder template, IEntryCacheInfo pEntry) {
        if (!pEntry.isOffHeapEntry() || template.getXidOriginatedTransaction() != null) {
            return false;
        }
        IServerTypeDesc serverTypeDesc = pEntry.getServerTypeDesc();
        if (template.isReadOperation()) {
            return isConsiderOptimizedReadForBlobstore(spaceEngine, template, serverTypeDesc, pEntry.getUID());
        }
        if (template.getBatchOperationContext() != null && template.getBatchOperationContext().isClear()) {
            return isConsiderOptimizedClearForBlobstore(spaceEngine, template, serverTypeDesc);
        }
        if (template.isTakeOperation()) {
            return isConsiderOptimizedTakeForBlobstore(spaceEngine, context, serverTypeDesc);
        }
        return false;
    }

    private static boolean isConsiderOptimizedTakeForBlobstore(SpaceEngine spaceEngine, Context context, IServerTypeDesc serverTypeDesc) {
        boolean onBackup = context.isFromReplication() && spaceEngine.getSpaceImpl().isBackup();
        if (onBackup) {
            LocalViewRegistrations registrations = spaceEngine.getLocalViewRegistrations();
            return spaceEngine.getCacheManager().getTemplatesManager().isBlobStoreClearTakeOptimizationAllowedNotify(serverTypeDesc, onBackup)
                    && registrations != null
                    && registrations.isBlobStoreClearOptimizationAllowed(serverTypeDesc);
        }

        return false;
    }

    private static boolean isConsiderOptimizedClearForBlobstore(SpaceEngine spaceEngine, ITemplateHolder template, IServerTypeDesc serverTypeDesc) {
        //NOTE- clear is only on primary- on backup its take-by-id from replication
        if (!spaceEngine.getCacheManager().optimizedBlobStoreClear()) {
            return false;
        }
        return template.isOptimizedForBlobStoreOp(spaceEngine.getCacheManager())
                && (spaceEngine.getLocalViewRegistrations() == null || spaceEngine.getLocalViewRegistrations().isBlobStoreClearOptimizationAllowed(serverTypeDesc))
                && spaceEngine.getCacheManager().getTemplatesManager().isBlobStoreClearTakeOptimizationAllowedNotify(serverTypeDesc, false /*onBackup*/);
    }

    private static boolean isConsiderOptimizedReadForBlobstore(SpaceEngine spaceEngine, ITemplateHolder template, IServerTypeDesc serverTypeDesc, String entryUid) {
        AbstractProjectionTemplate projectionTemplate = template.getProjectionTemplate();
        if (!template.isOptimizedForBlobStoreOp(spaceEngine.getCacheManager())) {
            return false;
        }
        if(template.isReturnOnlyUid()){
            return true;
        }
        if (projectionTemplate == null) {
            return false;
        }
        if(projectionTemplate.isMultiUidsProjection()){
            return projectionTemplate.isAllIndexesProjections(serverTypeDesc , template, entryUid);
        }
        return projectionTemplate.isAllIndexesProjections(serverTypeDesc, template);
    }
}
