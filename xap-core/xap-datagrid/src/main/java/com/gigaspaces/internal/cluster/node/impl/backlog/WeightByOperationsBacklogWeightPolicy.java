package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class WeightByOperationsBacklogWeightPolicy implements BacklogWeightPolicy {
    @Override
    public int calculateWeight(IReplicationPacketData<?> data) {
        return data.size();
    }
}
