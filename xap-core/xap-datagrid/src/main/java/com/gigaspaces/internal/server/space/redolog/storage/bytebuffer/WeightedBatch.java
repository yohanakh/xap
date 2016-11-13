package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.1
 */
public class WeightedBatch<T> {

    private  List<T> batch;
    private long weight;
    private boolean limitReached = false;

    public WeightedBatch() {
        batch = new ArrayList<T>();
    }

    public WeightedBatch(List<T> batch) {
        this.batch = batch;
    }

    public WeightedBatch(List<T> batch, long weight) {
        this.batch = batch;
        this.weight = weight;
    }

    public List<T> getBatch() {
        return batch;
    }

    public void setBatch(List<T> batch) {
        this.batch = batch;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public boolean isLimitReached() {
        return limitReached;
    }

    public void setLimitReached(boolean limitReached) {
        this.limitReached = limitReached;
    }

    public void addToBatch(T packet){
       batch.add(packet);
        int packetWeight = ((IReplicationOrderedPacket) packet).getWeight();
        this.weight += packetWeight;
    }

    public T removeLast(){
        T packet = batch.remove(batch.size() - 1);
        weight -= ((IReplicationOrderedPacket) packet).getWeight();
        return packet;
    }

    public int size() {
        return batch.size();
    }
}
