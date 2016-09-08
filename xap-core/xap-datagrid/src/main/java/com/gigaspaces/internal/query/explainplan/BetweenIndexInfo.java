package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class BetweenIndexInfo extends IndexInfo {
    private Comparable min;
    private Comparable max;
    private boolean includeMin;
    private boolean includeMax;

    public BetweenIndexInfo () {}

    public BetweenIndexInfo(String name, Integer size, SpaceIndexType type, Comparable min, boolean includeMin, Comparable max, boolean includeMax, QueryOperator operator, boolean b) {
        super(name, size, type, null, operator);
        this.max = max;
        this.min = min;
        this.includeMax = includeMax;
        this.includeMin = includeMin;
        setUsable(b);
    }

    public Comparable getMin() {
        return min;
    }

    public void setMin(Comparable min) {
        this.min = min;
    }

    public Comparable getMax() {
        return max;
    }

    public void setMax(Comparable max) {
        this.max = max;
    }

    public boolean isIncludeMin() {
        return includeMin;
    }

    public void setIncludeMin(boolean includeMin) {
        this.includeMin = includeMin;
    }

    public boolean isIncludeMax() {
        return includeMax;
    }

    public void setIncludeMax(boolean includeMax) {
        this.includeMax = includeMax;
    }

    @Override
    public String toString() {
        return "IndexInfo{(" + getName() + " BETWEEN " + min + " AND " + max + "), size= "
                + getSize() + ", type=" + getType() + "}";
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        IOUtils.writeString(objectOutput, getName());
        objectOutput.writeInt(getSize());
        objectOutput.writeObject(getType());
        objectOutput.writeObject(getOperator());
        objectOutput.writeObject(min);
        objectOutput.writeBoolean(includeMin);
        objectOutput.writeObject(max);
        objectOutput.writeBoolean(includeMax);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        setName(IOUtils.readString(objectInput));
        setSize(objectInput.readInt());
        setType((SpaceIndexType) objectInput.readObject());
        setOperator((QueryOperator) objectInput.readObject());
        min = (Comparable) objectInput.readObject();
        includeMin = objectInput.readBoolean();
        max = (Comparable) objectInput.readObject();
        includeMax = objectInput.readBoolean();
    }


}

