package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.metadata.index.SpaceIndexType;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class BetweenIndexInfo extends IndexInfo {
    private Comparable min;
    private Comparable max;
    private boolean includeMin;
    private boolean includeMax;

    public BetweenIndexInfo(String name, Integer size, SpaceIndexType type, Comparable min, boolean includeMin, Comparable max, boolean includeMax, QueryOperator operator) {
        super(name, size, type, null, operator);
        this.max = max;
        this.min = min;
        this.includeMax = includeMax;
        this.includeMin = includeMin;
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
}
//IndexInfo{(category GE 5), size=-1, type=EXTENDED}
