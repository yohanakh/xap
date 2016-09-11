package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class BetweenRangeNode extends RangeNode{
    private Comparable minValue;
    private Comparable maxValue;
    private boolean includeMin;
    private boolean includeMax;

    public BetweenRangeNode(){

    }

    public BetweenRangeNode(String fieldName, QueryOperator operator, String functionName, Comparable minValue, boolean includeMin, Comparable maxValue, boolean includeMax) {
        setFieldName(fieldName);
        setOperator(operator);
        setFunctionName(functionName);
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.includeMax = includeMax;
        this.includeMin = includeMin;
    }

    public Comparable getMinValue() {
        return minValue;
    }

    public void setMinValue(Comparable minValue) {
        this.minValue = minValue;
    }

    public Comparable getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Comparable maxValue) {
        this.maxValue = maxValue;
    }

    @Override
    public String toString(int depth) {
        if(getFunctionName() == null){
            return getOperator() + "(" + getFieldName() + ", [" + minValue.toString() + "," + maxValue.toString() + "])";
        }
        return getOperator() + "(" + getFunctionName() + ", [" + minValue.toString() + "," + maxValue.toString() + "])";
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
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getFieldName());
        out.writeObject(getOperator());
        out.writeObject(getFunctionName());
        out.writeObject(minValue);
        out.writeBoolean(includeMin);
        out.writeObject(maxValue);
        out.writeBoolean(includeMax);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setFieldName((String) in.readObject());
        setOperator((QueryOperator) in.readObject());
        setFunctionName((String) in.readObject());
        this.minValue = (Comparable) in.readObject();
        this.includeMin = in.readBoolean();
        this.maxValue = (Comparable) in.readObject();
        this.includeMax = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BetweenRangeNode that = (BetweenRangeNode) o;

        if (minValue != null ? !minValue.equals(that.minValue) : that.minValue != null)
            return false;
        return maxValue != null ? maxValue.equals(that.maxValue) : that.maxValue == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (minValue != null ? minValue.hashCode() : 0);
        result = 31 * result + (maxValue != null ? maxValue.hashCode() : 0);
        return result;
    }
}
