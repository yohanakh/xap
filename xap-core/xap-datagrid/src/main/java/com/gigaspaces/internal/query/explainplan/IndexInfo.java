package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class IndexInfo implements Externalizable {

    private String name;
    private Integer size;
    private SpaceIndexType type;
    private Object value;
    private QueryOperator operator;



    public IndexInfo() {
    }

    public IndexInfo(String name, Integer size, SpaceIndexType type, Object value, QueryOperator operator) {
        this.name = name;
        this.size = size;
        this.type = type;
        this.value = value;
        this.operator= operator;
        if(type == SpaceIndexType.EXTENDED){
            this.size = -1;
        }
    }

    public IndexInfo(String name) {
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setType(SpaceIndexType type) {
        this.type = type;
    }

    public SpaceIndexType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        IOUtils.writeString(objectOutput, this.name);
        objectOutput.writeObject(this.size);
        objectOutput.writeObject(this.type);
        objectOutput.writeObject(this.value);
        objectOutput.writeObject(this.operator);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.size = (Integer) objectInput.readObject();
        this.type = (SpaceIndexType) objectInput.readObject();
        this.value = objectInput.readObject();
        this.operator = (QueryOperator) objectInput.readObject();
    }

    @Override
    public String toString() {
        return "IndexInfo{(" +
                name + " " + operator + " " + value + ")"
                +", size=" + size +
                ", type=" + type +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexInfo indexInfo = (IndexInfo) o;

        if (name != null ? !name.equals(indexInfo.name) : indexInfo.name != null) return false;
        if (size != null ? !size.equals(indexInfo.size) : indexInfo.size != null) return false;
        if (type != indexInfo.type) return false;
        if (value != null ? !value.equals(indexInfo.value) : indexInfo.value != null) return false;
        return operator == indexInfo.operator;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (operator != null ? operator.hashCode() : 0);
        return result;
    }
}
