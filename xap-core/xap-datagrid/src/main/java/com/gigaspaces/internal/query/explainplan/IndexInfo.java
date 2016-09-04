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

    public SpaceIndexType getType() {
        return type;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        IOUtils.writeString(objectOutput, this.name);
        objectOutput.writeInt(this.size);
        objectOutput.writeObject(this.type);
        objectOutput.writeObject(this.value);
        objectOutput.writeObject(this.operator);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.size = objectInput.readInt();
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
}
