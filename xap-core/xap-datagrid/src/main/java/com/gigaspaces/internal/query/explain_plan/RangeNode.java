package com.gigaspaces.internal.query.explain_plan;

import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class RangeNode implements QueryOperationNode{

    private String fieldName;
    private Object value;
    private QueryOperator operator;
    private String functionName;

    public RangeNode(String fieldName, Object value, QueryOperator operator, String functionName) {

        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
        this.functionName = functionName;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public List<QueryOperationNode> getSubTrees() {
        return null;
    }

    @Override
    public void addSon(QueryOperationNode node) {
        return;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public String toString() {
        return "RangeNode{" +
                "fieldName='" + fieldName + '\'' +
                ", value=" + value +
                ", operator=" + operator +
                '}';
    }

}
