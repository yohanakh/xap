package com.gigaspaces.internal.query.explain_plan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class QueryJunctionNode implements QueryOperationNode{

    private final String name;
    private List<QueryOperationNode> subTrees;

    public QueryJunctionNode(String name) {
        subTrees = new ArrayList<QueryOperationNode>();
        this.name = name;
    }

    public void addSon(QueryOperationNode node){
        subTrees.add(node);
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    public List<QueryOperationNode> getSubTrees() {
        return subTrees;
    }

    public void setSubTrees(List<QueryOperationNode> subTrees) {
        this.subTrees = subTrees;
    }

    @Override
    public String toString() {
        return "QueryJunctionNode{" +
                "name='" + name + '\'' +
                ", subTrees=" + subTrees +
                '}';
    }
}
