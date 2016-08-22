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

    public String getName() {
        return name;
    }

    public String toString(int depth) {
        StringBuilder tab = new StringBuilder();
        for (int i =0; i<depth; i++){
            tab.append("\t");
        }
        StringBuilder res =new StringBuilder(name + "(\n" + tab);
        for (QueryOperationNode subTree : subTrees) {
            res.append(subTree.toString(depth +1) + "\n" + tab);
        }
        res.deleteCharAt(res.length() - 1);
        res.append(")");
//        System.out.println("************ "+name+" ************");
//        System.out.println(res);
//        System.out.println("************ "+name+" ************");
        return res.toString();
    }

    @Override
    public String toString() {
        return toString(1);
    }
}
