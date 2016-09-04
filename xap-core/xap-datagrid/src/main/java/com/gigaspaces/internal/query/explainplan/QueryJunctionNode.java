package com.gigaspaces.internal.query.explainplan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class QueryJunctionNode implements QueryOperationNode{

    private String name;
    private List<QueryOperationNode> subTrees;

    public QueryJunctionNode(){
        subTrees = new ArrayList<QueryOperationNode>();
    }

    public QueryJunctionNode(String name) {
        subTrees = new ArrayList<QueryOperationNode>();
        this.name = name;
    }

    public void addChild(QueryOperationNode node){
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
        return res.toString();
    }

    @Override
    public String toString() {
        return toString(1);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
        out.writeInt(subTrees.size());
        for (QueryOperationNode subTree : subTrees) {
            out.writeObject(subTree);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.name = (String) in.readObject();
        int size = in.readInt();
        for (int i=0; i < size; i++){
            addChild((QueryOperationNode) in.readObject());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryJunctionNode that = (QueryJunctionNode) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return subTrees != null ? subTrees.equals(that.subTrees) : that.subTrees == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (subTrees != null ? subTrees.hashCode() : 0);
        return result;
    }
}
