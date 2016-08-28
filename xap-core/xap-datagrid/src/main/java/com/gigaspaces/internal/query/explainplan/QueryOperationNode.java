package com.gigaspaces.internal.query.explainplan;

import java.io.Externalizable;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */

public interface QueryOperationNode extends Externalizable{

    public boolean isLeaf();
    public List<QueryOperationNode> getSubTrees();
    public void addSon(QueryOperationNode node);
    public String toString(int depth);
    @Override
    public String toString();
}
