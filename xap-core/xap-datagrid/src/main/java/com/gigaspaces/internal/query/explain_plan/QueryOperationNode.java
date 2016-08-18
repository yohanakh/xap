package com.gigaspaces.internal.query.explain_plan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */

public interface QueryOperationNode {

    public boolean isLeaf();
    public List<QueryOperationNode> getSubTrees();
    public void addSon(QueryOperationNode node);
    @Override
    public String toString();


}
