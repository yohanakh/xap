package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.io.Externalizable;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public interface QueryOperationNode extends Externalizable{

    public boolean isLeaf();
    public List<QueryOperationNode> getSubTrees();
    public void addChild(QueryOperationNode node);
    public String toString(int depth);
    @Override
    public String toString();
}
