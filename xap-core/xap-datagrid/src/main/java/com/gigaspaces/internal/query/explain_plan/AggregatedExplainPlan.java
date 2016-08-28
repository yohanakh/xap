package com.gigaspaces.internal.query.explain_plan;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class AggregatedExplainPlan {
    private QueryOperationNode tree;

    public AggregatedExplainPlan(QueryOperationNode tree) {
        this.tree = tree;
    }

    public QueryOperationNode getTree() {
        return tree;
    }

    public void setTree(QueryOperationNode tree) {
        this.tree = tree;
    }

    public void aggregate(ExplainPlan explainPlan) {
        // TODO aggregate index info
    }
}
