package com.gigaspaces.internal.query.explainplan;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class AggregatedExplainPlan {
    private Map<String,ExplainPlan> plans;

    public AggregatedExplainPlan() {
        this.plans = new HashMap<String, ExplainPlan>();
    }

    public ExplainPlan getPlan(String partitionId) {
        return plans.get(partitionId);
    }

    public void aggregate(ExplainPlan plan) {
        plans.put(plan.getPartitionId(),plan);
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        for (Map.Entry<String, ExplainPlan> entry : plans.entrySet()) {
            res.append(entry.getKey()).append(": \n");
            res.append(entry.getValue()).append("\n");
        }
        return res.toString();
    }
}
