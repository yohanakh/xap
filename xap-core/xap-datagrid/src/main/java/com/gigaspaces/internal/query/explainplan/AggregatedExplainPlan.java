package com.gigaspaces.internal.query.explainplan;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class AggregatedExplainPlan implements ExplainPlan{
    private Map<String,SingleExplainPlan> plans;

    public AggregatedExplainPlan() {
        this.plans = new HashMap<String, SingleExplainPlan>();
    }

    public SingleExplainPlan getPlan(String partitionId) {
        return plans.get(partitionId);
    }

    public Map<String, SingleExplainPlan> getAllPlans() {
        return plans;
    }

    public void aggregate(SingleExplainPlan plan) {
        plans.put(plan.getPartitionId(),plan);
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            res.append(entry.getKey()).append(": \n");
            res.append(entry.getValue()).append("\n");
        }
        return res.toString();
    }
}
