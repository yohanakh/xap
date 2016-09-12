/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class AggregatedExplainPlan implements ExplainPlan{

    private final Map<String,SingleExplainPlan> plans;
    private final SQLQuery<?> query;

    public AggregatedExplainPlan(SQLQuery query) {
        this.plans = new HashMap<String, SingleExplainPlan>();
        this.query = query;
    }

    public static AggregatedExplainPlan fromQueryPacket(Object query) {
        AggregatedExplainPlan result = null;
        if (query instanceof QueryTemplatePacket) {
            result = (AggregatedExplainPlan) ((QueryTemplatePacket)query).getExplainPlan();
        }
        if (result != null) {
            result.reset();
        }

        return result;
    }

    public SingleExplainPlan getPlan(String partitionId) {
        return plans.get(partitionId);
    }

    public Map<String, SingleExplainPlan> getAllPlans() {
        return plans;
    }

    public void reset() {
        plans.clear();
    }

    public void aggregate(SingleExplainPlan plan) {
        plans.put(plan.getPartitionId(), plan);
    }

    @Override
    public String toString() {
        final String NEWLINE = StringUtils.NEW_LINE;
        StringBuilder res = new StringBuilder();
        res.append("********** Explain plan report **********").append(NEWLINE);
        if (query != null) {
            res.append("Query: ").append(query.toString()).append(NEWLINE);
        }
        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            res.append(entry.getKey()).append(": ").append(NEWLINE);
            res.append(entry.getValue()).append(NEWLINE);
        }
        res.append("*****************************************").append(NEWLINE);
        return res.toString();
    }
}
