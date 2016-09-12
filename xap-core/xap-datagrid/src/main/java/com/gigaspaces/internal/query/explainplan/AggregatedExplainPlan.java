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
import java.util.List;
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
        StringBuilder sb = new StringBuilder();
        sb.append("********** Explain plan report **********").append(StringUtils.NEW_LINE);
        report(sb);
        sb.append("*****************************************").append(StringUtils.NEW_LINE);
        return sb.toString();
    }

    protected void report(StringBuilder sb) {
        sb.append("Query: ").append(query.toString()).append(StringUtils.NEW_LINE);
        if (plans.isEmpty()) {
            sb.append("Not executed yet").append(StringUtils.NEW_LINE);
        } else {
            final SingleExplainPlan first = plans.values().iterator().next();
            sb.append("Query Tree:").append(StringUtils.NEW_LINE)
                .append(first.getRoot()).append(StringUtils.NEW_LINE);
            sb.append("Num of partitions: ").append(plans.size()).append(StringUtils.NEW_LINE);
            for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
                report(sb, entry.getKey(), entry.getValue());
            }
        }
    }

    protected void report(StringBuilder sb, String partitionId, SingleExplainPlan singleExplainPlan) {
        String prefix = indent("");
        sb.append(prefix).append("Partition Id: ").append(partitionId).append(StringUtils.NEW_LINE);
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        final Map<String, ScanningInfo> scanningInfo = singleExplainPlan.getScanningInfo();
        if (indexesInfo.isEmpty()) {
            sb.append(prefix).append("Index Information: NO INDEX USED").append(StringUtils.NEW_LINE);
            prefix = indent(prefix);
            for (Map.Entry<String, ScanningInfo> entry : scanningInfo.entrySet()) {
                sb.append(prefix).append(entry.getKey()).append(":").append(StringUtils.NEW_LINE);
                append(sb, prefix, entry.getValue());
            }
        } else if (indexesInfo.size() == 1) {
            sb.append(prefix).append("Index Information:").append(StringUtils.NEW_LINE);
            Map.Entry<String, List<IndexChoiceNode>> entry = indexesInfo.entrySet().iterator().next();
            ScanningInfo scanningInfoEntry = scanningInfo != null ? scanningInfo.get(entry.getKey()) : null;
            prefix = indent(prefix);
            append(sb, prefix, null, entry.getValue(), scanningInfoEntry);
            //
        } else {
            sb.append(prefix).append("Index Information:").append(StringUtils.NEW_LINE);
            prefix = indent(prefix);
            for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
                ScanningInfo scanningInfoEntry = scanningInfo != null ? scanningInfo.get(entry.getKey()) : null;
                append(sb, prefix, entry.getKey(), entry.getValue(), scanningInfoEntry);
            }
        }
    }

    protected void append(StringBuilder sb, String prefix, String typeName, List<IndexChoiceNode> list, ScanningInfo scanningInfo) {
        if (typeName != null) {
            sb.append(prefix).append("Type name: ").append(typeName).append(StringUtils.NEW_LINE);
            prefix = indent(prefix);
        }
        append(sb, prefix, scanningInfo);
        sb.append(prefix).append("Index Info: ").append(StringUtils.NEW_LINE);
        prefix = indent(prefix);
        for (IndexChoiceNode node : list) {

            sb.append(prefix).append(node.getName()).append(StringUtils.NEW_LINE);
            sb.append(prefix).append("Options: ").append(StringUtils.NEW_LINE);
            String prefix1 = indent(prefix);
            for (IndexInfo option : node.getOptions()) {
                sb.append(prefix1).append(option.toString()).append(StringUtils.NEW_LINE);
            }
            sb.append(prefix).append("Selected: ").append(node.getChosen()).append(StringUtils.NEW_LINE);
        }
    }

    protected void append(StringBuilder sb, String prefix, ScanningInfo scanningInfo) {
        Integer scanned = scanningInfo != null ? scanningInfo.getScanned() : 0;
        Integer matched = scanningInfo != null ? scanningInfo.getMatched() : 0;
        sb.append(prefix).append("Scanned entries: ").append(scanned).append(StringUtils.NEW_LINE);
        sb.append(prefix).append("Matched entries: ").append(matched).append(StringUtils.NEW_LINE);
    }

    private static String indent(String prefix) {
        return prefix + "\t";
    }
}
