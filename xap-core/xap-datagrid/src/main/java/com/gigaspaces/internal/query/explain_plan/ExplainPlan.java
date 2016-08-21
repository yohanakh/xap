package com.gigaspaces.internal.query.explain_plan;

import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.TemplateEntryData;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlan {

    private QueryOperationNode root;
    private TemplateEntryData entryData;

    public ExplainPlan(IEntryData template) {
        this.entryData = (TemplateEntryData) template;
        buildQueryTree();
    }

    private void buildQueryTree() {
        QueryOperationNode currentNode = getNode(entryData.getCustomQuery());
//        if(entryData.getCustomQuery())
    }

    private QueryOperationNode getNode(ICustomQuery customQuery) {
        return null;
    }
}
