package com.gigaspaces.internal.query.explainplan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlanContext implements Externalizable{

    private ExplainPlan explainPlan;
    private IndexChoiceNode fatherNode;
    private IndexChoiceNode match;

    public ExplainPlanContext() {
    }

    public ExplainPlanContext(ExplainPlan explainPlan, IndexChoiceNode fatherNode, IndexChoiceNode match) {
        this.explainPlan = explainPlan;
        this.fatherNode = fatherNode;
        this.match = match;
    }

    public ExplainPlan getExplainPlan() {
        return explainPlan;
    }

    public void setExplainPlan(ExplainPlan explainPlan) {
        this.explainPlan = explainPlan;
    }

    public IndexChoiceNode getFatherNode() {
        return fatherNode;
    }

    public void setFatherNode(IndexChoiceNode fatherNode) {
        this.fatherNode = fatherNode;
    }

    public IndexChoiceNode getMatch() {
        return match;
    }

    public void setMatch(IndexChoiceNode match) {
        this.match = match;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

    }
}
