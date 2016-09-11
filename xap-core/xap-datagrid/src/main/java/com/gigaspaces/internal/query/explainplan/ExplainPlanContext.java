package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class ExplainPlanContext implements Externalizable{

    private SingleExplainPlan singleExplainPlan;
    private IndexChoiceNode fatherNode;
    private IndexChoiceNode match;

    public ExplainPlanContext() {
    }

    public ExplainPlanContext(SingleExplainPlan singleExplainPlan, IndexChoiceNode fatherNode, IndexChoiceNode match) {
        this.singleExplainPlan = singleExplainPlan;
        this.fatherNode = fatherNode;
        this.match = match;
    }

    public SingleExplainPlan getSingleExplainPlan() {
        return singleExplainPlan;
    }

    public void setSingleExplainPlan(SingleExplainPlan singleExplainPlan) {
        this.singleExplainPlan = singleExplainPlan;
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
        objectOutput.writeObject(this.singleExplainPlan);
        objectOutput.writeObject(this.fatherNode);
        objectOutput.writeObject(this.match);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.singleExplainPlan = (SingleExplainPlan) objectInput.readObject();
        this.fatherNode = (IndexChoiceNode) objectInput.readObject();
        this.match = (IndexChoiceNode) objectInput.readObject();
    }
}
