package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class IndexChoiceNode implements Externalizable {

    private String name; // AND or OR
    private List<IndexInfo> options;
    private IndexInfo chosen;

    public IndexChoiceNode(String name) {
        this.name = name;
        options = new ArrayList<IndexInfo>();
    }

    public IndexChoiceNode() {
        this.options = new ArrayList<IndexInfo>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<IndexInfo> getOptions() {
        return options;
    }

    public void addOption(IndexInfo option) {
        this.options.add(option);
    }

    public IndexInfo getChosen() {
        return chosen;
    }

    public void setChosen(IndexInfo chosen) {
        this.chosen = chosen;
    }

    public IndexInfo getOptionByName(String name){
        for (IndexInfo indexInfo : options) {
            if(indexInfo.getName().equals(name))
                return indexInfo;
        }
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        IOUtils.writeString(objectOutput, this.name);
        IOUtils.writeList(objectOutput, this.options);
        objectOutput.writeObject(this.chosen);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.options = (List<IndexInfo>) IOUtils.readList(objectInput);
        this.chosen = (IndexInfo) objectInput.readObject();
    }

    @Override
    public String toString() {
        return "\n" + name + "{ " +
                "options:  " + getOptionsString() +
                ",  chosen:  " + chosen + " }";
    }

    private String getOptionsString() {
        StringBuilder res = new StringBuilder("[");
        for (IndexInfo option : options) {
            if(option instanceof BetweenIndexInfo){
                res.append(((BetweenIndexInfo) option).toString());
            }
            else{
                res.append(option.toString());
            }
        }
        return  res.append("]").toString();
    }
}
