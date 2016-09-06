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

    public IndexChoiceNode(String name, List<IndexInfo> options, IndexInfo chosen) {
        this.name = name;
        this.options = options;
        this.chosen = chosen;
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
        writeList(objectOutput, this.options);
        objectOutput.writeObject(this.chosen);
    }

    private void writeList(ObjectOutput objectOutput, List<IndexInfo> options) throws IOException {
        int size = options.size();
        objectOutput.writeInt(size);
        for (IndexInfo option : options) {
            objectOutput.writeObject(option);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.options = readList(objectInput);
        this.chosen = (IndexInfo) objectInput.readObject();
    }

    private List<IndexInfo> readList(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        List<IndexInfo> res = new ArrayList<IndexInfo>();
        int size = objectInput.readInt();
        for(int i=0; i<size; i++){
            res.add((IndexInfo) objectInput.readObject());
        }
        return res;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexChoiceNode that = (IndexChoiceNode) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (options != null ? !options.equals(that.options) : that.options != null) return false;
        return !(chosen != null ? !chosen.equals(that.chosen) : that.chosen != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + (chosen != null ? chosen.hashCode() : 0);
        return result;
    }
}
