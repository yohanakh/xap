package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.IContainsItemsCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.j_spaces.jdbc.builder.range.CompositeRange;
import com.j_spaces.jdbc.builder.range.ContainsCompositeRange;
import com.j_spaces.jdbc.builder.range.Range;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlan implements Externalizable {

    //    private static final long serialVersionUID =
    private String partitionId;
    private QueryOperationNode root;
    private Map<String, List<IndexChoiceNode>> indexesInfo;

    public ExplainPlan() {
        this.indexesInfo = new HashMap<String, List<IndexChoiceNode>>();
    }

    public ExplainPlan(ICustomQuery customQuery) {
        this.indexesInfo = new HashMap<String, List<IndexChoiceNode>>();
        this.root = ExplainPlanUtil.buildQueryTree(customQuery);
    }

    public QueryOperationNode getRoot() {
        return root;
    }


    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("Query Tree: \n").append(this.root).append("\n");
        if(indexesInfo.size() > 0){
            res.append("Index Information: \n");
            for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
                res.append(entry.getKey()).append(": \n");
                res.append(entry.getValue()).append("\n");
            }
        }
        return res.toString();
    }

    public void setRoot(QueryOperationNode root) {
        this.root = root;
    }

    public void setIndexesInfo(Map<String, List<IndexChoiceNode>> indexesInfo) {
        this.indexesInfo = indexesInfo;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public Map<String, List<IndexChoiceNode>> getIndexesInfo() {
        return indexesInfo;
    }

    public void addIndexesInfo(String type, List<IndexChoiceNode> scanSelectionTree) {
        this.indexesInfo.put(type, scanSelectionTree);
    }

    public List<IndexChoiceNode> getScanSelectionTree(String clazz) {
        return indexesInfo.get(clazz);
    }

    public void addScanIndexChoiceNode(String clazz, IndexChoiceNode indexChoiceNode) {
        if (!indexesInfo.containsKey(clazz)) {
            List<IndexChoiceNode> scanSelectionTree = new ArrayList<IndexChoiceNode>();
            indexesInfo.put(clazz, scanSelectionTree);
        }
        indexesInfo.get(clazz).add(indexChoiceNode);
    }

    public IndexChoiceNode getLatestIndexChoiceNode(String clazz) {
        if (indexesInfo.size() == 0)
            return null;
        List<IndexChoiceNode> scanSelectionTree = indexesInfo.get(clazz);
        return scanSelectionTree.get(scanSelectionTree.size() - 1);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(root);
        IOUtils.writeString(objectOutput, partitionId);
        writeMap(objectOutput, indexesInfo);
    }

    private void writeMap(ObjectOutput objectOutput, Map<String, List<IndexChoiceNode>> map) throws IOException {
        int length = map.size();
        objectOutput.writeInt(length);
        for (Map.Entry<String, List<IndexChoiceNode>> entry : map.entrySet()) {
            objectOutput.writeObject(entry.getKey());
            if (entry.getValue() == null)
                objectOutput.writeInt(-1);
            else {
                int listLength = entry.getValue().size();
                objectOutput.writeInt(listLength);
                for (int i = 0; i < listLength; i++)
                    objectOutput.writeObject(entry.getValue().get(i));
            }
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.root = (QueryOperationNode) objectInput.readObject();
        this.partitionId = IOUtils.readString(objectInput);
        this.indexesInfo = readMap(objectInput);
    }

    private Map<String, List<IndexChoiceNode>> readMap(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        Map<String, List<IndexChoiceNode>> map = null;
        int length = (int) objectInput.readInt();
        if (length >= 0) {
            map = new HashMap<String, List<IndexChoiceNode>>(length);
            for (int i = 0; i < length; i++) {
                String key = (String) objectInput.readObject();
                List<IndexChoiceNode> list = null;
                int listLength = objectInput.readInt();
                if (listLength >= 0) {
                    list = new ArrayList<IndexChoiceNode>(listLength);
                    for (int j = 0; j < listLength; j++)
                        list.add((IndexChoiceNode) objectInput.readObject());
                }
                map.put(key, list);
            }
        }

        return map;
    }
}
