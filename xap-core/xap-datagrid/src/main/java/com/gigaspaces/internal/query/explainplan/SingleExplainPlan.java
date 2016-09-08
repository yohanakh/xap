package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.io.IOUtils;

import com.gigaspaces.internal.query.ICustomQuery;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class SingleExplainPlan implements Externalizable {

    //    private static final long serialVersionUID =
    private String partitionId;
    private QueryOperationNode root;
    private Map<String, List<IndexChoiceNode>> indexesInfo;
    private Map<String, ScanningInfo> scanningInfo; // Pair = (int scanned, int matched)

    public SingleExplainPlan() {
        this.scanningInfo = new HashMap<String, ScanningInfo>();
        this.indexesInfo = new HashMap<String, List<IndexChoiceNode>>();
    }

    public SingleExplainPlan(ICustomQuery customQuery) {
        this.scanningInfo = new HashMap<String, ScanningInfo>();
        this.indexesInfo = new HashMap<String, List<IndexChoiceNode>>();
        this.root = ExplainPlanUtil.buildQueryTree(customQuery);
    }




    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("Query Tree: \n").append(this.root).append("\n");
        if(indexesInfo.size() > 0){
            res.append("Index Information: \n");
            for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
                res.append(entry.getKey()).append(": \n");
                res.append(entry.getValue()).append("\n");
                if(scanningInfo != null && scanningInfo.containsKey(entry.getKey())){
                    Integer scanned = scanningInfo.get(entry.getKey()).getScanned();
                    Integer matched = scanningInfo.get(entry.getKey()).getMatched();
                    res.append("number of scanned entries: ").append(scanned).append("\n");
                    res.append("number of matched entries: ").append(matched).append("\n");
                }
            }
        }
        else {
            res.append("NO INDEX USED\n");
            for (Map.Entry<String, ScanningInfo> scanningInfoEntry : scanningInfo.entrySet()) {
                res.append(scanningInfoEntry.getKey()).append(":\n");
                res.append("number of scanned entries: ").append(scanningInfoEntry.getValue().getScanned()).append("\n");
                res.append("number of matched entries: ").append(scanningInfoEntry.getValue().getMatched()).append("\n");
            }
        }
        return res.toString();
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public void setRoot(QueryOperationNode root) {
        this.root = root;
    }

    public void setIndexesInfo(Map<String, List<IndexChoiceNode>> indexesInfo) {
        this.indexesInfo = indexesInfo;
    }

    public void setScanningInfo(Map<String, ScanningInfo> scanningInfo) {
        this.scanningInfo = scanningInfo;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public QueryOperationNode getRoot() {
        return root;
    }

    public Map<String, List<IndexChoiceNode>> getIndexesInfo() {
        return indexesInfo;
    }

    public Map<String, ScanningInfo> getScanningInfo() {
        return scanningInfo;
    }

    public void addIndexesInfo(String type, List<IndexChoiceNode> scanSelectionTree) {
        this.indexesInfo.put(type, scanSelectionTree);
    }

    public void addScanIndexChoiceNode(String clazz, IndexChoiceNode indexChoiceNode) {
        if (!indexesInfo.containsKey(clazz)) {
            List<IndexChoiceNode> scanSelectionTree = new ArrayList<IndexChoiceNode>();
            indexesInfo.put(clazz, scanSelectionTree);
        }
        indexesInfo.get(clazz).add(indexChoiceNode);
    }

    public List<IndexChoiceNode> getScanSelectionTree(String clazz) {
        return indexesInfo.get(clazz);
    }

    public IndexChoiceNode getLatestIndexChoiceNode(String clazz) {
        if (indexesInfo.size() == 0)
            return null;
        List<IndexChoiceNode> scanSelectionTree = indexesInfo.get(clazz);
        return scanSelectionTree.get(scanSelectionTree.size() - 1);
    }

    public Integer getNumberOfScannedEntries(String clazz){
        return scanningInfo.get(clazz).getScanned();
    }

    public Integer getNumberOfMatchedEntries(String clazz){
        return scanningInfo.get(clazz).getMatched();
    }

    public void incrementScanned(String clazz){
        if(!scanningInfo.containsKey(clazz)){
            ScanningInfo info = new ScanningInfo();
            scanningInfo.put(clazz, info);
        }
        ScanningInfo info = this.scanningInfo.get(clazz);
        info.setScanned(info.getScanned() +1);
    }

    public void incrementMatched(String clazz){
        if(!scanningInfo.containsKey(clazz)){
            ScanningInfo info = new ScanningInfo();
            scanningInfo.put(clazz, info);
        }
        ScanningInfo info = this.scanningInfo.get(clazz);
        info.setMatched(info.getMatched() +1);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(root);
        IOUtils.writeString(objectOutput, partitionId);
        writeIndexes(objectOutput);
        writeScannigInfo(objectOutput);

    }

    private void writeScannigInfo(ObjectOutput objectOutput) throws IOException {
        int length = scanningInfo.size();
        objectOutput.writeInt(length);
        for (Map.Entry<String, ScanningInfo> entry : scanningInfo.entrySet()) {
            objectOutput.writeObject(entry.getKey());
            objectOutput.writeObject(entry.getValue());
        }
    }

    private void writeIndexes(ObjectOutput objectOutput) throws IOException {
        int length = indexesInfo.size();
        objectOutput.writeInt(length);
        for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
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
        this.indexesInfo = readIndexes(objectInput);
        this.scanningInfo = readScanningInfo(objectInput);
    }

    private Map<String, ScanningInfo> readScanningInfo(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        int length = objectInput.readInt();
        Map<String, ScanningInfo> map = new HashMap<String, ScanningInfo>();
        for(int i=0; i<length; i++){
            String key = (String) objectInput.readObject();
            ScanningInfo val = (ScanningInfo) objectInput.readObject();
            map.put(key, val);
        }
        return map;
    }

    private Map<String, List<IndexChoiceNode>> readIndexes(ObjectInput objectInput) throws IOException, ClassNotFoundException {
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
