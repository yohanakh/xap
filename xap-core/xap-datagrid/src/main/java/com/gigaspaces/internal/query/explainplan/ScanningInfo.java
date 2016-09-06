package com.gigaspaces.internal.query.explainplan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ScanningInfo implements Externalizable {

    private Integer scanned;
    private Integer matched;

    public ScanningInfo() {
        this.scanned = 0;
        this.matched = 0;
    }

    public ScanningInfo(Integer scanned, Integer matched) {
        this.scanned = scanned;
        this.matched = matched;
    }

    public Integer getScanned() {
        return scanned;
    }

    public void setScanned(Integer scanned) {
        this.scanned = scanned;
    }

    public Integer getMatched() {
        return matched;
    }

    public void setMatched(Integer matched) {
        this.matched = matched;
    }

    @Override
    public String toString() {
        return "ScanningInfo{" +
                "scanned=" + scanned +
                ", matched=" + matched +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(scanned);
        objectOutput.writeObject(matched);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.scanned = (Integer) objectInput.readObject();
        this.matched = (Integer) objectInput.readObject();
    }
}
