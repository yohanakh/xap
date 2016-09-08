package com.gigaspaces.internal.query.explainplan;

import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class UnionIndexInfo extends IndexInfo {
    public UnionIndexInfo() {
    }

    public UnionIndexInfo(List<IndexInfo> options) {
        StringBuilder name = new StringBuilder("[");
        Integer size = 0;
        if (options.size() > 0) {
            for (IndexInfo option : options) {
                String optionName = "(" + option.getName() + " " + option.getOperator() + " " + option.getValue() + ")";
                name.append(optionName).append(", ");
                size = option.getSize() >= 0 ? size + option.getSize() : -1;
            }
            name.deleteCharAt(name.length() - 1);
            name.deleteCharAt(name.length() - 1);
        }
        name.append("]");
        setName(name.toString());
        size = size < 0 ? -1 : size;
        setSize(size);
    }

    @Override
    public String toString() {
        return "UnionIndexInfo{" + getName() + ", size=" + getSize() + "}";
    }
}
