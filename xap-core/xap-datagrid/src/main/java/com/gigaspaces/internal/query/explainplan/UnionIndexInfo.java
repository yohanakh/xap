package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class UnionIndexInfo extends IndexInfo {
    public UnionIndexInfo() {
    }

    public UnionIndexInfo(List<IndexInfo> options) {
        StringBuilder name = new StringBuilder("[");
        Integer size = -1;
        if (options.size() > 0) {
            for (IndexInfo option : options) {
                String optionName = "(" + option.getName() + " " + option.getOperator() + " " + option.getValue() + ")";
                name.append(optionName).append(", ");
                if (option.getSize() < 0){
                    size = -1;
                    break;
                }else{
                   size += size;
                }
            }
            name.deleteCharAt(name.length() - 1);
            name.deleteCharAt(name.length() - 1);
        }
        name.append("]");
        setName(name.toString());
        setSize(size);
        setUsable(true);
    }

    @Override
    public String toString() {
        return "UnionIndexInfo{" + getName() + ", size=" + getSize() + "}";
    }
}
