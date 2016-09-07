package com.gigaspaces.internal.query.explainplan;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class TraverseAll extends IndexInfo{
    public TraverseAll() {
    }

    @Override
    public String toString() {
        return "IndexInfo{TRAVERSING_All}";
    }
}
