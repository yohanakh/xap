package com.gigaspaces.internal.query.explainplan;

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
import java.util.LinkedList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlan implements Externalizable {

//    private static final long serialVersionUID =
    private QueryOperationNode root;

    public ExplainPlan() {
    }

    public ExplainPlan(ICustomQuery template) {
        if (template != null) {
            this.root = buildQueryTree(template);
        }
    }


    private QueryOperationNode buildQueryTree(ICustomQuery customQuery) {
        QueryOperationNode currentNode = QueryTypes.getNode(customQuery);
        List<ICustomQuery> subQueries = getSubQueries(customQuery);
        if (subQueries == null) {
            return currentNode;
        }

        List<ICustomQuery> finalSubqueries = new ArrayList<ICustomQuery>();
        finalSubqueries.addAll(subQueries);
        for (ICustomQuery subQuery : subQueries) {
            if (subQuery instanceof ContainsCompositeRange || subQuery instanceof CompositeRange) {
                finalSubqueries.remove(subQuery);
                finalSubqueries.addAll(getSubQueries(subQuery));
            }
        }

        for (ICustomQuery subQuery : finalSubqueries) {
            QueryOperationNode son = buildQueryTree(subQuery);
            currentNode.addSon(son);
        }

        return currentNode;
    }

    public QueryOperationNode getRoot() {
        return root;
    }


    @Override
    public String toString() {
        return root.toString();
    }


    private List<ICustomQuery> getSubQueries(ICustomQuery customQuery) {
        if (customQuery instanceof CompoundContainsItemsCustomQuery) {
            return compoundConvertList(((CompoundContainsItemsCustomQuery) customQuery).getSubQueries());
        }
        if (customQuery instanceof CompoundAndCustomQuery) {
            return ((CompoundAndCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompoundOrCustomQuery) {
            return ((CompoundOrCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompositeRange) {
            return rangeConvertList(((CompositeRange) customQuery).get_ranges());
        }
        return null;
    }

    private List<ICustomQuery> rangeConvertList(LinkedList<Range> ranges) {
        List<ICustomQuery> res = new ArrayList<ICustomQuery>();
        for (Range range : ranges) {
            res.add(range);
        }
        return res;
    }

    private List<ICustomQuery> compoundConvertList(List<IContainsItemsCustomQuery> subQueries) {
        List<ICustomQuery> res = new ArrayList<ICustomQuery>();
        for (IContainsItemsCustomQuery subQuery : subQueries) {
            res.add(subQuery);
        }
        return res;
    }


    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(root);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.root = (QueryOperationNode) objectInput.readObject();
    }
}
