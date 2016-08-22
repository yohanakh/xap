package com.gigaspaces.internal.query.explain_plan;

import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.IContainsItemsCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.j_spaces.jdbc.builder.range.CompositeRange;
import com.j_spaces.jdbc.builder.range.ContainsCompositeRange;
import com.j_spaces.jdbc.builder.range.ContainsValueRange;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import com.j_spaces.jdbc.builder.range.InRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.RelationRange;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlan {

    private QueryOperationNode root;
    private TemplateEntryData entryData;
    private Map<Class<?>, Integer> queryTypes = new HashMap<Class<?>, Integer>();

    public ExplainPlan(IEntryData template) {
        this.entryData = (TemplateEntryData) template;
        initMap();
        this.root = buildQueryTree(entryData.getCustomQuery());
    }

    private void initMap() {
        queryTypes.put(CompoundContainsItemsCustomQuery.class,1);
        queryTypes.put(CompoundAndCustomQuery.class, 2);
        queryTypes.put(CompoundOrCustomQuery.class, 3);
        queryTypes.put(RelationRange.class, 4);
        queryTypes.put(SegmentRange.class, 5);
        queryTypes.put(EqualValueRange.class, 6);
        queryTypes.put(ContainsValueRange.class, 7);
        queryTypes.put(InRange.class, 8);
    }

    private QueryOperationNode buildQueryTree(ICustomQuery customQuery) {
        QueryOperationNode currentNode = getNode(customQuery);
        List<ICustomQuery> subQueries = getSubQueries(customQuery);
        if(subQueries == null) {
            return currentNode;
        }

        List<ICustomQuery> finalSubqueries =new ArrayList<ICustomQuery>();
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

    private QueryOperationNode getNode(ICustomQuery customQuery) {
        Integer typeId = queryTypes.get(customQuery.getClass());
        switch (typeId){
            case 1: return new QueryJunctionNode("CONTAINS");
            case 2: return new QueryJunctionNode("AND");
            case 3: return new QueryJunctionNode("OR");
            case 4: RelationRange relation = (RelationRange) customQuery;
                    String relationFunc = createFunctionString(relation.getFunctionCallDescription(), relation.getPath());
                    return new RangeNode(relation.getPath(), relation.getValue(), getRelationOperator(relation.getRelation()), relationFunc);
            case 5: SegmentRange segment = (SegmentRange) customQuery;
                    String segmentFunc = createFunctionString(segment.getFunctionCallDescription(), segment.getPath());
                    Object value = segment.getMin() != null ? segment.getMin() : segment.getMax();
                    return new RangeNode(segment.getPath(), value, getSegmentOperator(segment), segmentFunc);
            case 6: EqualValueRange equalValue = (EqualValueRange) customQuery;
                    String equalValueFunc = createFunctionString(equalValue.getFunctionCallDescription(), equalValue.getPath());
                    return new RangeNode(equalValue.getPath(), equalValue.getValue(), QueryOperator.EQ, equalValueFunc);
            case 7: ContainsValueRange containsValue = (ContainsValueRange) customQuery;
                    String containsValueFunc = createFunctionString(containsValue.getFunctionCallDescription(), containsValue.getPath());
                    return new RangeNode(containsValue.getPath(), containsValue.getValue(), getOperartorFromMatchCode(containsValue.get_templateMatchCode()), containsValueFunc);
            case 8: InRange in = (InRange) customQuery;
                    String inFunc = createFunctionString(in.getFunctionCallDescription(), in.getPath());
                    return new RangeNode(in.getPath(), ((InSpacePredicate) in.getPredicate()).getInValues(), QueryOperator.IN, inFunc);

            default: return null;
        }

    }




    @Override
    public String toString() {
        return root.toString();
    }

    private String createFunctionString(FunctionCallDescription functionCallDescription, String path) {
        if(functionCallDescription == null)
            return null;
        StringBuilder res = new StringBuilder(functionCallDescription.getName() + "(" + path + ",");
        int num = functionCallDescription.getNumberOfArguments();
        boolean hasArgs =false;
        for (int i=0; i< num; i++){
            if(functionCallDescription.getArgument(i) != null){
                res.append(functionCallDescription.getArgument(i) + ",");
                hasArgs =true;
            }
        }
        if (hasArgs){
            res.deleteCharAt(res.length() - 1);
        }
        return res + ")" ;
    }

    private List<ICustomQuery> getSubQueries(ICustomQuery customQuery) {
        if(customQuery instanceof CompoundContainsItemsCustomQuery){
            return compoundConvertList(((CompoundContainsItemsCustomQuery) customQuery).getSubQueries());
        }
        if (customQuery instanceof CompoundAndCustomQuery){
            return  ((CompoundAndCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompoundOrCustomQuery){
            return  ((CompoundOrCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompositeRange){
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
        return  res;
    }

    private QueryOperator getOperartorFromMatchCode(short templateMatchCode) {
        switch (templateMatchCode){
            case 0 : return  QueryOperator.EQ;
            case 1 : return  QueryOperator.NE;
            case 2 : return  QueryOperator.GT;
            case 3 : return  QueryOperator.GE;
            case 4 : return  QueryOperator.LT;
            case 5 : return  QueryOperator.LE;
            case 6 : return  QueryOperator.IS_NULL;
            case 7 : return  QueryOperator.NOT_NULL;
            case 8 : return  QueryOperator.REGEX;
            case 9 : return  QueryOperator.CONTAINS_TOKEN;
            case 10 : return  QueryOperator.NOT_REGEX;
            case 11 : return  QueryOperator.IN;
            default: return  QueryOperator.NOT_SUPPORTED;
        }
    }

    private QueryOperator getSegmentOperator(SegmentRange segment) {
        if(segment.getMax() == null) {
            if (segment.isIncludeMin()) {
                return QueryOperator.GE;
            } else {
                return QueryOperator.GT;
            }
        }
        else {
            if (segment.isIncludeMax()) {
                return QueryOperator.LE;
            }
        }
        return QueryOperator.LT;
    }

    private QueryOperator getRelationOperator(String relation) {
        if("INTERSECTS".equals(relation)){
            return QueryOperator.INTERSECTS;
        }
        else{
            return QueryOperator.WITHIN;
        }
    }
}
