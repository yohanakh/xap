package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.j_spaces.jdbc.builder.range.ContainsValueRange;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import com.j_spaces.jdbc.builder.range.InRange;
import com.j_spaces.jdbc.builder.range.IsNullRange;
import com.j_spaces.jdbc.builder.range.NotEqualValueRange;
import com.j_spaces.jdbc.builder.range.NotNullRange;
import com.j_spaces.jdbc.builder.range.NotRegexRange;
import com.j_spaces.jdbc.builder.range.RegexRange;
import com.j_spaces.jdbc.builder.range.RelationRange;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tamirt on 23/08/16.
 */
public enum QueryTypes {
    COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY,
    COMPOUND_AND_CUSTOM_QUERY,
    COMPOUND_OR_CUSTOM_QUERY,
    RELATION_RANGE,
    SEGMENT_RANGE,
    EQUAL_VALUE_RANGE,
    CONTAINS_VALUE_RANGE,
    IN_RANGE,
    IS_NULL_RANGE,
    NOT_NULL_RANGE,
    NOT_EQUAL_VALUE_RANGE,
    REGEX_RANGE,
    NOT_REGEX_RANGE;

    public static final Map<Class<?>, QueryTypes> queryTypes = Collections.unmodifiableMap(initMap());

    private static Map<Class<?>, QueryTypes> initMap() {
        Map<Class<?>, QueryTypes> map = new HashMap<Class<?>, QueryTypes>();
        map.put(CompoundContainsItemsCustomQuery.class, QueryTypes.COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY);
        map.put(CompoundAndCustomQuery.class, QueryTypes.COMPOUND_AND_CUSTOM_QUERY);
        map.put(CompoundOrCustomQuery.class, QueryTypes.COMPOUND_OR_CUSTOM_QUERY);
        map.put(RelationRange.class, QueryTypes.RELATION_RANGE);
        map.put(SegmentRange.class, QueryTypes.SEGMENT_RANGE);
        map.put(EqualValueRange.class, QueryTypes.EQUAL_VALUE_RANGE);
        map.put(ContainsValueRange.class, QueryTypes.CONTAINS_VALUE_RANGE);
        map.put(InRange.class, QueryTypes.IN_RANGE);
        map.put(IsNullRange.class, QueryTypes.IS_NULL_RANGE);
        map.put(NotNullRange.class, QueryTypes.NOT_NULL_RANGE);
        map.put(NotEqualValueRange.class, QueryTypes.NOT_EQUAL_VALUE_RANGE);
        map.put(RegexRange.class, QueryTypes.REGEX_RANGE);
        map.put(NotRegexRange.class,QueryTypes.NOT_REGEX_RANGE);
        return map;
    }

    public static QueryOperationNode getNode(ICustomQuery customQuery) {
        QueryTypes queryType = queryTypes.get(customQuery.getClass());
        switch (queryType) {
            case COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY:
                return new QueryJunctionNode("CONTAINS");
            case COMPOUND_AND_CUSTOM_QUERY:
                return new QueryJunctionNode("AND");
            case COMPOUND_OR_CUSTOM_QUERY:
                return new QueryJunctionNode("OR");
            case RELATION_RANGE:
                RelationRange relation = (RelationRange) customQuery;
                String relationFunc = createFunctionString(relation.getFunctionCallDescription(), relation.getPath());
                return new RangeNode(relation.getPath(), relation.getValue(), getRelationOperator(relation.getRelation()), relationFunc);
            case SEGMENT_RANGE:
                SegmentRange segment = (SegmentRange) customQuery;
                String segmentFunc = createFunctionString(segment.getFunctionCallDescription(), segment.getPath());
                Object value = segment.getMin() != null ? segment.getMin() : segment.getMax();
                if(segment.getMin() != null && segment.getMax() != null ){
                    //String fieldName, QueryOperator operator, String functionName, Integer minValue, Integer maxValue
                    return new BetweenRangeNode(segment.getPath(), getSegmentOperator(segment), segmentFunc, segment.getMin(), segment.getMax());
                }
                return new RangeNode(segment.getPath(), value, getSegmentOperator(segment), segmentFunc);
            case EQUAL_VALUE_RANGE:
                EqualValueRange equalValue = (EqualValueRange) customQuery;
                String equalValueFunc = createFunctionString(equalValue.getFunctionCallDescription(), equalValue.getPath());
                return new RangeNode(equalValue.getPath(), equalValue.getValue(), QueryOperator.EQ, equalValueFunc);
            case CONTAINS_VALUE_RANGE:
                ContainsValueRange containsValue = (ContainsValueRange) customQuery;
                String containsValueFunc = createFunctionString(containsValue.getFunctionCallDescription(), containsValue.getPath());
                return new RangeNode(containsValue.getPath(), containsValue.getValue(), getOperartorFromMatchCode(containsValue.get_templateMatchCode()), containsValueFunc);
            case IN_RANGE:
                InRange in = (InRange) customQuery;
                String inFunc = createFunctionString(in.getFunctionCallDescription(), in.getPath());
                return new RangeNode(in.getPath(), ((InSpacePredicate) in.getPredicate()).getInValues(), QueryOperator.IN, inFunc);
            case IS_NULL_RANGE:
                IsNullRange isNull = (IsNullRange) customQuery;
                String isNullFunc = createFunctionString(isNull.getFunctionCallDescription(), isNull.getPath());
                return new RangeNode(isNull.getPath(), ((InSpacePredicate) isNull.getPredicate()).getInValues(), QueryOperator.IS_NULL, isNullFunc);
            case NOT_NULL_RANGE:
                NotNullRange notNull = (NotNullRange) customQuery;
                String notNullFunc = createFunctionString(notNull.getFunctionCallDescription(), notNull.getPath());
                return new RangeNode(notNull.getPath(), ((InSpacePredicate) notNull.getPredicate()).getInValues(), QueryOperator.NOT_NULL, notNullFunc);
            case NOT_EQUAL_VALUE_RANGE:
                NotEqualValueRange notEqualValue = (NotEqualValueRange) customQuery;
                String notEqualValueFunc = createFunctionString(notEqualValue.getFunctionCallDescription(), notEqualValue.getPath());
                return new RangeNode(notEqualValue.getPath(), ((InSpacePredicate) notEqualValue.getPredicate()).getInValues(), QueryOperator.NE, notEqualValueFunc);
            case REGEX_RANGE:
                RegexRange regex = (RegexRange) customQuery;
                String regexFunc = createFunctionString(regex.getFunctionCallDescription(), regex.getPath());
                return new RangeNode(regex.getPath(), ((InSpacePredicate) regex.getPredicate()).getInValues(), QueryOperator.REGEX, regexFunc);
            case NOT_REGEX_RANGE:
                NotRegexRange notRegex = (NotRegexRange) customQuery;
                String notRegexFunc = createFunctionString(notRegex.getFunctionCallDescription(), notRegex.getPath());
                return new RangeNode(notRegex.getPath(), ((InSpacePredicate) notRegex.getPredicate()).getInValues(), QueryOperator.NOT_REGEX, notRegexFunc);
            default:
                return null;
        }
    }

    private static QueryOperator getOperartorFromMatchCode(short templateMatchCode) {
        switch (templateMatchCode) {
            case 0:
                return QueryOperator.EQ;
            case 1:
                return QueryOperator.NE;
            case 2:
                return QueryOperator.GT;
            case 3:
                return QueryOperator.GE;
            case 4:
                return QueryOperator.LT;
            case 5:
                return QueryOperator.LE;
            case 6:
                return QueryOperator.IS_NULL;
            case 7:
                return QueryOperator.NOT_NULL;
            case 8:
                return QueryOperator.REGEX;
            case 9:
                return QueryOperator.CONTAINS_TOKEN;
            case 10:
                return QueryOperator.NOT_REGEX;
            case 11:
                return QueryOperator.IN;
            default:
                return QueryOperator.NOT_SUPPORTED;
        }
    }


    private static QueryOperator getSegmentOperator(SegmentRange segment) {
        if(segment.getMax() != null){
            if( segment.getMin() != null){
                return QueryOperator.BETWEEN;
            }
            else if(segment.isIncludeMax()){
                return QueryOperator.LE;
            }else {
                return QueryOperator.LT;
            }
        }else if(segment.getMin() != null){
            if(segment.isIncludeMin()){
                return QueryOperator.GE;
            }else {
                return QueryOperator.GT;
            }
        }
        return null;
    }

    private static QueryOperator getRelationOperator(String relation) {
        if ("INTERSECTS".equals(relation)) {
            return QueryOperator.INTERSECTS;
        } else {
            return QueryOperator.WITHIN;
        }
    }


    private static String createFunctionString(FunctionCallDescription functionCallDescription, String path) {
        if (functionCallDescription == null)
            return null;
        StringBuilder res = new StringBuilder(functionCallDescription.getName() + "(" + path + ",");
        int num = functionCallDescription.getNumberOfArguments();
        for (int i = 0; i < num; i++) {
            if (functionCallDescription.getArgument(i) != null) {
                res.append(functionCallDescription.getArgument(i) + ",");
            }
        }
        res.deleteCharAt(res.length() - 1);
        return res + ")";
    }


}
