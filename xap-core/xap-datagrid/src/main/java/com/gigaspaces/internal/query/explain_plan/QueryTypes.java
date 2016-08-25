package com.gigaspaces.internal.query.explain_plan;

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
    NOT_REGEX_RANGE
}
