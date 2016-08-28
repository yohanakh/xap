package com.gigaspaces.internal.query.explainplan;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public enum QueryOperator {
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE,
    IS_NULL,
    NOT_NULL,
    REGEX,
    CONTAINS_TOKEN,
    NOT_REGEX,
    IN,
    INTERSECTS,
    WITHIN,
    NOT_SUPPORTED,
    BETWEEN
}
