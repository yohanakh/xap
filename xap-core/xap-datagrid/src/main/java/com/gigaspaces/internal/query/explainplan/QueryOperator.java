package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
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
