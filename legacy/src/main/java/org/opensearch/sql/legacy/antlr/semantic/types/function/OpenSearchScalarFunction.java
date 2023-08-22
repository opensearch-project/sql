/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.function;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.GEO_POINT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;

import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression;

/** OpenSearch special scalar functions */
public enum OpenSearchScalarFunction implements TypeExpression {
  DATE_HISTOGRAM(), // this is aggregate function
  DAY_OF_MONTH(func(DATE).to(INTEGER)),
  DAY_OF_YEAR(func(DATE).to(INTEGER)),
  DAY_OF_WEEK(func(DATE).to(INTEGER)),
  EXCLUDE(), // can only be used in SELECT?
  EXTENDED_STATS(), // need confirm
  FIELD(), // couldn't find test cases related
  FILTER(),
  GEO_BOUNDING_BOX(func(GEO_POINT, NUMBER, NUMBER, NUMBER, NUMBER).to(BOOLEAN)),
  GEO_CELL(), // optional arg or overloaded spec is required.
  GEO_DISTANCE(func(GEO_POINT, STRING, NUMBER, NUMBER).to(BOOLEAN)),
  GEO_DISTANCE_RANGE(func(GEO_POINT, STRING, NUMBER, NUMBER).to(BOOLEAN)),
  GEO_INTERSECTS(), // ?
  GEO_POLYGON(), // varargs is required for 2nd arg
  HISTOGRAM(), // same as date_histogram
  HOUR_OF_DAY(func(DATE).to(INTEGER)),
  INCLUDE(), // same as exclude
  IN_TERMS(), // varargs
  MATCHPHRASE(func(STRING, STRING).to(BOOLEAN), func(STRING).to(STRING)), // slop arg is optional
  MATCH_PHRASE(MATCHPHRASE.specifications()),
  MATCHQUERY(func(STRING, STRING).to(BOOLEAN), func(STRING).to(STRING)),
  MATCH_QUERY(MATCHQUERY.specifications()),
  MINUTE_OF_DAY(func(DATE).to(INTEGER)), // or long?
  MINUTE_OF_HOUR(func(DATE).to(INTEGER)),
  MONTH_OF_YEAR(func(DATE).to(INTEGER)),
  MULTIMATCH(), // kw arguments
  MULTI_MATCH(MULTIMATCH.specifications()),
  NESTED(), // overloaded
  PERCENTILES(), // ?
  REGEXP_QUERY(), // ?
  REVERSE_NESTED(), // need overloaded
  QUERY(func(STRING).to(BOOLEAN)),
  RANGE(), // aggregate function
  SCORE(), // semantic problem?
  SECOND_OF_MINUTE(func(DATE).to(INTEGER)),
  STATS(),
  TERM(), // semantic problem
  TERMS(), // semantic problem
  TOPHITS(), // only available in SELECT
  WEEK_OF_YEAR(func(DATE).to(INTEGER)),
  WILDCARDQUERY(func(STRING, STRING).to(BOOLEAN), func(STRING).to(STRING)),
  WILDCARD_QUERY(WILDCARDQUERY.specifications());

  private final TypeExpressionSpec[] specifications;

  OpenSearchScalarFunction(TypeExpressionSpec... specifications) {
    this.specifications = specifications;
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public TypeExpressionSpec[] specifications() {
    return specifications;
  }

  private static TypeExpressionSpec func(Type... argTypes) {
    return new TypeExpressionSpec().map(argTypes);
  }

  @Override
  public String toString() {
    return "Function [" + name() + "]";
  }
}
