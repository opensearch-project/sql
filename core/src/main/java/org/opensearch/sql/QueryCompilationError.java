/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql;

import static org.opensearch.sql.common.utils.StringUtils.format;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Grouping error messages from {@link SemanticCheckException} thrown during query compilation. */
@UtilityClass
public class QueryCompilationError {

  public static SemanticCheckException fieldNotInGroupByClauseError(String name) {
    return new SemanticCheckException(
        format(
            "Field [%s] must appear in the GROUP BY clause or be used in an aggregate function",
            name));
  }

  public static SemanticCheckException aggregateFunctionNotAllowedInGroupByError(
      String functionName) {
    return new SemanticCheckException(
        format(
            "Aggregate function is not allowed in a GROUP BY clause, but found [%s]",
            functionName));
  }

  public static SemanticCheckException nonBooleanExpressionInFilterOrHavingError(ExprType type) {
    return new SemanticCheckException(
        format(
            "FILTER or HAVING expression must be type boolean, but found [%s]", type.typeName()));
  }

  public static SemanticCheckException aggregateFunctionNotAllowedInFilterError(
      String functionName) {
    return new SemanticCheckException(
        format("Aggregate function is not allowed in a FILTER, but found [%s]", functionName));
  }

  public static SemanticCheckException windowFunctionNotAllowedError() {
    return new SemanticCheckException("Window functions are not allowed in WHERE or HAVING");
  }

  public static SemanticCheckException unsupportedAggregateFunctionError(String functionName) {
    return new SemanticCheckException(format("Unsupported aggregation function %s", functionName));
  }

  public static SemanticCheckException ordinalRefersOutOfBounds(int ordinal) {
    return new SemanticCheckException(
        format("Ordinal [%d] is out of bound of select item list", ordinal));
  }

  public static SemanticCheckException groupByClauseIsMissingError(UnresolvedExpression expr) {
    return new SemanticCheckException(
        format(
            "Explicit GROUP BY clause is required because expression [%s] contains non-aggregated"
                + " column",
            expr));
  }
}
