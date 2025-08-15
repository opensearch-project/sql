/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Placeholder aggregator for functions that are only supported in Calcite v3 engine. This class
 * exists to register the function signatures in the legacy v2 system, but will throw an error if
 * actually executed outside of Calcite.
 */
public class CalciteOnlyAggregator extends Aggregator<CalciteOnlyAggregator.CalciteOnlyState> {

  private final String functionName;

  public CalciteOnlyAggregator(
      String functionName, List<Expression> arguments, ExprCoreType returnType) {
    super(FunctionName.of(functionName), arguments, returnType);
    this.functionName = functionName;
  }

  @Override
  public CalciteOnlyState create() {
    throw new ExpressionEvaluationException(
        String.format(
            "%s() function is only supported when Calcite engine is enabled. "
                + "Please enable Calcite by setting 'plugins.calcite.enabled' to true.",
            functionName));
  }

  @Override
  protected CalciteOnlyState iterate(ExprValue value, CalciteOnlyState state) {
    throw new ExpressionEvaluationException(
        String.format(
            "%s() function is only supported when Calcite engine is enabled. "
                + "Please enable Calcite by setting 'plugins.calcite.enabled' to true.",
            functionName));
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", functionName, getArguments());
  }

  protected static class CalciteOnlyState implements AggregationState {
    @Override
    public ExprValue result() {
      throw new ExpressionEvaluationException("CalciteOnlyAggregator should never be executed");
    }
  }
}
