/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The minimum aggregator aggregate the value evaluated by the expression. If the expression
 * evaluated result is NULL or MISSING, then the result is NULL.
 */
public class MinAggregator extends Aggregator<MinAggregator.MinState> {

  public MinAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.MIN.getName(), arguments, returnType);
  }

  @Override
  public MinState create() {
    return new MinState();
  }

  @Override
  protected MinState iterate(ExprValue value, MinState state) {
    state.min(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format("min(%s)", format(getArguments()));
  }

  protected static class MinState implements AggregationState {
    private ExprValue minResult;

    MinState() {
      minResult = LITERAL_NULL;
    }

    public void min(ExprValue value) {
      minResult = minResult.isNull() ? value : (minResult.compareTo(value) < 0) ? minResult : value;
    }

    @Override
    public ExprValue result() {
      return minResult;
    }
  }
}
