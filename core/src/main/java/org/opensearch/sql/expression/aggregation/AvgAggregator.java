/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The average aggregator aggregate the value evaluated by the expression.
 * If the expression evaluated result is NULL or MISSING, then the result is NULL.
 */
public class AvgAggregator extends Aggregator<AvgAggregator.AvgState> {

  public AvgAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.AVG.getName(), arguments, returnType);
  }

  @Override
  public AvgState create() {
    return new AvgState();
  }

  @Override
  protected AvgState iterate(ExprValue value, AvgState state) {
    state.count++;
    state.total += ExprValueUtils.getDoubleValue(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "avg(%s)", format(getArguments()));
  }

  /**
   * Average State.
   */
  protected static class AvgState implements AggregationState {
    private int count;
    private double total;

    AvgState() {
      this.count = 0;
      this.total = 0d;
    }

    @Override
    public ExprValue result() {
      return count == 0 ? ExprNullValue.of() : ExprValueUtils.doubleValue(total / count);
    }
  }
}
