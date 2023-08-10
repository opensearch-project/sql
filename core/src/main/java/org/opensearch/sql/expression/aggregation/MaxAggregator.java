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

public class MaxAggregator extends Aggregator<MaxAggregator.MaxState> {

  public MaxAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.MAX.getName(), arguments, returnType);
  }

  @Override
  public MaxState create() {
    return new MaxState();
  }

  @Override
  protected MaxState iterate(ExprValue value, MaxState state) {
    state.max(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format("max(%s)", format(getArguments()));
  }

  protected static class MaxState implements AggregationState {
    private ExprValue maxResult;

    MaxState() {
      maxResult = LITERAL_NULL;
    }

    public void max(ExprValue value) {
      maxResult = maxResult.isNull() ? value : (maxResult.compareTo(value) > 0) ? maxResult : value;
    }

    @Override
    public ExprValue result() {
      return maxResult;
    }
  }
}
