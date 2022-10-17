/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class TakeAggregator extends Aggregator<TakeAggregator.TakeState> {

  public TakeAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.TAKE.getName(), arguments, returnType);
  }

  @Override
  public TakeState create() {
    return new TakeState();
  }

  @Override
  protected TakeState iterate(ExprValue value, TakeState state) {
    state.take(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "take(%s)", format(getArguments()));
  }

  /**
   * Count State.
   */
  protected static class TakeState implements AggregationState {
    protected int count;

    TakeState() {
      this.count = 0;
    }

    public void take(ExprValue value) {
      count++;
    }

    @Override
    public ExprValue result() {
      return ExprValueUtils.integerValue(count);
    }
  }

  protected static class DistinctTakeState extends TakeState {
    private final Set<ExprValue> distinctValues = new HashSet<>();

    @Override
    public void take(ExprValue value) {
      if (!distinctValues.contains(value)) {
        distinctValues.add(value);
        count++;
      }
    }
  }
}
