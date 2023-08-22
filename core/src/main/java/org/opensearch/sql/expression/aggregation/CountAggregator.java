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
import org.opensearch.sql.expression.aggregation.CountAggregator.CountState;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class CountAggregator extends Aggregator<CountState> {

  public CountAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.COUNT.getName(), arguments, returnType);
  }

  @Override
  public CountAggregator.CountState create() {
    return distinct ? new DistinctCountState() : new CountState();
  }

  @Override
  protected CountState iterate(ExprValue value, CountState state) {
    state.count(value);
    return state;
  }

  @Override
  public String toString() {
    return distinct
        ? String.format(Locale.ROOT, "count(distinct %s)", format(getArguments()))
        : String.format(Locale.ROOT, "count(%s)", format(getArguments()));
  }

  /** Count State. */
  protected static class CountState implements AggregationState {
    protected int count;

    CountState() {
      this.count = 0;
    }

    public void count(ExprValue value) {
      count++;
    }

    @Override
    public ExprValue result() {
      return ExprValueUtils.integerValue(count);
    }
  }

  protected static class DistinctCountState extends CountState {
    private final Set<ExprValue> distinctValues = new HashSet<>();

    @Override
    public void count(ExprValue value) {
      if (!distinctValues.contains(value)) {
        distinctValues.add(value);
        count++;
      }
    }
  }
}
