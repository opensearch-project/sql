/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import com.tdunning.math.stats.AVLTreeDigest;
import java.util.List;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class PercentileApproximateAggregator
    extends Aggregator<PercentileApproximateAggregator.PercentileApproximateState> {

  public static Aggregator percentile(List<Expression> arguments, ExprCoreType returnType) {
    return new PercentileApproximateAggregator(arguments, returnType);
  }

  public PercentileApproximateAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.PERCENTILE_APPROX.getName(), arguments, returnType);
  }

  @Override
  public PercentileApproximateState create() {
    return new PercentileApproximateState(getArguments().get(1).valueOf().doubleValue());
  }

  @Override
  protected PercentileApproximateState iterate(ExprValue value, PercentileApproximateState state) {
    state.evaluate(value);
    return state;
  }

  @Override
  public String toString() {
    return StringUtils.format("%s(%s)", "percentile", format(getArguments()));
  }

  protected static class PercentileApproximateState extends AVLTreeDigest
      implements AggregationState {

    private static final double DEFAULT_COMPRESSION = 100.0;
    private final double p;

    PercentileApproximateState(double quantile) {
      super(DEFAULT_COMPRESSION);
      if (quantile < 0.0 || quantile > 100.0) {
        throw new IllegalArgumentException("out of bounds quantile value, must be in (0, 100]");
      }
      this.p = quantile / 100.0;
    }

    public void evaluate(ExprValue value) {
      this.add(value.doubleValue());
    }

    @Override
    public ExprValue result() {
      return this.size() == 0 ? ExprNullValue.of() : doubleValue(this.quantile(p));
    }
  }
}
