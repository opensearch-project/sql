/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class PercentileAggregator extends Aggregator<PercentileAggregator.PercentileState> {

  private final boolean isDiscrete;

  public static Aggregator percentile(List<Expression> arguments, ExprCoreType returnType) {
    return new PercentileAggregator(false, arguments, returnType);
  }

  public static Aggregator percentileCont(List<Expression> arguments, ExprCoreType returnType) {
    return new PercentileAggregator(false, arguments, returnType);
  }

  public static Aggregator percentileDisc(List<Expression> arguments, ExprCoreType returnType) {
    return new PercentileAggregator(true, arguments, returnType);
  }

  public PercentileAggregator(
      boolean isDiscrete, List<Expression> arguments, ExprCoreType returnType) {
    super(
        isDiscrete
            ? BuiltinFunctionName.PERCENTILE_DISC.getName()
            : BuiltinFunctionName.PERCENTILE_CONT.getName(),
        arguments,
        returnType);
    this.isDiscrete = isDiscrete;
  }

  @Override
  public PercentileState create() {
    return new PercentileState(isDiscrete, getArguments().get(1).valueOf().doubleValue());
  }

  @Override
  protected PercentileState iterate(ExprValue value, PercentileState state) {
    state.evaluate(value);
    return state;
  }

  @Override
  public String toString() {
    return StringUtils.format(
        "%s(%s)", isDiscrete ? "percentile_disc" : "percentile_cont", format(getArguments()));
  }

  protected static class PercentileState implements AggregationState {

    private final Percentile percentileCalculation;
    private final double p;
    private final List<Double> values = new ArrayList<>();

    PercentileState(boolean isDiscrete, double quantile) {
      if (quantile <= 0.0 || quantile > 100.0) {
        throw new IllegalArgumentException("out of bounds quantile value, must be in (0, 100]");
      }
      if (isDiscrete) {
        percentileCalculation =
            new Percentile(quantile).withEstimationType(Percentile.EstimationType.R_1);
      } else {
        percentileCalculation =
            new Percentile(quantile).withEstimationType(Percentile.EstimationType.R_7);
      }
      this.p = quantile;
    }

    public void evaluate(ExprValue value) {
      values.add(value.doubleValue());
    }

    @Override
    public ExprValue result() {
      return values.isEmpty()
          ? ExprNullValue.of()
          : doubleValue(
              percentileCalculation.evaluate(
                  values.stream().mapToDouble(d -> d).toArray(), this.p));
    }
  }
}
