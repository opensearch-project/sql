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

/** Aggregator to calculate approximate percentile. */
public class PercentileApproximateAggregator
    extends Aggregator<PercentileApproximateAggregator.PercentileApproximateState> {

  public static Aggregator percentileApprox(List<Expression> arguments, ExprCoreType returnType) {
    return new PercentileApproximateAggregator(arguments, returnType);
  }

  public PercentileApproximateAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.PERCENTILE_APPROX.getName(), arguments, returnType);
    if (!ExprCoreType.numberTypes().contains(returnType)) {
      throw new IllegalArgumentException(
          String.format("percentile aggregation over %s type is not supported", returnType));
    }
  }

  @Override
  public PercentileApproximateState create() {
    if (getArguments().size() == 2) {
      return new PercentileApproximateState(getArguments().get(1).valueOf().doubleValue());
    } else {
      return new PercentileApproximateState(
          getArguments().get(1).valueOf().doubleValue(),
          getArguments().get(2).valueOf().doubleValue());
    }
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

  /**
   * PercentileApproximateState is used to store the AVLTreeDigest state for percentile estimation.
   */
  protected static class PercentileApproximateState extends AVLTreeDigest
      implements AggregationState {
    // The compression level for the AVLTreeDigest, keep the same default value as OpenSearch core.
    public static final double DEFAULT_COMPRESSION = 100.0;
    private final double quantileRatio;

    PercentileApproximateState(double quantile) {
      super(DEFAULT_COMPRESSION);
      if (quantile < 0.0 || quantile > 100.0) {
        throw new IllegalArgumentException("out of bounds quantile value, must be in [0, 100]");
      }
      this.quantileRatio = quantile / 100.0;
    }

    /**
     * Constructor for specifying both quantile and compression level.
     *
     * @param quantile the quantile to compute, must be in [0, 100]
     * @param compression the compression factor of the t-digest sketches used
     */
    PercentileApproximateState(double quantile, double compression) {
      super(compression);
      if (quantile < 0.0 || quantile > 100.0) {
        throw new IllegalArgumentException("out of bounds quantile value, must be in [0, 100]");
      }
      this.quantileRatio = quantile / 100.0;
    }

    public void evaluate(ExprValue value) {
      this.add(value.doubleValue());
    }

    @Override
    public ExprValue result() {
      return this.size() == 0 ? ExprNullValue.of() : doubleValue(this.quantile(quantileRatio));
    }
  }
}
