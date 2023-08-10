/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/** Variance Aggregator. */
public class VarianceAggregator extends Aggregator<VarianceAggregator.VarianceState> {

  private final boolean isSampleVariance;

  /** Build Population Variance {@link VarianceAggregator}. */
  public static Aggregator variancePopulation(List<Expression> arguments, ExprCoreType returnType) {
    return new VarianceAggregator(false, arguments, returnType);
  }

  /** Build Sample Variance {@link VarianceAggregator}. */
  public static Aggregator varianceSample(List<Expression> arguments, ExprCoreType returnType) {
    return new VarianceAggregator(true, arguments, returnType);
  }

  /**
   * VarianceAggregator constructor.
   *
   * @param isSampleVariance true for sample variance aggregator, false for population variance
   *     aggregator.
   * @param arguments aggregator arguments.
   * @param returnType aggregator return types.
   */
  public VarianceAggregator(
      Boolean isSampleVariance, List<Expression> arguments, ExprCoreType returnType) {
    super(
        isSampleVariance
            ? BuiltinFunctionName.VARSAMP.getName()
            : BuiltinFunctionName.VARPOP.getName(),
        arguments,
        returnType);
    this.isSampleVariance = isSampleVariance;
  }

  @Override
  public VarianceState create() {
    return new VarianceState(isSampleVariance);
  }

  @Override
  protected VarianceState iterate(ExprValue value, VarianceState state) {
    state.evaluate(value);
    return state;
  }

  @Override
  public String toString() {
    return StringUtils.format(
        "%s(%s)", isSampleVariance ? "var_samp" : "var_pop", format(getArguments()));
  }

  protected static class VarianceState implements AggregationState {

    private final Variance variance;

    private final List<Double> values = new ArrayList<>();

    public VarianceState(boolean isSampleVariance) {
      this.variance = new Variance(isSampleVariance);
    }

    public void evaluate(ExprValue value) {
      values.add(value.doubleValue());
    }

    @Override
    public ExprValue result() {
      return values.size() == 0
          ? ExprNullValue.of()
          : doubleValue(variance.evaluate(values.stream().mapToDouble(d -> d).toArray()));
    }
  }
}
