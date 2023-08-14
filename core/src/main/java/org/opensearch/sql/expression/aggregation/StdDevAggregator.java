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
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/** StandardDeviation Aggregator. */
public class StdDevAggregator extends Aggregator<StdDevAggregator.StdDevState> {

  private final boolean isSampleStdDev;

  /** Build Population Variance {@link VarianceAggregator}. */
  public static Aggregator stddevPopulation(List<Expression> arguments, ExprCoreType returnType) {
    return new StdDevAggregator(false, arguments, returnType);
  }

  /** Build Sample Variance {@link VarianceAggregator}. */
  public static Aggregator stddevSample(List<Expression> arguments, ExprCoreType returnType) {
    return new StdDevAggregator(true, arguments, returnType);
  }

  /**
   * VarianceAggregator constructor.
   *
   * @param isSampleStdDev true for sample standard deviation aggregator, false for population
   *     standard deviation aggregator.
   * @param arguments aggregator arguments.
   * @param returnType aggregator return types.
   */
  public StdDevAggregator(
      Boolean isSampleStdDev, List<Expression> arguments, ExprCoreType returnType) {
    super(
        isSampleStdDev
            ? BuiltinFunctionName.STDDEV_SAMP.getName()
            : BuiltinFunctionName.STDDEV_POP.getName(),
        arguments,
        returnType);
    this.isSampleStdDev = isSampleStdDev;
  }

  @Override
  public StdDevAggregator.StdDevState create() {
    return new StdDevAggregator.StdDevState(isSampleStdDev);
  }

  @Override
  protected StdDevAggregator.StdDevState iterate(
      ExprValue value, StdDevAggregator.StdDevState state) {
    state.evaluate(value);
    return state;
  }

  @Override
  public String toString() {
    return StringUtils.format(
        "%s(%s)", isSampleStdDev ? "stddev_samp" : "stddev_pop", format(getArguments()));
  }

  protected static class StdDevState implements AggregationState {

    private final StandardDeviation standardDeviation;

    private final List<Double> values = new ArrayList<>();

    public StdDevState(boolean isSampleStdDev) {
      this.standardDeviation = new StandardDeviation(isSampleStdDev);
    }

    public void evaluate(ExprValue value) {
      values.add(value.doubleValue());
    }

    @Override
    public ExprValue result() {
      return values.size() == 0
          ? ExprNullValue.of()
          : doubleValue(standardDeviation.evaluate(values.stream().mapToDouble(d -> d).toArray()));
    }
  }
}
