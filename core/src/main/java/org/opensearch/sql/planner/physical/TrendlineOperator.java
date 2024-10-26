/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

/** Trendline command implementation */
@ToString
@EqualsAndHashCode(callSuper = false)
public class TrendlineOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final List<Trendline.TrendlineComputation> computations;
  @EqualsAndHashCode.Exclude private final List<TrendlineAccumulator> accumulators;
  @EqualsAndHashCode.Exclude private final Map<String, Integer> fieldToIndexMap;
  @EqualsAndHashCode.Exclude private final HashSet<String> aliases;

  public TrendlineOperator(PhysicalPlan input, List<Trendline.TrendlineComputation> computations) {
    this.input = input;
    this.computations = computations;
    this.accumulators = computations.stream().map(TrendlineOperator::createAccumulator).toList();
    fieldToIndexMap = new HashMap<>(computations.size());
    aliases = new HashSet<>(computations.size());
    for (int i = 0; i < computations.size(); ++i) {
      final Trendline.TrendlineComputation computation = computations.get(i);
      fieldToIndexMap.put(computation.getDataField().getChild().get(0).toString(), i);
      if (computation.getAlias() != null) {
        aliases.add(computation.getAlias());
      }
    }
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTrendline(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return getChild().getFirst().hasNext();
  }

  @Override
  public ExprValue next() {
    final ExprValue result;
    final ExprValue next = input.next();
    final Map<String, ExprValue> inputStruct = consumeInputTuple(next);
    final Builder<String, ExprValue> mapBuilder = new Builder<>();
    mapBuilder.putAll(inputStruct);

    // Add calculated trendline values, which might overwrite existing fields from the input.
    for (int i = 0; i < accumulators.size(); ++i) {
      final ExprValue calculateResult = accumulators.get(i).calculate();
      final String field =
          null != computations.get(i).getAlias()
              ? computations.get(i).getAlias()
              : computations.get(i).getDataField().getChild().get(0).toString();
      if (calculateResult != null) {
        mapBuilder.put(field, calculateResult);
      }
    }

    result = ExprTupleValue.fromExprValueMap(mapBuilder.buildKeepingLast());
    return result;
  }

  private Map<String, ExprValue> consumeInputTuple(ExprValue inputValue) {
    final Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
    for (String bindName : tupleValue.keySet()) {
      final Integer index = fieldToIndexMap.get(bindName);
      if (index != null) {
        accumulators.get(index).accumulate(tupleValue.get(bindName));
      }
    }
    tupleValue.keySet().removeAll(aliases);
    return tupleValue;
  }

  private static TrendlineAccumulator createAccumulator(
      Trendline.TrendlineComputation computation) {
    switch (computation.getComputationType()) {
      case SMA:
        return new SimpleMovingAverageAccumulator(computation);
      case WMA:
      default:
        throw new IllegalStateException("Unexpected value: " + computation.getComputationType());
    }
  }

  /** Maintains stateful information for calculating the trendline. */
  private interface TrendlineAccumulator {
    void accumulate(ExprValue value);

    ExprValue calculate();
  }

  // TODO: Make the actual math polymorphic based on types to deal with datetimes.
  private static class SimpleMovingAverageAccumulator implements TrendlineAccumulator {
    private final ExprValue dataPointsNeeded;
    private final EvictingQueue<ExprValue> receivedValues;
    private ExprValue runningAverage = null;

    public SimpleMovingAverageAccumulator(Trendline.TrendlineComputation computation) {
      dataPointsNeeded = new ExprIntegerValue(computation.getNumberOfDataPoints());
      receivedValues = EvictingQueue.create(computation.getNumberOfDataPoints());
    }

    @Override
    public void accumulate(ExprValue value) {
      if (value == null) {
        // Should this make the whole calculation null?
        return;
      }

      if (dataPointsNeeded.integerValue() == 1) {
        runningAverage = value;
        receivedValues.add(value);
        return;
      }

      final ExprValue valueToRemove;
      if (receivedValues.size() == dataPointsNeeded.integerValue()) {
        valueToRemove = receivedValues.remove();
      } else {
        valueToRemove = null;
      }
      receivedValues.add(value);

      if (receivedValues.size() == dataPointsNeeded.integerValue()) {
        if (runningAverage != null) {
          // We can use the previous average calculation.
          // Subtract the evicted value / period and add the new value / period.
          // Refactored, that would be previous + (newValue - oldValue) / period
          runningAverage =
              DSL.add(
                      DSL.literal(runningAverage),
                      DSL.divide(
                          DSL.subtract(DSL.literal(value), DSL.literal(valueToRemove)),
                          DSL.literal(dataPointsNeeded.doubleValue())))
                  .valueOf();
        } else {
          // This is the first average calculation so sum the entire receivedValues dataset.
          final List<ExprValue> data = receivedValues.stream().toList();
          Expression runningTotal = DSL.literal(0.0D);
          for (ExprValue entry : data) {
            runningTotal = DSL.add(runningTotal, DSL.literal(entry));
          }
          runningAverage =
              DSL.divide(runningTotal, DSL.literal(dataPointsNeeded.doubleValue())).valueOf();
        }
      }
    }

    @Override
    public ExprValue calculate() {
      if (receivedValues.size() < dataPointsNeeded.integerValue()) {
        return null;
      }
      return runningAverage;
    }
  }

  private static class WeightedMovingAverageAccumulator implements TrendlineAccumulator {

    @Override
    public void accumulate(ExprValue value) {}

    @Override
    public ExprValue calculate() {
      return null;
    }
  }
}
