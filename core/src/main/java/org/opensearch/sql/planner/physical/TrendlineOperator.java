/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap.Builder;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Trendline command implementation */
@ToString
@EqualsAndHashCode(callSuper = false)
public class TrendlineOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final List<Trendline.TrendlineComputation> computations;
  private final List<TrendlineAccumulator> accumulators;
  private final Map<String, Integer> fieldToIndexMap;
  private boolean hasAnotherRow = false;
  private boolean isTuple = false;

  public TrendlineOperator(PhysicalPlan input, List<Trendline.TrendlineComputation> computations) {
    this.input = input;
    this.computations = computations;
    this.accumulators = computations.stream()
        .map(TrendlineOperator::createAccumulator)
        .toList();
    fieldToIndexMap = new HashMap<>(computations.size());
    for (int i = 0; i < computations.size(); ++i) {

      fieldToIndexMap.put(computations.get(i).getDataField().getChild().getFirst().toString(), i);
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
    return hasAnotherRow;
  }

  @Override
  public ExecutionEngine.Schema schema() {
    // TODO: Don't hardcode the type.
    return new ExecutionEngine.Schema(
        computations.stream()
            .map(
                computation ->
                    new ExecutionEngine.Schema.Column(computation.getDataField().getChild().getFirst().toString(),
                        computation.getAlias(), DOUBLE))
            .collect(Collectors.toList()));
  }

  @Override
  public ExprValue next() {
    Preconditions.checkState(hasAnotherRow);
    final ExprValue result;
    if (isTuple) {
      Builder<String, ExprValue> mapBuilder = new Builder<>();
      for (int i = 0; i < accumulators.size(); ++i) {
        final ExprValue calculateResult = accumulators.get(i).calculate();
        if (calculateResult == null) {
          continue;
        }

        if (null != computations.get(i).getAlias()) {
          mapBuilder.put(computations.get(i).getAlias(), calculateResult);
        } else {
          mapBuilder.put(computations.get(i).getDataField().toString(), calculateResult);
        }
      }
      result = ExprTupleValue.fromExprValueMap(mapBuilder.build());
    } else {
      result = accumulators.getFirst().calculate();
    }

    if (input.hasNext()) {
      final ExprValue next = input.next();
      consumeInputTuple(next);
    } else {
      hasAnotherRow = false;
    }
    return result;
  }

  @Override
  public void open() {
    super.open();

    // Position the cursor such that enough data points have been accumulated
    // to get one trendline calculation.
    final int smallestNumberOfDataPoints = computations.stream()
        .mapToInt(Trendline.TrendlineComputation::getNumberOfDataPoints)
        .min().orElseThrow(() -> new SyntaxCheckException("Period not supplied."));

    int i;
    for (i = 0; i < smallestNumberOfDataPoints && input.hasNext(); ++i) {
      final ExprValue next = input.next();
      if (next.type() == STRUCT) {
        isTuple = true;
      }
      consumeInputTuple(next);
    }

    if (i == smallestNumberOfDataPoints) {
      hasAnotherRow = true;
    }
  }

  private void consumeInputTuple(ExprValue inputValue) {
    if (isTuple) {
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (String bindName : tupleValue.keySet()) {
        final Integer index = fieldToIndexMap.get(bindName);
        if (index == null) {
          continue;
        }
        accumulators.get(index).accumulate(tupleValue.get(bindName));
      }
    } else {
      accumulators.getFirst().accumulate(inputValue);
    }
  }

  private static TrendlineAccumulator createAccumulator(Trendline.TrendlineComputation computation) {
    switch (computation.getComputationType()) {
      case SMA:
        return new SimpleMovingAverageAccumulator(computation);
      case WMA:
      default:
        throw new IllegalStateException("Unexpected value: " + computation.getComputationType());
    }
  }

  /**
   * Maintains stateful information for calculating the trendline.
   */
  private interface TrendlineAccumulator {
    void accumulate(ExprValue value);
    ExprValue calculate();
  }

  private static class SimpleMovingAverageAccumulator implements TrendlineAccumulator {
    private final ExprValue dataPointsNeeded;
    private final EvictingQueue<ExprValue> receivedValues;
    private ExprValue runningAverage = new ExprDoubleValue(0.0);

    public SimpleMovingAverageAccumulator(Trendline.TrendlineComputation computation) {
      dataPointsNeeded = new ExprIntegerValue(computation.getNumberOfDataPoints());
      receivedValues = EvictingQueue.create(computation.getNumberOfDataPoints());
    }

    @Override
    public void accumulate(ExprValue value) {
      receivedValues.add(value);
    }

    @Override
    public ExprValue calculate() {
      // TODO: Calculate this properly using the DSL and optimize it to use
      // a running average instead of iterating over the whole window.
      if (receivedValues.size() < dataPointsNeeded.integerValue()) {
        return ExprNullValue.of();
      }
      ExprValue[] entries = new ExprValue[0];
      ExprValue[] data = receivedValues.toArray(entries);
      double result = 0;
      for (int i = 0; i < data.length; i++) {
        result += data[i].doubleValue();
      }

      result /= receivedValues.size();
      return new ExprDoubleValue(result);
    }
  }
}
