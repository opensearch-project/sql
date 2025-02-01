/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static java.time.temporal.ChronoUnit.MILLIS;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap.Builder;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;

/** Trendline command implementation */
@ToString
@EqualsAndHashCode(callSuper = false)
public class TrendlineOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final List<Pair<Trendline.TrendlineComputation, ExprCoreType>> computations;
  @EqualsAndHashCode.Exclude private final List<TrendlineAccumulator> accumulators;
  @EqualsAndHashCode.Exclude private final Map<String, Integer> fieldToIndexMap;
  @EqualsAndHashCode.Exclude private final HashSet<String> aliases;

  public TrendlineOperator(
      PhysicalPlan input, List<Pair<Trendline.TrendlineComputation, ExprCoreType>> computations) {
    this.input = input;
    this.computations = computations;
    this.accumulators = computations.stream().map(TrendlineOperator::createAccumulator).toList();
    fieldToIndexMap = new HashMap<>(computations.size());
    aliases = new HashSet<>(computations.size());
    for (int i = 0; i < computations.size(); ++i) {
      final Trendline.TrendlineComputation computation = computations.get(i).getKey();
      fieldToIndexMap.put(computation.getDataField().getChild().get(0).toString(), i);
      aliases.add(computation.getAlias());
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
      final String field = computations.get(i).getKey().getAlias();
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
        final ExprValue fieldValue = tupleValue.get(bindName);
        if (!fieldValue.isNull()) {
          accumulators.get(index).accumulate(fieldValue);
        }
      }
    }
    tupleValue.keySet().removeAll(aliases);
    return tupleValue;
  }

  private static TrendlineAccumulator createAccumulator(
      Pair<Trendline.TrendlineComputation, ExprCoreType> computation) {
      return switch (computation.getKey().getComputationType()) {
          case SMA -> new SimpleMovingAverageAccumulator(computation.getKey(), computation.getValue());
          case WMA -> new WeightedMovingAverageAccumulator(computation.getKey(), computation.getValue());
      };
  }

  /** Maintains stateful information for calculating the trendline. */
  private interface TrendlineAccumulator {
    void accumulate(ExprValue value);

    ExprValue calculate();

    static ArithmeticEvaluator getEvaluator(ExprCoreType type) {
      switch (type) {
        case DOUBLE:
          return NumericArithmeticEvaluator.INSTANCE;
        case DATE:
          return DateArithmeticEvaluator.INSTANCE;
        case TIME:
          return TimeArithmeticEvaluator.INSTANCE;
        case TIMESTAMP:
          return TimestampArithmeticEvaluator.INSTANCE;
      }
      throw new IllegalArgumentException(
          String.format("Invalid type %s used for moving average.", type.typeName()));
    }
  }

  private static class SimpleMovingAverageAccumulator implements TrendlineAccumulator {
    private final LiteralExpression dataPointsNeeded;
    private final EvictingQueue<ExprValue> receivedValues;
    private final ArithmeticEvaluator evaluator;
    private Expression runningTotal = null;

    public SimpleMovingAverageAccumulator(
        Trendline.TrendlineComputation computation, ExprCoreType type) {
      dataPointsNeeded = DSL.literal(computation.getNumberOfDataPoints().doubleValue());
      receivedValues = EvictingQueue.create(computation.getNumberOfDataPoints());
      evaluator = TrendlineAccumulator.getEvaluator(type);
    }

    @Override
    public void accumulate(ExprValue value) {
      if (dataPointsNeeded.valueOf().integerValue() == 1) {
        runningTotal = evaluator.calculateFirstTotal(Collections.singletonList(value));
        receivedValues.add(value);
        return;
      }

      final ExprValue valueToRemove;
      if (receivedValues.size() == dataPointsNeeded.valueOf().integerValue()) {
        valueToRemove = receivedValues.remove();
      } else {
        valueToRemove = null;
      }
      receivedValues.add(value);

      if (receivedValues.size() == dataPointsNeeded.valueOf().integerValue()) {
        if (runningTotal != null) {
          // We can use the previous calculation.
          // Subtract the evicted value and add the new value.
          // Refactored, that would be previous + (newValue - oldValue).
          runningTotal = evaluator.add(runningTotal, value, valueToRemove);
        } else {
          // This is the first average calculation so sum the entire receivedValues dataset.
          final List<ExprValue> data = receivedValues.stream().toList();
          runningTotal = evaluator.calculateFirstTotal(data);
        }
      }
    }

    @Override
    public ExprValue calculate() {
      if (receivedValues.size() < dataPointsNeeded.valueOf().integerValue()) {
        return null;
      } else if (dataPointsNeeded.valueOf().integerValue() == 1) {
        return receivedValues.peek();
      }
      return evaluator.evaluate(runningTotal, dataPointsNeeded);
    }
  }

  private static class WeightedMovingAverageAccumulator implements TrendlineAccumulator {
    private final LiteralExpression dataPointsNeeded;
    private final ArrayList<ExprValue> receivedValues;
    private final ExprCoreType type;


    public WeightedMovingAverageAccumulator(
            Trendline.TrendlineComputation computation, ExprCoreType type) {
      this.dataPointsNeeded = DSL.literal(computation.getNumberOfDataPoints().doubleValue());
      this.receivedValues = new ArrayList<>(computation.getNumberOfDataPoints()+1);
      this.type = type;
    }

    @Override
    public void accumulate(ExprValue value) {
      receivedValues.add(value);
      if (receivedValues.size() > dataPointsNeeded.valueOf().integerValue()) {
        receivedValues.removeFirst();
      }
    }

    @Override
    public ExprValue calculate() {
      if (receivedValues.size() < dataPointsNeeded.valueOf().integerValue()) {
        return null;
      } else if (dataPointsNeeded.valueOf().integerValue() == 1) {
        return receivedValues.getFirst();
      }
      return computeWma(receivedValues);
    }

    /**
     * Compute WMA values from provided dataset with in ascending order.
     * @param receivedValues the dataset for WMA calculation, sorted in ascending order.
     * @return ExprValue which represent he result onf WMA.
     */
    private ExprValue computeWma(ArrayList<ExprValue> receivedValues) {
      if (type == ExprCoreType.DOUBLE) {
        return new ExprDoubleValue(calculateWmaInDouble(receivedValues));

      } else if (type == ExprCoreType.DATE) {
        return ExprValueUtils.dateValue(
                ExprValueUtils.timestampValue(Instant.ofEpochMilli(
                        calculateWmaInLong(receivedValues))).dateValue());

      } else if ( type == ExprCoreType.TIME) {
        return ExprValueUtils.timeValue(
                LocalTime.MIN.plus(calculateWmaInLong(receivedValues), MILLIS));

      } else if (type == ExprCoreType.TIMESTAMP) {
        return ExprValueUtils.timestampValue(Instant.ofEpochMilli(
                calculateWmaInLong(receivedValues)));
      }
     return null;
    }

    private double calculateWmaInDouble (ArrayList<ExprValue> receivedValues) {
      double sum = 0D;
      for (int i=0 ; i<receivedValues.size() ; i++) {
        sum += receivedValues.get(i).doubleValue() / (i+1);
      }
      return sum / receivedValues.size();
    }

    private long calculateWmaInLong (ArrayList<ExprValue> receivedValues) {
      long sum = 0L;
      for (int i=0 ; i<receivedValues.size() ; i++) {
        sum += receivedValues.get(i).longValue() / (i+1);
      }
      return sum / receivedValues.size();
    }
  }



  private interface ArithmeticEvaluator {
    Expression calculateFirstTotal(List<ExprValue> dataPoints);

    Expression add(Expression runningTotal, ExprValue incomingValue, ExprValue evictedValue);

    ExprValue evaluate(Expression runningTotal, LiteralExpression numberOfDataPoints);
  }

  private static class NumericArithmeticEvaluator implements ArithmeticEvaluator {
    private static final NumericArithmeticEvaluator INSTANCE = new NumericArithmeticEvaluator();

    private NumericArithmeticEvaluator() {}

    @Override
    public Expression calculateFirstTotal(List<ExprValue> dataPoints) {
      Expression total = DSL.literal(0.0D);
      for (ExprValue dataPoint : dataPoints) {
        total = DSL.add(total, DSL.literal(dataPoint.doubleValue()));
      }
      return DSL.literal(total.valueOf().doubleValue());
    }

    @Override
    public Expression add(
        Expression runningTotal, ExprValue incomingValue, ExprValue evictedValue) {
      return DSL.literal(
          DSL.add(runningTotal, DSL.subtract(DSL.literal(incomingValue), DSL.literal(evictedValue)))
              .valueOf()
              .doubleValue());
    }

    @Override
    public ExprValue evaluate(Expression runningTotal, LiteralExpression numberOfDataPoints) {
      return DSL.divide(runningTotal, numberOfDataPoints).valueOf();
    }
  }

  private static class DateArithmeticEvaluator implements ArithmeticEvaluator {
    private static final DateArithmeticEvaluator INSTANCE = new DateArithmeticEvaluator();

    private DateArithmeticEvaluator() {}

    @Override
    public Expression calculateFirstTotal(List<ExprValue> dataPoints) {
      return TimestampArithmeticEvaluator.INSTANCE.calculateFirstTotal(dataPoints);
    }

    @Override
    public Expression add(
        Expression runningTotal, ExprValue incomingValue, ExprValue evictedValue) {
      return TimestampArithmeticEvaluator.INSTANCE.add(runningTotal, incomingValue, evictedValue);
    }

    @Override
    public ExprValue evaluate(Expression runningTotal, LiteralExpression numberOfDataPoints) {
      final ExprValue timestampResult =
          TimestampArithmeticEvaluator.INSTANCE.evaluate(runningTotal, numberOfDataPoints);
      return ExprValueUtils.dateValue(timestampResult.dateValue());
    }
  }

  private static class TimeArithmeticEvaluator implements ArithmeticEvaluator {
    private static final TimeArithmeticEvaluator INSTANCE = new TimeArithmeticEvaluator();

    private TimeArithmeticEvaluator() {}

    @Override
    public Expression calculateFirstTotal(List<ExprValue> dataPoints) {
      Expression total = DSL.literal(0);
      for (ExprValue dataPoint : dataPoints) {
        total = DSL.add(total, DSL.literal(MILLIS.between(LocalTime.MIN, dataPoint.timeValue())));
      }
      return DSL.literal(total.valueOf().longValue());
    }

    @Override
    public Expression add(
        Expression runningTotal, ExprValue incomingValue, ExprValue evictedValue) {
      return DSL.literal(
          DSL.add(
                  runningTotal,
                  DSL.subtract(
                      DSL.literal(MILLIS.between(LocalTime.MIN, incomingValue.timeValue())),
                      DSL.literal(MILLIS.between(LocalTime.MIN, evictedValue.timeValue()))))
              .valueOf());
    }

    @Override
    public ExprValue evaluate(Expression runningTotal, LiteralExpression numberOfDataPoints) {
      return ExprValueUtils.timeValue(
          LocalTime.MIN.plus(
              DSL.divide(runningTotal, numberOfDataPoints).valueOf().longValue(), MILLIS));
    }
  }

  private static class TimestampArithmeticEvaluator implements ArithmeticEvaluator {
    private static final TimestampArithmeticEvaluator INSTANCE = new TimestampArithmeticEvaluator();

    private TimestampArithmeticEvaluator() {}

    @Override
    public Expression calculateFirstTotal(List<ExprValue> dataPoints) {
      Expression total = DSL.literal(0);
      for (ExprValue dataPoint : dataPoints) {
        total = DSL.add(total, DSL.literal(dataPoint.timestampValue().toEpochMilli()));
      }
      return DSL.literal(total.valueOf().longValue());
    }

    @Override
    public Expression add(
        Expression runningTotal, ExprValue incomingValue, ExprValue evictedValue) {
      return DSL.literal(
          DSL.add(
                  runningTotal,
                  DSL.subtract(
                      DSL.literal(incomingValue.timestampValue().toEpochMilli()),
                      DSL.literal(evictedValue.timestampValue().toEpochMilli())))
              .valueOf());
    }

    @Override
    public ExprValue evaluate(Expression runningTotal, LiteralExpression numberOfDataPoints) {
      return ExprValueUtils.timestampValue(
          Instant.ofEpochMilli(DSL.divide(runningTotal, numberOfDataPoints).valueOf().longValue()));
    }
  }
}
