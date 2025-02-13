/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap.Builder;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;
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
      case WMA -> new WeightedMovingAverageAccumulator(
          computation.getKey(), computation.getValue());
    };
  }

  /** Maintains stateful information for calculating the trendline. */
  protected abstract static class TrendlineAccumulator {

    protected final LiteralExpression dataPointsNeeded;

    protected final Queue<ExprValue> receivedValues;

    private TrendlineAccumulator(Trendline.TrendlineComputation config) {
      Integer numberOfDataPoints = config.getNumberOfDataPoints();
      if (numberOfDataPoints <= 0) {
        throw new SemanticCheckException(
            String.format("Invalid dataPoints [%d] value.", numberOfDataPoints));
      }
      this.dataPointsNeeded = DSL.literal(numberOfDataPoints);
      this.receivedValues = EvictingQueue.create(numberOfDataPoints);
    }

    abstract void accumulate(ExprValue value);

    abstract ExprValue calculate();
  }

  private static class SimpleMovingAverageAccumulator extends TrendlineAccumulator {
    private final ArithmeticEvaluator evaluator;
    private Expression runningTotal = null;

    public SimpleMovingAverageAccumulator(
        Trendline.TrendlineComputation computation, ExprCoreType type) {
      super(computation);
      evaluator = getEvaluator(type);
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

    static ArithmeticEvaluator getEvaluator(ExprCoreType type) {
      return switch (type) {
        case INTEGER, SHORT, LONG, FLOAT, DOUBLE -> NumericArithmeticEvaluator.INSTANCE;
        case DATE -> DateArithmeticEvaluator.INSTANCE;
        case TIME -> TimeArithmeticEvaluator.INSTANCE;
        case TIMESTAMP -> TimestampArithmeticEvaluator.INSTANCE;
        default -> throw new SemanticCheckException(
            String.format("Invalid type %s used for moving average.", type.typeName()));
      };
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
            DSL.add(
                    runningTotal,
                    DSL.subtract(DSL.literal(incomingValue), DSL.literal(evictedValue)))
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
      private static final TimestampArithmeticEvaluator INSTANCE =
          new TimestampArithmeticEvaluator();

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
            Instant.ofEpochMilli(
                DSL.divide(runningTotal, numberOfDataPoints).valueOf().longValue()));
      }
    }
  }

  private static class WeightedMovingAverageAccumulator extends TrendlineAccumulator {

    private final Function<Queue<ExprValue>, ExprValue> evaluator;
    private final List<Double> weights;

    public WeightedMovingAverageAccumulator(
        Trendline.TrendlineComputation computation, ExprCoreType type) {
      super(computation);
      int dataPoints = computation.getNumberOfDataPoints();
      this.evaluator = getWmaEvaluator(type);
      this.weights =
          IntStream.rangeClosed(1, dataPoints)
              .mapToObj(i -> i / ((dataPoints * (dataPoints + 1)) / 2d))
              .collect(toList());
    }

    Function<Queue<ExprValue>, ExprValue> getWmaEvaluator(ExprCoreType type) {
      return switch (type) {
        case INTEGER, SHORT, LONG, FLOAT, DOUBLE -> WMA_NUMERIC_EVALUATOR;
        case DATE, TIMESTAMP -> WMA_TIMESTAMP_EVALUATOR;
        case TIME -> WMA_TIME_EVALUATOR;
        default -> throw new SemanticCheckException(
            String.format("Invalid type %s used for weighted moving average.", type.typeName()));
      };
    }

    @Override
    public void accumulate(ExprValue value) {
      receivedValues.add(value);
      if (receivedValues.size() > dataPointsNeeded.valueOf().integerValue()) {
        receivedValues.remove();
      }
    }

    @Override
    public ExprValue calculate() {
      if (receivedValues.size() < dataPointsNeeded.valueOf().integerValue()) {
        return null;
      } else if (dataPointsNeeded.valueOf().integerValue() == 1) {
        return receivedValues.peek();
      }
      return evaluator.apply(receivedValues);
    }

    public final Function<Queue<ExprValue>, ExprValue> WMA_NUMERIC_EVALUATOR =
        (receivedValues) ->
            new ExprDoubleValue(calculateWmaInDouble(receivedValues, ExprValue::doubleValue));
    ;

    public final Function<Queue<ExprValue>, ExprValue> WMA_TIMESTAMP_EVALUATOR =
        (receivedValues) -> {
          Long wmaResult =
              Math.round(
                  calculateWmaInDouble(
                      receivedValues, i -> (double) (i.timestampValue().toEpochMilli())));
          return ExprValueUtils.timestampValue(Instant.ofEpochMilli((wmaResult)));
        };

    public final Function<Queue<ExprValue>, ExprValue> WMA_TIME_EVALUATOR =
        (receivedValues) -> {
          Long wmaResult =
              Math.round(
                  calculateWmaInDouble(
                      receivedValues,
                      i -> (double) (MILLIS.between(LocalTime.MIN, i.timeValue()))));
          return ExprValueUtils.timeValue(LocalTime.MIN.plus(wmaResult, MILLIS));
        };

    /**
     * Responsible to iterate the internal buffer, perform necessary calculation, and the up-to-date
     * wma result in Double
     *
     * @param receivedValues internal buffer which stores all value in range.
     * @param exprToDouble transformation function to convert incoming values to double for
     *     calcaution.
     * @return wma result in Double form.
     */
    private Double calculateWmaInDouble(
        Queue<ExprValue> receivedValues, Function<ExprValue, Double> exprToDouble) {
      double sum = 0D;
      Iterator<Double> weightIter = weights.iterator();
      for (ExprValue next : receivedValues) {
        sum += exprToDouble.apply(next) * (weightIter.next());
      }
      return sum;
    }
  }
}
