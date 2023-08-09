/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The average aggregator aggregate the value evaluated by the expression. If the expression
 * evaluated result is NULL or MISSING, then the result is NULL.
 */
public class AvgAggregator extends Aggregator<AvgAggregator.AvgState> {

  /**
   * To process by different ways different data types, we need to store the type. Input data has
   * the same type as the result.
   */
  private final ExprCoreType dataType;

  public AvgAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.AVG.getName(), arguments, returnType);
    dataType = returnType;
  }

  @Override
  public AvgState create() {
    switch (dataType) {
      case DATE:
        return new DateAvgState();
      case DATETIME:
        return new DateTimeAvgState();
      case TIMESTAMP:
        return new TimestampAvgState();
      case TIME:
        return new TimeAvgState();
      case DOUBLE:
        return new DoubleAvgState();
      default: // unreachable code - we don't expose signatures for unsupported types
        throw new IllegalArgumentException(
            String.format("avg aggregation over %s type is not supported", dataType));
    }
  }

  @Override
  protected AvgState iterate(ExprValue value, AvgState state) {
    return state.iterate(value);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "avg(%s)", format(getArguments()));
  }

  /** Average State. */
  protected abstract static class AvgState implements AggregationState {
    protected ExprValue count;
    protected ExprValue total;

    AvgState() {
      this.count = new ExprIntegerValue(0);
      this.total = new ExprDoubleValue(0D);
    }

    @Override
    public abstract ExprValue result();

    protected AvgState iterate(ExprValue value) {
      count = DSL.add(DSL.literal(count), DSL.literal(1)).valueOf();
      return this;
    }
  }

  protected static class DoubleAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (0 == count.integerValue()) {
        return ExprNullValue.of();
      }
      return DSL.divide(DSL.literal(total), DSL.literal(count)).valueOf();
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total = DSL.add(DSL.literal(total), DSL.literal(value)).valueOf();
      return super.iterate(value);
    }
  }

  protected static class DateAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (0 == count.integerValue()) {
        return ExprNullValue.of();
      }

      return new ExprDateValue(
          new ExprTimestampValue(
                  Instant.ofEpochMilli(
                      DSL.divide(DSL.literal(total), DSL.literal(count)).valueOf().longValue()))
              .dateValue());
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total =
          DSL.add(DSL.literal(total), DSL.literal(value.timestampValue().toEpochMilli())).valueOf();
      return super.iterate(value);
    }
  }

  protected static class DateTimeAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (0 == count.integerValue()) {
        return ExprNullValue.of();
      }

      return new ExprDatetimeValue(
          new ExprTimestampValue(
                  Instant.ofEpochMilli(
                      DSL.divide(DSL.literal(total), DSL.literal(count)).valueOf().longValue()))
              .datetimeValue());
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total =
          DSL.add(DSL.literal(total), DSL.literal(value.timestampValue().toEpochMilli())).valueOf();
      return super.iterate(value);
    }
  }

  protected static class TimestampAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (0 == count.integerValue()) {
        return ExprNullValue.of();
      }

      return new ExprTimestampValue(
          Instant.ofEpochMilli(
              DSL.divide(DSL.literal(total), DSL.literal(count)).valueOf().longValue()));
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total =
          DSL.add(DSL.literal(total), DSL.literal(value.timestampValue().toEpochMilli())).valueOf();
      return super.iterate(value);
    }
  }

  protected static class TimeAvgState extends AvgState {
    @Override
    public ExprValue result() {
      if (0 == count.integerValue()) {
        return ExprNullValue.of();
      }

      return new ExprTimeValue(
          LocalTime.MIN.plus(
              DSL.divide(DSL.literal(total), DSL.literal(count)).valueOf().longValue(), MILLIS));
    }

    @Override
    protected AvgState iterate(ExprValue value) {
      total =
          DSL.add(DSL.literal(total), DSL.literal(MILLIS.between(LocalTime.MIN, value.timeValue())))
              .valueOf();
      return super.iterate(value);
    }
  }
}
