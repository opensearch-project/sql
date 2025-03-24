/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.utils.DateTimeUtils;

/** Rounding. */
@EqualsAndHashCode
public abstract class Rounding<T> {

  /** Create Rounding instance. */
  public static Rounding<?> createRounding(SpanExpression span) {
    ExprValue interval = span.getValue().valueOf();
    ExprType type = span.type();

    if (LONG.isCompatible(type)) {
      return new LongRounding(interval);
    }
    if (DOUBLE.isCompatible(type)) {
      return new DoubleRounding(interval);
    }
    if (type.equals(TIMESTAMP) || type.typeName().equalsIgnoreCase(TIMESTAMP.typeName())) {
      return new TimestampRounding(interval, span.getUnit().getName());
    }
    if (type.equals(DATE) || type.typeName().equalsIgnoreCase(DATE.typeName())) {
      return new DateRounding(interval, span.getUnit().getName());
    }
    if (type.equals(TIME) || type.typeName().equalsIgnoreCase(TIME.typeName())) {
      return new TimeRounding(interval, span.getUnit().getName());
    }
    return new UnknownRounding();
  }

  public abstract ExprValue round(ExprValue value);

  static class TimestampRounding extends Rounding<Instant> {
    private final ExprValue interval;
    private final DateTimeUnit dateTimeUnit;

    public TimestampRounding(ExprValue interval, String unit) {
      this.interval = interval;
      this.dateTimeUnit = DateTimeUnit.resolve(unit);
    }

    @Override
    public ExprValue round(ExprValue var) {
      Instant instant =
          Instant.ofEpochMilli(
              dateTimeUnit.round(var.timestampValue().toEpochMilli(), interval.integerValue()));
      return new ExprTimestampValue(instant);
    }
  }

  static class DateRounding extends Rounding<LocalDate> {
    private final ExprValue interval;
    private final DateTimeUnit dateTimeUnit;

    public DateRounding(ExprValue interval, String unit) {
      this.interval = interval;
      this.dateTimeUnit = DateTimeUnit.resolve(unit);
    }

    @Override
    public ExprValue round(ExprValue var) {
      Instant instant =
          Instant.ofEpochMilli(
              dateTimeUnit.round(
                  var.dateValue().atStartOfDay().atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
                  interval.integerValue()));
      return new ExprDateValue(instant.atZone(ZoneOffset.UTC).toLocalDate());
    }
  }

  static class TimeRounding extends Rounding<LocalTime> {
    private final ExprValue interval;
    private final DateTimeUnit dateTimeUnit;

    public TimeRounding(ExprValue interval, String unit) {
      this.interval = interval;
      this.dateTimeUnit = DateTimeUnit.resolve(unit);
    }

    @Override
    public ExprValue round(ExprValue var) {
      if (dateTimeUnit.id > 4) {
        throw new ExpressionEvaluationException(
            String.format("Unable to set span unit %s for TIME type", dateTimeUnit.getName()));
      }

      Instant instant =
          Instant.ofEpochMilli(
              dateTimeUnit.round(
                  var.timeValue().getLong(ChronoField.MILLI_OF_DAY), interval.integerValue()));
      return new ExprTimeValue(instant.atZone(ZoneOffset.UTC).toLocalTime());
    }
  }

  static class LongRounding extends Rounding<Long> {
    private final Long longInterval;

    protected LongRounding(ExprValue interval) {
      longInterval = interval.longValue();
    }

    @Override
    public ExprValue round(ExprValue value) {
      long rounded = Math.floorDiv(value.longValue(), longInterval) * longInterval;
      return ExprValueUtils.longValue(rounded);
    }
  }

  static class DoubleRounding extends Rounding<Double> {
    private final Double doubleInterval;

    protected DoubleRounding(ExprValue interval) {
      doubleInterval = interval.doubleValue();
    }

    @Override
    public ExprValue round(ExprValue value) {
      double rounded =
          Double.valueOf(value.doubleValue() / doubleInterval).intValue() * doubleInterval;
      return ExprValueUtils.doubleValue(rounded);
    }
  }

  @RequiredArgsConstructor
  static class UnknownRounding extends Rounding<Object> {
    @Override
    public ExprValue round(ExprValue var) {
      return null;
    }
  }

  @RequiredArgsConstructor
  public enum DateTimeUnit {
    MILLISECOND(1, "ms", true, ChronoField.MILLI_OF_SECOND.getBaseUnit().getDuration().toMillis()) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    SECOND(2, "s", true, ChronoField.SECOND_OF_MINUTE.getBaseUnit().getDuration().toMillis()) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    MINUTE(3, "m", true, ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis()) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    HOUR(4, "h", true, ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis()) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    DAY(5, "d", true, ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis()) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    WEEK(6, "w", true, TimeUnit.DAYS.toMillis(7L)) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundWeek(utcMillis, interval);
      }
    },

    MONTH(7, "M", false, 1) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundMonth(utcMillis, interval);
      }
    },

    QUARTER(8, "q", false, 3) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundQuarter(utcMillis, interval);
      }
    },

    YEAR(9, "y", false, 12) {
      @Override
      public long round(long utcMillis, int interval) {
        return DateTimeUtils.roundYear(utcMillis, interval);
      }
    };

    @Getter private final int id;
    @Getter private final String name;
    protected final boolean isMillisBased;
    protected final long ratio;

    public abstract long round(long utcMillis, int interval);

    /** Resolve the date time unit. */
    public static Rounding.DateTimeUnit resolve(String name) {
      switch (name) {
        case "M":
          return MONTH;
        case "m":
          return MINUTE;
        default:
          return Arrays.stream(values())
              .filter(v -> v.getName().equalsIgnoreCase(name))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Unable to resolve unit " + name));
      }
    }
  }
}
