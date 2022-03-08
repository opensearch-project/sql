/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * Rounding.
 */
@EqualsAndHashCode
public abstract class Rounding<T> {
  @Getter
  protected T maxRounded;
  @Getter
  protected T minRounded;

  /**
   * Create Rounding instance.
   */
  public static Rounding<?> createRounding(SpanExpression span) {
    ExprValue interval = span.getValue().valueOf(null);
    ExprType type = span.type();

    if (LONG.isCompatible(type)) {
      return new LongRounding(interval);
    }
    if (DOUBLE.isCompatible(type)) {
      return new DoubleRounding(interval);
    }
    if (type.equals(DATETIME)) {
      return new DatetimeRounding(interval, span.getUnit().getName());
    }
    if (type.equals(TIMESTAMP)) {
      return new TimestampRounding(interval, span.getUnit().getName());
    }
    if (type.equals(DATE)) {
      return new DateRounding(interval, span.getUnit().getName());
    }
    if (type.equals(TIME)) {
      return new TimeRounding(interval, span.getUnit().getName());
    }
    return new UnknownRounding();
  }

  public abstract ExprValue round(ExprValue value);

  public abstract Integer locate(ExprValue value);

  public abstract ExprValue[] createBuckets();


  static class TimestampRounding extends Rounding<Instant> {
    private final ExprValue interval;
    private final DateTimeUnit dateTimeUnit;

    public TimestampRounding(ExprValue interval, String unit) {
      this.interval = interval;
      this.dateTimeUnit = DateTimeUnit.resolve(unit);
    }

    @Override
    public ExprValue round(ExprValue var) {
      Instant instant = Instant.ofEpochMilli(dateTimeUnit.round(var.timestampValue()
          .toEpochMilli(), interval.integerValue()));
      updateRounded(instant);
      return new ExprTimestampValue(instant);
    }

    @Override
    public ExprValue[] createBuckets() {
      if (dateTimeUnit.isMillisBased) {
        int size = (int) ((maxRounded.toEpochMilli() - minRounded.toEpochMilli()) / (interval
            .integerValue() * dateTimeUnit.ratio)) + 1;
        return new ExprValue[size];
      } else {
        ZonedDateTime maxZonedDateTime = maxRounded.atZone(ZoneId.of("UTC"));
        ZonedDateTime minZonedDateTime = minRounded.atZone(ZoneId.of("UTC"));
        int monthDiff = (maxZonedDateTime.getYear() - minZonedDateTime
            .getYear()) * 12 + maxZonedDateTime.getMonthValue() - minZonedDateTime.getMonthValue();
        int size = monthDiff / ((int) dateTimeUnit.ratio * interval.integerValue()) + 1;
        return new ExprValue[size];
      }
    }

    @Override
    public Integer locate(ExprValue value) {
      if (dateTimeUnit.isMillisBased) {
        long intervalInEpochMillis = dateTimeUnit.ratio;
        return Long.valueOf((value.timestampValue()
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() - minRounded
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()) / (intervalInEpochMillis
            * interval.integerValue())).intValue();
      } else {
        int monthDiff = (value.dateValue().getYear() - minRounded.atZone(ZoneId.of("UTC"))
            .getYear()) * 12 + value.dateValue().getMonthValue() - minRounded
            .atZone(ZoneId.of("UTC")).getMonthValue();
        return (int) (monthDiff / (dateTimeUnit.ratio * interval.integerValue()));
      }
    }

    private void updateRounded(Instant value) {
      if (maxRounded == null || value.isAfter(maxRounded)) {
        maxRounded = value;
      }
      if (minRounded == null || value.isBefore(minRounded)) {
        minRounded = value;
      }
    }
  }


  static class DatetimeRounding extends Rounding<LocalDateTime> {
    private final ExprValue interval;
    private final DateTimeUnit dateTimeUnit;

    public DatetimeRounding(ExprValue interval, String unit) {
      this.interval = interval;
      this.dateTimeUnit = DateTimeUnit.resolve(unit);
    }

    @Override
    public ExprValue round(ExprValue var) {
      Instant instant = Instant.ofEpochMilli(dateTimeUnit.round(var.datetimeValue()
          .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(), interval.integerValue()));
      updateRounded(instant);
      return new ExprDatetimeValue(instant.atZone(ZoneId.of("UTC")).toLocalDateTime());
    }

    @Override
    public ExprValue[] createBuckets() {
      if (dateTimeUnit.isMillisBased) {
        int size = (int) ((maxRounded.atZone(ZoneId.of("UTC")).toInstant()
            .toEpochMilli() - minRounded.atZone(ZoneId.of("UTC")).toInstant()
            .toEpochMilli()) / (interval.integerValue() * dateTimeUnit.ratio)) + 1;
        return new ExprValue[size];
      } else {
        ZonedDateTime maxZonedDateTime = maxRounded.atZone(ZoneId.of("UTC"));
        ZonedDateTime minZonedDateTime = minRounded.atZone(ZoneId.of("UTC"));
        int monthDiff = (maxZonedDateTime.getYear() - minZonedDateTime
            .getYear()) * 12 + maxZonedDateTime.getMonthValue() - minZonedDateTime.getMonthValue();
        int size = monthDiff / ((int) dateTimeUnit.ratio * interval.integerValue()) + 1;
        return new ExprValue[size];
      }
    }

    @Override
    public Integer locate(ExprValue value) {
      if (dateTimeUnit.isMillisBased) {
        long intervalInEpochMillis = dateTimeUnit.ratio;
        return Long.valueOf((value.datetimeValue()
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() - minRounded
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()) / (intervalInEpochMillis
            * interval.integerValue())).intValue();
      } else {
        int monthDiff = (value.datetimeValue().getYear() - minRounded.getYear()) * 12
            + value.dateValue().getMonthValue() - minRounded.getMonthValue();
        return (int) (monthDiff / (dateTimeUnit.ratio * interval.integerValue()));
      }
    }

    private void updateRounded(Instant value) {
      if (maxRounded == null || value.isAfter(maxRounded
          .atZone(ZoneId.of("UTC")).toInstant())) {
        maxRounded = value.atZone(ZoneId.of("UTC")).toLocalDateTime();
      }
      if (minRounded == null || value.isBefore(minRounded
          .atZone(ZoneId.of("UTC")).toInstant())) {
        minRounded = value.atZone(ZoneId.of("UTC")).toLocalDateTime();
      }
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
      Instant instant = Instant.ofEpochMilli(dateTimeUnit.round(var.dateValue().atStartOfDay()
          .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(), interval.integerValue()));
      updateRounded(instant);
      return new ExprDateValue(instant.atZone(ZoneId.of("UTC")).toLocalDate());
    }

    @Override
    public ExprValue[] createBuckets() {
      if (dateTimeUnit.isMillisBased) {
        int size = (int) ((maxRounded.atStartOfDay().atZone(ZoneId.of("UTC")).toInstant()
            .toEpochMilli() - minRounded.atStartOfDay().atZone(ZoneId.of("UTC")).toInstant()
            .toEpochMilli()) / (interval.integerValue() * dateTimeUnit.ratio)) + 1;
        return new ExprValue[size];
      } else {
        ZonedDateTime maxZonedDateTime = maxRounded.atStartOfDay().atZone(ZoneId.of("UTC"));
        ZonedDateTime minZonedDateTime = minRounded.atStartOfDay().atZone(ZoneId.of("UTC"));
        int monthDiff = (maxZonedDateTime.getYear() - minZonedDateTime
            .getYear()) * 12 + maxZonedDateTime.getMonthValue() - minZonedDateTime.getMonthValue();
        int size = monthDiff / ((int) dateTimeUnit.ratio * interval.integerValue()) + 1;
        return new ExprValue[size];
      }
    }

    @Override
    public Integer locate(ExprValue value) {
      if (dateTimeUnit.isMillisBased) {
        long intervalInEpochMillis = dateTimeUnit.ratio;
        return Long.valueOf((value.dateValue().atStartOfDay()
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() - minRounded.atStartOfDay()
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()) / (intervalInEpochMillis
            * interval.integerValue())).intValue();
      } else {
        int monthDiff = (value.dateValue().getYear() - minRounded.getYear()) * 12
            + value.dateValue().getMonthValue() - minRounded.getMonthValue();
        return (int) (monthDiff / (dateTimeUnit.ratio * interval.integerValue()));
      }
    }

    private void updateRounded(Instant value) {
      if (maxRounded == null || value.isAfter(maxRounded.atStartOfDay()
          .atZone(ZoneId.of("UTC")).toInstant())) {
        maxRounded = value.atZone(ZoneId.of("UTC")).toLocalDate();
      }
      if (minRounded == null || value.isBefore(minRounded.atStartOfDay()
          .atZone(ZoneId.of("UTC")).toInstant())) {
        minRounded = value.atZone(ZoneId.of("UTC")).toLocalDate();
      }
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
        throw new ExpressionEvaluationException(String
            .format("Unable to set span unit %s for TIME type", dateTimeUnit.getName()));
      }

      Instant instant = Instant.ofEpochMilli(dateTimeUnit.round(var.timeValue().getLong(
          ChronoField.MILLI_OF_DAY), interval.integerValue()));
      updateRounded(instant);
      return new ExprTimeValue(instant.atZone(ZoneId.of("UTC")).toLocalTime());
    }

    @Override
    public ExprValue[] createBuckets() {
      // local time is converted to timestamp on 1970-01-01 for aggregations
      int size = (int) ((maxRounded.atDate(LocalDate.of(1970, 1, 1))
          .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() - minRounded
          .atDate(LocalDate.of(1970, 1, 1)).atZone(ZoneId.of("UTC")).toInstant()
          .toEpochMilli()) / (interval.integerValue() * dateTimeUnit.ratio)) + 1;
      return new ExprValue[size];
    }

    @Override
    public Integer locate(ExprValue value) {
      long intervalInEpochMillis = dateTimeUnit.ratio;
      return Long.valueOf((value.timeValue().atDate(LocalDate.of(1970, 1, 1))
          .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() - minRounded
          .atDate(LocalDate.of(1970, 1, 1))
          .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()) / (intervalInEpochMillis * interval
          .integerValue())).intValue();
    }

    private void updateRounded(Instant value) {
      if (maxRounded == null || value.isAfter(maxRounded.atDate(LocalDate.of(1970, 1, 1))
          .atZone(ZoneId.of("UTC")).toInstant())) {
        maxRounded = value.atZone(ZoneId.of("UTC")).toLocalTime();
      }
      if (minRounded == null) {
        minRounded = value.atZone(ZoneId.of("UTC")).toLocalTime();
      }
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
      updateRounded(rounded);
      return ExprValueUtils.longValue(rounded);
    }

    @Override
    public Integer locate(ExprValue value) {
      return Long.valueOf((value.longValue() - minRounded) / longInterval).intValue();
    }

    @Override
    public ExprValue[] createBuckets() {
      int size = Long.valueOf((maxRounded - minRounded) / longInterval).intValue() + 1;
      return new ExprValue[size];
    }

    private void updateRounded(Long value) {
      if (maxRounded == null || value > maxRounded) {
        maxRounded = value;
      }
      if (minRounded == null || value < minRounded) {
        minRounded = value;
      }
    }
  }


  static class DoubleRounding extends Rounding<Double> {
    private final Double doubleInterval;

    protected DoubleRounding(ExprValue interval) {
      doubleInterval = interval.doubleValue();
    }

    @Override
    public ExprValue round(ExprValue value) {
      double rounded = Double
          .valueOf(value.doubleValue() / doubleInterval).intValue() * doubleInterval;
      updateRounded(rounded);
      return ExprValueUtils.doubleValue(rounded);
    }

    @Override
    public Integer locate(ExprValue value) {
      return Double.valueOf((value.doubleValue() - minRounded) / doubleInterval).intValue();
    }

    @Override
    public ExprValue[] createBuckets() {
      int size = Double.valueOf((maxRounded - minRounded) / doubleInterval).intValue() + 1;
      return new ExprValue[size];
    }

    private void updateRounded(Double value) {
      if (maxRounded == null || value > maxRounded) {
        maxRounded = value;
      }
      if (minRounded == null || value < minRounded) {
        minRounded = value;
      }
    }
  }


  @RequiredArgsConstructor
  static class UnknownRounding extends Rounding<Object> {
    @Override
    public ExprValue round(ExprValue var) {
      return null;
    }

    @Override
    public Integer locate(ExprValue value) {
      return null;
    }

    @Override
    public ExprValue[] createBuckets() {
      return new ExprValue[0];
    }
  }


  @RequiredArgsConstructor
  public enum DateTimeUnit {
    MILLISECOND(1, "ms", true, ChronoField.MILLI_OF_SECOND
        .getBaseUnit().getDuration().toMillis()) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    SECOND(2, "s", true, ChronoField.SECOND_OF_MINUTE
        .getBaseUnit().getDuration().toMillis()) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    MINUTE(3, "m", true, ChronoField.MINUTE_OF_HOUR
        .getBaseUnit().getDuration().toMillis()) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    HOUR(4, "h", true, ChronoField.HOUR_OF_DAY
        .getBaseUnit().getDuration().toMillis()) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    DAY(5, "d", true, ChronoField.DAY_OF_MONTH
        .getBaseUnit().getDuration().toMillis()) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundFloor(utcMillis, ratio * interval);
      }
    },

    WEEK(6, "w", true, TimeUnit.DAYS.toMillis(7L)) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundWeek(utcMillis, interval);
      }
    },

    MONTH(7, "M", false, 1) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundMonth(utcMillis, interval);
      }
    },

    QUARTER(8, "q", false, 3) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundQuarter(utcMillis, interval);
      }
    },

    YEAR(9, "y", false, 12) {
      @Override
      long round(long utcMillis, int interval) {
        return DateTimeUtils.roundYear(utcMillis, interval);
      }
    };

    @Getter
    private final int id;
    @Getter
    private final String name;
    protected final boolean isMillisBased;
    protected final long ratio;

    abstract long round(long utcMillis, int interval);

    /**
     * Resolve the date time unit.
     */
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
