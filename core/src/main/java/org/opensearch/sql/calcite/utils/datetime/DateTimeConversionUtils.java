/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import org.apache.calcite.avatica.util.TimeUnit;
import org.opensearch.sql.data.model.*;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

public final class DateTimeConversionUtils {
  private DateTimeConversionUtils() {}

  /**
   * Convert the given ExprValue to an ExprTimestampValue. If the input is a string, it will convert
   * date / time / timestamp strings to ExprTimestampValue.
   *
   * @param value the value to convert, can be either a ExprDateValue, ExprTimeValue,
   *     ExprTimestampValue or ExprStringValue
   * @param properties the function properties
   * @return the converted ExprTimestampValue
   */
  public static ExprTimestampValue forceConvertToTimestampValue(
      ExprValue value, FunctionProperties properties) {
    return switch (value) {
      case ExprTimestampValue timestampValue -> timestampValue;
      case ExprDateValue dateValue -> (ExprTimestampValue)
          ExprValueUtils.timestampValue(dateValue.timestampValue());
      case ExprTimeValue timeValue -> (ExprTimestampValue)
          ExprValueUtils.timestampValue(timeValue.timestampValue(properties));
      case ExprStringValue stringValue -> new ExprTimestampValue(
          DateTimeParser.parse(stringValue.stringValue()));
      default -> throw new SemanticCheckException(
          String.format(
              "Cannot convert %s to timestamp, only STRING, DATE, TIME and TIMESTAMP are supported",
              value.type()));
    };
  }

  /**
   * Convert the given ExprValue to an ExprTimestampValue. If the input is a string, it only accepts
   * a string formatted as a valid timestamp 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'.
   *
   * @param value the value to convert, can be either a ExprDateValue, ExprTimeValue,
   *     ExprTimestampValue or ExprStringValue
   * @param properties the function properties
   * @return the converted ExprTimestampValue
   */
  public static ExprTimestampValue convertToTimestampValue(
      ExprValue value, FunctionProperties properties) {
    if (value instanceof ExprTimestampValue timestampValue) {
      return timestampValue;
    } else if (value instanceof ExprTimeValue) {
      return new ExprTimestampValue(((ExprTimeValue) value).timestampValue(properties));
    } else if (value.type() == ExprCoreType.STRING) {
      return new ExprTimestampValue(value.stringValue());
    } else {
      try {
        return new ExprTimestampValue(value.timestampValue());
      } catch (SemanticCheckException e) {
        throw new SemanticCheckException(
            String.format(
                "Cannot convert %s to timestamp, only STRING, DATE, TIME and TIMESTAMP are"
                    + " supported",
                value.type()),
            e);
      }
    }
  }

  /**
   * Convert the given ExprValue to an ExprDateValue. If the input is a string, it only accepts a
   * string formatted as a valid date 'yyyy-MM-dd'.
   *
   * @param value the value to convert, can be either a ExprDateValue, ExprTimeValue,
   *     ExprTimestampValue or ExprStringValue
   * @param properties the function properties
   * @return the converted ExprDateValue
   */
  public static ExprDateValue convertToDateValue(ExprValue value, FunctionProperties properties) {
    switch (value) {
      case ExprDateValue dateValue -> {
        return dateValue;
      }
      case ExprTimeValue timeValue -> {
        return new ExprDateValue(timeValue.dateValue(properties));
      }
      case ExprTimestampValue timestampValue -> {
        return new ExprDateValue(timestampValue.dateValue());
      }
      case ExprStringValue ignored -> {
        return new ExprDateValue(value.stringValue());
      }
      default -> {
        throw new SemanticCheckException(
            String.format(
                "Cannot convert %s to date, only STRING, DATE, TIME and TIMESTAMP are supported",
                value.type()));
      }
    }
  }

  /**
   * Create a temporal amount of the given number of units. For duration below a day, it returns
   * duration; for duration including and above a day, it returns period for natural days, months,
   * quarters, and years, which may be of unfixed lengths.
   *
   * @param number The count of unit
   * @param unit The unit of the temporal amount
   * @return A temporal amount value, can be either a Period or a Duration
   */
  public static TemporalAmount convertToTemporalAmount(long number, TimeUnit unit) {
    return switch (unit) {
      case YEAR -> Period.ofYears((int) number);
      case QUARTER -> Period.ofMonths((int) number * 3);
      case MONTH -> Period.ofMonths((int) number);
      case WEEK -> Period.ofWeeks((int) number);
      case DAY -> Period.ofDays((int) number);
      case HOUR -> Duration.ofHours(number);
      case MINUTE -> Duration.ofMinutes(number);
      case SECOND -> Duration.ofSeconds(number);
      case MILLISECOND -> Duration.ofMillis(number);
      case MICROSECOND -> Duration.ofNanos(number * 1000);
      case NANOSECOND -> Duration.ofNanos(number);

      default -> throw new UnsupportedOperationException(
          "No mapping defined for Calcite TimeUnit: " + unit);
    };
  }
}
