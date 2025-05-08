/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Objects;
import org.apache.calcite.avatica.util.TimeUnit;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

public final class DateTimeApplyUtils {
  private DateTimeApplyUtils() {}

  public static ExprValue transferInputToExprTimestampValue(
      Object candidate, ExprType typeName, FunctionProperties properties) {
    if (Objects.requireNonNull(typeName) == ExprCoreType.TIME) {
      ExprTimeValue timeValue = (ExprTimeValue) fromObjectValue(candidate, typeName);
      return new ExprTimestampValue(timeValue.timestampValue(properties));
    }
    try {
      return new ExprTimestampValue(fromObjectValue(candidate, typeName).timestampValue());
    } catch (SemanticCheckException e) {
      // If the candidate is a String and does not contain a colon, it means
      // it ought to be a date but in a malformed format. We rethrow the exception
      // in this case
      if (candidate instanceof String candidateStr) {
        if (!candidateStr.contains(":")) {
          throw e;
        }
      }
      ExprTimeValue hardTransferredTimeValue =
          (ExprTimeValue) fromObjectValue(candidate, ExprCoreType.TIME);
      return new ExprTimestampValue(hardTransferredTimeValue.timestampValue(properties));
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
