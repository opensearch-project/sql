/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

public interface DateTimeApplyUtils {
  static Instant applyInterval(Instant base, Duration interval, boolean isAdd) {
    return isAdd ? base.plus(interval) : base.minus(interval);
  }

  static ExprValue transferInputToExprValue(Object candidate, SqlTypeName sqlTypeName) {
    return fromObjectValue(candidate, convertSqlTypeNameToExprType(sqlTypeName));
  }

  static ExprValue transferInputToExprTimestampValue(
      Object candidate, SqlTypeName sqlTypeName, FunctionProperties properties) {
    switch (sqlTypeName) {
      case TIME:
        ExprTimeValue timeValue =
            (ExprTimeValue) fromObjectValue(candidate, convertSqlTypeNameToExprType(sqlTypeName));
        return new ExprTimestampValue(timeValue.timestampValue(properties));
      default:
        try {
          return new ExprTimestampValue(
              fromObjectValue(candidate, convertSqlTypeNameToExprType(sqlTypeName))
                  .timestampValue());
        } catch (SemanticCheckException e) {
          ExprTimeValue hardTransferredTimeValue =
              (ExprTimeValue) fromObjectValue(candidate, ExprCoreType.TIME);
          return new ExprTimestampValue(hardTransferredTimeValue.timestampValue(properties));
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
  static TemporalAmount convertToTemporalAmount(long number, TimeUnit unit) {
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

  static ExprValue transferTimeToTimestamp(
      ExprValue candidate, FunctionProperties functionProperties) {
    return new ExprTimestampValue(((ExprTimeValue) candidate).timestampValue(functionProperties));
  }
}
