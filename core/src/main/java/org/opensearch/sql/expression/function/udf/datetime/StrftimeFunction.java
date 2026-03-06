/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.convertToExprValues;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.*;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.StrftimeFormatterUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation of the STRFTIME function. This function takes a UNIX timestamp (in seconds) and
 * formats it according to POSIX-style format specifiers.
 *
 * <p>All timestamps are interpreted as UTC and all text formatting uses Locale.ROOT for consistent,
 * language-neutral output regardless of server locale settings.
 */
public class StrftimeFunction extends ImplementorUDF {

  // Maximum valid UNIX timestamp per MySQL documentation
  // On 64-bit platforms, effective maximum is 32536771199.999999 (3001-01-18 23:59:59.999999 UTC)
  private static final long MAX_UNIX_TIMESTAMP = 32536771199L;
  private static final int NANOS_PER_SECOND = 1_000_000_000;

  public StrftimeFunction() {
    super(new StrftimeImplementor(), NullPolicy.STRICT);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // Accepts (NUMERIC|TIMESTAMP, STRING) -> STRING
    // Note: STRING is NOT accepted for first parameter - use unix_timestamp() to convert
    // Calcite will auto-cast DATE and TIME to TIMESTAMP
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)
                .or(OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)));
  }

  public static class StrftimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      return Expressions.call(StrftimeFunction.class, "strftime", operands);
    }
  }

  /**
   * Static method for Calcite implementation.
   *
   * @param unixTime The UNIX timestamp to format
   * @param formatString The format string with POSIX-style specifiers
   * @return Formatted date string or null if invalid input
   */
  public static String strftime(ExprValue unixTime, ExprValue formatString) {
    if (unixTime.isNull() || formatString.isNull()) {
      return null;
    }
    Double unixSecondsDouble = extractUnixSecondsWithFraction(unixTime);
    // Combine null checks and validation
    if (unixSecondsDouble == null || !isValidTimestamp(unixSecondsDouble.longValue())) {
      return null;
    }
    ZonedDateTime dateTime = convertToZonedDateTimeWithFraction(unixSecondsDouble);
    return formatDateTime(dateTime, formatString.stringValue());
  }

  /**
   * Extract UNIX seconds with fractional part from various input types.
   *
   * @param unixTime The input value
   * @return UNIX seconds with fraction or null if cannot extract
   */
  private static Double extractUnixSecondsWithFraction(ExprValue unixTime) {
    // 1. Handle TIMESTAMP types (from now(), from_unixtime(), timestamp(), etc.)
    if (unixTime instanceof ExprTimestampValue timestamp) {
      Instant instant = timestamp.timestampValue();
      return instant.getEpochSecond() + instant.getNano() / (double) NANOS_PER_SECOND;
    }

    // 2. Handle DATE types (convert to timestamp at midnight UTC)
    if (unixTime instanceof ExprDateValue date) {
      // DATE converts to timestamp at midnight UTC
      Instant instant = date.timestampValue();
      return instant.getEpochSecond() + instant.getNano() / (double) NANOS_PER_SECOND;
    }

    // 3. Handle numeric types (UNIX timestamps in seconds or milliseconds)
    Double numericValue = extractNumericValue(unixTime);
    if (numericValue == null) {
      return null; // Not a numeric type (could be string, etc.)
    }

    // Auto-detect if value is in milliseconds (>= 100 billion)
    double absValue = Math.abs(numericValue);
    if (absValue >= 1e11) { // >= 100 billion, likely milliseconds
      return numericValue / 1000.0;
    } else {
      return numericValue;
    }
  }

  /**
   * Extract numeric value from various numeric ExprValue types.
   *
   * @param value The input ExprValue
   * @return Double value or null if not a numeric type
   */
  private static Double extractNumericValue(ExprValue value) {
    if (value instanceof ExprDoubleValue) {
      return value.doubleValue();
    } else if (value instanceof ExprLongValue) {
      return (double) value.longValue();
    } else if (value instanceof ExprIntegerValue) {
      return (double) value.integerValue();
    } else if (value instanceof ExprFloatValue) {
      return (double) value.floatValue();
    }

    // Not a numeric type
    return null;
  }

  /**
   * Check if the timestamp is within valid range. Accepts negative values for dates before 1970.
   *
   * @param unixSeconds The UNIX timestamp in seconds
   * @return true if valid, false otherwise
   */
  private static boolean isValidTimestamp(long unixSeconds) {
    return unixSeconds >= -MAX_UNIX_TIMESTAMP && unixSeconds <= MAX_UNIX_TIMESTAMP;
  }

  /**
   * Convert UNIX seconds with fractional part to ZonedDateTime in UTC.
   *
   * @param unixSeconds The UNIX timestamp in seconds with fractional seconds
   * @return ZonedDateTime in UTC
   */
  private static ZonedDateTime convertToZonedDateTimeWithFraction(double unixSeconds) {
    long seconds = (long) unixSeconds;
    int nanos = (int) ((unixSeconds - seconds) * NANOS_PER_SECOND);
    Instant instant = Instant.ofEpochSecond(seconds, nanos);
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
  }

  /**
   * Format the datetime using the format string.
   *
   * @param dateTime The datetime to format
   * @param format The format string
   * @return Formatted string or null if formatting fails
   */
  private static String formatDateTime(ZonedDateTime dateTime, String format) {
    ExprValue result = StrftimeFormatterUtil.formatZonedDateTime(dateTime, format);
    return result.stringValue();
  }
}
