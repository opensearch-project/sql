/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.convertToExprValues;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.prependFunctionProperties;

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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.StrftimeFormatterUtil;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation of the STRFTIME function. This function takes a UNIX timestamp (in seconds) and
 * formats it according to POSIX-style format specifiers.
 *
 * <p>All timestamps are interpreted as UTC and all text formatting uses Locale.ROOT for consistent,
 * language-neutral output regardless of server locale settings.
 *
 * <p>Signatures: - (DOUBLE, STRING) -> STRING - (LONG, STRING) -> STRING - (TIMESTAMP, STRING) ->
 * STRING - (STRING, STRING) -> STRING
 */
public class StrftimeFunction extends ImplementorUDF {

  public StrftimeFunction() {
    super(new StrftimeImplementor(), NullPolicy.STRICT);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Accepts (NUMERIC|TIMESTAMP|STRING, STRING) -> STRING
    return UDFOperandMetadata.wrap(
        (org.apache.calcite.sql.type.CompositeOperandTypeChecker)
            org.apache.calcite.sql.type.OperandTypes.family(
                    org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                    org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER)
                .or(
                    org.apache.calcite.sql.type.OperandTypes.family(
                        org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP,
                        org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER))
                .or(org.apache.calcite.sql.type.OperandTypes.CHARACTER_CHARACTER));
  }

  public static class StrftimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      List<Expression> operandsWithProperties =
          prependFunctionProperties(operands, rexToLixTranslator);
      return Expressions.call(StrftimeFunction.class, "strftime", operandsWithProperties);
    }
  }

  /** Static method for Calcite implementation. */
  public static String strftime(
      FunctionProperties functionProperties, ExprValue unixTime, ExprValue formatString) {

    if (unixTime.isNull() || formatString.isNull()) {
      return null;
    }

    try {
      // Extract UNIX seconds from the timestamp
      long unixSeconds;
      double originalValue = 0;
      boolean hasOriginalValue = false;

      if (unixTime instanceof ExprDoubleValue) {
        double doubleValue = ((ExprDoubleValue) unixTime).doubleValue();
        originalValue = doubleValue;
        hasOriginalValue = true;
        unixSeconds = StrftimeFormatterUtil.extractUnixSeconds(doubleValue);
      } else if (unixTime instanceof ExprLongValue) {
        long longValue = ((ExprLongValue) unixTime).longValue();
        originalValue = longValue;
        hasOriginalValue = true;
        unixSeconds = StrftimeFormatterUtil.extractUnixSeconds((double) longValue);
      } else if (unixTime instanceof ExprStringValue) {
        String strValue = ((ExprStringValue) unixTime).stringValue();
        try {
          double doubleValue = Double.parseDouble(strValue);
          originalValue = doubleValue;
          hasOriginalValue = true;
          unixSeconds = StrftimeFormatterUtil.extractUnixSeconds(doubleValue);
        } catch (NumberFormatException e) {
          return null;
        }
      } else if (unixTime instanceof ExprTimestampValue) {
        // If it's already a timestamp, get epoch seconds
        ExprTimestampValue timestamp = (ExprTimestampValue) unixTime;
        unixSeconds = timestamp.timestampValue().getEpochSecond();
      } else {
        // Try to convert to double
        try {
          double doubleValue = Double.parseDouble(unixTime.toString());
          originalValue = doubleValue;
          hasOriginalValue = true;
          unixSeconds = StrftimeFormatterUtil.extractUnixSeconds(doubleValue);
        } catch (NumberFormatException e) {
          return null;
        }
      }

      // Check for valid range (same as FROM_UNIXTIME)
      // If we have an original numeric value that could be seconds (11 digits or less)
      if (hasOriginalValue) {
        // Check if the original value is out of range for seconds
        if (originalValue > 32536771199L && originalValue < 100000000000L) {
          // This is larger than max seconds but smaller than typical milliseconds
          // It's likely an out-of-range seconds value
          return null;
        }
      }

      // Check the extracted seconds
      if (unixSeconds < 0 || unixSeconds > 32536771199L) {
        return null;
      }

      // Convert to ZonedDateTime
      Instant instant = Instant.ofEpochSecond(unixSeconds);
      ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));

      // Format using the format string
      String format = formatString.stringValue();
      ExprValue result = StrftimeFormatterUtil.formatZonedDateTime(dateTime, format);
      return result.isNull() ? null : result.stringValue();

    } catch (Exception e) {
      // Return NULL for any errors
      return null;
    }
  }
}
