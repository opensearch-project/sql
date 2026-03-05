/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * PPL mktime() conversion function.
 * Converts human-readable time strings to epoch time (UNIX timestamp).
 * Supports various date/time formats and optional custom timeformat parameter.
 */
public class MkTimeConvertFunction extends ImplementorUDF {

  public static final MkTimeConvertFunction INSTANCE = new MkTimeConvertFunction();

  // Common date/time patterns to try parsing when no custom format is provided
  private static final List<DateTimeFormatter> DEFAULT_TIME_FORMATTERS = Arrays.asList(
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy"),
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy") // ctime format
  );

  public MkTimeConvertFunction() {
    super(new MkTimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        factory ->
            factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE), true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.OPTIONAL_ANY;
  }

  public static class MkTimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        return Expressions.constant(null, Double.class);
      }

      Expression fieldValue = translatedOperands.get(0);

      if (translatedOperands.size() == 1) {
        // Single parameter: use default formats
        return Expressions.call(MkTimeConvertFunction.class, "convert", fieldValue);
      } else {
        // Two parameters: field value and custom timeformat
        Expression timeFormat = translatedOperands.get(1);
        return Expressions.call(MkTimeConvertFunction.class, "convertWithFormat", fieldValue, timeFormat);
      }
    }
  }

  // Method called when no custom timeformat is provided
  public static Object convert(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      // Already a number (timestamp), return as double
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    return convertWithDefaultFormats(str);
  }

  // Method called when custom timeformat is provided
  public static Object convertWithFormat(Object value, Object timeFormatObj) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      // Already a number (timestamp), return as double
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    String timeFormat = timeFormatObj != null ? timeFormatObj.toString().trim() : null;
    if (timeFormat == null || timeFormat.isEmpty()) {
      return convertWithDefaultFormats(str);
    }

    return convertWithCustomFormat(str, timeFormat);
  }

  private static Object convertWithDefaultFormats(String preprocessedValue) {
    // First try to parse as a number (already a timestamp)
    Double existingTimestamp = tryParseDouble(preprocessedValue);
    if (existingTimestamp != null) {
      return existingTimestamp;
    }

    // Try parsing with default date/time formats
    for (DateTimeFormatter formatter : DEFAULT_TIME_FORMATTERS) {
      try {
        LocalDateTime dateTime = LocalDateTime.parse(preprocessedValue, formatter);
        return (double) dateTime.toEpochSecond(ZoneOffset.UTC);
      } catch (DateTimeParseException e) {
        // Try next format
        continue;
      }
    }

    return null;
  }

  private static Object convertWithCustomFormat(String preprocessedValue, String customFormat) {
    // First try to parse as a number (already a timestamp)
    Double existingTimestamp = tryParseDouble(preprocessedValue);
    if (existingTimestamp != null) {
      return existingTimestamp;
    }

    try {
      DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern(customFormat);
      LocalDateTime dateTime = LocalDateTime.parse(preprocessedValue, customFormatter);
      return (double) dateTime.toEpochSecond(ZoneOffset.UTC);
    } catch (Exception e) {
      // If custom format fails, try default formats as fallback
      return convertWithDefaultFormats(preprocessedValue);
    }
  }

  private static String preprocessValue(Object value) {
    if (value == null) {
      return null;
    }
    String str = value instanceof String ? ((String) value).trim() : value.toString().trim();
    return str.isEmpty() ? null : str;
  }

  private static Double tryParseDouble(String str) {
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}