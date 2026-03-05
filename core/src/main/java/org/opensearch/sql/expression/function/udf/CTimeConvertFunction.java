/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * PPL ctime() conversion function.
 * Converts UNIX timestamps to human-readable format.
 * Format: "Mon Oct 13 20:07:13 PDT 2003"
 */
public class CTimeConvertFunction extends ImplementorUDF {

  // Format pattern matching expected output: "Mon Oct 13 20:07:13 PDT 2003"
  private static final DateTimeFormatter CTIME_FORMATTER =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy");

  public CTimeConvertFunction() {
    super(new CTimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.OPTIONAL_ANY;
  }

  public static class CTimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        return Expressions.constant(null, String.class);
      }

      Expression fieldValue = translatedOperands.get(0);

      if (translatedOperands.size() == 1) {
        // Single parameter: use default ctime format
        return Expressions.call(CTimeConvertFunction.class, "convert", fieldValue);
      } else {
        // Two parameters: field value and custom timeformat
        Expression timeFormat = translatedOperands.get(1);
        return Expressions.call(CTimeConvertFunction.class, "convertWithFormat", fieldValue, timeFormat);
      }
    }
  }

  @Strict
  public static String convert(Object value) {
    if (value == null) {
      return null;
    }

    try {
      double timestamp;
      if (value instanceof Number) {
        timestamp = ((Number) value).doubleValue();
      } else {
        String str = value.toString().trim();
        if (str.isEmpty()) {
          return null;
        }
        timestamp = Double.parseDouble(str);
      }

      // Convert to Instant and format
      Instant instant = Instant.ofEpochSecond((long) timestamp);
      return CTIME_FORMATTER.format(instant.atZone(ZoneOffset.UTC));

    } catch (Exception e) {
      return null;
    }
  }

  @Strict
  public static String convertWithFormat(Object value, Object timeFormatObj) {
    if (value == null) {
      return null;
    }

    String customFormat = timeFormatObj != null ? timeFormatObj.toString().trim() : null;
    if (customFormat == null || customFormat.isEmpty()) {
      // Fall back to default ctime format
      return convert(value);
    }

    try {
      double timestamp;
      if (value instanceof Number) {
        timestamp = ((Number) value).doubleValue();
      } else {
        String str = value.toString().trim();
        if (str.isEmpty()) {
          return null;
        }
        timestamp = Double.parseDouble(str);
      }

      // Convert to Instant and format with custom formatter
      Instant instant = Instant.ofEpochSecond((long) timestamp);
      DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern(customFormat);
      return customFormatter.format(instant.atZone(ZoneOffset.UTC));

    } catch (Exception e) {
      // If custom format fails, fall back to default ctime format
      return convert(value);
    }
  }
}