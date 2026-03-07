/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.datetime.StrftimeFormatterUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * PPL ctime() conversion function. Converts UNIX epoch timestamps to human-readable time strings
 * using strftime format specifiers. Default format: {@code %m/%d/%Y %H:%M:%S}.
 */
public class CTimeConvertFunction extends ImplementorUDF {

  private static final String DEFAULT_FORMAT = "%m/%d/%Y %H:%M:%S";

  public CTimeConvertFunction() {
    super(new CTimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.ANY_OPTIONAL_STRING;
  }

  public static class CTimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        return Expressions.constant(null, String.class);
      }
      Expression fieldValue = Expressions.box(translatedOperands.get(0));
      if (translatedOperands.size() == 1) {
        return Expressions.call(CTimeConvertFunction.class, "convert", fieldValue);
      }
      Expression timeFormat = Expressions.box(translatedOperands.get(1));
      return Expressions.call(
          CTimeConvertFunction.class, "convertWithFormat", fieldValue, timeFormat);
    }
  }

  public static String convert(Object value) {
    return convertWithFormat(value, null);
  }

  public static String convertWithFormat(Object value, Object timeFormatObj) {
    Double timestamp = toEpochSeconds(value);
    if (timestamp == null) {
      return null;
    }
    String format = (timeFormatObj != null) ? timeFormatObj.toString().trim() : DEFAULT_FORMAT;
    if (format.isEmpty()) {
      return null;
    }
    try {
      Instant instant = Instant.ofEpochSecond(timestamp.longValue());
      ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
      return StrftimeFormatterUtil.formatZonedDateTime(zdt, format).stringValue();
    } catch (Exception e) {
      return null;
    }
  }

  static Double toEpochSeconds(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    String str = value.toString().trim();
    if (str.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
