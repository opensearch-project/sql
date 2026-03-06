/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
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
import org.opensearch.sql.expression.datetime.StrftimeFormatterUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * PPL mktime() conversion function. Parses a human-readable time string into UNIX epoch seconds
 * using strftime format specifiers. Default format: {@code %m/%d/%Y %H:%M:%S} (SPL-compatible).
 */
public class MkTimeConvertFunction extends ImplementorUDF {

  public static final MkTimeConvertFunction INSTANCE = new MkTimeConvertFunction();

  private static final String DEFAULT_FORMAT = "%m/%d/%Y %H:%M:%S";

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
    return PPLOperandTypes.ANY_OPTIONAL_STRING;
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
        return Expressions.call(MkTimeConvertFunction.class, "convert", fieldValue);
      }
      Expression timeFormat = translatedOperands.get(1);
      return Expressions.call(
          MkTimeConvertFunction.class, "convertWithFormat", fieldValue, timeFormat);
    }
  }

  public static Object convert(Object value) {
    return convertWithFormat(value, null);
  }

  public static Object convertWithFormat(Object value, Object timeFormatObj) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    String str = value instanceof String ? ((String) value).trim() : value.toString().trim();
    if (str.isEmpty()) {
      return null;
    }
    // If already numeric, return as-is
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException ignored) {
      // Not a number, proceed with date parsing
    }

    String strftimeFormat =
        (timeFormatObj != null && !timeFormatObj.toString().trim().isEmpty())
            ? timeFormatObj.toString().trim()
            : DEFAULT_FORMAT;
    return parseWithFormat(str, strftimeFormat);
  }

  private static Object parseWithFormat(String dateStr, String strftimeFormat) {
    try {
      String javaPattern = StrftimeFormatterUtil.toJavaPattern(strftimeFormat);
      DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern(javaPattern, Locale.ROOT);
      LocalDateTime dateTime = LocalDateTime.parse(dateStr, formatter);
      return (double) dateTime.toEpochSecond(ZoneOffset.UTC);
    } catch (DateTimeParseException | IllegalArgumentException e) {
      return null;
    }
  }
}
