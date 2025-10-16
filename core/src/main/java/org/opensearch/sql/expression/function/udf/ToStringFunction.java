/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
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
 * A custom implementation of number/boolean to string .
 *
 * <p>This operator is necessary because tostring has following requirements "binary" Converts a
 * number to a binary value. "hex" Converts the number to a hexadecimal value. "commas" Formats the
 * number with commas. If the number includes a decimal, the function rounds the number to nearest
 * two decimal places. "duration" Converts the value in seconds to the readable time format
 * HH:MM:SS. if not format parameter provided, then consider value as boolean
 */
public class ToStringFunction extends ImplementorUDF {
  public ToStringFunction() {
    super(
        new org.opensearch.sql.expression.function.udf.ToStringFunction.ToStringImplementor(),
        NullPolicy.ANY);
  }

  public static final String DURATION_FORMAT = "duration";
  public static final String HEX_FORMAT = "hex";
  public static final String COMMAS_FORMAT = "commas";
  public static final String BINARY_FORMAT = "binary";

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.BOOLEAN_OR_NUMERIC_STRING_OR_STRING_STRING;
  }

  public static class ToStringImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      if (translatedOperands.size() > 1) {
        Expression format = translatedOperands.get(1);
        return Expressions.call(ToStringFunction.class, "toString", fieldValue, format);
      } else {
        return Expressions.call(ToStringFunction.class, "toString", fieldValue);
      }
    }
  }

  @Strict
  public static String toString(boolean fieldValue) {
    if (fieldValue) {
      return "True";
    } else {
      return "False";
    }
  }

  @Strict
  public static String toString(String fieldValue) {
    return toString(Boolean.parseBoolean(fieldValue));
  }

  @Strict
  public static String toString(BigDecimal num, String format) {
    if (format.equals(DURATION_FORMAT)) {
      Duration d = Duration.ofSeconds(num.toBigInteger().longValue());
      long hours = d.toHours();
      int minutes = d.toMinutesPart();
      int remainingSeconds = d.toSecondsPart();

      String time_str = String.format("%02d:%02d:%02d", hours, minutes, remainingSeconds);
      return time_str;
    } else if (format.equals(HEX_FORMAT)) {
      return num.toBigInteger().toString(16);
    } else if (format.equals(COMMAS_FORMAT)) {
      NumberFormat nf = NumberFormat.getNumberInstance(Locale.getDefault());
      nf.setMinimumFractionDigits(0);
      nf.setMaximumFractionDigits(2);
      return nf.format(num);

    } else if (format.equals(BINARY_FORMAT)) {
      BigInteger integerPart = num.toBigInteger(); // 42
      return integerPart.toString(2);
    }
    return num.toString();
  }

  @Strict
  public static String toString(double num, String format) {
    if (format.equals(DURATION_FORMAT)) {
      Duration d = Duration.ofSeconds(Math.round(num));
      long hours = d.toHours();
      int minutes = d.toMinutesPart();
      int remainingSeconds = d.toSecondsPart();
      String time_str = String.format("%02d:%02d:%02d", hours, minutes, remainingSeconds);
      return time_str;
    } else if (format.equals(HEX_FORMAT)) {
      return Double.toHexString(num);
    } else if (format.equals(COMMAS_FORMAT)) {
      NumberFormat nf = NumberFormat.getNumberInstance(Locale.getDefault());
      return nf.format(num);
    } else if (format.equals(BINARY_FORMAT)) {
      return Long.toBinaryString(Double.doubleToLongBits(num));
    }
    return Double.toString(num);
  }

  @Strict
  public static String toString(int num, String format) {

    if (format.equals(DURATION_FORMAT)) {

      int hours = num / 3600;
      int minutes = (num % 3600) / 60;
      int seconds = num % 60;

      String time_str = String.format("%02d:%02d:%02d", hours, minutes, seconds);
      return time_str;
    } else if (format.equals(HEX_FORMAT)) {
      return Integer.toHexString(num);
    } else if (format.equals(COMMAS_FORMAT)) {
      NumberFormat nf = NumberFormat.getNumberInstance(Locale.getDefault());
      return nf.format(num);
    } else if (format.equals(BINARY_FORMAT)) {
      return Integer.toBinaryString(num);
    }
    return Integer.toString(num);
  }

  @Strict
  public static String toString(String str, String format) {
    if (str.contains(".") || (str.length() > 10)) {
      return toString(Double.parseDouble(str), format);
    } else {
      return toString(Integer.parseInt(str), format);
    }
  }
}
