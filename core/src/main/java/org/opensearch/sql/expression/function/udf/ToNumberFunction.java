/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
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
public class ToNumberFunction extends ImplementorUDF {
  public ToNumberFunction() {
    super(
        new org.opensearch.sql.expression.function.udf.ToNumberFunction.ToNumberImplementor(),
        NullPolicy.ANY);
  }

  public static final String DURATION_FORMAT = "duration";
  public static final String DURATION_MILLIS_FORMAT = "duration_millis";
  public static final String HEX_FORMAT = "hex";
  public static final String COMMAS_FORMAT = "commas";
  public static final String BINARY_FORMAT = "binary";
  public static final SqlFunctions.DateFormatFunction dateTimeFormatter =
      new SqlFunctions.DateFormatFunction();
  public static final String format24hour = "%H:%M:%S"; // 24-hour format

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_OR_STRING_INTEGER;
  }

  public static class ToNumberImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      int base = 10;
      if (translatedOperands.size() > 1) {
        Expression baseExpr = translatedOperands.get(1);
        return Expressions.call(ToNumberFunction.class, "toNumber", fieldValue, baseExpr);
      } else {
        return Expressions.call(ToNumberFunction.class, "toNumber", fieldValue);
      }
    }
  }

  @Strict
  public static Number toNumber(String numStr) {
    return toNumber(numStr, 10);
  }

  @Strict
  public static Number toNumber(String numStr, int base) {
    if (base < 2 || base > 36) {
      throw new IllegalArgumentException("Base must be between 2 and 36");
    }

    if (numStr.contains(".")) {

      boolean isNegative = numStr.startsWith("-");
      if (isNegative) {
        numStr = numStr.substring(1);
      }

      // Split integer and fractional parts
      String[] parts = numStr.split("\\.");
      String intPart = parts[0];
      String fracPart = parts.length > 1 ? parts[1] : "";

      // Convert integer part
      double intValue = 0;
      for (char c : intPart.toCharArray()) {
        int digit = Character.digit(c, base);
        if (digit < 0) throw new IllegalArgumentException("Invalid digit: " + c);
        intValue = intValue * base + digit;
      }

      // Convert fractional part
      double fracValue = 0;
      double divisor = base;
      for (char c : fracPart.toCharArray()) {
        int digit = Character.digit(c, base);
        if (digit < 0) throw new IllegalArgumentException("Invalid digit: " + c);
        fracValue += (double) digit / divisor;
        divisor *= base;
      }

      double result = intValue + fracValue;
      return isNegative ? -result : result;
    } else {
      return Integer.parseInt(numStr, base);
    }
  }
}
