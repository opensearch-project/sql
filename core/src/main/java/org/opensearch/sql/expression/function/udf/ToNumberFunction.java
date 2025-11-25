/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.math.BigInteger;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * The following usage options are available, depending on the parameter types and the number of
 * parameters.
 *
 * <p><b>Usage:</b> {@code tonumber(string, [base])} converts the value in the first argument. The
 * second argument describes the base of the first argument. If the second argument is not provided,
 * the value is converted using base 10.
 *
 * <p><b>Return type:</b> Number
 *
 * <p>You can use this function with the eval commands and as part of eval expressions.
 *
 * <p>Base values can range from 2 to 36. The maximum value supported for base 10 is {@code +(2 −
 * 2^-52) · 2^1023} and the minimum is {@code −(2 − 2^-52) · 2^1023}.
 *
 * <p>The maximum for other supported bases is {@code 2^63 − 1} (or {@code 7FFFFFFFFFFFFFFF}) and
 * the minimum is {@code -2^63} (or {@code -7FFFFFFFFFFFFFFF}).
 */
public class ToNumberFunction extends ImplementorUDF {
  public ToNumberFunction() {
    super(
        new org.opensearch.sql.expression.function.udf.ToNumberFunction.ToNumberImplementor(),
        NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return (opBinding) -> {
      // Try to determine if the result will be Long or Double based on the input
      int base = 10;
      try {
        base =
            opBinding.getOperandCount() > 1
                ? opBinding.getOperandLiteralValue(1, Integer.class)
                : 10;
      } catch (NumberFormatException e) {
        // If parsing fails, default to  base 10
      }
      if (opBinding.getOperandCount() > 0 && opBinding.isOperandLiteral(0, false)) {
        String literal = opBinding.getOperandLiteralValue(0, String.class);
        if (literal != null) {

          // Check if it's a decimal number
          if (base != 10 || !(literal.contains("."))) {
            return opBinding
                .getTypeFactory()
                .createTypeWithNullability(
                    opBinding.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true);
          }
        }
      }
      // Default to Double when we can't determine the type at compile time or bigint type is
      // confirmed
      return opBinding
          .getTypeFactory()
          .createTypeWithNullability(
              opBinding
                  .getTypeFactory()
                  .createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE),
              true);
    };
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
      throw new IllegalArgumentException("Base has to be between 2 and 36.");
    }
    Number result = null;
    try {
      if (base == 10) {
        if (numStr.contains(".")) {
          result = Double.parseDouble(numStr);
        } else {
          result = Long.parseLong(numStr);
        }
      } else {
        BigInteger bigInteger = new BigInteger(numStr, base);
        result = bigInteger.longValue();
      }
    } catch (Exception e) {
      // Return null when parsing fails, matches function behavior
    }
    return result;
  }
}
