/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MAX(value1, value2, ...) returns the maximum value from the arguments.
 * For mixed types, strings have higher precedence than numbers.
 */
public class MaxFunction extends ImplementorUDF {

  public MaxFunction() {
    super(new MaxImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class MaxImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          MaxImplementor.class,
          "max",
          Expressions.newArrayInit(Object.class, translatedOperands));
    }

    public static Object max(Object[] args) {
      return findMax(args);
    }

    private static Object findMax(Object[] args) {
      if (args == null) {
        return null;
      }

      return Arrays.stream(args)
          .filter(Objects::nonNull)
          .reduce(MaxImplementor::compareMax)
          .orElse(null);
    }

    private static Object compareMax(Object a, Object b) {
      boolean aIsNumeric = isNumeric(a);
      boolean bIsNumeric = isNumeric(b);

      if (aIsNumeric != bIsNumeric) {
        return aIsNumeric ? b : a;
      }
      if (aIsNumeric) {
        return ((Number) a).doubleValue() >= ((Number) b).doubleValue() ? a : b;
      }
      return a.toString().compareTo(b.toString()) >= 0 ? a : b;
    }

    private static boolean isNumeric(Object obj) {
      return obj instanceof Number;
    }
  }
}