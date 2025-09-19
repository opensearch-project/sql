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
 *MIN(value1, value2, ...) returns the minimum value from the arguments.
 * For mixed types, numbers have higher precedence than strings.
 */
public class MinFunction extends ImplementorUDF {

  public MinFunction() {
    super(new MinImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class MinImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          MinImplementor.class,
          "min",
          Expressions.newArrayInit(Object.class, translatedOperands));
    }

    public static Object min(Object[] args) {
      return findMin(args);
    }

    private static Object findMin(Object[] args) {
      if (args == null) {
        return null;
      }

      return Arrays.stream(args)
          .filter(Objects::nonNull)
          .reduce(MinImplementor::compareMin)
          .orElse(null);
    }

    private static Object compareMin(Object a, Object b) {
      boolean aIsNumeric = isNumeric(a);
      boolean bIsNumeric = isNumeric(b);

      if (aIsNumeric != bIsNumeric) {
        return aIsNumeric ? a : b;
      }
      if (aIsNumeric) {
        return ((Number) a).doubleValue() <= ((Number) b).doubleValue() ? a : b;
      }
      return a.toString().compareTo(b.toString()) <= 0 ? a : b;
    }

    private static boolean isNumeric(Object obj) {
      return obj instanceof Number;
    }
  }
}