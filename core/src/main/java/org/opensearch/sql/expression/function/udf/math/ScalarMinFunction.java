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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.data.utils.MixedTypeComparator;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MIN(value1, value2, ...) returns the minimum value from the arguments. For mixed types, numbers
 * have higher precedence than strings.
 */
public class ScalarMinFunction extends ImplementorUDF {

  public ScalarMinFunction() {
    super(new MinImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY);
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.VARIADIC);
  }

  public static class MinImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          MinImplementor.class, "min", Expressions.newArrayInit(Object.class, translatedOperands));
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
          .min(MixedTypeComparator.INSTANCE)
          .orElse(null);
    }
  }
}
