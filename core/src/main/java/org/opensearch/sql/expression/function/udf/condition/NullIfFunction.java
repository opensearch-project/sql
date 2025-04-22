/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.condition;

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
import org.opensearch.sql.expression.function.ImplementorUDF;

public class NullIfFunction extends ImplementorUDF {
  public NullIfFunction() {
    // TODO: double check null policy
    super(new NullIfImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.ARG0_FORCE_NULLABLE;
  }

  public static class NullIfImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          NullIfImplementor.class,
          "nullIf",
          translatedOperands.stream()
              .map(Expressions::box)
              .toList() // Ensure all operands are boxed
          );
    }

    public static <T> T nullIf(T firstValue, T secondValue) {
      if (Objects.equals(firstValue, secondValue)) {
        return null;
      }
      return firstValue;
    }
  }
}
