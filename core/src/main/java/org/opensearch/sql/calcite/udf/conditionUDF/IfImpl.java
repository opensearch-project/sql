/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.conditionUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class IfImpl extends ImplementorUDF {
  public IfImpl() {
    super(new IfImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.ARG1;
  }

  public static class IfImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          IfImplementor.class,
          "ifImpl",
          translatedOperands.get(0),
          Expressions.box(translatedOperands.get(1)),
          Expressions.box(translatedOperands.get(2)));
    }

    public static <T> T ifImpl(boolean condition, T trueValue, T falseValue) {
      if (condition) {
        return trueValue;
      } else {
        return falseValue;
      }
    }
  }
}
