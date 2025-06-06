/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class ForallFunctionImpl extends ImplementorUDF {
  public ForallFunctionImpl() {
    super(new ForallImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class ForallImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(ForallFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    org.apache.calcite.linq4j.function.Function1 lambdaFunction =
        (org.apache.calcite.linq4j.function.Function1) args[1];
    List<Object> target = (List<Object>) args[0];
    try {
      for (Object candidate : target) {
        if (!(Boolean) lambdaFunction.apply(candidate)) {
          return false;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return true;
  }
}
