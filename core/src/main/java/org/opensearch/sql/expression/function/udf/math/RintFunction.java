/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Implementation for rint function. */
public class RintFunction extends ImplementorUDF {
  public RintFunction() {
    super(new RintImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  public static class RintImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      operand = Expressions.convert_(operand, double.class);

      return Expressions.call(RintImplementor.class, "rint", operand);
    }

    public static double rint(double x) {
      return Math.rint(x);
    }
  }
}
