/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Implementation for signum function. */
public class SignumFunction extends ImplementorUDF {
  public SignumFunction() {
    super(new SignumImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  public static class SignumImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      RelDataType inputType = call.getOperands().get(0).getType();

      if (SqlTypeFamily.INTEGER.contains(inputType)) {
        operand = Expressions.convert_(operand, Number.class);
        return Expressions.call(SignumImplementor.class, "IntegralSignum", operand);
      } else {
        operand = Expressions.convert_(operand, Number.class);
        return Expressions.call(SignumImplementor.class, "FloatingSignum", operand);
      }
    }

    public static Number IntegralSignum(Number x) {
      double x0 = x.doubleValue();
      return (int) Math.signum(x0);
    }

    public static Number FloatingSignum(Number x) {
      BigDecimal x0 = new BigDecimal(x.toString());
      return (int) Math.signum(x0.doubleValue());
    }
  }
}
