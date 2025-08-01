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

/** Implementation for sinh function. */
public class SinhFunction extends ImplementorUDF {
  public SinhFunction() {
    super(new SinhImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  public static class SinhImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      RelDataType inputType = call.getOperands().get(0).getType();

      if (SqlTypeFamily.INTEGER.contains(inputType)) {
        operand = Expressions.convert_(operand, Number.class);
        return Expressions.call(SinhImplementor.class, "IntegralSinh", operand);
      } else {
        operand = Expressions.convert_(operand, Number.class);
        return Expressions.call(SinhImplementor.class, "FloatingSinh", operand);
      }
    }

    public static Number IntegralSinh(Number x) {
      double x0 = x.doubleValue();
      return Math.sinh(x0);
    }

    public static Number FloatingSinh(Number x) {
      BigDecimal x0 = new BigDecimal(x.toString());
      return Math.sinh(x0.doubleValue());
    }
  }
}
