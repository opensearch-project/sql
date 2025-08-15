/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.condition;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class EnhancedCoalesceFunction extends ImplementorUDF {

  public EnhancedCoalesceFunction() {
    super(createImplementor(), NullPolicy.NONE);
  }

  private static NotNullImplementor createImplementor() {
    return (translator, call, translatedOperands) -> {
      List<Expression> exprValues =
          translatedOperands.stream()
              .map(operand -> Expressions.convert_(operand, Object.class))
              .map(
                  operand ->
                      (Expression)
                          Expressions.call(ExprValueUtils.class, "fromObjectValue", operand))
              .collect(java.util.stream.Collectors.toList());

      Expression result =
          Expressions.call(
              EnhancedCoalesceFunction.class,
              "enhancedCoalesce",
              Expressions.newArrayInit(ExprValue.class, exprValues));

      return Expressions.call(result, "valueForCalcite");
    };
  }

  public static ExprValue enhancedCoalesce(ExprValue... args) {
    for (ExprValue arg : args) {
      if (arg != null && !arg.isNull() && !arg.isMissing()) {
        return arg;
      }
    }
    return ExprValueUtils.nullValue();
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      for (int i = 0; i < opBinding.getOperandCount(); i++) {
        if (!opBinding.isOperandNull(i, false)) {
          return opBinding.getOperandType(i);
        }
      }
      return opBinding.getOperandCount() > 0
          ? opBinding.getOperandType(0)
          : opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }
}
