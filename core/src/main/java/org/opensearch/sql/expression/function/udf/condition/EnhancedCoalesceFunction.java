/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.condition;

import java.util.List;
import java.util.function.Supplier;
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
              .map(
                  operand ->
                      (Expression)
                          Expressions.call(
                              ExprValueUtils.class,
                              "fromObjectValue",
                              Expressions.convert_(operand, Object.class)))
              .toList();

      Expression returnTypeName = Expressions.constant(call.getType().getSqlTypeName().toString());

      Expression result =
          Expressions.call(
              EnhancedCoalesceFunction.class,
              "enhancedCoalesceWithType",
              Expressions.newArrayInit(ExprValue.class, exprValues),
              returnTypeName);

      return Expressions.call(result, "valueForCalcite");
    };
  }

  public static ExprValue enhancedCoalesceWithType(ExprValue[] args, String returnTypeName) {
    for (ExprValue arg : args) {
      if (arg != null && !arg.isNull() && !arg.isMissing()) {
        return coerceToType(arg, returnTypeName);
      }
    }
    return ExprValueUtils.nullValue();
  }

  private static ExprValue coerceToType(ExprValue value, String typeName) {
    switch (typeName) {
      case "INTEGER":
        return tryConvert(() -> ExprValueUtils.integerValue(value.integerValue()), value);
      case "BIGINT":
        return tryConvert(() -> ExprValueUtils.longValue(value.longValue()), value);
      case "SMALLINT":
      case "TINYINT":
        return tryConvert(() -> ExprValueUtils.integerValue(value.integerValue()), value);
      case "DOUBLE":
        return tryConvert(() -> ExprValueUtils.doubleValue(value.doubleValue()), value);
      case "FLOAT":
      case "REAL":
        return tryConvert(() -> ExprValueUtils.floatValue(value.floatValue()), value);
      case "BOOLEAN":
        return tryConvert(() -> ExprValueUtils.booleanValue(value.booleanValue()), value);
      case "VARCHAR":
      case "CHAR":
        return tryConvert(() -> ExprValueUtils.stringValue(String.valueOf(value.value())), value);
      case "DATE":
        return tryConvert(() -> ExprValueUtils.dateValue(value.dateValue()), value);
      case "TIME":
        return tryConvert(() -> ExprValueUtils.timeValue(value.timeValue()), value);
      case "TIMESTAMP":
        return tryConvert(() -> ExprValueUtils.timestampValue(value.timestampValue()), value);
      case "DECIMAL":
        return tryConvert(() -> ExprValueUtils.doubleValue(value.doubleValue()), value);
      default:
        return value;
    }
  }

  private static ExprValue tryConvert(Supplier<ExprValue> converter, ExprValue fallbackValue) {
    try {
      return converter.get();
    } catch (Exception e) {
      return ExprValueUtils.stringValue(String.valueOf(fallbackValue.value()));
    }
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      var operandTypes = opBinding.collectOperandTypes();

      // Let Calcite determine the least restrictive common type
      var commonType = opBinding.getTypeFactory().leastRestrictive(operandTypes);
      return commonType != null
          ? commonType
          : opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }
}
