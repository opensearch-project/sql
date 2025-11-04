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
                              Expressions.convert_(Expressions.box(operand), Object.class)))
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
    return switch (typeName) {
      case "INTEGER" -> tryConvert(() -> ExprValueUtils.integerValue(value.integerValue()), value);
      case "BIGINT" -> tryConvert(() -> ExprValueUtils.longValue(value.longValue()), value);
      case "SMALLINT", "TINYINT" -> tryConvert(
          () -> ExprValueUtils.integerValue(value.integerValue()), value);
      case "DOUBLE" -> tryConvert(() -> ExprValueUtils.doubleValue(value.doubleValue()), value);
      case "FLOAT", "REAL" -> tryConvert(
          () -> ExprValueUtils.floatValue(value.floatValue()), value);
      case "BOOLEAN" -> tryConvert(() -> ExprValueUtils.booleanValue(value.booleanValue()), value);
      case "VARCHAR", "CHAR" -> tryConvert(
          () -> ExprValueUtils.stringValue(String.valueOf(value.value())), value);
      case "DATE" -> tryConvert(() -> ExprValueUtils.dateValue(value.dateValue()), value);
      case "TIME" -> tryConvert(() -> ExprValueUtils.timeValue(value.timeValue()), value);
      case "TIMESTAMP" -> tryConvert(
          () -> ExprValueUtils.timestampValue(value.timestampValue()), value);
      case "DECIMAL" -> tryConvert(() -> ExprValueUtils.doubleValue(value.doubleValue()), value);
      default -> value;
    };
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
