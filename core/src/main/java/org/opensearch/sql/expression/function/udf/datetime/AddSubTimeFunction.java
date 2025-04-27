/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSubTime;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Adds or subtracts the second datetime to/from the first datetime.
 *
 * <ul>
 *   <li>If the argument is TIME, today's date is used.
 *   <li>If the argument is DATE, the time is set to midnight.
 * </ul>
 *
 * <p>Argument types:
 *
 * <ul>
 *   <li><code>DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME</code>
 * </ul>
 *
 * <p>Return types:
 *
 * <ul>
 *   <li><code>(DATE/TIMESTAMP, DATE/TIMESTAMP/TIME) -> TIMESTAMP</code>
 *   <li><code>(TIME, DATE/TIMESTAMP/TIME) -> TIME</code>
 * </ul>
 */
public class AddSubTimeFunction extends ImplementorUDF {
  public AddSubTimeFunction(boolean isAdd) {
    super(new TimeAddSubImplementor(isAdd), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataType temporalType = opBinding.getOperandType(0);
      if (OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType) == SqlTypeName.TIME) {
        return UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
      }
      return UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
    };
  }

  @RequiredArgsConstructor
  public static class TimeAddSubImplementor implements NotNullImplementor {
    final boolean isAdd;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression temporal = translatedOperands.get(0);
      Expression temporalDelta = translatedOperands.get(1);
      SqlTypeName temporalType =
          convertRelDataTypeToSqlTypeName(call.getOperands().get(0).getType());
      SqlTypeName temporalDeltaType =
          convertRelDataTypeToSqlTypeName(call.getOperands().get(1).getType());
      return Expressions.call(
          TimeAddSubImplementor.class,
          "timeManipulation",
          Expressions.convert_(temporal, Object.class),
          Expressions.constant(temporalType),
          Expressions.convert_(temporalDelta, Object.class),
          Expressions.constant(temporalDeltaType),
          Expressions.constant(isAdd),
          translator.getRoot());
    }

    public static Object timeManipulation(
        Object temporal,
        SqlTypeName temporalType,
        Object temporalDelta,
        SqlTypeName temporalDeltaType,
        boolean isAdd,
        DataContext propertyContext) {
      ExprValue baseValue = transferInputToExprValue(temporal, temporalType);
      ExprValue intervalValue = transferInputToExprValue(temporalDelta, temporalDeltaType);
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      ExprValue result;
      if (isAdd) {
        result = exprAddTime(restored, baseValue, intervalValue);
      } else {
        result = exprSubTime(restored, baseValue, intervalValue);
      }

      if (temporalType == SqlTypeName.TIME) {
        return new ExprTimeValue(result.timeValue()).valueForCalcite();
      } else {
        return result.valueForCalcite();
      }
    }
  }
}
