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

public class TimeAddSubFunction extends ImplementorUDF {
  public TimeAddSubFunction(boolean isAdd) {
    super(new TimeAddSubImplementor(isAdd), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> TimeAddSubImplementor.deriveReturnType(opBinding.getOperandType(0));
  }

  public static class TimeAddSubImplementor implements NotNullImplementor {
    final boolean isAdd;

    public TimeAddSubImplementor(boolean isAdd) {
      super();
      this.isAdd = isAdd;
    }

    public static RelDataType deriveReturnType(RelDataType temporalType) {
      if (OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType) == SqlTypeName.TIME) {
        return UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
      }
      return UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
    }

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
