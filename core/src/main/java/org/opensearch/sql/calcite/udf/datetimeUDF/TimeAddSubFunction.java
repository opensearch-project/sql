/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIME;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSubTime;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class TimeAddSubFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argBase = args[0];
    SqlTypeName baseType = (SqlTypeName) args[1];
    Object argInterval = args[2];
    SqlTypeName argIntervalType = (SqlTypeName) args[3];
    boolean isAdd = (boolean) args[4];
    ExprValue baseValue = transferInputToExprValue(args[0], baseType);
    ExprValue intervalValue = transferInputToExprValue(argInterval, argIntervalType);
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue result;
    if (isAdd) {
      result = exprAddTime(restored, baseValue, intervalValue);
    } else {
      result = exprSubTime(restored, baseValue, intervalValue);
    }

    if (baseType == SqlTypeName.TIME) {
      return new ExprTimeValue(result.timeValue()).valueForCalcite();
    } else {
      return result.valueForCalcite();
    }
  }

  /**
   * ADDTIME and SUBTIME has special return type maps: (DATE/TIMESTAMP, DATE/TIMESTAMP/TIME) ->
   * TIMESTAMP (TIME, DATE/TIMESTAMP/TIME) -> TIME Therefore, we create a special return type
   * inference for them.
   */
  public static SqlReturnTypeInference getReturnTypeForTimeAddSub() {
    return opBinding -> {
      RelDataType operandType0 = opBinding.getOperandType(0);
      if (operandType0 instanceof ExprSqlType) {
        OpenSearchTypeFactory.ExprUDT exprUDT = ((ExprSqlType) operandType0).getUdt();
        if (exprUDT == EXPR_DATE || exprUDT == EXPR_TIMESTAMP) {
          return UserDefinedFunctionUtils.nullableTimestampUDT;
        } else if (exprUDT == EXPR_TIME) {
          return UserDefinedFunctionUtils.nullableTimeUDT;
        } else {
          throw new IllegalArgumentException("Unsupported UDT type");
        }
      }
      SqlTypeName typeName = operandType0.getSqlTypeName();
      return switch (typeName) {
        case DATE, TIMESTAMP ->
        // Return TIMESTAMP
        UserDefinedFunctionUtils.nullableTimestampUDT;
        case TIME ->
        // Return TIME
        UserDefinedFunctionUtils.nullableTimeUDT;
        default -> throw new IllegalArgumentException("Unsupported type: " + typeName);
      };
    };
  }
}
