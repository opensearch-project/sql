/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.nullableDateUDT;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.nullableTimestampUDT;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.convertToTemporalAmount;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;

public class DateAddSubFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    TimeUnit unit = (TimeUnit) args[0];
    long interval = ((Number) args[1]).longValue();
    Object argBase = args[2];
    SqlTypeName sqlTypeName = (SqlTypeName) args[3];
    boolean isAdd = (Boolean) args[4];
    SqlTypeName returnSqlType = (SqlTypeName) args[5];
    ExprValue base = transferInputToExprValue(argBase, sqlTypeName);
    // Instant base = InstantUtils.convertToInstant(argBase, sqlTypeName, false);
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue resultDatetime =
        DateTimeFunctions.exprDateApplyInterval(
            restored, base, convertToTemporalAmount(interval, unit), isAdd);
    // Instant resultInstant = resultDatetime.timestampValue();
    if (returnSqlType == SqlTypeName.TIMESTAMP) {
      return resultDatetime.valueForCalcite();
    } else {
      return new ExprDateValue(resultDatetime.dateValue()).valueForCalcite();
    }
  }

  public static SqlReturnTypeInference getReturnTypeForAddOrSubDate() {
    return opBinding -> {
      RelDataType operandType0 = opBinding.getOperandType(6);
      SqlTypeName typeName = operandType0.getSqlTypeName();
      if (typeName == SqlTypeName.TIMESTAMP) {
        return nullableTimestampUDT;
      } else if (typeName == SqlTypeName.DATE) {
        return nullableDateUDT;
      }
      return opBinding.getTypeFactory().createSqlType(typeName);
    };
  }
}
