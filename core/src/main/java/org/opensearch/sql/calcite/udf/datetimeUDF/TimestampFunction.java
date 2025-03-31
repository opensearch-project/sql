/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;

import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

/**
 * We need to write our own since we are actually implement timestamp add here
 * (STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP (STRING/DATE/TIME/DATETIME/TIMESTAMP,
 * STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP
 */
public class TimestampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    if (Objects.isNull(args[0])) {
      return null;
    }
    if (args.length == 3) {
      SqlTypeName sqlTypeName = (SqlTypeName) args[1];
      FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
      return transferInputToExprTimestampValue(args[0], sqlTypeName, restored).valueForCalcite();
    } else {
      SqlTypeName sqlTypeName = (SqlTypeName) args[2];
      ExprValue dateTimeBase = transferInputToExprValue(args[0], sqlTypeName);
      ExprValue addTime = transferInputToExprValue(args[1], (SqlTypeName) args[3]);
      return exprAddTime(FunctionProperties.None, dateTimeBase, addTime).valueForCalcite();
    }
  }
}
