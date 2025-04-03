/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.unixTimeStamp;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.unixTimeStampOf;

import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class UnixTimeStampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    if (args.length == 1) {
      FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
      return unixTimeStamp(restored.getQueryStartClock()).longValue();
    }
    Object input = args[0];
    if (Objects.isNull(input)) {
      return null;
    }
    ExprValue candidate =
        fromObjectValue(args[0], convertSqlTypeNameToExprType((SqlTypeName) args[1]));
    return (double) unixTimeStampOf(candidate).longValue();
  }
}
