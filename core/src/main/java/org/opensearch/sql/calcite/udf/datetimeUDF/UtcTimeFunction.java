/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcTime;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.expression.function.FunctionProperties;

public class UtcTimeFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    return formatTime(exprUtcTime(new FunctionProperties()).timeValue());
    //return java.sql.Time.valueOf(exprUtcTime(new FunctionProperties()).timeValue());
  }
}
