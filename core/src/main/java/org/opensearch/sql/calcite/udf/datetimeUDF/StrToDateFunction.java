/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.runtime.SqlFunctions;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;

/**
 * str_to_date(string, string) is used to extract a TIMESTAMP from the first argument string using
 * the formats specified in the second argument string. The input argument must have enough
 * information to be parsed as a DATE, TIMESTAMP, or TIME. Acceptable string format specifiers are
 * the same as those used in the DATE_FORMAT function. It returns NULL when a statement cannot be
 * parsed due to an invalid pair of arguments, and when 0 is provided for any DATE field. Otherwise,
 * it will return a TIMESTAMP with the parsed values (as well as default values for any field that
 * was not parsed).
 */
public class StrToDateFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    ExprValue formatedDateExpr =
        DateTimeFunctions.exprStrToDate(
                restored,
            new ExprStringValue(args[0].toString()),
            new ExprStringValue(args[1].toString()));

    if (formatedDateExpr.isNull()) {
      return null;
    }

    return formatTimestamp(LocalDateTime.ofInstant(formatedDateExpr.timestampValue(), ZoneOffset.UTC));
  }
}
