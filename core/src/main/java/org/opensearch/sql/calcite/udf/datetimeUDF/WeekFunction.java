/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/** WEEK & WEEK_OF_YEAR */
public class WeekFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    UserDefinedFunctionUtils.validateArgumentCount("WEEK", 2, args.length, false);
    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args), List.of(Number.class, Number.class));

    if (UserDefinedFunctionUtils.containsNull(args)) {
        return null;
    }
    Instant i = InstantUtils.fromEpochMills(((Number) args[0]).longValue());
    ExprValue woyExpr =
        DateTimeFunctions.exprWeek(
            new ExprTimestampValue(i), new ExprIntegerValue((Number) args[1]));
    return woyExpr.integerValue();
  }
}
