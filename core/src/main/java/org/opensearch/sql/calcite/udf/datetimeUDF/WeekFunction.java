/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.type.SqlTypeName;
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
    //UserDefinedFunctionUtils.validateArgumentCount("WEEK", 2, args.length, false);
    //UserDefinedFunctionUtils.validateArgumentTypes(
     //   Arrays.asList(args), List.of(Number.class, Number.class));
    Instant i;
    if (Objects.isNull(args[0])) {
      return null;
    }
    i = InstantUtils.convertToInstant(args[0], (SqlTypeName) args[2], false);
    ExprValue woyExpr =
        DateTimeFunctions.exprWeek(
            new ExprTimestampValue(i), new ExprIntegerValue((Number) args[1]));
    return woyExpr.integerValue();
  }
}
