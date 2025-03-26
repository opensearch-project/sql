/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class PeriodDiffFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    UserDefinedFunctionUtils.validateArgumentCount("PERIOD_DIFF", 2, args.length, false);

    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args), ImmutableList.of(Number.class, Number.class));

    ExprValue periodDiffExpr =
        DateTimeFunctions.exprPeriodDiff(
            new ExprIntegerValue((Number) args[0]), new ExprIntegerValue((Number) args[1]));

    return periodDiffExpr.integerValue();
  }
}
