/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatDate;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.apache.calcite.runtime.SqlFunctions;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/**
 * Returns a date, given year and day-of-year values. dayofyear must be greater than 0 or the result
 * is NULL. The result is also NULL if either argument is NULL. Arguments are rounded to an integer.
 *
 * <p>Limitations: - Zero year interpreted as 2000; - Negative year is not accepted; - day-of-year
 * should be greater than zero; - day-of-year could be greater than 365/366, calculation switches to
 * the next year(s)
 */
public class MakeDateFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    UserDefinedFunctionUtils.validateArgumentCount("MAKE_DATE", 2, args.length, false);
    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args),
        ImmutableList.of(Number.class, Number.class),
        ImmutableList.of(true, true));

    ExprDoubleValue v1 = new ExprDoubleValue((Number) args[0]);
    ExprDoubleValue v2 = new ExprDoubleValue((Number) args[1]);
    ExprValue date = nullMissingHandling(DateTimeFunctions::exprMakeDate).apply(v1, v2);

    if (date.isNull()) {
      return null;
    }
    return formatDate(date.dateValue());
    //return SqlFunctions.toInt(java.sql.Date.valueOf(date.dateValue()));
  }
}
