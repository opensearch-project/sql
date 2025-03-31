/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/** WEEK & WEEK_OF_YEAR */
public class WeekFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (Objects.isNull(args[0])) {
      return null;
    }

    // Instant i = InstantUtils.convertToInstant(args[0], (SqlTypeName) args[2], false);
    ExprValue candidate =
        fromObjectValue(args[0], convertSqlTypeNameToExprType((SqlTypeName) args[2]));
    ExprValue woyExpr =
        DateTimeFunctions.exprWeek(candidate, new ExprIntegerValue((Number) args[1]));
    return woyExpr.integerValue();
  }
}
