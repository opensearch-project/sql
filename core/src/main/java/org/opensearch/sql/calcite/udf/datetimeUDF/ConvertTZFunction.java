/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class ConvertTZFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    Object argTimestamp = args[0];
    Object fromTz = args[1];
    Object toTz = args[2];
    SqlTypeName sqlTypeName = (SqlTypeName) args[3];
    Instant datetimeInstant;
    try {
      datetimeInstant = InstantUtils.convertToInstant(argTimestamp, sqlTypeName, true);
    } catch (IllegalArgumentException e) {
      return null;
    }
    ExprValue datetimeExpr =
        DateTimeFunctions.exprConvertTZ(
            new ExprTimestampValue(datetimeInstant),
            new ExprStringValue(fromTz.toString()),
            new ExprStringValue(toTz.toString()));

    if (datetimeExpr.isNull()) {
      return null;
    }
    return formatTimestamp(LocalDateTime.ofInstant(datetimeExpr.timestampValue(), ZoneOffset.UTC));
  }
}
