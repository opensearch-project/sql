/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAdd;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;

public class TimestampAddFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    if (args.length != 4) {
      throw new IllegalArgumentException("TimestampAddFunction requires 3 arguments");
    }
    String addUnit = (String) args[0];
    int amount = (int) args[1];
    SqlTypeName sqlTypeName = (SqlTypeName) args[3];
    Instant dateTimeBase;
    dateTimeBase = InstantUtils.convertToInstant(args[2], sqlTypeName);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTimeBase, ZoneOffset.UTC);
    ExprValue returnValue =
        exprTimestampAdd(
            new ExprStringValue(addUnit),
            new ExprLongValue(amount),
            new ExprTimestampValue(localDateTime));
    return java.sql.Timestamp.valueOf(
        LocalDateTime.ofInstant(returnValue.timestampValue(), ZoneOffset.UTC));
  }
}
