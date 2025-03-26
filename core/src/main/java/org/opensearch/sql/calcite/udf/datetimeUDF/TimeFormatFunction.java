/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedTime;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;

public class TimeFormatFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    SqlTypeName sqlTypeName = (SqlTypeName) args[2];
    String format = (String) args[1];
    Instant instant = InstantUtils.convertToInstant(args[0], sqlTypeName);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    return getFormattedTime(new ExprTimestampValue(localDateTime), new ExprStringValue(format))
        .stringValue();
  }
}
