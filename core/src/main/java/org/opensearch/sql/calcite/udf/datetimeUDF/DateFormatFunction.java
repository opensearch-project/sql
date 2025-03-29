/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.expression.datetime.DateTimeFormatterUtil;

public class DateFormatFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argDatetime = args[0];
    Object argDatetimeType = args[1];
    Object argFormat = args[2];

    Instant datetimeInstant =
        InstantUtils.convertToInstant(argDatetime, (SqlTypeName) argDatetimeType, false);
    LocalDateTime datetime = LocalDateTime.ofInstant(datetimeInstant, ZoneOffset.UTC);

    return DateTimeFormatterUtil.getFormattedDatetime(datetime, argFormat.toString());
  }
}
