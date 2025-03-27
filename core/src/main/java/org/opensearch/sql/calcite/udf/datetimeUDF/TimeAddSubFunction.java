/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTime;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;

public class TimeAddSubFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argBase = args[0];
    SqlTypeName baseType = (SqlTypeName) args[1];
    Object argInterval = args[2];
    SqlTypeName argIntervalType = (SqlTypeName) args[3];
    boolean isAdd = (boolean) args[4];

    Instant base = InstantUtils.convertToInstant(argBase, baseType, false);
    Instant interval = InstantUtils.convertToInstant(argInterval, argIntervalType, false);
    LocalTime time = interval.atZone(ZoneOffset.UTC).toLocalTime();
    Duration duration = Duration.between(LocalTime.MIN, time);

    Instant newInstant = DateTimeApplyUtils.applyInterval(base, duration, isAdd);

    if (baseType == SqlTypeName.TIME) {
      return formatTime(LocalTime.ofInstant(newInstant, ZoneOffset.UTC));
    } else {
      return formatTimestamp(LocalDateTime.ofInstant(newInstant, ZoneOffset.UTC));
    }
  }
}
