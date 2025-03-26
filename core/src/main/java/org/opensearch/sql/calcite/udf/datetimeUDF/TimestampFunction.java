/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

/**
 * We need to write our own since we are actually implement timestamp add here
 * (STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP (STRING/DATE/TIME/DATETIME/TIMESTAMP,
 * STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP
 */
public class TimestampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    LocalDateTime datetime;
    Instant dateTimeBase;
    Instant addTime;
    long addTimeMills = 0L;
    if (Objects.isNull(args[0])) {
      return null;
    }
    if (args.length == 2) {
      SqlTypeName sqlTypeName = (SqlTypeName) args[1];
      dateTimeBase = InstantUtils.convertToInstant(args[0], sqlTypeName, false);
    } else {
      SqlTypeName sqlTypeName = (SqlTypeName) args[2];
      dateTimeBase = InstantUtils.convertToInstant(args[0], sqlTypeName, false);
    }

    if (args.length > 2) { // Have something to add
      SqlTypeName addSqlTypeName = (SqlTypeName) args[3];
      addTime = InstantUtils.convertToInstant(args[1], addSqlTypeName, false);
      addTimeMills =
          addTime.atZone(ZoneOffset.UTC).toLocalTime().toNanoOfDay()
              / 1_000_000; // transfer it to millisecond
    }

    return addTwoTimestamp(dateTimeBase, addTimeMills);
  }

  private java.sql.Timestamp addTwoTimestamp(Instant timestamp, Long addTime) {
    Instant newInstant = timestamp.plusMillis(addTime);
    LocalDateTime newTime = LocalDateTime.ofInstant(newInstant, ZoneOffset.UTC);
    return java.sql.Timestamp.valueOf(newTime);
  }
}
