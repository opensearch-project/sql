/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

/**
 * We need to write our own since we are actually implement timestamp add here
 * (STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP (STRING/DATE/TIME/DATETIME/TIMESTAMP,
 * STRING/DATE/TIME/DATETIME/TIMESTAMP) -> TIMESTAMP
 */
public class timestampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    LocalDateTime datetime;
    Instant dateTimeBase;
    Instant addTime;
    long addTimeMills = 0L;
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    switch (sqlTypeName) {
      case DATE:
        dateTimeBase = InstantUtils.fromInternalDate((int) args[0]);
        break;
      case TIMESTAMP:
        dateTimeBase = InstantUtils.fromEpochMills((long) args[0]);
        break;
      case TIME:
        dateTimeBase = InstantUtils.fromInternalTime((int) args[0]);
        break;
      default:
        String timestampExpression = (String) args[0];
        datetime =
            LocalDateTime.parse(timestampExpression, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
        dateTimeBase = datetime.toInstant(ZoneOffset.UTC);
    }

    if (args.length > 2) { // Have something to add
      SqlTypeName addSqlTypeName = (SqlTypeName) args[3];
      switch (addSqlTypeName) {
        case TIMESTAMP:
          addTime = InstantUtils.fromEpochMills((long) args[2]);
          break;
        case TIME:
          addTime = InstantUtils.fromInternalTime((int) args[2]);
          break;
        default:
          String timestampExpression = (String) args[2];
          LocalDateTime addDateTime =
              LocalDateTime.parse(timestampExpression, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
          addTime = addDateTime.toInstant(ZoneOffset.UTC);
      }
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
