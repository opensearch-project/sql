/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprTimestampValue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

public interface DateTimeApplyUtils {
  static Instant applyInterval(Instant base, Duration interval, boolean isAdd) {
    return isAdd ? base.plus(interval) : base.minus(interval);
  }

  public static ExprTimestampValue transferCalciteValueToExprTimeStampValue(SqlTypeName type, Object target){
    LocalDateTime dateTime;
    switch (type) {
      case DATE:
        dateTime = LocalDateTime.ofInstant(InstantUtils.fromInternalDate((int) target), UTC);
        break;
      case TIME:
        dateTime = LocalDateTime.ofInstant(InstantUtils.fromInternalTime((int) target), UTC);
        break;
      default:
        dateTime = LocalDateTime.ofInstant(InstantUtils.fromEpochMills((long) target), UTC);
    }
    return new ExprTimestampValue(dateTime);
  }

}
