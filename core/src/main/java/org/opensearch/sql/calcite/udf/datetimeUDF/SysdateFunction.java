/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestamp;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

import java.time.Clock;
import java.time.LocalDateTime;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

public class SysdateFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    LocalDateTime localDateTime;
    if (args.length == 0) {
      localDateTime = formatNow(Clock.systemDefaultZone(), 0);
    } else {
      localDateTime = formatNow(Clock.systemDefaultZone(), (int) args[0]);
    }
    return formatTimestamp(localDateTime);
  }
}
