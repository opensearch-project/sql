/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class UtcTimeStampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    var zdt = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC);
    return zdt.toInstant().toEpochMilli();
  }
}
