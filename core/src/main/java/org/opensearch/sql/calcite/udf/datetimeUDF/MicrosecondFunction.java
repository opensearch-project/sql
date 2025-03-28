/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class MicrosecondFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Instant timestamp = InstantUtils.convertToInstant(args[0], (SqlTypeName) args[1], false);
    return DateTimeFunctions.exprMicrosecond(new ExprTimestampValue(timestamp)).integerValue();
  }
}
