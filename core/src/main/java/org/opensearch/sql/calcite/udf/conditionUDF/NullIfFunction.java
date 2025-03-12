/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.conditionUDF;

import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class NullIfFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("IfNull  function expects two arguments");
    }
    Object firstValue = args[0];
    Object secondValue = args[1];
    if (Objects.equals(firstValue, secondValue)) {
      return null;
    }
    return firstValue;
  }
}
