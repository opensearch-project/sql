/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class DivideFunction implements UserDefinedFunction {

  @Override
  public Object eval(Object... args) {
    double dividend = ((Number) args[0]).doubleValue();
    double divisor = ((Number) args[1]).doubleValue();
    return dividend / divisor;
  }
}
