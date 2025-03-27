/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.systemUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class TypeOfFunction implements UserDefinedFunction {

  @Override
  public Object eval(Object... args) {
    return args[0];
  }
}
