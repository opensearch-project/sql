/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.textUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/** We don't use calcite built in replace since it uses replace instead of replaceAll */
public class ReplaceFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    String baseValue = (String) args[0];
    String fromValue = (String) args[1];
    String toValue = (String) args[2];
    return baseValue.replace(fromValue, toValue);
  }
}
