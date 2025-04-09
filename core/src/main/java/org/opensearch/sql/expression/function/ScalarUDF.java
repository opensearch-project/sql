/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

/** The UDF which implements its functionality by invoking the `eval` method of a class */
public abstract class ScalarUDF implements UserDefinedFunctionBuilder {
  private final Class<?> clazz;

  protected ScalarUDF(Class<?> clazz) {
    this.clazz = clazz;
  }

  public ImplementableFunction getFunction() {
    return (ImplementableFunction)
        ScalarFunctionImpl.create(Types.lookupMethod(clazz, "eval", Object[].class));
  }
}
