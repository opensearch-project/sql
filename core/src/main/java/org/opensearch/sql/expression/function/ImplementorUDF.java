/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;

/** The UDF which implements its functionality by NotNullImplementor */
public abstract class ImplementorUDF implements UserDefinedFunctionHelper {
  private final NotNullImplementor implementor;

  protected ImplementorUDF(NotNullImplementor implementor) {
    this.implementor = implementor;
  }

  @Override
  public ImplementableFunction getFunction() {
    return new ImplementableFunction() {
      @Override
      public List<FunctionParameter> getParameters() {
        return List.of();
      }

      @Override
      public CallImplementor getImplementor() {
        return RexImpTable.createImplementor(implementor, NullPolicy.NONE, false);
      }
    };
  }
}
