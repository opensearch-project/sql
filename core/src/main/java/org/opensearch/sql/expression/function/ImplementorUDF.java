/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;

/** The UDF which implements its functionality by NotNullImplementor */
public abstract class ImplementorUDF implements UserDefinedFunctionBuilder {
  private final NotNullImplementor implementor;
  private final NullPolicy nullPolicy;

  protected ImplementorUDF(NotNullImplementor implementor, NullPolicy nullPolicy) {
    this.implementor = implementor;
    this.nullPolicy = nullPolicy;
  }

  @Override
  public ImplementableFunction getFunction() {
    return new ImplementableUDFunction(implementor, nullPolicy);
  }

  /**
   * Named ImplementableFunction that exposes the NotNullImplementor and NullPolicy. This allows
   * rewriters (e.g., DatetimeUdtRewriter) to access the original implementor for wrapping without
   * reflection.
   */
  @Getter
  @RequiredArgsConstructor
  public static class ImplementableUDFunction implements ImplementableFunction {
    private final NotNullImplementor notNullImplementor;
    private final NullPolicy nullPolicy;

    @Override
    public List<FunctionParameter> getParameters() {
      return List.of();
    }

    @Override
    public CallImplementor getImplementor() {
      return RexImpTable.createImplementor(notNullImplementor, nullPolicy, false);
    }
  }
}
