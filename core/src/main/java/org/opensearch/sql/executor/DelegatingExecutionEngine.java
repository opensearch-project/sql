/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * An {@link ExecutionEngine} that delegates Calcite RelNode execution to the first extension whose
 * {@link ExecutionEngine#canVectorize(RelNode)} returns {@code true}, falling back to a default
 * engine otherwise. All non-Calcite methods are forwarded directly to the default engine.
 */
@RequiredArgsConstructor
@Log4j2
public class DelegatingExecutionEngine implements ExecutionEngine {

  private final ExecutionEngine defaultEngine;
  private final List<ExecutionEngine> extensions;

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException("DelegatingExecutionEngine only accepts RelNode");
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException("DelegatingExecutionEngine only accepts RelNode");
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    throw new UnsupportedOperationException("DelegatingExecutionEngine only accepts RelNode");
  }

  @Override
  public boolean canVectorize(RelNode plan) {
    for (ExecutionEngine ext : extensions) {
      if (ext.canVectorize(plan)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    for (ExecutionEngine ext : extensions) {
      if (ext.canVectorize(plan)) {
        log.info("Routing query to extension engine : {}", ext.getClass().getSimpleName());
        ext.execute(plan, context, listener);
        return;
      }
    }
    defaultEngine.execute(plan, context, listener);
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    for (ExecutionEngine ext : extensions) {
      if (ext.canVectorize(plan)) {
        ext.explain(plan, mode, context, listener);
        return;
      }
    }
    defaultEngine.explain(plan, mode, context, listener);
  }
}
