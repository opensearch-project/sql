/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * An {@link ExecutionEngine} that delegates Calcite RelNode execution to the first extension whose
 * {@link ExecutionEngine#canVectorize(RelNode)} returns {@code true}, falling back to the default
 * engine otherwise. Non-Calcite ({@link PhysicalPlan}) methods and unmatched RelNode plans are
 * forwarded to the default engine.
 */
@RequiredArgsConstructor
@Log4j2
public class DelegatingExecutionEngine implements ExecutionEngine {

  private final ExecutionEngine defaultEngine;
  private final List<ExecutionEngine> extensions;

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    defaultEngine.execute(plan, listener);
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    defaultEngine.execute(plan, context, listener);
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    defaultEngine.explain(plan, listener);
  }

  @Override
  public boolean canVectorize(RelNode plan) {
    return findExtension(plan).isPresent();
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    Optional<ExecutionEngine> ext = findExtension(plan);
    if (ext.isPresent()) {
      log.info("Routing query to extension engine : {}", ext.get().getClass().getSimpleName());
      ext.get().execute(plan, context, listener);
    } else {
      defaultEngine.execute(plan, context, listener);
    }
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    Optional<ExecutionEngine> ext = findExtension(plan);
    if (ext.isPresent()) {
      ext.get().explain(plan, mode, context, listener);
    } else {
      defaultEngine.explain(plan, mode, context, listener);
    }
  }

  private Optional<ExecutionEngine> findExtension(RelNode plan) {
    return extensions.stream().filter(ext -> ext.canVectorize(plan)).findFirst();
  }
}
