/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** The low level interface of core engine. */
@RequiredArgsConstructor
public class QueryService {

  private final Analyzer analyzer;

  private final ExecutionEngine executionEngine;

  private final Planner planner;

  /**
   * Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br>
   * Todo. deprecated this interface after finalize {@link PlanContext}.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener}
   */
  public void execute(
      UnresolvedPlan plan, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      executePlan(analyze(plan), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * to get response.<br>
   * Todo. Pass split from PlanContext to ExecutionEngine in following PR.
   *
   * @param plan {@link LogicalPlan}
   * @param planContext {@link PlanContext}
   * @param listener {@link ResponseListener}
   */
  public void executePlan(
      LogicalPlan plan,
      PlanContext planContext,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      planContext
          .getSplit()
          .ifPresentOrElse(
              split -> executionEngine.execute(plan(plan), new ExecutionContext(split), listener),
              () ->
                  executionEngine.execute(
                      plan(plan), ExecutionContext.emptyExecutionContext(), listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(
      UnresolvedPlan plan, ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    try {
      executionEngine.explain(plan(analyze(plan)), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan) {
    return analyzer.analyze(plan, new AnalysisContext());
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }
}
