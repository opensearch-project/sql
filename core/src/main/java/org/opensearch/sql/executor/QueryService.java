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
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * The low level interface of core engine.
 */
@RequiredArgsConstructor
public class QueryService {

  private final Analyzer analyzer;

  private final ExecutionEngine executionEngine;

  private final Planner planner;

  /**
   * Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.
   *
   * @param plan  {@link UnresolvedPlan}
   * @param listener {@link ResponseListener}
   */
  public void execute(UnresolvedPlan plan,
                      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      executionEngine.execute(plan(plan), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to
   * get and format explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(UnresolvedPlan plan,
                      ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    try {
      executionEngine.explain(plan(plan), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private PhysicalPlan plan(UnresolvedPlan plan) {
    // 1.Analyze abstract syntax to generate logical plan
    LogicalPlan logicalPlan = analyzer.analyze(plan, new AnalysisContext());

    // 2.Generate optimal physical plan from logical plan
    return planner.plan(logicalPlan);
  }
}
