/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
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

  public ExecutionEngine.QueryResponse execute(UnresolvedPlan plan) {
    CompletableFuture<ExecutionEngine.QueryResponse> futureResponse = new CompletableFuture<>();
    execute(plan, new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        futureResponse.complete(response);
      }

      @Override
      public void onFailure(Exception e) {
        futureResponse.completeExceptionally(e);
      }
    });

    try {
      return futureResponse.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.
   *
   * @param plan  {@link UnresolvedPlan}
   * @param listener {@link ResponseListener}
   */
  public void execute(UnresolvedPlan plan,
                      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      executionEngine.execute(plan(analyze(plan)), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public void executePlan(PhysicalPlan plan,
                          ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      executionEngine.execute(plan, listener);
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
      executionEngine.explain(plan(analyze(plan)), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public LogicalPlan analyze(UnresolvedPlan plan) {
    return analyzer.analyze(plan, new AnalysisContext());
  }

  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }
}
