/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Query service for internal use.
 */
@RequiredArgsConstructor
public class QueryService {

  private final Analyzer analyzer;

  private final StorageEngine storageEngine;

  private final ExecutionEngine executionEngine;

  private final BuiltinFunctionRepository repository;

  /**
   * Sync call.
   */
  public void execute(UnresolvedPlan ast) {
    LogicalPlan analyzed = analyzer.analyze(ast, new AnalysisContext());
    Planner planner = new Planner(storageEngine,
        LogicalPlanOptimizer.create(new DSL(repository)));
    PhysicalPlan plan = planner.plan(analyzed);
    executionEngine.execute(plan);
  }

  /**
   * Parse, analyze, plan and execute the query.
   */
  public void execute(UnresolvedPlan ast,
                      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      LogicalPlan analyzed = analyzer.analyze(ast, new AnalysisContext());
      Planner planner = new Planner(storageEngine,
          LogicalPlanOptimizer.create(new DSL(repository)));
      PhysicalPlan plan = planner.plan(analyzed);
      executionEngine.execute(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
