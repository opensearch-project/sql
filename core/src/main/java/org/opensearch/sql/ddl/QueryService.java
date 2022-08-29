/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableMap;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
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
   * Generate optimal physical plan from logical plan.
   */
  public PhysicalPlan plan(UnresolvedPlan ast) {
    if (ast instanceof DataDefinitionPlan) {
      DataDefinitionPlan ddl = (DataDefinitionPlan) ast;
      ddl.getTask().setQueryService(this);
      ddl.getTask().setSystemCatalog(storageEngine);
    }

    LogicalPlan analyzed = analyzer.analyze(ast, new AnalysisContext());
    Planner planner = new Planner(storageEngine,
        LogicalPlanOptimizer.create(new DSL(repository)));
    return planner.plan(analyzed);
  }

  /**
   * Sync call.
   */
  public QueryResponse execute(UnresolvedPlan ast) {
    return executionEngine.execute(plan(ast));
  }

  /**
   * Async call
   */
  public ExprValue executeAsync(UnresolvedPlan ast) {
    // create query Id.
    String queryId = UUID.randomUUID().toString();

    executionEngine.executeAsync(plan(ast));

    // todo, update query execution table.

    // return queryId.
    return tupleValue(ImmutableMap.of("message", stringValue(queryId)));
  }

  /**
   * Parse, analyze, plan and execute the query.
   */
  public void execute(UnresolvedPlan ast,
                      ResponseListener<QueryResponse> listener) {
    try {
      PhysicalPlan plan = plan(ast);
      executionEngine.execute(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
