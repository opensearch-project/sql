/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

/** Plan that produces an AnalyzeResponse (AST + logical plan). */
public class AnalyzePlan extends AbstractPlan {

  private final UnresolvedPlan plan;
  private final QueryService queryService;
  private final ResponseListener<AnalyzeResponse> listener;

  public AnalyzePlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<AnalyzeResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
  }

  @Override
  public void execute() {
    queryService.analyzeWithCalcite(plan, getQueryType(), listener);
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode) {
    throw new UnsupportedOperationException("Explain is not supported for analyze plan");
  }
}
