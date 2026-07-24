/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import java.util.List;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.AnalyzeResponse.QuerySegment;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

/** Plan that produces an AnalyzeResponse (AST + logical plan). */
public class AnalyzePlan extends AbstractPlan {

  private final String query;
  private final List<QuerySegment> querySegments;
  private final UnresolvedPlan plan;
  private final QueryService queryService;
  private final ResponseListener<AnalyzeResponse> listener;

  public AnalyzePlan(
      QueryId queryId,
      QueryType queryType,
      String query,
      List<QuerySegment> querySegments,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<AnalyzeResponse> listener) {
    super(queryId, queryType);
    this.query = query;
    this.querySegments = querySegments;
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
  }

  @Override
  public void execute() {
    queryService.analyzeWithCalcite(query, querySegments, plan, getQueryType(), listener);
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode) {
    throw new UnsupportedOperationException("Explain is not supported for analyze plan");
  }
}
