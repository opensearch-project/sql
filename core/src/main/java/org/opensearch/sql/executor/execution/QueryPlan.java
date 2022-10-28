/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

/**
 * Query plan. Which includes.
 *
 * <p>select query.
 */
public class QueryPlan extends AbstractPlan {

  /**
   * The query plan ast.
   */
  private final UnresolvedPlan plan;

  /**
   * Query service.
   */
  private final QueryService queryService;

  private final ResponseListener<ExecutionEngine.QueryResponse> listener;

  /** constructor. */
  public QueryPlan(
      QueryId queryId,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
  }

  @Override
  public void execute() {
    queryService.execute(plan, listener);
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    queryService.explain(plan, listener);
  }
}
