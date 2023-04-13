/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * ContinuePaginatedPlan represents cursor a request.
 * It returns subsequent pages to the user (2nd page and all next).
 * {@link PaginatedPlan}
 */
public class ContinuePaginatedPlan extends AbstractPlan {

  private final String cursor;
  private final QueryService queryService;
  private final PlanSerializer planSerializer;

  private final ResponseListener<ExecutionEngine.QueryResponse> queryResponseListener;


  /**
   * Create an abstract plan that can continue paginating a given cursor.
   */
  public ContinuePaginatedPlan(QueryId queryId, String cursor, QueryService queryService,
                               PlanSerializer planCache,
                               ResponseListener<ExecutionEngine.QueryResponse>
                                   queryResponseListener) {
    super(queryId);
    this.cursor = cursor;
    this.planSerializer = planCache;
    this.queryService = queryService;
    this.queryResponseListener = queryResponseListener;
  }

  @Override
  public void execute() {
    try {
      PhysicalPlan plan = planSerializer.convertToPlan(cursor);
      queryService.executePlan(plan, queryResponseListener);
    } catch (Exception e) {
      queryResponseListener.onFailure(e);
    }
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    listener.onFailure(new UnsupportedOperationException(
        "Explain of a paged query continuation is not supported. "
        + "Use `explain` for the initial query request."));
  }
}
