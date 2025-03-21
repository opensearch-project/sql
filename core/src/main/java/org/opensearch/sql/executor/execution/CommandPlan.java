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
import org.opensearch.sql.executor.QueryType;

/**
 * Query plan which does not reflect a search query being executed. It contains a command or an
 * action, for example, a DDL query.
 */
public class CommandPlan extends AbstractPlan {

  /** The query plan ast. */
  protected final UnresolvedPlan plan;

  /** Query service. */
  protected final QueryService queryService;

  protected final ResponseListener<ExecutionEngine.QueryResponse> listener;

  /** Constructor. */
  public CommandPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
  }

  @Override
  public void execute() {
    queryService.execute(plan, getQueryType(), listener);
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    throw new UnsupportedOperationException("CommandPlan does not support explain");
  }
}
