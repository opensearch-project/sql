/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import java.util.Optional;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

/**
 * DML query execution. Which includes.
 *
 * <p>select query.
 */
public class DMLQueryExecution extends QueryExecution {

  /**
   * The query plan ast.
   */
  private final UnresolvedPlan plan;

  /**
   * True if the QueryExecution is explain only.
   */
  private final boolean isExplain;

  /**
   * Query service.
   */
  private final QueryService queryService;

  /**
   * Response listener.
   */
  private Optional<ResponseListener<?>> listener = Optional.empty();

  /**
   * constructor.
   */
  public DMLQueryExecution(QueryId queryId, UnresolvedPlan plan, boolean isExplain,
                           QueryService queryService) {
    super(queryId);
    this.plan = plan;
    this.isExplain = isExplain;
    this.queryService = queryService;
  }

  @Override
  public void start() {
    if (listener.isPresent()) {
      if (isExplain) {
        queryService.explain(
            plan, (ResponseListener<ExecutionEngine.ExplainResponse>) listener.get());
      } else {
        queryService.execute(
            plan, (ResponseListener<ExecutionEngine.QueryResponse>) listener.get());
      }
    }
  }

  @Override
  public void registerListener(ResponseListener<?> listener) {
    this.listener = Optional.of(listener);
  }
}
