/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;

/**
 * AbstractPlan represent the execution entity of the Statement.
 */
@RequiredArgsConstructor
public abstract class AbstractPlan {

  /**
   * Uniq query id.
   */
  @Getter
  private final QueryId queryId;

  /**
   * Start query execution.
   */
  public abstract void execute();

  /**
   * Explain query execution.
   *
   * @param listener query explain response listener.
   */
  public abstract void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener);
}
