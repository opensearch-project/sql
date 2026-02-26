/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;

/** AbstractPlan represent the execution entity of the Statement. */
@RequiredArgsConstructor
public abstract class AbstractPlan {

  /** Uniq query id. */
  @Getter private final QueryId queryId;

  @Getter protected final QueryType queryType;

  /**
   * Extra search-source JSON from the PPL request body. Set by PPLService before submitting the
   * plan to the query manager. The plan carries this across the thread boundary (REST handler
   * thread → sql-worker thread), and the worker thread sets it as a ThreadLocal before Calcite
   * planning and execution begin. The JSON is later parsed via {@code
   * SearchSourceBuilder.fromXContent()} and selectively merged into the index scan request.
   */
  @Getter @Setter private String extraSearchSource;

  /** Start query execution. */
  public abstract void execute();

  /**
   * Explain query execution.
   *
   * @param listener query explain response listener.
   */
  public abstract void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode);
}
