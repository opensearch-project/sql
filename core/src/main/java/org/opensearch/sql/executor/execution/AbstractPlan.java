/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.protocol.response.format.Format;

/** AbstractPlan represent the execution entity of the Statement. */
@RequiredArgsConstructor
public abstract class AbstractPlan {

  /** Uniq query id. */
  @Getter private final QueryId queryId;

  @Getter protected final QueryType queryType;

  /** Start query execution. */
  public abstract void execute();

  /**
   * Explain query execution.
   *
   * @param listener query explain response listener.
   */
  public abstract void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode);

  /**
   * Explain query execution with format.
   *
   * @param listener query explain response listener.
   * @param mode explain mode
   * @param format output format
   */
  public abstract void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode, Format format);
}
