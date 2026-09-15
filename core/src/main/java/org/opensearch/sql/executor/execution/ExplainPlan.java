/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.protocol.response.format.Format;

/** Explain plan. */
public class ExplainPlan extends AbstractPlan {

  private final AbstractPlan plan;
  private final ExplainMode mode;
  private final Format format;

  private final ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  /** Constructor. */
  public ExplainPlan(
      QueryId queryId,
      QueryType queryType,
      AbstractPlan plan,
      ExplainMode mode,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    this(queryId, queryType, plan, mode, null, explainListener);
  }

  /** Constructor with format. */
  public ExplainPlan(
      QueryId queryId,
      QueryType queryType,
      AbstractPlan plan,
      ExplainMode mode,
      Format format,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    super(queryId, queryType);
    this.plan = plan;
    this.mode = mode;
    this.format = format;
    this.explainListener = explainListener;
  }

  @Override
  public void execute() {
    plan.explain(explainListener, mode, format);
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode) {
    throw new UnsupportedOperationException("explain query can not been explained.");
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode, Format format) {
    throw new UnsupportedOperationException("explain query can not been explained.");
  }
}
