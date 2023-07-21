/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.execution;

import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;

/**
 * Explain plan.
 */
public class ExplainPlan extends AbstractPlan {

  private final AbstractPlan plan;

  private final ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  /**
   * Constructor.
   */
  public ExplainPlan(QueryId queryId,
                     AbstractPlan plan,
                     ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    super(queryId);
    this.plan = plan;
    this.explainListener = explainListener;
  }

  @Override
  public void execute() {
    plan.explain(explainListener);
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    throw new UnsupportedOperationException("explain query can not been explained.");
  }
}
