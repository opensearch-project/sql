/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;

/** Explain plan. */
public class ExplainPlan extends AbstractPlan {

  private final AbstractPlan plan;
  private final Explain.ExplainFormat format;

  private final ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  /** Constructor. */
  public ExplainPlan(
      QueryId queryId,
      QueryType queryType,
      AbstractPlan plan,
      Explain.ExplainFormat format,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    super(queryId, queryType);
    this.plan = plan;
    this.format = format;
    this.explainListener = explainListener;
  }

  @Override
  public void execute() {
    plan.explain(explainListener, format);
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, Explain.ExplainFormat format) {
    throw new UnsupportedOperationException("explain query can not been explained.");
  }
}
