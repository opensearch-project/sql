/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import java.util.Map;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;

/** Explain plan. */
public class ExplainPlan extends AbstractPlan {

  private final AbstractPlan plan;
  private final ExplainMode mode;

  private final ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  /** Constructor. */
  public ExplainPlan(
      QueryId queryId,
      QueryType queryType,
      AbstractPlan plan,
      ExplainMode mode,
      ResponseListener<ExecutionEngine.ExplainResponse> explainListener) {
    super(queryId, queryType);
    this.plan = plan;
    this.mode = mode;
    this.explainListener = explainListener;
  }

  @Override
  public void execute() {
    setHighlightThreadLocal();
    try {
      plan.explain(explainListener, mode);
    } finally {
      CalcitePlanContext.clearHighlightConfig();
    }
  }

  private void setHighlightThreadLocal() {
    Map<String, Object> config = getHighlightConfig();
    if (config != null) {
      CalcitePlanContext.setHighlightConfig(config);
    }
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, ExplainMode mode) {
    throw new UnsupportedOperationException("explain query can not been explained.");
  }
}
