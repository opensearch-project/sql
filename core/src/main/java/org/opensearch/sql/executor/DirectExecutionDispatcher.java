/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;

/**
 * Default no-op dispatcher that executes inline on the current thread. Used when slow-pool routing
 * is disabled or as a fallback.
 */
public class DirectExecutionDispatcher implements ExecutionDispatcher {

  @Override
  public void dispatch(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ExecutionEngine engine) {
    engine.execute(plan, context, listener);
  }
}
