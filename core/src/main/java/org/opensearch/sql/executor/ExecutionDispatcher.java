/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;

/**
 * Dispatches query execution to an appropriate thread pool based on plan characteristics. After
 * query analysis and optimization, the dispatcher inspects the plan and routes execution to either
 * the fast worker pool (for queries fully pushed to OpenSearch) or the slow worker pool (for
 * queries requiring scripts/table scans).
 */
public interface ExecutionDispatcher {

  /**
   * Dispatch execution of the given plan.
   *
   * @param plan the optimized Calcite plan
   * @param context the plan context
   * @param listener response listener for query results
   * @param engine the execution engine to invoke
   */
  void dispatch(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ExecutionEngine engine);
}
