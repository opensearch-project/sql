/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;

/**
 * ONLY USED FOR TEST PURPOSE.
 *
 * <p>Execute {@link AbstractPlan} on caller thread.
 */
public class ExecuteOnCallerThreadQueryManager implements QueryManager {
  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    queryPlan.execute();
    return queryPlan.getQueryId();
  }
}
