/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import org.opensearch.sql.executor.execution.AbstractPlan;

/**
 * QueryManager is the high-level interface of core engine.
 * Frontend submit {@link AbstractPlan} to QueryManager.
 */
public interface QueryManager {

  /**
   * Submit {@link AbstractPlan}.
   * @param queryPlan {@link AbstractPlan}.
   * @return {@link QueryId}.
   */
  QueryId submit(AbstractPlan queryPlan);

  /**
   * Cancel submitted {@link AbstractPlan} by {@link QueryId}.
   *
   * @return true indicate successful.
   */
  default boolean cancel(QueryId queryId) {
    throw new UnsupportedOperationException();
  }
}
