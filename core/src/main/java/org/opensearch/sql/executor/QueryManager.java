/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.executor.execution.AbstractPlan;

/**
 * QueryManager is the high-level interface of core engine. Frontend submit an {@link AbstractPlan}
 * to QueryManager.
 */
public interface QueryManager {

  /**
   * Submit {@link AbstractPlan}.
   *
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
