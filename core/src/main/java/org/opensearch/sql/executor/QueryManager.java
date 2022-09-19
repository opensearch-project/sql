/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.execution.QueryExecution;

/**
 * QueryManager is the high-level interface of core engine.
 * Frontend submit {@link QueryExecution} to QueryManager.
 */
public interface QueryManager {

  /**
   * Submit {@link QueryExecution}.
   * @param queryExecution {@link QueryExecution}.
   * @param listener {@link ResponseListener}.
   * @return {@link QueryId}.
   */
  QueryId submitQuery(QueryExecution queryExecution, ResponseListener<?> listener);
}
