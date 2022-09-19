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
 * Default QueryManager implementation. Query will be stated {@link QueryExecution} on
 * caller thread.
 */
public class DefaultQueryManager implements QueryManager {

  @Override
  public QueryId submitQuery(QueryExecution queryExecution, ResponseListener<?> listener) {
    // 1. register execution listener.
    queryExecution.registerListener(listener);

    // 2. start query execution.
    queryExecution.start();

    return queryExecution.getQueryId();
  }
}
