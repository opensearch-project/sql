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
 * Default QueryManager implementation which execute {@link AbstractPlan} on caller thread.
 */
public class DefaultQueryManager implements QueryManager {

  @Override
  public QueryId submit(AbstractPlan queryExecution) {
    queryExecution.execute();

    return queryExecution.getQueryId();
  }
}
