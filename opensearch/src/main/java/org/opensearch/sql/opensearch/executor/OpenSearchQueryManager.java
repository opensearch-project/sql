/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/** QueryManager implemented in OpenSearch cluster. */
@RequiredArgsConstructor
public class OpenSearchQueryManager implements QueryManager {

  private final NodeClient nodeClient;

  private static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    schedule(nodeClient, () -> queryPlan.execute());

    return queryPlan.getQueryId();
  }

  private void schedule(NodeClient client, Runnable task) {
    ThreadPool threadPool = client.threadPool();
    threadPool.schedule(withCurrentContext(task), new TimeValue(0), SQL_WORKER_THREAD_POOL_NAME);
  }

  private Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
