/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.transport.TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX;

import java.util.Map;
import java.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/** Test AsyncRestExecutor behavior. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class AsyncRestExecutorTest {

  private static final boolean NON_BLOCKING = false;

  @Mock private RestExecutor executor;

  @Mock private Client client;

  private final Map<String, String> params = emptyMap();

  @Mock private QueryAction action;

  @Mock private RestChannel channel;

  @Mock private ClusterSettings clusterSettings;

  @Before
  public void setUp() {
    when(client.threadPool()).thenReturn(mock(ThreadPool.class));
    when(action.getSqlRequest()).thenReturn(SqlRequest.NULL);
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);

    OpenSearchSettings settings = spy(new OpenSearchSettings(clusterSettings));
    doReturn(emptyList()).when(settings).getSettings();
    LocalClusterState.state().setPluginSettings(settings);
  }

  @Test
  public void executeBlockingQuery() throws Exception {
    Thread.currentThread().setName(TRANSPORT_WORKER_THREAD_NAME_PREFIX);
    execute();
    verifyRunInWorkerThread();
  }

  @Test
  public void executeBlockingQueryButNotInTransport() throws Exception {
    execute();
    verifyRunInCurrentThread();
  }

  @Test
  public void executeNonBlockingQuery() throws Exception {
    execute(anyAction -> NON_BLOCKING);
    verifyRunInCurrentThread();
  }

  private void execute() throws Exception {
    AsyncRestExecutor asyncExecutor = new AsyncRestExecutor(executor);
    asyncExecutor.execute(client, params, action, channel);
  }

  private void execute(Predicate<QueryAction> isBlocking) throws Exception {
    AsyncRestExecutor asyncExecutor = new AsyncRestExecutor(executor, isBlocking);
    asyncExecutor.execute(client, params, action, channel);
  }

  private void verifyRunInCurrentThread() {
    verify(client, never()).threadPool();
  }

  private void verifyRunInWorkerThread() {
    verify(client, times(1)).threadPool();
  }
}
