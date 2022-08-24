/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSQLQueryAction.NOT_SUPPORTED_YET;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest {

  @Mock
  private ClusterService clusterService;

  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Mock
  private Settings settings;

  @Mock
  private CatalogService catalogService;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void handleQueryThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings, catalogService);
    assertNotSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

  @Test
  public void handleExplainThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        EXPLAIN_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings, catalogService);
    assertNotSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

  @Test
  public void skipQueryThatNotSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject(
            "{\"query\": \"SELECT name FROM test1 JOIN test2 ON test1.name = test2.name\"}"),
        "SELECT name FROM test1 JOIN test2 ON test1.name = test2.name",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(clusterService, settings, catalogService);
    assertSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

}
