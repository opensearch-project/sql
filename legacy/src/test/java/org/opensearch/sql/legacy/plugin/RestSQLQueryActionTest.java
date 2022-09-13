/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.opensearch.sql.legacy.plugin.RestSQLQueryAction.NOT_SUPPORTED_YET;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.sql.config.SQLServiceConfig;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.threadpool.ThreadPool;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest {

  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  private AnnotationConfigApplicationContext context;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    context = new AnnotationConfigApplicationContext();
    context.registerBean(StorageEngine.class, () -> Mockito.mock(StorageEngine.class));
    context.registerBean(ExecutionEngine.class, () -> Mockito.mock(ExecutionEngine.class));
    context.registerBean(CatalogService.class, () -> Mockito.mock(CatalogService.class));
    context.register(SQLServiceConfig.class);
    context.refresh();
    Mockito.lenient().when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void handleQueryThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    assertNotSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

  @Test
  public void handleExplainThatCanSupport() {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        EXPLAIN_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
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

    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    assertSame(NOT_SUPPORTED_YET, queryAction.prepareRequest(request, nodeClient));
  }

}
