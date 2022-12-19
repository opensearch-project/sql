/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.config.SQLServiceConfig;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.threadpool.ThreadPool;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest extends BaseRestHandler {

  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Mock
  private QueryManager queryManager;

  @Mock
  private QueryPlanFactory factory;

  @Mock
  private ExecutionEngine.Schema schema;

  @Mock
  private RestChannel restChannel;

  private AnnotationConfigApplicationContext context;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    context = new AnnotationConfigApplicationContext();
    context.registerBean(StorageEngine.class, () -> Mockito.mock(StorageEngine.class));
    context.registerBean(ExecutionEngine.class, () -> Mockito.mock(ExecutionEngine.class));
    context.registerBean(DataSourceService.class, () -> Mockito.mock(DataSourceService.class));
    context.registerBean(QueryManager.class, () -> queryManager);
    context.registerBean(QueryPlanFactory.class, () -> factory);
    context.register(SQLServiceConfig.class);
    context.refresh();
    Mockito.lenient().when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void handleQueryThatCanSupport() throws Exception {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    queryAction.prepareRequest(request, (channel, exception) -> {
      fail();
    }, (channel, exception) -> {
      fail();
    }).accept(restChannel);
  }

  @Test
  public void handleExplainThatCanSupport() throws Exception {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        EXPLAIN_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    queryAction.prepareRequest(request, (channel, exception) -> {
      fail();
    }, (channel, exception) -> {
      fail();
    }).accept(restChannel);
  }

  @Test
  public void queryThatNotSupportIsHandledByFallbackHandler() throws Exception {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject(
            "{\"query\": \"SELECT name FROM test1 JOIN test2 ON test1.name = test2.name\"}"),
        "SELECT name FROM test1 JOIN test2 ON test1.name = test2.name",
        QUERY_API_ENDPOINT,
        "");

    AtomicBoolean fallback = new AtomicBoolean(false);
    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    queryAction.prepareRequest(request, (channel, exception) -> {
      fallback.set(true);
      assertTrue(exception instanceof SyntaxCheckException);
    }, (channel, exception) -> {
      fail();
    }).accept(restChannel);

    assertTrue(fallback.get());
  }

  @Test
  public void queryExecutionFailedIsHandledByExecutionErrorHandler() throws Exception {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject(
            "{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    doThrow(new IllegalStateException("execution exception"))
        .when(queryManager)
        .submit(any());

    AtomicBoolean executionErrorHandler = new AtomicBoolean(false);
    RestSQLQueryAction queryAction = new RestSQLQueryAction(context);
    queryAction.prepareRequest(request, (channel, exception) -> {
      assertTrue(exception instanceof SyntaxCheckException);
    }, (channel, exception) -> {
      executionErrorHandler.set(true);
      assertTrue(exception instanceof IllegalStateException);
    }).accept(restChannel);

    assertTrue(executionErrorHandler.get());
  }

  @Override
  public String getName() {
    // do nothing, RestChannelConsumer is protected which required to extend BaseRestHandler
    return null;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    // do nothing, RestChannelConsumer is protected which required to extend BaseRestHandler
    return null;
  }
}
