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
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest extends BaseRestHandler {

  private NodeClient nodeClient;

  @Mock private ThreadPool threadPool;

  @Mock private QueryManager queryManager;

  @Mock private QueryPlanFactory factory;

  @Mock private RestChannel restChannel;

  private Injector injector;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
        });
    injector = modules.createInjector();
    Mockito.lenient()
        .when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void handleQueryThatCanSupport() throws Exception {
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT -123\"}"),
            "SELECT -123",
            QUERY_API_ENDPOINT,
            "jdbc");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    queryAction
        .prepareRequest(
            request,
            (channel, exception) -> {
              fail();
            },
            (channel, exception) -> {
              fail();
            })
        .accept(restChannel);
  }

  @Test
  public void handleExplainThatCanSupport() throws Exception {
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT -123\"}"),
            "SELECT -123",
            EXPLAIN_API_ENDPOINT,
            "standard");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    queryAction
        .prepareRequest(
            request,
            (channel, exception) -> {
              fail();
            },
            (channel, exception) -> {
              fail();
            })
        .accept(restChannel);
  }

  @Test
  public void queryThatNotSupportIsHandledByFallbackHandler() throws Exception {
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject(
                "{\"query\": \"SELECT name FROM test1 JOIN test2 ON test1.name = test2.name\"}"),
            "SELECT name FROM test1 JOIN test2 ON test1.name = test2.name",
            QUERY_API_ENDPOINT,
            "jdbc");

    AtomicBoolean fallback = new AtomicBoolean(false);
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    queryAction
        .prepareRequest(
            request,
            (channel, exception) -> {
              fallback.set(true);
              assertTrue(exception instanceof SyntaxCheckException);
            },
            (channel, exception) -> {
              fail();
            })
        .accept(restChannel);

    assertTrue(fallback.get());
  }

  @Test
  public void queryExecutionFailedIsHandledByExecutionErrorHandler() throws Exception {
    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT -123\"}"),
            "SELECT -123",
            QUERY_API_ENDPOINT,
            "jdbc");

    doThrow(new IllegalStateException("execution exception")).when(queryManager).submit(any());

    AtomicBoolean executionErrorHandler = new AtomicBoolean(false);
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    queryAction
        .prepareRequest(
            request,
            (channel, exception) -> {
              assertTrue(exception instanceof SyntaxCheckException);
            },
            (channel, exception) -> {
              executionErrorHandler.set(true);
              assertTrue(exception instanceof IllegalStateException);
            })
        .accept(restChannel);

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
