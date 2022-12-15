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
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;

@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionTest extends BaseRestHandler {

  @Mock
  private QueryManager queryManager;

  @Mock
  private QueryPlanFactory factory;

  @Mock
  private RestChannel restChannel;

  private SQLService sqlService;

  @Before
  public void setup() {
    sqlService = new SQLService(new SQLSyntaxParser(), queryManager, factory);
  }

  @Test
  public void handleQueryThatCanSupport() throws Exception {
    SQLQueryRequest request = new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT -123\"}"),
        "SELECT -123",
        QUERY_API_ENDPOINT,
        "");

    RestSQLQueryAction queryAction = new RestSQLQueryAction(sqlService);
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

    RestSQLQueryAction queryAction = new RestSQLQueryAction(sqlService);
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
    RestSQLQueryAction queryAction = new RestSQLQueryAction(sqlService);
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
    RestSQLQueryAction queryAction = new RestSQLQueryAction(sqlService);
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
