/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertFalse;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;

/**
 * A test suite that verifies fallback behaviour of cursor queries.
 */
@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionCursorFallbackTest extends BaseRestHandler {

  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Mock
  private QueryManager queryManager;

  @Mock
  private QueryPlanFactory factory;

  @Mock
  private RestChannel restChannel;

  private Injector injector;

  @Before
  public void setup() {
    nodeClient = new NodeClient(org.opensearch.common.settings.Settings.EMPTY, threadPool);
    ModulesBuilder modules = new ModulesBuilder();
    modules.add(b -> {
      b.bind(SQLService.class).toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
    });
    injector = modules.createInjector();
    Mockito.lenient().when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  // Initial page request test cases

  @Test
  public void no_fallback_with_column_reference() throws Exception {
    String query = "SELECT name FROM test1";
    SQLQueryRequest request = createSqlQueryRequest(query, Optional.empty(),
        Optional.of(5));

    assertFalse(doesQueryFallback(request));
  }

  private static SQLQueryRequest createSqlQueryRequest(String query, Optional<String> cursorId,
                                                       Optional<Integer> fetchSize) throws IOException {
    var builder = XContentFactory.jsonBuilder()
        .startObject()
        .field("query").value(query);
    if (cursorId.isPresent()) {
      builder.field("cursor").value(cursorId.get());
    }

    if (fetchSize.isPresent()) {
      builder.field("fetch_size").value(fetchSize.get());
    }
    builder.endObject();
    JSONObject jsonContent = new JSONObject(Strings.toString(builder));

    return  new SQLQueryRequest(jsonContent, query, QUERY_API_ENDPOINT,
        Map.of("format", "jdbc"), cursorId.orElse(""));
  }

  boolean doesQueryFallback(SQLQueryRequest request) throws Exception {
    AtomicBoolean fallback = new AtomicBoolean(false);
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    queryAction.prepareRequest(request, (channel, exception) -> {
      fallback.set(true);
    }, (channel, exception) -> {
    }).accept(restChannel);
    return fallback.get();
  }

  @Override
  public String getName() {
    // do nothing, RestChannelConsumer is protected which required to extend BaseRestHandler
    return null;
  }

  @Override
  protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
     {
    // do nothing, RestChannelConsumer is protected which required to extend BaseRestHandler
    return null;
  }
}
