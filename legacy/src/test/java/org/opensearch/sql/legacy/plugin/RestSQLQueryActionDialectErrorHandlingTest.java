/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.DialectRegistry;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.dialect.clickhouse.ClickHouseDialectPlugin;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Unit tests for error handling in the dialect query execution path. Validates requirements 8.1,
 * 8.2, 8.3, 7.5, 4.4.
 */
@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionDialectErrorHandlingTest extends BaseRestHandler {

  @Mock private ThreadPool threadPool;
  @Mock private QueryManager queryManager;
  @Mock private QueryPlanFactory factory;
  @Mock private Settings settings;
  @Mock private DataSourceService dataSourceService;
  @Mock private ExecutionEngine executionEngine;

  private DialectRegistry dialectRegistry;
  private Injector injector;

  @Before
  public void setup() {
    dialectRegistry = new DialectRegistry();
    dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);

    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
          b.bind(Settings.class).toInstance(settings);
          b.bind(DialectRegistry.class).toInstance(dialectRegistry);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
          b.bind(ExecutionEngine.class).toInstance(executionEngine);
        });
    injector = modules.createInjector();

    Mockito.lenient()
        .when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  /**
   * Test that a syntax error in a dialect query returns 400 with position info. Validates
   * Requirement 4.4: error message includes approximate position of the error.
   */
  @Test
  public void parseErrorReturns400WithPositionInfo() throws Exception {
    // A query with a syntax error — missing FROM clause after SELECT columns
    SQLQueryRequest request = createDialectRequest("SELECT * FORM my_table");

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    assertTrue("Response should contain 'SQL parse error'", content.contains("SQL parse error"));
    // Calcite's SqlParseException includes position info like "line" and "column"
    assertTrue(
        "Response should contain position info (line/column)",
        content.toLowerCase().contains("line")
            || content.toLowerCase().contains("column")
            || content.toLowerCase().contains("pos"));
  }

  /** Test that a completely invalid SQL returns 400 with parse error. Validates Requirement 4.4. */
  @Test
  public void completelyInvalidSqlReturns400() throws Exception {
    SQLQueryRequest request = createDialectRequest("THIS IS NOT SQL AT ALL");

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    assertTrue("Response should contain 'SQL parse error'", content.contains("SQL parse error"));
  }

  /**
   * Test that the error response follows the standard JSON format. Validates that dialect errors
   * use the same format as /_plugins/_sql errors.
   */
  @Test
  public void errorResponseFollowsStandardJsonFormat() throws Exception {
    SQLQueryRequest request = createDialectRequest("INVALID SQL QUERY !!!");

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    String content = response.content().utf8ToString();
    JSONObject json = new JSONObject(content);

    // Verify standard error format: { "error": { "reason": ..., "details": ..., "type": ... },
    // "status": ... }
    assertTrue("Response should have 'error' field", json.has("error"));
    assertTrue("Response should have 'status' field", json.has("status"));
    JSONObject error = json.getJSONObject("error");
    assertTrue("Error should have 'reason' field", error.has("reason"));
    assertTrue("Error should have 'details' field", error.has("details"));
    assertTrue("Error should have 'type' field", error.has("type"));
    assertEquals(
        "Type should be DialectQueryException", "DialectQueryException", error.getString("type"));
  }

  /**
   * Test that internal errors return 500 with a generic message. Validates Requirement 8.3: generic
   * error message, no internal details exposed.
   */
  @Test
  public void internalErrorReturns500WithGenericMessage() throws Exception {
    // Use a mock plugin that throws an unexpected RuntimeException during preprocessing
    DialectPlugin failingPlugin = Mockito.mock(DialectPlugin.class);
    when(failingPlugin.dialectName()).thenReturn("failing");
    when(failingPlugin.preprocessor())
        .thenThrow(
            new RuntimeException("java.lang.NullPointerException: some.internal.Class.method"));

    DialectRegistry failingRegistry = new DialectRegistry();
    failingRegistry.register(failingPlugin);

    // Create a new injector with the failing registry
    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
          b.bind(Settings.class).toInstance(settings);
          b.bind(DialectRegistry.class).toInstance(failingRegistry);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
          b.bind(ExecutionEngine.class).toInstance(executionEngine);
        });
    Injector failingInjector = modules.createInjector();

    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT 1\"}"),
            "SELECT 1",
            QUERY_API_ENDPOINT,
            Map.of("dialect", "failing"),
            null);

    RestSQLQueryAction queryAction = new RestSQLQueryAction(failingInjector);

    BytesRestResponse response = executeAndCaptureResponseWith(queryAction, request);

    assertNotNull("Should have captured a response", response);
    assertEquals(
        "Response status should be 500", RestStatus.INTERNAL_SERVER_ERROR, response.status());
    String content = response.content().utf8ToString();

    // Verify generic message
    assertTrue(
        "Response should contain generic error message", content.contains("internal server error"));

    // Verify no Java class names, package paths, or stack traces are exposed
    assertTrue(
        "Response should NOT contain Java class names",
        !content.contains("java.lang.NullPointerException"));
    assertTrue(
        "Response should NOT contain package paths", !content.contains("some.internal.Class"));
    assertTrue("Response should NOT contain 'at ' stack trace lines", !content.contains("at org."));
  }

  /**
   * Test that the 500 response does not expose Java exception type names. Validates Requirement
   * 8.3.
   */
  @Test
  public void internalErrorDoesNotExposeExceptionClassName() throws Exception {
    DialectPlugin failingPlugin = Mockito.mock(DialectPlugin.class);
    when(failingPlugin.dialectName()).thenReturn("failing2");
    when(failingPlugin.preprocessor())
        .thenThrow(
            new IllegalStateException("Unexpected state in org.opensearch.sql.internal.SomeClass"));

    DialectRegistry failingRegistry = new DialectRegistry();
    failingRegistry.register(failingPlugin);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
          b.bind(Settings.class).toInstance(settings);
          b.bind(DialectRegistry.class).toInstance(failingRegistry);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
          b.bind(ExecutionEngine.class).toInstance(executionEngine);
        });
    Injector failingInjector = modules.createInjector();

    SQLQueryRequest request =
        new SQLQueryRequest(
            new JSONObject("{\"query\": \"SELECT 1\"}"),
            "SELECT 1",
            QUERY_API_ENDPOINT,
            Map.of("dialect", "failing2"),
            null);

    RestSQLQueryAction queryAction = new RestSQLQueryAction(failingInjector);
    BytesRestResponse response = executeAndCaptureResponseWith(queryAction, request);

    assertNotNull("Should have captured a response", response);
    assertEquals(
        "Response status should be 500", RestStatus.INTERNAL_SERVER_ERROR, response.status());
    String content = response.content().utf8ToString();

    // Should not contain the exception class name or internal package path
    assertTrue(
        "Response should NOT contain IllegalStateException",
        !content.contains("IllegalStateException"));
    assertTrue(
        "Response should NOT contain internal package path",
        !content.contains("org.opensearch.sql.internal"));
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private SQLQueryRequest createDialectRequest(String query) {
    return new SQLQueryRequest(
        new JSONObject("{\"query\": \"" + query.replace("\"", "\\\"") + "\"}"),
        query,
        QUERY_API_ENDPOINT,
        Map.of("dialect", "clickhouse"),
        null);
  }

  private BytesRestResponse executeAndCaptureResponse(SQLQueryRequest request) throws Exception {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    return executeAndCaptureResponseWith(queryAction, request);
  }

  private BytesRestResponse executeAndCaptureResponseWith(
      RestSQLQueryAction queryAction, SQLQueryRequest request) throws Exception {
    AtomicReference<BytesRestResponse> capturedResponse = new AtomicReference<>();
    RestChannel mockChannel = Mockito.mock(RestChannel.class);
    Mockito.doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(mockChannel)
        .sendResponse(Mockito.any(BytesRestResponse.class));

    BaseRestHandler.RestChannelConsumer consumer =
        queryAction.prepareRequest(
            request,
            (channel, exception) -> {
              // Fallback handler — should not be called for dialect requests
            },
            (channel, exception) -> {
              // Execution error handler — should not be called for properly handled errors
            });

    consumer.accept(mockChannel);
    return capturedResponse.get();
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    return null;
  }
}
