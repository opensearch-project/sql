/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.opensearch.sql.api.dialect.DialectNames;
import org.opensearch.sql.api.dialect.DialectRegistry;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.dialect.clickhouse.ClickHouseDialectPlugin;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Unit tests for dialect parameter edge cases in {@link RestSQLQueryAction}.
 * Validates Requirements 10.3 and 10.4.
 */
@RunWith(MockitoJUnitRunner.class)
public class RestSQLQueryActionDialectParamEdgeCaseTest extends BaseRestHandler {

  @Mock private ThreadPool threadPool;
  @Mock private QueryManager queryManager;
  @Mock private QueryPlanFactory factory;
  @Mock private Settings settings;

  private DialectRegistry dialectRegistry;
  private Injector injector;

  @Before
  public void setup() {
    dialectRegistry = new DialectRegistry();
    dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);
    dialectRegistry.freeze();

    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b -> {
          b.bind(SQLService.class)
              .toInstance(new SQLService(new SQLSyntaxParser(), queryManager, factory));
          b.bind(Settings.class).toInstance(settings);
          b.bind(DialectRegistry.class).toInstance(dialectRegistry);
        });
    injector = modules.createInjector();

    Mockito.lenient()
        .when(threadPool.getThreadContext())
        .thenReturn(new ThreadContext(org.opensearch.common.settings.Settings.EMPTY));
  }

  // -------------------------------------------------------------------------
  // Test: empty dialect param → 400
  // Validates Requirement 10.4
  // -------------------------------------------------------------------------

  @Test
  public void emptyDialectParamReturns400() throws Exception {
    SQLQueryRequest request = createRequestWithDialect("");

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    assertTrue(
        "Response should indicate dialect must be non-empty",
        content.contains("non-empty"));
  }

  // -------------------------------------------------------------------------
  // Test: excessively long string → 400 (truncated, sanitized)
  // Validates Requirement 10.3
  // -------------------------------------------------------------------------

  @Test
  public void excessivelyLongDialectParamReturns400() throws Exception {
    // Build a string longer than 64 chars (the sanitization truncation limit)
    String longDialect = "a".repeat(200);
    SQLQueryRequest request = createRequestWithDialect(longDialect);

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    // The full 200-char string should NOT appear in the response (it was truncated)
    assertFalse(
        "Response should not contain the full 200-char input",
        content.contains(longDialect));
    // The response should be a structured UNKNOWN_DIALECT error since the truncated
    // string won't match any registered dialect
    assertTrue(
        "Response should indicate unknown dialect",
        content.contains("UNKNOWN_DIALECT") || content.contains("Unknown SQL dialect"));
  }

  @Test
  public void longDialectParamIsTruncatedTo64Chars() {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    String longInput = "x".repeat(100);
    String sanitized = queryAction.sanitizeDialectParam(longInput);
    assertEquals("Sanitized output should be at most 64 chars", 64, sanitized.length());
  }

  // -------------------------------------------------------------------------
  // Test: control characters → 400 (sanitized, not reflected)
  // Validates Requirement 10.3
  // -------------------------------------------------------------------------

  @Test
  public void controlCharactersInDialectParamReturns400() throws Exception {
    // Dialect param with control characters embedded
    String malicious = "click\u0000house\u001b[31m";
    SQLQueryRequest request = createRequestWithDialect(malicious);

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    // The raw control characters should NOT appear in the response
    assertFalse(
        "Response should not contain null byte",
        content.contains("\u0000"));
    assertFalse(
        "Response should not contain escape sequence",
        content.contains("\u001b"));
  }

  @Test
  public void onlyControlCharactersDialectParamReturns400AsEmpty() throws Exception {
    // A dialect param that is entirely control characters → sanitizes to empty
    String allControl = "\u0001\u0002\u0003\u0004";
    SQLQueryRequest request = createRequestWithDialect(allControl);

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    assertEquals("Response status should be 400", RestStatus.BAD_REQUEST, response.status());
    String content = response.content().utf8ToString();
    assertTrue(
        "Response should indicate dialect must be non-empty",
        content.contains("non-empty"));
  }

  @Test
  public void sanitizeDialectParamStripsControlCharacters() {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    String withControl = "click\u0000house\u001f";
    String sanitized = queryAction.sanitizeDialectParam(withControl);
    assertEquals("Control chars should be stripped", DialectNames.CLICKHOUSE, sanitized);
  }

  @Test
  public void sanitizeDialectParamStripsNonAscii() {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    String withNonAscii = "click\u0080house\u00ff";
    String sanitized = queryAction.sanitizeDialectParam(withNonAscii);
    assertEquals("Non-ASCII chars should be stripped", DialectNames.CLICKHOUSE, sanitized);
  }

  // -------------------------------------------------------------------------
  // Test: valid dialect after sanitization still routes correctly
  // Validates Requirement 10.3
  // -------------------------------------------------------------------------

  @Test
  public void validDialectAfterSanitizationRoutesCorrectly() throws Exception {
    // "clickhouse" with some leading/trailing whitespace — should still route
    SQLQueryRequest request = createRequestWithDialect("  clickhouse  ");

    BytesRestResponse response = executeAndCaptureResponse(request);

    assertNotNull("Should have captured a response", response);
    // The dialect pipeline will be entered. Since we don't have full Calcite
    // infrastructure wired, it will produce a 500 (internal error from execution),
    // NOT a 400 (dialect validation error). This confirms routing succeeded.
    assertTrue(
        "Response should NOT be a dialect validation error (400 with UNKNOWN_DIALECT)",
        !response.content().utf8ToString().contains("UNKNOWN_DIALECT"));
  }

  @Test
  public void sanitizeDialectParamTrimsWhitespace() {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    String withSpaces = "  clickhouse  ";
    String sanitized = queryAction.sanitizeDialectParam(withSpaces);
    assertEquals("Whitespace should be trimmed", DialectNames.CLICKHOUSE, sanitized);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private SQLQueryRequest createRequestWithDialect(String dialect) {
    return new SQLQueryRequest(
        new JSONObject("{\"query\": \"SELECT 1\"}"),
        "SELECT 1",
        QUERY_API_ENDPOINT,
        Map.of("dialect", dialect),
        null);
  }

  private BytesRestResponse executeAndCaptureResponse(SQLQueryRequest request) throws Exception {
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);

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
            (channel, exception) -> {},
            (channel, exception) -> {});

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
