/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import net.jqwik.api.*;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
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
import org.opensearch.transport.client.node.NodeClient;

/**
 * Property-based test for malicious dialect parameter sanitization.
 *
 * <p>Validates: Requirements 10.3
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class RestSQLQueryActionMaliciousDialectSanitizationPropertyTest {

  // -------------------------------------------------------------------------
  // Property 22: Malicious dialect parameter sanitization
  // -------------------------------------------------------------------------

  /**
   * Property 22: Malicious dialect parameter sanitization — For any dialect parameter string
   * containing control characters (U+0000–U+001F), non-ASCII characters, or strings longer than 64
   * characters, the HTTP 400 error response body SHALL NOT contain the raw unsanitized input.
   *
   * <p>Validates: Requirements 10.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 22: Malicious dialect parameter sanitization")
  void maliciousDialectParamIsNotReflectedInResponse(
      @ForAll("maliciousDialectParams") String maliciousDialect) throws Exception {
    TestHarness harness = new TestHarness();
    BytesRestResponse response = harness.executeDialectQuery(maliciousDialect, "SELECT 1");

    assertNotNull(response, "Should have captured a response");

    // Status must be 400
    assertEquals(
        RestStatus.BAD_REQUEST,
        response.status(),
        "Malicious dialect param should return HTTP 400");

    String content = response.content().utf8ToString();

    // The raw unsanitized input must NOT appear in the response body
    assertFalse(
        content.contains(maliciousDialect),
        "Response body must NOT contain the raw unsanitized input. "
            + "Raw input: "
            + escapeForMessage(maliciousDialect)
            + ", Response: "
            + content);

    // Verify no control characters (U+0000–U+001F) appear in the response body
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (c >= '\u0000' && c <= '\u001F' && c != '\n' && c != '\r' && c != '\t') {
        fail(
            "Response body contains control character U+"
                + String.format("%04X", (int) c)
                + " at position "
                + i);
      }
    }

    // Verify no non-ASCII characters from the input leak into the response
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (c >= '\u007F' && c <= '\u00FF') {
        fail(
            "Response body contains non-ASCII character U+"
                + String.format("%04X", (int) c)
                + " at position "
                + i);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> maliciousDialectParams() {
    // Generate strings that contain control characters, non-ASCII chars, or are overly long.
    Arbitrary<String> withControlChars = stringsWithControlCharacters();
    Arbitrary<String> withNonAscii = stringsWithNonAsciiCharacters();
    Arbitrary<String> overlyLong = overlyLongStrings();
    Arbitrary<String> mixed = mixedMaliciousStrings();

    return Arbitraries.oneOf(withControlChars, withNonAscii, overlyLong, mixed);
  }

  /** Strings containing control characters (U+0000–U+001F). */
  private Arbitrary<String> stringsWithControlCharacters() {
    Arbitrary<Character> controlChar =
        Arbitraries.chars().range('\u0000', '\u001F');
    Arbitrary<String> prefix =
        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10);
    Arbitrary<String> suffix =
        Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(10);

    return Combinators.combine(prefix, controlChar, suffix)
        .as((p, c, s) -> p + c + s);
  }

  /** Strings containing non-ASCII characters (U+007F–U+00FF). */
  private Arbitrary<String> stringsWithNonAsciiCharacters() {
    Arbitrary<Character> nonAsciiChar =
        Arbitraries.chars().range('\u007F', '\u00FF');
    Arbitrary<String> prefix =
        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10);
    Arbitrary<String> suffix =
        Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(10);

    return Combinators.combine(prefix, nonAsciiChar, suffix)
        .as((p, c, s) -> p + c + s);
  }

  /** Strings longer than 64 characters. */
  private Arbitrary<String> overlyLongStrings() {
    return Arbitraries.strings()
        .withCharRange('a', 'z')
        .withCharRange('0', '9')
        .ofMinLength(65)
        .ofMaxLength(200);
  }

  /** Mixed strings combining control chars, non-ASCII, and length. */
  private Arbitrary<String> mixedMaliciousStrings() {
    return Arbitraries.strings()
        .withCharRange('\u0000', '\u00FF')
        .ofMinLength(1)
        .ofMaxLength(150)
        .filter(s -> hasMaliciousContent(s));
  }

  /** Check if a string has at least one malicious characteristic. */
  private boolean hasMaliciousContent(String s) {
    if (s.length() > 64) return true;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c >= '\u0000' && c <= '\u001F') return true;
      if (c >= '\u007F' && c <= '\u00FF') return true;
    }
    return false;
  }

  /** Escape non-printable characters for assertion messages. */
  private String escapeForMessage(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < Math.min(s.length(), 80); i++) {
      char c = s.charAt(i);
      if (c >= 0x20 && c < 0x7F) {
        sb.append(c);
      } else {
        sb.append(String.format("\\u%04X", (int) c));
      }
    }
    if (s.length() > 80) {
      sb.append("...(len=").append(s.length()).append(")");
    }
    return sb.toString();
  }

  // -------------------------------------------------------------------------
  // Test Harness
  // -------------------------------------------------------------------------

  private static class TestHarness extends BaseRestHandler {
    private final Injector injector;

    TestHarness() {
      DialectRegistry dialectRegistry = new DialectRegistry();
      dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);
      dialectRegistry.freeze();

      Settings settings = Mockito.mock(Settings.class);
      when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

      QueryManager queryManager = Mockito.mock(QueryManager.class);
      QueryPlanFactory factory = Mockito.mock(QueryPlanFactory.class);
      DataSourceService dataSourceService = Mockito.mock(DataSourceService.class);
      ExecutionEngine executionEngine = Mockito.mock(ExecutionEngine.class);

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
    }

    BytesRestResponse executeDialectQuery(String dialect, String query) throws Exception {
      SQLQueryRequest request =
          new SQLQueryRequest(
              new JSONObject("{\"query\": \"" + query.replace("\"", "\\\"") + "\"}"),
              query,
              QUERY_API_ENDPOINT,
              Map.of("dialect", dialect),
              null);

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

      RestChannelConsumer consumer =
          queryAction.prepareRequest(
              request, (channel, exception) -> {}, (channel, exception) -> {});
      consumer.accept(mockChannel);
      return capturedResponse.get();
    }

    @Override
    public String getName() {
      return "test-harness";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {
      return null;
    }
  }
}
