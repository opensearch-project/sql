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
import java.util.Set;
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
import org.opensearch.sql.api.dialect.DialectNames;
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
 * Property-based test for structured dialect validation error responses.
 *
 * <p>Validates: Requirements 10.1, 10.2
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class RestSQLQueryActionStructuredDialectErrorPropertyTest {

  // Known registered dialect names to exclude from generation
  private static final Set<String> REGISTERED_DIALECTS = Set.of(DialectNames.CLICKHOUSE);

  // -------------------------------------------------------------------------
  // Property 21: Structured dialect validation error response
  // -------------------------------------------------------------------------

  /**
   * Property 21: Structured dialect validation error response — For any string that is not a
   * registered dialect name, the HTTP response SHALL have status 400 and the JSON body SHALL
   * contain {@code error_type}, {@code message}, and {@code dialect_requested} fields, where
   * {@code message} includes the list of supported dialects.
   *
   * <p>Validates: Requirements 10.1, 10.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 21: Structured dialect validation error response")
  void unknownDialectReturnsStructuredErrorWithAllFields(
      @ForAll("unregisteredDialectNames") String unknownDialect) throws Exception {
    TestHarness harness = new TestHarness();
    BytesRestResponse response = harness.executeDialectQuery(unknownDialect, "SELECT 1");

    assertNotNull(response, "Should have captured a response");

    // Status must be 400
    assertEquals(
        RestStatus.BAD_REQUEST,
        response.status(),
        "Unknown dialect '" + unknownDialect + "' should return HTTP 400");

    String content = response.content().utf8ToString();

    // Parse as JSON — must be valid JSON
    JSONObject json;
    try {
      json = new JSONObject(content);
    } catch (Exception e) {
      fail("Response body must be valid JSON. Content: " + content);
      return;
    }

    // Must contain error_type field
    assertTrue(
        json.has("error_type"),
        "JSON body must contain 'error_type' field. Content: " + content);
    assertEquals(
        "UNKNOWN_DIALECT",
        json.getString("error_type"),
        "error_type should be 'UNKNOWN_DIALECT'");

    // Must contain message field
    assertTrue(
        json.has("message"), "JSON body must contain 'message' field. Content: " + content);
    String message = json.getString("message");

    // Message must include the list of supported dialects
    for (String registeredDialect : REGISTERED_DIALECTS) {
      assertTrue(
          message.contains(registeredDialect),
          "Message should list supported dialect '"
              + registeredDialect
              + "'. Message: "
              + message);
    }

    // Must contain dialect_requested field
    assertTrue(
        json.has("dialect_requested"),
        "JSON body must contain 'dialect_requested' field. Content: " + content);
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> unregisteredDialectNames() {
    // Generate strings that are NOT registered dialect names.
    // Mix of plausible misspellings, random strings, and edge cases.
    Arbitrary<String> misspellings =
        Arbitraries.of(
            "clickhous",
            "clickhousee",
            "ClickHouse",
            "CLICKHOUSE",
            "click_house",
            "click-house",
            "clckhouse",
            "clikhouse");

    Arbitrary<String> otherDialects =
        Arbitraries.of(
            "mysql", "postgres", "presto", "trino", "spark", "hive", "sqlite", "oracle", "mssql");

    Arbitrary<String> randomAlpha =
        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30).filter(this::isNotRegistered);

    Arbitrary<String> randomAlphaNumeric =
        Arbitraries.strings()
            .withCharRange('a', 'z')
            .withCharRange('0', '9')
            .ofMinLength(1)
            .ofMaxLength(20)
            .filter(this::isNotRegistered);

    return Arbitraries.oneOf(misspellings, otherDialects, randomAlpha, randomAlphaNumeric);
  }

  private boolean isNotRegistered(String name) {
    return !REGISTERED_DIALECTS.contains(name.toLowerCase());
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
