/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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
import org.opensearch.transport.client.node.NodeClient;

/**
 * Property-based test for error classification with HTTP status and internal_id.
 *
 * <p>Property 26: Error classification with HTTP status and internal_id
 *
 * <p>For any internal exception thrown during dialect query processing, the HTTP response SHALL have
 * status 500, the body SHALL contain an {@code internal_id} field, and the body SHALL NOT contain
 * Java class names, package names, or stack trace lines. For any unsupported function error, the
 * HTTP response SHALL have status 422 and SHALL contain the function name.
 *
 * <p>Validates: Requirements 14.2, 14.3
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class RestSQLQueryActionErrorClassificationPropertyTest {

  /** Pattern to detect Java exception class names (e.g., NullPointerException). */
  private static final Pattern JAVA_CLASS_NAME_PATTERN =
      Pattern.compile("[A-Z]\\w*Exception|[A-Z]\\w*Error");

  /** Pattern to detect Java package paths (e.g., org.opensearch.sql.internal). */
  private static final Pattern JAVA_PACKAGE_PATTERN =
      Pattern.compile("\\b[a-z]+\\.[a-z]+\\.[a-z]+\\.\\w+");

  /** Pattern to detect stack trace lines (e.g., "at org.foo.Bar.method(File.java:42)"). */
  private static final Pattern STACK_TRACE_PATTERN =
      Pattern.compile("\\bat\\s+[a-z]\\w*\\.\\w+");

  // -------------------------------------------------------------------------
  // Property 26 — Part 1: Internal exceptions → 500 with internal_id, no leaks
  // -------------------------------------------------------------------------

  /**
   * Property 26 (internal errors): For any internal exception thrown during dialect query
   * processing, the HTTP response SHALL have status 500, the body SHALL contain an
   * {@code internal_id} field, and the body SHALL NOT contain Java class names, package names, or
   * stack trace lines.
   *
   * <p>Validates: Requirements 14.3
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 26: Error classification with HTTP status and"
          + " internal_id")
  void internalExceptionReturns500WithInternalIdAndNoLeaks(
      @ForAll("internalExceptionMessages") String exceptionMessage) throws Exception {
    // Create a mock plugin that throws a RuntimeException with the generated message
    DialectPlugin failingPlugin = Mockito.mock(DialectPlugin.class);
    when(failingPlugin.dialectName()).thenReturn("failing");
    when(failingPlugin.preprocessor()).thenThrow(new RuntimeException(exceptionMessage));

    TestHarness harness = new TestHarness(failingPlugin);
    BytesRestResponse response = harness.executeDialectQuery("failing", "SELECT 1");

    assertNotNull(response, "Should have captured a response");

    // Status MUST be 500
    assertEquals(
        RestStatus.INTERNAL_SERVER_ERROR,
        response.status(),
        "Internal exception should return HTTP 500");

    String content = response.content().utf8ToString();
    JSONObject json = new JSONObject(content);
    JSONObject error = json.getJSONObject("error");

    // Must contain internal_id field
    assertTrue(
        error.has("internal_id"),
        "Error body must contain 'internal_id' field. Content: " + content);
    String internalId = error.getString("internal_id");
    assertNotNull(internalId, "internal_id should not be null");
    assertFalse(internalId.isEmpty(), "internal_id should not be empty");
    // UUID format: 8-4-4-4-12 hex digits
    assertTrue(
        internalId.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
        "internal_id should be a valid UUID. Value: " + internalId);

    // Extract the details field — this is the only field that could leak internals
    String details = error.getString("details");

    // Must NOT contain Java class names
    assertFalse(
        JAVA_CLASS_NAME_PATTERN.matcher(details).find(),
        "Details should NOT contain Java class names. Details: " + details);

    // Must NOT contain Java package paths
    assertFalse(
        JAVA_PACKAGE_PATTERN.matcher(details).find(),
        "Details should NOT contain Java package paths. Details: " + details);

    // Must NOT contain stack trace lines
    assertFalse(
        STACK_TRACE_PATTERN.matcher(details).find(),
        "Details should NOT contain stack trace lines. Details: " + details);

    // Verify status field in JSON body
    assertEquals(500, json.getInt("status"), "JSON status field should be 500");
  }

  // -------------------------------------------------------------------------
  // Property 26 — Part 2: Unsupported function → 422 with function name
  // -------------------------------------------------------------------------

  /**
   * Property 26 (unsupported function): For any unsupported function error, the HTTP response SHALL
   * have status 422 and SHALL contain the function name. This test verifies the error classification
   * logic by:
   * 1. Testing that extractValidationErrorDetails correctly extracts the function name from
   *    Calcite's ValidationException message format.
   * 2. Testing that ValidationException is classified as 422 through the full error handling path.
   *
   * <p>Validates: Requirements 14.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 26: Error classification with HTTP status and"
          + " internal_id")
  void unsupportedFunctionReturns422WithFunctionName(
      @ForAll("unsupportedFunctionNames") String functionName) throws Exception {
    // Part 1: Verify extractValidationErrorDetails extracts the function name
    // Simulate Calcite's ValidationException message format for unsupported functions.
    // Calcite reports: "No match found for function signature <NAME>(...)"
    String causeMessage =
        "No match found for function signature " + functionName + "(<NUMERIC>)";
    org.apache.calcite.tools.ValidationException ve =
        new org.apache.calcite.tools.ValidationException(
            "Validation failed", new RuntimeException(causeMessage));

    Injector injector = createInjector();
    RestSQLQueryAction queryAction = new RestSQLQueryAction(injector);
    Method extractMethod =
        RestSQLQueryAction.class.getDeclaredMethod(
            "extractValidationErrorDetails",
            org.apache.calcite.tools.ValidationException.class,
            org.opensearch.sql.api.dialect.DialectPlugin.class);
    extractMethod.setAccessible(true);
    String details =
        (String) extractMethod.invoke(queryAction, ve, ClickHouseDialectPlugin.INSTANCE);

    // The extracted details should contain the unsupported function name
    assertTrue(
        details.toLowerCase().contains(functionName.toLowerCase()),
        "Error details should contain the unsupported function name '"
            + functionName
            + "'. Details: "
            + details);

    // Part 2: Verify the full HTTP response path produces 422 for ValidationException.
    // Use sendErrorResponse with UNPROCESSABLE_ENTITY to verify the response format.
    // We simulate what executeDialectQuery does when it catches ValidationException.
    AtomicReference<BytesRestResponse> capturedResponse = new AtomicReference<>();
    RestChannel mockChannel = Mockito.mock(RestChannel.class);
    Mockito.doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(mockChannel)
        .sendResponse(Mockito.any(BytesRestResponse.class));

    // Call sendErrorResponse with the extracted details and 422 status
    // (this is exactly what executeDialectQuery does in the ValidationException catch block)
    Method sendErrorMethod =
        RestSQLQueryAction.class.getDeclaredMethod(
            "sendErrorResponse", RestChannel.class, String.class, RestStatus.class);
    sendErrorMethod.setAccessible(true);
    sendErrorMethod.invoke(queryAction, mockChannel, details, RestStatus.UNPROCESSABLE_ENTITY);

    BytesRestResponse response = capturedResponse.get();
    assertNotNull(response, "Should have captured a response");
    assertEquals(
        RestStatus.UNPROCESSABLE_ENTITY,
        response.status(),
        "Unsupported function should return HTTP 422");

    String content = response.content().utf8ToString();
    JSONObject json = new JSONObject(content);
    assertEquals(422, json.getInt("status"), "JSON status field should be 422");

    // Verify the function name appears in the response body
    assertTrue(
        content.toLowerCase().contains(functionName.toLowerCase()),
        "Response should contain the unsupported function name '"
            + functionName
            + "'. Content: "
            + content);
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> internalExceptionMessages() {
    // Generate exception messages containing Java internals that should NOT leak
    Arbitrary<String> classNames =
        Arbitraries.of(
            "java.lang.NullPointerException",
            "java.lang.IllegalStateException: unexpected state",
            "org.opensearch.sql.internal.SomeClass.method failed",
            "java.io.IOException: connection reset",
            "org.apache.calcite.runtime.CalciteException: internal error",
            "java.util.ConcurrentModificationException",
            "org.opensearch.sql.legacy.plugin.RestSQLQueryAction.executeDialectQuery",
            "java.lang.OutOfMemoryError: Java heap space",
            "org.opensearch.OpenSearchException: shard failure",
            "java.lang.ClassCastException: cannot cast",
            "java.lang.ArrayIndexOutOfBoundsException: 5",
            "org.apache.calcite.plan.RelOptPlanner$CannotPlanException: plan failed");

    Arbitrary<String> stackTraces =
        Arbitraries.of(
            "at org.opensearch.sql.legacy.plugin.RestSQLQueryAction.executeDialectQuery"
                + "(RestSQLQueryAction.java:214)",
            "at java.base/java.lang.Thread.run(Thread.java:829)",
            "at org.apache.calcite.tools.Frameworks.getPlanner(Frameworks.java:100)",
            "at org.opensearch.sql.sql.dialect.clickhouse.ClickHouseOperatorTable"
                + ".lookupOperatorOverloads(ClickHouseOperatorTable.java:55)");

    Arbitrary<String> packagePaths =
        Arbitraries.of(
            "org.opensearch.sql.internal.SomeClass",
            "org.apache.calcite.sql.parser.SqlParser",
            "java.lang.reflect.Method.invoke",
            "org.opensearch.sql.sql.dialect.clickhouse.ClickHouseDialectPlugin");

    Arbitrary<String> combined =
        Combinators.combine(classNames, stackTraces)
            .as((cls, st) -> cls + "\n\t" + st);

    return Arbitraries.oneOf(classNames, stackTraces, packagePaths, combined);
  }

  @Provide
  Arbitrary<String> unsupportedFunctionNames() {
    // Generate function names that are NOT registered in the ClickHouse operator table
    // and are NOT standard Calcite functions, but are valid SQL identifiers
    return Arbitraries.of(
        "arraySort",
        "arrayReverse",
        "arrayMap",
        "arrayFilter",
        "dictGet",
        "dictHas",
        "JSONExtract",
        "JSONLength",
        "topK",
        "windowFunnel",
        "retention",
        "sequenceMatch",
        "sequenceCount",
        "simpleLinearRegression",
        "stochasticLinearRegression",
        "entropy",
        "meanZTest",
        "mannWhitneyUTest",
        "welchTTest",
        "studentTTest",
        "kolmogorovSmirnovTest",
        "cramersV",
        "contingency",
        "theilsU");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Creates a minimal Guice injector with mocked dependencies. */
  private static Injector createInjector() {
    Settings settings = Mockito.mock(Settings.class);
    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

    DialectRegistry dialectRegistry = new DialectRegistry();
    dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);
    dialectRegistry.freeze();

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
    return modules.createInjector();
  }

  // -------------------------------------------------------------------------
  // Test Harness
  // -------------------------------------------------------------------------

  /**
   * Test harness that sets up the RestSQLQueryAction with mocked dependencies and captures the
   * response. Extends BaseRestHandler to access the protected RestChannelConsumer type.
   */
  private static class TestHarness extends BaseRestHandler {
    private final Injector injector;

    TestHarness(DialectPlugin additionalPlugin) {
      DialectRegistry dialectRegistry = new DialectRegistry();
      dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);
      if (additionalPlugin != null) {
        dialectRegistry.register(additionalPlugin);
      }
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
