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
 * Property-based tests for error handling in the dialect query execution path. Validates:
 * Requirements 4.4, 8.2, 8.3
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class RestSQLQueryActionDialectErrorHandlingPropertyTest {

  // -------------------------------------------------------------------------
  // Property 5: Syntax error position reporting
  // -------------------------------------------------------------------------

  /**
   * Property 5: Syntax error position reporting — For any query containing a syntax error, the
   * error message returned by the Dialect_Handler SHALL contain a numeric position or line/column
   * indicator.
   *
   * <p>Validates: Requirements 4.4
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 5: Syntax error position reporting")
  void syntaxErrorResponseContainsPositionInfo(
      @ForAll("queriesWithSyntaxErrors") String brokenQuery) throws Exception {
    TestHarness harness = new TestHarness();
    BytesRestResponse response = harness.executeDialectQuery("clickhouse", brokenQuery);

    assertNotNull(response, "Should have captured a response");
    assertEquals(RestStatus.BAD_REQUEST, response.status(), "Syntax errors should return 400");

    String content = response.content().utf8ToString();
    assertTrue(
        content.contains("SQL parse error"),
        "Response should indicate a parse error. Content: " + content);

    // Calcite's SqlParseException includes position info like "at line X, column Y"
    // The error message must contain a numeric position indicator
    String lowerContent = content.toLowerCase();
    boolean hasPositionInfo = POSITION_PATTERN.matcher(lowerContent).find();
    assertTrue(
        hasPositionInfo,
        "Error message should contain numeric position info (line/column/pos). Content: "
            + content);
  }

  /** Pattern to detect numeric position indicators in error messages. */
  private static final Pattern POSITION_PATTERN =
      Pattern.compile("(line \\d+|column \\d+|pos(ition)? \\d+)");

  // -------------------------------------------------------------------------
  // Property 12: Internal errors do not expose details
  // -------------------------------------------------------------------------

  /**
   * Property 12: Internal errors do not expose details — For any internal exception thrown during
   * dialect query processing, the HTTP response body SHALL not contain Java class names, package
   * names, or stack trace lines.
   *
   * <p>Validates: Requirements 8.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 12: Internal errors do not expose details")
  void internalErrorResponseDoesNotExposeDetails(
      @ForAll("internalExceptionMessages") String exceptionMessage) throws Exception {
    // Create a mock plugin that throws a RuntimeException with the generated message
    DialectPlugin failingPlugin = Mockito.mock(DialectPlugin.class);
    when(failingPlugin.dialectName()).thenReturn("failing");
    when(failingPlugin.preprocessor()).thenThrow(new RuntimeException(exceptionMessage));

    TestHarness harness = new TestHarness(failingPlugin);
    BytesRestResponse response = harness.executeDialectQuery("failing", "SELECT 1");

    assertNotNull(response, "Should have captured a response");
    assertEquals(
        RestStatus.INTERNAL_SERVER_ERROR, response.status(), "Internal errors should return 500");

    String content = response.content().utf8ToString();

    // Parse the JSON to extract the details field specifically.
    // The "type" field is expected to contain "DialectQueryException" — that's by design.
    // We only check the "details" field for leaked internal information.
    JSONObject json = new JSONObject(content);
    String details = json.getJSONObject("error").getString("details");

    // Verify generic error message is present
    assertTrue(
        details.contains("internal server error"),
        "Details should contain generic error message. Details: " + details);

    // Verify no Java class names (e.g., NullPointerException, IllegalStateException)
    assertFalse(
        JAVA_CLASS_NAME_PATTERN.matcher(details).find(),
        "Details should NOT contain Java class names. Details: " + details);

    // Verify no Java package paths (e.g., org.opensearch.sql.internal)
    assertFalse(
        JAVA_PACKAGE_PATTERN.matcher(details).find(),
        "Details should NOT contain Java package paths. Details: " + details);

    // Verify no stack trace lines (e.g., "at org.opensearch.sql.SomeClass.method(File.java:42)")
    assertFalse(
        STACK_TRACE_PATTERN.matcher(details).find(),
        "Details should NOT contain stack trace lines. Details: " + details);
  }

  /** Pattern to detect Java exception class names (e.g., NullPointerException). */
  private static final Pattern JAVA_CLASS_NAME_PATTERN =
      Pattern.compile("[A-Z]\\w*Exception|[A-Z]\\w*Error");

  /** Pattern to detect Java package paths (e.g., org.opensearch.sql.internal). */
  private static final Pattern JAVA_PACKAGE_PATTERN =
      Pattern.compile("\\b[a-z]+\\.[a-z]+\\.[a-z]+\\.\\w+");

  /** Pattern to detect stack trace lines (e.g., "at org.foo.Bar.method(File.java:42)"). */
  private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("\\bat\\s+[a-z]\\w*\\.\\w+");

  // -------------------------------------------------------------------------
  // Property 13: Unsupported type error identification
  // -------------------------------------------------------------------------

  /**
   * Property 13: Unsupported type error identification — For any data type name that has no
   * OpenSearch mapping, the error message SHALL contain the unsupported type name.
   *
   * <p>Validates: Requirements 8.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 13: Unsupported type error identification")
  void unsupportedTypeErrorContainsTypeName(@ForAll("unsupportedTypeNames") String typeName)
      throws Exception {
    // Test the extractValidationErrorDetails method directly via reflection.
    // This method is the core logic that extracts type names from Calcite's ValidationException.
    RestSQLQueryAction queryAction = new RestSQLQueryAction(createInjector());

    // Simulate Calcite's error message format: "Unknown datatype name '<TYPE>'"
    String causeMessage = "Unknown datatype name '" + typeName + "'";
    org.apache.calcite.tools.ValidationException ve =
        new org.apache.calcite.tools.ValidationException(
            "Validation failed", new RuntimeException(causeMessage));

    // Use reflection to call the private extractValidationErrorDetails method
    Method extractMethod =
        RestSQLQueryAction.class.getDeclaredMethod(
            "extractValidationErrorDetails", org.apache.calcite.tools.ValidationException.class);
    extractMethod.setAccessible(true);
    String result = (String) extractMethod.invoke(queryAction, ve);

    // The extracted error message should contain the unsupported type name
    assertTrue(
        result.contains(typeName),
        "Error message should contain the unsupported type name '"
            + typeName
            + "'. Result: "
            + result);
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> queriesWithSyntaxErrors() {
    // Generate queries that will definitely fail Calcite parsing with position info
    return Arbitraries.of(
        "SELECT * FORM my_table",
        "SELECT a, FROM my_table",
        "SELECT a FROM",
        "SELECT a FROM t WHERE",
        "SELECT SELECT a FROM t",
        "SELECT COUNT( FROM t",
        "SELECT a FROM t ORDER",
        "SELECT a FROM t WHERE a = !!!",
        "SELECT a FROM t GROUP",
        "SELECT a FROM t LIMIT abc",
        "SELECT a FROM t WHERE a >",
        "SELECT 'unclosed FROM t",
        "SELECT a FROM t HAVING ??? > 1",
        "SELECT a FROM t JOIN",
        "SELECT a AS FROM t");
  }

  @Provide
  Arbitrary<String> internalExceptionMessages() {
    // Generate exception messages that contain Java internals that should NOT leak
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
            "org.opensearch.OpenSearchException: shard failure");

    Arbitrary<String> stackTraces =
        Arbitraries.of(
            "at org.opensearch.sql.legacy.plugin.RestSQLQueryAction.executeDialectQuery"
                + "(RestSQLQueryAction.java:214)",
            "at java.base/java.lang.Thread.run(Thread.java:829)",
            "at org.apache.calcite.tools.Frameworks.getPlanner(Frameworks.java:100)");

    Arbitrary<String> packagePaths =
        Arbitraries.of(
            "org.opensearch.sql.internal.SomeClass",
            "org.apache.calcite.sql.parser.SqlParser",
            "java.lang.reflect.Method.invoke");

    // Combine different types of internal details
    return Arbitraries.oneOf(
        classNames,
        stackTraces,
        packagePaths,
        Combinators.combine(classNames, stackTraces).as((cls, st) -> cls + "\n\t" + st));
  }

  @Provide
  Arbitrary<String> unsupportedTypeNames() {
    // Generate type names that have no OpenSearch mapping
    return Arbitraries.of(
        "UUID",
        "Decimal128",
        "FixedString",
        "Enum8",
        "Enum16",
        "Array",
        "Tuple",
        "Nested",
        "LowCardinality",
        "SimpleAggregateFunction",
        "AggregateFunction",
        "IPv4",
        "IPv6",
        "Nullable",
        "Nothing",
        "Ring",
        "Polygon",
        "MultiPolygon");
  }

  // -------------------------------------------------------------------------
  // Test Harness
  // -------------------------------------------------------------------------

  /** Creates a minimal Guice injector with mocked dependencies. */
  private static Injector createInjector() {
    Settings settings = Mockito.mock(Settings.class);
    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);

    DialectRegistry dialectRegistry = new DialectRegistry();
    dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);

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

  /**
   * Test harness that sets up the RestSQLQueryAction with mocked dependencies and captures the
   * response. Extends BaseRestHandler to access the protected RestChannelConsumer type.
   */
  private static class TestHarness extends BaseRestHandler {
    private final Injector injector;

    TestHarness() {
      this(null);
    }

    TestHarness(DialectPlugin additionalPlugin) {
      DialectRegistry dialectRegistry = new DialectRegistry();
      dialectRegistry.register(ClickHouseDialectPlugin.INSTANCE);
      if (additionalPlugin != null) {
        dialectRegistry.register(additionalPlugin);
      }

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
