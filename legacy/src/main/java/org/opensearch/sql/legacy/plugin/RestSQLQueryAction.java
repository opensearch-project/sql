/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.DialectRegistry;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.UnsupportedCursorRequestException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CommandResponseFormatter;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * New SQL REST action handler. This will not be registered to OpenSearch unless:
 *
 * <ol>
 *   <li>we want to test new SQL engine;
 *   <li>all old functionalities migrated to new query engine and legacy REST handler removed.
 * </ol>
 */
public class RestSQLQueryAction extends BaseRestHandler {

  private static final Logger LOG = LogManager.getLogger();

  public static final RestChannelConsumer NOT_SUPPORTED_YET = null;

  private final Injector injector;

  /** Constructor of RestSQLQueryAction. */
  public RestSQLQueryAction(Injector injector) {
    super();
    this.injector = injector;
  }

  @Override
  public String getName() {
    return "sql_query_action";
  }

  @Override
  public List<Route> routes() {
    throw new UnsupportedOperationException("New SQL handler is not ready yet");
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) {
    throw new UnsupportedOperationException("New SQL handler is not ready yet");
  }

  /**
   * Prepare REST channel consumer for a SQL query request.
   *
   * @param request SQL request
   * @param fallbackHandler handle request fallback to legacy engine.
   * @param executionErrorHandler handle error response during new engine execution.
   * @return {@link RestChannelConsumer}
   */
  public RestChannelConsumer prepareRequest(
      SQLQueryRequest request,
      BiConsumer<RestChannel, Exception> fallbackHandler,
      BiConsumer<RestChannel, Exception> executionErrorHandler) {
    if (!request.isSupported()) {
      return channel -> fallbackHandler.accept(channel, new IllegalStateException("not supported"));
    }

    // Check for dialect parameter and route to dialect pipeline if present
    Optional<String> dialectParam = request.getDialect();
    if (dialectParam.isPresent()) {
      String sanitized = sanitizeDialectParam(dialectParam.get());
      if (sanitized.isEmpty()) {
        return channel -> {
            LOG.warn(
                "[{}] Dialect query rejected: empty dialect parameter",
                QueryContext.getRequestId());
            sendErrorResponse(
                channel,
                "Dialect parameter must be non-empty.",
                RestStatus.BAD_REQUEST);
        };
      }
      return prepareDialectRequest(request, sanitized, executionErrorHandler);
    }

    SQLService sqlService = injector.getInstance(SQLService.class);

    if (request.isExplainRequest()) {
      return channel ->
          sqlService.explain(
              request,
              fallBackListener(
                  channel,
                  createExplainResponseListener(channel, executionErrorHandler),
                  fallbackHandler));
    }
    // If close request, sqlService.closeCursor
    else {
      return channel ->
          sqlService.execute(
              request,
              fallBackListener(
                  channel,
                  createQueryResponseListener(channel, request, executionErrorHandler),
                  fallbackHandler),
              fallBackListener(
                  channel,
                  createExplainResponseListener(channel, executionErrorHandler),
                  fallbackHandler));
    }
  }

  /**
   * Prepare a REST channel consumer for a dialect query request. Validates the dialect parameter,
   * checks Calcite engine status, and routes to the dialect execution pipeline.
   */
  private RestChannelConsumer prepareDialectRequest(
      SQLQueryRequest request,
      String dialectName,
      BiConsumer<RestChannel, Exception> executionErrorHandler) {

    Settings settings = injector.getInstance(Settings.class);
    DialectRegistry dialectRegistry = injector.getInstance(DialectRegistry.class);

    // Check if Calcite engine is enabled — dialect support requires it
    boolean calciteEnabled = isCalciteEnabled(settings);
    if (!calciteEnabled) {
      return channel -> {
        String errorMsg =
            "Dialect query support requires the Calcite engine to be enabled. "
                + "Set plugins.calcite.enabled=true to use dialect queries.";
        LOG.warn(
            "[{}] Dialect query rejected: Calcite engine is disabled",
            QueryContext.getRequestId());
        sendErrorResponse(channel, errorMsg, RestStatus.BAD_REQUEST);
      };
    }

    // Resolve dialect from registry
    Optional<DialectPlugin> dialectPlugin = dialectRegistry.resolve(dialectName);
    if (dialectPlugin.isEmpty()) {
      return channel -> {
        String message =
            String.format(
                Locale.ROOT,
                "Unknown SQL dialect '%s'. Supported dialects: %s",
                dialectName,
                dialectRegistry.availableDialects());
        LOG.warn(
            "[{}] Unknown dialect requested: '{}'", QueryContext.getRequestId(), dialectName);
        String errorJson =
            new JSONObject()
                .put("error_type", "UNKNOWN_DIALECT")
                .put("message", message)
                .put("dialect_requested", dialectName)
                .toString();
        channel.sendResponse(
            new BytesRestResponse(
                RestStatus.BAD_REQUEST, "application/json; charset=UTF-8", errorJson));
      };
    }

    // Route to dialect execution pipeline
    DialectPlugin plugin = dialectPlugin.get();
    LOG.info(
        "[{}] Routing query to dialect '{}' pipeline",
        QueryContext.getRequestId(),
        dialectName);
    incrementMetric(MetricName.DIALECT_REQUESTS_TOTAL);
    return channel ->
        executeDialectQuery(plugin, request, settings, channel, executionErrorHandler);
  }

  /**
   * Execute a dialect query through the Calcite pipeline. Steps: preprocess → parse → validate →
   * convert to RelNode → execute
   *
   * <p>Error handling strategy:
   *
   * <ul>
   *   <li>Parse errors (SqlParseException): 400 with position info from Calcite
   *   <li>Validation errors (unsupported function/type): 400 with function/type name
   *   <li>Missing index (IndexNotFoundException): 404 with index name
   *   <li>Internal errors: 500 with generic message, stack trace logged at ERROR level
   * </ul>
   */
  private void executeDialectQuery(
      DialectPlugin plugin,
      SQLQueryRequest request,
      Settings settings,
      RestChannel channel,
      BiConsumer<RestChannel, Exception> executionErrorHandler) {
    try {
      long startNanos = System.nanoTime();

      // 1. Preprocess the query to strip dialect-specific clauses
      String preprocessedQuery = plugin.preprocessor().preprocess(request.getQuery());
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "[{}] Preprocessed query: {}",
            QueryContext.getRequestId(),
            preprocessedQuery);
      }

      // 2. Build FrameworkConfig with dialect-specific parser config and operator table
      DataSourceService dataSourceService = injector.getInstance(DataSourceService.class);
      FrameworkConfig frameworkConfig = buildDialectFrameworkConfig(plugin, dataSourceService);

      // 3. Parse, validate, and convert to RelNode using Calcite Planner
      Planner planner = Frameworks.getPlanner(frameworkConfig);
      SqlNode parsed = planner.parse(preprocessedQuery);
      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      RelNode relNode = relRoot.rel;
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "[{}] Calcite plan: {}",
            QueryContext.getRequestId(),
            RelOptUtil.toString(relNode));
      }
      planner.close();

      // 4. Create CalcitePlanContext and execute via the execution engine
      CalcitePlanContext context =
          CalcitePlanContext.create(
              frameworkConfig, SysLimit.fromSettings(settings), QueryType.CLICKHOUSE);

      ExecutionEngine executionEngine = injector.getInstance(ExecutionEngine.class);
      ResponseListener<QueryResponse> queryListener =
          createDialectQueryResponseListener(channel, request, executionErrorHandler);

      executionEngine.execute(relNode, context, queryListener);

      // Record dialect execution latency
      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
      addToMetric(MetricName.DIALECT_UNPARSE_LATENCY_MS, elapsedMs);

    } catch (SqlParseException e) {
      incrementMetric(MetricName.DIALECT_TRANSLATION_ERRORS_TOTAL);
      // Parse errors: return 400 with position info from Calcite's SqlParseException.
      // Extract line/column from getPos() for structured position reporting.
      // Sanitize the message to remove any internal class names or package paths.
      String sanitizedMsg = sanitizeErrorMessage(e.getMessage());
      String errorMsg = String.format(Locale.ROOT, "SQL parse error: %s", sanitizedMsg);
      LOG.warn("[{}] Dialect query parse error: {}", QueryContext.getRequestId(), e.getMessage());
      SqlParserPos pos = e.getPos();
      if (pos != null && pos.getLineNum() > 0) {
        sendErrorResponseWithPosition(
            channel, errorMsg, RestStatus.BAD_REQUEST, pos.getLineNum(), pos.getColumnNum());
      } else {
        sendErrorResponse(channel, errorMsg, RestStatus.BAD_REQUEST);
      }
    } catch (ValidationException e) {
      incrementMetric(MetricName.DIALECT_TRANSLATION_ERRORS_TOTAL);
      // Validation errors (unsupported function or type): return 422 Unprocessable Entity.
      // Extract function/type name from the Calcite validation message and include suggestions.
      LOG.warn(
          "[{}] Dialect query validation error: {}",
          QueryContext.getRequestId(),
          e.getMessage());
      String details = extractValidationErrorDetails(e, plugin);
      sendErrorResponse(channel, details, RestStatus.UNPROCESSABLE_ENTITY);
    } catch (RelConversionException e) {
      incrementMetric(MetricName.DIALECT_TRANSLATION_ERRORS_TOTAL);
      // Sanitize the conversion error message to remove internal class names or package paths.
      String sanitizedMsg = sanitizeErrorMessage(e.getMessage());
      LOG.warn(
          "[{}] Dialect query conversion error: {}",
          QueryContext.getRequestId(),
          e.getMessage());
      sendErrorResponse(
          channel,
          "SQL conversion error: " + sanitizedMsg,
          RestStatus.BAD_REQUEST);
    } catch (IndexNotFoundException e) {
      // Missing index: return 404 with the index name
      String indexName = e.getIndex() != null ? e.getIndex().getName() : "unknown";
      String errorMsg = String.format(Locale.ROOT, "Index not found: %s", indexName);
      LOG.warn("[{}] Dialect query index not found: {}", QueryContext.getRequestId(), indexName);
      sendErrorResponse(channel, errorMsg, RestStatus.NOT_FOUND);
    } catch (Exception e) {
      incrementMetric(MetricName.DIALECT_TRANSLATION_ERRORS_TOTAL);
      // Internal errors: return 500 with generic message and internal_id for log correlation.
      // Never expose Java class names, package paths, or stack traces in the response.
      String internalId = UUID.randomUUID().toString();
      LOG.error("Internal error during dialect query execution [internal_id={}]", internalId, e);
      sendInternalErrorResponse(channel, internalId);
    }
  }

  /**
   * Extract meaningful error details from a Calcite ValidationException. Identifies unsupported
   * function names and unsupported type names from the message. For unsupported functions, includes
   * available alternatives from the dialect's operator table.
   *
   * @param e the validation exception
   * @param plugin the dialect plugin (used to retrieve available function names for suggestions)
   */
  private String extractValidationErrorDetails(ValidationException e, DialectPlugin plugin) {
    String message = e.getMessage() != null ? e.getMessage() : "";
    // Calcite wraps the real cause; check the cause chain for more details
    Throwable cause = e.getCause();
    String causeMessage =
        cause != null && cause.getMessage() != null ? cause.getMessage() : message;

    // Check for unsupported function pattern:
    // Calcite typically reports "No match found for function signature <NAME>(...)"
    Matcher funcMatcher = UNSUPPORTED_FUNCTION_PATTERN.matcher(causeMessage);
    if (funcMatcher.find()) {
      String funcName = funcMatcher.group(1);
      // Build suggestion list from the dialect's operator table
      String suggestions = buildFunctionSuggestions(plugin);
      if (!suggestions.isEmpty()) {
        return String.format(
            Locale.ROOT,
            "Unsupported function: %s. Available alternatives: %s",
            funcName,
            suggestions);
      }
      return String.format(Locale.ROOT, "Unsupported function: %s", funcName);
    }

    // Check for unsupported type pattern:
    // Calcite may report "Unknown datatype name '<TYPE>'" or similar
    Matcher typeMatcher = UNSUPPORTED_TYPE_PATTERN.matcher(causeMessage);
    if (typeMatcher.find()) {
      String typeName = typeMatcher.group(1);
      return String.format(Locale.ROOT, "Unsupported type: %s", typeName);
    }

    // Fallback: sanitize the validation message to remove internal class names or package paths
    return String.format(
        Locale.ROOT, "SQL validation error: %s", sanitizeErrorMessage(causeMessage));
  }

  /**
   * Build a comma-separated list of available function names from the dialect's operator table. Used
   * to suggest alternatives when an unsupported function is encountered.
   */
  private String buildFunctionSuggestions(DialectPlugin plugin) {
    try {
      SqlOperatorTable operatorTable = plugin.operatorTable();
      List<org.apache.calcite.sql.SqlOperator> operators = operatorTable.getOperatorList();
      if (operators == null || operators.isEmpty()) {
        return "";
      }
      return operators.stream()
          .map(op -> op.getName().toLowerCase(Locale.ROOT))
          .distinct()
          .sorted()
          .collect(Collectors.joining(", "));
    } catch (Exception ex) {
      // If we can't retrieve function names, return empty (no suggestions)
      return "";
    }
  }

  /** Pattern to extract function name from Calcite validation error messages. */
  private static final Pattern UNSUPPORTED_FUNCTION_PATTERN =
      Pattern.compile(
          "No match found for function signature ([\\w]+)\\(", Pattern.CASE_INSENSITIVE);

  /** Pattern to extract type name from Calcite validation error messages. */
  private static final Pattern UNSUPPORTED_TYPE_PATTERN =
      Pattern.compile("Unknown (?:datatype|type)(?: name)? '([\\w]+)'", Pattern.CASE_INSENSITIVE);

  /**
   * Create a query response listener for dialect queries that handles execution-phase errors (e.g.,
   * IndexNotFoundException from OpenSearch) with proper error responses.
   */
  private ResponseListener<QueryResponse> createDialectQueryResponseListener(
      RestChannel channel,
      SQLQueryRequest request,
      BiConsumer<RestChannel, Exception> executionErrorHandler) {
    Format format = request.format();
    ResponseFormatter<QueryResult> formatter;

    if (request.isCursorCloseRequest()) {
      formatter = new CommandResponseFormatter();
    } else if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(request.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter(request.pretty());
    } else {
      formatter = new JdbcResponseFormatter(PRETTY);
    }
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        Cursor cursor = response.getCursor() != null ? response.getCursor() : Cursor.None;
        sendResponse(
            channel,
            OK,
            formatter.format(
                new QueryResult(response.getSchema(), response.getResults(), cursor)),
            formatter.contentType());
      }

      @Override
      public void onFailure(Exception e) {
        handleDialectExecutionError(channel, e);
      }
    };
  }

  /**
   * Handle errors that occur during the execution phase of a dialect query (after
   * parsing/validation, during OpenSearch query execution).
   */
  private void handleDialectExecutionError(RestChannel channel, Exception e) {
    // Unwrap to find the root cause
    Throwable cause = unwrapCause(e);

    if (cause instanceof IndexNotFoundException) {
      IndexNotFoundException infe = (IndexNotFoundException) cause;
      String indexName = infe.getIndex() != null ? infe.getIndex().getName() : "unknown";
      String errorMsg = String.format(Locale.ROOT, "Index not found: %s", indexName);
      LOG.warn(
          "[{}] Dialect query execution - index not found: {}",
          QueryContext.getRequestId(),
          indexName);
      sendErrorResponse(channel, errorMsg, RestStatus.NOT_FOUND);
    } else {
      // Internal error: log full stack trace with internal_id, return generic message
      String internalId = UUID.randomUUID().toString();
      LOG.error("Internal error during dialect query execution [internal_id={}]", internalId, e);
      sendInternalErrorResponse(channel, internalId);
    }
  }

  /** Unwrap exception cause chain to find the root cause. */
  private static Throwable unwrapCause(Throwable t) {
    Throwable result = t;
    while (result.getCause() != null && result.getCause() != result) {
      result = result.getCause();
    }
    return result;
  }

  /**
   * Build a FrameworkConfig for dialect query processing. Uses the dialect's parser config and
   * operator table, chained with the OpenSearch schema.
   */
  private FrameworkConfig buildDialectFrameworkConfig(
      DialectPlugin plugin, DataSourceService dataSourceService) {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));

    // Chain the dialect's operator table with the default Calcite operator table
    SqlOperatorTable chainedOperatorTable =
        SqlOperatorTables.chain(plugin.operatorTable(), SqlStdOperatorTable.instance());

    return Frameworks.newConfigBuilder()
        .parserConfig(plugin.parserConfig())
        .operatorTable(chainedOperatorTable)
        .defaultSchema(opensearchSchema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.standard())
        .typeSystem(OpenSearchTypeSystem.INSTANCE)
        .build();
  }

  /**
   * Sanitize the dialect parameter to prevent injection and reflection attacks.
   *
   * <ul>
   *   <li>Truncate to max 64 characters
   *   <li>Strip control characters (chars &lt; 0x20 except tab)
   *   <li>Strip non-ASCII characters (chars &gt;= 0x7f)
   * </ul>
   *
   * @param raw the raw dialect parameter value
   * @return the sanitized string (may be empty if input was entirely invalid)
   */
  String sanitizeDialectParam(String raw) {
    if (raw.length() > 64) {
      raw = raw.substring(0, 64);
    }
    return raw.replaceAll("[\\x00-\\x1f\\x7f-\\xff]", "").trim();
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      Boolean enabled = settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
      return enabled != null && enabled;
    }
    return false;
  }

  private void sendErrorResponse(RestChannel channel, String message, RestStatus status) {
    String escapedMessage = escapeJsonString(message);
    String errorJson =
        String.format(
            Locale.ROOT,
            "{\"error\":{\"reason\":\"Invalid Query\","
                + "\"details\":\"%s\","
                + "\"type\":\"DialectQueryException\"},"
                + "\"status\":%d}",
            escapedMessage,
            status.getStatus());
    channel.sendResponse(
        new BytesRestResponse(status, "application/json; charset=UTF-8", errorJson));
  }

  private void sendErrorResponseWithPosition(
      RestChannel channel, String message, RestStatus status, int line, int column) {
    String escapedMessage = escapeJsonString(message);
    String errorJson =
        String.format(
            Locale.ROOT,
            "{\"error\":{\"reason\":\"Invalid Query\","
                + "\"details\":\"%s\","
                + "\"type\":\"DialectQueryException\","
                + "\"position\":{\"line\":%d,\"column\":%d}},"
                + "\"status\":%d}",
            escapedMessage,
            line,
            column,
            status.getStatus());
    channel.sendResponse(
        new BytesRestResponse(status, "application/json; charset=UTF-8", errorJson));
  }

  /**
   * Send a 500 Internal Error response with a sanitized message and an internal_id for log
   * correlation. The internal_id is a UUID that is also included in the ERROR log entry, allowing
   * operators to correlate client-visible error responses with server-side log entries.
   *
   * @param channel the REST channel to send the response on
   * @param internalId the UUID string for log correlation
   */
  private void sendInternalErrorResponse(RestChannel channel, String internalId) {
    String errorJson =
        String.format(
            Locale.ROOT,
            "{\"error\":{\"reason\":\"Internal Error\","
                + "\"details\":\"An internal error occurred processing the dialect query.\","
                + "\"type\":\"InternalError\","
                + "\"internal_id\":\"%s\"},"
                + "\"status\":500}",
            escapeJsonString(internalId));
    channel.sendResponse(
        new BytesRestResponse(
            RestStatus.INTERNAL_SERVER_ERROR, "application/json; charset=UTF-8", errorJson));
  }

  /** Escape a string for safe inclusion in a JSON string value. */
  private static String escapeJsonString(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  /**
   * Sanitize an error message to remove internal implementation details before including it in an
   * HTTP response. Strips:
   *
   * <ul>
   *   <li>Java fully-qualified class names (e.g., {@code org.apache.calcite.sql.SomeClass})
   *   <li>Stack trace lines (e.g., {@code at org.opensearch.sql.SomeClass.method(File.java:42)})
   *   <li>Exception class name prefixes (e.g., {@code java.lang.NullPointerException:})
   * </ul>
   *
   * @param message the raw error message
   * @return the sanitized message safe for client-facing responses
   */
  static String sanitizeErrorMessage(String message) {
    if (message == null) {
      return "";
    }
    // Remove stack trace lines: "at org.package.Class.method(File.java:123)"
    String sanitized = STACK_TRACE_PATTERN.matcher(message).replaceAll("");
    // Remove exception class name prefixes: "java.lang.NullPointerException: ..."
    sanitized = EXCEPTION_PREFIX_PATTERN.matcher(sanitized).replaceAll("");
    // Remove remaining fully-qualified Java class/package references
    sanitized = PACKAGE_PATH_PATTERN.matcher(sanitized).replaceAll("");
    // Collapse multiple spaces and trim
    return sanitized.replaceAll("\\s+", " ").trim();
  }

  /** Pattern matching stack trace lines like "at org.package.Class.method(File.java:123)". */
  private static final Pattern STACK_TRACE_PATTERN =
      Pattern.compile("\\bat\\s+[a-zA-Z_][a-zA-Z0-9_.]*\\([^)]*\\)");

  /**
   * Pattern matching exception class name prefixes like "java.lang.NullPointerException:" or
   * "org.apache.calcite.SomeException:".
   */
  private static final Pattern EXCEPTION_PREFIX_PATTERN =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*){2,}(?:Exception|Error)\\s*:?\\s*");

  /**
   * Pattern matching fully-qualified Java package/class paths like "org.apache.calcite.sql.SomeClass"
   * (at least 3 dot-separated segments where the last starts with uppercase).
   */
  private static final Pattern PACKAGE_PATH_PATTERN =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*){2,}\\.[A-Z][a-zA-Z0-9_]*");

  private <T> ResponseListener<T> fallBackListener(
      RestChannel channel,
      ResponseListener<T> next,
      BiConsumer<RestChannel, Exception> fallBackHandler) {
    return new ResponseListener<T>() {
      @Override
      public void onResponse(T response) {
        LOG.info("[{}] Request is handled by new SQL query engine", QueryContext.getRequestId());
        next.onResponse(response);
      }

      @Override
      public void onFailure(Exception e) {
        if (e instanceof SyntaxCheckException || e instanceof UnsupportedCursorRequestException) {
          fallBackHandler.accept(channel, e);
        } else {
          next.onFailure(e);
        }
      }
    };
  }

  private ResponseListener<ExplainResponse> createExplainResponseListener(
      RestChannel channel, BiConsumer<RestChannel, Exception> errorHandler) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExplainResponse response) {
        JsonResponseFormatter<ExplainResponse> formatter =
            new JsonResponseFormatter<>(PRETTY) {
              @Override
              protected Object buildJsonObject(ExplainResponse response) {
                return response;
              }
            };
        sendResponse(channel, OK, formatter.format(response), formatter.contentType());
      }

      @Override
      public void onFailure(Exception e) {
        errorHandler.accept(channel, e);
      }
    };
  }

  private ResponseListener<QueryResponse> createQueryResponseListener(
      RestChannel channel,
      SQLQueryRequest request,
      BiConsumer<RestChannel, Exception> errorHandler) {
    Format format = request.format();
    ResponseFormatter<QueryResult> formatter;

    if (request.isCursorCloseRequest()) {
      formatter = new CommandResponseFormatter();
    } else if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(request.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter(request.pretty());
    } else {
      formatter = new JdbcResponseFormatter(PRETTY);
    }
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        sendResponse(
            channel,
            OK,
            formatter.format(
                new QueryResult(response.getSchema(), response.getResults(), response.getCursor())),
            formatter.contentType());
      }

      @Override
      public void onFailure(Exception e) {
        errorHandler.accept(channel, e);
      }
    };
  }

  private void sendResponse(
      RestChannel channel, RestStatus status, String content, String contentType) {
    channel.sendResponse(new BytesRestResponse(status, contentType, content));
  }

  private static void logAndPublishMetrics(Exception e) {
    LOG.error("Server side error during query execution", e);
    Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
  }

  /**
   * Safely increment a metric counter. If the metric is not registered (e.g., in unit tests
   * that don't call {@code Metrics.getInstance().registerDefaultMetrics()}), the increment
   * is silently skipped.
   */
  private static void incrementMetric(MetricName metricName) {
    NumericMetric metric = Metrics.getInstance().getNumericalMetric(metricName);
    if (metric != null) {
      metric.increment();
    }
  }

  /**
   * Safely add a value to a metric counter. If the metric is not registered, the add
   * is silently skipped.
   */
  private static void addToMetric(MetricName metricName, long value) {
    NumericMetric metric = Metrics.getInstance().getNumericalMetric(metricName);
    if (metric != null) {
      metric.increment(value);
    }
  }
}
