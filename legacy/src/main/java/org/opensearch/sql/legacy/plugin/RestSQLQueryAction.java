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
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
      return prepareDialectRequest(request, dialectParam.get(), executionErrorHandler);
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
        sendErrorResponse(channel, errorMsg, RestStatus.BAD_REQUEST);
      };
    }

    // Resolve dialect from registry
    Optional<DialectPlugin> dialectPlugin = dialectRegistry.resolve(dialectName);
    if (dialectPlugin.isEmpty()) {
      return channel -> {
        String errorMsg =
            String.format(
                Locale.ROOT,
                "Unsupported dialect '%s'. Available dialects: %s",
                dialectName,
                dialectRegistry.availableDialects());
        sendErrorResponse(channel, errorMsg, RestStatus.BAD_REQUEST);
      };
    }

    // Route to dialect execution pipeline
    DialectPlugin plugin = dialectPlugin.get();
    LOG.info(
        "[{}] Routing query to dialect '{}' pipeline",
        QueryContext.getRequestId(),
        dialectName);
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
      // 1. Preprocess the query to strip dialect-specific clauses
      String preprocessedQuery = plugin.preprocessor().preprocess(request.getQuery());
      LOG.debug(
          "[{}] Dialect query preprocessed: original='{}', result='{}'",
          QueryContext.getRequestId(),
          request.getQuery(),
          preprocessedQuery);

      // 2. Build FrameworkConfig with dialect-specific parser config and operator table
      DataSourceService dataSourceService = injector.getInstance(DataSourceService.class);
      FrameworkConfig frameworkConfig = buildDialectFrameworkConfig(plugin, dataSourceService);

      // 3. Parse, validate, and convert to RelNode using Calcite Planner
      Planner planner = Frameworks.getPlanner(frameworkConfig);
      SqlNode parsed = planner.parse(preprocessedQuery);
      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      RelNode relNode = relRoot.rel;
      planner.close();

      // 4. Create CalcitePlanContext and execute via the execution engine
      CalcitePlanContext context =
          CalcitePlanContext.create(
              frameworkConfig, SysLimit.fromSettings(settings), QueryType.CLICKHOUSE);

      ExecutionEngine executionEngine = injector.getInstance(ExecutionEngine.class);
      ResponseListener<QueryResponse> queryListener =
          createDialectQueryResponseListener(channel, request, executionErrorHandler);

      executionEngine.execute(relNode, context, queryListener);

    } catch (SqlParseException e) {
      // Parse errors: return 400 with position info from Calcite's SqlParseException.
      // Calcite's message already includes position (e.g., "at line 1, column 5").
      String errorMsg = String.format(Locale.ROOT, "SQL parse error: %s", e.getMessage());
      sendErrorResponse(channel, errorMsg, RestStatus.BAD_REQUEST);
    } catch (ValidationException e) {
      // Validation errors: unsupported function or type.
      // Extract function/type name from the Calcite validation message.
      String details = extractValidationErrorDetails(e);
      sendErrorResponse(channel, details, RestStatus.BAD_REQUEST);
    } catch (RelConversionException e) {
      sendErrorResponse(channel, "SQL conversion error: " + e.getMessage(), RestStatus.BAD_REQUEST);
    } catch (IndexNotFoundException e) {
      // Missing index: return 404 with the index name
      String indexName = e.getIndex() != null ? e.getIndex().getName() : "unknown";
      String errorMsg = String.format(Locale.ROOT, "Index not found: %s", indexName);
      sendErrorResponse(channel, errorMsg, RestStatus.NOT_FOUND);
    } catch (Exception e) {
      // Internal errors: return 500 with generic message, log full stack trace.
      // Never expose Java class names, package paths, or stack traces in the response.
      LOG.error("Internal error during dialect query execution", e);
      sendErrorResponse(
          channel,
          "An internal server error occurred during query execution. "
              + "Check server logs for details.",
          RestStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Extract meaningful error details from a Calcite ValidationException. Identifies unsupported
   * function names and unsupported type names from the message.
   */
  private String extractValidationErrorDetails(ValidationException e) {
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
      return String.format(Locale.ROOT, "Unsupported function: %s", funcName);
    }

    // Check for unsupported type pattern:
    // Calcite may report "Unknown datatype name '<TYPE>'" or similar
    Matcher typeMatcher = UNSUPPORTED_TYPE_PATTERN.matcher(causeMessage);
    if (typeMatcher.find()) {
      String typeName = typeMatcher.group(1);
      return String.format(Locale.ROOT, "Unsupported type: %s", typeName);
    }

    // Fallback: return the validation message as-is
    return String.format(Locale.ROOT, "SQL validation error: %s", causeMessage);
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
      sendErrorResponse(channel, errorMsg, RestStatus.NOT_FOUND);
    } else {
      // Internal error: log full stack trace, return generic message
      LOG.error("Internal error during dialect query execution", e);
      sendErrorResponse(
          channel,
          "An internal server error occurred during query execution. "
              + "Check server logs for details.",
          RestStatus.INTERNAL_SERVER_ERROR);
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

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      Boolean enabled = settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
      return enabled != null && enabled;
    }
    return false;
  }

  private void sendErrorResponse(RestChannel channel, String message, RestStatus status) {
    String errorJson =
        String.format(
            Locale.ROOT,
            "{\"error\":{\"reason\":\"Invalid Query\","
                + "\"details\":\"%s\","
                + "\"type\":\"DialectQueryException\"},"
                + "\"status\":%d}",
            message.replace("\"", "\\\""),
            status.getStatus());
    channel.sendResponse(
        new BytesRestResponse(status, "application/json; charset=UTF-8", errorJson));
  }

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
}
