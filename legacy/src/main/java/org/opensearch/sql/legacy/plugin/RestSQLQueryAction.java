/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.List;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.exception.UnsupportedCursorRequestException;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.security.SecurityAccess;
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

    SQLService sqlService =
        SecurityAccess.doPrivileged(() -> injector.getInstance(SQLService.class));

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
                  fallbackHandler));
    }
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
