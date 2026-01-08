/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.sql.legacy.cursor.Cursor;
import org.opensearch.sql.legacy.cursor.DefaultCursor;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.QueryActionElasticExecutor;
import org.opensearch.sql.legacy.executor.RestExecutor;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.transport.client.Client;

public class PrettyFormatRestExecutor implements RestExecutor {

  private static final Logger LOG = LogManager.getLogger();

  private final String format;

  public PrettyFormatRestExecutor(String format) {
    this.format = Objects.requireNonNull(format, "Format cannot be null").toLowerCase(Locale.ROOT);
  }

  /** Execute the QueryAction and return the REST response using the channel. */
  @Override
  public void execute(
      Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) {
    String formattedResponse = execute(client, params, queryAction);
    BytesRestResponse bytesRestResponse;
    if (format.equals("jdbc")) {
      bytesRestResponse =
          new BytesRestResponse(
              RestStatus.OK, "application/json; charset=UTF-8", formattedResponse);
    } else {
      bytesRestResponse = new BytesRestResponse(RestStatus.OK, formattedResponse);
    }

    if (!BackOffRetryStrategy.isHealthy(2 * bytesRestResponse.content().length(), this)) {
      throw new IllegalStateException(
          "[PrettyFormatRestExecutor] Memory could be insufficient when sendResponse().");
    }

    channel.sendResponse(bytesRestResponse);
  }

  @Override
  public String execute(Client client, Map<String, String> params, QueryAction queryAction) {
    Protocol protocol;

    try {
      if (queryAction instanceof DefaultQueryAction) {
        protocol = buildProtocolForDefaultQuery(client, (DefaultQueryAction) queryAction);
      } else {
        Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
        protocol = new Protocol(client, queryAction, queryResult, format, Cursor.NULL_CURSOR);
      }
    } catch (SqlParseException e) {
      LOG.warn("SQL parsing error: {}", e.getMessage(), e);
      protocol = new Protocol(e);
    } catch (OpenSearchException e) {
      LOG.warn("An error occurred in OpenSearch engine: {}", e.getDetailedMessage(), e);
      protocol = new Protocol(e);
    } catch (Exception e) {
      LOG.warn("Error happened in pretty formatter", e);
      protocol = new Protocol(e);
    }

    return protocol.format();
  }

  /**
   * Builds protocol for default query execution.
   *
   * <p>Routes to pagination or non-pagination execution based on fetch_size parameter.
   */
  private Protocol buildProtocolForDefaultQuery(Client client, DefaultQueryAction queryAction)
      throws SqlParseException {

    queryAction.explain();

    Integer fetchSize = queryAction.getSqlRequest().fetchSize();
    if (fetchSize != null && fetchSize > 0) {
      return buildProtocolWithPagination(client, queryAction, fetchSize);
    } else {
      return buildProtocolWithoutPagination(client, queryAction);
    }
  }

  /** Executes query with pagination support using Point-in-Time (PIT). */
  private Protocol buildProtocolWithPagination(
      Client client, DefaultQueryAction queryAction, Integer fetchSize) {

    PointInTimeHandler pit =
        new PointInTimeHandlerImpl(client, queryAction.getSelect().getIndexArr());
    pit.create();

    try {
      SearchRequestBuilder searchRequest = queryAction.getRequestBuilder();
      searchRequest.setPointInTime(new PointInTimeBuilder(pit.getPitId()));
      SearchResponse response = searchRequest.get();

      if (shouldCreateCursor(response, queryAction, fetchSize)) {
        DefaultCursor cursor = createCursorWithPit(pit, response, queryAction, fetchSize);
        return new Protocol(client, queryAction, response.getHits(), format, cursor);
      } else {
        pit.delete();
        return new Protocol(client, queryAction, response.getHits(), format, Cursor.NULL_CURSOR);
      }
    } catch (RuntimeException e) {
      try {
        pit.delete();
      } catch (RuntimeException deleteException) {
        LOG.error("Failed to delete PIT", deleteException);
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
      }
      throw e;
    }
  }

  private Protocol buildProtocolWithoutPagination(Client client, DefaultQueryAction queryAction) {
    SearchRequestBuilder searchRequest = queryAction.getRequestBuilder();
    SearchResponse response = searchRequest.get();
    return new Protocol(client, queryAction, response.getHits(), format, Cursor.NULL_CURSOR);
  }

  private DefaultCursor createCursorWithPit(
      PointInTimeHandler pit,
      SearchResponse response,
      DefaultQueryAction queryAction,
      Integer fetchSize) {
    DefaultCursor cursor = new DefaultCursor();
    cursor.setLimit(queryAction.getSelect().getRowCount());
    cursor.setFetchSize(fetchSize);
    cursor.setPitId(pit.getPitId());
    cursor.setSearchSourceBuilder(queryAction.getRequestBuilder().request().source());

    if (response.getHits().getHits().length > 0) {
      cursor.setSortFields(
          response.getHits().getAt(response.getHits().getHits().length - 1).getSortValues());
    }

    return cursor;
  }

  protected boolean shouldCreateCursor(
      SearchResponse searchResponse, DefaultQueryAction queryAction, Integer fetchSize) {
    return fetchSize != null
        && searchResponse.getHits() != null
        && Objects.requireNonNull(searchResponse.getHits().getTotalHits()).value() >= fetchSize;
  }
}
