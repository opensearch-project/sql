/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

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
    this.format = format.toLowerCase();
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
    } catch (Exception e) {
      if (e instanceof OpenSearchException) {
        LOG.warn(
            "An error occurred in OpenSearch engine: "
                + ((OpenSearchException) e).getDetailedMessage(),
            e);
      } else {
        LOG.warn("Error happened in pretty formatter", e);
      }
      protocol = new Protocol(e);
    }

    return protocol.format();
  }

  /**
   * QueryActionElasticExecutor.executeAnyAction() returns SearchHits inside SearchResponse. In
   * order to get scroll ID if any, we need to execute DefaultQueryAction ourselves for
   * SearchResponse.
   *
   * <p>This method conditionally creates PIT only when pagination is requested (fetch_size > 0) and
   * ensures proper cleanup of PIT resources when they are not used for cursor-based pagination.
   */
  private Protocol buildProtocolForDefaultQuery(Client client, DefaultQueryAction queryAction)
      throws SqlParseException {

    PointInTimeHandler pit = null;
    SearchResponse response;
    Protocol protocol;
    boolean cursorCreated = false;

    queryAction.explain();

    Integer fetchSize = queryAction.getSqlRequest().fetchSize();
    if (fetchSize != null && fetchSize > 0) {
      pit = new PointInTimeHandlerImpl(client, queryAction.getSelect().getIndexArr());
      pit.create();
    }

    try {
      SearchRequestBuilder searchRequest = queryAction.getRequestBuilder();
      if (pit != null) {
        searchRequest.setPointInTime(new PointInTimeBuilder(pit.getPitId()));
      }
      response = searchRequest.get();

      if (pit != null && isDefaultCursor(response, queryAction)) {
        DefaultCursor defaultCursor = new DefaultCursor();
        defaultCursor.setLimit(queryAction.getSelect().getRowCount());
        defaultCursor.setFetchSize(fetchSize);
        defaultCursor.setPitId(pit.getPitId());
        defaultCursor.setSearchSourceBuilder(queryAction.getRequestBuilder().request().source());
        defaultCursor.setSortFields(
            response.getHits().getAt(response.getHits().getHits().length - 1).getSortValues());

        protocol = new Protocol(client, queryAction, response.getHits(), format, defaultCursor);
        cursorCreated = true;
      } else {
        protocol =
            new Protocol(client, queryAction, response.getHits(), format, Cursor.NULL_CURSOR);
      }
    } catch (Exception e) {
      if (pit != null) {
        try {
          pit.delete();
          pit = null; // Prevent double deletion in finally
        } catch (RuntimeException cleanupException) {
          LOG.error("Failed to delete PIT during exception handling", cleanupException);
          Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        }
      }
      throw e;
    } finally {
      // Cursor owns PIT lifecycle when created; otherwise clean up here
      if (pit != null && !cursorCreated) {
        try {
          pit.delete();
        } catch (RuntimeException e) {
          LOG.error("Failed to delete PIT in finally block", e);
          Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        }
      }
    }

    return protocol;
  }

  protected boolean isDefaultCursor(SearchResponse searchResponse, DefaultQueryAction queryAction) {
    Integer fetchSize = queryAction.getSqlRequest().fetchSize();
    return fetchSize != null
        && fetchSize > 0
        && Objects.requireNonNull(searchResponse.getHits().getTotalHits()).value() >= fetchSize;
  }
}
