/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.cursor;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

import java.util.Arrays;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.legacy.cursor.CursorType;
import org.opensearch.sql.legacy.cursor.DefaultCursor;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.executor.format.Protocol;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;

public class CursorResultExecutor implements CursorRestExecutor {

  private String cursorId;
  private Format format;

  private static final Logger LOG = LogManager.getLogger(CursorResultExecutor.class);

  public CursorResultExecutor(String cursorId, Format format) {
    this.cursorId = cursorId;
    this.format = format;
  }

  public void execute(Client client, Map<String, String> params, RestChannel channel)
      throws Exception {
    try {
      String formattedResponse = execute(client, params);
      channel.sendResponse(
          new BytesRestResponse(OK, "application/json; charset=UTF-8", formattedResponse));
    } catch (IllegalArgumentException | JSONException e) {
      Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
      LOG.error("Error parsing the cursor", e);
      channel.sendResponse(new BytesRestResponse(channel, e));
    } catch (OpenSearchException e) {
      int status = (e.status().getStatus());
      if (status > 399 && status < 500) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
      } else if (status > 499) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
      }
      LOG.error("Error completing cursor request", e);
      channel.sendResponse(new BytesRestResponse(channel, e));
    }
  }

  public String execute(Client client, Map<String, String> params) throws Exception {
    /**
     * All cursor's are of the form <cursorType>:<base64 encoded cursor> The serialized form before
     * encoding is upto Cursor implementation
     */
    String[] splittedCursor = cursorId.split(":", 2);

    if (splittedCursor.length != 2) {
      throw new VerificationException("Not able to parse invalid cursor");
    }

    String type = splittedCursor[0];
    CursorType cursorType = CursorType.getById(type);

    switch (cursorType) {
      case DEFAULT:
        DefaultCursor defaultCursor = DefaultCursor.from(splittedCursor[1]);
        return handleDefaultCursorRequest(client, defaultCursor);
      case AGGREGATION:
      case JOIN:
      default:
        throw new VerificationException("Unsupported cursor type [" + type + "]");
    }
  }

  private String handleDefaultCursorRequest(Client client, DefaultCursor cursor) {
    LocalClusterState clusterState = LocalClusterState.state();
    TimeValue paginationTimeout = clusterState.getSettingValue(SQL_CURSOR_KEEP_ALIVE);

    SearchResponse scrollResponse = null;
    if (clusterState.getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      String pitId = cursor.getPitId();
      SearchSourceBuilder source = cursor.getSearchSourceBuilder();
      source.searchAfter(cursor.getSortFields());
      source.pointInTimeBuilder(new PointInTimeBuilder(pitId));
      SearchRequest searchRequest = new SearchRequest();
      searchRequest.source(source);
      scrollResponse = client.search(searchRequest).actionGet();
    } else {
      String previousScrollId = cursor.getScrollId();
      scrollResponse =
          client.prepareSearchScroll(previousScrollId).setScroll(paginationTimeout).get();
    }
    SearchHits searchHits = scrollResponse.getHits();
    SearchHit[] searchHitArray = searchHits.getHits();
    String newScrollId = scrollResponse.getScrollId();
    String newPitId = scrollResponse.pointInTimeId();

    int rowsLeft = (int) cursor.getRowsLeft();
    int fetch = cursor.getFetchSize();

    if (rowsLeft < fetch && rowsLeft < searchHitArray.length) {
      /**
       * This condition implies we are on the last page, and we might need to truncate the result
       * from SearchHit[] Avoid truncating in following two scenarios
       *
       * <ol>
       *   <li>number of rows to be sent equals fetchSize
       *   <li>size of SearchHit[] is already less that rows that needs to be sent
       * </ol>
       *
       * Else truncate to desired number of rows
       */
      SearchHit[] newSearchHits = Arrays.copyOf(searchHitArray, rowsLeft);
      searchHits =
          new SearchHits(newSearchHits, searchHits.getTotalHits(), searchHits.getMaxScore());
    }

    rowsLeft = rowsLeft - fetch;

    if (rowsLeft <= 0) {
      /** Clear the scroll context on last page */
      if (newScrollId != null) {
        ClearScrollResponse clearScrollResponse =
            client.prepareClearScroll().addScrollId(newScrollId).get();
        if (!clearScrollResponse.isSucceeded()) {
          Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
          LOG.info("Error closing the cursor context {} ", newScrollId);
        }
      }
      if (newPitId != null) {
        PointInTimeHandler pit = new PointInTimeHandlerImpl(client, newPitId);
        try {
          pit.delete();
        } catch (RuntimeException e) {
          Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
          LOG.info("Error deleting point in time {} ", newPitId);
        }
      }
    }

    cursor.setRowsLeft(rowsLeft);
    if (clusterState.getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      cursor.setPitId(newPitId);
      cursor.setSearchSourceBuilder(cursor.getSearchSourceBuilder());
      cursor.setSortFields(
          scrollResponse
              .getHits()
              .getAt(scrollResponse.getHits().getHits().length - 1)
              .getSortValues());
    } else {
      cursor.setScrollId(newScrollId);
    }
    Protocol protocol = new Protocol(client, searchHits, format.name().toLowerCase(), cursor);
    return protocol.cursorFormat();
  }
}
