/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.pointInTime;

import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.physical.node.Paginate;

/** OpenSearch Search API with Point in time as physical implementation of TableScan */
public class PointInTime extends Paginate {
  private static final Logger LOG = LogManager.getLogger();

  private String pitId;
  private PointInTimeHandlerImpl pit;

  public PointInTime(TableInJoinRequestBuilder request, int pageSize) {
    super(request, pageSize);
  }

  @Override
  public void close() {
    // Key on the PIT, not on the search response. loadFirstBatch() creates the PIT before the
    // first search, so a first search that fails (refused, rejected, timed out) leaves
    // searchResponse null while the PIT stays open until its keep-alive expires.
    if (pitId != null) {
      LOG.debug("Closing Point In Time (PIT) context");
      PointInTimeHandlerImpl openPit = pit;
      // Reset first so a repeated close() (the join closes its right side more than once) does
      // not delete the same PIT again, even if this delete fails.
      pit = null;
      pitId = null;
      searchResponse = null;
      openPit.delete();
    } else {
      LOG.debug("PIT context is already closed or was never opened");
    }
  }

  @Override
  protected void loadFirstBatch() {
    // Check if this table has JOIN_TIME_OUT hint configured
    if (request.getHintJoinTimeout() != null) {
      TimeValue customTimeout = request.getHintJoinTimeout();
      LOG.info(
          "PointInTime: Creating PIT with JOIN_TIME_OUT hint: {} seconds",
          customTimeout.getSeconds());
      pit =
          new PointInTimeHandlerImpl(
              client, request.getOriginalSelect().getIndexArr(), customTimeout);
    } else {
      LOG.info("PointInTime: Creating PIT with default timeout value: {}");
      pit = new PointInTimeHandlerImpl(client, request.getOriginalSelect().getIndexArr());
    }

    pit.create();
    pitId = pit.getPitId();

    LOG.info("Loading first batch of response using Point In Time");
    searchResponse =
        request
            .getRequestBuilder()
            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
            .addSort(METADATA_FIELD_ID, SortOrder.ASC)
            .setSize(pageSize)
            .setTimeout(TimeValue.timeValueSeconds(timeout))
            .setPointInTime(new PointInTimeBuilder(pitId))
            .get();
  }

  @Override
  protected void loadNextBatch() {
    // Add PIT with search after to fetch next batch of data
    SearchHit[] hits = searchResponse.getHits().getHits();
    if (hits != null && hits.length > 0) {
      Object[] sortValues = hits[hits.length - 1].getSortValues();

      LOG.info("Loading next batch of response using Point In Time. - " + pitId);
      searchResponse =
          request
              .getRequestBuilder()
              .setSize(pageSize)
              .setTimeout(TimeValue.timeValueSeconds(timeout))
              .setPointInTime(new PointInTimeBuilder(pitId))
              .searchAfter(sortValues)
              .get();
    }
  }
}
