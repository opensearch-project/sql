/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.pointInTime;

import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;

import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.Config;
import org.opensearch.sql.legacy.query.planner.physical.node.Paginate;

/** OpenSearch Search API with Point in time as physical implementation of TableScan */
public class PointInTime extends Paginate {

  private static final Logger LOG = LogManager.getLogger();

  private String pitId;
  private PointInTimeHandlerImpl pit;
  private final Optional<Config> config;

  public PointInTime(TableInJoinRequestBuilder request, int pageSize) {
    super(request, pageSize);
    this.config = Optional.empty();
  }

  /**
   * Enhanced constructor with Config for custom timeout support
   *
   * @param request Table request builder
   * @param pageSize Page size for pagination
   * @param config Configuration object containing custom PIT settings
   */
  public PointInTime(TableInJoinRequestBuilder request, int pageSize, Config config) {
    super(request, pageSize);
    this.config = Optional.ofNullable(config);
  }

  @Override
  public void close() {
    if (searchResponse != null) {
      LOG.debug("Closing Point In Time (PIT) context");
      // Delete the Point In Time context
      pit.delete();
      searchResponse = null;
    } else {
      LOG.debug("PIT context is already closed or was never opened");
    }
  }

  @Override
  protected void loadFirstBatch() {
    if (config.isPresent()) {
      LOG.debug("PointInTime: Creating PIT with config support");
      pit =
          new PointInTimeHandlerImpl(
              client, request.getOriginalSelect().getIndexArr(), config.get());
    } else {
      LOG.debug("PointInTime: Creating PIT with default settings");
      pit = new PointInTimeHandlerImpl(client, request.getOriginalSelect().getIndexArr());
    }

    pit.create();
    pitId = pit.getPitId();

    LOG.debug("Loading first batch of response using Point In Time");
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

      LOG.debug("Loading next batch of response using Point In Time. - " + pitId);
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
