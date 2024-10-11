/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.scroll;

import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.physical.node.Paginate;

import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;

/** OpenSearch Scroll API as physical implementation of TableScan */
public class Scroll extends Paginate {

  public Scroll(TableInJoinRequestBuilder request, int pageSize) {
    super(request, pageSize);
  }

  @Override
  public void close() {
    if (searchResponse != null) {
      LOG.debug("Closing all scroll resources");
      ClearScrollResponse clearScrollResponse =
          client.prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
      if (!clearScrollResponse.isSucceeded()) {
        LOG.warn("Failed to close scroll: {}", clearScrollResponse.status());
      }
      searchResponse = null;
    } else {
      LOG.debug("Scroll already be closed");
    }
  }

  @Override
  protected void loadFirstBatch() {
    searchResponse =
        request
            .getRequestBuilder()
            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
            .addSort(METADATA_FIELD_ID, SortOrder.ASC)
            .setSize(pageSize)
            .setScroll(TimeValue.timeValueSeconds(timeout))
            .get();
  }

  @Override
  protected void loadNextBatch() {
    searchResponse =
        client
            .prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(TimeValue.timeValueSeconds(timeout))
            .get();
  }
}
