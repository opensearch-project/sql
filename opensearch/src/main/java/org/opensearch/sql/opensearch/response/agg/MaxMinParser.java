/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.Map;
import lombok.Value;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser for MAX/MIN aggregations on text fields. */
@Value
public class MaxMinParser implements MetricParser {

  String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    Map<String, Object> source = hits[0].getSourceAsMap();

    if (source.isEmpty()) {
      return Collections.singletonMap(agg.getName(), null);
    } else {
      Object value = source.values().iterator().next();
      // Convert all values to strings to handle mixed types consistently with lexicographical
      // sorting
      String stringValue = value != null ? value.toString() : null;
      return Collections.singletonMap(agg.getName(), stringValue);
    }
  }
}
