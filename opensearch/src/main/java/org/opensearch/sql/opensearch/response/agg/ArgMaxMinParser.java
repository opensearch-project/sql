/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser for ARG_MAX/ARG_MIN aggregations. */
@Value
public class ArgMaxMinParser implements MetricParser {

  String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }

    // Get value from fields (fetchField)
    List<Map<String, Object>> res =
        Arrays.stream(hits)
            .filter(hit -> hit.getFields() != null && hit.getFields().isEmpty())
            .map(hit -> hit.getFields().values().iterator().next().getValue())
            .map(v -> new HashMap<>(Collections.singletonMap(agg.getName(), v)))
            .map(v -> (Map<String, Object>) v)
            .toList();
    if (!res.isEmpty()) {
      return res;
    } else {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }
  }
}
