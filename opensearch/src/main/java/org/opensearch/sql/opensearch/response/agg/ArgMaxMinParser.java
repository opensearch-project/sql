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

/** {@link TopHits} metric parser for ARG_MAX/ARG_MIN aggregations. */
@Value
public class ArgMaxMinParser implements MetricParser {

  String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    // Get value from fields (fetchField)
    if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
      Object value = hits[0].getFields().values().iterator().next().getValue();
      return Collections.singletonMap(agg.getName(), value);
    }

    return Collections.singletonMap(agg.getName(), null);
  }
}
