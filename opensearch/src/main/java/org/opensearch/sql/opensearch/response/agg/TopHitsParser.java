/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser. */
@EqualsAndHashCode
public class TopHitsParser implements MetricParser {

  @Getter private final String name;
  private final boolean returnSingleValue;

  public TopHitsParser(String name) {
    this.name = name;
    this.returnSingleValue = false;
  }

  public TopHitsParser(String name, boolean returnSingleValue) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
  }

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    if (returnSingleValue) {
      // Extract the single value from the first (and only) hit from fields (fetchField)
      if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
        Object value = hits[0].getFields().values().iterator().next().getValue();
        return Collections.singletonMap(agg.getName(), value);
      }
      return Collections.singletonMap(agg.getName(), null);
    } else {
      // Return all values as a list from fields (fetchField)
      if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
        return Collections.singletonMap(
            agg.getName(),
            Arrays.stream(hits)
                .flatMap(h -> h.getFields().values().stream())
                .map(f -> f.getValue())
                .filter(v -> v != null) // Filter out null values
                .collect(Collectors.toList()));
      }
      return Collections.singletonMap(agg.getName(), Collections.emptyList());
    }
  }
}
