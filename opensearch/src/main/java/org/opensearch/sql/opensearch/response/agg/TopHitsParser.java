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
  private final boolean flatten; // used in Composite + TopHits Aggregate

  public TopHitsParser(String name) {
    this(name, false, false);
  }

  public TopHitsParser(String name, boolean returnSingleValue) {
    this(name, returnSingleValue, false);
  }

  public TopHitsParser(String name, boolean returnSingleValue, boolean flatten) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
    this.flatten = flatten;
  }

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonMap(agg.getName(), null);
    }

    if (hits[0].getFields() == null || hits[0].getFields().isEmpty()) {
      return Collections.singletonMap(agg.getName(), Collections.emptyList());
    }
    if (returnSingleValue) {
      // Extract the single value from the first (and only) hit from fields (fetchField)
      Object value = hits[0].getFields().values().iterator().next().getValue();
      return Collections.singletonMap(agg.getName(), value);
    } else if (flatten) {
      return hits[0].getFields().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    } else {
      // Return all values as a list from fields (fetchField)
      return Collections.singletonMap(
          agg.getName(),
          Arrays.stream(hits)
              .flatMap(h -> h.getFields().values().stream())
              .map(f -> f.getValue())
              .filter(v -> v != null) // Filter out null values
              .collect(Collectors.toList()));
    }
  }
}
