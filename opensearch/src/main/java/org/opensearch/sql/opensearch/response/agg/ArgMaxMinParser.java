/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser for ARG_MAX/ARG_MIN aggregations. */
@Getter
@EqualsAndHashCode
@ToString
public class ArgMaxMinParser implements MetricParser {

  private final String name;

  /**
   * The path of the field to read from {@code _source}: a knn_vector, which the top_hits fetches
   * from {@code _source} alone. The value is then read from there only, as a hit's {@code fields}
   * can hold meta fields such as {@code _routing}. {@code null} for other fields, read from {@code
   * fields}.
   */
  @Nullable private final String sourcePath;

  /**
   * Whether the row records the index of its hit (see {@link TopHitsParser#HIT_INDEX_KEY}), for a
   * knn_vector whose indices disagree on whether elements are bytes.
   */
  private final boolean recordsHitIndex;

  public ArgMaxMinParser(String name) {
    this(name, null, false);
  }

  public ArgMaxMinParser(String name, @Nullable String sourcePath, boolean recordsHitIndex) {
    this.name = name;
    this.sourcePath = sourcePath;
    this.recordsHitIndex = recordsHitIndex;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }

    if (sourcePath != null) {
      Object value = TopHitsParser.sourceValue(hits[0], sourcePath);
      Map<String, Object> row = new HashMap<>(Collections.singletonMap(agg.getName(), value));
      if (recordsHitIndex && value != null) {
        row.put(TopHitsParser.HIT_INDEX_KEY + agg.getName(), TopHitsParser.indexOf(hits[0]));
      }
      return Collections.singletonList(row);
    } else if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
      // Get value from fields (fetchField)
      Object value = hits[0].getFields().values().iterator().next().getValue();
      return Collections.singletonList(Collections.singletonMap(agg.getName(), value));
    } else {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }
  }
}
