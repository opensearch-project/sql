/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/** {@link TopHits} metric parser. */
@EqualsAndHashCode
public class TopHitsParser implements MetricParser {

  @Getter private final String name;
  private final boolean returnSingleValue;
  private final boolean returnMergeValue;

  public TopHitsParser(String name, boolean returnSingleValue, boolean returnMergeValue) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
    this.returnMergeValue = returnMergeValue;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }

    if (returnSingleValue) {
      if (hits[0].getFields() == null || hits[0].getFields().isEmpty()) {
        return Collections.singletonList(
            new HashMap<>(Collections.singletonMap(agg.getName(), null)));
      }
      // Extract the single value from the first (and only) hit from fields (fetchField)
      Object value = hits[0].getFields().values().iterator().next().getValue();
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), value)));
    } else if (returnMergeValue) {
      if ((hits[0].getFields() == null || hits[0].getFields().isEmpty())
          && (hits[0].getSourceAsMap() == null || hits[0].getSourceAsMap().isEmpty())) {
        return Collections.singletonList(
            new HashMap<>(Collections.singletonMap(agg.getName(), Collections.emptyList())));
      }
      // Return all values as a list from fields (fetchField) and source
      return Collections.singletonList(
          Collections.singletonMap(
              agg.getName(),
              Stream.concat(
                      Arrays.stream(hits)
                          .map(SearchHit::getFields)
                          .filter(Objects::nonNull)
                          .flatMap(map -> map.values().stream())
                          .map(DocumentField::getValue)
                          .filter(Objects::nonNull),
                      Arrays.stream(hits)
                          .map(SearchHit::getSourceAsMap)
                          .filter(Objects::nonNull)
                          .flatMap(map -> map.values().stream())
                          .filter(Objects::nonNull))
                  .collect(Collectors.toList())));

    } else {
      // "hits": {
      //    "hits": [
      //      {
      //        "_source": {
      //          "name": "A",
      //          "category": "X"
      //        }
      //      },
      //      {
      //        "_source": {
      //          "name": "A",
      //          "category": "Y"
      //        }
      //      }
      //    ]
      // }
      // will converts to:
      // List[
      //   LinkedHashMap["name" -> "A", "category" -> "X"],
      //   LinkedHashMap["name" -> "A", "category" -> "Y"]
      // ]
      return Arrays.stream(hits)
          .map(
              hit -> {
                Map<String, Object> map = new LinkedHashMap<>(hit.getSourceAsMap());
                hit.getFields().values().forEach(f -> map.put(f.getName(), f.getValue()));
                return map;
              })
          .toList();
    }
  }
}
