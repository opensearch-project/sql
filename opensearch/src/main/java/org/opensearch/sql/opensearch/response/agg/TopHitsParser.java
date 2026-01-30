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
      Object value = null;
      if (!isSourceEmpty(hits)) {
        // Extract the single value from the first (and only) hit from source (fetchSource)
        value = getLeafValue(hits[0].getSourceAsMap().values().iterator().next());
      }
      if (!isFieldsEmpty(hits)) {
        // Extract the single value from the first (and only) hit from fields (fetchField)
        value = hits[0].getFields().values().iterator().next().getValue();
      }
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), value)));
    } else if (returnMergeValue) {
      if (isEmptyHits(hits)) {
        return Collections.singletonList(
            new HashMap<>(Collections.singletonMap(agg.getName(), Collections.emptyList())));
      }
      List<Object> list = Collections.emptyList();
      if (!isSourceEmpty(hits)) {
        // Return all values as a list from _source (fetchSource)
        list =
            Arrays.stream(hits)
                .map(SearchHit::getSourceAsMap)
                .filter(Objects::nonNull)
                .flatMap(map -> map.values().stream())
                .filter(Objects::nonNull)
                .toList();
      }
      if (!isFieldsEmpty(hits)) {
        // Return all values as a list from fields (fetchField)
        list =
            Arrays.stream(hits)
                .flatMap(h -> h.getFields().values().stream())
                .map(DocumentField::getValue)
                .filter(Objects::nonNull)
                .toList();
      }
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), list)));
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

  private boolean isEmptyHits(SearchHit[] hits) {
    return isFieldsEmpty(hits) && isSourceEmpty(hits);
  }

  private boolean isFieldsEmpty(SearchHit[] hits) {
    return hits[0].getFields().isEmpty();
  }

  private boolean isSourceEmpty(SearchHit[] hits) {
    return hits[0].getSourceAsMap() == null || hits[0].getSourceAsMap().isEmpty();
  }

  private Object getLeafValue(Object object) {
    if (object instanceof Map map) {
      return getLeafValue(map.values().iterator().next());
    } else {
      return object;
    }
  }
}
