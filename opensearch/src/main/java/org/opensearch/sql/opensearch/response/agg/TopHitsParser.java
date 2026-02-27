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

  /**
   * Mapping from original OpenSearch field names to output field names (e.g., renamed via {@code
   * rename} command). When a field is renamed (e.g., {@code rename value as val}), the top_hits
   * response still contains the original field name ({@code value}), but the output schema expects
   * the renamed name ({@code val}). This mapping enables the translation.
   */
  private final Map<String, String> fieldNameMapping;

  public TopHitsParser(String name, boolean returnSingleValue, boolean returnMergeValue) {
    this(name, returnSingleValue, returnMergeValue, Collections.emptyMap());
  }

  public TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      Map<String, String> fieldNameMapping) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
    this.returnMergeValue = returnMergeValue;
    this.fieldNameMapping = fieldNameMapping;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();

    if (hits.length == 0) {
      return Collections.singletonList(
          new HashMap<>(Collections.singletonMap(agg.getName(), null)));
    }

    // Field name mapping is not applied in returnSingleValue or returnMergeValue paths
    // because they use the aggregation name (agg.getName()) as the map key, not field names.
    // Only the multi-row path below uses actual field names as keys and needs mapping.
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
      //        },
      //        "fields": {
      //          "name": [
      //            "B"
      //          ],
      //          "category": [
      //            "Z"
      //          ]
      //        }
      //      },
      //      {
      //        "_source": {
      //          "name": "A",
      //          "category": "Y"
      //        },
      //        "fields": {
      //          "category": [
      //            "A"
      //          ],
      //          "state": [
      //            "N"
      //          ]
      //        }
      //      }
      //    ]
      // }
      // will converts to:
      // List[
      //   LinkedHashMap["name" -> "B", "category" -> "Z"],
      //   LinkedHashMap["name" -> "A", "category" -> "A", "state" -> "N"]
      // ]
      return Arrays.stream(hits)
          .map(
              hit -> {
                Map<String, Object> source = hit.getSourceAsMap();
                Map<String, Object> map =
                    source == null
                        ? new LinkedHashMap<>()
                        : new LinkedHashMap<>(hit.getSourceAsMap());
                hit.getFields().values().forEach(f -> map.put(f.getName(), f.getValue()));
                return applyFieldNameMapping(map);
              })
          .toList();
    }
  }

  /**
   * Apply field name mapping to translate original OpenSearch field names to output field names.
   * Fields not present in the mapping are kept as-is.
   */
  private Map<String, Object> applyFieldNameMapping(Map<String, Object> map) {
    if (fieldNameMapping.isEmpty()) {
      return map;
    }
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String mappedName = fieldNameMapping.getOrDefault(entry.getKey(), entry.getKey());
      result.put(mappedName, entry.getValue());
    }
    return result;
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
