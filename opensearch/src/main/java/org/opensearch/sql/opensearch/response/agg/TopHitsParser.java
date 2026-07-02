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
import javax.annotation.Nullable;
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
   * Maps each original OpenSearch field name to one or more output (renamed) field names. Used by
   * the dedup aggregation pushdown path when {@code rename} creates aliases that differ from the
   * index field name: top_hits returns {@code _source} / {@code fields} entries keyed by the
   * original name, while the Calcite row-type expects the renamed name. A single original field may
   * map to multiple output names when both {@code rename} and an {@code eval} column reference
   * resolve to the same source field (issue #5197).
   */
  @Nullable private final Map<String, List<String>> fieldNameMapping;

  public TopHitsParser(String name, boolean returnSingleValue, boolean returnMergeValue) {
    this(name, returnSingleValue, returnMergeValue, null);
  }

  public TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      @Nullable Map<String, List<String>> fieldNameMapping) {
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
                applyFieldNameMapping(map);
                return map;
              })
          .toList();
    }
  }

  /**
   * Applies {@link #fieldNameMapping} to a parsed hit map in-place.
   *
   * <p>For each {@code (originalName → [outputName1, outputName2, ...])} entry: the value stored
   * under {@code originalName} is copied to every output name, and {@code originalName} is removed
   * unless it is itself one of the expected output names. This handles both the single-rename case
   * (issue #5150) and the many-to-one collision case where two aliases resolve to the same source
   * field (issue #5197).
   */
  private void applyFieldNameMapping(Map<String, Object> map) {
    if (fieldNameMapping == null || fieldNameMapping.isEmpty()) {
      return;
    }
    for (Map.Entry<String, List<String>> entry : fieldNameMapping.entrySet()) {
      String originalName = entry.getKey();
      Object value = map.get(originalName);
      if (value == null && !map.containsKey(originalName)) {
        continue;
      }
      List<String> outputNames = entry.getValue();
      for (String outputName : outputNames) {
        map.put(outputName, value);
      }
      // Remove the original key only when it is not itself one of the expected output names.
      if (!outputNames.contains(originalName)) {
        map.remove(originalName);
      }
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
