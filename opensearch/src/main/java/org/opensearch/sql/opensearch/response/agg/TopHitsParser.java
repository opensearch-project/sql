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
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.transport.RemoteClusterAware;

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

  /**
   * The key under which a row holds the index of the hit its values come from, when {@link
   * #recordsHitIndex}: a single value under this prefix and the value's name, a dedup row under the
   * prefix alone. The Calcite scan decodes a base64 knn_vector with that index's data type when the
   * indices of the query disagree on whether elements are bytes.
   */
  public static final String HIT_INDEX_KEY = "_hit_index#";

  /**
   * Whether rows record the index of their hit, for a top_hits that reads a knn_vector field whose
   * indices disagree on whether elements are bytes.
   */
  private final boolean recordsHitIndex;

  /**
   * For a single value, the path of the field to read from {@code _source} by name, or {@code
   * null}. Set for a knn_vector field, which the top_hits fetches from {@code _source} alone; the
   * value is then read from there only, as a hit's {@code fields} can hold meta fields such as
   * {@code _routing}. Without it, the single value is the first leaf of the first field in {@code
   * _source}, or the first field in {@code fields}.
   */
  @Nullable private final String sourcePath;

  /**
   * The output columns of a dedup that the top_hits computes as script fields, by the name a hit
   * returns each under in {@code fields}. They are kept apart from the field values the mapping
   * reads: a script column can be named like a field that a renamed column reads.
   */
  private final Map<String, String> scriptFields;

  public TopHitsParser(String name, boolean returnSingleValue, boolean returnMergeValue) {
    this(name, returnSingleValue, returnMergeValue, null);
  }

  public TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      @Nullable Map<String, List<String>> fieldNameMapping) {
    this(name, returnSingleValue, returnMergeValue, fieldNameMapping, false);
  }

  /**
   * @param recordsHitIndex whether rows record the index of their hit (see {@link #HIT_INDEX_KEY})
   */
  public TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      @Nullable Map<String, List<String>> fieldNameMapping,
      boolean recordsHitIndex) {
    this(name, returnSingleValue, returnMergeValue, fieldNameMapping, recordsHitIndex, null);
  }

  /**
   * @param sourcePath for a single value, the path of the field to read from {@code _source} (see
   *     {@link #sourcePath})
   */
  public TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      @Nullable Map<String, List<String>> fieldNameMapping,
      boolean recordsHitIndex,
      @Nullable String sourcePath) {
    this(
        name,
        returnSingleValue,
        returnMergeValue,
        fieldNameMapping,
        recordsHitIndex,
        sourcePath,
        Map.of());
  }

  private TopHitsParser(
      String name,
      boolean returnSingleValue,
      boolean returnMergeValue,
      @Nullable Map<String, List<String>> fieldNameMapping,
      boolean recordsHitIndex,
      @Nullable String sourcePath,
      Map<String, String> scriptFields) {
    this.name = name;
    this.returnSingleValue = returnSingleValue;
    this.returnMergeValue = returnMergeValue;
    this.fieldNameMapping = fieldNameMapping;
    this.recordsHitIndex = recordsHitIndex;
    this.sourcePath = sourcePath;
    this.scriptFields = Map.copyOf(scriptFields);
  }

  /**
   * This parser, for a dedup whose top_hits computes output columns as scripts.
   *
   * @param scriptFields the output columns, by the name a hit returns each under
   */
  public TopHitsParser withScriptFields(Map<String, String> scriptFields) {
    return new TopHitsParser(
        name,
        returnSingleValue,
        returnMergeValue,
        fieldNameMapping,
        recordsHitIndex,
        sourcePath,
        scriptFields);
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
      if (sourcePath != null) {
        value = sourceValue(hits[0], sourcePath);
      } else {
        if (!isSourceEmpty(hits)) {
          // Extract the single value from the first (and only) hit from source (fetchSource)
          value = getLeafValue(hits[0].getSourceAsMap().values().iterator().next());
        }
        if (!isFieldsEmpty(hits)) {
          // Extract the single value from the first (and only) hit from fields (fetchField)
          value = hits[0].getFields().values().iterator().next().getValue();
        }
      }
      Map<String, Object> row = new HashMap<>(Collections.singletonMap(agg.getName(), value));
      if (recordsHitIndex && value != null) {
        row.put(HIT_INDEX_KEY + agg.getName(), indexOf(hits[0]));
      }
      return Collections.singletonList(row);
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
      return Arrays.stream(hits).map(this::dedupRow).toList();
    }
  }

  /**
   * Whether this parser gives rows of a document's values (a dedup), which the response reads as it
   * reads a search hit (see {@code OpenSearchExprValueFactory#constructDocumentValue}): a field of
   * several values gives them all only with field type tolerance, as without pushdown.
   */
  public boolean readsDocuments() {
    return !returnSingleValue && !returnMergeValue;
  }

  /**
   * The row of a dedup hit, with each field's value as a search hit's row reads it from {@code
   * _source}, given to the output columns by {@link #applyFieldNameMapping}. A field of {@code
   * _source} keeps its value as it is, and a field inside an object takes the value at its path
   * (see {@link #pathValue}); only a field that {@code _source} does not have, such as an alias,
   * takes its value from {@code fields}. The fields API gives a field of one value in an array as
   * that value, keeps only the first of several values in {@code DocumentField.getValue()}, and
   * gives a field inside an array of objects the values of every object. The script fields are put
   * under their columns' names after the mapping.
   */
  private Map<String, Object> dedupRow(SearchHit hit) {
    Map<String, Object> source = hit.getSourceAsMap();
    Map<String, Object> values =
        source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
    Map<String, Object> scripts = new LinkedHashMap<>();
    hit.getFields()
        .values()
        .forEach(
            f -> {
              String column = scriptFields.get(f.getName());
              Object value = f.getValues().size() > 1 ? f.getValues() : f.getValue();
              if (column != null) {
                scripts.put(column, value);
                return;
              }
              Object fromSource = source == null ? MISSING : pathValue(source, f.getName());
              values.put(f.getName(), fromSource == MISSING ? value : fromSource);
            });
    Map<String, Object> row = applyFieldNameMapping(values);
    row.putAll(scripts);
    if (recordsHitIndex) {
      row.put(HIT_INDEX_KEY, indexOf(hit));
    }
    return row;
  }

  /** Stands for a path that {@code _source} does not have. */
  private static final Object MISSING = new Object();

  /**
   * The value of the field at {@code path} in a hit's {@code _source}, as the row of a search hit
   * resolves it ({@code ExprValueUtils.resolveRefPaths}): by a dotted key, else through objects,
   * taking the first object of an array; {@link #MISSING} if there is none.
   */
  private static Object pathValue(Map<String, Object> source, String path) {
    return pathValue(source, List.of(path.split("\\.")));
  }

  private static Object pathValue(Object value, List<String> path) {
    if (value instanceof List<?> list) {
      value = list.isEmpty() ? null : list.getFirst();
    }
    if (!(value instanceof Map<?, ?> map)) {
      return MISSING;
    }
    String wholePath = String.join(".", path);
    if (map.containsKey(wholePath)) {
      return map.get(wholePath);
    }
    if (path.size() == 1 || !map.containsKey(path.getFirst())) {
      return MISSING;
    }
    return pathValue(map.get(path.getFirst()), path.subList(1, path.size()));
  }

  /**
   * Gives each output column of {@link #fieldNameMapping} the value of its field, read from the
   * hit's values as they are, whatever the other columns of the mapping do: a field that only
   * mapped columns read is dropped, every mapped column takes its field's value or none, and a
   * column that keeps its field's name is one of the field's columns. This handles the
   * single-rename case (issue #5150), several columns of one field (issue #5197), a column that
   * keeps its field beside a copy or an alias of it, and columns that swap their fields.
   *
   * @param values the hit's field values, which are not changed
   * @return the row
   */
  private Map<String, Object> applyFieldNameMapping(Map<String, Object> values) {
    if (fieldNameMapping == null || fieldNameMapping.isEmpty()) {
      return new LinkedHashMap<>(values);
    }
    Map<String, Object> row = new LinkedHashMap<>(values);
    fieldNameMapping.forEach(
        (originalName, outputNames) -> {
          row.remove(originalName);
          outputNames.forEach(row::remove);
        });
    fieldNameMapping.forEach(
        (originalName, outputNames) -> {
          if (values.containsKey(originalName)) {
            Object value = values.get(originalName);
            outputNames.forEach(outputName -> row.put(outputName, value));
          }
        });
    return row;
  }

  /**
   * The value of the field at {@code path} in a hit's {@code _source}, found by name through nested
   * objects or a dotted key, or {@code null}.
   */
  static @Nullable Object sourceValue(SearchHit hit, String path) {
    Map<String, Object> source = hit.getSourceAsMap();
    return source == null ? null : XContentMapValues.extractValue(path, source);
  }

  /** The index of a hit, named as the index mappings are: a remote one with its cluster alias. */
  static String indexOf(SearchHit hit) {
    return RemoteClusterAware.buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex());
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
