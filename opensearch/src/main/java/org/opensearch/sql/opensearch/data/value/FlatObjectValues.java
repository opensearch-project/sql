/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.utils.Content;

/**
 * Flattens a flat_object value into the shape the engine presents it as: a single-level map from
 * dotted leaf path to the text of the value written there.
 *
 * <p>A flat_object declares no sub-fields, so its shape is known only from the document, and the
 * index files one term per leaf with the path folded in. This mirrors that: every way of writing a
 * path reaches the same entry ({@code {"a": {"b": 1}}} and {@code {"a.b": 1}} both give {@code
 * a.b}), a path written more than once holds every value, and each value is the text the index
 * holds for it -- which is also the only form the field can be looked up by.
 */
@UtilityClass
public class FlatObjectValues {

  /**
   * Depth guard. A flat_object exists so that documents can escape the mapping depth limit, so
   * nothing upstream bounds this recursion. Levels beyond the cap are kept as one leaf rather than
   * dropped.
   */
  static final int MAX_DEPTH = 20;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Flatten a parsed flat_object value.
   *
   * @param content the field's value as parsed from {@code _source}
   * @return a tuple keyed by dotted leaf path, or null if the value is neither an object nor an
   *     array (a scalar where the mapping promised an object, e.g. a wildcard over conflicting
   *     mappings)
   */
  public static ExprValue flatten(Content content) {
    if (content == null || content.isNull()) {
      return ExprNullValue.of();
    }
    LinkedHashMap<String, ExprValue> leaves = new LinkedHashMap<>();
    if (content.isObject()) {
      flattenInto(content, "", leaves, 0);
    } else if (content.isArray()) {
      // OpenSearch accepts an array at the top level and folds every element's paths into the same
      // terms, so the elements merge into one map here too.
      content
          .array()
          .forEachRemaining(
              element -> {
                if (element.isObject()) {
                  flattenInto(element, "", leaves, 0);
                }
              });
    } else {
      return ExprNullValue.of();
    }
    return ExprTupleValue.fromExprValueMap(leaves);
  }

  private static void flattenInto(
      Content content, String prefix, LinkedHashMap<String, ExprValue> out, int depth) {
    content
        .map()
        .forEachRemaining(
            entry -> {
              String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
              fold(entry.getValue(), key, out, depth);
            });
  }

  /**
   * Fold one value into the map under {@code key}, the way the index folds it into terms: an object
   * contributes its own leaves under the dotted path, and an array contributes each element under
   * the same path, whatever the element is. So {@code {"a": [1, 2]}}, {@code [{"a": 1}, {"a": 2}]}
   * and {@code {"a": {"b": 1}, "a.b": 2}} all reach the same shape the index holds.
   */
  private static void fold(
      Content value, String key, LinkedHashMap<String, ExprValue> out, int depth) {
    if (value.isObject() && depth < MAX_DEPTH) {
      flattenInto(value, key, out, depth + 1);
      return;
    }
    if (value.isArray()) {
      value.array().forEachRemaining(element -> fold(element, key, out, depth));
      return;
    }
    put(out, key, text(value));
  }

  /**
   * A leaf as the text the index holds for it. The index has no type for a leaf -- every value is a
   * keyword term -- so the engine presents the same text, and a query reads and compares exactly
   * what a lookup on the field would match.
   */
  private static ExprValue text(Content value) {
    ExprValue parsed = OpenSearchExprValueFactory.parseContent(value);
    return parsed.isNull()
        ? ExprNullValue.of()
        : new ExprStringValue(String.valueOf(parsed.value()));
  }

  /**
   * A path written once holds its value. A path written more than once -- both spellings of a
   * dotted path in one document, or one path across the elements of an array -- holds all of them,
   * since the index files a term for each and answers a lookup for any of them, and they read as
   * their JSON, the way {@code spath} reads an array out of a JSON string.
   */
  private static void put(LinkedHashMap<String, ExprValue> out, String key, ExprValue value) {
    ExprValue existing = out.get(key);
    if (existing == null) {
      out.put(key, value);
      return;
    }
    List<String> values = new ArrayList<>();
    collect(existing, values);
    collect(value, values);
    out.put(key, new ExprStringValue(asJsonArray(values)));
  }

  /** The values a key already holds, unwrapped from the JSON an earlier merge wrote. */
  private static void collect(ExprValue value, List<String> into) {
    if (value.isNull()) {
      return;
    }
    String text = value.stringValue();
    if (text.startsWith("[") && text.endsWith("]")) {
      into.addAll(fromJsonArray(text));
    } else {
      into.add(text);
    }
  }

  private static String asJsonArray(List<String> values) {
    try {
      return OBJECT_MAPPER.writeValueAsString(values);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot render a flat_object leaf as JSON", e);
    }
  }

  private static List<String> fromJsonArray(String json) {
    try {
      return OBJECT_MAPPER.readValue(json, new TypeReference<List<String>>() {});
    } catch (JsonProcessingException e) {
      // not JSON this class wrote: a single value that happens to look like an array
      return List.of(json);
    }
  }
}
