/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;

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
   * nothing upstream bounds this recursion -- the depth comes from the document, not the mapping.
   * The cap is the default of {@code index.mapping.depth.limit}, which is the depth a mapped object
   * is allowed, so a flat_object is read as deeply as one. Nothing is dropped at the cap: the
   * object that would have been descended into is kept as one leaf, holding its own JSON as the
   * value.
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
    LinkedHashMap<String, List<String>> leaves = collect(content);
    return leaves == null ? ExprNullValue.of() : ExprTupleValue.fromExprValueMap(render(leaves));
  }

  /**
   * The same flattening as {@link #flatten}, as plain text, for a pushed-down script. A script
   * reads the field out of {@code _source}, which holds the document as written -- a leaf would not
   * be found under its dotted path, and an array at the root would not even be a map -- so it has
   * to be given the same map the engine presents.
   *
   * @param source the field's value as {@code _source} holds it
   * @return the leaves by dotted path, or null if the value is neither an object nor an array
   */
  public static @Nullable Map<String, String> flattenToText(Object source) {
    LinkedHashMap<String, List<String>> leaves = collect(new ObjectContent(source));
    if (leaves == null) {
      return null;
    }
    LinkedHashMap<String, String> out = new LinkedHashMap<>();
    leaves.forEach((key, values) -> out.put(key, text(values)));
    return out;
  }

  /** The leaves a value holds, by dotted path; null when it is neither an object nor an array. */
  private static @Nullable LinkedHashMap<String, List<String>> collect(Content content) {
    if (content == null || content.isNull()) {
      return null;
    }
    LinkedHashMap<String, List<String>> leaves = new LinkedHashMap<>();
    if (content.isObject()) {
      flattenInto(content, "", leaves, 0);
    } else if (content.isArray()) {
      // OpenSearch accepts an array at the top level and folds every element's paths into the same
      // terms, so the elements merge into one map here too -- including an element that is itself
      // an array, which the index flattens just the same.
      flattenRoot(content, leaves);
    } else {
      return null;
    }
    return leaves;
  }

  /** Every object reachable through the arrays at the root, merged into one map. */
  private static void flattenRoot(Content content, LinkedHashMap<String, List<String>> leaves) {
    content
        .array()
        .forEachRemaining(
            element -> {
              if (element.isObject()) {
                flattenInto(element, "", leaves, 0);
              } else if (element.isArray()) {
                flattenRoot(element, leaves);
              }
            });
  }

  private static void flattenInto(
      Content content, String prefix, LinkedHashMap<String, List<String>> out, int depth) {
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
      Content value, String key, LinkedHashMap<String, List<String>> out, int depth) {
    if (value.isObject() && depth < MAX_DEPTH) {
      flattenInto(value, key, out, depth + 1);
      return;
    }
    if (value.isArray()) {
      value.array().forEachRemaining(element -> fold(element, key, out, depth));
      return;
    }
    add(out, key, token(value));
  }

  /**
   * A leaf as the text the index holds for it, or null when nothing was written. The index has no
   * type for a leaf -- every value is a keyword term -- so the engine presents the same text, and a
   * query reads and compares exactly what a lookup on the field would match.
   */
  private static @Nullable String token(Content value) {
    ExprValue parsed = OpenSearchExprValueFactory.parseContent(value);
    return parsed.isNull() ? null : String.valueOf(parsed.value());
  }

  /** Notes one more value for a path. A path can be written more than once; each value is kept. */
  private static void add(
      LinkedHashMap<String, List<String>> out, String key, @Nullable String value) {
    List<String> values = out.computeIfAbsent(key, ignored -> new ArrayList<>());
    if (value != null) {
      values.add(value);
    }
  }

  /**
   * A path written once reads as its value. A path written more than once -- both spellings of a
   * dotted path in one document, or one path across the elements of an array -- reads as the JSON
   * of all of them, the way {@code spath} reads an array out of a JSON string; the index files a
   * term for each and answers a lookup for any of them. A path whose only value was null reads as
   * null.
   */
  private static LinkedHashMap<String, ExprValue> render(
      LinkedHashMap<String, List<String>> leaves) {
    LinkedHashMap<String, ExprValue> out = new LinkedHashMap<>();
    leaves.forEach(
        (key, values) -> {
          String rendered = text(values);
          out.put(key, rendered == null ? ExprNullValue.of() : new ExprStringValue(rendered));
        });
    return out;
  }

  /** One value reads as itself, several as their JSON, none as null. */
  private static @Nullable String text(List<String> values) {
    return switch (values.size()) {
      case 0 -> null;
      case 1 -> values.get(0);
      default -> asJsonArray(values);
    };
  }

  private static String asJsonArray(List<String> values) {
    try {
      return OBJECT_MAPPER.writeValueAsString(values);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot render a flat_object leaf as JSON", e);
    }
  }
}
