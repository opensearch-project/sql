/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bundles highlight configuration: field names (or wildcards) with per-field options, and optional
 * global pre/post tags and fragment size. Supports both the simple array format ({@code ["*"]}) and
 * the rich OSD object format with {@code pre_tags}, {@code post_tags}, {@code fields}, and {@code
 * fragment_size}.
 *
 * <p>The {@code fields} map keys are field names or wildcards; the values are per-field option maps
 * that are passed through to the OpenSearch highlight builder (e.g. {@code fragment_size}, {@code
 * number_of_fragments}, {@code type}).
 */
public record HighlightConfig(
    Map<String, Map<String, Object>> fields,
    List<String> preTags,
    List<String> postTags,
    Integer fragmentSize) {

  /** Convenience constructor for the simple array format (fields only, no tag/size overrides). */
  public HighlightConfig(List<String> fieldNames) {
    this(toFieldMap(fieldNames), null, null, null);
  }

  /** Returns the field names as a list (for display and iteration). */
  public List<String> fieldNames() {
    return fields == null ? List.of() : List.copyOf(fields.keySet());
  }

  private static Map<String, Map<String, Object>> toFieldMap(List<String> fieldNames) {
    if (fieldNames == null) {
      return null;
    }
    Map<String, Map<String, Object>> map = new LinkedHashMap<>();
    for (String name : fieldNames) {
      map.put(name, Map.of());
    }
    return map;
  }
}
