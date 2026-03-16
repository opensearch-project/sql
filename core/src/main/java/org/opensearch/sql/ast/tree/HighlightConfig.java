/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.List;

/**
 * Bundles highlight configuration: field names (or wildcards) and optional pre/post tags and
 * fragment size. Supports both the simple array format ({@code ["*"]}) and the rich OSD object
 * format with {@code pre_tags}, {@code post_tags}, {@code fields}, and {@code fragment_size}.
 */
public record HighlightConfig(
    List<String> fields, List<String> preTags, List<String> postTags, Integer fragmentSize) {

  /** Convenience constructor for the simple array format (fields only, no tag/size overrides). */
  public HighlightConfig(List<String> fields) {
    this(fields, null, null, null);
  }
}
