/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.sql.exception.ExpressionEvaluationException;

/** Filter placement strategy for vectorSearch() WHERE clauses. */
public enum FilterType {
  /** WHERE placed in bool.filter outside the knn clause (post-filtering). */
  POST("post"),

  /** WHERE placed inside knn.filter for efficient pre-filtering. */
  EFFICIENT("efficient");

  private final String value;

  FilterType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  private static final Set<String> VALID_VALUES =
      Arrays.stream(values()).map(FilterType::getValue).collect(Collectors.toSet());

  public static FilterType fromString(String str) {
    for (FilterType ft : values()) {
      if (ft.value.equals(str)) {
        return ft;
      }
    }
    throw new ExpressionEvaluationException(
        String.format("filter_type must be one of %s, got '%s'", VALID_VALUES, str));
  }
}
