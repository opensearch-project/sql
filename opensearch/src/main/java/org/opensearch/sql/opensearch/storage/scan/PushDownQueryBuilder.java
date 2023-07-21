/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Translates a logical query plan into OpenSearch DSL and an appropriate request.
 */
public interface PushDownQueryBuilder {
  default boolean pushDownFilter(LogicalFilter filter) {
    return false;
  }

  default boolean pushDownSort(LogicalSort sort) {
    return false;
  }

  default boolean pushDownLimit(LogicalLimit limit) {
    return false;
  }

  default boolean pushDownProject(LogicalProject project) {
    return false;
  }

  default boolean pushDownHighlight(LogicalHighlight highlight) {
    return false;
  }

  default boolean pushDownPageSize(LogicalPaginate paginate) {
    return false;
  }

  default boolean pushDownNested(LogicalNested nested) {
    return false;
  }

  OpenSearchRequestBuilder build();
}
