/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

public interface PushDownTranslator {
  boolean pushDownFilter(LogicalFilter filter);

  boolean pushDownSort(LogicalSort sort);

  boolean pushDownLimit(LogicalLimit limit);

  boolean pushDownProject(LogicalProject project);

  boolean pushDownHighlight(LogicalHighlight highlight);

  boolean pushDownPageSize(LogicalPaginate paginate);

  boolean pushDownNested(LogicalNested nested);

  boolean pushDownAggregation(LogicalAggregation aggregation);

  OpenSearchRequestBuilder build();
}
