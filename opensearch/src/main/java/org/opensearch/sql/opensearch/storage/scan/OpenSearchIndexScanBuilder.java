/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.storage.OpenSearchIndexScan;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Table scan builder that builds table scan operator for OpenSearch. The actual work is performed
 * by delegated builder internally. This is to avoid conditional check of different push down logic
 * for non-aggregate and aggregate query everywhere.
 */
public class OpenSearchIndexScanBuilder extends TableScanBuilder {

  /**
   * Delegated index scan builder for non-aggregate or aggregate query.
   */
  @EqualsAndHashCode.Include
  private TableScanBuilder delegate;

  private boolean hasLimit = false;

  @VisibleForTesting
  OpenSearchIndexScanBuilder(TableScanBuilder delegate) {
    this.delegate = delegate;
  }

  /**
   * Initialize with given index scan.
   *
   * @param indexScan index scan to optimize
   */
  public OpenSearchIndexScanBuilder(OpenSearchIndexScan indexScan) {
    this.delegate = new OpenSearchIndexScanQueryBuilder(indexScan);
  }

  @Override
  public TableScanOperator build() {
    return delegate.build();
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    return delegate.pushDownFilter(filter);
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    if (hasLimit) {
      return false;
    }

    // Switch to builder for aggregate query which has different push down logic
    //  for later filter, sort and limit operator.
    delegate = new OpenSearchIndexScanAggregationBuilder(
        (OpenSearchIndexScan) delegate.build());

    return delegate.pushDownAggregation(aggregation);
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    if (!sortByFieldsOnly(sort)) {
      return false;
    }
    return delegate.pushDownSort(sort);
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    hasLimit = true;
    return delegate.pushDownLimit(limit);
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    return delegate.pushDownProject(project);
  }

  @Override
  public boolean pushDownHighlight(LogicalHighlight highlight) {
    return delegate.pushDownHighlight(highlight);
  }

  private boolean sortByFieldsOnly(LogicalSort sort) {
    return sort.getSortList().stream()
        .map(sortItem -> sortItem.getRight() instanceof ReferenceExpression)
        .reduce(true, Boolean::logicalAnd);
  }
}
