/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;

import java.util.function.Function;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
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

  private final Function<OpenSearchRequestBuilder, OpenSearchIndexScan> scanFactory;

  /** Delegated index scan builder for non-aggregate or aggregate query. */
  @EqualsAndHashCode.Include private PushDownQueryBuilder delegate;

  /** Is limit operator pushed down. */
  private boolean isLimitPushedDown = false;

  /** Constructor used during query execution. */
  public OpenSearchIndexScanBuilder(
      OpenSearchRequestBuilder requestBuilder,
      Function<OpenSearchRequestBuilder, OpenSearchIndexScan> scanFactory) {
    this.delegate = new OpenSearchIndexScanQueryBuilder(requestBuilder);
    this.scanFactory = scanFactory;
  }

  /** Constructor used for unit tests. */
  protected OpenSearchIndexScanBuilder(
      PushDownQueryBuilder translator,
      Function<OpenSearchRequestBuilder, OpenSearchIndexScan> scanFactory) {
    this.delegate = translator;
    this.scanFactory = scanFactory;
  }

  @Override
  public TableScanOperator build() {
    return scanFactory.apply(delegate.build());
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    return delegate.pushDownFilter(filter);
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    if (isLimitPushedDown) {
      return false;
    }

    // Switch to builder for aggregate query which has different push down logic
    //  for later filter, sort and limit operator.
    delegate = new OpenSearchIndexScanAggregationBuilder(delegate.build(), aggregation);
    return true;
  }

  @Override
  public boolean pushDownPageSize(LogicalPaginate paginate) {
    return delegate.pushDownPageSize(paginate);
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
    // Assume limit push down happening on OpenSearchIndexScanQueryBuilder
    isLimitPushedDown = true;
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

  @Override
  public boolean pushDownNested(LogicalNested nested) {
    return delegate.pushDownNested(nested);
  }

  /**
   * Valid if sorting is only by fields.
   *
   * @param sort Logical sort
   * @return True if sorting by fields only
   */
  private boolean sortByFieldsOnly(LogicalSort sort) {
    return sort.getSortList().stream()
        .map(
            sortItem ->
                sortItem.getRight() instanceof ReferenceExpression
                    || isNestedFunction(sortItem.getRight()))
        .reduce(true, Boolean::logicalAnd);
  }
}
