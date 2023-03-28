/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.AggregationQueryBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Index scan builder for aggregate query used by {@link OpenSearchIndexScanBuilder} internally.
 */
class OpenSearchIndexScanAggregationBuilder extends TableScanBuilder {

  /** OpenSearch index scan to be optimized. */
  private final OpenSearchIndexScan indexScan;

  /** Aggregators pushed down. */
  private List<NamedAggregator> aggregatorList;

  /** Grouping items pushed down. */
  private List<NamedExpression> groupByList;

  /** Sorting items pushed down. */
  private List<Pair<Sort.SortOption, Expression>> sortList;

  /**
   * Initialize with given index scan and perform push-down optimization later.
   *
   * @param indexScan index scan not fully optimized yet
   */
  OpenSearchIndexScanAggregationBuilder(OpenSearchIndexScan indexScan) {
    this.indexScan = indexScan;
  }

  @Override
  public TableScanOperator build() {
    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(new DefaultExpressionSerializer());
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
        builder.buildAggregationBuilder(aggregatorList, groupByList, sortList);
    indexScan.getRequestBuilder().pushDownAggregation(aggregationBuilder);
    indexScan.getRequestBuilder().pushTypeMapping(
        builder.buildTypeMapping(aggregatorList, groupByList));
    return indexScan;
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    aggregatorList = aggregation.getAggregatorList();
    groupByList = aggregation.getGroupByList();
    return true;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    if (hasAggregatorInSortBy(sort)) {
      return false;
    }

    sortList = sort.getSortList();
    return true;
  }

  private boolean hasAggregatorInSortBy(LogicalSort sort) {
    final Set<String> aggregatorNames =
        aggregatorList.stream().map(NamedAggregator::getName).collect(Collectors.toSet());
    for (Pair<Sort.SortOption, Expression> sortPair : sort.getSortList()) {
      if (aggregatorNames.contains(((ReferenceExpression) sortPair.getRight()).getAttr())) {
        return true;
      }
    }
    return false;
  }
}
