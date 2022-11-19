/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.storage.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.TableScanBuilder;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Index scan builder for simple non-aggregate query used by
 * {@link OpenSearchIndexScanBuilder} internally.
 */
@VisibleForTesting
class OpenSearchSimpleIndexScanBuilder extends TableScanBuilder {

  /** OpenSearch index scan to be optimized. */
  @EqualsAndHashCode.Include
  private final OpenSearchIndexScan indexScan;

  /**
   * Initialize with given index scan and perform push-down optimization later.
   *
   * @param indexScan index scan not optimized yet
   */
  OpenSearchSimpleIndexScanBuilder(OpenSearchIndexScan indexScan) {
    this.indexScan = indexScan;
  }

  @Override
  public TableScanOperator build() {
    return indexScan;
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
        new DefaultExpressionSerializer());
    QueryBuilder query = queryBuilder.build(filter.getCondition());
    indexScan.getRequestBuilder().pushDown(query);
    return true;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    if (!sortByFieldsOnly(sort)) {
      return false;
    }

    List<Pair<Sort.SortOption, Expression>> sortList = sort.getSortList();
    final SortQueryBuilder builder = new SortQueryBuilder();
    indexScan.getRequestBuilder().pushDownSort(sortList.stream()
        .map(sortItem -> builder.build(sortItem.getValue(), sortItem.getKey()))
        .collect(Collectors.toList()));
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    indexScan.getRequestBuilder().pushDownLimit(limit.getLimit(), limit.getOffset());
    return true;
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    indexScan.getRequestBuilder().pushDownProjects(
        findReferenceExpressions(project.getProjectList()));

    // Return false intentionally to keep the original project operator
    return false;
  }

  private boolean sortByFieldsOnly(LogicalSort sort) {
    return sort.getSortList().stream()
        .map(sortItem -> sortItem.getRight() instanceof ReferenceExpression)
        .reduce(true, Boolean::logicalAnd);
  }

  /**
   * Find reference expression from expression.
   * @param expressions a list of expression.
   *
   * @return a list of ReferenceExpression
   */
  public static Set<ReferenceExpression> findReferenceExpressions(
      List<NamedExpression> expressions) {
    Set<ReferenceExpression> projectList = new HashSet<>();
    for (NamedExpression namedExpression : expressions) {
      projectList.addAll(findReferenceExpression(namedExpression));
    }
    return projectList;
  }

  /**
   * Find reference expression from expression.
   * @param expression expression.
   *
   * @return a list of ReferenceExpression
   */
  public static List<ReferenceExpression> findReferenceExpression(NamedExpression expression) {
    List<ReferenceExpression> results = new ArrayList<>();
    expression.accept(new ExpressionNodeVisitor<>() {
      @Override
      public Object visitReference(ReferenceExpression node, Object context) {
        return results.add(node);
      }
    }, null);
    return results;
  }
}
