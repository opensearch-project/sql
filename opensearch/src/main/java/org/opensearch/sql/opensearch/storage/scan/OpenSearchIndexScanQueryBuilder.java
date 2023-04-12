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
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.opensearch.storage.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Index scan builder for simple non-aggregate query used by
 * {@link OpenSearchIndexScanBuilder} internally.
 */
@VisibleForTesting
class OpenSearchIndexScanQueryBuilder extends TableScanBuilder {

  /** OpenSearch index scan to be optimized. */
  @EqualsAndHashCode.Include
  private final OpenSearchIndexScan indexScan;

  /**
   * Initialize with given index scan and perform push-down optimization later.
   *
   * @param indexScan index scan not optimized yet
   */
  OpenSearchIndexScanQueryBuilder(OpenSearchIndexScan indexScan) {
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
    Expression queryCondition = filter.getCondition();
    QueryBuilder query = queryBuilder.build(queryCondition);
    indexScan.getRequestBuilder().pushDown(query);
    indexScan.getRequestBuilder().pushDownTrackedScore(
        trackScoresFromOpenSearchFunction(queryCondition));
    return true;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
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

  @Override
  public boolean pushDownHighlight(LogicalHighlight highlight) {
    indexScan.getRequestBuilder().pushDownHighlight(
        StringUtils.unquoteText(highlight.getHighlightField().toString()),
        highlight.getArguments());
    return true;
  }

  private boolean trackScoresFromOpenSearchFunction(Expression condition) {
    if (condition instanceof OpenSearchFunctions.OpenSearchFunction
        && ((OpenSearchFunctions.OpenSearchFunction) condition).isScoreTracked()) {
      return true;
    }
    if (condition instanceof FunctionExpression) {
      return ((FunctionExpression) condition).getArguments().stream()
          .anyMatch(this::trackScoresFromOpenSearchFunction);
    }
    return false;
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
