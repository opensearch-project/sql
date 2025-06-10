/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.PushDownUnSupportedException;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Index scan builder for simple non-aggregate query used by {@link OpenSearchIndexScanBuilder}
 * internally.
 */
@VisibleForTesting
@EqualsAndHashCode
class OpenSearchIndexScanQueryBuilder implements PushDownQueryBuilder {
  private static final Logger LOG = LogManager.getLogger(OpenSearchIndexScanQueryBuilder.class);

  final OpenSearchRequestBuilder requestBuilder;

  public OpenSearchIndexScanQueryBuilder(OpenSearchRequestBuilder requestBuilder) {
    this.requestBuilder = requestBuilder;
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
    Expression queryCondition = filter.getCondition();
    QueryBuilder query = queryBuilder.build(queryCondition);
    requestBuilder.pushDownFilter(query);
    requestBuilder.pushDownTrackedScore(trackScoresFromOpenSearchFunction(queryCondition));
    return true;
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    List<Pair<Sort.SortOption, Expression>> sortList = sort.getSortList();
    final SortQueryBuilder builder = new SortQueryBuilder();
    requestBuilder.pushDownSort(
        sortList.stream()
            .map(sortItem -> builder.build(sortItem.getValue(), sortItem.getKey()))
            .collect(Collectors.toList()));
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    try {
      requestBuilder.pushDownLimit(limit.getLimit(), limit.getOffset());
      return true;
    } catch (PushDownUnSupportedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown limit {} with offset {}", limit.getLimit(), limit.getOffset(), e);
      } else {
        LOG.info("Cannot pushdown limit {} with offset {}", limit.getLimit(), limit.getOffset());
      }
      return false;
    }
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    requestBuilder.pushDownProjects(findReferenceExpressions(project.getProjectList()));

    // Return false intentionally to keep the original project operator
    return false;
  }

  @Override
  public boolean pushDownHighlight(LogicalHighlight highlight) {
    requestBuilder.pushDownHighlight(
        StringUtils.unquoteText(highlight.getHighlightField().toString()),
        highlight.getArguments());
    return true;
  }

  @Override
  public boolean pushDownPageSize(LogicalPaginate paginate) {
    requestBuilder.pushDownPageSize(paginate.getPageSize());
    return true;
  }

  private boolean trackScoresFromOpenSearchFunction(Expression condition) {
    if (condition instanceof OpenSearchFunctions.OpenSearchFunction
        && ((OpenSearchFunctions.OpenSearchFunction) condition).isScoreTracked()) {
      return true;
    }
    if (condition instanceof FunctionExpression) {
      return ((FunctionExpression) condition)
          .getArguments().stream().anyMatch(this::trackScoresFromOpenSearchFunction);
    }
    return false;
  }

  @Override
  public boolean pushDownNested(LogicalNested nested) {
    requestBuilder.pushDownNested(nested.getFields());
    requestBuilder.pushDownProjects(findReferenceExpressions(nested.getProjectList()));
    // Return false intentionally to keep the original nested operator
    // Since we return false we need to pushDownProject here as it won't be
    // pushed down due to no matching push down rule.
    // TODO: improve LogicalPlanOptimizer pushdown api.
    return false;
  }

  @Override
  public OpenSearchRequestBuilder build() {
    return requestBuilder;
  }

  /**
   * Find reference expression from expression.
   *
   * @param expressions a list of expression.
   * @return a set of ReferenceExpression
   */
  public static Set<ReferenceExpression> findReferenceExpressions(
      List<NamedExpression> expressions) {
    // Use LinkedHashSet to make sure explained OpenSearchRequest included fields in order
    Set<ReferenceExpression> projectList = new LinkedHashSet<>();
    for (NamedExpression namedExpression : expressions) {
      projectList.addAll(findReferenceExpression(namedExpression));
    }
    return projectList;
  }

  /**
   * Find reference expression from expression.
   *
   * @param expression expression.
   * @return a list of ReferenceExpression
   */
  public static List<ReferenceExpression> findReferenceExpression(NamedExpression expression) {
    List<ReferenceExpression> results = new ArrayList<>();
    expression.accept(
        new ExpressionNodeVisitor<>() {
          @Override
          public Object visitReference(ReferenceExpression node, Object context) {
            return results.add(node);
          }
        },
        null);
    return results;
  }
}
