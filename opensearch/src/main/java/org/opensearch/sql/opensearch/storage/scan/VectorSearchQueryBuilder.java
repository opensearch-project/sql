/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.FilterType;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Query builder for vector search that keeps the knn query in a scoring (must) context and puts
 * WHERE filters in a non-scoring (filter) context. This prevents the knn relevance scores from
 * being destroyed when a WHERE clause is pushed down.
 *
 * <p>Supports two filter placement strategies via {@link FilterType}:
 *
 * <ul>
 *   <li>{@code POST} — WHERE in {@code bool.filter} outside knn (post-filtering, default)
 *   <li>{@code EFFICIENT} — WHERE inside {@code knn.filter} for pre-filtering during ANN search
 * </ul>
 */
public class VectorSearchQueryBuilder extends OpenSearchIndexScanQueryBuilder {

  private final QueryBuilder knnQuery;
  private final Map<String, String> options;
  private final FilterType filterType;
  private final boolean filterTypeExplicit;
  private final Function<QueryBuilder, QueryBuilder> rebuildKnnWithFilter;
  private boolean filterPushed = false;

  /** Full constructor with filter type support. */
  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder,
      QueryBuilder knnQuery,
      Map<String, String> options,
      FilterType filterType,
      boolean filterTypeExplicit,
      Function<QueryBuilder, QueryBuilder> rebuildKnnWithFilter) {
    super(requestBuilder);
    requestBuilder.getSourceBuilder().query(knnQuery);
    this.knnQuery = knnQuery;
    this.options = options;
    this.filterType = filterType != null ? filterType : FilterType.POST;
    this.filterTypeExplicit = filterTypeExplicit;
    this.rebuildKnnWithFilter = rebuildKnnWithFilter;
  }

  /** Backward-compatible constructor — defaults to POST, not explicit. */
  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder, QueryBuilder knnQuery, Map<String, String> options) {
    this(requestBuilder, knnQuery, options, FilterType.POST, false, null);
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
    Expression queryCondition = filter.getCondition();
    QueryBuilder whereQuery = queryBuilder.build(queryCondition);
    filterPushed = true;

    if (filterType == FilterType.EFFICIENT) {
      QueryBuilder rebuiltKnn = rebuildKnnWithFilter.apply(whereQuery);
      requestBuilder.getSourceBuilder().query(rebuiltKnn);
    } else {
      // POST mode: knn in must (scores), WHERE in filter (no scoring impact)
      BoolQueryBuilder combined = QueryBuilders.boolQuery().must(knnQuery).filter(whereQuery);
      requestBuilder.getSourceBuilder().query(combined);
    }
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    validateLimitWithinK(limit.getLimit());
    return super.pushDownLimit(limit);
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    // Vector search returns results sorted by _score DESC by default.
    // Only _score DESC is meaningful; reject all other sort expressions.
    for (Pair<SortOption, Expression> sortItem : sort.getSortList()) {
      Expression expr = sortItem.getRight();
      if (!(expr instanceof ReferenceExpression)
          || !"_score".equals(((ReferenceExpression) expr).getAttr())) {
        throw new ExpressionEvaluationException(
            String.format(
                "vectorSearch only supports ORDER BY _score DESC; "
                    + "unsupported sort expression: %s",
                expr));
      }
      if (sortItem.getLeft().getSortOrder() != Sort.SortOrder.DESC) {
        throw new ExpressionEvaluationException(
            "vectorSearch only supports ORDER BY _score DESC; _score ASC is not supported");
      }
    }
    // _score DESC is the natural knn order — no need to push the sort itself to OpenSearch.
    // Preserve the parent's sort.getCount() → limit pushdown contract: SQL always sets count=0,
    // but PPL or future callers may set a non-zero count to combine sort+limit in one node.
    if (sort.getCount() != 0) {
      validateLimitWithinK(sort.getCount());
      requestBuilder.pushDownLimit(sort.getCount(), 0);
    }
    return true;
  }

  /** Validates that the requested limit does not exceed k in top-k mode. */
  private void validateLimitWithinK(int limit) {
    if (options.containsKey("k")) {
      int k = Integer.parseInt(options.get("k"));
      if (limit > k) {
        throw new ExpressionEvaluationException(
            String.format("LIMIT %d exceeds k=%d in top-k vector search", limit, k));
      }
    }
  }

  @Override
  public OpenSearchRequestBuilder build() {
    if (filterTypeExplicit && !filterPushed) {
      throw new ExpressionEvaluationException("filter_type requires a pushdownable WHERE clause");
    }
    return super.build();
  }
}
