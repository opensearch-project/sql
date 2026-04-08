/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Map;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
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
 * <p>Without this, the default pushDownFilter wraps both queries into bool.filter, which is a
 * non-scoring context.
 */
public class VectorSearchQueryBuilder extends OpenSearchIndexScanQueryBuilder {

  private final QueryBuilder knnQuery;
  private final Map<String, String> options;

  public VectorSearchQueryBuilder(
      OpenSearchRequestBuilder requestBuilder, QueryBuilder knnQuery, Map<String, String> options) {
    super(requestBuilder);
    requestBuilder.getSourceBuilder().query(knnQuery);
    this.knnQuery = knnQuery;
    this.options = options;
  }

  @Override
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
    Expression queryCondition = filter.getCondition();
    QueryBuilder whereQuery = queryBuilder.build(queryCondition);

    // Combine: knn in must (scores), WHERE in filter (no scoring impact)
    BoolQueryBuilder combined = QueryBuilders.boolQuery().must(knnQuery).filter(whereQuery);
    requestBuilder.getSourceBuilder().query(combined);
    return true;
  }

  @Override
  public boolean pushDownLimit(LogicalLimit limit) {
    if (options.containsKey("k")) {
      int k = Integer.parseInt(options.get("k"));
      if (limit.getLimit() > k) {
        throw new ExpressionEvaluationException(
            String.format("LIMIT %d exceeds k=%d in top-k vector search", limit.getLimit(), k));
      }
    }
    return super.pushDownLimit(limit);
  }

  @Override
  public boolean pushDownSort(LogicalSort sort) {
    // Vector search returns results sorted by _score DESC by default.
    // Reject non-trivial sort pushdowns — only _score DESC is meaningful.
    // For now, let the parent handle it; unsupported sort rejection is
    // deferred until we can inspect the sort expression for _score references.
    return super.pushDownSort(sort);
  }
}
