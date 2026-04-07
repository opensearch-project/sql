/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;

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

  public VectorSearchQueryBuilder(OpenSearchRequestBuilder requestBuilder, QueryBuilder knnQuery) {
    super(requestBuilder);
    // Set knn as the initial query (scoring context)
    requestBuilder.getSourceBuilder().query(knnQuery);
    this.knnQuery = knnQuery;
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
}
