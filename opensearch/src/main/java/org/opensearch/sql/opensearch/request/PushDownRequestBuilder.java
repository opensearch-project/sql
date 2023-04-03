/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

public interface PushDownRequestBuilder {

  default boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  private String throwUnsupported(String operation) {
    return String.format("%s: push down %s in cursor requests is not supported",
        getClass().getSimpleName(), operation);
  }

  default void pushDownFilter(QueryBuilder query) {
    throw new UnsupportedOperationException(throwUnsupported("filter"));
  }

  default void pushDownAggregation(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder) {
    throw new UnsupportedOperationException(throwUnsupported("aggregation"));
  }

  default void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throw new UnsupportedOperationException(throwUnsupported("sort"));
  }

  default void pushDownLimit(Integer limit, Integer offset) {
    throw new UnsupportedOperationException(throwUnsupported("limit"));
  }

  default void pushDownHighlight(String field, Map<String, Literal> arguments) {
    throw new UnsupportedOperationException(throwUnsupported("highlight"));
  }

  default void pushDownProjects(Set<ReferenceExpression> projects) {
    throw new UnsupportedOperationException(throwUnsupported("projects"));
  }

  default void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    throw new UnsupportedOperationException(throwUnsupported("type mapping"));
  }

  default void pushDownPageSize(int pageSize) {
    throw new UnsupportedOperationException(throwUnsupported("type mapping"));
  }
}
