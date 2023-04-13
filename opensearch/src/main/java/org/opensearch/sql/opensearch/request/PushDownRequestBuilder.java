/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

public interface PushDownRequestBuilder {

  default boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  void pushDownFilter(QueryBuilder query);

  void pushDownAggregation(Pair<List<AggregationBuilder>,
                               OpenSearchAggregationResponseParser> aggregationBuilder);

  void pushDownSort(List<SortBuilder<?>> sortBuilders);

  void pushDownLimit(Integer limit, Integer offset);

  void pushDownHighlight(String field, Map<String, Literal> arguments);

  void pushDownProjects(Set<ReferenceExpression> projects);

  void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping);

  void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs);

  void pushDownTrackedScore(boolean trackScores);
}