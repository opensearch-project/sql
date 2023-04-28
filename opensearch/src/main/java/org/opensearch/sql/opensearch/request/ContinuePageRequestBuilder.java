/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds a {@link ContinuePageRequest} to handle subsequent pagination/scroll/cursor requests.
 */
public class ContinuePageRequestBuilder implements PushDownRequestBuilder {

  public static final String PUSH_DOWN_NOT_SUPPORTED =
      "Cursor requests don't support any push down";

  @Getter
  private final String scrollId;
  private final OpenSearchExprValueFactory exprValueFactory;
  private final TimeValue scrollTimeout;

  /** Constructor. */
  public ContinuePageRequestBuilder(String scrollId, TimeValue scrollTimeout,
                                    OpenSearchExprValueFactory exprValueFactory) {
    this.scrollId = scrollId;
    this.scrollTimeout = scrollTimeout;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public  OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                                  int maxResultWindow,
                                  Settings settings) {
    return new ContinuePageRequest(scrollId, scrollTimeout, exprValueFactory);
  }

  @Override
  public int getQuerySize() {
    return Integer.MAX_VALUE;
  }

  @Override
  public void pushDownFilter(QueryBuilder query) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownAggregation(Pair<List<AggregationBuilder>,
                                      OpenSearchAggregationResponseParser> aggregationBuilder) {
    throw new UnsupportedOperationException(ContinuePageRequestBuilder.PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownLimit(Integer limit, Integer offset) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    throw new UnsupportedOperationException(PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs) {
    throw new UnsupportedOperationException(ContinuePageRequestBuilder.PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownTrackedScore(boolean trackScores) {
    throw new UnsupportedOperationException(ContinuePageRequestBuilder.PUSH_DOWN_NOT_SUPPORTED);
  }

  @Override
  public void pushDownPageSize(int pageSize) {
    throw new UnsupportedOperationException(ContinuePageRequestBuilder.PUSH_DOWN_NOT_SUPPORTED);
  }
}
