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

/**
 * Builds a {@link ContinuePageRequest} to handle subsequent pagination/scroll/cursor requests.
 * Initial search requests is handled by {@link InitialPageRequestBuilder}.
 */
public class ContinuePageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  @Getter
  private final String scrollId;
  private final TimeValue scrollTimeout;
  private final OpenSearchExprValueFactory exprValueFactory;

  /** Constructor. */
  public ContinuePageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                    String scrollId,
                                    Settings settings,
                                    OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.scrollId = scrollId;
    this.scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchRequest build() {
    return new ContinuePageRequest(scrollId, scrollTimeout, exprValueFactory);
  }

  @Override
  public void pushDownFilter(QueryBuilder query) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownAggregation(Pair<List<AggregationBuilder>,
                                      OpenSearchAggregationResponseParser> aggregationBuilder) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownLimit(Integer limit, Integer offset) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }

  @Override
  public void pushDownTrackedScore(boolean trackScores) {
    throw new UnsupportedOperationException("Cursor requests don't support any push down");
  }
}
