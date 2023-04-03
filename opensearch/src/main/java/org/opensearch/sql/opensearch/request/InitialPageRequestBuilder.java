/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * This builder assists creating the initial OpenSearch paging (scrolling) request.
 * It is used only on the first page (pagination request).
 * Subsequent requests (cursor requests) use {@link ContinuePageRequestBuilder}.
 */
public class InitialPageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final SearchSourceBuilder sourceBuilder;
  private final OpenSearchExprValueFactory exprValueFactory;
  private final TimeValue scrollTimeout;

  /**
   * Constructor.
   *
   * @param indexName        index being scanned
   * @param exprValueFactory value factory
   */
  // TODO accept indexName as string (same way as `OpenSearchRequestBuilder` does)?
  public InitialPageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                   Settings settings,
                                   OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.exprValueFactory = exprValueFactory;
    this.scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    this.sourceBuilder = new SearchSourceBuilder()
        .from(0)
        .timeout(DEFAULT_QUERY_TIMEOUT);
  }

  @Override
  public OpenSearchScrollRequest build() {
    return new OpenSearchScrollRequest(indexName, scrollTimeout, sourceBuilder, exprValueFactory);
  }

  /**
   * Push down project expression to OpenSearch.
   */
  @Override
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    sourceBuilder.fetchSource(projects.stream().map(ReferenceExpression::getAttr)
        .distinct().toArray(String[]::new), new String[0]);
  }

  @Override
  public void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    exprValueFactory.extendTypeMapping(typeMapping);
  }

  @Override
  public void pushDownPageSize(int pageSize) {
    sourceBuilder.size(pageSize);
  }
}
