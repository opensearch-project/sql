/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

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
   * @param indexName index being scanned
   * @param pageSize page size
   * @param exprValueFactory value factory
   */
  // TODO accept indexName as string (same way as `OpenSearchRequestBuilder` does)?
  public InitialPageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                   int pageSize,
                                   Settings settings,
                                   OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.exprValueFactory = exprValueFactory;
    this.scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    this.sourceBuilder = new SearchSourceBuilder()
        .from(0)
        .size(pageSize)
        .timeout(DEFAULT_QUERY_TIMEOUT);
  }

  @Override
  public OpenSearchScrollRequest build() {
    return new OpenSearchScrollRequest(indexName, scrollTimeout, sourceBuilder, exprValueFactory);
  }

  @Override
  public void pushDownAggregation(Pair<List<AggregationBuilder>,
                                      OpenSearchAggregationResponseParser> aggregationBuilder) {
    throw new UnsupportedOperationException("Pagination does not support aggregations");
  }

  @Override
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    throw new UnsupportedOperationException("Pagination does not support sort (ORDER BY clause)");
  }

  @Override
  public void pushDownLimit(Integer limit, Integer offset) {
    throw new UnsupportedOperationException("Pagination does not support limit (LIMIT clause)");
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
  public void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs) {
    throw new UnsupportedOperationException("Pagination does not support nested function");
  }

  @Override
  public void pushDownTrackedScore(boolean trackScores) {
    throw new UnsupportedOperationException("Pagination does not support score function");
  }

  @Override
  public void pushDownFilter(QueryBuilder query) {
    QueryBuilder current = sourceBuilder.query();

    if (current == null) {
      sourceBuilder.query(query);
    } else {
      if (isBoolFilterQuery(current)) {
        ((BoolQueryBuilder) current).filter(query);
      } else {
        sourceBuilder.query(QueryBuilders.boolQuery()
            .filter(current)
            .filter(query));
      }
    }

    if (sourceBuilder.sorts() == null) {
      sourceBuilder.sort(DOC_FIELD_NAME, ASC); // Make sure consistent order
    }
  }

  @Override
  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    HighlightBuilder highlightBuilder;
    String unquotedField = StringUtils.unquoteText(field);
    if (sourceBuilder.highlighter() != null) {
      // OS does not allow duplicates of highlight fields
      if (sourceBuilder.highlighter().fields().stream()
          .anyMatch(f -> f.name().equals(unquotedField))) {
        throw new SemanticCheckException(String.format(
            "Duplicate field %s in highlight", field));
      }
      highlightBuilder = sourceBuilder.highlighter().field(unquotedField);
    } else {
      highlightBuilder = new HighlightBuilder().field(unquotedField);
      sourceBuilder.highlighter(highlightBuilder);
    }

    if (arguments.containsKey("pre_tags")) {
      highlightBuilder.preTags(arguments.get("pre_tags").toString());
    }
    if (arguments.containsKey("post_tags")) {
      highlightBuilder.postTags(arguments.get("post_tags").toString());
    }
  }
}
