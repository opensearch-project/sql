/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.*;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

/**
 * OpenSearch search request builder.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchRequestBuilder implements PushDownRequestBuilder {

  /**
   * Default query timeout in minutes.
   */
  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * Search request source builder.
   */
  private final SearchSourceBuilder sourceBuilder;

  /**
   * Query size of the request -- how many rows will be returned.
   */
  private int querySize;

  /**
   * Size of each page request to return.
   */
  private Optional<Integer> pageSize = Optional.empty();

  /**
   * OpenSearchExprValueFactory.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;
  private int startFrom = 0;

  /**
   * Constructor.
   */
  public OpenSearchRequestBuilder(int querySize, OpenSearchExprValueFactory exprValueFactory) {
    this.querySize = querySize;
    this.sourceBuilder = new SearchSourceBuilder()
        .from(startFrom)
        .timeout(DEFAULT_QUERY_TIMEOUT)
        .trackScores(false);
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public int getQuerySize() {
    return pageSize.orElse(querySize);
  }
  /**
   * Build DSL request.
   *
   * @return query request or scroll request
   */
  @Override
  public OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                                 int maxResultWindow,
                                 Settings settings) {
    int size = querySize;
    TimeValue scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    if (pageSize.isEmpty()) {
      if (startFrom + size > maxResultWindow) {
        sourceBuilder.size(maxResultWindow - startFrom);
        return new OpenSearchScrollRequest(
            indexName, scrollTimeout, sourceBuilder, exprValueFactory);
      } else {
        sourceBuilder.from(startFrom);
        sourceBuilder.size(size);
        return new OpenSearchQueryRequest(indexName, sourceBuilder, exprValueFactory);
      }
    } else {
      if (startFrom != 0) {
        throw new UnsupportedOperationException("Non-zero offset is not supported with pagination");
      }
      sourceBuilder.size(pageSize.get());
      return new OpenSearchScrollRequest(indexName, scrollTimeout,
          sourceBuilder, exprValueFactory);
    }
  }


  boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }
  /**
   * Push down query to DSL request.
   *
   * @param query  query request
   */
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

  /**
   * Push down aggregation to DSL request.
   *
   * @param aggregationBuilder pair of aggregation query and aggregation parser.
   */
  @Override
  public void pushDownAggregation(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder) {
    aggregationBuilder.getLeft().forEach(sourceBuilder::aggregation);
    sourceBuilder.size(0);
    exprValueFactory.setParser(aggregationBuilder.getRight());
  }

  /**
   * Push down sort to DSL request.
   *
   * @param sortBuilders sortBuilders.
   */
  @Override
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    // TODO: Sort by _doc is added when filter push down. Remove both logic once doctest fixed.
    if (isSortByDocOnly()) {
      sourceBuilder.sorts().clear();
    }

    for (SortBuilder<?> sortBuilder : sortBuilders) {
      sourceBuilder.sort(sortBuilder);
    }
  }

  /**
   * Push down size (limit) and from (offset) to DSL request.
   */
  @Override
  public void pushDownLimit(Integer limit, Integer offset) {
    querySize = limit;
    startFrom = offset;
    sourceBuilder.from(offset).size(limit);
  }

  @Override
  public void pushDownTrackedScore(boolean trackScores) {
    sourceBuilder.trackScores(trackScores);
  }

  @Override
  public void pushDownPageSize(int pageSize) {
    this.pageSize = Optional.of(pageSize);
  }

  /**
   * Add highlight to DSL requests.
   * @param field name of the field to highlight
   */
  @Override
  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    String unquotedField = StringUtils.unquoteText(field);
    if (sourceBuilder.highlighter() != null) {
      // OS does not allow duplicates of highlight fields
      if (sourceBuilder.highlighter().fields().stream()
          .anyMatch(f -> f.name().equals(unquotedField))) {
        throw new SemanticCheckException(String.format(
            "Duplicate field %s in highlight", field));
      }

      sourceBuilder.highlighter().field(unquotedField);
    } else {
      HighlightBuilder highlightBuilder =
          new HighlightBuilder().field(unquotedField);
      sourceBuilder.highlighter(highlightBuilder);
    }

    // lastFieldIndex denotes previously set highlighter with field parameter
    int lastFieldIndex = sourceBuilder.highlighter().fields().size() - 1;
    if (arguments.containsKey("pre_tags")) {
      sourceBuilder.highlighter().fields().get(lastFieldIndex)
          .preTags(arguments.get("pre_tags").toString());
    }
    if (arguments.containsKey("post_tags")) {
      sourceBuilder.highlighter().fields().get(lastFieldIndex)
          .postTags(arguments.get("post_tags").toString());
    }
  }

  /**
   * Push down project list to DSL requests.
   */
  @Override
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    sourceBuilder.fetchSource(projects.stream().map(ReferenceExpression::getAttr).distinct().toArray(String[]::new),
      new String[0]);
  }

  @Override
  public void pushTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    exprValueFactory.extendTypeMapping(typeMapping);
  }

  private boolean isSortByDocOnly() {
    List<SortBuilder<?>> sorts = sourceBuilder.sorts();
    if (sorts != null) {
      return sorts.equals(List.of(SortBuilders.fieldSort(DOC_FIELD_NAME)));
    }
    return false;
  }

  /**
   * Push down nested to sourceBuilder.
   * @param nestedArgs : Nested arguments to push down.
   */
  @Override
  public void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs) {
    initBoolQueryFilter();
    groupFieldNamesByPath(nestedArgs).forEach(
          (path, fieldNames) -> buildInnerHit(
              fieldNames, createEmptyNestedQuery(path)
          )
    );
  }

  /**
   * Initialize bool query for push down.
   */
  private void initBoolQueryFilter() {
    if (sourceBuilder.query() == null) {
      sourceBuilder.query(QueryBuilders.boolQuery());
    } else {
      sourceBuilder.query(QueryBuilders.boolQuery().must(sourceBuilder.query()));
    }

    sourceBuilder.query(QueryBuilders.boolQuery().filter(sourceBuilder.query()));
  }

  /**
   * Map all field names in nested queries that use same path.
   * @param fields : Fields for nested queries.
   * @return : Map of path and associated field names.
   */
  private Map<String, List<String>> groupFieldNamesByPath(
      List<Map<String, ReferenceExpression>> fields) {
    // TODO filter out reverse nested when supported - .filter(not(isReverseNested()))
    return fields.stream().collect(
        Collectors.groupingBy(
            m -> m.get("path").toString(),
            mapping(
                m -> m.get("field").toString(),
                toList()
            )
        )
    );
  }

  /**
   * Build inner hits portion to nested query.
   * @param paths : Set of all paths used in nested queries.
   * @param query : Current pushDown query.
   */
  private void buildInnerHit(List<String> paths, NestedQueryBuilder query) {
    query.innerHit(new InnerHitBuilder().setFetchSourceContext(
        new FetchSourceContext(true, paths.toArray(new String[0]), null)
    ));
  }

  /**
   * Create a nested query with match all filter to place inner hits.
   */
  private NestedQueryBuilder createEmptyNestedQuery(String path) {
    NestedQueryBuilder nestedQuery = nestedQuery(path, matchAllQuery(), ScoreMode.None);
    ((BoolQueryBuilder) query().filter().get(0)).must(nestedQuery);
    return nestedQuery;
  }

  /**
   * Return current query.
   * @return : Current source builder query.
   */
  private BoolQueryBuilder query() {
    return (BoolQueryBuilder) sourceBuilder.query();
  }
}
