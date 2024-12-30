/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/** OpenSearch search request builder. */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchRequestBuilder {

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  /** Query size of the request -- how many rows will be returned. */
  private int requestedTotalSize;

  /** Size of each page request to return. */
  private Integer pageSize = null;

  /** OpenSearchExprValueFactory. */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  private int startFrom = 0;

  private final Settings settings;

  /** Constructor. */
  public OpenSearchRequestBuilder(
      int requestedTotalSize, OpenSearchExprValueFactory exprValueFactory, Settings settings) {
    this.requestedTotalSize = requestedTotalSize;
    this.settings = settings;
    this.sourceBuilder =
        new SearchSourceBuilder()
            .from(startFrom)
            .timeout(OpenSearchRequest.DEFAULT_QUERY_TIMEOUT)
            .trackScores(false);
    this.exprValueFactory = exprValueFactory;
  }

  /**
   * Build DSL request.
   *
   * @return query request with PIT or scroll request
   */
  public OpenSearchRequest build(
      OpenSearchRequest.IndexName indexName,
      int maxResultWindow,
      TimeValue cursorKeepAlive,
      OpenSearchClient client) {
    if (this.settings.getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER)) {
      return buildRequestWithPit(indexName, maxResultWindow, cursorKeepAlive, client);
    } else {
      return buildRequestWithScroll(indexName, maxResultWindow, cursorKeepAlive);
    }
  }

  private OpenSearchRequest buildRequestWithPit(
      OpenSearchRequest.IndexName indexName,
      int maxResultWindow,
      TimeValue cursorKeepAlive,
      OpenSearchClient client) {
    int size = requestedTotalSize;
    FetchSourceContext fetchSource = this.sourceBuilder.fetchSource();
    List<String> includes = fetchSource != null ? Arrays.asList(fetchSource.includes()) : List.of();

    if (pageSize == null) {
      if (startFrom + size > maxResultWindow) {
        sourceBuilder.size(maxResultWindow - startFrom);
        // Search with PIT request
        String pitId = createPit(indexName, cursorKeepAlive, client);
        return new OpenSearchQueryRequest(
            indexName, sourceBuilder, exprValueFactory, includes, cursorKeepAlive, pitId);
      } else {
        sourceBuilder.from(startFrom);
        sourceBuilder.size(requestedTotalSize);
        // Search with non-Pit request
        return new OpenSearchQueryRequest(indexName, sourceBuilder, exprValueFactory, includes);
      }
    } else {
      if (startFrom != 0) {
        throw new UnsupportedOperationException("Non-zero offset is not supported with pagination");
      }
      sourceBuilder.size(pageSize);
      // Search with PIT request
      String pitId = createPit(indexName, cursorKeepAlive, client);
      return new OpenSearchQueryRequest(
          indexName, sourceBuilder, exprValueFactory, includes, cursorKeepAlive, pitId);
    }
  }

  private OpenSearchRequest buildRequestWithScroll(
      OpenSearchRequest.IndexName indexName, int maxResultWindow, TimeValue cursorKeepAlive) {
    int size = requestedTotalSize;
    FetchSourceContext fetchSource = this.sourceBuilder.fetchSource();
    List<String> includes = fetchSource != null ? Arrays.asList(fetchSource.includes()) : List.of();

    if (pageSize == null) {
      if (startFrom + size > maxResultWindow) {
        sourceBuilder.size(maxResultWindow - startFrom);
        return new OpenSearchScrollRequest(
            indexName, cursorKeepAlive, sourceBuilder, exprValueFactory, includes);
      } else {
        sourceBuilder.from(startFrom);
        sourceBuilder.size(requestedTotalSize);
        return new OpenSearchQueryRequest(indexName, sourceBuilder, exprValueFactory, includes);
      }
    } else {
      if (startFrom != 0) {
        throw new UnsupportedOperationException("Non-zero offset is not supported with pagination");
      }
      sourceBuilder.size(pageSize);
      return new OpenSearchScrollRequest(
          indexName, cursorKeepAlive, sourceBuilder, exprValueFactory, includes);
    }
  }

  private String createPit(
      OpenSearchRequest.IndexName indexName, TimeValue cursorKeepAlive, OpenSearchClient client) {
    // Create PIT ID for request
    CreatePitRequest createPitRequest =
        new CreatePitRequest(cursorKeepAlive, false, indexName.getIndexNames());
    return client.createPit(createPitRequest);
  }

  boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  /**
   * Push down query to DSL request.
   *
   * @param query query request
   */
  public void pushDownFilter(QueryBuilder query) {
    QueryBuilder current = sourceBuilder.query();

    if (current == null) {
      sourceBuilder.query(query);
    } else {
      if (isBoolFilterQuery(current)) {
        ((BoolQueryBuilder) current).filter(query);
      } else {
        sourceBuilder.query(QueryBuilders.boolQuery().filter(current).filter(query));
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
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    // TODO: Sort by _doc is added when filter push down. Remove both logic once doctest fixed.
    if (isSortByDocOnly()) {
      sourceBuilder.sorts().clear();
    }

    for (SortBuilder<?> sortBuilder : sortBuilders) {
      sourceBuilder.sort(sortBuilder);
    }
  }

  /** Pushdown size (limit) and from (offset) to DSL request. */
  public void pushDownLimit(Integer limit, Integer offset) {
    requestedTotalSize = limit;
    startFrom = offset;
    sourceBuilder.from(offset).size(limit);
  }

  public void pushDownTrackedScore(boolean trackScores) {
    sourceBuilder.trackScores(trackScores);
  }

  public void pushDownPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  /**
   * Add highlight to DSL requests.
   *
   * @param field name of the field to highlight
   */
  public void pushDownHighlight(String field, Map<String, Literal> arguments) {
    String unquotedField = StringUtils.unquoteText(field);
    if (sourceBuilder.highlighter() != null) {
      // OS does not allow duplicates of highlight fields
      if (sourceBuilder.highlighter().fields().stream()
          .anyMatch(f -> f.name().equals(unquotedField))) {
        throw new SemanticCheckException(String.format("Duplicate field %s in highlight", field));
      }

      sourceBuilder.highlighter().field(unquotedField);
    } else {
      HighlightBuilder highlightBuilder = new HighlightBuilder().field(unquotedField);
      sourceBuilder.highlighter(highlightBuilder);
    }

    // lastFieldIndex denotes previously set highlighter with field parameter
    int lastFieldIndex = sourceBuilder.highlighter().fields().size() - 1;
    if (arguments.containsKey("pre_tags")) {
      sourceBuilder
          .highlighter()
          .fields()
          .get(lastFieldIndex)
          .preTags(arguments.get("pre_tags").toString());
    }
    if (arguments.containsKey("post_tags")) {
      sourceBuilder
          .highlighter()
          .fields()
          .get(lastFieldIndex)
          .postTags(arguments.get("post_tags").toString());
    }
  }

  /** Push down project list to DSL requests. */
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    sourceBuilder.fetchSource(
        projects.stream().map(ReferenceExpression::getAttr).distinct().toArray(String[]::new),
        new String[0]);
  }

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
   *
   * @param nestedArgs : Nested arguments to push down.
   */
  public void pushDownNested(List<Map<String, ReferenceExpression>> nestedArgs) {
    initBoolQueryFilter();
    List<NestedQueryBuilder> nestedQueries = extractNestedQueries(query());
    groupFieldNamesByPath(nestedArgs)
        .forEach(
            (path, fieldNames) ->
                buildInnerHit(fieldNames, findNestedQueryWithSamePath(nestedQueries, path)));
  }

  /**
   * InnerHit must be added to the NestedQueryBuilder. We need to extract the nested queries
   * currently in the query if there is already a filter push down with nested query.
   *
   * @param query : current query.
   * @return : grouped nested queries currently in query.
   */
  private List<NestedQueryBuilder> extractNestedQueries(QueryBuilder query) {
    List<NestedQueryBuilder> result = new ArrayList<>();
    if (query instanceof NestedQueryBuilder) {
      result.add((NestedQueryBuilder) query);
    } else if (query instanceof BoolQueryBuilder) {
      BoolQueryBuilder boolQ = (BoolQueryBuilder) query;
      Stream.of(boolQ.filter(), boolQ.must(), boolQ.should())
          .flatMap(Collection::stream)
          .forEach(q -> result.addAll(extractNestedQueries(q)));
    }
    return result;
  }

  public int getMaxResponseSize() {
    return pageSize == null ? requestedTotalSize : pageSize;
  }

  /** Initialize bool query for push down. */
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
   *
   * @param fields : Fields for nested queries.
   * @return : Map of path and associated field names.
   */
  private Map<String, List<String>> groupFieldNamesByPath(
      List<Map<String, ReferenceExpression>> fields) {
    // TODO filter out reverse nested when supported - .filter(not(isReverseNested()))
    return fields.stream()
        .collect(
            Collectors.groupingBy(
                m -> m.get("path").toString(), mapping(m -> m.get("field").toString(), toList())));
  }

  /**
   * Build inner hits portion to nested query.
   *
   * @param paths : Set of all paths used in nested queries.
   * @param query : Current pushDown query.
   */
  private void buildInnerHit(List<String> paths, NestedQueryBuilder query) {
    query.innerHit(
        new InnerHitBuilder()
            .setFetchSourceContext(
                new FetchSourceContext(true, paths.toArray(new String[0]), null)));
  }

  /**
   * We need to group nested queries with same path for adding new fields with same path of inner
   * hits. If we try to add additional inner hits with same path we get an OS error.
   *
   * @param nestedQueries Current list of nested queries in query.
   * @param path path comparing with current nested queries.
   * @return Query with same path or new empty nested query.
   */
  private NestedQueryBuilder findNestedQueryWithSamePath(
      List<NestedQueryBuilder> nestedQueries, String path) {
    return nestedQueries.stream()
        .filter(query -> isSamePath(path, query))
        .findAny()
        .orElseGet(createEmptyNestedQuery(path));
  }

  /**
   * Check if is nested query is of the same path value.
   *
   * @param path Value of path to compare with nested query.
   * @param query nested query builder to compare with path.
   * @return true if nested query has same path.
   */
  private boolean isSamePath(String path, NestedQueryBuilder query) {
    return nestedQuery(path, query.query(), query.scoreMode()).equals(query);
  }

  /** Create a nested query with match all filter to place inner hits. */
  private Supplier<NestedQueryBuilder> createEmptyNestedQuery(String path) {
    return () -> {
      NestedQueryBuilder nestedQuery = nestedQuery(path, matchAllQuery(), ScoreMode.None);
      ((BoolQueryBuilder) query().filter().get(0)).must(nestedQuery);
      return nestedQuery;
    };
  }

  /**
   * Return current query.
   *
   * @return : Current source builder query.
   */
  private BoolQueryBuilder query() {
    return (BoolQueryBuilder) sourceBuilder.query();
  }
}
