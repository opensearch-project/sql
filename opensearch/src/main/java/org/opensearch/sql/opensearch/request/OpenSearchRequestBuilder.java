/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.join.ScoreMode;
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
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/**
 * OpenSearch search request builder.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchRequestBuilder {

  /**
   * Default query timeout in minutes.
   */
  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  /**
   * Index max result window.
   */
  private final Integer maxResultWindow;

  /**
   * Search request source builder.
   */
  private final SearchSourceBuilder sourceBuilder;

  /**
   * OpenSearchExprValueFactory.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Query size of the request.
   */
  private Integer querySize;

  public OpenSearchRequestBuilder(String indexName,
                                  Integer maxResultWindow,
                                  Settings settings,
                                  OpenSearchExprValueFactory exprValueFactory) {
    this(new OpenSearchRequest.IndexName(indexName), maxResultWindow, settings, exprValueFactory);
  }

  /**
   * Constructor.
   */
  public OpenSearchRequestBuilder(OpenSearchRequest.IndexName indexName,
                                  Integer maxResultWindow,
                                  Settings settings,
                                  OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.maxResultWindow = maxResultWindow;
    this.sourceBuilder = new SearchSourceBuilder();
    this.exprValueFactory = exprValueFactory;
    this.querySize = settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT);
    sourceBuilder.from(0);
    sourceBuilder.size(querySize);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
  }

  /**
   * Build DSL request.
   *
   * @return query request or scroll request
   */
  public OpenSearchRequest build() {
    Integer from = sourceBuilder.from();
    Integer size = sourceBuilder.size();

    if (from + size <= maxResultWindow) {
      return new OpenSearchQueryRequest(indexName, sourceBuilder, exprValueFactory);
    } else {
      sourceBuilder.size(maxResultWindow - from);
      return new OpenSearchScrollRequest(indexName, sourceBuilder, exprValueFactory);
    }
  }

  /**
   * Push down query to DSL request.
   *
   * @param query  query request
   */
  public void pushDown(QueryBuilder query) {
    QueryBuilder current = sourceBuilder.query();

    if (current == null) {
      sourceBuilder.query(query);
    } else {
      if (isBoolFilterQuery(current)) {
        ((BoolQueryBuilder) current).filter(query);
      } else {
        sourceBuilder.query(boolQuery()
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
  public void pushDownAggregation(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder) {
    aggregationBuilder.getLeft().forEach(builder -> sourceBuilder.aggregation(builder));
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

  /**
   * Push down size (limit) and from (offset) to DSL request.
   */
  public void pushDownLimit(Integer limit, Integer offset) {
    querySize = limit;
    sourceBuilder.from(offset).size(limit);
  }

  /**
   * Add highlight to DSL requests.
   * @param field name of the field to highlight
   */
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
   * Push down project list to DSL requets.
   */
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    final Set<String> projectsSet =
        projects.stream().map(ReferenceExpression::getAttr).collect(Collectors.toSet());
    sourceBuilder.fetchSource(projectsSet.toArray(new String[0]), new String[0]);
  }

  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    exprValueFactory.setTypeMapping(typeMapping);
  }

  private boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  private boolean isSortByDocOnly() {
    List<SortBuilder<?>> sorts = sourceBuilder.sorts();
    if (sorts != null) {
      return sorts.equals(Arrays.asList(SortBuilders.fieldSort(DOC_FIELD_NAME)));
    }
    return false;
  }

  public void pushDownNested(String field) {// if(is Nested) create field with nested boolean member variable
    project(List.of(new Field(new QualifiedName(field))));
  }

  public void project(List<Field> fields) {
    if (isAnyNestedField(fields)) {
      initBoolQueryFilterIfNull();
      List<NestedQueryBuilder> nestedQueries = extractNestedQueries(query());

      groupFieldNamesByPath(fields).forEach(
          (path, fieldNames) -> buildInnerHit(fieldNames, findNestedQueryWithSamePath(nestedQueries, path))
      );
    }
  }

  /**
   * Check via traditional for loop first to avoid lambda performance impact on all queries
   * even though those without nested field
   */
  private boolean isAnyNestedField(List<Field> fields) {
    for (Field field : fields) {
      if (field.toString().contains(".")) {
        return true;
      }
    }
    return false;
  }

  private void initBoolQueryFilterIfNull() {
    if (sourceBuilder == null || query() == null) {
      sourceBuilder.query(QueryBuilders.boolQuery());
    }
    if (query().filter().isEmpty()) {
      query().filter(boolQuery());
    }
  }

  private Map<String, List<String>> groupFieldNamesByPath(List<Field> fields) {
    return fields.stream().
        filter(Field::isNested).
        filter(not(Field::isReverseNested)).
        collect(groupingBy(Field::getNestedPath, mapping(Field::getName, toList())));
  }

  /**
   * Why search for NestedQueryBuilder recursively?
   * Because 1) it was added and wrapped by BoolQuery when WHERE explained (far from here)
   * 2) InnerHit must be added to the NestedQueryBuilder related
   * <p>
   * Either we store it to global data structure (which requires to be thread-safe or ThreadLocal)
   * or we peel off BoolQuery to find it (the way we followed here because recursion tree should be very thin).
   */
  private List<NestedQueryBuilder> extractNestedQueries(QueryBuilder query) {
    List<NestedQueryBuilder> result = new ArrayList<>();
    if (query instanceof NestedQueryBuilder) {
      result.add((NestedQueryBuilder) query);
    } else if (query instanceof BoolQueryBuilder) {
      BoolQueryBuilder boolQ = (BoolQueryBuilder) query;
      Stream.of(boolQ.filter(), boolQ.must(), boolQ.should()).
          flatMap(Collection::stream).
          forEach(q -> result.addAll(extractNestedQueries(q)));
    }
    return result;
  }

  private void buildInnerHit(List<String> fieldNames, NestedQueryBuilder query) {
    query.innerHit(new InnerHitBuilder().setFetchSourceContext(
        new FetchSourceContext(true, fieldNames.toArray(new String[0]), null)
    ));
  }

  /**
   * Why linear search? Because NestedQueryBuilder hides "path" field from any access.
   * Assumption: collected NestedQueryBuilder list should be very small or mostly only one.
   */
  private NestedQueryBuilder findNestedQueryWithSamePath(List<NestedQueryBuilder> nestedQueries, String path) {
    return nestedQueries.stream().
        filter(query -> isSamePath(path, query)).
        findAny().
        orElseGet(createEmptyNestedQuery(path));
  }

  private boolean isSamePath(String path, NestedQueryBuilder query) {
    return nestedQuery(path, query.query(), query.scoreMode()).equals(query);
  }

  /**
   * Create a nested query with match all filter to place inner hits
   */
  private Supplier<NestedQueryBuilder> createEmptyNestedQuery(String path) {
    return () -> {
      NestedQueryBuilder nestedQuery = nestedQuery(path, matchAllQuery(), ScoreMode.None);
      ((BoolQueryBuilder) query().filter().get(0)).must(nestedQuery);
      return nestedQuery;
    };
  }

  private BoolQueryBuilder query() {
    return (BoolQueryBuilder) sourceBuilder.query();
  }

  private <T> Predicate<T> not(Predicate<T> predicate) {
    return predicate.negate();
  }
}
