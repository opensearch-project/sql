/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.planner.logical.LogicalNested;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchRequestBuilderTest {

  private static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);
  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_LIMIT = 200;
  private static final Integer MAX_RESULT_WINDOW = 500;

  private static final OpenSearchRequest.IndexName indexName =
      new OpenSearchRequest.IndexName("test");

  @Mock private OpenSearchExprValueFactory exprValueFactory;

  private OpenSearchRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    requestBuilder = new OpenSearchRequestBuilder(DEFAULT_LIMIT, exprValueFactory);
  }

  @Test
  void build_query_request() {
    Integer limit = 200;
    Integer offset = 0;
    requestBuilder.pushDownLimit(limit, offset);
    requestBuilder.pushDownTrackedScore(true);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(limit)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .trackScores(true),
            exprValueFactory,
            List.of()),
        requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void build_scroll_request_with_correct_size() {
    Integer limit = 800;
    Integer offset = 10;
    requestBuilder.pushDownLimit(limit, offset);

    assertEquals(
        new OpenSearchScrollRequest(
            new OpenSearchRequest.IndexName("test"),
            TimeValue.timeValueMinutes(1),
            new SearchSourceBuilder()
                .from(offset)
                .size(MAX_RESULT_WINDOW - offset)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            exprValueFactory,
            List.of()),
        requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void test_push_down_query() {
    QueryBuilder query = QueryBuilders.termQuery("intA", 1);
    requestBuilder.pushDownFilter(query);

    var r = requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT);
    Function<SearchRequest, SearchResponse> querySearch =
        searchRequest -> {
          assertEquals(
              new SearchSourceBuilder()
                  .from(DEFAULT_OFFSET)
                  .size(DEFAULT_LIMIT)
                  .timeout(DEFAULT_QUERY_TIMEOUT)
                  .query(query)
                  .sort(DOC_FIELD_NAME, ASC),
              searchRequest.source());
          return mock();
        };
    Function<SearchScrollRequest, SearchResponse> scrollSearch =
        searchScrollRequest -> {
          throw new UnsupportedOperationException();
        };
    r.search(querySearch, scrollSearch);
  }

  @Test
  void test_push_down_aggregation() {
    AggregationBuilder aggBuilder =
        AggregationBuilders.composite(
            "composite_buckets", Collections.singletonList(new TermsValuesSourceBuilder("longA")));
    OpenSearchAggregationResponseParser responseParser =
        new CompositeAggregationParser(new SingleValueParser("AVG(intA)"));
    requestBuilder.pushDownAggregation(Pair.of(List.of(aggBuilder), responseParser));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(0)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .aggregation(aggBuilder),
        requestBuilder.getSourceBuilder());
    verify(exprValueFactory).setParser(responseParser);
  }

  @Test
  void test_push_down_query_and_sort() {
    QueryBuilder query = QueryBuilders.termQuery("intA", 1);
    requestBuilder.pushDownFilter(query);

    FieldSortBuilder sortBuilder = SortBuilders.fieldSort("intA");
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .query(query)
            .sort(sortBuilder),
        requestBuilder);
  }

  void assertSearchSourceBuilder(
      SearchSourceBuilder expected, OpenSearchRequestBuilder requestBuilder)
      throws UnsupportedOperationException {
    Function<SearchRequest, SearchResponse> querySearch =
        searchRequest -> {
          assertEquals(expected, searchRequest.source());
          return when(mock(SearchResponse.class).getHits())
              .thenReturn(
                  new SearchHits(
                      new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f))
              .getMock();
        };
    Function<SearchScrollRequest, SearchResponse> scrollSearch =
        searchScrollRequest -> {
          throw new UnsupportedOperationException();
        };
    requestBuilder
        .build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT)
        .search(querySearch, scrollSearch);
  }

  @Test
  void test_push_down_sort() {
    FieldSortBuilder sortBuilder = SortBuilders.fieldSort("intA");
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(sortBuilder),
        requestBuilder);
  }

  @Test
  void test_push_down_non_field_sort() {
    ScoreSortBuilder sortBuilder = SortBuilders.scoreSort();
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(sortBuilder),
        requestBuilder);
  }

  @Test
  void test_push_down_multiple_sort() {
    requestBuilder.pushDownSort(
        List.of(SortBuilders.fieldSort("intA"), SortBuilders.fieldSort("intB")));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(SortBuilders.fieldSort("intA"))
            .sort(SortBuilders.fieldSort("intB")),
        requestBuilder);
  }

  @Test
  void test_push_down_project() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .fetchSource(new String[] {"intA"}, new String[0]),
        requestBuilder);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(DEFAULT_OFFSET)
                .size(DEFAULT_LIMIT)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .fetchSource("intA", null),
            exprValueFactory,
            List.of("intA")),
        requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void test_push_down_project_limit() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    Integer limit = 200;
    Integer offset = 0;
    requestBuilder.pushDownLimit(limit, offset);

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(offset)
            .size(limit)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .fetchSource(new String[] {"intA"}, new String[0]),
        requestBuilder);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(limit)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .fetchSource("intA", null),
            exprValueFactory,
            List.of("intA")),
        requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void test_push_down_project_limit_and_offset() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    Integer limit = 200;
    Integer offset = 10;
    requestBuilder.pushDownLimit(limit, offset);

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .from(offset)
            .size(limit)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .fetchSource(new String[] {"intA"}, new String[0]),
        requestBuilder);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(limit)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .fetchSource("intA", null),
            exprValueFactory,
            List.of("intA")),
        requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void test_push_down_nested() {
    List<Map<String, ReferenceExpression>> args =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("message.info", DSL.nested(DSL.ref("message.info", STRING)), null));

    LogicalNested nested = new LogicalNested(null, args, projectList);
    requestBuilder.pushDownNested(nested.getFields());

    NestedQueryBuilder nestedQuery =
        nestedQuery("message", matchAllQuery(), ScoreMode.None)
            .innerHit(
                new InnerHitBuilder()
                    .setFetchSourceContext(
                        new FetchSourceContext(true, new String[] {"message.info"}, null)));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .query(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(nestedQuery)))
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT),
        requestBuilder);
  }

  @Test
  void test_push_down_multiple_nested_with_same_path() {
    List<Map<String, ReferenceExpression>> args =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("message.from", STRING),
                "path", new ReferenceExpression("message", STRING)));
    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("message.info", DSL.nested(DSL.ref("message.info", STRING)), null),
            new NamedExpression("message.from", DSL.nested(DSL.ref("message.from", STRING)), null));

    LogicalNested nested = new LogicalNested(null, args, projectList);
    requestBuilder.pushDownNested(nested.getFields());

    NestedQueryBuilder nestedQuery =
        nestedQuery("message", matchAllQuery(), ScoreMode.None)
            .innerHit(
                new InnerHitBuilder()
                    .setFetchSourceContext(
                        new FetchSourceContext(
                            true, new String[] {"message.info", "message.from"}, null)));
    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .query(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(nestedQuery)))
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT),
        requestBuilder);
  }

  @Test
  void test_push_down_nested_with_filter() {
    List<Map<String, ReferenceExpression>> args =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("message.info", DSL.nested(DSL.ref("message.info", STRING)), null));

    LogicalNested nested = new LogicalNested(null, args, projectList);
    requestBuilder.getSourceBuilder().query(QueryBuilders.rangeQuery("myNum").gt(3));
    requestBuilder.pushDownNested(nested.getFields());

    NestedQueryBuilder nestedQuery =
        nestedQuery("message", matchAllQuery(), ScoreMode.None)
            .innerHit(
                new InnerHitBuilder()
                    .setFetchSourceContext(
                        new FetchSourceContext(true, new String[] {"message.info"}, null)));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .query(
                QueryBuilders.boolQuery()
                    .filter(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.rangeQuery("myNum").gt(3))
                            .must(nestedQuery)))
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT),
        requestBuilder);
  }

  @Test
  void testPushDownNestedWithNestedFilter() {
    List<Map<String, ReferenceExpression>> args =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("message.info", DSL.nested(DSL.ref("message.info", STRING)), null));

    QueryBuilder innerFilterQuery = QueryBuilders.rangeQuery("myNum").gt(3);
    QueryBuilder filterQuery =
        QueryBuilders.nestedQuery("message", innerFilterQuery, ScoreMode.None);
    LogicalNested nested = new LogicalNested(null, args, projectList);
    requestBuilder.getSourceBuilder().query(filterQuery);
    requestBuilder.pushDownNested(nested.getFields());

    NestedQueryBuilder nestedQuery =
        nestedQuery("message", matchAllQuery(), ScoreMode.None)
            .innerHit(
                new InnerHitBuilder()
                    .setFetchSourceContext(
                        new FetchSourceContext(true, new String[] {"message.info"}, null)));

    assertSearchSourceBuilder(
        new SearchSourceBuilder()
            .query(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(filterQuery)))
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT),
        requestBuilder);
  }

  @Test
  void test_push_type_mapping() {
    Map<String, OpenSearchDataType> typeMapping = Map.of("intA", OpenSearchDataType.of(INTEGER));
    requestBuilder.pushTypeMapping(typeMapping);

    verify(exprValueFactory).extendTypeMapping(typeMapping);
  }

  @Test
  void push_down_highlight_with_repeating_fields() {
    requestBuilder.pushDownHighlight("name", Map.of());
    var exception =
        assertThrows(
            SemanticCheckException.class, () -> requestBuilder.pushDownHighlight("name", Map.of()));
    assertEquals("Duplicate field name in highlight", exception.getMessage());
  }

  @Test
  void push_down_page_size() {
    requestBuilder.pushDownPageSize(3);
    assertSearchSourceBuilder(
        new SearchSourceBuilder().from(DEFAULT_OFFSET).size(3).timeout(DEFAULT_QUERY_TIMEOUT),
        requestBuilder);
  }

  @Test
  void exception_when_non_zero_offset_and_page_size() {
    requestBuilder.pushDownPageSize(3);
    requestBuilder.pushDownLimit(300, 2);
    assertThrows(
        UnsupportedOperationException.class,
        () -> requestBuilder.build(indexName, MAX_RESULT_WINDOW, DEFAULT_QUERY_TIMEOUT));
  }

  @Test
  void maxResponseSize_is_page_size() {
    requestBuilder.pushDownPageSize(4);
    assertEquals(4, requestBuilder.getMaxResponseSize());
  }

  @Test
  void maxResponseSize_is_limit() {
    requestBuilder.pushDownLimit(100, 0);
    assertEquals(100, requestBuilder.getMaxResponseSize());
  }
}
