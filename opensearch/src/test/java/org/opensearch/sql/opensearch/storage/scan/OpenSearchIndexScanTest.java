/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchIndexScanTest {

  public static final int QUERY_SIZE = 200;
  public static final OpenSearchRequest.IndexName INDEX_NAME
      = new OpenSearchRequest.IndexName("employees");
  public static final int MAX_RESULT_WINDOW = 10000;
  public static final TimeValue CURSOR_KEEP_ALIVE = TimeValue.timeValueMinutes(1);
  @Mock
  private OpenSearchClient client;

  private final OpenSearchExprValueFactory exprValueFactory = new OpenSearchExprValueFactory(
      Map.of("name", OpenSearchDataType.of(STRING),
              "department", OpenSearchDataType.of(STRING)));

  @BeforeEach
  void setup() {
  }

  @Test
  void explain() {
    var request = mock(OpenSearchRequest.class);
    when(request.toString()).thenReturn("explain works!");
    try (var indexScan = new OpenSearchIndexScan(client, QUERY_SIZE, request)) {
      assertEquals("explain works!", indexScan.explain());
    }
  }

  @Test
  @SneakyThrows
  void throws_no_cursor_exception() {
    var request = mock(OpenSearchRequest.class);
    when(request.hasAnotherBatch()).thenReturn(false);
    try (var indexScan = new OpenSearchIndexScan(client, QUERY_SIZE, request);
         var byteStream = new ByteArrayOutputStream();
         var objectStream = new ObjectOutputStream(byteStream)) {
      assertThrows(NoCursorException.class, () -> objectStream.writeObject(indexScan));
    }
  }

  @Test
  @SneakyThrows
  void serialize() {
    var searchSourceBuilder = new SearchSourceBuilder().size(4);

    var factory = mock(OpenSearchExprValueFactory.class);
    var engine = mock(OpenSearchStorageEngine.class);
    var index = mock(OpenSearchIndex.class);
    when(engine.getClient()).thenReturn(client);
    when(engine.getTable(any(), any())).thenReturn(index);
    var request = new OpenSearchScrollRequest(
        INDEX_NAME, CURSOR_KEEP_ALIVE, searchSourceBuilder, factory, List.of());
    request.setScrollId("valid-id");
    // make a response, so OpenSearchResponse::isEmpty would return true and unset needClean
    var response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(mock());
    var hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    when(response.getScrollId()).thenReturn("valid-id");
    when(hits.getHits()).thenReturn(new SearchHit[]{ mock() });
    request.search(null, (req) -> response);

    try (var indexScan = new OpenSearchIndexScan(client, QUERY_SIZE, request)) {
      var planSerializer = new PlanSerializer(engine);
      var cursor = planSerializer.convertToCursor(indexScan);
      var newPlan = planSerializer.convertToPlan(cursor.toString());
      assertEquals(indexScan, newPlan);
    }
  }

  @Test
  void plan_for_serialization() {
    var request = mock(OpenSearchRequest.class);
    try (var indexScan = new OpenSearchIndexScan(client, QUERY_SIZE, request)) {
      assertEquals(indexScan, indexScan.getPlanForSerialization());
    }
  }

  @Test
  void query_empty_result() {
    mockResponse(client);
    final var name = new OpenSearchRequest.IndexName("test");
    final var requestBuilder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        QUERY_SIZE, requestBuilder.build(name, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE))) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_all_results_with_query() {
    mockResponse(client, new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT")});

    final var requestBuilder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        10, requestBuilder.build(INDEX_NAME, 10000, CURSOR_KEEP_ALIVE))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext())
      );
    }
    verify(client).cleanup(any());
  }

  static final OpenSearchRequest.IndexName EMPLOYEES_INDEX
      = new OpenSearchRequest.IndexName("employees");

  @Test
  void query_all_results_with_scroll() {
    mockResponse(client,
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT")});

    final var requestBuilder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        10, requestBuilder.build(INDEX_NAME, 10000, CURSOR_KEEP_ALIVE))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext())
      );
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_some_results_with_query() {
    mockResponse(client, new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT"),
        employee(4, "Bob", "HR")});

    final int limit = 3;
    OpenSearchRequestBuilder builder = new OpenSearchRequestBuilder(0, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        limit, builder.build(INDEX_NAME, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext())
      );
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_some_results_with_scroll() {
    mockTwoPageResponse(client);
    final var requestuilder = new OpenSearchRequestBuilder(10, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        3, requestuilder.build(INDEX_NAME, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext())
      );
    }
    verify(client).cleanup(any());
  }

  static void mockTwoPageResponse(OpenSearchClient client) {
    mockResponse(client,
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT"), employee(4, "Bob", "HR")});
  }

  @Test
  void query_results_limited_by_query_size() {
    mockResponse(client, new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT"),
        employee(4, "Bob", "HR")});

    final int defaultQuerySize = 2;
    final var requestBuilder = new OpenSearchRequestBuilder(defaultQuerySize, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
        defaultQuerySize, requestBuilder.build(INDEX_NAME, QUERY_SIZE, CURSOR_KEEP_ALIVE))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext())
      );
    }
    verify(client).cleanup(any());
  }

  @Test
  void push_down_filters() {
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .shouldQuery(QueryBuilders.termQuery("name", "John"))
        .pushDown(QueryBuilders.termQuery("age", 30))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30)))
        .pushDown(QueryBuilders.rangeQuery("balance").gte(10000))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30))
                .filter(QueryBuilders.rangeQuery("balance").gte(10000)));
  }

  @Test
  void push_down_highlight() {
    Map<String, Literal> args = new HashMap<>();
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .pushDownHighlight("Title", args)
        .pushDownHighlight("Body", args)
        .shouldQueryHighlight(QueryBuilders.termQuery("name", "John"),
            new HighlightBuilder().field("Title").field("Body"));
  }

  @Test
  void push_down_highlight_with_arguments() {
    Map<String, Literal> args = new HashMap<>();
    args.put("pre_tags", new Literal("<mark>", DataType.STRING));
    args.put("post_tags", new Literal("</mark>", DataType.STRING));
    HighlightBuilder highlightBuilder = new HighlightBuilder()
        .field("Title");
    highlightBuilder.fields().get(0).preTags("<mark>").postTags("</mark>");
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .pushDownHighlight("Title", args)
        .shouldQueryHighlight(QueryBuilders.termQuery("name", "John"),
            highlightBuilder);
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory);
  }

  private static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchRequestBuilder requestBuilder;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    public PushDownAssertion(OpenSearchClient client,
                             OpenSearchExprValueFactory valueFactory) {
      this.client = client;
      this.requestBuilder = new OpenSearchRequestBuilder(QUERY_SIZE, valueFactory);

      this.response = mock(OpenSearchResponse.class);
      this.factory = valueFactory;
      when(response.isEmpty()).thenReturn(true);
    }

    PushDownAssertion pushDown(QueryBuilder query) {
      requestBuilder.pushDownFilter(query);
      return this;
    }

    PushDownAssertion pushDownHighlight(String query, Map<String, Literal> arguments) {
      requestBuilder.pushDownHighlight(query, arguments);
      return this;
    }

    PushDownAssertion shouldQueryHighlight(QueryBuilder query, HighlightBuilder highlight) {
      var sourceBuilder = new SearchSourceBuilder()
          .from(0)
          .timeout(CURSOR_KEEP_ALIVE)
          .query(query)
          .size(QUERY_SIZE)
          .highlighter(highlight)
          .sort(DOC_FIELD_NAME, ASC);
      OpenSearchRequest request =
          new OpenSearchQueryRequest(EMPLOYEES_INDEX, sourceBuilder, factory, List.of());

      when(client.search(request)).thenReturn(response);
      var indexScan = new OpenSearchIndexScan(client,
          QUERY_SIZE, requestBuilder.build(EMPLOYEES_INDEX, 10000, CURSOR_KEEP_ALIVE));
      indexScan.open();
      return this;
    }

    PushDownAssertion shouldQuery(QueryBuilder expected) {
      var builder = new SearchSourceBuilder()
          .from(0)
          .query(expected)
          .size(QUERY_SIZE)
          .timeout(CURSOR_KEEP_ALIVE)
          .sort(DOC_FIELD_NAME, ASC);
      OpenSearchRequest request =
          new OpenSearchQueryRequest(EMPLOYEES_INDEX, builder, factory, List.of());
      when(client.search(request)).thenReturn(response);
      var indexScan = new OpenSearchIndexScan(client,
          10000, requestBuilder.build(EMPLOYEES_INDEX, 10000, CURSOR_KEEP_ALIVE));
      indexScan.open();
      return this;
    }
  }

  public static void mockResponse(OpenSearchClient client, ExprValue[]... searchHitBatches) {
    when(client.search(any()))
        .thenAnswer(
            new Answer<OpenSearchResponse>() {
              private int batchNum;

              @Override
              public OpenSearchResponse answer(InvocationOnMock invocation) {
                OpenSearchResponse response = mock(OpenSearchResponse.class);
                int totalBatch = searchHitBatches.length;
                if (batchNum < totalBatch) {
                  when(response.isEmpty()).thenReturn(false);
                  ExprValue[] searchHit = searchHitBatches[batchNum];
                  when(response.iterator()).thenReturn(Arrays.asList(searchHit).iterator());
                } else {
                  when(response.isEmpty()).thenReturn(true);
                }

                batchNum++;
                return response;
              }
            });
  }

  public static ExprValue employee(int docId, String name, String department) {
    SearchHit hit = new SearchHit(docId);
    hit.sourceRef(
        new BytesArray("{\"name\":\"" + name + "\",\"department\":\"" + department + "\"}"));
    return tupleValue(hit);
  }

  private static ExprValue tupleValue(SearchHit hit) {
    return ExprValueUtils.tupleValue(hit.getSourceAsMap());
  }
}
