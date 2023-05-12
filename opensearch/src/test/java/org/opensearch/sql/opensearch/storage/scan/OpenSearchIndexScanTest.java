/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchIndexScanTest {

  public static final int QUERY_SIZE = 200;
  public static final OpenSearchRequest.IndexName INDEX_NAME = new OpenSearchRequest.IndexName("employees");
  public static final int MAX_RESULT_WINDOW = 10000;
  public static final TimeValue CURSOR_KEEP_ALIVE = TimeValue.timeValueMinutes(1);
  @Mock
  private OpenSearchClient client;

  @Mock
  private Settings settings;

  private final OpenSearchExprValueFactory exprValueFactory = new OpenSearchExprValueFactory(
      Map.of("name", OpenSearchDataType.of(STRING),
              "department", OpenSearchDataType.of(STRING)));

  @BeforeEach
  void setup() {
  }

  @Test
  void query_empty_result() {
    mockResponse(client);
    final var name = new OpenSearchRequest.IndexName("test");
    final var requestBuilder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client,
      QUERY_SIZE, requestBuilder.build(name, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE))) {
      indexScan.open();
      assertAll(
          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(0, indexScan.getTotalHits())
      );
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

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
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

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
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

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
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

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
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

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(2, indexScan.getTotalHits())
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
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  private static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchRequestBuilder requestBuilder;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    public PushDownAssertion(OpenSearchClient client,
                             OpenSearchExprValueFactory valueFactory,
                             Settings settings) {
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
      OpenSearchRequest request = new OpenSearchQueryRequest(EMPLOYEES_INDEX, sourceBuilder, factory);

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
      OpenSearchRequest request = new OpenSearchQueryRequest(EMPLOYEES_INDEX, builder, factory);
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
                  lenient().when(response.getTotalHits())
                      .thenReturn((long) searchHitBatches[batchNum].length);
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
