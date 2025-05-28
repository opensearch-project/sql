/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.opensearch.sql.common.setting.Settings;
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

  public static final OpenSearchRequest.IndexName INDEX_NAME =
      new OpenSearchRequest.IndexName("employees");
  public static final int MAX_RESULT_WINDOW = 10000;
  public static final TimeValue CURSOR_KEEP_ALIVE = TimeValue.timeValueMinutes(1);
  @Mock private OpenSearchClient client;
  @Mock private Settings settings;

  private final OpenSearchExprValueFactory exprValueFactory =
      new OpenSearchExprValueFactory(
          Map.of(
              "name", OpenSearchDataType.of(STRING), "department", OpenSearchDataType.of(STRING)),
          true);

  @BeforeEach
  void setup() {
    lenient()
        .when(settings.getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER))
        .thenReturn(true);
    lenient().when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE)).thenReturn(true);
  }

  @Test
  void explain() {
    var request = mock(OpenSearchRequest.class);
    when(request.toString()).thenReturn("explain works!");
    try (var indexScan = new OpenSearchIndexScan(client, request)) {
      assertEquals("explain works!", indexScan.explain());
    }
  }

  @Test
  @SneakyThrows
  void throws_no_cursor_exception() {
    var request = mock(OpenSearchRequest.class);
    when(request.hasAnotherBatch()).thenReturn(false);
    try (var indexScan = new OpenSearchIndexScan(client, request);
        var byteStream = new ByteArrayOutputStream();
        var objectStream = new ObjectOutputStream(byteStream)) {
      assertThrows(NoCursorException.class, () -> objectStream.writeObject(indexScan));
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(ints = {0, 150})
  void serialize(Integer numberOfIncludes) {
    var searchSourceBuilder = new SearchSourceBuilder().size(4);

    var factory = mock(OpenSearchExprValueFactory.class);
    var engine = mock(OpenSearchStorageEngine.class);
    var index = mock(OpenSearchIndex.class);
    when(engine.getClient()).thenReturn(client);
    when(engine.getTable(any(), any())).thenReturn(index);
    var includes =
        Stream.iterate(1, i -> i + 1)
            .limit(numberOfIncludes)
            .map(i -> "column" + i)
            .collect(Collectors.toList());
    var request =
        new OpenSearchScrollRequest(
            INDEX_NAME, CURSOR_KEEP_ALIVE, searchSourceBuilder, factory, includes);
    request.setScrollId("valid-id");
    // make a response, so OpenSearchResponse::isEmpty would return true and unset needClean
    var response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(mock());
    var hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    when(response.getScrollId()).thenReturn("valid-id");
    when(hits.getHits()).thenReturn(new SearchHit[] {mock()});
    request.search(null, (req) -> response);

    try (var indexScan = new OpenSearchIndexScan(client, QUERY_SIZE, request)) {
      var planSerializer = new PlanSerializer(engine);
      var cursor = planSerializer.convertToCursor(indexScan);
      var newPlan = planSerializer.convertToPlan(cursor.toString());
      assertEquals(indexScan, newPlan);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(ints = {0, 150})
  void serialize_PIT(Integer numberOfIncludes) {
    var searchSourceBuilder = new SearchSourceBuilder().size(4);

    var factory = mock(OpenSearchExprValueFactory.class);
    var engine = mock(OpenSearchStorageEngine.class);
    var index = mock(OpenSearchIndex.class);
    when(engine.getClient()).thenReturn(client);
    when(engine.getTable(any(), any())).thenReturn(index);
    Map map = mock(Map.class);
    when(map.get(any(String.class))).thenReturn("true");
    when(client.meta()).thenReturn(map);
    var includes =
        Stream.iterate(1, i -> i + 1)
            .limit(numberOfIncludes)
            .map(i -> "column" + i)
            .collect(Collectors.toList());
    var request =
        new OpenSearchQueryRequest(
            INDEX_NAME, searchSourceBuilder, factory, includes, CURSOR_KEEP_ALIVE, "samplePitId");
    // make a response, so OpenSearchResponse::isEmpty would return true and unset needClean
    var response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(mock());
    var hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSortValues()).thenReturn(new String[] {"sample1"});
    when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    request.search((req) -> response, null);

    try (var indexScan = new OpenSearchIndexScan(client, request)) {
      var planSerializer = new PlanSerializer(engine);
      var cursor = planSerializer.convertToCursor(indexScan);
      var newPlan = planSerializer.convertToPlan(cursor.toString());
      assertNotNull(newPlan);

      verify(client).meta();
      verify(map).get(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER.getKeyValue());
    }
  }

  @SneakyThrows
  @Test
  void throws_io_exception_if_too_short() {
    var request = mock(OpenSearchRequest.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeInt(4);
    objectOutput.flush();
    ObjectInputStream objectInput =
        new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()));

    try (var indexScan = new OpenSearchIndexScan(client, request)) {
      assertThrows(IOException.class, () -> indexScan.readExternal(objectInput));
    }
  }

  @Test
  void plan_for_serialization() {
    var request = mock(OpenSearchRequest.class);
    try (var indexScan = new OpenSearchIndexScan(client, request)) {
      assertEquals(indexScan, indexScan.getPlanForSerialization());
    }
  }

  @Test
  void query_empty_result() {
    mockResponse(client);
    final var name = new OpenSearchRequest.IndexName("test");
    final var requestBuilder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client, requestBuilder.build(name, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_all_results_with_query() {
    mockResponse(
        client,
        new ExprValue[] {
          employee(1, "John", "IT"), employee(2, "Smith", "HR"), employee(3, "Allen", "IT")
        });

    final var requestBuilder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client, requestBuilder.build(INDEX_NAME, 10000, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),
          () -> assertFalse(indexScan.hasNext()));
    }
    verify(client).cleanup(any());
  }

  static final OpenSearchRequest.IndexName EMPLOYEES_INDEX =
      new OpenSearchRequest.IndexName("employees");

  @Test
  void query_all_results_with_scroll() {
    mockResponse(
        client,
        new ExprValue[] {employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[] {employee(3, "Allen", "IT")});

    final var requestBuilder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client, requestBuilder.build(INDEX_NAME, 10000, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),
          () -> assertFalse(indexScan.hasNext()));
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_some_results_with_query() {
    mockResponse(
        client,
        new ExprValue[] {
          employee(1, "John", "IT"), employee(2, "Smith", "HR"), employee(3, "Allen", "IT"),
        });

    OpenSearchRequestBuilder builder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    builder.pushDownLimit(3, 0);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client, builder.build(INDEX_NAME, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),
          () -> assertFalse(indexScan.hasNext()));
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_some_results_with_scroll() {
    mockTwoPageResponse(client);
    final var requestuilder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    requestuilder.pushDownLimit(3, 0);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client,
            3,
            requestuilder.build(INDEX_NAME, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),
          () -> assertFalse(indexScan.hasNext()));
    }
    verify(client).cleanup(any());
  }

  static void mockTwoPageResponse(OpenSearchClient client) {
    mockResponse(
        client,
        new ExprValue[] {employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[] {employee(3, "Allen", "IT"), employee(4, "Bob", "HR")});
  }

  @Test
  void query_results_limited_by_query_size() {
    mockResponse(
        client,
        new ExprValue[] {
          employee(1, "John", "IT"), employee(2, "Smith", "HR"),
        });

    final var requestBuilder = new OpenSearchRequestBuilder(exprValueFactory, settings);
    requestBuilder.pushDownLimit(2, 0);
    try (OpenSearchIndexScan indexScan =
        new OpenSearchIndexScan(
            client,
            requestBuilder.build(INDEX_NAME, MAX_RESULT_WINDOW, CURSOR_KEEP_ALIVE, client))) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),
          () -> assertFalse(indexScan.hasNext()));
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
        .shouldQueryHighlight(
            QueryBuilders.termQuery("name", "John"),
            new HighlightBuilder().field("Title").field("Body"));
  }

  @Test
  void push_down_highlight_with_arguments() {
    Map<String, Literal> args = new HashMap<>();
    args.put("pre_tags", new Literal("<mark>", DataType.STRING));
    args.put("post_tags", new Literal("</mark>", DataType.STRING));
    HighlightBuilder highlightBuilder = new HighlightBuilder().field("Title");
    highlightBuilder.fields().get(0).preTags("<mark>").postTags("</mark>");
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .pushDownHighlight("Title", args)
        .shouldQueryHighlight(QueryBuilders.termQuery("name", "John"), highlightBuilder);
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  private static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchRequestBuilder requestBuilder;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    public PushDownAssertion(
        OpenSearchClient client, OpenSearchExprValueFactory valueFactory, Settings settings) {
      this.client = client;
      this.requestBuilder = new OpenSearchRequestBuilder(valueFactory, settings);

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
      var sourceBuilder =
          new SearchSourceBuilder()
              .from(0)
              .timeout(CURSOR_KEEP_ALIVE)
              .query(query)
              .size(MAX_RESULT_WINDOW)
              .highlighter(highlight)
              .sort(DOC_FIELD_NAME, ASC);
      OpenSearchRequest request =
          new OpenSearchQueryRequest(
              EMPLOYEES_INDEX, sourceBuilder, factory, List.of(), CURSOR_KEEP_ALIVE, null);

      when(client.search(request)).thenReturn(response);
      var indexScan =
          new OpenSearchIndexScan(
              client, requestBuilder.build(EMPLOYEES_INDEX, 10000, CURSOR_KEEP_ALIVE, client));
      indexScan.open();
      return this;
    }

    PushDownAssertion shouldQuery(QueryBuilder expected) {
      var builder =
          new SearchSourceBuilder()
              .from(0)
              .query(expected)
              .size(MAX_RESULT_WINDOW)
              .timeout(CURSOR_KEEP_ALIVE)
              .sort(DOC_FIELD_NAME, ASC);
      OpenSearchRequest request =
          new OpenSearchQueryRequest(
              EMPLOYEES_INDEX, builder, factory, List.of(), CURSOR_KEEP_ALIVE, null);
      when(client.search(request)).thenReturn(response);
      var indexScan =
          new OpenSearchIndexScan(
              client, requestBuilder.build(EMPLOYEES_INDEX, 10000, CURSOR_KEEP_ALIVE, client));
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
