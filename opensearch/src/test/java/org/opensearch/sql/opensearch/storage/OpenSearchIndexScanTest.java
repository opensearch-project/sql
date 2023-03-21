/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexScanTest {

  @Mock
  private OpenSearchClient client;

  @Mock
  private Settings settings;

  private OpenSearchExprValueFactory exprValueFactory = new OpenSearchExprValueFactory(
      Map.of("name", OpenSearchDataType.of(STRING),
              "department", OpenSearchDataType.of(STRING)));

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
  }

  @Test
  void queryEmptyResult() {
    mockResponse();
    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "test", 3, exprValueFactory)) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void queryAllResultsWithQuery() {
    mockResponse(new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "employees", 10, exprValueFactory)) {
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void queryAllResultsWithScroll() {
    mockResponse(
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "employees", 2, exprValueFactory)) {
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void querySomeResultsWithQuery() {
    mockResponse(new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT"),
        employee(4, "Bob", "HR")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "employees", 10, exprValueFactory)) {
      indexScan.getRequestBuilder().pushDownLimit(3, 0);
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void querySomeResultsWithScroll() {
    mockResponse(
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT"), employee(4, "Bob", "HR")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "employees", 2, exprValueFactory)) {
      indexScan.getRequestBuilder().pushDownLimit(3, 0);
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void pushDownFilters() {
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
  void pushDownHighlight() {
    Map<String, Literal> args = new HashMap<>();
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .pushDownHighlight("Title", args)
        .pushDownHighlight("Body", args)
        .shouldQueryHighlight(QueryBuilders.termQuery("name", "John"),
            new HighlightBuilder().field("Title").field("Body"));
  }

  @Test
  void pushDownHighlightWithArguments() {
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

  @Test
  void pushDownHighlightWithRepeatingFields() {
    mockResponse(
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT"), employee(4, "Bob", "HR")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "test", 2, exprValueFactory)) {
      indexScan.getRequestBuilder().pushDownLimit(3, 0);
      indexScan.open();
      Map<String, Literal> args = new HashMap<>();
      indexScan.getRequestBuilder().pushDownHighlight("name", args);
      indexScan.getRequestBuilder().pushDownHighlight("name", args);
    } catch (SemanticCheckException e) {
      assertTrue(e.getClass().equals(SemanticCheckException.class));
    }
    verify(client).cleanup(any());
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  private static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchIndexScan indexScan;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    public PushDownAssertion(OpenSearchClient client,
                             OpenSearchExprValueFactory valueFactory,
                             Settings settings) {
      this.client = client;
      this.indexScan = new OpenSearchIndexScan(client, settings, "test", 10000, valueFactory);
      this.response = mock(OpenSearchResponse.class);
      this.factory = valueFactory;
      when(response.isEmpty()).thenReturn(true);
    }

    PushDownAssertion pushDown(QueryBuilder query) {
      indexScan.getRequestBuilder().pushDown(query);
      return this;
    }

    PushDownAssertion pushDownHighlight(String query, Map<String, Literal> arguments) {
      indexScan.getRequestBuilder().pushDownHighlight(query, arguments);
      return this;
    }

    PushDownAssertion shouldQueryHighlight(QueryBuilder query, HighlightBuilder highlight) {
      OpenSearchRequest request = new OpenSearchQueryRequest("test", 200, factory);
      request.getSourceBuilder()
          .query(query)
          .highlighter(highlight)
          .sort(DOC_FIELD_NAME, ASC);
      when(client.search(request)).thenReturn(response);
      indexScan.open();
      return this;
    }

    PushDownAssertion shouldQuery(QueryBuilder expected) {
      OpenSearchRequest request = new OpenSearchQueryRequest("test", 200, factory);
      request.getSourceBuilder()
             .query(expected)
             .sort(DOC_FIELD_NAME, ASC);
      when(client.search(request)).thenReturn(response);
      indexScan.open();
      return this;
    }
  }

  private void mockResponse(ExprValue[]... searchHitBatches) {
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

  protected ExprValue employee(int docId, String name, String department) {
    SearchHit hit = new SearchHit(docId);
    hit.sourceRef(
        new BytesArray("{\"name\":\"" + name + "\",\"department\":\"" + department + "\"}"));
    return tupleValue(hit);
  }

  private ExprValue tupleValue(SearchHit hit) {
    return ExprValueUtils.tupleValue(hit.getSourceAsMap());
  }
}
