/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.DEFAULT_QUERY_TIMEOUT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchPagedIndexScan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class InitialPageRequestBuilderTest {

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  @Mock
  private Settings settings;

  @Mock
  private OpenSearchClient client;

  private static final int pageSize = 42;

  private static final OpenSearchRequest.IndexName indexName =
      new OpenSearchRequest.IndexName("test");

  private InitialPageRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    requestBuilder = new InitialPageRequestBuilder(
        indexName, pageSize, settings, exprValueFactory);
  }

  @Test
  public void build() {
    assertEquals(
        new OpenSearchScrollRequest(indexName, TimeValue.timeValueMinutes(1),
            new SearchSourceBuilder()
                .from(0)
                .size(pageSize)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  public void push_down_not_supported() {
    assertAll(
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownAggregation(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownSort(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownLimit(1, 2)),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownNested(List.of())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownTrackedScore(true))
    );
  }

  @Test
  public void push_down_type_mapping() {
    Map<String, OpenSearchDataType> typeMapping = Map.of("intA", OpenSearchDataType.of(INTEGER));
    requestBuilder.pushTypeMapping(typeMapping);

    verify(exprValueFactory).extendTypeMapping(typeMapping);
  }

  @Test
  public void push_down_project() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    assertEquals(
        new OpenSearchScrollRequest(indexName, TimeValue.timeValueMinutes(1),
            new SearchSourceBuilder()
                .from(0)
                .size(pageSize)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .fetchSource(new String[]{"intA"}, new String[0]),
            exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  void push_down_query() {
    QueryBuilder query = QueryBuilders.termQuery("intA", 1);
    requestBuilder.pushDownFilter(query);

    assertEquals(
        new OpenSearchScrollRequest(indexName, TimeValue.timeValueMinutes(1),
            new SearchSourceBuilder()
                .from(0)
                .size(pageSize)
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .query(query)
                .sort(DOC_FIELD_NAME, ASC),
            exprValueFactory),
        requestBuilder.build()
    );
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
        .shouldQueryHighlight();
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
        .shouldQueryHighlight();
  }

  @Test
  void push_down_highlight_with_repeating_fields() {
    when(client.search(any())).thenReturn(mock());

    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(
        client, new InitialPageRequestBuilder(indexName, pageSize, settings, exprValueFactory))) {
      indexScan.open();

      indexScan.getRequestBuilder().pushDownHighlight("name", Map.of());
      indexScan.getRequestBuilder().pushDownHighlight("title", Map.of());
      indexScan.getRequestBuilder().pushDownHighlight("name", Map.of());
    } catch (SemanticCheckException e) {
      assertEquals(e.getClass(), SemanticCheckException.class);
    }
    verify(client).cleanup(any());
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  public static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchPagedIndexScan indexScan;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    /** Constructor. */
    public PushDownAssertion(OpenSearchClient client,
                             OpenSearchExprValueFactory valueFactory,
                             Settings settings) {
      this.client = client;
      this.indexScan = new OpenSearchPagedIndexScan(client,
          new InitialPageRequestBuilder(indexName, pageSize, settings, valueFactory));
      this.response = mock(OpenSearchResponse.class);
      this.factory = valueFactory;
      when(response.isEmpty()).thenReturn(true);
    }

    public PushDownAssertion pushDown(QueryBuilder query) {
      indexScan.getRequestBuilder().pushDownFilter(query);
      return this;
    }

    public PushDownAssertion pushDownHighlight(String query, Map<String, Literal> arguments) {
      indexScan.getRequestBuilder().pushDownHighlight(query, arguments);
      return this;
    }

    /** Validate highlight. */
    public PushDownAssertion shouldQueryHighlight() {
      when(client.search(any())).thenReturn(response);
      indexScan.open();
      return this;
    }

    /** Query validation. */
    PushDownAssertion shouldQuery(QueryBuilder expected) {
      Settings settings = mock();
      when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
          .thenReturn(TimeValue.timeValueMinutes(1));

      OpenSearchRequest request = new InitialPageRequestBuilder(
          indexName, pageSize, settings, factory).build();
      request.getSourceBuilder()
             .query(expected)
             .sort(DOC_FIELD_NAME, ASC);
      when(client.search(any())).thenReturn(response);
      indexScan.open();
      return this;
    }
  }

  @Test
  public void getIndexName() {
    assertEquals(indexName, requestBuilder.getIndexName());
  }
}
