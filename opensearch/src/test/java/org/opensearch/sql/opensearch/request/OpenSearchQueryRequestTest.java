/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.sql.opensearch.request.OpenSearchRequest.DEFAULT_QUERY_TIMEOUT;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

@ExtendWith(MockitoExtension.class)
public class OpenSearchQueryRequestTest {

  @Mock private Function<SearchRequest, SearchResponse> searchAction;

  @Mock private Function<SearchScrollRequest, SearchResponse> scrollAction;

  @Mock private Consumer<String> cleanAction;

  @Mock private SearchResponse searchResponse;

  @Mock private SearchHits searchHits;

  @Mock private SearchHit searchHit;

  @Mock private SearchSourceBuilder sourceBuilder;

  @Mock private FetchSourceContext fetchSourceContext;

  @Mock private OpenSearchExprValueFactory factory;

  private final OpenSearchQueryRequest request =
      new OpenSearchQueryRequest("test", 200, factory, List.of());

  private final OpenSearchQueryRequest remoteRequest =
      new OpenSearchQueryRequest("ccs:test", 200, factory, List.of());

  @Mock private StreamOutput streamOutput;
  @Mock private StreamInput streamInput;
  @Mock private OpenSearchStorageEngine engine;
  @Mock private PointInTimeBuilder pointInTimeBuilder;

  private OpenSearchQueryRequest serializationRequest;

  private SearchSourceBuilder sourceBuilderForSerializer;

  @BeforeEach
  void setup() {
    sourceBuilderForSerializer = new SearchSourceBuilder();
    sourceBuilderForSerializer.pointInTimeBuilder(pointInTimeBuilder);
    sourceBuilderForSerializer.timeout(TimeValue.timeValueSeconds(30));
  }

  @SneakyThrows
  @Test
  void testWriteTo() throws IOException {
    when(pointInTimeBuilder.getId()).thenReturn("samplePITId");
    sourceBuilderForSerializer.searchAfter(new Object[] {"value1", 123});
    List<String> includes = List.of("field1", "field2");
    serializationRequest =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilderForSerializer,
            factory,
            includes,
            new TimeValue(1000),
            "samplePITId");

    Field searchAfterField = OpenSearchQueryRequest.class.getDeclaredField("searchAfter");
    searchAfterField.setAccessible(true);
    searchAfterField.set(serializationRequest, new Object[] {"value1", 123});

    serializationRequest.writeTo(streamOutput);

    String expectedJson = "{\"timeout\":\"30s\",\"search_after\":[\"value1\",123]}";
    verify(streamOutput).writeString(expectedJson);
    verify(streamOutput).writeTimeValue(TimeValue.timeValueSeconds(30));
    verify(streamOutput).writeString("samplePITId");
    verify(streamOutput).writeStringCollection(includes);

    verify(streamOutput).writeVInt(2);
    verify(streamOutput).writeGenericValue("value1");
    verify(streamOutput).writeGenericValue(123);
  }

  @Test
  void testWriteToWithoutSearchAfter()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    when(pointInTimeBuilder.getId()).thenReturn("samplePITId");

    List<String> includes = List.of("field1", "field2");
    serializationRequest =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilderForSerializer,
            factory,
            includes,
            new TimeValue(1000),
            "samplePITId");

    serializationRequest.writeTo(streamOutput);
    verify(streamOutput).writeString("{\"timeout\":\"30s\"}");
    verify(streamOutput).writeTimeValue(TimeValue.timeValueSeconds(30));
    verify(streamOutput).writeString("samplePITId");
    verify(streamOutput).writeStringCollection(includes);
    verify(streamOutput, never()).writeVInt(anyInt());
    verify(streamOutput, never()).writeGenericValue(any());
  }

  @Test
  void testWriteToWithoutPIT() {
    serializationRequest = new OpenSearchQueryRequest("test", 200, factory, List.of());

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              request.writeTo(streamOutput);
            });

    assertEquals(
        "OpenSearchQueryRequest serialization is not implemented.", exception.getMessage());
  }

  @Test
  void search() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"), sourceBuilder, factory, List.of());

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    assertFalse(searchResponse.isEmpty());
    searchResponse = request.search(searchAction, scrollAction);
    assertTrue(searchResponse.isEmpty());
    verify(searchAction, times(1)).apply(any());
  }

  @Test
  void search_with_pit() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    when(searchHit.getSortValues()).thenReturn(new String[] {"sortedValue"});
    when(sourceBuilder.sorts()).thenReturn(null);

    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);
    assertFalse(openSearchResponse.isEmpty());
    verify(searchAction, times(1)).apply(any());

    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchResponse.getAggregations()).thenReturn(null);
    when(searchHits.getHits()).thenReturn(null);
    openSearchResponse = request.searchWithPIT(searchAction);
    assertTrue(openSearchResponse.isEmpty());
    verify(searchAction, times(2)).apply(any());

    openSearchResponse = request.searchWithPIT(searchAction);
    assertTrue(openSearchResponse.isEmpty());
  }

  @Test
  void search_with_pit_hits_null() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    when(sourceBuilder.sorts()).thenReturn(null);

    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);
    assertFalse(openSearchResponse.isEmpty());
  }

  @Test
  void search_with_pit_hits_empty() {
    SearchResponse searchResponse = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {});
    when(sourceBuilder.sorts()).thenReturn(null);

    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);
    assertTrue(openSearchResponse.isEmpty());
  }

  @Test
  void search_with_pit_null() {
    SearchResponse searchResponse = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "sample");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse openSearchResponse = request.search(searchAction, scrollAction);
    assertFalse(openSearchResponse.isEmpty());
  }

  @Test
  void has_another_batch() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "sample");
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void has_another_batch_pid_null() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            null);
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void has_another_batch_need_clean() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);
    assertTrue(request.hasAnotherBatch());
  }

  @Test
  void search_withoutContext() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"), sourceBuilder, factory, List.of());

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    assertFalse(searchResponse.isEmpty());
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void search_withIncludes() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"), sourceBuilder, factory, List.of());

    String[] includes = {"_id", "_index"};
    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    assertFalse(searchResponse.isEmpty());

    searchResponse = request.search(searchAction, scrollAction);
    assertTrue(searchResponse.isEmpty());

    verify(searchAction, times(1)).apply(any());
  }

  @Test
  void clean() {
    request.clean(cleanAction);
    verify(cleanAction, never()).accept(any());
  }

  @Test
  void testCleanConditionTrue() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(null);
    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);

    request.clean(cleanAction);

    verify(cleanAction, times(1)).accept("samplePid");
    assertTrue(request.isSearchDone());
    assertNull(request.getPitId());
  }

  @Test
  void testCleanConditionFalse_needCleanFalse() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            "samplePid");

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    OpenSearchResponse openSearchResponse = request.searchWithPIT(searchAction);

    request.clean(cleanAction);
    verify(cleanAction, never()).accept(anyString());
    assertFalse(request.isSearchDone());
    assertNull(request.getPitId());
  }

  @Test
  void testCleanConditionFalse_pidNull() {
    OpenSearchQueryRequest request =
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            sourceBuilder,
            factory,
            List.of(),
            new TimeValue(1000),
            null);

    request.clean(cleanAction);
    verify(cleanAction, never()).accept(anyString());
    assertFalse(request.isSearchDone());
    assertNull(request.getPitId());
  }

  @Test
  void searchRequest() {
    request.getSourceBuilder().query(QueryBuilders.termQuery("name", "John"));

    assertSearchRequest(
        new SearchRequest()
            .indices("test")
            .source(
                new SearchSourceBuilder()
                    .timeout(DEFAULT_QUERY_TIMEOUT)
                    .from(0)
                    .size(200)
                    .query(QueryBuilders.termQuery("name", "John"))),
        request);
  }

  @Test
  void searchCrossClusterRequest() {
    remoteRequest.getSourceBuilder().query(QueryBuilders.termQuery("name", "John"));

    assertSearchRequest(
        new SearchRequest()
            .indices("ccs:test")
            .source(
                new SearchSourceBuilder()
                    .timeout(DEFAULT_QUERY_TIMEOUT)
                    .from(0)
                    .size(200)
                    .query(QueryBuilders.termQuery("name", "John"))),
        remoteRequest);
  }

  @Test
  void writeTo_unsupported() {
    assertThrows(
        UnsupportedOperationException.class, () -> request.writeTo(mock(StreamOutput.class)));
  }

  @Test
  void constructor_serialized() throws IOException {
    StreamInput stream = mock(StreamInput.class);
    OpenSearchStorageEngine engine = mock(OpenSearchStorageEngine.class);
    when(stream.readString()).thenReturn("{}");
    when(stream.readStringArray()).thenReturn(new String[] {"sample"});
    OpenSearchIndex index = mock(OpenSearchIndex.class);
    when(engine.getTable(null, "sample")).thenReturn(index);
    when(stream.readVInt()).thenReturn(2);
    when(stream.readGenericValue()).thenReturn("sampleSearchAfter");
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(stream, engine);
    assertNotNull(request);
  }

  private void assertSearchRequest(SearchRequest expected, OpenSearchQueryRequest request) {
    Function<SearchRequest, SearchResponse> querySearch =
        searchRequest -> {
          assertEquals(expected, searchRequest);
          return when(mock(SearchResponse.class).getHits())
              .thenReturn(
                  new SearchHits(
                      new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f))
              .getMock();
        };
    request.search(querySearch, searchScrollRequest -> null);
  }
}
