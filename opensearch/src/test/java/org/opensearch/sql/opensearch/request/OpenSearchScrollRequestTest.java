/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.request.OpenSearchScrollRequest.NO_SCROLL_ID;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchScrollRequestTest {

  public static final OpenSearchRequest.IndexName INDEX_NAME
      = new OpenSearchRequest.IndexName("test");

  public static final OpenSearchRequest.IndexName ROUTING_ID
      = new OpenSearchRequest.IndexName("shard");
  public static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1);
  @Mock
  private SearchResponse searchResponse;

  @Mock
  private SearchHits searchHits;

  @Mock
  private SearchHit searchHit;

  @Mock
  private SearchSourceBuilder sourceBuilder;

  @Mock
  private OpenSearchExprValueFactory factory;

  private final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
  private final OpenSearchScrollRequest request = new OpenSearchScrollRequest(
      INDEX_NAME, ROUTING_ID, SCROLL_TIMEOUT,
      searchSourceBuilder, factory);

  @Test
  void constructor() {
    searchSourceBuilder.fetchSource(new String[] {"test"}, null);
    var request = new OpenSearchScrollRequest(INDEX_NAME, ROUTING_ID, SCROLL_TIMEOUT,
        searchSourceBuilder, factory);
    assertNotEquals(List.of(), request.getIncludes());
  }

  @Test
  void constructor2() {
    searchSourceBuilder.fetchSource(new String[]{"test"}, null);
    var request = new OpenSearchScrollRequest(INDEX_NAME, ROUTING_ID, SCROLL_TIMEOUT, searchSourceBuilder,
        factory);
    assertNotEquals(List.of(), request.getIncludes());
  }

  @Test
  void searchRequest() {
    searchSourceBuilder.query(QueryBuilders.termQuery("name", "John"));
    request.search(searchRequest -> {
      assertEquals(
        new SearchRequest()
          .indices("test")
          .routing("shard")
          .scroll(TimeValue.timeValueMinutes(1))
          .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "John"))),
          searchRequest);
      SearchHits searchHitsMock = when(mock(SearchHits.class).getHits())
          .thenReturn(new SearchHit[0]).getMock();
      return when(mock(SearchResponse.class).getHits()).thenReturn(searchHitsMock).getMock();
    }, searchScrollRequest -> null);
  }

  @Test
  void isScrollStarted() {
    assertFalse(request.isScroll());

    request.setScrollId("scroll123");
    assertTrue(request.isScroll());

    request.reset();
    assertFalse(request.isScroll());
  }

  @Test
  void scrollRequest() {
    request.setScrollId("scroll123");
    assertEquals(
        new SearchScrollRequest()
            .scroll(TimeValue.timeValueMinutes(1))
            .scrollId("scroll123"),
        request.scrollRequest());
  }

  @Test
  void search() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        new OpenSearchRequest.IndexName("shard"),
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse response = request.search((sr) -> searchResponse, (sr) -> fail());
    assertFalse(response.isEmpty());
  }

  @Test
  void search_without_context() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        new OpenSearchRequest.IndexName("shard"),
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse response = request.search((sr) -> searchResponse, (sr) -> fail());
    verify(sourceBuilder, times(1)).fetchSource();
    assertFalse(response.isEmpty());
  }

  @Test
  @SneakyThrows
  void search_without_scroll_and_initial_request_should_throw() {
    // Steps: serialize a not used request, deserialize it, then use
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        ROUTING_ID,
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );
    var outStream = new BytesStreamOutput();
    request.writeTo(outStream);
    outStream.flush();
    var inStream = new BytesStreamInput(outStream.bytes().toBytesRef().bytes);
    var indexMock = mock(OpenSearchIndex.class);
    var engine = mock(OpenSearchStorageEngine.class);
    when(engine.getTable(any(), any(), any())).thenReturn(indexMock);
    var request2 = new OpenSearchScrollRequest(inStream, engine);
    assertAll(
        () -> assertFalse(request2.isScroll()),
        () -> assertNull(request2.getInitialSearchRequest()),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> request2.search(sr -> fail("search"), sr -> fail("scroll")))
    );
  }

  @Test
  void search_withoutIncludes() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        ROUTING_ID,
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse response = request.search((sr) -> searchResponse, (sr) -> fail());
    assertFalse(response.isEmpty());
  }

  @Test
  @SneakyThrows
  void hasAnotherBatch() {
    FieldUtils.writeField(request, "needClean", false, true);
    request.setScrollId("scroll123");
    assertTrue(request.hasAnotherBatch());

    request.reset();
    assertFalse(request.hasAnotherBatch());

    request.setScrollId("");
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void clean_on_empty_response() {
    // This could happen on sequential search calls
    SearchResponse searchResponse = mock();
    when(searchResponse.getScrollId()).thenReturn("scroll1", "scroll2");
    when(searchResponse.getHits()).thenReturn(
        new SearchHits(new SearchHit[1], new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1F),
        new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1F));

    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertEquals("scroll1", request.getScrollId());
    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertEquals("scroll1", request.getScrollId());

    AtomicBoolean cleanCalled = new AtomicBoolean(false);
    request.clean((s) -> cleanCalled.set(true));

    assertEquals(NO_SCROLL_ID, request.getScrollId());
    assertTrue(cleanCalled.get());
  }

  @Test
  void no_clean_on_non_empty_response() {
    SearchResponse searchResponse = mock();
    when(searchResponse.getScrollId()).thenReturn("scroll");
    when(searchResponse.getHits()).thenReturn(
        new SearchHits(new SearchHit[1], new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1F));

    request.search((sr) -> searchResponse, (sr) -> searchResponse);
    assertEquals("scroll", request.getScrollId());

    request.clean((s) -> fail());
    assertEquals(NO_SCROLL_ID, request.getScrollId());
  }

  @Test
  void no_cursor_on_empty_response() {
    SearchResponse searchResponse = mock();
    when(searchResponse.getHits()).thenReturn(
      new SearchHits(new SearchHit[0], null, 1f));

    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void no_clean_if_no_scroll_in_response() {
    SearchResponse searchResponse = mock();
    when(searchResponse.getHits()).thenReturn(
        new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1F));

    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertEquals(NO_SCROLL_ID, request.getScrollId());

    request.clean((s) -> fail());
  }

  @Test
  @SneakyThrows
  void serialize_deserialize_no_needClean() {
    var stream = new BytesStreamOutput();
    request.writeTo(stream);
    stream.flush();
    assertTrue(stream.size() > 0);

    // deserialize
    var inStream = new BytesStreamInput(stream.bytes().toBytesRef().bytes);
    var indexMock = mock(OpenSearchIndex.class);
    var engine = mock(OpenSearchStorageEngine.class);
    when(engine.getTable(any(), any(), any())).thenReturn(indexMock);
    var newRequest = new OpenSearchScrollRequest(inStream, engine);
    assertEquals(request, newRequest);
    assertEquals("", newRequest.getScrollId());
  }

  @Test
  @SneakyThrows
  void serialize_deserialize_needClean() {
    lenient().when(searchResponse.getHits()).thenReturn(
      new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1F));
    lenient().when(searchResponse.getScrollId()).thenReturn("");

    var stream = new BytesStreamOutput();
    request.search(searchRequest -> searchResponse, null);
    request.writeTo(stream);
    stream.flush();
    assertTrue(stream.size() > 0);

    // deserialize
    var inStream = new BytesStreamInput(stream.bytes().toBytesRef().bytes);
    var indexMock = mock(OpenSearchIndex.class);
    var engine = mock(OpenSearchStorageEngine.class);
    when(engine.getTable(any(), any(), any())).thenReturn(indexMock);
    var newRequest = new OpenSearchScrollRequest(inStream, engine);
    assertEquals(request, newRequest);
    assertEquals("", newRequest.getScrollId());
  }

  @Test
  void setScrollId() {
    request.setScrollId("test");
    assertEquals("test", request.getScrollId());
  }

  @Test
  void includes() {

    assertIncludes(List.of(), searchSourceBuilder);

    searchSourceBuilder.fetchSource((String[])null, (String[])null);
    assertIncludes(List.of(), searchSourceBuilder);

    searchSourceBuilder.fetchSource(new String[] {"test"}, null);
    assertIncludes(List.of("test"), searchSourceBuilder);

  }

  void assertIncludes(List<String> expected, SearchSourceBuilder sourceBuilder) {
    assertEquals(expected, new OpenSearchScrollRequest(
        INDEX_NAME, ROUTING_ID, SCROLL_TIMEOUT, sourceBuilder, factory).getIncludes());
  }
}
