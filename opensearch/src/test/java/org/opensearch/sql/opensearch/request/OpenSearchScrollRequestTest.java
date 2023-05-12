/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchScrollRequestTest {

  @Mock
  private Function<SearchRequest, SearchResponse> searchAction;

  @Mock
  private Function<SearchScrollRequest, SearchResponse> scrollAction;

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
      new OpenSearchRequest.IndexName("test"), TimeValue.timeValueMinutes(1),
    searchSourceBuilder, factory);

  @Test
  void searchRequest() {
    searchSourceBuilder.query(QueryBuilders.termQuery("name", "John"));
    request.search(searchRequest -> {
      assertEquals(
        new SearchRequest()
          .indices("test")
          .scroll(TimeValue.timeValueMinutes(1))
          .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "John"))),
        searchRequest);
      SearchHits searchHitsMock = when(mock(SearchHits.class).getHits()).thenReturn(new SearchHit[0]).getMock();
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
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse openSearchResponse = request.search(searchRequest -> searchResponse,
        searchScrollRequest -> {throw new AssertionError();});

    assertFalse(openSearchResponse.isEmpty());
  }

  @Test
  void search_withoutContext() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    verify(sourceBuilder, times(1)).fetchSource();
    assertFalse(searchResponse.isEmpty());
  }

  @Test
  void search_withoutIncludes() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        TimeValue.timeValueMinutes(1),
        sourceBuilder,
        factory
    );

    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    assertFalse(searchResponse.isEmpty());
  }

  @Test
  void hasAnotherBatch() {
    request.setScrollId("scroll123");
    assertTrue(request.hasAnotherBatch());

    request.reset();
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

    assertNull(request.getScrollId());
    assertTrue(cleanCalled.get());
  }

  @Test
  void no_clean_on_non_empty_response() {
    SearchResponse searchResponse = mock();
    when(searchResponse.getScrollId()).thenReturn("scroll");
    when(searchResponse.getHits()).thenReturn(
        new SearchHits(new SearchHit[1], new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1F));

    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertEquals("scroll", request.getScrollId());

    request.clean((s) -> fail());
    assertNull(request.getScrollId());
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
    assertNull(request.getScrollId());

    request.clean((s) -> fail());
  }
}
