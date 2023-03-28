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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchScrollRequestTest {

  @Mock
  private OpenSearchExprValueFactory factory;

  private final OpenSearchScrollRequest request =
      new OpenSearchScrollRequest("test", factory);

  @Test
  void searchRequest() {
    request.getSourceBuilder().query(QueryBuilders.termQuery("name", "John"));

    assertEquals(
        new SearchRequest()
            .indices("test")
            .scroll(OpenSearchScrollRequest.DEFAULT_SCROLL_TIMEOUT)
            .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "John"))),
        request.searchRequest());
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
            .scroll(OpenSearchScrollRequest.DEFAULT_SCROLL_TIMEOUT)
            .scrollId("scroll123"),
        request.scrollRequest());
  }

  @Test
  void toCursor() {
    request.setScrollId("scroll123");
    assertEquals("scroll123", request.toCursor());

    request.reset();
    assertNull(request.toCursor());
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
  void no_clean_if_no_scroll_in_response() {
    SearchResponse searchResponse = mock();
    when(searchResponse.getHits()).thenReturn(
        new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1F));

    request.search((x) -> searchResponse, (x) -> searchResponse);
    assertNull(request.getScrollId());

    request.clean((s) -> fail());
  }
}
