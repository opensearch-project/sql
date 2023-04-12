/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;
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
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
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
  private FetchSourceContext fetchSourceContext;
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
    assertFalse(request.isScrollStarted());

    request.setScrollId("scroll123");
    assertTrue(request.isScrollStarted());
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
  void search() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        sourceBuilder,
        factory
    );

    String[] includes = {"_id", "_index"};
    when(sourceBuilder.fetchSource()).thenReturn(fetchSourceContext);
    when(fetchSourceContext.includes()).thenReturn(includes);
    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    verify(fetchSourceContext, times(2)).includes();
    assertFalse(searchResponse.isEmpty());
  }

  @Test
  void search_withoutContext() {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest(
        new OpenSearchRequest.IndexName("test"),
        sourceBuilder,
        factory
    );

    when(sourceBuilder.fetchSource()).thenReturn(null);
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
        sourceBuilder,
        factory
    );

    when(sourceBuilder.fetchSource()).thenReturn(fetchSourceContext);
    when(fetchSourceContext.includes()).thenReturn(null);
    when(searchAction.apply(any())).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});

    OpenSearchResponse searchResponse = request.search(searchAction, scrollAction);
    verify(fetchSourceContext, times(1)).includes();
    assertFalse(searchResponse.isEmpty());
  }
}
