/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.function.Consumer;
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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
public class OpenSearchQueryRequestTest {

  @Mock
  private Function<SearchRequest, SearchResponse> searchAction;

  @Mock
  private Function<SearchScrollRequest, SearchResponse> scrollAction;

  @Mock
  private Consumer<String> cleanAction;

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

  private final OpenSearchQueryRequest request =
      new OpenSearchQueryRequest("test", 200, factory);

  @Test
  void search() {
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(
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
    searchResponse = request.search(searchAction, scrollAction);
    assertTrue(searchResponse.isEmpty());
    verify(searchAction, times(1)).apply(any());
  }

  @Test
  void search_withoutContext() {
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(
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
  void search_withIncludes() {
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(
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
  void searchRequest() {
    request.getSourceBuilder().query(QueryBuilders.termQuery("name", "John"));

    assertEquals(
        new SearchRequest()
            .indices("test")
            .source(new SearchSourceBuilder()
                .timeout(OpenSearchQueryRequest.DEFAULT_QUERY_TIMEOUT)
                .from(0)
                .size(200)
                .query(QueryBuilders.termQuery("name", "John"))),
        request.searchRequest());
  }
}
