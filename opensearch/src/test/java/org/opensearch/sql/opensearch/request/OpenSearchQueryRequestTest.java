/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.request.OpenSearchRequest.DEFAULT_QUERY_TIMEOUT;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
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
      new OpenSearchQueryRequest("test", "key", 200, factory);

  private final OpenSearchQueryRequest remoteRequest =
      new OpenSearchQueryRequest("ccs:test", "ccs:key", 200, factory);

  @Test
  void search() {
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(
        new OpenSearchRequest.IndexName("test"),
        new OpenSearchRequest.IndexName("key"),
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
        new OpenSearchRequest.IndexName("key"),
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
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void search_withIncludes() {
    OpenSearchQueryRequest request = new OpenSearchQueryRequest(
        new OpenSearchRequest.IndexName("test"),
        new OpenSearchRequest.IndexName("key"),
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

    assertSearchRequest(new SearchRequest()
        .indices("test")
        .routing("key")
        .source(new SearchSourceBuilder()
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
            .routing("key")
            .source(new SearchSourceBuilder()
                .timeout(DEFAULT_QUERY_TIMEOUT)
                .from(0)
                .size(200)
                .query(QueryBuilders.termQuery("name", "John"))),
        remoteRequest);
  }

  @Test
  void writeTo_unsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> request.writeTo(mock(StreamOutput.class)));
  }

  private void assertSearchRequest(SearchRequest expected, OpenSearchQueryRequest request) {
    Function<SearchRequest, SearchResponse> querySearch = searchRequest -> {
      assertEquals(expected, searchRequest);
      return when(mock(SearchResponse.class).getHits())
        .thenReturn(new SearchHits(new SearchHit[0],
            new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f))
        .getMock();
    };
    request.search(querySearch, searchScrollRequest -> null);
  }
}
