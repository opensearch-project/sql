/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.opendistroforelasticsearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.opensearch.response.OpenSearchResponse;
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
  private OpenSearchExprValueFactory factory;

  private final OpenSearchQueryRequest request =
      new OpenSearchQueryRequest("test", 200, factory);

  @Test
  void search() {
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
