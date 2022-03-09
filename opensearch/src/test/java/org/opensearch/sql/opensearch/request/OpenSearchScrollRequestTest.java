/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@ExtendWith(MockitoExtension.class)
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
}
