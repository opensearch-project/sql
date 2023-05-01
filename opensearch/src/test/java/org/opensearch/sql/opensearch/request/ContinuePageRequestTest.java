/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class ContinuePageRequestTest {

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

  private final String scroll = "scroll";
  private final String nextScroll = "nextScroll";

  private final ContinuePageRequest request = new ContinuePageRequest(
      new SerializedPageRequest(scroll, List.of()), TimeValue.timeValueMinutes(1), factory);

  @Test
  public void search_with_non_empty_response() {
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[] {searchHit});
    when(searchResponse.getScrollId()).thenReturn(nextScroll);

    OpenSearchResponse response = request.search((sr) -> fail(), (sr) -> searchResponse);
    assertAll(
        () -> assertFalse(response.isEmpty()),
        () -> assertEquals(new SerializedPageRequest(nextScroll, List.of()), request.toCursor())
    );
  }

  @Test
  // Empty response means scroll search is done and no cursor/scroll should be set
  public void search_with_empty_response() {
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(null);
    lenient().when(searchResponse.getScrollId()).thenReturn(nextScroll);

    OpenSearchResponse response = request.search((sr) -> fail(), (sr) -> searchResponse);
    assertAll(
        () -> assertTrue(response.isEmpty()),
        () -> assertNull(request.toCursor())
    );
  }

  @Test
  @SneakyThrows
  public void clean() {
    request.clean(cleanAction);
    verify(cleanAction, never()).accept(any());
    // Enforce cleaning by setting a private field.
    FieldUtils.writeField(request, "scrollFinished", true, true);
    request.clean(cleanAction);
    verify(cleanAction, times(1)).accept(any());
  }

  @Test
  // Added for coverage only
  public void getters() {
    factory = mock();
    assertAll(
        () -> assertThrows(Throwable.class, request::getSourceBuilder),
        () -> assertSame(factory, new ContinuePageRequest(mock(), null, factory)
            .getExprValueFactory())
    );
  }
}
