/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class ContinuePageRequestBuilderTest {

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  @Mock
  private Settings settings;

  private final OpenSearchRequest.IndexName indexName = new OpenSearchRequest.IndexName("test");
  private final SerializedPageRequest scrollId = new SerializedPageRequest("scroll", List.of());

  private ContinuePageRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    requestBuilder = new ContinuePageRequestBuilder(
        indexName, scrollId, settings, exprValueFactory);
  }

  @Test
  public void build() {
    assertEquals(
        new ContinuePageRequest(scrollId, TimeValue.timeValueMinutes(1), exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  public void getIndexName() {
    assertEquals(indexName, requestBuilder.getIndexName());
  }

  @Test
  public void pushDown_not_supported() {
    assertAll(
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownFilter(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownAggregation(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownSort(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownLimit(1, 2)),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownHighlight("", Map.of())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownProjects(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushTypeMapping(mock())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownNested(List.of())),
        () -> assertThrows(UnsupportedOperationException.class,
            () -> requestBuilder.pushDownTrackedScore(true))
    );
  }

  private static Stream<Arguments> getScrollsForTest() {
    return Stream.of(
        Arguments.of(new SerializedPageRequest("scroll", List.of("1", "2")),
                    new SerializedPageRequest("new_scroll", List.of("1", "2")),
                    List.of("1", "2"), "with includes"),
        Arguments.of(new SerializedPageRequest("scroll", List.of()),
                    new SerializedPageRequest("new_scroll", List.of()),
                    List.of(), "no includes")
    );
  }

  /** Test different scenarios - with and without `includes`. */
  @ParameterizedTest(name = "{3}")
  @MethodSource("getScrollsForTest")
  @SneakyThrows
  public void parse_and_serialize_includes(SerializedPageRequest serializedPageRequest,
                                           SerializedPageRequest expectedNewScroll,
                                           List<String> includes,
                                           String testName) {
    var request = new ContinuePageRequest(serializedPageRequest,
        TimeValue.timeValueMinutes(1), exprValueFactory);
    SearchResponse searchResponse = mock();
    when(searchResponse.getScrollId()).thenReturn("new_scroll");
    SearchHits hits = mock();
    when(hits.getHits()).thenReturn(new SearchHit[] { new SearchHit(1) });
    when(searchResponse.getHits()).thenReturn(hits);
    var response = request.search((s) -> null, (s) -> searchResponse);
    assertEquals(expectedNewScroll, request.toCursor());
    // extract private field
    var field = response.getClass().getDeclaredField("includes");
    field.setAccessible(true);
    assertEquals(includes, field.get(response));
  }
}
