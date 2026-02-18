/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;

@ExtendWith(MockitoExtension.class)
class AbstractCalciteIndexScanHighlightTest {

  @Mock private OpenSearchRequestBuilder requestBuilder;

  @AfterEach
  void cleanup() {
    CalcitePlanContext.clearHighlightConfig();
  }

  @Test
  void applyHighlightConfig_withNullConfig_doesNothing() {
    // No ThreadLocal set — config is null, requestBuilder should not be called
    AbstractCalciteIndexScan.applyHighlightConfig(requestBuilder);
    // If config is null, method returns early — no interaction with requestBuilder
  }

  @Test
  void applyHighlightConfig_withWildcardFields_setsHighlighter() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setHighlightConfig(Map.of("fields", Map.of("*", Map.of())));
    AbstractCalciteIndexScan.applyHighlightConfig(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertNotNull(highlighter);
    assertEquals(1, highlighter.fields().size());
    assertEquals("*", highlighter.fields().get(0).name());
  }

  @Test
  void applyHighlightConfig_withCustomTags_setsTags() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setHighlightConfig(
        Map.of(
            "fields", Map.of("message", Map.of()),
            "pre_tags", List.of("<em>"),
            "post_tags", List.of("</em>")));
    AbstractCalciteIndexScan.applyHighlightConfig(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertArrayEquals(new String[] {"<em>"}, highlighter.preTags());
    assertArrayEquals(new String[] {"</em>"}, highlighter.postTags());
  }

  @Test
  void applyHighlightConfig_withFragmentSize_setsPerField() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setHighlightConfig(
        Map.of("fields", Map.of("*", Map.of()), "fragment_size", 2147483647));
    AbstractCalciteIndexScan.applyHighlightConfig(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertEquals(1, highlighter.fields().size());
    assertEquals(Integer.valueOf(2147483647), highlighter.fields().get(0).fragmentSize());
  }

  @Test
  void applyHighlightConfig_withMalformedConfig_handlesGracefully() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    // "fields" is a String instead of Map — should not throw NPE
    CalcitePlanContext.setHighlightConfig(Map.of("fields", "not_a_map"));
    AbstractCalciteIndexScan.applyHighlightConfig(requestBuilder);

    // Should still create a highlighter (just with no fields)
    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertNotNull(highlighter);
    assertEquals(0, highlighter.fields().size());
  }
}
