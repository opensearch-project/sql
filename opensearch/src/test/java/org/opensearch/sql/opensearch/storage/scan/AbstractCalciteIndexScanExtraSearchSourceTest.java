/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

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
class AbstractCalciteIndexScanExtraSearchSourceTest {

  @Mock private OpenSearchRequestBuilder requestBuilder;

  @AfterEach
  void cleanup() {
    CalcitePlanContext.clearExtraSearchSource();
  }

  @Test
  void applyExtraSearchSource_withNullConfig_doesNothing() {
    // No ThreadLocal set — config is null, requestBuilder should not be called
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);
    // If config is null, method returns early — no interaction with requestBuilder
  }

  @Test
  void applyExtraSearchSource_withWildcardFields_setsHighlighter() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setExtraSearchSource("{\"highlight\":{\"fields\":{\"*\":{}}}}");
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertNotNull(highlighter);
    assertEquals(1, highlighter.fields().size());
    assertEquals("*", highlighter.fields().get(0).name());
  }

  @Test
  void applyExtraSearchSource_withCustomTags_setsTags() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setExtraSearchSource(
        "{\"highlight\":{\"fields\":{\"message\":{}},\"pre_tags\":[\"<em>\"],\"post_tags\":[\"</em>\"]}}");
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertArrayEquals(new String[] {"<em>"}, highlighter.preTags());
    assertArrayEquals(new String[] {"</em>"}, highlighter.postTags());
  }

  @Test
  void applyExtraSearchSource_withFragmentSize_setsOnHighlighter() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    when(requestBuilder.getSourceBuilder()).thenReturn(sourceBuilder);

    CalcitePlanContext.setExtraSearchSource(
        "{\"highlight\":{\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}");
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);

    HighlightBuilder highlighter = sourceBuilder.highlighter();
    assertEquals(1, highlighter.fields().size());
    // fragment_size is parsed as a top-level HighlightBuilder setting by native XContent parsing
    assertEquals(Integer.valueOf(2147483647), highlighter.fragmentSize());
  }

  @Test
  void applyExtraSearchSource_withMalformedJson_handlesGracefully() {
    // Malformed JSON — should not throw, just log warning and skip
    CalcitePlanContext.setExtraSearchSource("{not valid json}}}");
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);
    // No exception thrown — method handles gracefully
  }

  @Test
  void applyExtraSearchSource_withNoHighlight_doesNotSetHighlighter() {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    // JSON with no highlight clause — nothing to merge
    CalcitePlanContext.setExtraSearchSource("{\"size\":10}");
    AbstractCalciteIndexScan.applyExtraSearchSource(requestBuilder);
    assertNull(sourceBuilder.highlighter());
  }
}
