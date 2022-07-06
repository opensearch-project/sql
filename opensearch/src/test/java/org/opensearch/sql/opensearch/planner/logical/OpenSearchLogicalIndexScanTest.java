/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class OpenSearchLogicalIndexScanTest {

  @Test
  void has_projects() {
    assertFalse(OpenSearchLogicalIndexScan.builder()
        .projectList(ImmutableSet.of()).build()
        .hasProjects());

    assertFalse(OpenSearchLogicalIndexScan.builder().build().hasProjects());
  }

  @Test
  void has_highlight() {
    assertTrue(
        OpenSearchLogicalIndexScan.builder().highlightField("fieldA").build().hasHighlight());
  }

  @Test
  void no_highlight_by_default() {
    assertFalse(OpenSearchLogicalIndexScan.builder().build().hasHighlight());
  }
}
