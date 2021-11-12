/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import static org.junit.jupiter.api.Assertions.assertFalse;

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
}
