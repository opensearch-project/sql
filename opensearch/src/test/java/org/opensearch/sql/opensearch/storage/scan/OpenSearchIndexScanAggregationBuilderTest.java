/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexScanAggregationBuilderTest {
  @Mock
  OpenSearchRequestBuilder requestBuilder;
  @Mock
  LogicalAggregation logicalAggregation;
  OpenSearchIndexScanAggregationBuilder builder;

  @BeforeEach
  void setup() {
    builder = new OpenSearchIndexScanAggregationBuilder(requestBuilder, logicalAggregation);
  }

  @Test
  void pushDownFilter() {
    assertFalse(builder.pushDownFilter(mock(LogicalFilter.class)));
  }

  @Test
  void pushDownSort() {
    assertTrue(builder.pushDownSort(mock(LogicalSort.class)));
  }

  @Test
  void pushDownLimit() {
    assertFalse(builder.pushDownLimit(mock(LogicalLimit.class)));
  }

  @Test
  void pushDownProject() {
    assertFalse(builder.pushDownProject(mock(LogicalProject.class)));
  }

  @Test
  void pushDownHighlight() {
    assertFalse(builder.pushDownHighlight(mock(LogicalHighlight.class)));
  }

  @Test
  void pushDownPageSize() {
    assertFalse(builder.pushDownPageSize(mock(LogicalPaginate.class)));
  }

  @Test
  void pushDownNested() {
    assertFalse(builder.pushDownNested(mock(LogicalNested.class)));
  }

}
