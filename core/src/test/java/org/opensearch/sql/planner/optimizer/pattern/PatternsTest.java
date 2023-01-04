/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer.pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
class PatternsTest {

  @Mock
  LogicalPlan plan;

  @Test
  void source_is_empty() {
    when(plan.getChild()).thenReturn(Collections.emptyList());
    assertFalse(Patterns.source().getFunction().apply(plan).isPresent());
    assertFalse(Patterns.source(null).getProperty().getFunction().apply(plan).isPresent());
  }

  @Test
  void table_is_empty() {
    plan = Mockito.mock(LogicalFilter.class);
    assertFalse(Patterns.table().getFunction().apply(plan).isPresent());
    assertFalse(Patterns.writeTable().getFunction().apply(plan).isPresent());
  }
}
