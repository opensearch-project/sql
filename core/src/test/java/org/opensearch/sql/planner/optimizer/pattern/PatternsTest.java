/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer.pattern;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PatternsTest {

  @Test
  void source_is_empty() {
    var plan = mock(LogicalPlan.class);
    when(plan.getChild()).thenReturn(Collections.emptyList());
    assertAll(
        () -> assertFalse(Patterns.source().getFunction().apply(plan).isPresent()),
        () -> assertFalse(Patterns.source(null).getProperty().getFunction().apply(plan).isPresent())
    );
  }

  @Test
  void table_is_empty() {
    var plan = mock(LogicalFilter.class);
    assertAll(
        () -> assertFalse(Patterns.table().getFunction().apply(plan).isPresent()),
        () -> assertFalse(Patterns.writeTable().getFunction().apply(plan).isPresent())
    );
  }
}
