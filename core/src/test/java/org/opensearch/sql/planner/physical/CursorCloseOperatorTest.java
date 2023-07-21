/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CursorCloseOperatorTest {

  @Test
  public void never_hasNext() {
    var plan = new CursorCloseOperator(null);
    assertFalse(plan.hasNext());
    plan.open();
    assertFalse(plan.hasNext());
  }

  @Test
  public void open_is_not_propagated() {
    var child = mock(PhysicalPlan.class);
    var plan = new CursorCloseOperator(child);
    plan.open();
    verify(child, never()).open();
  }

  @Test
  public void close_is_propagated() {
    var child = mock(PhysicalPlan.class);
    var plan = new CursorCloseOperator(child);
    plan.close();
    verify(child).close();
  }

  @Test
  public void next_always_throws() {
    var plan = new CursorCloseOperator(null);
    assertThrows(Throwable.class, plan::next);
    plan.open();
    assertThrows(Throwable.class, plan::next);
  }

  @Test
  public void produces_empty_schema() {
    var child = mock(PhysicalPlan.class);
    var plan = new CursorCloseOperator(child);
    assertEquals(0, plan.schema().getColumns().size());
    verify(child, never()).schema();
  }
}
