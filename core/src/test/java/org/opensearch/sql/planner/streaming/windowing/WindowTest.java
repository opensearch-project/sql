/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.dateValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.window;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.WINDOW;
import static org.opensearch.sql.planner.streaming.windowing.Window.END_NAME;
import static org.opensearch.sql.planner.streaming.windowing.Window.START_NAME;
import static org.opensearch.sql.planner.streaming.windowing.Window.UNBOUND;

import java.util.Map;
import org.junit.jupiter.api.Test;

class WindowTest {

  @Test
  void testNumericWindow() {
    Window window = window(1, 10);
    assertEquals(integerValue(1), window.getLowerBound());
    assertEquals(integerValue(10), window.getUpperBound());
  }

  @Test
  void testDateTimeWindow() {
    Window window = window("2022-11-01", "2022-11-05", DATE);
    assertEquals(dateValue("2022-11-01"), window.getLowerBound());
    assertEquals(dateValue("2022-11-05"), window.getUpperBound());
  }

  @Test
  void testValue() {
    Window window = window(1, 10);
    assertEquals(
        tupleValue(Map.of(
            START_NAME, integerValue(1),
            END_NAME, integerValue(10))),
        window.value());
  }

  @Test
  void testIllegalWindow() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new Window(integerValue(1), booleanValue(true)));
    assertEquals(
        "Lower bound [INTEGER] and upper bound [BOOLEAN] must be of the same type",
        error.getMessage());
  }

  @Test
  void testWindowType() {
    assertEquals(WINDOW, window(1, 2).type());
  }

  @Test
  void testWindowComparison() {
    assertTrue(window(1, 10).compareTo(window(1, 9)) > 0);
    assertEquals(window(1, 10), window(1, 10));
  }

  @Test
  void testWindowComparisonWithUnbound() {
    Window unboundWindow = new Window(integerValue(1), UNBOUND);
    assertTrue(unboundWindow.compareTo(window(1, 10)) > 0);
    assertTrue(window(1, 10).compareTo(unboundWindow) < 0);
    assertEquals(0, unboundWindow.compareTo(new Window(integerValue(1), UNBOUND)));
  }
}