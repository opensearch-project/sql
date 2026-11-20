/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.Warning;

class CalcitePlanContextWarningsTest {

  private static final Warning WARNING = new Warning(Warning.TYPE_PARTIAL_RESULT, "shard", null);

  @AfterEach
  void clear() {
    CalcitePlanContext.drainWarnings();
  }

  @Test
  void peekWarnings_isEmptyWithoutWarnings() {
    assertEquals(List.of(), CalcitePlanContext.peekWarnings());
  }

  @Test
  void peekWarnings_deduplicatesWithoutClearing() {
    CalcitePlanContext.addWarning(WARNING);
    CalcitePlanContext.addWarning(WARNING);
    assertEquals(List.of(WARNING), CalcitePlanContext.peekWarnings());
    assertEquals(2, CalcitePlanContext.warningCount());
    assertEquals(List.of(WARNING), CalcitePlanContext.drainWarnings());
    assertEquals(0, CalcitePlanContext.warningCount());
  }
}
