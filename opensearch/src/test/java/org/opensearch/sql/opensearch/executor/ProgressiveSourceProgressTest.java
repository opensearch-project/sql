/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;

class ProgressiveSourceProgressTest {

  @Test
  void source_progress_is_monotonic_and_reserves_twenty_percent() {
    ProgressiveSourceProgress tracker = ProgressiveSourceProgress.forTesting(Map.of(1L, 100L));

    QueryProgress half = tracker.updateRows(1L, 50L, 100L, true, false);
    QueryProgress revised = tracker.updateRows(1L, 60L, 200L, true, false);
    QueryProgress complete = tracker.updateRows(1L, 200L, 200L, true, true);

    assertEquals(0.4D, half.fractionDone(), 0D);
    assertEquals(half.fractionDone(), revised.fractionDone(), 0D);
    assertEquals(0.8D, complete.fractionDone(), 0D);
  }

  @Test
  void multiple_known_sources_are_weighted_by_estimated_documents() {
    Map<Long, Long> estimates = new LinkedHashMap<>();
    estimates.put(1L, 9_000_000L);
    estimates.put(2L, 1_000_000L);
    ProgressiveSourceProgress tracker = ProgressiveSourceProgress.forTesting(estimates);

    tracker.updateRows(1L, 4_500_000L, 9_000_000L, true, false);
    QueryProgress progress = tracker.updateRows(2L, 1_000_000L, 1_000_000L, true, true);

    assertEquals(0.44D, progress.fractionDone(), 1e-12);
  }

  @Test
  void unknown_total_uses_adaptive_denominator_and_stays_bounded() {
    ProgressiveSourceProgress tracker = ProgressiveSourceProgress.forTesting(Map.of(1L, -1L));

    double first = tracker.updateRows(1L, 100L, -1L, false, false).fractionDone();
    double second = tracker.updateRows(1L, 200L, -1L, false, false).fractionDone();

    assertTrue(first > 0D && first < 0.8D);
    assertTrue(second >= first && second < 0.8D);
  }

  @Test
  void pit_in_flight_page_progress_contributes_before_the_page_response() {
    ProgressiveSourceProgress tracker =
        ProgressiveSourceProgress.forTesting(Map.of(1L, 1_000_000L));

    QueryProgress inFlight = tracker.updatePage(1L, new QueryProgress(0.5D), 10_000L);
    QueryProgress completedPage = tracker.updateRows(1L, 10_000L, -1L, false, false);

    assertEquals(0.004D, inFlight.fractionDone(), 1e-12);
    assertEquals(0.008D, completedPage.fractionDone(), 1e-12);
  }
}
