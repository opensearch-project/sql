/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link QueryPhaseTracker}. */
public class QueryPhaseTrackerTest {

  @AfterEach
  void cleanup() {
    QueryPhaseTracker.clear();
  }

  @Test
  public void testStartCreatesTrackerAndSetsCurrent() {
    assertNull(QueryPhaseTracker.current());
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    assertNotNull(tracker);
    assertSame(tracker, QueryPhaseTracker.current());
  }

  @Test
  public void testBeginPhaseAndEndCurrentPhase() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.beginPhase("parse");
    // Simulate some work
    consumeTime();
    tracker.endCurrentPhase();

    String serialized = tracker.serialize();
    assertTrue(
        serialized.startsWith("parse:"),
        "Expected serialized to start with 'parse:': " + serialized);
    // The nanos value should be > 0
    String wallNanos = serialized.split("\\|")[0].split(":")[1];
    assertTrue(Long.parseLong(wallNanos) > 0, "Wall-clock nanos should be positive");
  }

  @Test
  public void testMultiplePhasesInSequence() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();

    tracker.beginPhase("parse");
    consumeTime();
    tracker.endCurrentPhase();

    tracker.beginPhase("analyze");
    consumeTime();
    tracker.endCurrentPhase();

    tracker.beginPhase("plan");
    consumeTime();
    tracker.endCurrentPhase();

    String serialized = tracker.serialize();
    // All three phases should be present in order
    assertTrue(serialized.contains("parse:"), "Missing parse phase: " + serialized);
    assertTrue(serialized.contains("analyze:"), "Missing analyze phase: " + serialized);
    assertTrue(serialized.contains("plan:"), "Missing plan phase: " + serialized);

    // Verify ordering (LinkedHashMap preserves insertion order)
    int parseIdx = serialized.indexOf("parse:");
    int analyzeIdx = serialized.indexOf("analyze:");
    int planIdx = serialized.indexOf("plan:");
    assertTrue(parseIdx < analyzeIdx, "parse should come before analyze");
    assertTrue(analyzeIdx < planIdx, "analyze should come before plan");
  }

  @Test
  public void testEndAllAddsTotalPhase() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();

    tracker.beginPhase("parse");
    consumeTime();
    tracker.endCurrentPhase();

    tracker.endAll();

    String serialized = tracker.serialize();
    assertTrue(serialized.contains("total:"), "Missing total phase: " + serialized);

    // total should be the last entry
    String[] parts = serialized.split(",");
    assertTrue(parts[parts.length - 1].startsWith("total:"), "total should be last: " + serialized);
  }

  @Test
  public void testEndAllFinishesActivePhase() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.beginPhase("execute");
    consumeTime();
    // Don't call endCurrentPhase — endAll should do it
    tracker.endAll();

    String serialized = tracker.serialize();
    assertTrue(serialized.contains("execute:"), "Missing execute phase: " + serialized);
    assertTrue(serialized.contains("total:"), "Missing total phase: " + serialized);
  }

  @Test
  public void testSerializeFormat() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    // Use addCompletedPhase to have deterministic values
    tracker.addCompletedPhase("parse", 1000L, 800L, 5000L);
    tracker.addCompletedPhase("analyze", 2000L, 1500L, 10000L);

    String serialized = tracker.serialize();
    // Expected: "parse:1000|cpu:800|mem:5000,analyze:2000|cpu:1500|mem:10000"
    assertEquals("parse:1000|cpu:800|mem:5000,analyze:2000|cpu:1500|mem:10000", serialized);
  }

  @Test
  public void testSerializeFormatWithoutCpuAndMem() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    // Overload with only nanos
    tracker.addCompletedPhase("parse", 1000L);
    tracker.addCompletedPhase("analyze", 2000L);

    String serialized = tracker.serialize();
    // No cpu/mem segments
    assertEquals("parse:1000,analyze:2000", serialized);
  }

  @Test
  public void testSerializeFormatWithZeroCpuAndMem() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    // cpuNanos=0 and memBytes=0 should be omitted
    tracker.addCompletedPhase("parse", 1000L, 0L, 0L);

    String serialized = tracker.serialize();
    assertEquals("parse:1000", serialized);
  }

  @Test
  public void testPersistStoresToThreadContext() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.addCompletedPhase("parse", 1234L, 1000L, 5000L);

    tracker.persist();

    String stored = ThreadContext.get("_sql_phase_tracker");
    assertNotNull(stored);
    assertEquals("parse:1234|cpu:1000|mem:5000", stored);
  }

  @Test
  public void testStartOrRestoreRestoresPhasesFromThreadContext() {
    // Simulate a prior thread persisting data
    ThreadContext.put(
        "_sql_phase_tracker", "parse:1234|cpu:1000|mem:5000,analyze:5678|cpu:4000|mem:20000");

    QueryPhaseTracker tracker = QueryPhaseTracker.startOrRestore();
    assertNotNull(tracker);
    assertSame(tracker, QueryPhaseTracker.current());

    String serialized = tracker.serialize();
    assertTrue(serialized.contains("parse:1234"), "parse phase not restored: " + serialized);
    assertTrue(serialized.contains("cpu:1000"), "parse cpu not restored: " + serialized);
    assertTrue(serialized.contains("mem:5000"), "parse mem not restored: " + serialized);
    assertTrue(serialized.contains("analyze:5678"), "analyze phase not restored: " + serialized);
    assertTrue(serialized.contains("cpu:4000"), "analyze cpu not restored: " + serialized);
    assertTrue(serialized.contains("mem:20000"), "analyze mem not restored: " + serialized);
  }

  @Test
  public void testStartOrRestoreWithEmptyThreadContext() {
    // No prior data
    ThreadContext.remove("_sql_phase_tracker");

    QueryPhaseTracker tracker = QueryPhaseTracker.startOrRestore();
    assertNotNull(tracker);
    // Should produce an empty serialization (no phases yet)
    assertEquals("", tracker.serialize());
  }

  @Test
  public void testStartOrRestoreCanContinueWithNewPhases() {
    ThreadContext.put("_sql_phase_tracker", "parse:1000");

    QueryPhaseTracker tracker = QueryPhaseTracker.startOrRestore();
    tracker.beginPhase("analyze");
    consumeTime();
    tracker.endCurrentPhase();

    String serialized = tracker.serialize();
    assertTrue(serialized.contains("parse:1000"), "Restored parse missing: " + serialized);
    assertTrue(serialized.contains("analyze:"), "New analyze phase missing: " + serialized);
  }

  @Test
  public void testClearRemovesFromThreadLocalAndThreadContext() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.addCompletedPhase("parse", 1000L);
    tracker.persist();

    // Verify presence before clear
    assertNotNull(QueryPhaseTracker.current());
    assertNotNull(ThreadContext.get("_sql_phase_tracker"));

    QueryPhaseTracker.clear();

    assertNull(QueryPhaseTracker.current());
    assertNull(ThreadContext.get("_sql_phase_tracker"));
  }

  @Test
  public void testAddCompletedPhaseWallOnly() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.addCompletedPhase("custom", 9999L);

    String serialized = tracker.serialize();
    assertEquals("custom:9999", serialized);
  }

  @Test
  public void testAddCompletedPhaseWithAllMetrics() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.addCompletedPhase("execute", 50000L, 30000L, 100000L);

    String serialized = tracker.serialize();
    assertEquals("execute:50000|cpu:30000|mem:100000", serialized);
  }

  @Test
  public void testBeginPhaseImplicitlyEndsCurrentPhase() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    tracker.beginPhase("parse");
    consumeTime();
    // Calling beginPhase again should end "parse" first
    tracker.beginPhase("analyze");
    consumeTime();
    tracker.endCurrentPhase();

    String serialized = tracker.serialize();
    assertTrue(serialized.contains("parse:"), "parse should have been ended: " + serialized);
    assertTrue(serialized.contains("analyze:"), "analyze missing: " + serialized);
  }

  @Test
  public void testEndCurrentPhaseWithNoActivePhaseIsNoop() {
    QueryPhaseTracker tracker = QueryPhaseTracker.start();
    // Should not throw
    tracker.endCurrentPhase();
    assertEquals("", tracker.serialize());
  }

  @Test
  public void testQuerySourceHeadersConstants() {
    assertEquals("x-query-source", QuerySourceHeaders.QUERY_SOURCE_HEADER);
    assertEquals("x-original-query", QuerySourceHeaders.ORIGINAL_QUERY_HEADER);
    assertEquals("x-query-execution-id", QuerySourceHeaders.QUERY_EXECUTION_ID_HEADER);
    assertEquals("x-query-phases", QuerySourceHeaders.QUERY_PHASES_HEADER);
    assertEquals(4096, QuerySourceHeaders.MAX_ORIGINAL_QUERY_LENGTH);
  }

  /** Burn a small amount of wall-clock time to ensure non-zero nanos. */
  private void consumeTime() {
    long start = System.nanoTime();
    while (System.nanoTime() - start < 1_000_000) {
      // spin for ~1ms
    }
  }
}
