/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link TransportPPLQueryAction#extractPplIndices(String)} — the heuristic that
 * pulls index names out of a PPL query string for Query Insights reporting.
 */
public class ExtractPplIndicesTest {

  @Test
  public void singleIndex() {
    assertEquals(
        List.of("employees"), TransportPPLQueryAction.extractPplIndices("source=employees"));
  }

  @Test
  public void indexKeywordAlias() {
    assertEquals(List.of("logs"), TransportPPLQueryAction.extractPplIndices("index=logs"));
  }

  @Test
  public void caseInsensitiveKeyword() {
    assertEquals(List.of("t"), TransportPPLQueryAction.extractPplIndices("SOURCE=t"));
  }

  @Test
  public void spacesAroundEquals() {
    assertEquals(
        List.of("employees"), TransportPPLQueryAction.extractPplIndices("source = employees"));
  }

  @Test
  public void stopsAtPipe() {
    // The value must not swallow the trailing "| where ..." clause.
    assertEquals(
        List.of("employees"),
        TransportPPLQueryAction.extractPplIndices(
            "source=employees | where dept=\"eng\" | fields name"));
  }

  @Test
  public void stopsAtWhitespace() {
    // A space-separated trailing term must not become part of the index name.
    assertEquals(
        List.of("employees"), TransportPPLQueryAction.extractPplIndices("source=employees a=1"));
  }

  @Test
  public void commaSeparatedList() {
    assertEquals(
        List.of("a", "b", "c"),
        TransportPPLQueryAction.extractPplIndices("source=a,b,c | stats count()"));
  }

  @Test
  public void quotedIndexIsUnquoted() {
    assertEquals(
        List.of("my index"),
        TransportPPLQueryAction.extractPplIndices("source=\"my index\" | fields x"));
  }

  @Test
  public void deduplicatesInEncounterOrder() {
    assertEquals(
        List.of("a", "b"),
        TransportPPLQueryAction.extractPplIndices("source=a | join source=b | join source=a"));
  }

  @Test
  public void nullAndEmptyYieldEmpty() {
    assertTrue(TransportPPLQueryAction.extractPplIndices(null).isEmpty());
    assertTrue(TransportPPLQueryAction.extractPplIndices("").isEmpty());
  }

  @Test
  public void noSourceClauseYieldsEmpty() {
    assertTrue(TransportPPLQueryAction.extractPplIndices("search x=1 | head 5").isEmpty());
  }

  /** Regression guard: hostile input must match in linear time (no catastrophic backtracking). */
  @Test
  public void hostileInputRunsInLinearTime() {
    StringBuilder sb = new StringBuilder("source=");
    for (int i = 0; i < 50_000; i++) {
      sb.append("a,\"");
    }
    final String hostile = sb.toString();
    long start = System.nanoTime();
    TransportPPLQueryAction.extractPplIndices(hostile);
    Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
    assertTrue(
        "extractPplIndices should complete quickly on hostile input, took "
            + elapsed.toMillis()
            + "ms",
        elapsed.toMillis() < 2_000);
  }
}
