/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.sql.executor.Warning;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ShardStatsTest {

  @Test
  void captures_every_counter_off_the_search_response() {
    ShardStats stats =
        ShardStats.from(
            searchResponse(10, 8, 1, 1, false, failure("logs-2024", 3, "circuit_breaking")));

    assertEquals(10, stats.total());
    assertEquals(8, stats.successful());
    assertEquals(1, stats.skipped());
    assertEquals(1, stats.failed());
    assertFalse(stats.timedOut());
    assertEquals(
        List.of("[logs-2024][3] IllegalStateException: circuit_breaking"), stats.failures());
  }

  @Test
  void a_search_every_shard_answered_is_complete_and_raises_nothing() {
    // A skipped shard is reported in both skipped and successful, so a whole search over 4 shards
    // with one skipped reads successful=4, skipped=1.
    ShardStats stats = ShardStats.from(searchResponse(4, 4, 1, 0, false));

    assertTrue(stats.isComplete());
    assertEquals(0, stats.missing());
    assertEquals(Optional.empty(), stats.toWarning());
  }

  @Test
  void unknown_stats_raise_nothing() {
    assertTrue(ShardStats.UNKNOWN.isComplete());
    assertEquals(Optional.empty(), ShardStats.UNKNOWN.toWarning());
  }

  @Test
  void failed_shards_are_reported_with_counts_and_reasons() {
    ShardStats stats =
        ShardStats.from(
            searchResponse(
                2,
                1,
                0,
                1,
                false,
                failure("osdq-badtype", 0, "'100' is not an IP string literal")));

    Warning warning = stats.toWarning().orElseThrow();
    assertEquals(Warning.TYPE_PARTIAL_RESULT_SHARD_FAILURE, warning.getType());
    assertEquals("Results are partial: 1 of 2 shards failed.", warning.getMessage());
    assertTrue(
        warning
            .getDetail()
            .contains(
                "Shard failures: [[osdq-badtype][0] IllegalStateException: '100' is not an IP"
                    + " string literal]."),
        warning.getDetail());
    assertTrue(warning.getDetail().contains("may be undercounted"), warning.getDetail());
  }

  @Test
  void shards_missing_without_a_failure_are_still_reported() {
    // The node-drop shape: a shard had no available copy, so it is absent from every counter
    // rather than counted as failed. This is the case a failed-only check would miss.
    ShardStats stats = ShardStats.from(searchResponse(4, 3, 0, 0, false));

    assertFalse(stats.isComplete());
    assertEquals(1, stats.missing());
    Warning warning = stats.toWarning().orElseThrow();
    assertEquals("Results are partial: 1 of 4 shards did not return data.", warning.getMessage());
    assertTrue(warning.getDetail().contains("No copy of those shards was available"));
  }

  /**
   * Measured on a live cluster: three shards, one unassigned, one skipped by can-match and one
   * searched, reports {@code total: 3, successful: 2, skipped: 1}. Subtracting skipped a second
   * time scored this as complete and hid the missing shard -- and a skipped shard is the norm for a
   * time-filtered query over many shards, so the gap would hide in exactly the common case.
   */
  @Test
  void a_missing_shard_is_reported_even_when_another_shard_was_skipped() {
    ShardStats stats = ShardStats.from(searchResponse(3, 2, 1, 0, false));

    assertFalse(stats.isComplete());
    assertEquals(1, stats.missing());
    assertEquals(
        "Results are partial: 1 of 3 shards did not return data.",
        stats.toWarning().orElseThrow().getMessage());
  }

  @Test
  void skipped_shards_alone_never_make_a_result_partial() {
    // Every shard answered; three of them were skipped by can-match, which is not a gap.
    ShardStats stats = ShardStats.from(searchResponse(5, 5, 3, 0, false));

    assertTrue(stats.isComplete());
    assertEquals(0, stats.missing());
    assertEquals(Optional.empty(), stats.toWarning());
  }

  @Test
  void a_timeout_alone_makes_the_result_partial() {
    ShardStats stats = ShardStats.from(searchResponse(4, 4, 0, 0, true));

    assertFalse(stats.isComplete());
    Warning warning = stats.toWarning().orElseThrow();
    assertEquals(
        "Results are partial: the search timed out before all shards responded.",
        warning.getMessage());
  }

  @Test
  void one_cause_repeated_across_shards_is_reported_once() {
    ShardStats stats =
        ShardStats.from(
            searchResponse(
                4,
                2,
                0,
                2,
                false,
                failure("logs", 0, "too many buckets"),
                failure("logs", 0, "too many buckets")));

    assertEquals(List.of("[logs][0] IllegalStateException: too many buckets"), stats.failures());
  }

  @Test
  void reasons_beyond_the_cap_are_summarized() {
    ShardStats stats =
        ShardStats.from(
            searchResponse(
                8,
                4,
                0,
                4,
                false,
                failure("logs", 0, "a"),
                failure("logs", 1, "b"),
                failure("logs", 2, "c"),
                failure("logs", 3, "d")));

    String detail = stats.toWarning().orElseThrow().getDetail();
    assertTrue(detail.contains("and 1 more"), detail);
    assertFalse(detail.contains("[logs][3]"), detail);
  }

  @Test
  void a_failure_without_a_cause_falls_back_to_its_reason_text() {
    ShardSearchFailure bare = mock(ShardSearchFailure.class);
    when(bare.index()).thenReturn("logs");
    when(bare.shardId()).thenReturn(2);
    when(bare.getCause()).thenReturn(null);
    when(bare.reason()).thenReturn("shard not available");

    ShardStats stats = ShardStats.from(searchResponse(2, 1, 0, 1, false, bare));

    assertEquals(List.of("[logs][2] shard not available"), stats.failures());
  }

  private static ShardSearchFailure failure(String index, int shard, String message) {
    ShardSearchFailure failure = mock(ShardSearchFailure.class);
    when(failure.index()).thenReturn(index);
    when(failure.shardId()).thenReturn(shard);
    when(failure.getCause()).thenReturn(new IllegalStateException(message));
    return failure;
  }

  private static SearchResponse searchResponse(
      int total,
      int successful,
      int skipped,
      int failed,
      boolean timedOut,
      ShardSearchFailure... failures) {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getTotalShards()).thenReturn(total);
    when(response.getSuccessfulShards()).thenReturn(successful);
    when(response.getSkippedShards()).thenReturn(skipped);
    when(response.getFailedShards()).thenReturn(failed);
    when(response.isTimedOut()).thenReturn(timedOut);
    when(response.getShardFailures()).thenReturn(failures);
    return response;
  }
}
