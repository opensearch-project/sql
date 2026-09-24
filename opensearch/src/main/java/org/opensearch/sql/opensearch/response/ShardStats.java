/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.sql.executor.Warning;

/**
 * The shard-level outcome of one search, captured off the {@link SearchResponse} so a scan can tell
 * whether the rows it is about to return cover every shard of the queried indices.
 *
 * <p>OpenSearch answers with HTTP 200 as long as one shard responded ({@code
 * search.default_allow_partial_results} defaults to true), so a result built from {@code hits} and
 * {@code aggregations} alone can silently omit whole shards. Three outcomes make a result partial,
 * and only the first sets {@code failed}: a shard returned an error, a shard had no available copy
 * (the counts do not add up to {@code total}), or the search timed out before every shard replied.
 *
 * <p>Note the counters are not disjoint: a shard skipped by the can-match phase is reported in both
 * {@code skipped} and {@code successful}, so the invariant is {@code total = successful + failed +
 * missing} and {@code skipped} plays no part in deciding completeness. See {@link #missing()}.
 */
public record ShardStats(
    int total, int successful, int skipped, int failed, boolean timedOut, List<String> failures) {

  /** Used where the shard outcome is unknown, e.g. a response synthesized from bare hits. */
  public static final ShardStats UNKNOWN = new ShardStats(0, 0, 0, 0, false, List.of());

  /**
   * Max distinct failure reasons to spell out in a warning; the rest are summarized as "N more".
   */
  static final int MAX_REASONS = 3;

  public static ShardStats from(SearchResponse searchResponse) {
    return new ShardStats(
        searchResponse.getTotalShards(),
        searchResponse.getSuccessfulShards(),
        searchResponse.getSkippedShards(),
        searchResponse.getFailedShards(),
        searchResponse.isTimedOut(),
        describeFailures(searchResponse.getShardFailures()));
  }

  /**
   * Shards the search never reached, e.g. no copy was available.
   *
   * <p>Counted as {@code total - successful - failed}, because a shard skipped by the can-match
   * phase is reported in {@code skipped} <em>and</em> in {@code successful} -- verified against a
   * live cluster: one unassigned shard, one skipped and one searched reports {@code total: 3,
   * successful: 2, skipped: 1}. Subtracting {@code skipped} a second time would hide a missing
   * shard behind every skipped one, which is the norm for a time-filtered query over many shards.
   */
  public int missing() {
    return Math.max(0, total - successful - failed);
  }

  /** Whether the rows produced alongside these stats cover every shard. */
  public boolean isComplete() {
    return !timedOut && failed == 0 && missing() == 0;
  }

  /**
   * The warning to attach to the response, or empty when the search covered every shard. Built here
   * rather than at the call site so every scan reports the same outcome identically.
   */
  public Optional<Warning> toWarning() {
    if (total == 0 || isComplete()) {
      return Optional.empty();
    }
    return Optional.of(
        new Warning(Warning.TYPE_PARTIAL_RESULT_SHARD_FAILURE, buildMessage(), buildDetail()));
  }

  /** Leads with the consequence, then the counts, mirroring how the DSL clients phrase it. */
  private String buildMessage() {
    if (failed > 0) {
      return String.format("Results are partial: %d of %d shards failed.", failed, total);
    }
    if (missing() > 0) {
      return String.format(
          "Results are partial: %d of %d shards did not return data.", missing(), total);
    }
    return "Results are partial: the search timed out before all shards responded.";
  }

  private String buildDetail() {
    StringBuilder detail =
        new StringBuilder(
            "Rows and aggregate values from the shards that did not respond are missing, so counts"
                + " may be undercounted.");
    if (failed > 0 && !failures.isEmpty()) {
      detail.append(" Shard failures: ").append(formatReasons()).append('.');
    } else if (missing() > 0) {
      detail.append(
          " No copy of those shards was available -- a node may be down or a shard unassigned.");
    }
    if (timedOut && (failed > 0 || missing() > 0)) {
      detail.append(" The search also timed out before every shard replied.");
    }
    detail.append(
        " Retry the query, or set search.default_allow_partial_results to false so such searches"
            + " fail instead of returning a subset.");
    return detail.toString();
  }

  private String formatReasons() {
    String spelled =
        failures.stream().limit(MAX_REASONS).collect(Collectors.joining("; ", "[", "]"));
    int remaining = failures.size() - MAX_REASONS;
    return remaining > 0 ? spelled + " and " + remaining + " more" : spelled;
  }

  /**
   * Summarize per-shard failures the way the DSL response does (index, shard, reason), keeping each
   * distinct reason once so one cause repeated across many shards is reported once.
   */
  private static List<String> describeFailures(ShardSearchFailure[] shardFailures) {
    if (shardFailures == null || shardFailures.length == 0) {
      return List.of();
    }
    Set<String> distinct =
        Arrays.stream(shardFailures)
            .map(ShardStats::describeFailure)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return List.copyOf(distinct);
  }

  private static String describeFailure(ShardSearchFailure failure) {
    String location =
        failure.index() == null
            ? "unknown shard"
            : String.format("[%s][%d]", failure.index(), failure.shardId());
    Throwable cause = failure.getCause();
    if (cause == null) {
      return String.format("%s %s", location, failure.reason());
    }
    String message = cause.getMessage();
    return message == null
        ? String.format("%s %s", location, cause.getClass().getSimpleName())
        : String.format("%s %s: %s", location, cause.getClass().getSimpleName(), message);
  }
}
