/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.sql.executor.Warning;

/**
 * Shard-level outcome of one search, so a scan can tell whether its rows cover every shard. A
 * search returns 200 once any shard responds, so three outcomes make a result partial: a shard
 * failed, a shard had no available copy, or the search timed out.
 */
public record ShardStats(
    int total, int successful, int skipped, int failed, boolean timedOut, List<String> failures) {

  /** No SearchResponse to read, e.g. a page synthesized from bare hits. */
  public static final ShardStats UNKNOWN = new ShardStats(0, 0, 0, 0, false, List.of());

  /** Reasons to spell out in a warning; the rest are summarized as "N more". */
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
   * A PIT created over only some of its shards pins that subset, so every search against it looks
   * complete. The gap is only visible here, at creation.
   */
  public static ShardStats from(CreatePitResponse pitResponse) {
    return new ShardStats(
        pitResponse.getTotalShards(),
        pitResponse.getSuccessfulShards(),
        pitResponse.getSkippedShards(),
        pitResponse.getFailedShards(),
        false,
        describeFailures(pitResponse.getShardFailures()));
  }

  /**
   * Shards the search never reached. Not {@code - skipped}: a can-match skipped shard is counted in
   * both {@code skipped} and {@code successful}, so subtracting it twice hides a missing shard.
   */
  public int missing() {
    return Math.max(0, total - successful - failed);
  }

  public boolean isComplete() {
    return !timedOut && failed == 0 && missing() == 0;
  }

  /** The warning to attach, or empty when the search covered every shard. */
  public Optional<Warning> toWarning() {
    if (isComplete()) {
      return Optional.empty();
    }
    return Optional.of(
        new Warning(Warning.TYPE_PARTIAL_RESULT_SHARD_FAILURE, buildMessage(), buildDetail()));
  }

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
   * One line per distinct reason, keyed on the reason alone so one cause spanning many shards is
   * reported once, labelled with the first shard that hit it.
   */
  private static List<String> describeFailures(ShardSearchFailure[] shardFailures) {
    if (shardFailures == null || shardFailures.length == 0) {
      return List.of();
    }
    Map<String, String> byReason = new LinkedHashMap<>();
    for (ShardSearchFailure failure : shardFailures) {
      byReason.putIfAbsent(reasonOf(failure), location(failure) + " " + reasonOf(failure));
    }
    return List.copyOf(byReason.values());
  }

  private static String reasonOf(ShardSearchFailure failure) {
    Throwable cause = failure.getCause();
    if (cause == null) {
      return failure.reason();
    }
    String message = cause.getMessage();
    return message == null
        ? cause.getClass().getSimpleName()
        : cause.getClass().getSimpleName() + ": " + message;
  }

  private static String location(ShardSearchFailure failure) {
    return failure.index() == null
        ? "unknown shard"
        : String.format("[%s][%d]", failure.index(), failure.shardId());
  }
}
