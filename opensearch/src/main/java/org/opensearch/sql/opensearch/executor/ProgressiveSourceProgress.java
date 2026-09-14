/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/**
 * Combines low-cost progress signals from every physical OpenSearch source.
 *
 * <p>Source completion contributes at most 80% of public progress. The remaining 20% is reserved
 * for coordinator work without pretending to measure it. Successful job completion changes the
 * public value from at most 0.8 to 1.0.
 */
final class ProgressiveSourceProgress {
  static final double MAX_RUNNING_FRACTION = 0.8D;

  private final Map<Long, SourceState> states = new LinkedHashMap<>();
  private double lastFraction;

  static ProgressiveSourceProgress create(RelNode physicalPlan) {
    ProgressiveSourceProgress tracker = new ProgressiveSourceProgress();
    tracker.register(physicalPlan);
    return tracker;
  }

  static ProgressiveSourceProgress forTesting(Map<Long, Long> estimatedDocuments) {
    ProgressiveSourceProgress tracker = new ProgressiveSourceProgress();
    estimatedDocuments.forEach(
        (sourceId, estimate) -> tracker.states.put(sourceId, new SourceState(estimate)));
    return tracker;
  }

  private void register(RelNode rel) {
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      long sourceId = ProgressiveQueryContext.registerSourceOccurrence(scan);
      if (sourceId > 0L) {
        long estimate = scan.getOsIndex().getDocumentCountEstimate();
        states.put(sourceId, new SourceState(estimate));
        scan.setProgressiveSource(sourceId);
      }
    }
    for (RelNode input : rel.getInputs()) {
      register(input);
    }
  }

  boolean hasSources() {
    return !states.isEmpty();
  }

  synchronized QueryProgress updateSearch(long sourceId, QueryProgress searchProgress) {
    SourceState state = states.get(sourceId);
    if (state != null) {
      state.searchFraction = clampUnit(searchProgress.fractionDone());
    }
    return snapshot();
  }

  synchronized QueryProgress updateRows(
      long sourceId,
      long completedRows,
      long observedTotalRows,
      boolean observedTotalExact,
      boolean complete) {
    SourceState state = states.get(sourceId);
    if (state != null) {
      long previousCompleted = state.completedRows;
      state.completedRows = Math.max(previousCompleted, Math.max(0L, completedRows));
      state.inFlightRows = 0D;
      long latestPage = Math.max(1L, state.completedRows - previousCompleted);
      if (observedTotalRows >= 0L) {
        state.estimatedRows =
            observedTotalExact
                ? Math.max(state.completedRows, observedTotalRows)
                : Math.max(
                    state.estimatedRows,
                    Math.max(state.completedRows + latestPage, observedTotalRows));
        state.estimateKnown = true;
      } else if (!state.estimateKnown && state.completedRows > 0L) {
        state.estimatedRows = state.completedRows + latestPage;
      }
      state.complete |= complete;
    }
    return snapshot();
  }

  synchronized QueryProgress updatePage(
      long sourceId, QueryProgress pageProgress, long expectedPageUnits) {
    SourceState state = states.get(sourceId);
    if (state != null) {
      state.inFlightRows = clampUnit(pageProgress.fractionDone()) * Math.max(1L, expectedPageUnits);
    }
    return snapshot();
  }

  synchronized QueryProgress current() {
    return snapshot();
  }

  private QueryProgress snapshot() {
    if (states.isEmpty()) {
      return QueryProgress.ZERO;
    }
    boolean allEstimatesKnown = states.values().stream().allMatch(state -> state.estimateKnown);
    boolean allEstimatesEmpty = allEstimatesKnown && totalEstimatedRows() == 0L;
    double weightedDone = 0D;
    double totalWeight = 0D;
    for (SourceState state : states.values()) {
      double weight = allEstimatesKnown ? Math.max(0D, state.estimatedRows) : 1D;
      if (allEstimatesEmpty) {
        weight = 1D;
      }
      weightedDone += weight * state.fraction();
      totalWeight += weight;
    }
    double combined = totalWeight == 0D ? 0D : weightedDone / totalWeight;
    double candidate = MAX_RUNNING_FRACTION * clampUnit(combined);
    lastFraction = Math.max(lastFraction, Math.min(MAX_RUNNING_FRACTION, candidate));
    return new QueryProgress(lastFraction);
  }

  private long totalEstimatedRows() {
    long total = 0L;
    for (SourceState state : states.values()) {
      if (state.estimatedRows > Long.MAX_VALUE - total) {
        return Long.MAX_VALUE;
      }
      total += Math.max(0L, state.estimatedRows);
    }
    return total;
  }

  private static double clampUnit(double value) {
    if (!Double.isFinite(value)) {
      return 0D;
    }
    return Math.max(0D, Math.min(1D, value));
  }

  private static final class SourceState {
    private long completedRows;
    private long estimatedRows;
    private double inFlightRows;
    private double searchFraction;
    private boolean complete;
    private boolean estimateKnown;

    private SourceState(long estimatedRows) {
      this.estimatedRows = Math.max(0L, estimatedRows);
      this.estimateKnown = estimatedRows >= 0L;
    }

    private double fraction() {
      if (complete) {
        return 1D;
      }
      if (completedRows > 0L) {
        return Math.min(
            1D,
            (completedRows + inFlightRows) / Math.max(completedRows + inFlightRows, estimatedRows));
      }
      if (inFlightRows > 0D) {
        return Math.min(1D, inFlightRows / Math.max(inFlightRows, estimatedRows));
      }
      return searchFraction;
    }
  }
}
