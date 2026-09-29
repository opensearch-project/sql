/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.TimewrapPivot;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;

/**
 * A snapshot of the timewrap pivot signals that {@code CalciteRelNodeVisitor.visitTimewrap} stores
 * in {@link CalcitePlanContext} thread-locals.
 *
 * <p>On the analytics route, planning and result-conversion run on different threads — the result
 * callback fires on an analytics worker pool, not the SQL worker thread that planned the query.
 * Capturing the signals at execute() entry (on the planning thread) and carrying them into the
 * callback keeps the pivot correct across that thread hop. Capturing also clears the thread-locals
 * so they don't leak onto the planning thread's next query.
 */
public final class TimewrapSignals {

  private final boolean active;
  private final String unitName;
  private final String series;

  private TimewrapSignals(boolean active, String unitName, String series) {
    this.active = active;
    this.unitName = unitName;
    this.series = series;
  }

  /** Captures the current thread's timewrap signals and clears the thread-locals. */
  public static TimewrapSignals captureAndClear() {
    boolean active = TimewrapPivot.isTimewrap();
    String unitName = CalcitePlanContext.timewrapUnitName.get();
    String series = CalcitePlanContext.timewrapSeries.get();
    CalcitePlanContext.clearTimewrapSignals();
    return new TimewrapSignals(active, unitName, series);
  }

  /**
   * Applies the timewrap pivot to {@code response} if this snapshot is from a timewrap query;
   * otherwise returns it unchanged. The returned response carries over the input's profile/error.
   */
  public QueryResponse pivot(QueryResponse response) {
    if (!active) {
      return response;
    }
    TimewrapPivot.Result pivoted =
        TimewrapPivot.pivot(
            response.getSchema().getColumns(), response.getResults(), unitName, series);
    QueryResponse out =
        new QueryResponse(new Schema(pivoted.columns()), pivoted.values(), response.getCursor());
    out.setProfile(response.getProfile());
    out.setError(response.getError());
    return out;
  }
}
