/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.Locale;

/**
 * One measured boundary that feeds both query profiling and — if a {@link PhaseListener} is
 * installed — distributed tracing. Times a code block with {@link System#nanoTime()} and, on {@link
 * #close()}, adds the elapsed nanoseconds to the phase's {@link ProfileMetric} and closes the
 * listener's handle. Use with try-with-resources so both outputs share the same start/end boundary.
 *
 * <p>The phase name (span name and profile phase key alike) is derived from the {@link MetricName}
 * — {@code metric.name().toLowerCase(Locale.ROOT)} — the same rule {@link QueryProfile} uses, so
 * the two cannot drift apart.
 *
 * <p>Callers call {@link #setError(Throwable)} before rethrowing to record a failure — the listener
 * forwards it to its handle (e.g. onto a span). try-with-resources cannot observe the thrown
 * exception here.
 *
 * <p>Kept trace-free by design: the {@link PhaseListener} indirection means core/ppl never
 * reference {@code Tracer}. The {@code opensearch} module installs the tracing listener at startup.
 */
public final class ProfileScope implements AutoCloseable {

  private static volatile PhaseListener listener = PhaseListener.NOOP;

  /** Install a global phase listener (typically from the {@code opensearch} module). */
  public static void installListener(PhaseListener newListener) {
    listener = newListener == null ? PhaseListener.NOOP : newListener;
  }

  private final ProfileMetric metric;
  private final long startNanos;
  private final PhaseListener.Handle handle;

  private ProfileScope(ProfileMetric metric, long startNanos, PhaseListener.Handle handle) {
    this.metric = metric;
    this.startNanos = startNanos;
    this.handle = handle;
  }

  /** Open a phase that records a profile metric and (if a listener is installed) a tracing span. */
  public static ProfileScope open(MetricName metric) {
    return new ProfileScope(
        QueryProfiling.current().getOrCreateMetric(metric),
        System.nanoTime(),
        listener.onPhaseStart(metric.name().toLowerCase(Locale.ROOT)));
  }

  /**
   * Open a trace-only phase — a span with no matching profile metric. Used for work that happens
   * before {@link QueryProfiling} is activated on the current thread (e.g. transport-side parse and
   * dispatch) and therefore can't feed a metric anyway. {@code phaseName} must be a fixed literal
   * chosen by the caller, not derived from user input.
   */
  public static ProfileScope openTraceOnly(String phaseName) {
    return new ProfileScope(
        NoopProfileMetric.INSTANCE, System.nanoTime(), listener.onPhaseStart(phaseName));
  }

  /** Record a failure on the listener's handle. Callers invoke this before rethrowing. */
  public void setError(Throwable t) {
    handle.setError(t);
  }

  @Override
  public void close() {
    metric.add(System.nanoTime() - startNanos);
    handle.close();
  }
}
