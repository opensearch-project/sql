/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/**
 * Observes query-phase boundaries — parse, analyze, optimize, compile, etc. — without pulling any
 * tracing dependency into core/ppl. {@link ProfileScope} calls {@link #onPhaseStart(String)} when a
 * phase opens and closes the returned handle when the phase ends, so the listener sees the exact
 * same start/end boundary that feeds the profile metric.
 *
 * <p>The default implementation is a no-op ({@link #NOOP}). Modules that emit tracing spans (e.g.
 * the {@code opensearch} module) install a concrete listener via {@link
 * ProfileScope#installListener(PhaseListener)}.
 */
public interface PhaseListener {

  /** Called when a phase starts. The returned handle is closed when the phase ends. */
  Handle onPhaseStart(String phaseName);

  /** Observation of a single phase — the caller closes it (and may call {@link #setError}). */
  interface Handle extends AutoCloseable {
    /** Record a failure that occurred inside the phase. Called before {@link #close()}. */
    default void setError(Throwable t) {}

    @Override
    void close();
  }

  Handle NOOP_HANDLE =
      new Handle() {
        @Override
        public void close() {}
      };

  PhaseListener NOOP = phaseName -> NOOP_HANDLE;
}
