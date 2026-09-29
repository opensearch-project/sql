/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.tracing;

import org.opensearch.sql.monitor.profile.PhaseListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * {@link PhaseListener} that opens an OpenTelemetry span for each phase and closes it in lockstep
 * with the profile-metric timing. Bridges the tracer-free core/ppl {@link
 * org.opensearch.sql.monitor.profile.ProfileScope} to the {@link Tracer} living in the {@code
 * opensearch} module.
 *
 * <p>Install once at startup via {@link
 * org.opensearch.sql.monitor.profile.ProfileScope#installListener(PhaseListener)}.
 */
public final class TracingPhaseListener implements PhaseListener {

  private static final String SPAN_NAME_PREFIX = "opensearch.query.";

  private final Tracer tracer;

  public TracingPhaseListener(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public Handle onPhaseStart(String phaseName) {
    Span span = tracer.startSpan(SpanCreationContext.internal().name(SPAN_NAME_PREFIX + phaseName));
    SpanScope scope = tracer.withSpanInScope(span);
    return new Handle() {
      @Override
      public void setError(Throwable t) {
        span.setError(t instanceof Exception ? (Exception) t : new RuntimeException(t));
      }

      @Override
      public void close() {
        scope.close();
        span.endSpan();
      }
    };
  }
}
