/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Objects;

/**
 * Result of a bounded, non-blocking wait on a {@link QueryJob}.
 *
 * <p>Sealed to three variants so callers switch exhaustively — no leaked {@code CompletionStage},
 * no checked exceptions from blocking {@code .get()}, no separate {@code CancellationException}
 * catch. Cancellation surfaces as {@link Failed} with the raw {@link
 * java.util.concurrent.CancellationException} as its cause.
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li>Exactly one variant is produced per bounded-wait call.
 *   <li>{@link Terminal} implies the job entered {@link QueryJobState#SUCCEEDED}. Failures
 *       (including cancellations) surface as {@link Failed}.
 *   <li>{@link Pending} means the wait budget expired before the job reached a terminal state. The
 *       job is still live in the store and observable via a subsequent GET.
 *   <li>{@link Failed} carries an already-unwrapped {@link Exception}, so the transport-layer
 *       {@code ActionListener.onFailure(Exception)} needs no additional cast or unwrap.
 * </ul>
 *
 * <p>This type intentionally lives beside {@link QueryResult} rather than inside it. The two
 * describe different concepts: {@code QueryResult} is what the engine produced (durable, may be
 * stored on {@link QueryJobStatus}, serialized across nodes); {@code Outcome} is the transient
 * per-call observation of a bounded wait — never stored, never persisted.
 */
public sealed interface Outcome permits Outcome.Terminal, Outcome.Pending, Outcome.Failed {

  /**
   * Runner completed successfully within the caller's wait budget.
   *
   * @param result engine-produced result; never {@code null}
   */
  record Terminal(QueryResult result) implements Outcome {
    public Terminal {
      Objects.requireNonNull(result, "result must not be null");
    }
  }

  /**
   * Wait budget expired before the runner reached a terminal state. The job continues in the store;
   * a subsequent {@code GET} on its id observes {@code RUNNING} or a later terminal state.
   */
  record Pending() implements Outcome {}

  /**
   * Runner reached a non-{@code SUCCEEDED} terminal state within the wait budget. The cause has
   * already been unwrapped ({@link QueryJob#unwrap}); it is guaranteed to be an {@link Exception}
   * so callers can pass it directly to listeners.
   *
   * @param cause runner exception or cancellation; never {@code null}
   */
  record Failed(Exception cause) implements Outcome {
    public Failed {
      Objects.requireNonNull(cause, "cause must not be null");
    }
  }
}
