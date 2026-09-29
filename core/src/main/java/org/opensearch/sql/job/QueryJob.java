/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Active object that carries one query through the lifecycle state machine.
 *
 * <p>Instances are constructed only by {@link QueryJobService} implementations. The public surface
 * exposes state readers ({@link #id()}, {@link #owner()}, {@link #status()}), lifecycle waits
 * ({@link #awaitOutcome(Duration)}, {@link #onTerminal(Runnable)}), and a single mutator ({@link
 * #cancel()}). Consumers cannot reach the underlying reactive stage — the raw {@code
 * CompletableFuture} is fully encapsulated so no caller can invent its own blocking or reactive
 * coupling.
 *
 * <p>State transitions:
 *
 * <pre>
 *   PENDING --startRunner()--&gt; RUNNING --runner success--&gt; SUCCEEDED
 *                                     \--runner failure--&gt; FAILED
 *                                     \--cancel()------&gt; CANCELLED
 *   PENDING --cancel()------&gt; CANCELLED
 * </pre>
 *
 * <h2>Thread safety</h2>
 *
 * All mutable state is guarded by {@code this}. Side effects that may block or re-enter — invoking
 * the runner, cancelling it, completing the future — are performed after the monitor is released.
 */
public final class QueryJob {

  private final QueryJobId id;
  private final Principal owner;
  private final QueryRunner runner;
  private final Clock clock;
  private final long submittedAtMillis;
  private final CompletableFuture<QueryResult> completion = new CompletableFuture<>();

  private QueryJobState state = QueryJobState.PENDING;
  private OptionalLong startedAtMillis = OptionalLong.empty();
  private OptionalLong completedAtMillis = OptionalLong.empty();
  private Optional<QueryFailure> failure = Optional.empty();
  private Optional<QueryResult> result = Optional.empty();

  /**
   * Creates a job in {@link QueryJobState#PENDING}. Package-private: only {@link QueryJobService}
   * implementations, which share this package, may construct a job. The submission time is captured
   * from {@code clock} at construction; the runner is not started here.
   *
   * @param id opaque, node-routable identifier for this job
   * @param owner caller identity retained for later authorization on {@code get} / {@code cancel}
   * @param runner engine adapter that will produce the result; started later via {@link
   *     #startRunner()}
   * @param clock time source used for submission, start, and completion timestamps
   * @throws NullPointerException if any argument is {@code null}
   */
  QueryJob(QueryJobId id, Principal owner, QueryRunner runner, Clock clock) {
    this.id = Objects.requireNonNull(id, "id must not be null");
    this.owner = Objects.requireNonNull(owner, "owner must not be null");
    this.runner = Objects.requireNonNull(runner, "runner must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.submittedAtMillis = clock.millis();
  }

  /** Returns the opaque, node-routable job identifier. */
  public QueryJobId id() {
    return id;
  }

  /** Returns the caller identity captured at submission. */
  public Principal owner() {
    return owner;
  }

  /** Returns an immutable snapshot of the job's current state. */
  public synchronized QueryJobStatus status() {
    return new QueryJobStatus(
        id, state, submittedAtMillis, startedAtMillis, completedAtMillis, failure, result);
  }

  /**
   * Bounded, non-blocking wait. Returns immediately with a stage that fires exactly once with an
   * {@link Outcome} — {@link Outcome.Terminal} when the runner succeeds within budget, {@link
   * Outcome.Failed} on runner failure or cancellation, {@link Outcome.Pending} when the wait budget
   * expires first.
   *
   * <p>The calling thread returns as soon as the callbacks are registered. Whichever event fires
   * first (runner completion or timeout) wins; the loser's later firing is a no-op.
   *
   * <p>The caller's {@code whenComplete} runs on whichever thread completes the outcome:
   * runner-completing thread on the terminal / failed path, JDK's internal {@code Delayer} daemon
   * on the pending path. Callers that require a specific pool should use {@code
   * whenCompleteAsync(cb, executor)} on the returned stage. Callers that need {@code ThreadContext}
   * preserved (security identity, tracing span) should wrap their listener via {@code
   * ContextPreservingActionListener.wrapPreservingContext(listener, threadContext)} before
   * registering the callback.
   *
   * <p>Non-{@link Exception} throwables from the runner are re-thrown from the internal handler so
   * JVM-level errors (e.g. {@link OutOfMemoryError}) are not silently downgraded to an
   * application-level {@link Outcome.Failed}.
   *
   * @param budget maximum time to wait. {@code null}, zero, or negative → {@link Outcome.Pending}
   *     immediately unless the job is already terminal.
   * @return stage that completes with exactly one {@link Outcome}
   */
  public CompletionStage<Outcome> awaitOutcome(Duration budget) {
    CompletableFuture<Outcome> out = new CompletableFuture<>();
    completion.whenComplete(
        (value, throwable) -> {
          if (throwable == null) {
            out.complete(new Outcome.Terminal(value));
            return;
          }
          Throwable cause = unwrap(throwable);
          if (cause instanceof Error error) {
            throw error;
          }
          Exception ex =
              cause instanceof Exception exception ? exception : new RuntimeException(cause);
          out.complete(new Outcome.Failed(ex));
        });
    long millis = budget == null ? 0L : budget.toMillis();
    if (millis <= 0L) {
      out.complete(new Outcome.Pending());
    } else {
      out.completeOnTimeout(new Outcome.Pending(), millis, TimeUnit.MILLISECONDS);
    }
    return out.minimalCompletionStage();
  }

  /**
   * Registers a one-shot terminal hook. {@code action} runs exactly once when the job reaches any
   * terminal state ({@code SUCCEEDED}, {@code FAILED}, or {@code CANCELLED}).
   *
   * <p>Narrow API for subscribers (retention, metrics, tracing) that need only "the runner is done"
   * and do not consume the result. Fires on the thread that completed the runner; wrap behavior in
   * an executor if a specific pool is required.
   *
   * @param action non-null runnable; exceptions escape to the completion thread's default handler
   */
  public void onTerminal(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    completion.whenComplete((result, err) -> action.run());
  }

  /**
   * Requests cancellation. Terminal states are unaffected. Cancellation from {@code PENDING} or
   * {@code RUNNING} moves the job to {@code CANCELLED}, cancels the runner (best effort), and
   * completes the internal future exceptionally with {@link CancellationException}.
   */
  public void cancel() {
    boolean shouldCancelRunner;
    synchronized (this) {
      if (state.isTerminal()) {
        return;
      }
      state = QueryJobState.CANCELLED;
      completedAtMillis = OptionalLong.of(clock.millis());
      shouldCancelRunner = true;
    }
    if (shouldCancelRunner) {
      safeCancelRunner();
    }
    completion.cancel(false);
  }

  /**
   * Transitions the job from {@link QueryJobState#PENDING} to {@link QueryJobState#RUNNING}, calls
   * {@link QueryRunner#run()}, and wires the returned stage into this job's state machine.
   *
   * <p>Package-private. The service invokes this exactly once, immediately after publishing the job
   * to the {@link QueryJobStore}. Any of the following short-circuit the call:
   *
   * <ul>
   *   <li>the job has already been cancelled while pending — the runner is not started;
   *   <li>{@code runner.run()} throws — the job moves to {@link QueryJobState#FAILED};
   *   <li>{@code runner.run()} returns {@code null} — treated as a runner failure.
   * </ul>
   */
  void startRunner() {
    synchronized (this) {
      if (state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.RUNNING;
      startedAtMillis = OptionalLong.of(clock.millis());
    }
    CompletionStage<QueryResult> stage;
    try {
      stage = Objects.requireNonNull(runner.run(), "runner must not return null");
    } catch (RuntimeException e) {
      onRunnerFailure(e);
      return;
    }
    stage.whenComplete(
        (value, throwable) -> {
          if (throwable != null) {
            onRunnerFailure(unwrap(throwable));
          } else if (value == null) {
            onRunnerFailure(new IllegalStateException("runner completed with null result"));
          } else {
            onRunnerSuccess(value);
          }
        });
  }

  /**
   * Terminal transition from {@link QueryJobState#RUNNING} to {@link QueryJobState#SUCCEEDED}.
   * Ignored if the job has already left {@code RUNNING} (e.g. a concurrent {@link #cancel()} beat
   * the runner).
   */
  private void onRunnerSuccess(QueryResult value) {
    synchronized (this) {
      if (state != QueryJobState.RUNNING) {
        return;
      }
      state = QueryJobState.SUCCEEDED;
      completedAtMillis = OptionalLong.of(clock.millis());
      result = Optional.of(value);
    }
    completion.complete(value);
  }

  /**
   * Terminal transition to {@link QueryJobState#FAILED}. Accepts from either {@code RUNNING} or
   * {@code PENDING} (synchronous throw from {@link QueryRunner#run()}). Ignored once terminal.
   */
  private void onRunnerFailure(Throwable throwable) {
    synchronized (this) {
      if (state != QueryJobState.RUNNING && state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.FAILED;
      completedAtMillis = OptionalLong.of(clock.millis());
      failure = Optional.of(QueryFailure.of(throwable));
    }
    completion.completeExceptionally(throwable);
  }

  /** Best-effort cancel of the runner; swallows RuntimeException. */
  private void safeCancelRunner() {
    try {
      runner.cancel();
    } catch (RuntimeException ignored) {
      // Cancellation is best-effort; a misbehaving runner must not block the state machine.
    }
  }

  /**
   * Peels a single {@link CompletionException} wrapper so downstream reporting sees the original
   * runner exception. Non-wrapper throwables and wrappers with no cause pass through unchanged.
   * Public so future consumers share the same unwrap semantics.
   *
   * @param throwable throwable observed on the completion stage
   * @return the underlying cause when {@code throwable} is a {@link CompletionException} carrying a
   *     non-{@code null} cause; otherwise the original throwable
   */
  public static Throwable unwrap(Throwable throwable) {
    return throwable instanceof CompletionException && throwable.getCause() != null
        ? throwable.getCause()
        : throwable;
  }
}
