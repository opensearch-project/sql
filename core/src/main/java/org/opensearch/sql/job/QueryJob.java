/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Active object that carries one query through the lifecycle state machine.
 *
 * <p>Instances are constructed only by {@link QueryJobService} implementations. The public surface
 * is deliberately small — four accessors and one mutator — so that engines cannot observe or drive
 * lifecycle transitions except through the runner they were given.
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
 * All mutable state is guarded by {@code this}. Side effects that may block or reenter — invoking
 * the runner, cancelling it, completing the future — are performed after the monitor is released.
 * The {@link CompletableFuture} that backs {@link #completion()} is never leaked to callers; only
 * its {@link CompletionStage} view is returned.
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
   * Returns a read-only view of the completion future. Completes normally with the {@link
   * QueryResult} on success, exceptionally with the original runner exception on failure, and
   * exceptionally with {@link java.util.concurrent.CancellationException} on cancel.
   */
  public CompletionStage<QueryResult> completion() {
    return completion.minimalCompletionStage();
  }

  /**
   * Requests cancellation. Terminal states are unaffected. Cancellation from {@code PENDING} or
   * {@code RUNNING} moves the job to {@code CANCELLED}, cancels the runner (best effort), and
   * completes {@link #completion()} exceptionally.
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
   * the runner). Records the result inside the monitor; completes the future outside it.
   *
   * @param value final result produced by the runner; never {@code null}
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
   * Terminal transition to {@link QueryJobState#FAILED}. Accepts the transition from either {@code
   * RUNNING} (normal failure path) or {@code PENDING} (synchronous throw from {@link
   * QueryRunner#run()}). Ignored once the job is already terminal.
   *
   * @param throwable exception raised by the runner; may be a raw cause or a {@link
   *     CompletionException} wrapper (already unwrapped in {@link #startRunner()})
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

  /**
   * Best-effort cancel of the runner. Swallows {@link RuntimeException} — a misbehaving runner must
   * not block the state machine, and the job has already been marked {@code CANCELLED} before this
   * is called.
   */
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
   *
   * @param throwable throwable observed on the runner's completion stage
   * @return the underlying cause when {@code throwable} is a {@link CompletionException} carrying a
   *     non-{@code null} cause; otherwise the original throwable
   */
  /**
   * Peels a single {@link CompletionException} wrapper so downstream reporting sees the original
   * runner exception. Non-wrapper throwables and wrappers with no cause pass through unchanged.
   * Public so consumers of {@link #completion()} share the same unwrap semantics.
   *
   * @param throwable throwable observed on the completion stage or through {@code get()}
   * @return the underlying cause when {@code throwable} is a {@link CompletionException} carrying a
   *     non-{@code null} cause; otherwise the original throwable
   */
  public static Throwable unwrap(Throwable throwable) {
    return throwable instanceof CompletionException && throwable.getCause() != null
        ? throwable.getCause()
        : throwable;
  }
}
