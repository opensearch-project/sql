/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.plugin.PPLQueryErrorHandler;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.PPLQueryTask;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Active object representing one asynchronous PPL query on the owner node.
 *
 * <p>Each job owns its execution handle, cancellable task, retention deadline, and lease timer. It
 * publishes itself to a {@link QueryJobRegistry} at creation and removes itself when it becomes
 * unreachable through DELETE, expiry, direct completion, or shutdown. All state transitions are
 * synchronized on the job; every observable side effect (registry mutation, listener callback, task
 * cancellation, execution close) happens after the lock has been released.
 *
 * <p>The state machine tracks two axes: retention and completion.
 *
 * <pre>
 * Current state       Event               Next state             Response
 * RUNNING             direct success      REMOVED                final snapshot without ID
 * RUNNING             direct failure      REMOVED                failure without ID
 * RUNNING             retain deadline     RETAINED_RUNNING       running snapshot with ID
 * RETAINED_RUNNING    success             RETAINED_SUCCEEDED     none (final result kept)
 * RETAINED_RUNNING    failure             RETAINED_FAILED        none (failure kept)
 * *                   DELETE              REMOVED                status observed at DELETE
 * RETAINED_*          expire / shutdown   REMOVED                none
 * </pre>
 */
public final class QueryJob {
  private static final Logger LOG = LogManager.getLogger(QueryJob.class);

  private static final Cancellable NO_TIMEOUT = () -> {};

  /** Lifecycle state exposed in asynchronous PPL responses. */
  public enum Status {
    /** Query execution is still running. */
    RUNNING,

    /** Query execution completed successfully. */
    SUCCEEDED,

    /** Query execution failed. */
    FAILED,

    /** Query execution was cancelled by DELETE. */
    CANCELLED
  }

  /**
   * Immutable point-in-time response for a job.
   *
   * <p>Each variant exposes only the data valid for its lifecycle state.
   */
  public sealed interface Snapshot {
    /**
     * Returns the retained job ID.
     *
     * @return job ID, or empty for a final response returned directly by POST
     */
    Optional<QueryJobId> id();

    /**
     * Returns the public lifecycle state.
     *
     * @return response status
     */
    Status status();

    /**
     * Snapshot of a query that is still running.
     *
     * @param jobId retained job ID
     * @param response current result, or empty before a result is available
     */
    record Running(QueryJobId jobId, Optional<QueryResponse> response) implements Snapshot {
      @Override
      public Optional<QueryJobId> id() {
        return Optional.of(jobId);
      }

      @Override
      public Status status() {
        return Status.RUNNING;
      }
    }

    /**
     * Snapshot of a successfully completed query.
     *
     * @param id retained job ID, or empty for a direct POST response
     * @param response final query result
     * @param tookMillis elapsed execution time
     */
    record Succeeded(Optional<QueryJobId> id, QueryResponse response, long tookMillis)
        implements Snapshot {
      @Override
      public Status status() {
        return Status.SUCCEEDED;
      }
    }

    /**
     * Snapshot of a failed query.
     *
     * @param id retained job ID, or empty for a direct POST response
     * @param failure client-visible failure
     * @param tookMillis elapsed execution time
     */
    record Failed(Optional<QueryJobId> id, Failure failure, long tookMillis) implements Snapshot {
      @Override
      public Status status() {
        return Status.FAILED;
      }
    }
  }

  /**
   * Client-visible failure retained with a failed job.
   *
   * @param type exception type
   * @param reason client-facing failure reason
   */
  public record Failure(String type, String reason) {
    /**
     * Derives a client-visible failure from an exception.
     *
     * @param exception exception raised by execution
     * @return failure with the exception's simple type and message
     */
    public static Failure from(Exception exception) {
      String type =
          exception.getClass().getSimpleName().isBlank()
              ? exception.getClass().getName()
              : exception.getClass().getSimpleName();
      String reason =
          exception.getMessage() == null || exception.getMessage().isBlank()
              ? "query execution failed"
              : exception.getMessage();
      return new Failure(type, reason);
    }
  }

  /** Cancels a previously scheduled timer. Multiple cancellations are safe. */
  @FunctionalInterface
  public interface Cancellable {
    /** Cancels the timer if it has not already fired. */
    void cancel();
  }

  /** Schedules a one-shot runnable. Production wraps {@link ThreadPool}; tests use a fake. */
  @FunctionalInterface
  public interface Scheduler {
    /**
     * Schedules {@code task} to run once after {@code delayMillis} milliseconds.
     *
     * @param delayMillis delay before firing
     * @param task action to run
     * @return handle used to cancel the scheduled task
     */
    Cancellable schedule(long delayMillis, Runnable task);
  }

  /** Owned cancellable task and its registration cleanup. Package-private for tests. */
  record JobTask(CancellableTask task, Runnable release) {
    /** Releases task-manager registrations owned by this wrapper. Safe to call once. */
    void close() {
      release.run();
    }
  }

  /** Internal lifecycle; unlike {@link Status}, this includes retention and removal. */
  private enum State {
    RUNNING,
    RETAINED_RUNNING,
    RETAINED_SUCCEEDED,
    RETAINED_FAILED,
    REMOVED
  }

  private final QueryJobId id;
  private final QueryJobRegistry registry;
  private final LongSupplier clock;
  private final Scheduler scheduler;
  private final TaskManager taskManager;
  private final QueryJobOwner owner;
  private final long startTimeMillis;
  private final ActionListener<Snapshot> responseListener;

  private State state = State.RUNNING;
  private long keepAliveMillis;
  private long expirationTimeMillis;
  private long completionTimeMillis = -1L;
  private JobTask task;
  private AsyncQueryExecution execution;
  private Failure failure;
  private Cancellable retentionTimer = NO_TIMEOUT;
  private Cancellable expiryTimer = NO_TIMEOUT;

  private QueryJob(
      QueryJobId id,
      QueryJobRegistry registry,
      LongSupplier clock,
      Scheduler scheduler,
      TaskManager taskManager,
      QueryJobOwner owner,
      long keepAliveMillis,
      JobTask task,
      ActionListener<Snapshot> responseListener) {
    this.id = id;
    this.registry = registry;
    this.clock = clock;
    this.scheduler = scheduler;
    this.taskManager = taskManager;
    this.owner = owner;
    this.startTimeMillis = clock.getAsLong();
    this.keepAliveMillis = keepAliveMillis;
    this.expirationTimeMillis = startTimeMillis + keepAliveMillis;
    this.task = task;
    this.responseListener = responseListener;
  }

  /**
   * Starts an asynchronous PPL query in production wiring.
   *
   * <p>{@link TaskManager} registers the job's cancellable task as a child of {@code parentTask} so
   * cancellation of the POST request cascades. The job schedules its own retention deadline on
   * {@code threadPool} and calls {@code executionStarter} inline. If execution finishes before the
   * deadline, {@code responseListener} receives the final result without a job ID. Otherwise the
   * job becomes retained and the listener receives a running snapshot with the ID.
   *
   * @param registry registry that will hold the job while it is discoverable
   * @param threadPool schedules the retention deadline and keep-alive expiry
   * @param taskManager registers and cancels the job's own task
   * @param parentTask POST request task; the job task becomes its child
   * @param request transport request used to register the job task
   * @param owner caller retained with the job for later authorization
   * @param keepAlive lease duration applied when the job is retained
   * @param waitForCompletion maximum time to wait for a direct result
   * @param executionStarter starts execution using the job-owned cancellable task
   * @param responseListener receives either the direct result or the retained running snapshot
   * @return newly created job, already published to {@code registry}
   * @throws NullPointerException if any required argument is null
   */
  public static QueryJob create(
      QueryJobRegistry registry,
      ThreadPool threadPool,
      TaskManager taskManager,
      PPLQueryTask parentTask,
      TransportPPLQueryRequest request,
      QueryJobOwner owner,
      TimeValue keepAlive,
      TimeValue waitForCompletion,
      Function<CancellableTask, AsyncQueryExecution> executionStarter,
      ActionListener<Snapshot> responseListener) {
    Objects.requireNonNull(threadPool);
    Objects.requireNonNull(taskManager);
    JobTask jobTask = registerJobTask(taskManager, parentTask, request);
    try {
      return create(
          taskManager.localNode().getId(),
          registry,
          System::currentTimeMillis,
          wrapScheduler(threadPool),
          taskManager,
          jobTask,
          owner,
          keepAlive,
          waitForCompletion,
          executionStarter,
          responseListener);
    } catch (RuntimeException | Error e) {
      jobTask.close();
      throw e;
    }
  }

  /**
   * Test entry point that accepts injectable clock, scheduler, and pre-registered task.
   *
   * @param localNodeId local node ID used to route the ID back on future requests
   * @param registry registry that will hold the job while it is discoverable
   * @param clock time source
   * @param scheduler timer used for retention deadline and keep-alive expiry
   * @param taskManager cancels the job task with descendant cascade; may be {@code null} when the
   *     job task should be cancelled directly
   * @param task pre-registered cancellable task owned by the job
   * @param owner caller retained with the job for later authorization
   * @param keepAlive lease duration applied when the job is retained
   * @param waitForCompletion maximum time to wait for a direct result
   * @param executionStarter starts execution using the job-owned cancellable task
   * @param responseListener receives either the direct result or the retained running snapshot
   * @return newly created job, already published to {@code registry}
   */
  static QueryJob create(
      String localNodeId,
      QueryJobRegistry registry,
      LongSupplier clock,
      Scheduler scheduler,
      TaskManager taskManager,
      JobTask task,
      QueryJobOwner owner,
      TimeValue keepAlive,
      TimeValue waitForCompletion,
      Function<CancellableTask, AsyncQueryExecution> executionStarter,
      ActionListener<Snapshot> responseListener) {
    Objects.requireNonNull(localNodeId);
    Objects.requireNonNull(registry);
    Objects.requireNonNull(clock);
    Objects.requireNonNull(scheduler);
    Objects.requireNonNull(task);
    Objects.requireNonNull(owner);
    Objects.requireNonNull(keepAlive);
    Objects.requireNonNull(waitForCompletion);
    Objects.requireNonNull(executionStarter);
    Objects.requireNonNull(responseListener);

    QueryJob job =
        publish(
            localNodeId,
            registry,
            clock,
            scheduler,
            taskManager,
            owner,
            keepAlive.millis(),
            task,
            responseListener);
    try {
      job.scheduleRetention(waitForCompletion);
    } catch (RuntimeException | Error e) {
      job.discard("PPL asynchronous query startup failed");
      throw e;
    }
    job.startExecution(executionStarter);
    return job;
  }

  private static QueryJob publish(
      String localNodeId,
      QueryJobRegistry registry,
      LongSupplier clock,
      Scheduler scheduler,
      TaskManager taskManager,
      QueryJobOwner owner,
      long keepAliveMillis,
      JobTask task,
      ActionListener<Snapshot> responseListener) {
    while (true) {
      QueryJobId id = QueryJobId.create(localNodeId);
      QueryJob job =
          new QueryJob(
              id,
              registry,
              clock,
              scheduler,
              taskManager,
              owner,
              keepAliveMillis,
              task,
              responseListener);
      if (registry.add(job) == null) {
        return job;
      }
    }
  }

  /**
   * Returns the opaque ID assigned to this job.
   *
   * @return job ID used as the registry key
   */
  public QueryJobId getJobId() {
    return id;
  }

  /**
   * Returns the immutable owner identity captured when this job was created.
   *
   * @return job owner used for GET and DELETE authorization
   */
  public QueryJobOwner getOwner() {
    return owner;
  }

  /**
   * Returns the current snapshot of the job.
   *
   * <p>A non-null {@code keepAlive} starts a new lease from the current time. A call at or after
   * the current expiration removes the job and reports it as not found.
   *
   * @param keepAlive new lease duration, or {@code null} to leave the current lease unchanged
   * @return immutable snapshot of the current state
   * @throws ResourceNotFoundException if the job has been removed or has just expired
   */
  public Snapshot get(TimeValue keepAlive) {
    long now = clock.getAsLong();
    AsyncQueryExecution executionForResult;
    State stateForResult;
    long tookMillis;
    Failure failureForResult;
    boolean expired = false;
    Cancellable retentionToCancel = NO_TIMEOUT;
    Cancellable expiryToCancel = NO_TIMEOUT;
    Cancellable rescheduledExpiry = NO_TIMEOUT;
    JobTask taskToClose = null;
    AsyncQueryExecution executionToClose = null;

    synchronized (this) {
      ensurePresent();
      if (state != State.RUNNING && now >= expirationTimeMillis) {
        expired = true;
        retentionToCancel = retentionTimer;
        expiryToCancel = expiryTimer;
        retentionTimer = NO_TIMEOUT;
        expiryTimer = NO_TIMEOUT;
        taskToClose = detachTask();
        executionToClose = detachExecution();
        state = State.REMOVED;
        executionForResult = null;
        stateForResult = null;
        tookMillis = 0L;
        failureForResult = null;
      } else {
        if (keepAlive != null && state != State.RUNNING) {
          keepAliveMillis = keepAlive.millis();
          expirationTimeMillis = now + keepAliveMillis;
          expiryToCancel = expiryTimer;
          expiryTimer = NO_TIMEOUT;
          rescheduledExpiry = scheduler.schedule(keepAliveMillis, this::expire);
          expiryTimer = rescheduledExpiry;
        }
        executionForResult = execution;
        stateForResult = state;
        tookMillis =
            completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
        failureForResult = failure;
      }
    }

    if (expired) {
      retentionToCancel.cancel();
      expiryToCancel.cancel();
      registry.remove(id, this);
      cancelTaskAsync(taskToClose, "PPL asynchronous query expired");
      closeExecution(executionToClose);
      throw notFound();
    }

    expiryToCancel.cancel();
    return buildSnapshot(stateForResult, executionForResult, failureForResult, tookMillis, true);
  }

  /**
   * Cancels this job and returns the status observed at cancellation.
   *
   * @param reason human-readable reason recorded on the underlying task
   * @return the {@link Status} observed when the job was removed
   * @throws ResourceNotFoundException if the job has been removed or has just expired
   */
  public Status cancel(String reason) {
    Objects.requireNonNull(reason);
    Status observed;
    Cancellable retentionToCancel;
    Cancellable expiryToCancel;
    JobTask taskToClose;
    AsyncQueryExecution executionToClose;
    String cancelReason;
    boolean cancelRunning;

    synchronized (this) {
      ensurePresent();
      if (state != State.RUNNING && clock.getAsLong() >= expirationTimeMillis) {
        // Late DELETE races expiry; the job is gone.
        retentionToCancel = retentionTimer;
        expiryToCancel = expiryTimer;
        retentionTimer = NO_TIMEOUT;
        expiryTimer = NO_TIMEOUT;
        taskToClose = detachTask();
        executionToClose = detachExecution();
        state = State.REMOVED;
        cancelReason = "PPL asynchronous query expired";
        cancelRunning = true;
        observed = null;
      } else {
        observed =
            switch (state) {
              case RUNNING, RETAINED_RUNNING -> Status.CANCELLED;
              case RETAINED_SUCCEEDED -> Status.SUCCEEDED;
              case RETAINED_FAILED -> Status.FAILED;
              case REMOVED -> throw new IllegalStateException("PPL asynchronous query was removed");
            };
        cancelRunning = (state == State.RUNNING || state == State.RETAINED_RUNNING);
        retentionToCancel = retentionTimer;
        expiryToCancel = expiryTimer;
        retentionTimer = NO_TIMEOUT;
        expiryTimer = NO_TIMEOUT;
        taskToClose = detachTask();
        executionToClose = detachExecution();
        state = State.REMOVED;
        cancelReason = reason;
      }
    }

    retentionToCancel.cancel();
    expiryToCancel.cancel();
    registry.remove(id, this);
    if (cancelRunning) {
      cancelTaskAsync(taskToClose, cancelReason);
    } else {
      closeTask(taskToClose);
    }
    closeExecution(executionToClose);

    if (observed == null) {
      throw notFound();
    }
    return observed;
  }

  /**
   * Removes the job as part of an internal cleanup (shutdown or startup abort). Idempotent.
   *
   * @param reason reason recorded on the underlying task
   */
  void discard(String reason) {
    Objects.requireNonNull(reason);
    Cancellable retentionToCancel;
    Cancellable expiryToCancel;
    JobTask taskToClose;
    AsyncQueryExecution executionToClose;
    boolean wasExecuting;

    synchronized (this) {
      if (state == State.REMOVED) {
        return;
      }
      wasExecuting = (state == State.RUNNING || state == State.RETAINED_RUNNING);
      retentionToCancel = retentionTimer;
      expiryToCancel = expiryTimer;
      retentionTimer = NO_TIMEOUT;
      expiryTimer = NO_TIMEOUT;
      taskToClose = detachTask();
      executionToClose = detachExecution();
      state = State.REMOVED;
    }

    retentionToCancel.cancel();
    expiryToCancel.cancel();
    registry.remove(id, this);
    if (wasExecuting) {
      cancelTaskAsync(taskToClose, reason);
    } else {
      closeTask(taskToClose);
    }
    closeExecution(executionToClose);
  }

  // ---------- Retention deadline ----------

  private void scheduleRetention(TimeValue waitForCompletion) {
    if (waitForCompletion.millis() == 0) {
      onRetentionDeadline();
      return;
    }
    Cancellable timer = scheduler.schedule(waitForCompletion.millis(), this::onRetentionDeadline);
    synchronized (this) {
      if (state == State.RUNNING) {
        retentionTimer = timer;
        return;
      }
    }
    // Retention state already changed while the timer was being wired up. Cancel it.
    timer.cancel();
  }

  private void onRetentionDeadline() {
    Snapshot snapshotToDeliver;
    Cancellable rescheduledExpiry;

    synchronized (this) {
      if (state != State.RUNNING) {
        return;
      }
      state = State.RETAINED_RUNNING;
      long now = clock.getAsLong();
      expirationTimeMillis = now + keepAliveMillis;
      snapshotToDeliver = new Snapshot.Running(id, currentResultCopy());
      retentionTimer = NO_TIMEOUT;
      rescheduledExpiry = scheduler.schedule(keepAliveMillis, this::expire);
      expiryTimer = rescheduledExpiry;
    }

    try {
      responseListener.onResponse(snapshotToDeliver);
    } catch (RuntimeException e) {
      // Cannot deliver retained ID; discard the job so no state leaks.
      discard("PPL asynchronous query startup failed");
      safeNotifyFailure(e);
    }
  }

  // ---------- Execution attachment and completion ----------

  private void startExecution(Function<CancellableTask, AsyncQueryExecution> executionStarter) {
    AsyncQueryExecution started;
    try {
      started = Objects.requireNonNull(executionStarter.apply(task().task()));
    } catch (RuntimeException e) {
      fail(e);
      return;
    }
    if (!tryAttachExecution(started)) {
      closeExecution(started);
      return;
    }
    started
        .completion()
        .whenComplete(
            (ignored, err) -> {
              if (err == null) {
                complete();
              } else {
                fail(asException(err));
              }
            });
  }

  private synchronized JobTask task() {
    return task;
  }

  private synchronized boolean tryAttachExecution(AsyncQueryExecution incoming) {
    if (state != State.RUNNING && state != State.RETAINED_RUNNING) {
      return false;
    }
    if (execution != null) {
      return false;
    }
    execution = incoming;
    return true;
  }

  private void complete() {
    long now = clock.getAsLong();
    boolean directResponse;
    JobTask taskToClose;
    AsyncQueryExecution executionForSnapshot;
    Cancellable retentionToCancel;

    synchronized (this) {
      if (state != State.RUNNING && state != State.RETAINED_RUNNING) {
        return;
      }
      if (execution == null) {
        throw new IllegalStateException(
            "PPL asynchronous execution must be attached before successful completion");
      }
      completionTimeMillis = now;
      directResponse = (state == State.RUNNING);
      retentionToCancel = retentionTimer;
      retentionTimer = NO_TIMEOUT;
      taskToClose = detachTask();
      if (directResponse) {
        executionForSnapshot = detachExecution();
        state = State.REMOVED;
      } else {
        executionForSnapshot = null;
        state = State.RETAINED_SUCCEEDED;
      }
    }

    retentionToCancel.cancel();
    if (directResponse) {
      registry.remove(id, this);
      Snapshot snapshot;
      try {
        QueryResponse result =
            executionForSnapshot
                .currentResult()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Successful PPL asynchronous execution completed without a final"
                                + " result"));
        snapshot =
            new Snapshot.Succeeded(Optional.empty(), defensiveCopy(result), now - startTimeMillis);
      } catch (RuntimeException e) {
        safeNotifyFailure(e);
        closeTask(taskToClose);
        closeExecution(executionForSnapshot);
        return;
      }
      try {
        responseListener.onResponse(snapshot);
      } catch (RuntimeException e) {
        safeNotifyFailure(e);
      } finally {
        closeTask(taskToClose);
        closeExecution(executionForSnapshot);
      }
    } else {
      closeTask(taskToClose);
    }
  }

  private void fail(Exception cause) {
    Objects.requireNonNull(cause);
    long now = clock.getAsLong();
    boolean directResponse;
    JobTask taskToClose;
    AsyncQueryExecution executionToClose;
    Cancellable retentionToCancel;
    Failure captured = Failure.from(cause);

    synchronized (this) {
      if (state != State.RUNNING && state != State.RETAINED_RUNNING) {
        return;
      }
      completionTimeMillis = now;
      failure = captured;
      directResponse = (state == State.RUNNING);
      retentionToCancel = retentionTimer;
      retentionTimer = NO_TIMEOUT;
      taskToClose = detachTask();
      executionToClose = detachExecution();
      state = directResponse ? State.REMOVED : State.RETAINED_FAILED;
    }

    retentionToCancel.cancel();
    if (directResponse) {
      registry.remove(id, this);
      try {
        responseListener.onFailure(cause);
      } catch (RuntimeException e) {
        LOG.warn("PPL asynchronous listener rejected failure ({})", e.getClass().getSimpleName());
      } finally {
        closeTask(taskToClose);
        closeExecution(executionToClose);
      }
    } else {
      PPLQueryErrorHandler.recordFailure(cause);
      closeTask(taskToClose);
      closeExecution(executionToClose);
    }
  }

  private void expire() {
    long now = clock.getAsLong();
    Cancellable retentionToCancel;
    JobTask taskToClose;
    AsyncQueryExecution executionToClose;

    synchronized (this) {
      if (state == State.REMOVED || state == State.RUNNING || now < expirationTimeMillis) {
        return;
      }
      retentionToCancel = retentionTimer;
      retentionTimer = NO_TIMEOUT;
      expiryTimer = NO_TIMEOUT;
      taskToClose = detachTask();
      executionToClose = detachExecution();
      state = State.REMOVED;
    }

    retentionToCancel.cancel();
    registry.remove(id, this);
    cancelTaskAsync(taskToClose, "PPL asynchronous query expired");
    closeExecution(executionToClose);
  }

  // ---------- Helpers ----------

  private void ensurePresent() {
    if (state == State.REMOVED) {
      throw notFound();
    }
  }

  private JobTask detachTask() {
    JobTask detached = task;
    task = null;
    return detached;
  }

  private AsyncQueryExecution detachExecution() {
    AsyncQueryExecution detached = execution;
    execution = null;
    return detached;
  }

  private Optional<QueryResponse> currentResultCopy() {
    if (execution == null) {
      return Optional.empty();
    }
    return execution.currentResult().map(QueryJob::defensiveCopy);
  }

  private Snapshot buildSnapshot(
      State from,
      AsyncQueryExecution execForResult,
      Failure failureForResult,
      long tookMillis,
      boolean withId) {
    Optional<QueryJobId> snapshotId = withId ? Optional.of(id) : Optional.empty();
    return switch (from) {
      case RUNNING, RETAINED_RUNNING -> {
        Optional<QueryResponse> current =
            execForResult == null
                ? Optional.empty()
                : execForResult.currentResult().map(QueryJob::defensiveCopy);
        yield new Snapshot.Running(id, current);
      }
      case RETAINED_SUCCEEDED -> {
        QueryResponse result =
            Objects.requireNonNull(execForResult, "attached execution required")
                .currentResult()
                .map(QueryJob::defensiveCopy)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Successful PPL asynchronous execution completed without a final"
                                + " result"));
        yield new Snapshot.Succeeded(snapshotId, result, tookMillis);
      }
      case RETAINED_FAILED ->
          new Snapshot.Failed(snapshotId, Objects.requireNonNull(failureForResult), tookMillis);
      case REMOVED -> throw new IllegalStateException("PPL asynchronous query was removed");
    };
  }

  private void safeNotifyFailure(Exception cause) {
    try {
      responseListener.onFailure(cause);
    } catch (RuntimeException e) {
      LOG.warn("PPL asynchronous listener rejected failure ({})", e.getClass().getSimpleName());
    }
  }

  private static QueryResponse defensiveCopy(QueryResponse response) {
    Schema schema = new Schema(List.copyOf(response.getSchema().getColumns()));
    QueryResponse copy =
        new QueryResponse(schema, List.copyOf(response.getResults()), response.getCursor());
    copy.setWarnings(List.copyOf(response.getWarnings()));
    return copy;
  }

  private static void closeTask(JobTask task) {
    if (task != null) {
      task.close();
    }
  }

  private void cancelTaskAsync(JobTask jobTask, String reason) {
    if (jobTask == null) {
      return;
    }
    CancellableTask cancellable = jobTask.task();
    if (cancellable == null || cancellable.isCancelled()) {
      jobTask.close();
      return;
    }
    try {
      if (taskManager == null) {
        cancellable.cancel(reason);
        jobTask.close();
      } else {
        taskManager.cancelTaskAndDescendants(
            cancellable,
            reason,
            false,
            ActionListener.wrap(
                ignored -> jobTask.close(),
                error -> {
                  jobTask.close();
                  LOG.warn(
                      "Failed to cancel descendants of PPL asynchronous query task ({})",
                      error.getClass().getSimpleName());
                }));
      }
    } catch (RuntimeException e) {
      jobTask.close();
      LOG.warn("Failed to cancel PPL asynchronous query task ({})", e.getClass().getSimpleName());
    }
  }

  private static void closeExecution(AsyncQueryExecution execution) {
    if (execution == null) {
      return;
    }
    try {
      execution.close();
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to close PPL asynchronous query execution ({})", e.getClass().getSimpleName());
    }
  }

  private static Exception asException(Throwable failure) {
    Throwable cause =
        failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    return cause instanceof Exception exception ? exception : new RuntimeException(cause);
  }

  private static ResourceNotFoundException notFound() {
    return new ResourceNotFoundException("PPL asynchronous query not found");
  }

  private static Scheduler wrapScheduler(ThreadPool threadPool) {
    return (delayMillis, task) -> {
      ScheduledCancellable cancellable =
          threadPool.schedule(
              task, TimeValue.timeValueMillis(delayMillis), ThreadPool.Names.GENERIC);
      return cancellable::cancel;
    };
  }

  private static JobTask registerJobTask(
      TaskManager taskManager, PPLQueryTask parentTask, TransportPPLQueryRequest request) {
    Objects.requireNonNull(parentTask, "PPL asynchronous query request task is not initialized");
    DiscoveryNode localNode =
        Objects.requireNonNull(taskManager.localNode(), "Local node is not initialized");

    // Registered directly rather than through TransportAction.execute(); reproduce the two child-
    // task bookkeeping steps TransportAction would perform. childNode registration lets parent
    // cancellation ban this node; parentTaskId lets the ban find and cancel the retained task.
    Releasable childNodeRegistration = taskManager.registerChildNode(parentTask.getId(), localNode);
    TaskId originalParent = request.getParentTask();
    boolean registered = false;
    try {
      request.setParentTask(localNode.getId(), parentTask.getId());
      Task task = taskManager.register("transport", PPLQueryAction.NAME, request);
      if (!(task instanceof PPLQueryTask pplQueryTask)) {
        taskManager.unregister(task);
        throw new IllegalStateException("Failed to create PPL asynchronous query task");
      }
      registered = true;
      return new JobTask(
          pplQueryTask,
          () -> {
            try {
              taskManager.unregister(pplQueryTask);
            } finally {
              childNodeRegistration.close();
            }
          });
    } finally {
      request.setParentTask(originalParent);
      if (!registered) {
        childNodeRegistration.close();
      }
    }
  }
}
