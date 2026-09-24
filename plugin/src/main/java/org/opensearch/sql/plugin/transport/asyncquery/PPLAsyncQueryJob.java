/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.util.Objects;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.tasks.CancellableTask;

/**
 * Mutable state machine for one asynchronous PPL query.
 *
 * <p>Every lifecycle and lease transition is synchronized on this object. Methods mutate only
 * job-owned state and return immutable transition values; they never call listeners, mutate the
 * service map, update capacity counters, or cancel tasks while holding the lock.
 *
 * <p>After attachment, the job owns one {@link AsyncQueryExecution}. Reads borrow it through a
 * {@link ResponseContext}; removal transitions detach it so the service can close it outside the
 * lock.
 *
 * <pre>
 * Every job starts in RUNNING. It becomes retained when wait_for_completion_timeout expires.
 *
 * Current state       Event              Next state             Response
 * RUNNING             success            REMOVED                final result without ID
 * RUNNING             failure            REMOVED                failure without ID
 * RUNNING             retain             RETAINED_RUNNING       running status with ID
 * RETAINED_RUNNING    success            RETAINED_SUCCEEDED     none
 * RETAINED_RUNNING    failure            RETAINED_FAILED        none
 * RUNNING             abort/close        REMOVED                none
 * RETAINED_*          delete/expire/abort/close REMOVED         none
 * </pre>
 *
 * <p>GET lease renewal and execution attachment do not change the lifecycle state. Events received
 * after {@code REMOVED} are ignored or reported as not found; a late execution handle is rejected
 * so the service can close it.
 */
final class PPLAsyncQueryJob {
  private final String id;
  private final PPLAsyncQueryUser owner;
  private final long startTimeMillis;
  private JobTask task;

  private long keepAliveMillis;
  private long expirationTimeMillis;
  private State state;
  private AsyncQueryExecution execution;
  private PPLAsyncQueryService.Failure failure;
  private long completionTimeMillis = -1L;

  /**
   * Creates an unretained job in the {@link State#RUNNING} state.
   *
   * @param id opaque ID assigned by the owner node
   * @param owner caller that is allowed to access the retained job
   * @param startTimeMillis execution start time
   * @param keepAliveMillis lease duration applied when the job is retained
   * @param task independently cancellable task owned by the job
   */
  PPLAsyncQueryJob(
      String id,
      PPLAsyncQueryUser owner,
      long startTimeMillis,
      long keepAliveMillis,
      JobTask task) {
    this.id = id;
    this.owner = owner;
    this.startTimeMillis = startTimeMillis;
    this.keepAliveMillis = keepAliveMillis;
    this.expirationTimeMillis = startTimeMillis + keepAliveMillis;
    this.task = Objects.requireNonNull(task);
    this.state = State.RUNNING;
  }

  /**
   * Returns the opaque ID assigned to this job.
   *
   * @return job ID used as the service registry key
   */
  String id() {
    return id;
  }

  /**
   * Returns the immutable identity captured when this job was created.
   *
   * @return job owner used by the service for GET and DELETE authorization
   */
  PPLAsyncQueryUser owner() {
    return owner;
  }

  /**
   * Retains a running job after its initial response wait expires.
   *
   * @param now time at which the job becomes visible to GET and DELETE
   * @return transition that publishes the job ID, or {@code null} if the job already finished
   */
  synchronized Transition retain(long now) {
    if (state != State.RUNNING) {
      return null;
    }
    state = State.RETAINED_RUNNING;
    expirationTimeMillis = now + keepAliveMillis;
    return Transition.retain(retainedResponse());
  }

  /**
   * Transfers ownership of an execution handle to this job.
   *
   * @param execution handle that produces current and final query results
   * @return {@code true} when attached; {@code false} when the caller must close the rejected
   *     handle
   */
  synchronized boolean tryAttachExecution(AsyncQueryExecution execution) {
    Objects.requireNonNull(execution);
    if (!isExecuting() || this.execution != null) {
      return false;
    }
    this.execution = execution;
    return true;
  }

  /**
   * Records successful execution completion.
   *
   * @param now completion time
   * @return direct-response or retained-completion transition, or {@code null} after removal
   * @throws IllegalStateException if successful completion is reported before execution attachment
   */
  synchronized Transition complete(long now) {
    if (!isExecuting()) {
      return null;
    }
    if (execution == null) {
      throw new IllegalStateException(
          "PPL asynchronous execution must be attached before successful completion");
    }
    return finish(State.RETAINED_SUCCEEDED, now);
  }

  /**
   * Records failed execution completion.
   *
   * @param failure client-visible failure retained with the job
   * @param now completion time
   * @return direct-response or retained-completion transition, or {@code null} after removal
   */
  synchronized Transition fail(PPLAsyncQueryService.Failure failure, long now) {
    if (!isExecuting()) {
      return null;
    }
    this.failure = failure;
    return finish(State.RETAINED_FAILED, now);
  }

  private Transition finish(State terminalState, long now) {
    boolean returnDirect = state == State.RUNNING;
    JobTask taskToClose = detachTask();
    state = terminalState;
    completionTimeMillis = now;
    if (returnDirect) {
      ResponseContext response = directResponse();
      state = State.REMOVED;
      return Transition.returnDirect(response, detachExecution(), taskToClose);
    }
    return Transition.finishRetained(
        terminalState == State.RETAINED_FAILED ? detachExecution() : null, taskToClose);
  }

  /**
   * Returns the current retained response.
   *
   * <p>A supplied {@code requestedKeepAlive} starts a new lease from {@code now}. A request at or
   * after the existing expiration time returns an expiration removal instead of data.
   *
   * @param now request time
   * @param requestedKeepAlive replacement lease, or {@code null} to keep the current expiration
   * @return current response or the removal required for an expired job
   * @throws ResourceNotFoundException if the job was already removed
   */
  synchronized GetResult get(long now, TimeValue requestedKeepAlive) {
    ensurePresent();
    if (now >= expirationTimeMillis) {
      return new GetResult.Expired(expireLocked("PPL asynchronous query expired"));
    }
    if (requestedKeepAlive != null) {
      keepAliveMillis = requestedKeepAlive.millis();
      expirationTimeMillis = now + keepAliveMillis;
    }
    return new GetResult.Found(retainedResponse());
  }

  /**
   * Cancels if still running and removes this job.
   *
   * @param now request time
   * @return removal containing the response status and detached resources
   * @throws ResourceNotFoundException if the job was already removed
   */
  synchronized Removal delete(long now) {
    ensurePresent();
    if (now >= expirationTimeMillis) {
      return expireLocked("PPL asynchronous query expired");
    }
    return remove(
        isExecuting() ? PPLAsyncQueryService.Status.CANCELLED : responseStatus(),
        "PPL asynchronous query cancelled by user");
  }

  /**
   * Removes a retained job whose lease has expired.
   *
   * @param now expiration check time
   * @return removal with detached resources, or {@code null} if no expiration is due
   */
  synchronized Removal expire(long now) {
    if (state == State.REMOVED || state == State.RUNNING || now < expirationTimeMillis) {
      return null;
    }
    return expireLocked("PPL asynchronous query expired");
  }

  private Removal expireLocked(String reason) {
    return remove(responseStatus(), reason, true);
  }

  /**
   * Removes a job whose submission could not be completed.
   *
   * @return removal with detached resources, or {@code null} if already removed
   */
  synchronized Removal abort() {
    return removeIfPresent("PPL asynchronous query startup failed");
  }

  /**
   * Removes this job during service shutdown.
   *
   * @param reason cancellation reason used if execution is still running
   * @return removal with detached resources, or {@code null} if already removed
   */
  synchronized Removal close(String reason) {
    return removeIfPresent(reason);
  }

  private Removal removeIfPresent(String reason) {
    if (state == State.REMOVED) {
      return null;
    }
    return remove(responseStatus(), reason);
  }

  private Removal remove(PPLAsyncQueryService.Status responseStatus, String reason) {
    return remove(responseStatus, reason, false);
  }

  private Removal remove(
      PPLAsyncQueryService.Status responseStatus, String reason, boolean expired) {
    Removal removal = new Removal(responseStatus, detachTask(), detachExecution(), reason, expired);
    state = State.REMOVED;
    return removal;
  }

  private ResponseContext directResponse() {
    return responseContext(null);
  }

  private ResponseContext retainedResponse() {
    return responseContext(id);
  }

  private ResponseContext responseContext(String responseId) {
    long tookMillis =
        completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
    return new ResponseContext(responseId, responseStatus(), execution, failure, tookMillis);
  }

  private boolean isExecuting() {
    return state == State.RUNNING || state == State.RETAINED_RUNNING;
  }

  private PPLAsyncQueryService.Status responseStatus() {
    return switch (state) {
      case RUNNING, RETAINED_RUNNING -> PPLAsyncQueryService.Status.RUNNING;
      case RETAINED_SUCCEEDED -> PPLAsyncQueryService.Status.SUCCEEDED;
      case RETAINED_FAILED -> PPLAsyncQueryService.Status.FAILED;
      case REMOVED -> throw new IllegalStateException("PPL asynchronous query was removed");
    };
  }

  private AsyncQueryExecution detachExecution() {
    AsyncQueryExecution detached = execution;
    execution = null;
    return detached;
  }

  private JobTask detachTask() {
    JobTask detached = task;
    task = null;
    return detached;
  }

  private void ensurePresent() {
    if (state == State.REMOVED) {
      throw new ResourceNotFoundException("PPL asynchronous query not found");
    }
  }

  /**
   * Lifecycle data captured under the job lock for later response materialization.
   *
   * @param id job ID included in a retained response, or {@code null} for a direct POST response
   * @param status public lifecycle status
   * @param execution execution handle borrowed for result materialization
   * @param failure failure returned for a failed job
   * @param tookMillis elapsed execution time, or {@code -1} while running
   */
  record ResponseContext(
      String id,
      PPLAsyncQueryService.Status status,
      AsyncQueryExecution execution,
      PPLAsyncQueryService.Failure failure,
      long tookMillis) {}

  /**
   * Cancellable task and the cleanup that releases its TaskManager registrations.
   *
   * @param task task used to cancel query execution
   * @param release registration cleanup
   */
  record JobTask(CancellableTask task, Runnable release) {
    /** Releases the task and child-node registrations owned by this wrapper. */
    void close() {
      release.run();
    }
  }

  /** Internal lifecycle; unlike the response status, this includes retention and removal. */
  enum State {
    RUNNING,
    RETAINED_RUNNING,
    RETAINED_SUCCEEDED,
    RETAINED_FAILED,
    REMOVED
  }

  /** Whether a transition keeps the job in or removes it from the service registry. */
  enum Retention {
    RETAIN,
    REMOVE
  }

  /**
   * State-machine output consumed by {@link PPLAsyncQueryService}.
   *
   * @param response response to materialize and publish, or {@code null} when none is due
   * @param retention registry action
   * @param executionToClose execution handle detached by the transition
   * @param taskToClose completed task registration detached by the transition
   */
  record Transition(
      ResponseContext response,
      Retention retention,
      AsyncQueryExecution executionToClose,
      JobTask taskToClose) {

    private static Transition retain(ResponseContext response) {
      return new Transition(response, Retention.RETAIN, null, null);
    }

    private static Transition returnDirect(
        ResponseContext response, AsyncQueryExecution executionToClose, JobTask taskToClose) {
      return new Transition(response, Retention.REMOVE, executionToClose, taskToClose);
    }

    private static Transition finishRetained(
        AsyncQueryExecution executionToClose, JobTask taskToClose) {
      return new Transition(null, Retention.RETAIN, executionToClose, taskToClose);
    }

    /** Returns whether this transition releases a running-query capacity slot. */
    boolean releasesRunningSlot() {
      return taskToClose != null;
    }
  }

  /** Result of an authorized GET attempt. */
  sealed interface GetResult {
    /**
     * GET result for a live retained job.
     *
     * @param response current response context
     */
    record Found(ResponseContext response) implements GetResult {}

    /**
     * GET result when the lease expired before the request.
     *
     * @param removal cleanup required for the expired job
     */
    record Expired(Removal removal) implements GetResult {}
  }

  /**
   * Resources and accounting changes produced when a job leaves the registry.
   *
   * @param responseStatus status returned to DELETE when the job has not expired
   * @param task running task to cancel
   * @param execution execution handle to close
   * @param reason cancellation or removal reason
   * @param expired whether expiration caused the removal
   */
  record Removal(
      PPLAsyncQueryService.Status responseStatus,
      JobTask task,
      AsyncQueryExecution execution,
      String reason,
      boolean expired) {

    /** Returns whether this removal releases a running-query capacity slot. */
    boolean releasesRunningSlot() {
      return task != null;
    }
  }
}
